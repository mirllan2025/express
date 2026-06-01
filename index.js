import Redis from 'ioredis';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import _ from 'lodash';
import axios from 'axios';
const MEMBER_TTL_SEC = 30;
const HEARTBEAT_INTERVAL_MS = 10_000;
const POLL_INTERVAL_MS = 2_000;
const argv = await yargs(hideBin(process.argv))
    .option('redis', {
    alias: 'redisUrl',
    type: 'string',
    description: 'Redis 连接 URL（也可通过 REDIS_URL 环境变量设置）',
    default: process.env.REDIS_URL,
})
    .option('path', {
    alias: 'barrierPath',
    type: 'string',
    description: '屏障路径（映射为 Redis key 前缀）',
    default: '/barrier',
})
    .option('count', {
    alias: 'participantCount',
    type: 'number',
    description: '需要同步的进程数量',
    default: 50,
})
    .option('value', {
    alias: 'participantValue',
    type: 'number',
    description: '参与者数值',
})
    .option('exitDelay', {
    alias: 'exitDelayMs',
    type: 'number',
    description: '屏障通过后安全退出的延迟时间（毫秒）',
    default: 2000,
})
    .help()
    .argv;
const redisUrl = argv.redis;
const barrierPath = argv.path;
const participantCount = argv.count;
const participantValue = argv.value;
const exitDelayMs = argv.exitDelay;
if (!redisUrl) {
    console.error('请设置 REDIS_URL 环境变量或通过 --redis 指定连接 URL');
    process.exit(1);
}
function barrierKeyPrefix(path) {
    const name = path.replace(/^\//, '').replace(/\//g, ':') || 'barrier';
    return `{${name}}`;
}
function memberKey(prefix, memberId) {
    return `${prefix}:member:${memberId}`;
}
const redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
const subscriber = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
const prefix = barrierKeyPrefix(barrierPath);
const membersKey = `${prefix}:members`;
const seqKey = `${prefix}:seq`;
const eventsChannel = `${prefix}:events`;
let memberId = null;
let heartbeatTimer = null;
let pollTimer = null;
let noNewNodeTimer = null;
let cleaningUp = false;
async function cleanup() {
    if (cleaningUp)
        return;
    cleaningUp = true;
    if (heartbeatTimer)
        clearInterval(heartbeatTimer);
    if (pollTimer)
        clearInterval(pollTimer);
    if (noNewNodeTimer)
        clearTimeout(noNewNodeTimer);
    try {
        if (memberId) {
            await redis.zrem(membersKey, memberId);
            await redis.del(memberKey(prefix, memberId));
        }
    }
    catch {
        // ignore cleanup errors
    }
    subscriber.disconnect();
    redis.disconnect();
}
process.on('SIGINT', async () => {
    console.log('SIGINT: 用户中断');
    await cleanup();
    process.exit();
});
process.on('SIGTERM', async () => {
    console.log('SIGTERM: 终止请求');
    await cleanup();
    process.exit();
});
async function registerParticipant(meta) {
    const seq = await redis.incr(seqKey);
    const id = `participant-${String(seq).padStart(10, '0')}`;
    const payload = JSON.stringify(meta);
    await redis
        .multi()
        .set(memberKey(prefix, id), payload, 'EX', MEMBER_TTL_SEC)
        .zadd(membersKey, seq, id)
        .publish(eventsChannel, `join:${id}`)
        .exec();
    return id;
}
function startHeartbeat(id) {
    heartbeatTimer = setInterval(() => {
        redis.expire(memberKey(prefix, id), MEMBER_TTL_SEC).catch(() => { });
    }, HEARTBEAT_INTERVAL_MS);
}
async function getMemberMeta(id) {
    const data = await redis.get(memberKey(prefix, id));
    if (!data)
        return null;
    return JSON.parse(data);
}
async function listMembers() {
    return redis.zrange(membersKey, 0, -1);
}
async function enterBarrier(fullCreatedNode, count, value) {
    const startTime = Date.now();
    let lastChildren = [];
    const participantMetaMap = new Map();
    let barrierPassed = false;
    let leaderNode;
    let checking = false;
    function resetNoNewNodeTimer() {
        if (noNewNodeTimer)
            clearTimeout(noNewNodeTimer);
        noNewNodeTimer = setTimeout(() => {
            if (!barrierPassed) {
                console.error('::error::120秒内未检测到新节点，自动退出');
                cleanup().then(() => process.exit(1));
            }
        }, 120_000);
    }
    return new Promise((resolve, reject) => {
        async function checkBarrier() {
            if (barrierPassed || checking)
                return;
            checking = true;
            try {
                const children = await listMembers();
                if (barrierPassed)
                    return;
                if (lastChildren.length > 0 && children.length < lastChildren.length) {
                    console.error(`::error::检测到屏障节点数减少（${lastChildren.length} -> ${children.length}），可能有参与者异常退出，屏障流程终止。`);
                    await cleanup();
                    process.exit(1);
                }
                const added = children.filter((child) => !lastChildren.includes(child));
                lastChildren = children;
                if (added.length > 0) {
                    resetNoNewNodeTimer();
                }
                if (added.length > 0 && value != null) {
                    if (!leaderNode) {
                        leaderNode = [...children].sort()[0];
                    }
                    if (fullCreatedNode === leaderNode) {
                        const startGet = Date.now();
                        await Promise.all(added.map(async (child) => {
                            const meta = await getMemberMeta(child);
                            if (meta)
                                participantMetaMap.set(child, meta);
                        }));
                        const participantMetas = children
                            .map((child) => participantMetaMap.get(child))
                            .filter(Boolean);
                        console.log('当前节点总数:', participantMetas.length);
                        const allValues = participantMetas.map((meta) => meta.participantValue);
                        const max = _.max(allValues);
                        const min = _.min(allValues);
                        const avg = _.mean(allValues);
                        console.log(`最大值: ${max}, 最小值: ${min}, 平均值: ${avg}`);
                        const allIps = participantMetas.map((meta) => meta.ip);
                        const uniqueIpCount = new Set(allIps).size;
                        const top10Ips = Object.entries(_.countBy(allIps))
                            .sort((a, b) => b[1] - a[1])
                            .slice(0, 10);
                        console.log('不重复IP数量:', uniqueIpCount);
                        console.log('出现次数最多的前10个IP:', top10Ips);
                        console.log('新增节点总数:', added.length);
                        console.log(`本轮耗时: ${((Date.now() - startGet) / 1000).toFixed(1)} 秒`);
                    }
                    else {
                        let leaderMeta = participantMetaMap.get(leaderNode);
                        if (!leaderMeta) {
                            console.log(`未找到leader节点(${leaderNode})的元信息，将从 Redis 拉取数据...`);
                            leaderMeta = (await getMemberMeta(leaderNode)) ?? undefined;
                            if (leaderMeta)
                                participantMetaMap.set(leaderNode, leaderMeta);
                        }
                        console.log(`leader节点(${leaderNode})的元信息:`, leaderMeta);
                    }
                }
                if (children.length < count) {
                    console.log(barrierPath, `等待中，当前已就绪: ${children.length} / ${count}`);
                    return;
                }
                barrierPassed = true;
                console.log('屏障已通过！所有参与者已就绪。', children.length);
                console.log(`屏障耗时: ${((Date.now() - startTime) / 1000).toFixed(1)} 秒`);
                if (fullCreatedNode === leaderNode) {
                    console.log('::notice::' + leaderNode);
                }
                if (noNewNodeTimer) {
                    clearTimeout(noNewNodeTimer);
                    noNewNodeTimer = null;
                }
                resolve();
            }
            catch (err) {
                reject(err);
            }
            finally {
                checking = false;
            }
        }
        subscriber.on('message', () => {
            checkBarrier();
        });
        pollTimer = setInterval(() => {
            checkBarrier();
        }, POLL_INTERVAL_MS);
        checkBarrier();
    });
}
try {
    await redis.ping();
    console.log('已连接 Redis');
    const { data: ip } = await axios.get('https://ipinfo.io/ip');
    memberId = await registerParticipant({
        repository: process.env.GITHUB_REPOSITORY,
        participantValue,
        ip: ip.trim(),
    });
    console.log('已注册参与者:', memberId);
    startHeartbeat(memberId);
    await subscriber.subscribe(eventsChannel);
    await enterBarrier(memberId, participantCount, participantValue);
    console.log('屏障已通过，等待所有参与者检测到屏障...');
    setTimeout(async () => {
        console.log('安全退出');
        await cleanup();
        process.exit();
    }, exitDelayMs);
}
catch (e) {
    console.error('屏障出错:', e);
    await cleanup();
    process.exit(1);
}
//# sourceMappingURL=distributed_barrier_redis.js.map
