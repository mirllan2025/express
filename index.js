import * as zookeeper from 'node-zookeeper-client';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import _ from 'lodash';
import axios from 'axios';
const argv = await yargs(hideBin(process.argv))
    .option('zk', {
    alias: 'zookeeper',
    type: 'string',
    description: 'Zookeeper 连接串',
    default: '70.36.96.27:2181'
})
    .option('path', {
    alias: 'barrierPath',
    type: 'string',
    description: '屏障节点路径',
    default: '/barrier'
})
    .option('count', {
    alias: 'participantCount',
    type: 'number',
    description: '需要同步的进程数量',
    default: 50
})
    .option('value', {
    alias: 'participantValue',
    type: 'number',
    description: '参与者数值'
})
    .option('exitDelay', {
    alias: 'exitDelayMs',
    type: 'number',
    description: '屏障通过后安全退出的延迟时间（毫秒）',
    default: 2000
})
    .help()
    .argv;
const zkConnectionString = argv.zk;
const barrierPath = argv.path;
const participantCount = argv.count;
const participantValue = argv.value;
const exitDelayMs = argv.exitDelay;
const client = zookeeper.createClient(zkConnectionString);
process.on('SIGINT', async () => {
    // Ctrl+C
    console.log('SIGINT: 用户中断');
    client.close();
    process.exit();
});
process.on('SIGTERM', async () => {
    // timeout docker-compose down/stop 会触发 SIGTERM 信号
    console.log('SIGTERM: 终止请求');
    client.close();
    process.exit();
});
// 屏障同步逻辑，统计数值
function enterBarrier(client, barrierPath, participantCount, participantValue) {
    return new Promise((resolve, reject) => {
        // 记录屏障开始时间
        const startTime = Date.now();
        // 创建屏障父节点（如不存在）
        client.mkdirp(barrierPath, async (err) => {
            if (err)
                return reject(err);
            // 每个参与者创建自己的临时子节点
            const nodePath = `${barrierPath}/participant-`;
            const { data: ip } = await axios.get('https://ipinfo.io/ip');
            client.create(nodePath, Buffer.from(JSON.stringify({ repository: process.env.GITHUB_REPOSITORY, participantValue, ip })), zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL, (err, createdPath) => {
                if (err)
                    return reject(err);
                const fullCreatedNode = createdPath.split('/').pop();
                let lastChildren = [];
                const participantMetaMap = new Map();
                let barrierPassed = false;
                let leaderNode;
                let noNewNodeTimer = null;
                function resetNoNewNodeTimer() {
                    if (noNewNodeTimer)
                        clearTimeout(noNewNodeTimer);
                    noNewNodeTimer = setTimeout(() => {
                        if (!barrierPassed) {
                            console.error(`::error::120秒内未检测到新节点，自动退出`);
                            client.close();
                            process.exit(1);
                        }
                    }, 120_000);
                }
                function checkBarrier() {
                    if (barrierPassed)
                        return;
                    client.getChildren(barrierPath, (event) => { checkBarrier(); }, async (err, children) => {
                        if (err)
                            return reject(err);
                        if (barrierPassed)
                            return;
                        if (lastChildren.length > 0 && children.length < lastChildren.length) {
                            console.error(`::error::检测到屏障节点数减少（${lastChildren.length} -> ${children.length}），可能有参与者异常退出，屏障流程终止。`);
                            client.close();
                            process.exit(1);
                        }
                        // 找出新增节点
                        const added = children.filter(child => !lastChildren.includes(child));
                        lastChildren = children;
                        // 每次有新节点就重置定时器
                        if (added.length > 0) {
                            resetNoNewNodeTimer();
                        }
                        if (added.length > 0 && participantValue != null) {
                            if (!leaderNode) {
                                const sortedChildren = [...children].sort();
                                leaderNode = sortedChildren[0];
                            }
                            if (fullCreatedNode == leaderNode) {
                                const startGet = Date.now();
                                await Promise.all(added.map(child => {
                                    return new Promise(resolve => {
                                        client.getData(`${barrierPath}/${child}`, (err, data) => {
                                            if (!err) {
                                                const meta = JSON.parse(data.toString());
                                                participantMetaMap.set(child, meta);
                                            }
                                            resolve();
                                        });
                                    });
                                }));
                                const participantMetas = children.map(child => participantMetaMap.get(child)).filter(Boolean);
                                console.log('当前节点总数:', participantMetas.length);
                                const allValues = participantMetas.map(meta => meta.participantValue);
                                const max = _.max(allValues);
                                const min = _.min(allValues);
                                const avg = _.mean(allValues);
                                console.log(`最大值: ${max}, 最小值: ${min}, 平均值: ${avg}`);
                                const allIps = participantMetas.map(meta => meta.ip);
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
                                    console.log(`未找到leader节点(${leaderNode})的元信息，将从zookeeper拉取数据...`);
                                    await new Promise(resolve => {
                                        client.getData(`${barrierPath}/${leaderNode}`, (err, data) => {
                                            if (!err) {
                                                const meta = JSON.parse(data.toString());
                                                participantMetaMap.set(leaderNode, meta);
                                            }
                                            resolve();
                                        });
                                    });
                                    leaderMeta = participantMetaMap.get(leaderNode);
                                }
                                console.log(`leader节点(${leaderNode})的元信息:`, leaderMeta);
                            }
                        }
                        if (children.length < participantCount) {
                            console.log(barrierPath, `等待中，当前已就绪: ${children.length} / ${participantCount}`);
                            return;
                        }
                        barrierPassed = true;
                        console.log('屏障已通过！所有参与者已就绪。', children.length);
                        console.log(`屏障耗时: ${((Date.now() - startTime) / 1000).toFixed(1)} 秒`);
                        fullCreatedNode == leaderNode && console.log("::notice::" + leaderNode);
                        if (noNewNodeTimer) {
                            clearTimeout(noNewNodeTimer);
                            noNewNodeTimer = null;
                        }
                        resolve();
                    });
                }
                checkBarrier();
            });
        });
    });
}
client.once('connected', async () => {
    try {
        await enterBarrier(client, barrierPath, participantCount, participantValue);
        console.log('屏障已通过，等待所有参与者检测到屏障...');
        setTimeout(() => {
            console.log('安全退出');
            client.close();
            process.exit();
        }, exitDelayMs);
    }
    catch (e) {
        console.error('屏障出错:', e);
        client.close();
        process.exit(1);
    }
});
client.connect();
//# sourceMappingURL=distributed_barrier.js.map
