package io.github.nasaruntime.redis.cache.redis.job;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 业务作用：维护当前进程的稳定节点身份、Worker 能力、轻量心跳和不可变 Fanout 快照。
 */
final class RedisJobExecutorRegistry {

    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final String nodeIdentity;
    private final String startupId = UUID.randomUUID().toString().replace("-", "");
    private final String executorId;
    private final Set<String> workers = ConcurrentHashMap.newKeySet();
    private final AtomicInteger inflight = new AtomicInteger();
    private final Object inflightMonitor = new Object();
    private volatile String state = "ACTIVE";
    private volatile boolean withdrawn;

    /**
     * 业务作用：建立执行器两层身份，稳定节点身份用于定向 inbox，启动身份只用于 owner 观测。
     *
     * @param properties Job 配置
     * @param keys       键路由器
     * @param scripts    脚本执行器
     */
    RedisJobExecutorRegistry(RedisJobProperties properties, RedisJobKeyspace keys, RedisJobScriptExecutor scripts) {
        this.properties = properties;
        this.keys = keys;
        this.scripts = scripts;
        String stable = properties.getInstanceIdentity();
        if (stable == null || stable.isBlank()) stable = hostIdentity();
        this.nodeIdentity = RedisJobNames.requireName(properties.getApplicationName(), "applicationName")
                + ':' + RedisJobNames.requireName(stable, "instanceIdentity");
        this.executorId = nodeIdentity + ':' + startupId;
    }

    /**
     * 业务作用：读取稳定节点身份。 @return 节点身份。
     */
    String nodeIdentity() {
        return nodeIdentity;
    }

    /**
     * 业务作用：读取当前进程执行器身份。 @return 执行器身份。
     */
    String executorId() {
        return executorId;
    }

    /**
     * 业务作用：执行重路径能力登记并原子比对规范契约与 Worker 摘要绑定。
     *
     * @param definition 本地任务定义
     *                   返回：无返回值；冲突时抛出异常并拒绝登记能力。
     */
    synchronized void register(RedisJobDefinition definition) {
        if (withdrawn) throw new IllegalStateException("RedisJob executor is withdrawn");
        String workerName = definition.workerName();
        String codecs = definition.wireCodecs();
        String implementationDigest = RedisJobIdentifiers.workerKey(
                properties.getApplicationName() + ":java:" + definition.name());
        String capabilityDigest = RedisJobIdentifiers.workerKey(workerName + ':' + definition.contractRevision()
                + ':' + definition.schemaId() + ':' + codecs);
        List<Object> result = scripts.list(RedisJobScript.EXECUTOR_REGISTER,
                new String[]{keys.executors(), keys.executor(executorId), keys.capability(workerName),
                        keys.capabilityMeta(workerName), keys.contract(workerName, definition.contractRevision()),
                        keys.workerKeyBinding(workerName)},
                executorId, nodeIdentity, startupId, properties.getApplicationName(), "java", state,
                workerName, definition.contractRevision(), definition.schemaId(), codecs,
                implementationDigest, properties.getExecutorCapacity(), properties.getExecutorExpireMs(),
                capabilityDigest, capabilityDigest);
        String code = value(result, 0);
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob capability rejected: " + code);
        workers.add(workerName);
    }

    /**
     * 业务作用：走轻量路径续期执行器和已经登记的能力，不重复写契约元数据。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    synchronized void heartbeat() {
        if (withdrawn) return;
        List<String> sorted = workers.stream().sorted().toList();
        String[] heartbeatKeys = new String[3 + sorted.size()];
        heartbeatKeys[0] = keys.executors();
        heartbeatKeys[1] = keys.executor(executorId);
        heartbeatKeys[2] = keys.fanoutEvidence(nodeIdentity);
        for (int index = 0; index < sorted.size(); index++) {
            heartbeatKeys[index + 3] = keys.capability(sorted.get(index));
        }
        List<Object> result = scripts.list(RedisJobScript.EXECUTOR_HEARTBEAT, heartbeatKeys,
                executorId, state, properties.getExecutorExpireMs(), inflight.get(), "ACTIVE".equals(state));
        String code = value(result, 0);
        if ("NOT_FOUND".equals(code)) throw new IllegalStateException("RedisJob executor registration expired");
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob heartbeat rejected: " + code);
    }

    /**
     * 业务作用：在 registry slot 原子选择当前存活且契约兼容的执行器，并按稳定节点身份冻结顺序。
     *
     * @param workerName       Worker 能力名
     * @param contractRevision 契约修订号
     * @param schemaId         Schema 标识
     * @param codec            线编码
     * @return 不可变能力快照。
     */
    RedisJobClusterSnapshot snapshot(String workerName, long contractRevision, String schemaId,
                                     RedisJobWireCodec codec) {
        List<Object> result = scripts.list(RedisJobScript.FANOUT_SNAPSHOT,
                new String[]{keys.executors(), keys.capability(workerName), keys.capabilityMeta(workerName),
                        keys.contract(workerName, contractRevision)},
                workerName, contractRevision, schemaId, codec.name(), properties.getFanoutMaxMembers(),
                keys.registryKeyPrefix());
        String code = value(result, 0);
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob snapshot rejected: " + code);
        long selectedAt = Long.parseLong(value(result, 1));
        List<RedisJobExecutorMember> members = new ArrayList<>();
        for (int index = 2; index + 6 < result.size(); index += 7) {
            members.add(new RedisJobExecutorMember(value(result, index), value(result, index + 1),
                    value(result, index + 2), value(result, index + 3), value(result, index + 4),
                    value(result, index + 5), Long.parseLong(value(result, index + 6))));
        }
        members.sort(Comparator.comparing(RedisJobExecutorMember::nodeIdentity));
        String canonical = members.stream().map(member -> member.nodeIdentity() + ':' + member.executorId()
                + ':' + member.heartbeatRevision()).collect(Collectors.joining("|"));
        String digest = RedisJobIdentifiers.workerKey(workerName + ':' + selectedAt + ':' + canonical);
        String snapshotId = digest.substring(0, 32);
        return new RedisJobClusterSnapshot(snapshotId, workerName, selectedAt, members, digest);
    }

    /**
     * 业务作用：记录跨 Fanout 的节点失联证据；只有匹配观测启动和心跳修订号的不同根才能降级节点。
     *
     * @param nodeIdentity      目标稳定节点
     * @param startupId         快照观测的启动标识
     * @param heartbeatRevision 快照观测的心跳修订号
     * @param fanoutId          产生证据的 Fanout 标识
     * @return 证据处理状态码。
     */
    String recordFanoutEvidence(String nodeIdentity, String startupId,
                                long heartbeatRevision, String fanoutId) {
        List<Object> result = scripts.list(RedisJobScript.EXECUTOR_RECORD_FANOUT_EVIDENCE,
                new String[]{keys.executors(), keys.fanoutEvidence(nodeIdentity)},
                nodeIdentity, startupId, heartbeatRevision, fanoutId,
                properties.getNodeUnreadyEvidenceCount(), properties.getExecutorExpireMs(),
                properties.getRegistryGcGraceMs(), keys.registryKeyPrefix());
        return value(result, 0);
    }

    /**
     * 业务作用：回收超过宽限期的执行器记录及其能力反向索引，避免注册表随进程重启无限增长。
     *
     * <p>参数说明: 无。
     *
     * @return 本轮实际删除的执行器数量。
     */
    long gc() {
        List<Object> result = scripts.list(RedisJobScript.REGISTRY_GC,
                new String[]{keys.executors(), keys.registryGc()},
                properties.getRegistryGcGraceMs(), properties.getScanBatchSize(), keys.registryKeyPrefix());
        return Long.parseLong(value(result, 0));
    }

    /**
     * 业务作用：把执行器置为 DRAINING，停止进入新的能力快照并保留已有 attempt 的续期权。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void drain() {
        state = "DRAINING";
        try {
            heartbeat();
        } catch (IllegalStateException error) {
            if (error.getMessage() == null || !error.getMessage().contains("registration expired")) throw error;
        }
    }

    /**
     * 业务作用：重新声明执行器 ACTIVE，使通知门禁就绪后可以领取新任务。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void activate() {
        state = "ACTIVE";
        heartbeat();
    }

    /**
     * 业务作用：在优雅停机末尾原子移除本执行器及全部本地能力索引，避免已退出节点继续进入 Fanout 快照。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值；重复注销按幂等成功处理。
     */
    synchronized void unregister() {
        if (withdrawn) return;
        withdrawn = true;
        List<String> sorted = workers.stream().sorted().toList();
        String[] unregisterKeys = new String[3 + sorted.size() * 2];
        unregisterKeys[0] = keys.executors();
        unregisterKeys[1] = keys.executor(executorId);
        unregisterKeys[2] = keys.registryGc();
        for (int index = 0; index < sorted.size(); index++) {
            unregisterKeys[3 + index * 2] = keys.capability(sorted.get(index));
            unregisterKeys[4 + index * 2] = keys.capabilityMeta(sorted.get(index));
        }
        String code = value(scripts.list(RedisJobScript.EXECUTOR_UNREGISTER, unregisterKeys, executorId), 0);
        if (!"OK".equals(code) && !"NOT_FOUND".equals(code)) {
            throw new IllegalStateException("RedisJob executor unregister rejected: " + code);
        }
    }

    /**
     * 业务作用：记录本地开始执行的 Handler 数量，供心跳容量观测。 返回：无返回值。
     */
    void executionStarted() {
        inflight.incrementAndGet();
    }

    /**
     * 业务作用：记录本地结束执行的 Handler 数量，供心跳容量观测。 返回：无返回值。
     */
    void executionFinished() {
        inflight.updateAndGet(value -> Math.max(0, value - 1));
        synchronized (inflightMonitor) {
            inflightMonitor.notifyAll();
        }
    }

    /**
     * 业务作用：优雅停机期间等待已经取得执行权的 Handler 退出，使其租约与完成提交保持可用到等待期限。
     *
     * @param timeoutMs 最大等待毫秒数
     * @return 全部 Handler 已退出返回 true，达到期限仍有执行返回 false。
     */
    boolean awaitIdle(long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (inflightMonitor) {
            while (inflight.get() > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) return false;
                try {
                    TimeUnit.NANOSECONDS.timedWait(inflightMonitor, remaining);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 业务作用：取得部署环境可跨进程复用的主机身份作为未显式配置时的节点标识。
     *
     * @return 主机名。
     */
    private static String hostIdentity() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new IllegalStateException("instanceIdentity must be configured when hostname is unavailable", e);
        }
    }

    /**
     * 业务作用：安全读取脚本复合返回字段。
     *
     * @param values 返回列表
     * @param index  下标
     * @return 字符串值。
     */
    private static String value(List<Object> values, int index) {
        return index >= values.size() || values.get(index) == null ? "" : Objects.toString(values.get(index));
    }
}
