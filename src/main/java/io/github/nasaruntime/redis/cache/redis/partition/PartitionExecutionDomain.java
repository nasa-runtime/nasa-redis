package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.Partition;
import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** 业务作用：独占物理来源集合的本地执行、数量份额和恢复确认驱动。 */
final class PartitionExecutionDomain {
    final PartitionExecutionPlan.DomainSpec spec;
    final Partition.PartitionRunner runner;
    final PartitionRecordCapacity records;
    final PartitionDispatchCapacity dispatch;
    final StreamRetryCoordinator retries;
    final StreamCommitCoordinator commits;
    private final StreamRuntimeStatus status;
    private final StreamPartitionRuntime runtime;
    private final RedisPartitionProperties.ExecutorScope scope;
    private volatile boolean stopped;
    private final AtomicBoolean degraded = new AtomicBoolean();

    /**
     * 业务作用：创建尚未开放消费的独立资源域，后继责任不与其它域竞争许可。
     * @param spec 冻结身份与份额
     * @param runner 当前代理独占的执行器
     * @param runtime 全源顺序和生命周期协调器
     * @param proxy Redis 命令代理
     * @param orderedKeys 全源共享业务键门禁
     * @param status 全源排干责任计数
     * 返回: 初始全部份额空闲的执行域。
     */
    PartitionExecutionDomain(PartitionExecutionPlan.DomainSpec spec, Partition.PartitionRunner runner,
                             StreamPartitionRuntime runtime, RedisProxy proxy,
                             OrderedKeyCoordinator orderedKeys, StreamRuntimeStatus status) {
        this.spec = spec;
        this.runner = runner;
        this.status = status;
        this.runtime = runtime;
        this.scope = proxy.getStream().getPartition().getExecutor().getScope();
        var quota = spec.quota();
        var config = proxy.getStream().getPartition().getLocalConsumer();
        dispatch = new PartitionDispatchCapacity(quota.tasks(), quota.attempts(), quota.commits());
        commits = new StreamCommitCoordinator(proxy.getQualifier(), orderedKeys, status, runtime.waitExecutor(),
                config.getAckReconcileInitialDelayMs(), config.getAckReconcileMaxDelayMs(), proxy::streamPartitionMetrics);
        retries = new StreamRetryCoordinator(runtime, status, quota.retries(), quota.unordered(), quota.routeBlocked(),
                config.getRetryInitialDelayMs(), config.getRetryMaxDelayMs());
        boolean composite = proxy.getStream().getPartition().getExecutor().getScope() != RedisPartitionProperties.ExecutorScope.SOURCE;
        records = new PartitionRecordCapacity(quota.records(), this::degrade, composite ? this::reserveRead : null);
    }

    /** 业务作用：定位完整拓扑中的固定域。参数说明: 无。返回: 不随持锁变化的编号。 */
    int id() { return spec.id(); }

    /**
     * 业务作用：在 Redis 读取前同时取得执行、确认与失败承接份额，任何不足都回滚已取得部分。
     * @param count 最坏物理记录数量
     * @return 完整预留；容量暂满时为 null
     */
    private PartitionReadReservation reserveRead(int count) {
        var execution = dispatch.tryReserveRead(count);
        if (execution == null) return null;
        var successor = retries.reserveRead(count);
        if (successor == null) { execution.close(); return null; }
        return new PartitionReadReservation(execution, successor, count);
    }

    /**
     * 业务作用：复验本域执行依赖并锁存失健康状态，禁止自动切换 Runner 或恢复已失效代次。
     * 参数说明: 无。
     * @return Runner 及时间轮完整且从未失健康时为 true
     */
    boolean healthy() {
        if (!runner.isStarted() || !runner.isHealthy()) degrade();
        return !degraded.get();
    }

    /**
     * 业务作用：关闭本域新读取并保留已有责任，其它域的准入保持独立。
     * 参数说明: 无。
     * 返回: 无返回值；重复故障信号保持幂等。
     */
    void degrade() {
        degraded.set(true);
        records.closeAdmission();
        dispatch.wakeAdmissionWaiters();
        status.failReadiness("execution_domain_" + id());
    }

    /**
     * 业务作用：启动时间轮和 Runner 后复验实际槽数，完整就绪前不得开放消费。
     * @param slots 预检证明的槽数
     * 返回: 无返回值；依赖或槽数不符时拒绝激活。
     */
    void start(int slots) {
        runner.getTimingWheel().start();
        runner.start();
        if (!runner.isHealthy() || runner.partitionCount() != slots) throw new IllegalStateException("execution domain startup contract mismatch: " + id());
    }

    /**
     * 业务作用：在所有来源已排干后按依赖顺序结束当前域，不关闭其它域。
     * 参数说明: 无。
     * @return Runner 和时间轮均停止时为 true，未排干时保留时间轮并返回 false
     */
    boolean stop() {
        if (!runner.stop()) return false;
        runner.getTimingWheel().stop();
        stopped = !runner.isStarted() && !runner.getTimingWheel().isStarted();
        return stopped;
    }

    /**
     * 业务作用：读取本域容量与确认恢复使用量，观测不访问 Redis。
     * 参数说明: 无。
     * @return 可用于源级求和的固定名称快照
     */
    Map<String, Long> usage() {
        var values = new LinkedHashMap<String, Long>();
        values.putAll(records.snapshot());
        values.putAll(dispatch.snapshot());
        values.putAll(retries.snapshot());
        values.put("pending_commit_attempts", commits.pendingAttempts());
        values.put("ack_unknown_records", commits.unknownRecords());
        values.put("active_tasks", runtime.activeTasks(this));
        return Map.copyOf(values);
    }
    /** 业务作用：导出本域固定份额和真实终态，不读取外部状态。参数说明: 无。返回: 不可变结构快照。 */
    PartitionExecutionDomainSnapshot snapshot() {
        var q = spec.quota();
        Map<String, Long> quota = Map.of("records", (long) q.records(), "tasks", (long) q.tasks(),
                "commit_attempts", (long) q.attempts(), "commit_records", (long) q.commits(),
                "retry_execution", (long) q.retries(), "unordered_retries", (long) q.unordered(),
                "route_blocked_records", (long) q.routeBlocked());
        return new PartitionExecutionDomainSnapshot(scope, id(), spec.group(), spec.partition(), quota, usage(),
                !degraded.get() && runner.isStarted() && runner.isHealthy(),
                runner.getTimingWheel().isStarted(), stopped);
    }

}
