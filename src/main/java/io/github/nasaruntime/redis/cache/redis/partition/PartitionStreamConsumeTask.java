package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.AnyHolder;
import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.base.Partition;
import io.github.nasaruntime.core.utils.MapUtils;
import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import io.github.nasaruntime.redis.cache.redis.RedisProxyHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务作用：把一个 Redis 消费单元交给 Partition 执行，并把真实执行或未执行终态发布到同一个 Future。
 */
final class PartitionStreamConsumeTask implements Partition.Task, ObjectPool.Recycler<PartitionStreamConsumeTask> {

    private static final int INITIAL_OWNER = Integer.MIN_VALUE;

    private final AtomicInteger owner = new AtomicInteger(INITIAL_OWNER);
    private final AtomicBoolean completed = new AtomicBoolean();
    private final StreamDispatchUnit unit;
    private final StreamSourceAuthority.Snapshot authority;
    private final OrderedKeyCoordinator orderedKeys;
    private final OrderedKeyCoordinator.GateToken gateToken;
    private final CompletableFuture<ConsumeTaskOutcome> future;
    private final StreamRuntimeStatus status;
    private final RedisProxy redisProxy;
    private final long submittedAtNanos = System.nanoTime();
    private final AtomicBoolean queueObserved = new AtomicBoolean();

    /**
     * 业务作用：创建一个由 Redis batch 持有数据、由 Partition 决定执行终态的任务包装。
     *
     * @param unit        同计划同路由的有序消息单元
     * @param authority   提交时冻结的来源权威代次
     * @param orderedKeys ordered key 门禁协调器
     * @param gateToken   ordered 单元取得的门禁令牌；unordered 为 null
     * @param future      dispatcher 等待真实执行结论的 Future
     *                    返回: owner 尚未进入 Partition 状态机的任务。
     */
    PartitionStreamConsumeTask(StreamDispatchUnit unit,
                               StreamSourceAuthority.Snapshot authority,
                               OrderedKeyCoordinator orderedKeys,
                               OrderedKeyCoordinator.GateToken gateToken,
                               CompletableFuture<ConsumeTaskOutcome> future) {
        this(unit, authority, orderedKeys, gateToken, future, null);
    }

    /**
     * 业务作用：创建一个带共享运行观测的 Partition 任务包装，使每次 listener 调用进入低基数延迟直方图。
     *
     * @param unit        同计划同路由的有序消息单元
     * @param authority   提交时冻结的来源权威代次
     * @param orderedKeys ordered key 门禁协调器
     * @param gateToken   ordered 单元取得的门禁令牌；unordered 为 null
     * @param future      dispatcher 等待真实执行结论的 Future
     * @param status      当前 RedisProxy 的运行观测与排干状态
     *                    返回: owner 尚未进入 Partition 状态机并已绑定运行观测的任务。
     */
    PartitionStreamConsumeTask(StreamDispatchUnit unit,
                               StreamSourceAuthority.Snapshot authority,
                               OrderedKeyCoordinator orderedKeys,
                               OrderedKeyCoordinator.GateToken gateToken,
                               CompletableFuture<ConsumeTaskOutcome> future,
                               StreamRuntimeStatus status) {
        this(unit, authority, orderedKeys, gateToken, future, status, null);
    }

    /**
     * 业务作用：创建带动态指标后端的 Partition 任务，后端晚绑定或缺席均不改变业务终态。
     *
     * @param unit        同计划同路由的有序消息单元
     * @param authority   提交时冻结的来源权威代次
     * @param orderedKeys ordered key 门禁协调器
     * @param gateToken   ordered 单元令牌；unordered 为 null
     * @param future      dispatcher 等待的唯一 Future
     * @param status      共享运行状态
     * @param redisProxy  用于读取当前可选指标桥接器的代理
     *                    返回: 已冻结排队起点的任务包装。
     */
    PartitionStreamConsumeTask(StreamDispatchUnit unit,
                               StreamSourceAuthority.Snapshot authority,
                               OrderedKeyCoordinator orderedKeys,
                               OrderedKeyCoordinator.GateToken gateToken,
                               CompletableFuture<ConsumeTaskOutcome> future,
                               StreamRuntimeStatus status,
                               RedisProxy redisProxy) {
        this.unit = Objects.requireNonNull(unit, "unit");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.orderedKeys = Objects.requireNonNull(orderedKeys, "orderedKeys");
        this.gateToken = gateToken;
        this.future = Objects.requireNonNull(future, "future");
        this.status = status;
        this.redisProxy = redisProxy;
    }

    /**
     * 业务作用：返回计划冻结的 ordered/unordered taskType。参数说明: 无。返回: 稳定任务类型。
     */
    @Override
    public int taskType() {
        return unit.route().ordered() ? unit.plan().orderedTaskType() : unit.plan().unorderedTaskType();
    }

    /**
     * 业务作用：声明非空 partitionKey 单元要求严格 FIFO。参数说明: 无。返回: ordered 路由为 true。
     */
    @Override
    public boolean strictOrder() {
        return unit.route().ordered();
    }

    /**
     * 业务作用：以原子可见语义读取 Partition 任务所有者。参数说明: 无。返回: 当前 owner。
     */
    @Override
    public int getOwner() {
        return owner.get();
    }

    /**
     * 业务作用：以原子可见语义发布 Partition 任务所有者。参数说明: 新 owner。返回: 无返回值。
     */
    @Override
    public void setOwner(int value) {
        owner.set(value);
    }

    /**
     * 业务作用：原子竞争 Partition 任务所有权迁移。参数说明: 期望 owner 与新 owner。返回: CAS 结果。
     */
    @Override
    public boolean compareAndSetOwner(int expectedOwner, int newOwner) {
        return owner.compareAndSet(expectedOwner, newOwner);
    }

    /**
     * 业务作用：在本地权威与 ordered gate 仍有效时逐条调用 listener，并先发布门禁结论再完成 Future。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；listener 异常在结果可见后继续交给 Partition 记录。
     */
    @Override
    public void exec() {
        if (queueObserved.compareAndSet(false, true)) {
            metrics().taskLatency(unit.plan(), unit.route().ordered(),
                    "queue", System.nanoTime() - submittedAtNanos);
        }
        ConsumeTaskOutcome outcome;
        Throwable rethrow = null;
        try {
            outcome = invokeRecordsInOrder();
            rethrow = outcome.cause();
        } catch (Throwable infrastructureFailure) {
            if (gateToken != null) {
                if (unit.proxyBoth()) orderedKeys.proxyRecordDecision(gateToken, unit.refs());
                else orderedKeys.blocked(gateToken, unit.refs());
            }
            outcome = ConsumeTaskOutcome.partitionFailed(unit.refs(), infrastructureFailure);
            rethrow = infrastructureFailure;
        }
        completeOnce(outcome);
        if (rethrow != null) sneakyThrow(rethrow);
    }

    /**
     * 业务作用：在 Partition 确定任务不会执行时按真实 owner 映射结果，并先关闭 ordered key 再唤醒等待者。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；Future 与正常执行路径至多完成一次。
     */
    @Override
    public void cancelledRecycle() {
        ConsumeTaskOutcome outcome = ConsumeTaskOutcome.notExecuted(owner.get(), unit.refs());
        if (gateToken != null) {
            if (authority.allowsExecution()) orderedKeys.blocked(gateToken, unit.refs());
            else orderedKeys.invalidate(gateToken);
        }
        completeOnce(outcome);
    }

    /**
     * 业务作用：在 submit 明确尚未受理时完成拒绝结论，使 batch 保留 PEL 且不会无限等待 Future。
     *
     * @param cause 同步提交失败原因
     *              返回: 无返回值；gate 先进入阻断态，Future 至多完成一次。
     */
    void definitelyNotSubmitted(Throwable cause) {
        ConsumeTaskOutcome outcome = ConsumeTaskOutcome.rejected(unit.refs(), cause);
        if (gateToken != null) orderedKeys.blocked(gateToken, unit.refs());
        completeOnce(outcome);
    }

    /**
     * 业务作用：报告任务是否仍停留在 nasa-redis 本地初始 owner，用于证明同步 submit 尚未受理。
     *
     * <p>参数说明: 无。
     *
     * @return owner 仍为本地初始哨兵时返回 true
     */
    boolean hasNeverBeenOwned() {
        return owner.get() == INITIAL_OWNER;
    }

    /**
     * 业务作用：当前任务不进入对象池，保留空复位实现以满足 Partition 未执行回调合同。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；任务数据仍由原始 batch 生命周期统一释放。
     */
    @Override
    public void restore() {
        // Task 数据由 BatchStreamPollTask 持有到 Future 与 ACK 决策完成，本类型不单独归池。
    }

    /**
     * 业务作用：执行单元内的 listener 调用，并生成成功前缀、失败头和 deferred tail 的完整分段。
     *
     * <p>参数说明: 无。
     *
     * @return listener 的真实执行结论
     */
    private ConsumeTaskOutcome invokeRecordsInOrder() {
        List<PartitionRecordRef> successful = new ArrayList<>(unit.records().size());
        for (int index = 0; index < unit.records().size(); index++) {
            ConsumeTaskOutcome revoked = revokedBeforeListener(index, successful);
            if (revoked != null) return revoked;
            DecodedPartitionRecord record = unit.records().get(index);
            try {
                installContext(record);
                long listenerStartNanos = System.nanoTime();
                try {
                    unit.plan().listener().onEvent(record.data());
                } finally {
                    long elapsedNanos = System.nanoTime() - listenerStartNanos;
                    if (status != null) status.recordListenerLatency(elapsedNanos);
                    metrics().taskLatency(unit.plan(), unit.route().ordered(), "listener", elapsedNanos);
                }
                successful.add(record.ref());
            } catch (Throwable listenerFailure) {
                List<PartitionRecordRef> deferred = unit.refsFrom(index + 1);
                if (gateToken != null) {
                    List<PartitionRecordRef> blocked = new ArrayList<>();
                    blocked.add(record.ref());
                    blocked.addAll(deferred);
                    // BLOCKED 必须先于 Future 可见，后继批次才能可靠观察失败门禁。
                    if (unit.proxyBoth()) orderedKeys.proxyRecordDecision(gateToken, unit.refs());
                    else orderedKeys.listenerFailed(gateToken, successful, blocked);
                }
                return ConsumeTaskOutcome.listenerFailed(
                        List.copyOf(successful), record.ref(), deferred, listenerFailure);
            } finally {
                AnyHolder.clear();
            }
        }
        if (gateToken != null) {
            if (unit.proxyBoth()) orderedKeys.proxyRecordDecision(gateToken, unit.refs());
            else orderedKeys.awaitingAck(gateToken, unit.refs());
        }
        return ConsumeTaskOutcome.success(List.copyOf(successful));
    }

    /**
     * 业务作用：在每条 listener 产生业务副作用前复验来源代次和 ordered gate，失权后只保留已完成前缀并停止尾部。
     *
     * @param index      即将执行的记录下标
     * @param successful 已经完成 listener 的成功前缀
     * @return 仍有执行权时返回 null；失权时返回包含当前项与尾部的未执行结论
     */
    private ConsumeTaskOutcome revokedBeforeListener(int index, List<PartitionRecordRef> successful) {
        ConsumeStatus status;
        if (!authority.allowsExecution()) {
            status = ConsumeStatus.STALE_AUTHORITY;
            if (gateToken != null) orderedKeys.invalidate(gateToken);
        } else if (gateToken != null && !orderedKeys.ownsExecution(gateToken)) {
            status = ConsumeStatus.KEY_BLOCKED;
        } else {
            return null;
        }
        return ConsumeTaskOutcome.executionRevoked(
                status, List.copyOf(successful), unit.refsFrom(index));
    }

    /**
     * 业务作用：为单条 listener 调用安装 RedisProxy、recordId、passthrough 与 trace 上下文。
     *
     * @param record 当前待执行记录
     *               返回: 无返回值；调用方必须在 finally 中清理线程上下文。
     */
    private void installContext(DecodedPartitionRecord record) {
        RedisProxyHolder.setRedisProxy(record.redisProxy());
        RedisProxyHolder.setStreamRecordId(record.ref().id());
        Map<String, Object> passthrough = record.passthrough();
        if (passthrough == null) return;
        RedisProxyHolder.set(record.data(), passthrough);
        String traceId = MapUtils.getString(passthrough, AnyHolder.TRACE_ID);
        if (traceId != null) AnyHolder.set(AnyHolder.TRACE_ID, traceId);
    }

    /**
     * 业务作用：以 CAS 保证正常执行、取消和同步拒绝只能发布一个 Future 终态。
     *
     * @param outcome 待发布的不可变消费结论
     *                返回: 无返回值；已有终态时保持原结论。
     */
    private void completeOnce(ConsumeTaskOutcome outcome) {
        if (!completed.compareAndSet(false, true)) return;
        StreamPartitionMetrics metrics = metrics();
        metrics.taskOutcome(unit.plan(), unit.route().ordered(), outcome.status());
        metrics.taskLatency(unit.plan(), unit.route().ordered(),
                "future", System.nanoTime() - submittedAtNanos);
        future.complete(outcome);
    }

    /**
     * 业务作用：读取当前动态指标桥接器。参数说明: 无。返回: 后端缺席时返回 NOOP。
     */
    private StreamPartitionMetrics metrics() {
        return redisProxy == null ? StreamPartitionMetrics.NOOP : redisProxy.streamPartitionMetrics();
    }

    /**
     * 业务作用：在 Future 终态已经发布后原样抛出 listener 或基础设施异常，交由 Partition worker 观测。
     *
     * @param throwable 待保留原类型抛出的异常
     * @param <T>       编译期推断的异常类型
     *                  返回: 本方法不正常返回。
     */
    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
        throw (T) throwable;
    }
}

enum ConsumeStatus {
    SUCCESS,
    LISTENER_FAILED,
    STALE_AUTHORITY,
    KEY_BLOCKED,
    REJECTED,
    CANCELLED,
    PARTITION_FAILED
}

record ConsumeTaskOutcome(ConsumeStatus status,
                          Throwable cause,
                          List<PartitionRecordRef> successfulPrefix,
                          PartitionRecordRef failed,
                          List<PartitionRecordRef> deferredTail,
                          boolean listenerInvoked) {

    /**
     * 业务作用：创建 listener 全部成功的结论。参数说明: 成功坐标。返回: SUCCESS 结论。
     */
    static ConsumeTaskOutcome success(List<PartitionRecordRef> successful) {
        return new ConsumeTaskOutcome(ConsumeStatus.SUCCESS, null, successful, null, List.of(), true);
    }

    /**
     * 业务作用：创建 listener 失败且保留成功前缀的结论。参数说明: 三段坐标与原因。返回: LISTENER_FAILED 结论。
     */
    static ConsumeTaskOutcome listenerFailed(List<PartitionRecordRef> successful,
                                             PartitionRecordRef failed,
                                             List<PartitionRecordRef> deferred,
                                             Throwable cause) {
        return new ConsumeTaskOutcome(
                ConsumeStatus.LISTENER_FAILED, cause, successful, failed, deferred, true);
    }

    /**
     * 业务作用：创建未调用 listener 的权威或门禁结论。参数说明: 状态与全部坐标。返回: 未执行结论。
     */
    static ConsumeTaskOutcome notInvoked(ConsumeStatus status, List<PartitionRecordRef> records) {
        PartitionRecordRef failed = records.isEmpty() ? null : records.get(0);
        List<PartitionRecordRef> tail = records.size() < 2 ? List.of() : List.copyOf(records.subList(1, records.size()));
        return new ConsumeTaskOutcome(status, null, List.of(), failed, tail, false);
    }

    /**
     * 业务作用：表达 Task 在成功前缀之后失去来源或门禁权威，当前项与尾部必须留给有效代次重放。
     *
     * @param status      STALE_AUTHORITY 或 KEY_BLOCKED
     * @param successful  已完成 listener 的成功前缀
     * @param notExecuted 从当前项开始的未执行坐标
     * @return 保留成功事实且不把尾部误报为 SUCCESS 的结论
     */
    static ConsumeTaskOutcome executionRevoked(ConsumeStatus status,
                                               List<PartitionRecordRef> successful,
                                               List<PartitionRecordRef> notExecuted) {
        if (status != ConsumeStatus.STALE_AUTHORITY && status != ConsumeStatus.KEY_BLOCKED) {
            throw new IllegalArgumentException("execution revoked status is invalid: " + status);
        }
        PartitionRecordRef failed = notExecuted.isEmpty() ? null : notExecuted.getFirst();
        List<PartitionRecordRef> tail = notExecuted.size() < 2
                ? List.of() : List.copyOf(notExecuted.subList(1, notExecuted.size()));
        return new ConsumeTaskOutcome(
                status, null, List.copyOf(successful), failed, tail, !successful.isEmpty());
    }

    /**
     * 业务作用：按 Partition owner 映射确定未执行状态。参数说明: owner 与全部坐标。返回: 未执行结论。
     */
    static ConsumeTaskOutcome notExecuted(int owner, List<PartitionRecordRef> records) {
        ConsumeStatus status = owner == Partition.OWNER_CANCELLED
                ? ConsumeStatus.CANCELLED
                : owner == Partition.OWNER_REJECTED
                  ? ConsumeStatus.REJECTED
                  : ConsumeStatus.PARTITION_FAILED;
        return notInvoked(status, records);
    }

    /**
     * 业务作用：创建同步提交明确未受理的结论。参数说明: 全部坐标与原因。返回: REJECTED 结论。
     */
    static ConsumeTaskOutcome rejected(List<PartitionRecordRef> records, Throwable cause) {
        ConsumeTaskOutcome base = notInvoked(ConsumeStatus.REJECTED, records);
        return new ConsumeTaskOutcome(base.status(), cause, base.successfulPrefix(), base.failed(), base.deferredTail(), false);
    }

    /**
     * 业务作用：创建执行基础设施异常结论。参数说明: 全部坐标与原因。返回: PARTITION_FAILED 结论。
     */
    static ConsumeTaskOutcome partitionFailed(List<PartitionRecordRef> records, Throwable cause) {
        ConsumeTaskOutcome base = notInvoked(ConsumeStatus.PARTITION_FAILED, records);
        return new ConsumeTaskOutcome(base.status(), cause, base.successfulPrefix(), base.failed(), base.deferredTail(), false);
    }
}

record PartitionRecordRef(String stream,
                          String group,
                          String consumer,
                          String id,
                          String topic,
                          String event,
                          String field,
                          StreamRecordSource source,
                          StreamSourceAuthority authority,
                          long sourceGeneration) {

    /**
     * 业务作用：为 RedisPartition 单 field 记录构造兼容坐标。
     *
     * @param stream 物理 Stream key
     * @param id     record id
     * @param topic  业务 topic
     * @param event  事件名
     *               返回: 来源标记为 REDIS_PARTITION 的坐标。
     */
    PartitionRecordRef(String stream, String id, String topic, String event) {
        this(stream, null, null, id, topic, event, event, StreamRecordSource.REDIS_PARTITION, null, 0L);
    }

    /**
     * 业务作用：为不参与跨来源 gate 的调用方保留旧坐标构造方式。
     *
     * @param stream   Stream key
     * @param group    consumer group
     * @param consumer consumer 名
     * @param id       record id
     * @param topic    业务 topic
     * @param event    计划事件
     * @param field    hash field
     * @param source   来源种类
     *                 返回: 不带来源代次引用的坐标。
     */
    PartitionRecordRef(String stream,
                       String group,
                       String consumer,
                       String id,
                       String topic,
                       String event,
                       String field,
                       StreamRecordSource source) {
        this(stream, group, consumer, id, topic, event, field, source, null, 0L);
    }

    /**
     * 业务作用：复制一条不持有消息体的精确 Redis 坐标。
     *
     * @param stream           Stream key
     * @param group            consumer group；物理分区可为 null
     * @param consumer         consumer epoch 名；物理分区可为 null
     * @param id               record id
     * @param topic            业务 topic
     * @param event            计划事件名
     * @param field            物理 hash field
     * @param source           消息来源
     * @param authority        来源共享权威
     * @param sourceGeneration 来源所有权代次
     *                         返回: 不可变坐标。
     */
    PartitionRecordRef {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(source, "source");
    }
}

enum StreamRecordSource {REDIS_PARTITION, PROXY_BOTH}

record DecodedPartitionRecord(RedisProxy redisProxy,
                              PartitionRecordRef ref,
                              Object data,
                              Map<String, Object> passthrough) {
}

record StreamDispatchUnit(StreamSubscriptionPlan plan,
                          ResolvedPartitionKey route,
                          List<DecodedPartitionRecord> records) {

    /**
     * 业务作用：返回执行单元全部 Redis 坐标。参数说明: 无。返回: 保持执行顺序的不可变坐标。
     */
    List<PartitionRecordRef> refs() {
        return records.stream().map(DecodedPartitionRecord::ref).toList();
    }

    /**
     * 业务作用：返回指定下标起的未执行尾部坐标。参数说明: 起始下标。返回: 不可变尾部坐标。
     */
    List<PartitionRecordRef> refsFrom(int fromIndex) {
        if (fromIndex >= records.size()) return List.of();
        return records.subList(fromIndex, records.size()).stream().map(DecodedPartitionRecord::ref).toList();
    }

    /**
     * 业务作用：判断本单元是否来自 BOTH 的普通 Stream dedicated consumer。
     *
     * <p>参数说明: 无。
     *
     * @return 首条坐标标记为 PROXY_BOTH 时返回 true
     */
    boolean proxyBoth() {
        return !records.isEmpty() && records.getFirst().ref().source() == StreamRecordSource.PROXY_BOTH;
    }
}
