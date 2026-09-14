package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.Partition;
import io.github.nasaruntime.core.base.TimingWheel;
import io.github.nasaruntime.redis.cache.redis.ConsumeMode;
import io.github.nasaruntime.redis.cache.redis.RedisEventSingleListener;

import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 业务作用：冻结一个 Stream listener 在本地 Partition bridge 中的路由、Runner 与 taskType 合同。
 * 运行期不得重新调用 listener.partition()，也不得在 Runner 失健康后切换执行域。
 */
final class StreamSubscriptionPlan {

    private final long planId;
    private final RedisEventSingleListener<Object> listener;
    private final Partition.PartitionRunner runner;
    private final ConsumeMode mode;
    private final TaskTypeReservation taskTypes;
    private final String[] topics;
    private final String event;
    private final String group;
    private final boolean autoDelete;

    /**
     * 业务作用：创建已经通过注册期校验的不可变消费计划。
     *
     * @param planId     进程内单调计划标识
     * @param listener   单条业务 listener
     * @param runner     当前 RedisProxy 独占、可由其多个计划共享的 PartitionRunner
     * @param mode       消费来源模式
     * @param taskTypes  本计划持有的两个任务类型预留
     * @param topics     不可变主题快照
     * @param event      事件名
     * @param group      BOTH dedicated consumer group；PARTITION 为 null
     * @param autoDelete 权威确认后是否删除正文
     *                   返回: 构造完成的计划；全部字段在计划生命周期内保持不变。
     */
    StreamSubscriptionPlan(long planId,
                           RedisEventSingleListener<Object> listener,
                           Partition.PartitionRunner runner,
                           ConsumeMode mode,
                           TaskTypeReservation taskTypes,
                           String[] topics,
                           String event,
                           String group,
                           boolean autoDelete) {
        this.planId = planId;
        this.listener = Objects.requireNonNull(listener, "listener");
        this.runner = Objects.requireNonNull(runner, "runner");
        this.mode = Objects.requireNonNull(mode, "mode");
        this.taskTypes = Objects.requireNonNull(taskTypes, "taskTypes");
        this.topics = topics.clone();
        this.event = event;
        this.group = group;
        this.autoDelete = autoDelete;
    }

    /**
     * 业务作用：为不使用普通 Stream group 的 PARTITION 计划构造兼容快照。
     *
     * @param planId     计划标识
     * @param listener   单条 listener
     * @param runner     PartitionRunner
     * @param mode       消费模式
     * @param taskTypes  taskType 预留
     * @param topics     topic 快照
     * @param event      事件名
     * @param autoDelete 删除策略
     *                   返回: group 为 null 的计划。
     */
    StreamSubscriptionPlan(long planId,
                           RedisEventSingleListener<Object> listener,
                           Partition.PartitionRunner runner,
                           ConsumeMode mode,
                           TaskTypeReservation taskTypes,
                           String[] topics,
                           String event,
                           boolean autoDelete) {
        this(planId, listener, runner, mode, taskTypes, topics, event, null, autoDelete);
    }

    /**
     * 业务作用：返回进程内计划标识，用于 gate 与恢复状态隔离。参数说明: 无。返回: 正整数计划标识。
     */
    long planId() {
        return planId;
    }

    /**
     * 业务作用：返回单条业务 listener，Task 只通过该入口产生业务副作用。参数说明: 无。返回: 注册期冻结的 listener。
     */
    RedisEventSingleListener<Object> listener() {
        return listener;
    }

    /**
     * 业务作用：返回注册期冻结的 PartitionRunner，不允许运行期自动切换。参数说明: 无。返回: 计划 Runner。
     */
    Partition.PartitionRunner runner() {
        return runner;
    }

    /**
     * 业务作用：返回声明的来源模式，供注册与确认路径选择。参数说明: 无。返回: PARTITION 或 BOTH。
     */
    ConsumeMode mode() {
        return mode;
    }

    /**
     * 业务作用：返回 ordered Task 独占的 taskType。参数说明: 无。返回: 已提交的负数 taskType。
     */
    int orderedTaskType() {
        return taskTypes.pair().ordered();
    }

    /**
     * 业务作用：返回 unordered Task 独占的 taskType。参数说明: 无。返回: 已提交的负数 taskType。
     */
    int unorderedTaskType() {
        return taskTypes.pair().unordered();
    }

    /**
     * 业务作用：返回本计划持有的 taskType 预留，注册事务只能把它提交或废弃一次。
     *
     * <p>参数说明: 无。
     *
     * @return 与计划同生命周期的预留对象
     */
    TaskTypeReservation taskTypes() {
        return taskTypes;
    }

    /**
     * 业务作用：返回主题快照副本，避免调用方改变计划路由。参数说明: 无。返回: 新数组副本。
     */
    String[] topics() {
        return topics.clone();
    }

    /**
     * 业务作用：返回计划事件名。参数说明: 无。返回: 非空事件名。
     */
    String event() {
        return event;
    }

    /**
     * 业务作用：返回 BOTH dedicated consumer 的 group 快照。
     *
     * <p>参数说明: 无。
     *
     * @return BOTH 的非空 group；PARTITION 返回 null
     */
    String group() {
        return group;
    }

    /**
     * 业务作用：返回权威确认成功后是否删除 Stream 正文。参数说明: 无。返回: 计划冻结的删除策略。
     */
    boolean autoDelete() {
        return autoDelete;
    }

    /**
     * 业务作用：生成跨实例稳定且不含内部 planId 的订阅指标名，供低基数计划维度聚合。
     *
     * <p>参数说明: 无。
     *
     * @return 由排序后的 topic、event 与 group 组成的稳定名称
     */
    String metricSubscription() {
        String normalizedTopics = Arrays.stream(topics).sorted().reduce((left, right) -> left + "," + right)
                .orElse("<none>");
        String normalizedGroup = group == null || group.isBlank() ? "<partition>" : group;
        return normalizedTopics + "/" + event + "/" + normalizedGroup;
    }

    /**
     * 业务作用：按消息体计算并冻结本次本地 Partition 路由，Number 与 Object 使用各自真实入口的 hash。
     *
     * @param data 已完成反序列化的业务消息
     * @return 只在同步 submit 前短暂持有对象 key 的路由结果
     */
    ResolvedPartitionKey resolvePartitionKey(Object data) {
        return ResolvedPartitionKey.resolve(listener.partitionKey(data));
    }

    /**
     * 业务作用：按依赖顺序启动本计划的 TimingWheel 与 PartitionRunner，并复验完整健康状态后才允许读消息。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；依赖未形成同一完整代次时抛出 IllegalStateException。
     */
    void ensureRuntimeStarted() {
        TimingWheel.TimingWheelRunner timingWheel = runner.getTimingWheel();
        timingWheel.start();
        if (!timingWheel.isStarted()) {
            throw new IllegalStateException("Partition 对应的 TimingWheel 未启动: " + runner.getRunnerName());
        }
        runner.start();
        if (!runner.isStarted() || !runner.isHealthy()) {
            throw new IllegalStateException("PartitionRunner 未形成完整可用代次: " + runner.getRunnerName());
        }
    }
}

/**
 * 业务作用：为每个 PartitionRunner 分配互不冲突的 ordered/unordered taskType，并禁止数值回拨复用。
 */
final class StreamTaskTypeRegistry {

    private static final int FIRST = Integer.MIN_VALUE;
    private static final int LAST = -1_000_000_000;

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Long, TaskTypeReservation> reservations = new HashMap<>();
    private int next = FIRST;

    /**
     * 业务作用：为一个新计划预留连续的 ordered/unordered taskType，保证同一 Runner 的顺序策略互不污染。
     *
     * <p>参数说明: 无。
     *
     * @return 永不与本注册表历史分配重复的 taskType 对
     */
    TaskTypeReservation reserve(long planId) {
        lock.lock();
        try {
            if (reservations.containsKey(planId)) {
                throw new IllegalStateException("planId 已经预留 Partition taskType: " + planId);
            }
            if (next > LAST - 1) throw new IllegalStateException("Partition taskType 保留区间已耗尽");
            TaskTypeReservation reservation = new TaskTypeReservation(
                    planId, new TaskTypePair(next, next + 1));
            next += 2;
            reservations.put(planId, reservation);
            return reservation;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在路由发布锁内提交本计划的 taskType 所有权，使已激活计划不得被回滚路径撤销。
     *
     * @param reservation prepare 阶段创建的预留
     *                    返回: 无返回值；预留不属于本注册表或状态已变化时拒绝激活。
     */
    void commit(TaskTypeReservation reservation) {
        lock.lock();
        try {
            TaskTypeReservation current = reservations.get(reservation.planId());
            if (current != reservation || !reservation.commitPrepared()) {
                throw new IllegalStateException("Partition taskType reservation 已失效");
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在远端路由合同提交前冻结 taskType 预留，阻止就绪票据持有期间被其它回滚路径废弃。
     *
     * @param reservation prepare 阶段创建的预留
     *                    返回: 无返回值；预留不属于本注册表或不再处于 PREPARED 时拒绝进入激活事务。
     */
    void beginActivation(TaskTypeReservation reservation) {
        lock.lock();
        try {
            TaskTypeReservation current = reservations.get(reservation.planId());
            if (current != reservation || !reservation.markActivating()) {
                throw new IllegalStateException("Partition taskType reservation 无法进入激活事务");
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：把已经冻结的 taskType 预留提交为已发布计划的永久所有权。
     *
     * @param reservation 已进入激活事务的预留
     *                    返回: 无返回值；调用方持有计划发布票据时，状态只能从 ACTIVATING 单向进入 COMMITTED。
     */
    void finishActivation(TaskTypeReservation reservation) {
        lock.lock();
        try {
            TaskTypeReservation current = reservations.get(reservation.planId());
            if (current != reservation || !reservation.commitActivating()) {
                throw new IllegalStateException("Partition taskType activation 状态失效");
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：废弃未发布计划的 taskType 所有权但不回拨数值，隔离迟到任务形成的 ABA 风险。
     *
     * @param reservation 需要废弃的预留
     *                    返回: 无返回值；已提交或已废弃的预留保持现状。
     */
    void abandon(TaskTypeReservation reservation) {
        lock.lock();
        try {
            if (reservation.markAbandoned()) reservations.remove(reservation.planId(), reservation);
        } finally {
            lock.unlock();
        }
    }

    record TaskTypePair(int ordered, int unordered) {
    }
}

/**
 * 业务作用：按 Runner 对象身份隔离 taskType 注册表；相同名称但不同对象不能共享内部数值所有权。
 */
final class StreamTaskTypesByRunner {

    private final ReentrantLock lock = new ReentrantLock();
    private final IdentityHashMap<Partition.PartitionRunner, StreamTaskTypeRegistry> registries =
            new IdentityHashMap<>();

    /**
     * 业务作用：从指定 Runner 的保留区间取得一对 taskType。
     *
     * @param runner 计划实际使用的 Runner 对象
     * @param planId 订阅计划身份，用于限定该 Runner 内 taskType 预留的归属
     * @return 该 Runner 内唯一的 taskType 对
     */
    TaskTypeReservation reserve(Partition.PartitionRunner runner, long planId) {
        lock.lock();
        try {
            return registries.computeIfAbsent(runner, ignored -> new StreamTaskTypeRegistry()).reserve(planId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：提交指定 Runner 内的 taskType 预留，使路由发布和数值所有权形成同一注册结论。
     *
     * @param runner      计划实际使用的 Runner 对象
     * @param reservation 待提交的预留
     *                    返回: 无返回值；Runner 从未建立注册表时拒绝提交。
     */
    void commit(Partition.PartitionRunner runner, TaskTypeReservation reservation) {
        lock.lock();
        try {
            StreamTaskTypeRegistry registry = registries.get(runner);
            if (registry == null) throw new IllegalStateException("PartitionRunner 没有 taskType 注册表");
            registry.commit(reservation);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在远端合同提交前冻结指定 Runner 的 taskType 预留，形成不可并发回滚的本地就绪证据。
     *
     * @param runner      计划实际使用的 Runner 对象
     * @param reservation 待冻结的预留
     *                    返回: 无返回值；Runner 或预留状态不匹配时在远端副作用前拒绝。
     */
    void beginActivation(Partition.PartitionRunner runner, TaskTypeReservation reservation) {
        lock.lock();
        try {
            StreamTaskTypeRegistry registry = registries.get(runner);
            if (registry == null) throw new IllegalStateException("PartitionRunner 没有 taskType 注册表");
            registry.beginActivation(reservation);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：提交指定 Runner 已冻结的 taskType 预留，供本地路由最终发布使用。
     *
     * @param runner      计划实际使用的 Runner 对象
     * @param reservation 已冻结的预留
     *                    返回: 无返回值；持有计划发布票据时完成 ACTIVATING 到 COMMITTED 的单向迁移。
     */
    void finishActivation(Partition.PartitionRunner runner, TaskTypeReservation reservation) {
        lock.lock();
        try {
            StreamTaskTypeRegistry registry = registries.get(runner);
            if (registry == null) throw new IllegalStateException("PartitionRunner 没有 taskType 注册表");
            registry.finishActivation(reservation);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：废弃指定 Runner 内尚未发布的 taskType 预留，数值保持单调不复用。
     *
     * @param runner      计划实际使用的 Runner 对象
     * @param reservation 待废弃的预留
     *                    返回: 无返回值；Runner 未建立注册表时保持 no-op。
     */
    void abandon(Partition.PartitionRunner runner, TaskTypeReservation reservation) {
        lock.lock();
        try {
            StreamTaskTypeRegistry registry = registries.get(runner);
            if (registry != null) registry.abandon(reservation);
        } finally {
            lock.unlock();
        }
    }
}

/**
 * 业务作用：表达一对 taskType 从准备到提交或废弃的单向所有权状态。
 */
final class TaskTypeReservation {

    enum State {PREPARED, ACTIVATING, COMMITTED, ABANDONED}

    private final long planId;
    private final StreamTaskTypeRegistry.TaskTypePair pair;
    private final AtomicReference<State> state = new AtomicReference<>(State.PREPARED);

    /**
     * 业务作用：创建尚未对外发布的 taskType 预留。
     *
     * @param planId 所属计划标识
     * @param pair   ordered 与 unordered 数值
     *               返回: 状态为 PREPARED 的一次性预留。
     */
    TaskTypeReservation(long planId, StreamTaskTypeRegistry.TaskTypePair pair) {
        this.planId = planId;
        this.pair = pair;
    }

    /**
     * 业务作用：返回预留所属计划标识。参数说明: 无。返回: 计划标识。
     */
    long planId() {
        return planId;
    }

    /**
     * 业务作用：返回预留的 ordered/unordered 数值对。参数说明: 无。返回: 不可变数值对。
     */
    StreamTaskTypeRegistry.TaskTypePair pair() {
        return pair;
    }

    /**
     * 业务作用：直接提交未进入分布式登记的普通预留。参数说明: 无。返回: PREPARED 成功进入 COMMITTED 时为 true。
     */
    boolean commitPrepared() {
        return state.compareAndSet(State.PREPARED, State.COMMITTED);
    }

    /**
     * 业务作用：冻结准备完成的预留。参数说明: 无。返回: PREPARED 成功进入 ACTIVATING 时为 true。
     */
    boolean markActivating() {
        return state.compareAndSet(State.PREPARED, State.ACTIVATING);
    }

    /**
     * 业务作用：提交远端合同已经接受的预留。参数说明: 无。返回: ACTIVATING 成功进入 COMMITTED 时为 true。
     */
    boolean commitActivating() {
        return state.compareAndSet(State.ACTIVATING, State.COMMITTED);
    }

    /**
     * 业务作用：废弃尚未发布的预留。参数说明: 无。返回: PREPARED 或 ACTIVATING 成功进入 ABANDONED 时为 true。
     */
    boolean markAbandoned() {
        while (true) {
            State current = state.get();
            if (current == State.COMMITTED || current == State.ABANDONED) return false;
            if (state.compareAndSet(current, State.ABANDONED)) return true;
        }
    }
}

/**
 * 业务作用：冻结 partitionKey 的真实提交入口与有效 hash；对象 key 只保留到同步 submit 返回。
 */
final class ResolvedPartitionKey {

    enum Kind {UNORDERED, LONG, STRING, OBJECT}

    private final Kind kind;
    private final int effectiveHash;
    private final long longKey;
    private Object objectKey;
    private boolean objectReleased;

    /**
     * 业务作用：创建一次已经解析的路由结果。
     *
     * @param kind          Partition 提交入口种类
     * @param effectiveHash 该入口实际读取的原始 hash
     * @param longKey       long 入口的归一化值
     * @param objectKey     Object 入口在同步 submit 前使用的原始对象
     *                      返回: 构造后的路由结果。
     */
    private ResolvedPartitionKey(Kind kind, int effectiveHash, long longKey, Object objectKey) {
        this.kind = kind;
        this.effectiveHash = effectiveHash;
        this.longKey = longKey;
        this.objectKey = objectKey;
    }

    /**
     * 业务作用：按公开合同把 null、Number 与其它对象分流到 Partition 的真实入口。
     *
     * @param rawKey listener 返回的原始业务 key
     * @return 冻结入口与有效 hash 的路由结果
     */
    static ResolvedPartitionKey resolve(Object rawKey) {
        if (rawKey == null) return new ResolvedPartitionKey(Kind.UNORDERED, 0, 0L, null);
        if (rawKey instanceof Number number) {
            long value = number.longValue();
            return new ResolvedPartitionKey(Kind.LONG, Long.hashCode(value), value, null);
        }
        if (rawKey instanceof CharSequence) {
            return new ResolvedPartitionKey(Kind.STRING, rawKey.hashCode(), 0L, rawKey);
        }
        return new ResolvedPartitionKey(Kind.OBJECT, rawKey.hashCode(), 0L, rawKey);
    }

    /**
     * 业务作用：声明当前消息是否要求同有效 hash 严格顺序。参数说明: 无。返回: 非 null key 时为 true。
     */
    boolean ordered() {
        return kind != Kind.UNORDERED;
    }

    /**
     * 业务作用：返回 Partition 提交入口种类。参数说明: 无。返回: UNORDERED、LONG、STRING 或 OBJECT。
     */
    Kind kind() {
        return kind;
    }

    /**
     * 业务作用：返回对应真实入口的有效 hash，供分桶与 ordered gate 共用。参数说明: 无。返回: 原始入口 hash。
     */
    int effectiveHash() {
        return effectiveHash;
    }

    /**
     * 业务作用：返回 Number.longValue() 归一化后的 primitive long 路由值。参数说明: 无。返回: long 路由值。
     */
    long longKey() {
        return longKey;
    }

    /**
     * 业务作用：在同步 submit 前取出原始 Object key，并拒绝已经释放的旧引用再次参与路由。
     *
     * <p>参数说明: 无。
     *
     * @return 仍由当前路由结果持有的原始对象 key
     */
    Object objectKeyForSubmit() {
        if ((kind != Kind.OBJECT && kind != Kind.STRING) || objectReleased || objectKey == null) {
            throw new IllegalStateException("对象路由 key 已释放或入口类型不匹配");
        }
        return objectKey;
    }

    /**
     * 业务作用：复验非 Number 对象在等待提交期间 hash 未改变，避免 gate 与 Partition 落入不同执行槽。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；hash 已改变时在业务副作用发生前拒绝提交。
     */
    void verifyStableHashBeforeSubmit() {
        if ((kind == Kind.OBJECT || kind == Kind.STRING)
                && objectKeyForSubmit().hashCode() != effectiveHash) {
            throw new IllegalStateException("对象路由 key 在等待提交期间改变了 hash");
        }
    }

    /**
     * 业务作用：释放原始 Object key，使 Task、gate、重试和确认状态不长期持有业务对象。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；重复释放保持幂等。
     */
    void releaseObjectKeyAfterSubmit() {
        objectReleased = true;
        objectKey = null;
    }
}

/**
 * 业务作用：为 RedisProxy 内所有 Stream 计划生成不回退的进程内标识。
 */
final class StreamPlanIds {

    private final AtomicLong sequence = new AtomicLong();

    /**
     * 业务作用：取得下一计划标识，零值保留给未登记状态。
     *
     * <p>参数说明: 无。
     *
     * @return 正整数计划标识
     */
    long next() {
        long value = sequence.incrementAndGet();
        if (value <= 0) throw new IllegalStateException("Stream planId 已耗尽");
        return value;
    }
}
