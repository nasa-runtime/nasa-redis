package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.Partition;
import io.github.nasaruntime.core.utils.StringUtils;
import io.github.nasaruntime.redis.cache.redis.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 业务作用：以 prepare/activate 事务登记 Redis Stream 的 Partition 消费计划，避免校验失败留下半发布路由。
 */
final class StreamSubscriptionRegistry {

    /**
     * 业务作用：以不持有 RedisProxy 或 listener 的轻量身份记录 Runner 的永久代理归属。
     *
     * @param id        当前 RedisProxy 分区运行时在 JVM 内的唯一身份
     * @param qualifier 仅用于拒绝信息的代理名
     */
    private record RunnerOwner(long id, String qualifier) {
    }

    private static final AtomicLong RUNNER_OWNER_IDS = new AtomicLong();
    private static final ReentrantLock RUNNER_OWNERS_LOCK = new ReentrantLock();
    private static final IdentityHashMap<Partition.PartitionRunner, RunnerOwner> RUNNER_OWNERS =
            new IdentityHashMap<>();

    private final RedisProxy redisProxy;
    private final RunnerOwner runnerOwner;
    private final String defaultRunnerName;
    private final StreamPlanIds planIds = new StreamPlanIds();
    private final StreamTaskTypesByRunner taskTypesByRunner = new StreamTaskTypesByRunner();
    private final ReentrantLock publishLock = new ReentrantLock();
    private final Map<RouteRegistrationKey, StreamSubscriptionPlan> routes = new HashMap<>();
    private final Map<Long, StreamSubscriptionPlan> plans = new HashMap<>();
    private Partition.PartitionRunner partitionRunner;

    /**
     * 业务作用：创建仅服务一个 RedisProxy 的计划注册域，使相同 Redis key 在不同代理间互不污染。
     *
     * @param redisProxy 计划对应的 Redis 命令代理
     *                   返回: 尚无计划与路由的注册表。
     */
    StreamSubscriptionRegistry(RedisProxy redisProxy) {
        this.redisProxy = Objects.requireNonNull(redisProxy, "redisProxy");
        long ownerId = RUNNER_OWNER_IDS.incrementAndGet();
        this.runnerOwner = new RunnerOwner(ownerId, redisProxy.getQualifier());
        this.defaultRunnerName = "nasa-redis-partition-" + redisProxy.getQualifier() + '-' + ownerId;
    }

    /**
     * 业务作用：完成不会产生 Redis 消费副作用的纯校验，并冻结 listener、Runner、路由与 taskType 所有权。
     *
     * @param listener 待登记的 Stream listener
     * @return 尚未发布、只能被 activate 或 abandon 一次的不可变计划
     */
    @SuppressWarnings("unchecked")
    StreamSubscriptionPlan prepare(StreamSubscribe<?, ?> listener) {
        Objects.requireNonNull(listener, "listener");
        ConsumeMode mode = Objects.requireNonNull(listener.mode(), "consume mode");
        if (mode == ConsumeMode.PROXY) {
            throw new IllegalArgumentException("PROXY listener 不应进入 Partition 计划注册");
        }
        if (!redisProxy.getStream().getPartition().isEnabled()) {
            throw new IllegalStateException("Stream PARTITION/BOTH 要求 stream.partition.enabled=true");
        }
        if (!(listener instanceof RedisEventSingleListener<?> single)) {
            throw new IllegalArgumentException("Stream PARTITION/BOTH 仅支持 RedisEventSingleListener: "
                    + listener.getClass().getName());
        }
        String[] topics = validatedTopics(listener);
        String event = listener.event();
        if (StringUtils.isBlank(event)) {
            throw new IllegalArgumentException("listener event() must not be blank: " + listener.getClass().getName());
        }
        String group = null;
        if (mode == ConsumeMode.BOTH) {
            group = listener.group();
            if (StringUtils.isBlank(group)) {
                throw new IllegalArgumentException("BOTH listener group() must not be blank: "
                        + listener.getClass().getName());
            }
            validateBothGroups(topics, group);
        }
        validateNoRouteConflict(mode, topics, event, group);

        // 每个 RedisProxy 只允许一个 Partition 执行域；首个计划完成选择后，后续计划只能复用该对象。
        Partition.PartitionRunner runner = selectPartitionRunner(listener.partition());
        long planId = planIds.next();
        TaskTypeReservation taskTypes = taskTypesByRunner.reserve(runner, planId);
        try {
            return new StreamSubscriptionPlan(
                    planId,
                    (RedisEventSingleListener<Object>) single,
                    runner,
                    mode,
                    taskTypes,
                    topics,
                    event,
                    group,
                    listener.autoDelete());
        } catch (Throwable failure) {
            taskTypesByRunner.abandon(runner, taskTypes);
            throw failure;
        }
    }

    /**
     * 业务作用：在启动 Runner 和消耗 taskType 前尽早检查当前已发布路由；activate 仍在同一锁内做最终复验。
     *
     * @param mode   PARTITION 或 BOTH
     * @param topics topic 快照
     * @param event  事件名
     * @param group  BOTH group；PARTITION 为 null
     *               返回: 无返回值；已知冲突时立即拒绝准备计划。
     */
    private void validateNoRouteConflict(ConsumeMode mode,
                                         String[] topics,
                                         String event,
                                         String group) {
        publishLock.lock();
        try {
            for (String topic : topics) {
                RouteRegistrationKey partition = RouteRegistrationKey.partition(redisProxy, topic, event);
                if (routes.containsKey(partition)) {
                    throw new IllegalStateException("Stream Partition 路由重复: " + partition.describe());
                }
                if (mode == ConsumeMode.BOTH) {
                    RouteRegistrationKey proxy = RouteRegistrationKey.proxyBoth(redisProxy, topic, group, event);
                    if (routes.containsKey(proxy)) {
                        throw new IllegalStateException("Stream Partition 路由重复: " + proxy.describe());
                    }
                }
            }
        } finally {
            publishLock.unlock();
        }
    }

    /**
     * 业务作用：完成全部可失败的本地就绪动作，并持有发布锁返回一次性激活票据。
     * Runner 健康、路由唯一性和 taskType 预留在远端合同提交前形成稳定证据。
     *
     * @param plan prepare 阶段创建的计划
     * @return 持有发布锁且必须提交或关闭的激活票据
     */
    Activation prepareActivation(StreamSubscriptionPlan plan) {
        Objects.requireNonNull(plan, "plan");
        List<RouteRegistrationKey> keys = routeKeys(plan);
        boolean publishLocked = false;
        boolean ready = false;
        try {
            publishLock.lock();
            publishLocked = true;
            for (RouteRegistrationKey key : keys) {
                if (routes.containsKey(key)) {
                    throw new IllegalStateException("Stream Partition 路由重复: " + key.describe());
                }
            }
            if (plans.containsKey(plan.planId())) {
                throw new IllegalStateException("Stream planId 已发布: " + plan.planId());
            }
            // 本地路由结论在锁内稳定后才启动代理独占执行域；单项登记失败不能停止已承载其它计划的同代理 Runner。
            plan.ensureRuntimeStarted();
            taskTypesByRunner.beginActivation(plan.runner(), plan.taskTypes());
            Activation activation = new Activation(plan, keys);
            ready = true;
            return activation;
        } finally {
            if (!ready) {
                if (publishLocked) publishLock.unlock();
            }
        }
    }

    /**
     * 业务作用：为不需要远端合同的内部调用一次性完成本地就绪与发布，保持原激活入口语义。
     *
     * @param plan prepare 阶段创建的计划
     *             返回: 无返回值；本地就绪失败时不发布计划并废弃 taskType 预留。
     */
    void activate(StreamSubscriptionPlan plan) {
        Activation activation = null;
        try {
            activation = prepareActivation(plan);
            try (Activation current = activation) {
                current.commit();
            }
        } catch (Throwable failure) {
            if (activation == null) taskTypesByRunner.abandon(plan.runner(), plan.taskTypes());
            throw failure;
        }
    }

    /**
     * 业务作用：封装一次 listener 本地发布事务；持有期间阻止其它计划改变路由与 taskType 就绪结论。
     */
    final class Activation implements AutoCloseable {

        private final StreamSubscriptionPlan plan;
        private final List<RouteRegistrationKey> keys;
        private boolean committed;
        private boolean closed;

        /**
         * 业务作用：创建已经完成本地就绪并持有 publishLock 的一次性票据。
         *
         * @param plan 待发布计划
         * @param keys 已复验唯一性的全部本地路由键
         *             返回: 尚未提交且持有发布锁的票据。
         */
        private Activation(StreamSubscriptionPlan plan, List<RouteRegistrationKey> keys) {
            this.plan = plan;
            this.keys = keys;
        }

        /**
         * 业务作用：在远端合同成功后只提交本地内存所有权，不再启动资源或执行外部 I/O。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；同一票据只能提交一次。
         */
        void commit() {
            if (closed || committed) throw new IllegalStateException("Stream activation ticket 已结束");
            // 就绪阶段已经冻结该预留；持有 publishLock 时没有其它路径能够改变本计划的发布结论。
            taskTypesByRunner.finishActivation(plan.runner(), plan.taskTypes());
            plans.put(plan.planId(), plan);
            for (RouteRegistrationKey key : keys) routes.put(key, plan);
            committed = true;
        }

        /**
         * 业务作用：结束激活票据；未提交时废弃预留，已提交时开放本地路由并登记可选观测。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；重复关闭保持幂等。
         */
        @Override
        public void close() {
            if (closed) return;
            try {
                if (!committed) taskTypesByRunner.abandon(plan.runner(), plan.taskTypes());
            } finally {
                closed = true;
                publishLock.unlock();
            }
            if (committed) {
                RedisPartition partition = RedisPartition.current(redisProxy);
                if (partition != null) redisProxy.streamPartitionMetrics().planAvailable(partition, plan);
            }
        }
    }

    /**
     * 业务作用：在宿主资源校验未通过时废弃尚未激活的计划，防止注册事务留下悬空所有权。
     *
     * @param plan prepare 阶段创建但尚未发布的计划
     *             返回: 无返回值；已提交计划不会被本路径撤销。
     */
    void abandon(StreamSubscriptionPlan plan) {
        taskTypesByRunner.abandon(plan.runner(), plan.taskTypes());
    }

    /**
     * 业务作用：为当前 RedisProxy 选择并冻结唯一 PartitionRunner，隔离代理之间的队列、类型状态和故障门禁。
     *
     * @param requested listener 显式选择的 Runner；null 表示使用当前代理的独立默认 Runner
     * @return 当前 RedisProxy 全部 Partition listener 共用的唯一 Runner
     */
    private Partition.PartitionRunner selectPartitionRunner(Partition.PartitionRunner requested) {
        publishLock.lock();
        try {
            if (partitionRunner == null) {
                Partition.PartitionRunner selected = requested == null
                        ? Partition.of(defaultRunnerName)
                        : requested;
                // Runner 必须先取得当前代理的永久归属，才能成为本地计划或触发任何外部副作用。
                requireRunnerOwnership(selected);
                partitionRunner = selected;
                return selected;
            }
            if (requested != null && requested != partitionRunner) {
                throw new IllegalStateException("RedisProxy [" + redisProxy.getQualifier()
                        + "] 已绑定 PartitionRunner [" + partitionRunner.getRunnerName()
                        + "]，不能切换到 [" + requested.getRunnerName() + "]");
            }
            return partitionRunner;
        } finally {
            publishLock.unlock();
        }
    }

    /**
     * 业务作用：把显式或默认 Runner 永久绑定到当前 RedisProxy 运行时，阻止不同代理共享类型状态与故障门禁。
     *
     * @param runner 当前 listener 选择的执行域
     *               返回: 无返回值；Runner 已归属其它 RedisProxy 运行时时在任何启动和 Redis 副作用前拒绝。
     */
    private void requireRunnerOwnership(Partition.PartitionRunner runner) {
        RUNNER_OWNERS_LOCK.lock();
        try {
            RunnerOwner current = RUNNER_OWNERS.get(runner);
            if (current == null) {
                // 归属不随代理销毁释放，避免仍存活的 Runner 被新代理以重置后的 taskType 水位重新认领。
                RUNNER_OWNERS.put(runner, runnerOwner);
                return;
            }
            if (current.id() != runnerOwner.id()) {
                throw new IllegalStateException("PartitionRunner [" + runner.getRunnerName()
                        + "] 已绑定 RedisProxy [" + current.qualifier()
                        + "]，不能由 RedisProxy [" + runnerOwner.qualifier() + "] 共享");
            }
        } finally {
            RUNNER_OWNERS_LOCK.unlock();
        }
    }

    /**
     * 业务作用：在 RedisPartition 已排干全部来源和本地任务后，按依赖顺序停止当前代理独占的执行域。
     *
     * <p>参数说明: 无。
     *
     * @return 没有创建 Runner 或 Runner 与 TimingWheel 都进入停止态时返回 true；Runner 无法完整排空时返回 false
     */
    boolean stopOwnedRunner() {
        Partition.PartitionRunner runner;
        publishLock.lock();
        try {
            runner = partitionRunner;
        } finally {
            publishLock.unlock();
        }
        if (runner == null) return true;
        // Runner 先封闭提交并排空 worker；只有其完整停止后才能关闭负责迁移观察与延迟任务的同名 TimingWheel。
        if (!runner.stop()) return false;
        runner.getTimingWheel().stop();
        return !runner.isStarted() && !runner.getTimingWheel().isStarted();
    }

    /**
     * 业务作用：按 RedisPartition 单 field 的真实 topic/event 路由取得已激活计划。
     *
     * @param topic 消息中的业务 topic
     * @param event 消息中的事件名
     * @return 已激活计划；没有精确路由时返回 null
     */
    StreamSubscriptionPlan partitionPlan(String topic, String event) {
        publishLock.lock();
        try {
            return routes.get(RouteRegistrationKey.partition(redisProxy, topic, event));
        } finally {
            publishLock.unlock();
        }
    }

    /**
     * 业务作用：按 BOTH dedicated consumer 的 stream/group/field 取得与物理分区共享的订阅计划。
     *
     * @param stream 普通 Stream key
     * @param group  dedicated consumer group
     * @param event  record hash field
     * @return 已激活的 BOTH 计划；没有精确路由时返回 null
     */
    StreamSubscriptionPlan proxyPlan(String stream, String group, String event) {
        publishLock.lock();
        try {
            return routes.get(RouteRegistrationKey.proxyBoth(redisProxy, stream, group, event));
        } finally {
            publishLock.unlock();
        }
    }

    /**
     * 业务作用：返回已激活计划的不可变快照，供所有 listener 发布完成后一次性建立 BOTH container。
     *
     * <p>参数说明: 无。
     *
     * @return 按 planId 发布顺序排列的计划列表
     */
    List<StreamSubscriptionPlan> plans() {
        publishLock.lock();
        try {
            return plans.values().stream()
                    .sorted(java.util.Comparator.comparingLong(StreamSubscriptionPlan::planId))
                    .toList();
        } finally {
            publishLock.unlock();
        }
    }

    /**
     * 业务作用：复验计划依赖仍处于完整健康代次，供 Redis 来源 readiness 关闭新读取。
     *
     * @param planId 计划标识
     * @return 计划存在且 Runner 已启动并健康时返回 true
     */
    boolean isRunnerHealthy(long planId) {
        publishLock.lock();
        try {
            StreamSubscriptionPlan plan = plans.get(planId);
            return plan != null && plan.runner().isStarted() && plan.runner().isHealthy();
        } finally {
            publishLock.unlock();
        }
    }

    /**
     * 业务作用：校验并复制 listener topic 声明，重复 topic 在注册期直接拒绝以消除同计划自冲突。
     *
     * @param listener 提供 topic 声明的 listener
     * @return 保持声明顺序且不含空白、重复项的快照
     */
    private static String[] validatedTopics(StreamSubscribe<?, ?> listener) {
        String[] declared = listener.topics();
        if (declared == null || declared.length == 0) {
            throw new IllegalArgumentException("listener topics() must not be empty: " + listener.getClass().getName());
        }
        Set<String> distinct = new LinkedHashSet<>();
        for (String topic : declared) {
            if (StringUtils.isBlank(topic)) {
                throw new IllegalArgumentException("listener topic must not be blank: " + listener.getClass().getName());
            }
            if (!distinct.add(topic)) {
                throw new IllegalArgumentException("listener topics() contains duplicate topic: " + topic);
            }
        }
        return distinct.toArray(String[]::new);
    }

    /**
     * 业务作用：校验 BOTH 始终由一个不开旧并发包装的本地 consumer 处理，手工 ACK 由 dedicated 路径内部强制。
     *
     * @param topics 计划 topic 快照
     * @param group  listener 声明的 group
     *               返回: 无返回值；旧多 consumer 或 event executor 配置会阻止 container 开放。
     */
    private void validateBothGroups(String[] topics, String group) {
        for (String topic : topics) {
            Map<String, NasaLettuceConfig.Group> groups = redisProxy.getStream().getGroup().get(topic);
            NasaLettuceConfig.Group config = groups == null
                    ? NasaLettuceConfig.Group.DEFAULT
                    : groups.getOrDefault(group, NasaLettuceConfig.Group.DEFAULT);
            if (config.getConsumers() != 1) {
                throw new IllegalArgumentException("BOTH requires consumers=1: " + topic + "/" + group);
            }
            if (config.isEventExecutorEnable()) {
                throw new IllegalArgumentException("BOTH requires eventExecutorEnable=false: " + topic + "/" + group);
            }
        }
    }

    /**
     * 业务作用：把计划展开为 RedisPartition 的真实 topic/event 消费入口键。
     *
     * @param plan 已完成声明校验的计划
     * @return 该计划必须原子发布的全部路由键
     */
    private List<RouteRegistrationKey> routeKeys(StreamSubscriptionPlan plan) {
        List<RouteRegistrationKey> keys = new ArrayList<>();
        for (String topic : plan.topics()) {
            keys.add(RouteRegistrationKey.partition(redisProxy, topic, plan.event()));
            if (plan.mode() == ConsumeMode.BOTH) {
                keys.add(RouteRegistrationKey.proxyBoth(redisProxy, topic, plan.group(), plan.event()));
            }
        }
        return List.copyOf(keys);
    }
}

/**
 * 业务作用：按 RedisProxy 对象身份及真实消费入口值语义标识一条唯一 Stream 路由。
 */
final class RouteRegistrationKey {

    enum SourceKind {REDIS_PARTITION, PROXY_BOTH}

    private final RedisProxy redisProxy;
    private final SourceKind sourceKind;
    private final String stream;
    private final String event;
    private final String group;

    /**
     * 业务作用：创建一条不可变真实入口键。
     *
     * @param redisProxy Redis 命令代理对象
     * @param sourceKind 来源类型
     * @param stream     业务 Stream/topic
     * @param event      事件名
     * @param group      proxy dedicated group；物理分区来源为 null
     *                   返回: 用对象身份与值字段共同判等的路由键。
     */
    private RouteRegistrationKey(RedisProxy redisProxy,
                                 SourceKind sourceKind,
                                 String stream,
                                 String event,
                                 String group) {
        this.redisProxy = redisProxy;
        this.sourceKind = sourceKind;
        this.stream = stream;
        this.event = event;
        this.group = group;
    }

    /**
     * 业务作用：创建 RedisPartition 物理来源路由键。
     *
     * @param redisProxy Redis 命令代理对象
     * @param topic      业务 topic
     * @param event      事件名
     * @return 不包含普通消费组的分区路由键
     */
    static RouteRegistrationKey partition(RedisProxy redisProxy, String topic, String event) {
        return new RouteRegistrationKey(redisProxy, SourceKind.REDIS_PARTITION, topic, event, null);
    }

    /**
     * 业务作用：创建 BOTH 普通 Stream dedicated consumer 路由键。
     *
     * @param redisProxy Redis 命令代理对象
     * @param stream     普通 Stream key
     * @param group      consumer group
     * @param event      record hash field
     * @return 带 group 边界的普通 Stream 路由键
     */
    static RouteRegistrationKey proxyBoth(RedisProxy redisProxy, String stream, String group, String event) {
        return new RouteRegistrationKey(redisProxy, SourceKind.PROXY_BOTH, stream, event, group);
    }

    /**
     * 业务作用：生成不含 listener 数据的冲突描述，供启动失败诊断。
     *
     * <p>参数说明: 无。
     *
     * @return source/stream/event/group 组成的可读描述
     */
    String describe() {
        return sourceKind + ":" + stream + ":" + event + (group == null ? "" : ":" + group);
    }

    /**
     * 业务作用：按代理对象身份和入口值字段比较路由唯一性。参数说明: 待比较对象。返回: 表示同一真实入口时为 true。
     */
    @Override
    public boolean equals(Object value) {
        if (this == value) return true;
        if (!(value instanceof RouteRegistrationKey other)) return false;
        return redisProxy == other.redisProxy
                && sourceKind == other.sourceKind
                && Objects.equals(stream, other.stream)
                && Objects.equals(event, other.event)
                && Objects.equals(group, other.group);
    }

    /**
     * 业务作用：生成与对象身份和值字段判等一致的哈希。参数说明: 无。返回: 路由键哈希。
     */
    @Override
    public int hashCode() {
        int result = System.identityHashCode(redisProxy);
        result = 31 * result + sourceKind.hashCode();
        result = 31 * result + Objects.hashCode(stream);
        result = 31 * result + Objects.hashCode(event);
        return 31 * result + Objects.hashCode(group);
    }
}
