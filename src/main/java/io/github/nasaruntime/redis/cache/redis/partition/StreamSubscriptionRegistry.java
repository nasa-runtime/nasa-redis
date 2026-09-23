package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.Partition;
import io.github.nasaruntime.core.utils.StringUtils;
import io.github.nasaruntime.redis.cache.redis.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** 业务作用：收集不可变业务声明，在完整拓扑就绪后统一绑定执行域。 */
final class StreamSubscriptionRegistry {
    private static final AtomicLong OWNER_IDS = new AtomicLong();
    private static final IdentityHashMap<Partition.PartitionRunner, Owner> OWNERS = new IdentityHashMap<>();
    private final long ownerId = OWNER_IDS.incrementAndGet();
    private final RedisProxy proxy;
    private final StreamPlanIds planIds = new StreamPlanIds();
    private final StreamTaskTypesByRunner taskTypes = new StreamTaskTypesByRunner();
    private final Map<Route, StreamSubscriptionPlan> routes = new LinkedHashMap<>();
    private final Map<Long, StreamSubscriptionPlan> plans = new LinkedHashMap<>();
    private Partition.PartitionRunner sourceRunner;
    private volatile List<PartitionExecutionDomain> domains = List.of();
    private boolean frozen;
    private RedisPartitionProperties.ExecutorScope declarationScope;
    private final IdentityHashMap<StreamSubscribe<?, ?>, StreamSubscriptionPlan> declarations = new IdentityHashMap<>();
    private record Owner(long source, int domain) {}
    private record Route(String topic, String event) {}

    /** 业务作用：建立代理独占的声明集合。@param proxy 命令代理；返回: 未激活的注册表。 */
    StreamSubscriptionRegistry(RedisProxy proxy) { this.proxy = Objects.requireNonNull(proxy); }

    /**
     * 业务作用：冻结 listener 声明并在任何执行器启动之前拒绝非法模式、重复路由和显式 Runner 冲突。
     * @param listener 待注册处理器
     * @return 不持有运行资源的计划声明
     */
    synchronized StreamSubscriptionPlan prepare(StreamSubscribe<?, ?> listener) {
        if (frozen) throw new IllegalStateException("partition declarations already frozen");
        return prepare(listener, declarations.containsKey(listener) ? ConsumeMode.PARTITION : listener.mode());
    }

    /**
     * 业务作用：接续调用方已冻结的消费模式，首次声明校验与业务计划创建不得再次查询可变模式。
     * @param listener 原始订阅对象
     * @param declaredMode 调用方首次读取的消费模式
     * @return 同一对象已冻结的计划或新建计划；非分区模式及非法声明立即拒绝
     */
    @SuppressWarnings("unchecked")
    synchronized StreamSubscriptionPlan prepare(StreamSubscribe<?, ?> listener, ConsumeMode declaredMode) {
        if (frozen) throw new IllegalStateException("partition declarations already frozen");
        var scope = Objects.requireNonNull(proxy.getStream().getPartition().getExecutor().getScope(), "executor.scope");
        if (declarationScope != null && declarationScope != scope) throw new IllegalStateException("executor.scope changed after declarations");
        if (declaredMode != ConsumeMode.PARTITION) throw new IllegalArgumentException("partition declaration requires PARTITION mode");
        if (declarations.containsKey(listener)) return declarations.get(listener);
        if (!proxy.getStream().getPartition().isEnabled())
            throw new IllegalArgumentException("PARTITION requires stream.partition.enabled=true");
        if (!(listener instanceof RedisEventSingleListener<?> single))
            throw new IllegalArgumentException("PARTITION only supports RedisEventSingleListener");
        String[] declared = listener.topics();
        String event = listener.event();
        if (declared == null || declared.length == 0 || StringUtils.isBlank(event))
            throw new IllegalArgumentException("partition topics/event must not be empty");
        var topics = new LinkedHashSet<String>();
        for (String topic : declared) {
            if (StringUtils.isBlank(topic) || !topics.add(topic)) throw new IllegalArgumentException("invalid partition topic");
            if (declarations.values().stream().anyMatch(plan -> plan.event().equals(event)
                    && Arrays.asList(plan.topics()).contains(topic))) throw new IllegalStateException("duplicate partition declaration: " + topic);
            if (routes.containsKey(new Route(topic, event))) throw new IllegalStateException("duplicate partition route: " + topic);
        }
        Partition.PartitionRunner requested = listener.partition();
        Partition.PartitionRunner selected = sourceRunner;
        if (scope != RedisPartitionProperties.ExecutorScope.SOURCE) {
            if (requested != null) throw new IllegalArgumentException("explicit PartitionRunner requires executor.scope=source");
        } else {
            if (declarations.isEmpty()) selected = requested;
            else if (requested != null && requested != selected)
                throw new IllegalArgumentException("source scope requires one PartitionRunner per RedisProxy");
            // 声明只保存选择；默认 Runner 延迟创建，显式 Runner 此时不取得永久归属或停机权。
            if (selected != null) requireAvailableRunner(selected, 0);
        }
        var plan = new StreamSubscriptionPlan(planIds.next(), (RedisEventSingleListener<Object>) single,
                requested, topics.toArray(String[]::new), event, listener.autoDelete());
        sourceRunner = selected;
        declarationScope = scope;
        declarations.put(listener, plan);
        return plan;
    }

    /**
     * 业务作用：准备声明发布票据，完整拓扑激活前不启动 Runner。
     * @param plan 待发布声明
     * @return 必须提交或关闭的一次性票据
     */
    synchronized Activation prepareActivation(StreamSubscriptionPlan plan) {
        if (frozen) throw new IllegalStateException("partition declarations already frozen");
        requirePrepared(plan);
        return new Activation(plan);
    }

    /**
     * 业务作用：确认发布票据仍指向当前声明，已撤销或被替换的计划不能重新取得路由和执行器。
     * @param plan 票据保存的原始声明
     * 返回: 无返回值；声明失效、已发布或 scope 改变时拒绝。
     */
    private void requirePrepared(StreamSubscriptionPlan plan) {
        // 票据身份必须与当前声明一致，不能把迟到提交解释为后继声明的授权。
        if (declarations.get(plan.listener()) != plan || plans.containsKey(plan.planId()))
            throw new IllegalStateException("partition declaration is not prepared");
        if (declarationScope != proxy.getStream().getPartition().getExecutor().getScope())
            throw new IllegalStateException("executor.scope changed after declarations");
    }

    /** 业务作用：原子发布一组精确路由声明。 */
    final class Activation implements AutoCloseable {
        private final StreamSubscriptionPlan plan;
        private boolean closed;
        /** 业务作用：保存未发布声明。@param plan 计划；返回: 一次性票据。 */
        Activation(StreamSubscriptionPlan plan) { this.plan = plan; }
        /**
         * 业务作用：复验声明和全部路由后取得 Runner 永久归属，统一登记整个计划。
         * 参数说明: 无。
         * 返回: 无返回值；失效声明、路由或归属冲突时不发布计划，成功后归属不再撤销。
         */
        void commit() {
            synchronized (StreamSubscriptionRegistry.this) {
                if (closed || frozen) throw new IllegalStateException("activation closed");
                requirePrepared(plan);
                for (String topic : plan.topics()) {
                    if (routes.containsKey(new Route(topic, plan.event()))) throw new IllegalStateException("duplicate partition route: " + topic);
                }
                if (declarationScope == RedisPartitionProperties.ExecutorScope.SOURCE) {
                    var selected = sourceRunner == null ? Partition.of("nasa-redis-partition-" + ownerId + "-0") : sourceRunner;
                    // 物理组与远端只读预检已通过；在进程级身份锁内复验并提交，防止并发代理共享同一 Runner。
                    own(selected, 0);
                    sourceRunner = selected;
                }
                plans.put(plan.planId(), plan);
                for (String topic : plan.topics()) routes.put(new Route(topic, plan.event()), plan);
                closed = true;
            }
        }
        /**
         * 业务作用：撤销未提交声明及其最后一份候选选择，保留其它声明和已发布计划的归属。
         * 参数说明: 无。
         * 返回: 无返回值；已提交或已关闭票据保持原状态。
         */
        @Override public void close() {
            synchronized (StreamSubscriptionRegistry.this) {
                if (closed) return;
                closed = true;
                abandon(plan);
            }
        }
    }

    /**
     * 业务作用：撤销未发布声明；最后一份声明撤销时允许重新选择 scope 和 SOURCE Runner。
     * @param plan 未发布计划
     * 返回: 无返回值；已发布计划及其永久归属不受影响。
     */
    synchronized void abandon(StreamSubscriptionPlan plan) {
        // 已发布声明可能已产生执行责任，不能因后续登记失败撤销它的 Runner 归属。
        if (plans.containsKey(plan.planId())) return;
        declarations.values().removeIf(value -> value == plan);
        if (declarations.isEmpty()) {
            sourceRunner = null;
            declarationScope = null;
        }
    }

    /**
     * 业务作用：在声明准备时只读检查 Runner 归属，尽早拒绝已确定的跨来源或跨域复用。
     * @param runner 候选执行器
     * @param domain 固定域编号
     * 返回: 无返回值；已有其它归属时拒绝，通过不代表取得所有权，提交时必须再次复验。
     */
    private void requireAvailableRunner(Partition.PartitionRunner runner, int domain) {
        synchronized (OWNERS) {
            Owner current = OWNERS.get(runner);
            if (current != null && !current.equals(new Owner(ownerId, domain)))
                throw new IllegalArgumentException("PartitionRunner already belongs to another source/domain");
        }
    }

    /**
     * 业务作用：按冻结拓扑准备全部执行域及任务类型，任何失败都不开放部分来源消费。
     * @param topology 已验证的固定域和数量份额
     * @param topicGroups 业务主题到逻辑组的完整映射
     * @param runtime 全源运行时
     * @param orderedKeys 全源顺序门禁
     * @param status 全源排干计数
     * 返回: 无返回值；启动失败时保留真实未结束的资源以供关闭继续收敛。
     */
    synchronized void start(PartitionExecutionPlan topology, Map<String, String> topicGroups,
                            StreamPartitionRuntime runtime, OrderedKeyCoordinator orderedKeys, StreamRuntimeStatus status) {
        if (frozen || plans.isEmpty()) throw new IllegalStateException("partition requires unfrozen nonempty declarations");
        if (declarationScope != topology.scope) throw new IllegalStateException("executor.scope changed after declarations");
        frozen = true;
        var bindings = new LinkedHashMap<StreamSubscriptionPlan, Map<Integer, StreamPlanExecutionBinding>>();
        try {
            var created = new ArrayList<PartitionExecutionDomain>();
            for (var spec : topology.domains) {
                var runner = topology.scope == RedisPartitionProperties.ExecutorScope.SOURCE ? sourceRunner
                        : Partition.of("nasa-redis-partition-" + ownerId + "-" + spec.id());
                own(runner, spec.id());
                created.add(new PartitionExecutionDomain(spec, runner, runtime, proxy, orderedKeys, status));
                domains = List.copyOf(created);
            }
            for (var plan : plans.values()) {
                var reachable = new LinkedHashMap<Integer, StreamPlanExecutionBinding>();
                bindings.put(plan, reachable);
                for (String topic : plan.topics()) {
                    String group = topicGroups.getOrDefault(topic, "");
                    var physical = topology.groups.get(group);
                    if (physical == null) throw new IllegalStateException("unprepared partition group: " + group);
                    for (int partition = 0; partition < physical.count(); partition++) {
                        int id = topology.domain(group, partition);
                        if (!reachable.containsKey(id)) {
                            var domain = domains.get(id);
                            reachable.put(id, new StreamPlanExecutionBinding(domain, taskTypes.reserve(domain.runner, plan.planId())));
                        }
                    }
                }
            }
            // 所有类型预留和资源都已成立才启动；消费来源仍封闭，远端合同由宿主随后最终复验。
            for (var domain : domains) domain.start(topology.slots);
            for (var entry : bindings.entrySet()) {
                for (var binding : entry.getValue().values()) taskTypes.commit(binding.domain().runner, binding.taskTypes());
                entry.getKey().bind(entry.getValue());
            }
        } catch (Throwable failure) {
            for (var values : bindings.values()) for (var binding : values.values())
                taskTypes.abandon(binding.domain().runner, binding.taskTypes());
            // 启动失败先关闭全源准入，禁止后续控制动作误把局部就绪当成激活成功。
            runtime.beginShutdown();
            stopOwnedRunner();
            throw failure;
        }
    }

    /**
     * 业务作用：永久记录 Runner 的来源和域归属，迟到任务不能被另一运行时重新解释。
     * @param runner 所选执行器
     * @param domain 固定域编号
     * 返回: 无返回值；跨来源或跨域复用立即拒绝。
     */
    private void own(Partition.PartitionRunner runner, int domain) {
        synchronized (OWNERS) {
            Owner expected = new Owner(ownerId, domain);
            Owner current = OWNERS.putIfAbsent(runner, expected);
            if (current != null && !current.equals(expected)) throw new IllegalArgumentException("PartitionRunner already belongs to another source/domain");
        }
    }

    /** 业务作用：读取显式已启动执行器的真实槽数。参数说明: 无。返回: 未启动时为零。 */
    synchronized int sourceSlots() { return sourceRunner != null && sourceRunner.isStarted() ? sourceRunner.partitionCount() : 0; }
    /** 业务作用：枚举包括零流量域在内的固定资源。参数说明: 无。返回: 不可变快照。 */
    List<PartitionExecutionDomain> domains() { return domains; }
    /** 业务作用：定位物理消息的精确业务计划。@param topic 主题 @param event 事件 @return 声明或 null */
    synchronized StreamSubscriptionPlan partitionPlan(String topic, String event) { return routes.get(new Route(topic, event)); }
    /** 业务作用：枚举全部业务声明供统一激活和观测。参数说明: 无。返回: 不可变快照。 */
    synchronized List<StreamSubscriptionPlan> plans() { return List.copyOf(plans.values()); }

    /**
     * 业务作用：对全部域推进真实停止；一个域未完成仍继续收口其余域。
     * 参数说明: 无。
     * @return 全部 Runner 及对应时间轮实际结束时为 true
     */
    boolean stopOwnedRunner() {
        boolean complete = true;
        Partition.PartitionRunner committedSource;
        synchronized (this) {
            committedSource = plans.isEmpty() ? null : sourceRunner;
        }
        // 未提交的显式 Runner 只有候选身份，可能已归另一代理所有，本代理无权关闭它。
        if (domains.isEmpty() && committedSource != null) {
            if (!committedSource.stop()) return false;
            committedSource.getTimingWheel().stop();
        }
        for (var domain : domains) {
            try { if (!domain.stop()) complete = false; }
            catch (RuntimeException failure) { complete = false; }
        }
        return complete;
    }
}
