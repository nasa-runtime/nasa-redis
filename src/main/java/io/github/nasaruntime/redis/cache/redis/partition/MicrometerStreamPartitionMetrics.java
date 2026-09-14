package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.Partition;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.noop.NoopMeter;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * 业务作用：把 Stream Partition 内部快照登记到应用已有的 MeterRegistry，Prometheus 可直接采集固定指标合同。
 */
final class MicrometerStreamPartitionMetrics implements StreamPartitionMetrics {

    /**
     * 共享 leaf 按稳定身份序号取锁；此元数据门禁只保护赋序，不跨越 registry 操作或应用回调。
     */
    private static final List<RegistryLockOrder> REGISTRY_LOCK_ORDERS = new ArrayList<>();
    private static long nextRegistryLockOrder;
    private static final List<WeakReference<MeterRegistry>> OWNERSHIP_LISTENERS = new ArrayList<>();
    private static final ThreadLocal<RegistrationAttempt> ACTIVE_REGISTRATION = new ThreadLocal<>();
    private static final ThreadLocal<CompositeRegistration> ACTIVE_COMPOSITE_REGISTRATION = new ThreadLocal<>();
    private static final ThreadLocal<OwnedRegistration> ACTIVE_COMPOSITE_REMOVAL = new ThreadLocal<>();
    private static final ThreadLocal<RegisteredMeter> ACTIVE_METER_REMOVAL = new ThreadLocal<>();
    private static final MeterRegistryAccess METER_REGISTRY_ACCESS = resolveMeterRegistryAccess();

    private final List<MeterRegistry> registries;
    private final Map<CompositeMeterRegistry, Set<MeterRegistry>> compositeMembers;
    private final Map<MeterRegistry, Set<MeterRegistry>> initialLeafRegistries;
    private final List<OwnedRegistration> ownedRegistrations = new ArrayList<>();
    private final Set<String> boundGroups = new LinkedHashSet<>();
    private final Set<Long> boundPlans = new LinkedHashSet<>();
    private final Map<MeterRegistry, Map<MetricKey, Meter>> eventMeters = new IdentityHashMap<>();
    private final Map<MeterRegistry, Set<MetricKey>> skippedEventMeters = new IdentityHashMap<>();
    private RedisPartition partition;
    private boolean runtimeBound;
    /**
     * 关闭开始前发布的不可逆终态；volatile 供高频事件入口快速拒绝，所有 meter 创建仍在当前实例 monitor 内复验。
     */
    private volatile boolean closed;

    /**
     * 业务作用：冻结当前应用内可保持门面可见且不会重复传播的指标注册表拓扑。
     *
     * @param registries Spring 上下文发现的 MeterRegistry 集合
     *                   返回: 尚未绑定 RedisPartition 的桥接器。
     */
    MicrometerStreamPartitionMetrics(Collection<?> registries) {
        RegistryTopology topology = normalizeRegistries(registries);
        this.registries = topology.registries();
        this.compositeMembers = topology.compositeMembers();
        this.initialLeafRegistries = topology.initialLeafRegistries();
    }

    /**
     * 业务作用：保留应用可查询的顶层 registry，并拒绝多个顶层门面重复触达同一实际后端的歧义拓扑。
     *
     * @param candidates 应用上下文发现或调用方直接提供的注册表
     *                   返回: 顶层 registry、Composite 成员与初始叶子快照；重叠传播路径会明确失败，初始叶子供脱图后撤销。
     */
    private static RegistryTopology normalizeRegistries(Collection<?> candidates) {
        Set<MeterRegistry> identities = Collections.newSetFromMap(new IdentityHashMap<>());
        List<MeterRegistry> resolved = new ArrayList<>();
        for (Object candidate : candidates) {
            if (candidate instanceof MeterRegistry registry && identities.add(registry)) {
                resolved.add(registry);
            }
        }

        Set<MeterRegistry> descendants = Collections.newSetFromMap(new IdentityHashMap<>());
        for (MeterRegistry registry : resolved) {
            if (registry instanceof CompositeMeterRegistry composite) {
                collectDescendantRegistries(
                        composite,
                        descendants,
                        Collections.newSetFromMap(new IdentityHashMap<>()));
            }
        }
        List<MeterRegistry> topLevel = resolved.stream()
                .filter(registry -> !descendants.contains(registry))
                .toList();

        Map<MeterRegistry, Set<MeterRegistry>> targetLeaves = new IdentityHashMap<>();
        for (MeterRegistry target : topLevel) {
            Set<MeterRegistry> leaves = leafRegistries(target);
            for (Map.Entry<MeterRegistry, Set<MeterRegistry>> existing : targetLeaves.entrySet()) {
                MeterRegistry shared = sharedRegistry(existing.getValue(), leaves);
                if (shared != null) {
                    throw new IllegalStateException(
                            "Overlapping MeterRegistry topology: "
                                    + registryIdentity(existing.getKey()) + " and "
                                    + registryIdentity(target) + " both reach "
                                    + registryIdentity(shared));
                }
            }
            targetLeaves.put(target, leaves);
        }

        Map<CompositeMeterRegistry, Set<MeterRegistry>> compositeMembers = new IdentityHashMap<>();
        for (MeterRegistry target : topLevel) {
            if (target instanceof CompositeMeterRegistry composite) {
                snapshotCompositeMembers(composite, compositeMembers);
            }
        }
        return new RegistryTopology(
                List.copyOf(topLevel),
                Collections.unmodifiableMap(compositeMembers),
                Collections.unmodifiableMap(targetLeaves));
    }

    /**
     * 业务作用：递归收集 Composite 的全部成员身份，用于排除已经由顶层门面覆盖的候选 registry。
     *
     * @param composite   当前遍历的 Composite
     * @param descendants 已确认由候选 Composite 覆盖的成员身份
     * @param visited     当前遍历已访问的 Composite 身份
     *                    返回: 无返回值；descendants 原地累积全部可达成员。
     */
    private static void collectDescendantRegistries(CompositeMeterRegistry composite,
                                                    Set<MeterRegistry> descendants,
                                                    Set<MeterRegistry> visited) {
        if (!visited.add(composite)) return;
        for (MeterRegistry child : List.copyOf(composite.getRegistries())) {
            descendants.add(child);
            if (child instanceof CompositeMeterRegistry nested) {
                collectDescendantRegistries(nested, descendants, visited);
            }
        }
    }

    /**
     * 业务作用：计算一个操作目标实际触达的非 Composite 后端身份集合。
     *
     * @param registry 顶层 Composite 或独立 registry
     *                 返回: 按对象身份保存的不可变实际后端集合；空 Composite 返回空集合。
     */
    private static Set<MeterRegistry> leafRegistries(MeterRegistry registry) {
        Set<MeterRegistry> leaves = Collections.newSetFromMap(new IdentityHashMap<>());
        collectLeafRegistries(
                registry,
                leaves,
                Collections.newSetFromMap(new IdentityHashMap<>()));
        return Collections.unmodifiableSet(leaves);
    }

    /**
     * 业务作用：递归展开 Composite 并收集非 Composite 后端，以对象身份阻断重复路径。
     *
     * @param registry 当前目标或 Composite 成员
     * @param leaves   已发现的实际后端身份
     * @param visited  已展开的 Composite 身份
     *                 返回: 无返回值；leaves 原地累积全部可达实际后端。
     */
    private static void collectLeafRegistries(MeterRegistry registry,
                                              Set<MeterRegistry> leaves,
                                              Set<MeterRegistry> visited) {
        if (!(registry instanceof CompositeMeterRegistry composite)) {
            leaves.add(registry);
            return;
        }
        if (!visited.add(composite)) return;
        for (MeterRegistry child : List.copyOf(composite.getRegistries())) {
            collectLeafRegistries(child, leaves, visited);
        }
    }

    /**
     * 业务作用：冻结整个 Composite 图中每个节点的直接成员身份，使高频事件无需递归分配即可发现任意层改图。
     *
     * @param composite 当前 Composite 节点
     * @param snapshots 已冻结的节点与直接成员身份集合
     *                  返回: 无返回值；共享嵌套节点只保存一次。
     */
    private static void snapshotCompositeMembers(
            CompositeMeterRegistry composite,
            Map<CompositeMeterRegistry, Set<MeterRegistry>> snapshots) {
        if (snapshots.containsKey(composite)) return;
        Set<MeterRegistry> members = Collections.newSetFromMap(new IdentityHashMap<>());
        members.addAll(List.copyOf(composite.getRegistries()));
        snapshots.put(composite, Collections.unmodifiableSet(members));
        for (MeterRegistry member : members) {
            if (member instanceof CompositeMeterRegistry nested) {
                snapshotCompositeMembers(nested, snapshots);
            }
        }
    }

    /**
     * 业务作用：查找两个传播结果共享的实际后端。参数说明: 两个叶子身份集合。返回: 首个共享 registry；无交集返回 null。
     */
    private static MeterRegistry sharedRegistry(Set<MeterRegistry> first, Set<MeterRegistry> second) {
        for (MeterRegistry registry : first) {
            if (second.contains(registry)) return registry;
        }
        return null;
    }

    /**
     * 业务作用：生成可定位冲突对象且不依赖 equals 的注册表标识。参数说明: registry。返回: 类型与对象身份组合。
     */
    private static String registryIdentity(MeterRegistry registry) {
        return registry.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(registry));
    }

    /**
     * 业务作用：复验装配后的 Composite 仍触达原有后端集合，避免运行期改图静默改变事件传播路径。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；拓扑变化时关闭当前桥接器并明确失败，调用方需创建新的桥接器。
     */
    private synchronized void requireTopologyStable() {
        for (Map.Entry<CompositeMeterRegistry, Set<MeterRegistry>> entry : compositeMembers.entrySet()) {
            Set<MeterRegistry> current = entry.getKey().getRegistries();
            if (!sameRegistryIdentities(entry.getValue(), current)) {
                String registry = registryIdentity(entry.getKey());
                close();
                throw new IllegalStateException(
                        "MeterRegistry topology changed after binding: " + registry
                                + "; create a new metrics bridge");
            }
        }
    }

    /**
     * 业务作用：按对象身份比较两份实际后端集合。参数说明: 预期集合与当前集合。返回: 身份成员完全一致时为 true。
     */
    private static boolean sameRegistryIdentities(Set<MeterRegistry> expected,
                                                  Set<MeterRegistry> current) {
        if (expected.size() != current.size()) return false;
        for (MeterRegistry registry : expected) {
            if (!current.contains(registry)) return false;
        }
        return true;
    }

    /**
     * 业务作用：绑定分区入口并补登记已经发布的逻辑分组，使指标后端晚于业务组件创建时仍得到完整合同。
     *
     * @param partition 当前 RedisProxy 的分区入口
     *                  返回: 无返回值。
     */
    @Override
    public synchronized void partitionAvailable(RedisPartition partition) {
        if (closed) return;
        requireTopologyStable();
        this.partition = Objects.requireNonNull(partition, "partition");
        bindRuntime();
        if (closed) return;
        for (String groupName : partition.metricGroupNames()) groupAvailable(partition, groupName);
        if (closed) return;
        for (StreamSubscriptionPlan plan : partition.metricPlans()) planAvailable(partition, plan);
    }

    /**
     * 业务作用：为一个逻辑分组登记八项集群合同 gauge，标签只使用 qualifier、group 与 instanceId。
     *
     * @param partition 当前 RedisProxy 的分区入口
     * @param groupName 逻辑分组名
     *                  返回: 无返回值；同一分组重复通知保持幂等。
     */
    @Override
    public synchronized void groupAvailable(RedisPartition partition, String groupName) {
        if (closed) return;
        requireTopologyStable();
        if (this.partition == null) this.partition = partition;
        if (this.partition != partition || !boundGroups.add(groupName)) return;
        String group = groupName == null || groupName.isEmpty() ? "<default>" : groupName;
        Tags tags = Tags.of(
                "qualifier", partition.metricQualifier(),
                "group", group,
                "instanceId", partition.metricInstanceId());
        bindGroupGauge("node_alive", tags, groupName, RedisPartitionMetricSnapshot::nodeAlive);
        bindGroupGauge("claimed_partitions", tags, groupName,
                RedisPartitionMetricSnapshot::claimedPartitions);
        bindGroupGauge("fair_target_partitions", tags, groupName,
                RedisPartitionMetricSnapshot::fairTargetPartitions);
        bindGroupGauge("claim_generation", tags, groupName,
                RedisPartitionMetricSnapshot::claimGeneration);
        bindGroupGauge("pel_pending", tags, groupName, RedisPartitionMetricSnapshot::pelPending);
        bindGroupGauge("pel_oldest_idle_ms", tags, groupName,
                RedisPartitionMetricSnapshot::pelOldestIdleMillis);
        bindGroupGauge("owner_convergence_ms", tags, groupName,
                RedisPartitionMetricSnapshot::ownerConvergenceMillis);
        bindGroupGauge("pel_takeover_ms", tags, groupName,
                RedisPartitionMetricSnapshot::pelTakeoverMillis);
        bindGroupGauge("lock_self_checks", tags, groupName,
                RedisPartitionMetricSnapshot::lockSelfChecks);
        bindGroupGauge("wake_signals", tags, groupName,
                RedisPartitionMetricSnapshot::wakeSignals);
        bindGroupGauge("rebalance_fallbacks", tags, groupName,
                RedisPartitionMetricSnapshot::rebalanceFallbacks);
        bindGroupGauge("drain_timeouts", tags, groupName,
                RedisPartitionMetricSnapshot::drainTimeouts);
    }

    /**
     * 业务作用：为计划预登记全部 Task 终态与 Runner 健康 meter，使零流量计划仍可被部署检查发现。
     *
     * @param partition 当前 RedisProxy 的分区入口
     * @param plan      已激活订阅计划
     *                  返回: 无返回值；相同 planId 重复通知保持幂等。
     */
    @Override
    public synchronized void planAvailable(RedisPartition partition, StreamSubscriptionPlan plan) {
        if (closed) return;
        requireTopologyStable();
        if (this.partition == null) this.partition = partition;
        if (this.partition != partition || !boundPlans.add(plan.planId())) return;
        for (boolean ordered : new boolean[]{false, true}) {
            for (ConsumeStatus outcome : ConsumeStatus.values()) {
                Tags planTags = planTags(plan);
                if (planTags == null) return;
                Tags tags = planTags.and(
                        "ordered", Boolean.toString(ordered),
                        "outcome", outcome.name().toLowerCase(Locale.ROOT));
                for (MeterRegistry registry : registries)
                    counter(
                            registry, "stream_partition_tasks", tags,
                            "Partition Task terminal outcomes");
            }
        }
        bindRunnerGauge(plan, "started", runner -> runner.isStarted() ? 1.0D : 0.0D);
        bindRunnerGauge(plan, "healthy", runner -> runner.isHealthy() ? 1.0D : 0.0D);
        bindRunnerGauge(plan, "failed_partitions", Partition.PartitionRunner::failedPartitionCount);
        bindRunnerGauge(plan, "partitions", Partition.PartitionRunner::partitionCount);
    }

    /**
     * 业务作用：记录 partitionKey 粗类型、结果与耗时。参数说明: 计划、类型、结果和耗时。返回: 无返回值。
     */
    @Override
    public void partitionKey(StreamSubscriptionPlan plan, String kind, String result, long elapsedNanos) {
        Tags planTags = planTags(plan);
        if (planTags == null) return;
        Tags tags = planTags.and("kind", kind, "result", result);
        for (MeterRegistry registry : registries) {
            Timer timer = timer(registry, "stream_partition_key_duration", tags,
                    "partitionKey invocation latency");
            if (timer != null) timer.record(Math.max(0L, elapsedNanos), TimeUnit.NANOSECONDS);
        }
    }

    /**
     * 业务作用：记录 Redis batch 大小、拆分 Task 数和恢复来源。参数说明: 来源、阶段、恢复标志和值。返回: 无返回值。
     */
    @Override
    public void batch(StreamRecordSource source, String stage, boolean recovery, long value) {
        Tags runtimeTags = runtimeTags();
        if (runtimeTags == null) return;
        Tags tags = runtimeTags.and(
                "source", source.name().toLowerCase(Locale.ROOT),
                "stage", stage,
                "recovery", Boolean.toString(recovery));
        for (MeterRegistry registry : registries) {
            DistributionSummary summary = summary(registry, "stream_partition_batch", tags,
                    "Redis batch size and task split");
            if (summary != null) summary.record(Math.max(0L, value));
        }
    }

    /**
     * 业务作用：记录同计划同有效 hash 的 ordered bucket 大小。参数说明: 计划与数量。返回: 无返回值。
     */
    @Override
    public void orderedBucket(StreamSubscriptionPlan plan, long size) {
        Tags planTags = planTags(plan);
        if (planTags == null) return;
        Tags tags = planTags.and("ordered", "true");
        for (MeterRegistry registry : registries) {
            DistributionSummary summary = summary(registry, "stream_partition_ordered_bucket", tags,
                    "Ordered bucket size");
            if (summary != null) summary.record(Math.max(0L, size));
        }
    }

    /**
     * 业务作用：按计划稳定维度累计 Task 终态。参数说明: 计划、顺序属性与终态。返回: 无返回值。
     */
    @Override
    public void taskOutcome(StreamSubscriptionPlan plan, boolean ordered, ConsumeStatus outcome) {
        Tags planTags = planTags(plan);
        if (planTags == null) return;
        Tags tags = planTags.and(
                "ordered", Boolean.toString(ordered),
                "outcome", outcome.name().toLowerCase(Locale.ROOT));
        for (MeterRegistry registry : registries) {
            Counter counter = counter(registry, "stream_partition_tasks", tags,
                    "Partition Task terminal outcomes");
            if (counter != null) counter.increment();
        }
    }

    /**
     * 业务作用：记录 Task 排队、listener 与 Future 等待耗时。参数说明: 计划、顺序属性、阶段与耗时。返回: 无返回值。
     */
    @Override
    public void taskLatency(StreamSubscriptionPlan plan,
                            boolean ordered,
                            String stage,
                            long elapsedNanos) {
        Tags planTags = planTags(plan);
        if (planTags == null) return;
        Tags tags = planTags.and("ordered", Boolean.toString(ordered), "stage", stage);
        for (MeterRegistry registry : registries) {
            Timer timer = timer(registry, "stream_partition_task_latency", tags,
                    "Partition task stage latency");
            if (timer != null) timer.record(Math.max(0L, elapsedNanos), TimeUnit.NANOSECONDS);
        }
    }

    /**
     * 业务作用：累计 Submission 取消及拒绝结果。参数说明: 计划、顺序属性与结果。返回: 无返回值。
     */
    @Override
    public void submission(StreamSubscriptionPlan plan, boolean ordered, String result) {
        Tags planTags = planTags(plan);
        if (planTags == null) return;
        Tags tags = planTags.and("ordered", Boolean.toString(ordered), "result", result);
        for (MeterRegistry registry : registries) {
            Counter counter = counter(registry, "stream_partition_submissions", tags,
                    "Partition Submission outcomes");
            if (counter != null) counter.increment();
        }
    }

    /**
     * 业务作用：累计 PEL exact、route 与 XAUTOCLAIM 恢复结果。参数说明: 来源、类型与结果。返回: 无返回值。
     */
    @Override
    public void recovery(StreamRecordSource source, String type, String result) {
        Tags runtimeTags = runtimeTags();
        if (runtimeTags == null) return;
        Tags tags = runtimeTags.and(
                "source", source.name().toLowerCase(Locale.ROOT), "type", type, "result", result);
        for (MeterRegistry registry : registries) {
            Counter counter = counter(registry, "stream_partition_recovery", tags,
                    "Stream recovery outcomes");
            if (counter != null) counter.increment();
        }
    }

    /**
     * 业务作用：累计 fencing ACK 与 XPENDING 复验结果。参数说明: 来源、阶段与结果。返回: 无返回值。
     */
    @Override
    public void ack(StreamRecordSource source, String stage, String result) {
        Tags runtimeTags = runtimeTags();
        if (runtimeTags == null) return;
        Tags tags = runtimeTags.and(
                "source", source.name().toLowerCase(Locale.ROOT), "stage", stage, "result", result);
        for (MeterRegistry registry : registries) {
            Counter counter = counter(registry, "stream_partition_ack", tags,
                    "Fenced ACK and pending reconciliation outcomes");
            if (counter != null) counter.increment();
        }
    }

    /**
     * 业务作用：把运行时硬容量、门禁、确认、重试和延迟快照登记为一个固定 result 维度的低基数指标族。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；每个桥接器只登记一次。
     */
    private void bindRuntime() {
        if (closed || runtimeBound || partition == null) return;
        runtimeBound = true;
        for (String result : partition.runtimeMetrics().keySet()) {
            if (closed) return;
            Tags tags = Tags.of(
                    "qualifier", partition.metricQualifier(),
                    "group", "<runtime>",
                    "instanceId", partition.metricInstanceId(),
                    "result", result);
            for (MeterRegistry registry : registries) {
                if (closed) return;
                registerOwnedMeter(registry, gaugeDefinition(
                        "stream_partition_runtime",
                        tags,
                        "Stream Partition low-cardinality runtime state",
                        partition,
                        current -> current.runtimeMetrics().getOrDefault(result, 0L).doubleValue()));
            }
        }
    }

    /**
     * 业务作用：把一个分组快照字段登记到全部指标后端，scrape 期间只读取本地缓存。
     *
     * @param name      Prometheus 合同名
     * @param tags      固定低基数标签
     * @param groupName 逻辑分组名
     * @param value     快照取值函数
     *                  返回: 无返回值。
     */
    private void bindGroupGauge(String name,
                                Tags tags,
                                String groupName,
                                ToDoubleFunction<RedisPartitionMetricSnapshot> value) {
        if (closed) return;
        for (MeterRegistry registry : registries) {
            if (closed) return;
            registerOwnedMeter(registry, gaugeDefinition(
                    name,
                    tags,
                    "Stream Partition cluster convergence state",
                    partition,
                    current -> value.applyAsDouble(current.metricSnapshot(groupName))));
        }
    }

    /**
     * 业务作用：登记计划 Runner 的稳定健康字段，名称标签不使用对象身份或内部 planId。
     *
     * @param plan   计划及其 Runner
     * @param result 固定健康字段
     * @param value  Runner 取值函数
     *               返回: 无返回值。
     */
    private void bindRunnerGauge(StreamSubscriptionPlan plan,
                                 String result,
                                 ToDoubleFunction<io.github.nasaruntime.core.base.Partition.PartitionRunner> value) {
        if (closed) return;
        Tags tags = planTags(plan).and("runner", plan.runner().getRunnerName(), "result", result);
        for (MeterRegistry registry : registries) {
            if (closed) return;
            registerOwnedMeter(registry, gaugeDefinition(
                    "stream_partition_runner",
                    tags,
                    "Partition Runner health",
                    plan.runner(),
                    value));
        }
    }

    /**
     * 业务作用：构造计划级稳定标签。参数说明: 计划。返回: 不含 planId 和业务 key 的标签集；关闭后返回 null。
     */
    private synchronized Tags planTags(StreamSubscriptionPlan plan) {
        Tags runtimeTags = runtimeTags();
        if (runtimeTags == null) return null;
        return runtimeTags.and(
                "subscription", plan.metricSubscription(),
                "mode", plan.mode().name().toLowerCase(Locale.ROOT));
    }

    /**
     * 业务作用：构造当前代理级固定标签。参数说明: 无。返回: qualifier 与 instanceId 标签；关闭后返回 null。
     */
    private synchronized Tags runtimeTags() {
        RedisPartition current = partition;
        if (closed || current == null) return null;
        requireTopologyStable();
        return Tags.of(
                "qualifier", current.metricQualifier(),
                "instanceId", current.metricInstanceId());
    }

    /**
     * 业务作用：保存 Gauge 的过滤前身份和取值逻辑，使 Composite mapped Id 能在 leaf 上复用同一业务语义。
     *
     * @param name        指标名称
     * @param tags        指标标签
     * @param description 指标描述
     * @param target      Gauge 弱引用的业务快照来源
     * @param value       Gauge 取值函数
     *                    返回: 可按指定 pre-filter Id 重建 Gauge 的定义。
     */
    private static <T> MeterDefinition gaugeDefinition(
            String name,
            Tags tags,
            String description,
            T target,
            ToDoubleFunction<T> value) {
        Meter.Id sourceId = new Meter.Id(name, tags, null, description, Meter.Type.GAUGE);
        return new MeterDefinition(sourceId, (registry, id) -> Gauge.builder(id.getName(), target, value)
                .tags(id.getTagsAsIterable())
                .description(id.getDescription())
                .baseUnit(id.getBaseUnit())
                .register(registry));
    }

    /**
     * 业务作用：保存 Counter 的过滤前身份。参数说明: 名称、标签和描述。返回: 可按 mapped Id 登记的 Counter 定义。
     */
    private static MeterDefinition counterDefinition(String name, Tags tags, String description) {
        Meter.Id sourceId = new Meter.Id(name, tags, null, description, Meter.Type.COUNTER);
        return new MeterDefinition(sourceId, (registry, id) -> Counter.builder(id.getName())
                .tags(id.getTagsAsIterable())
                .description(id.getDescription())
                .baseUnit(id.getBaseUnit())
                .register(registry));
    }

    /**
     * 业务作用：保存 Timer 的过滤前身份。参数说明: 名称、标签和描述。返回: 可按 mapped Id 登记的 Timer 定义。
     */
    private static MeterDefinition timerDefinition(String name, Tags tags, String description) {
        Meter.Id sourceId = new Meter.Id(name, tags, null, description, Meter.Type.TIMER);
        return new MeterDefinition(sourceId, (registry, id) -> Timer.builder(id.getName())
                .tags(id.getTagsAsIterable())
                .description(id.getDescription())
                .register(registry));
    }

    /**
     * 业务作用：保存 Summary 的过滤前身份。参数说明: 名称、标签和描述。返回: 可按 mapped Id 登记的 Summary 定义。
     */
    private static MeterDefinition summaryDefinition(String name, Tags tags, String description) {
        Meter.Id sourceId = new Meter.Id(name, tags, null, description, Meter.Type.DISTRIBUTION_SUMMARY);
        return new MeterDefinition(sourceId, (registry, id) -> DistributionSummary.builder(id.getName())
                .tags(id.getTagsAsIterable())
                .description(id.getDescription())
                .baseUnit(id.getBaseUnit())
                .register(registry));
    }

    /**
     * 业务作用：按精确 meter 身份复用 Counter。参数说明: 注册表、名称、标签和描述。返回: 唯一 Counter；关闭后返回 null。
     */
    private synchronized Counter counter(MeterRegistry registry, String name, Tags tags, String description) {
        if (closed) return null;
        return (Counter) eventMeter(registry, new MetricKey("counter", name, tagList(tags)),
                counterDefinition(name, tags, description));
    }

    /**
     * 业务作用：按精确 meter 身份复用 Timer。参数说明: 注册表、名称、标签和描述。返回: 唯一 Timer；关闭后返回 null。
     */
    private synchronized Timer timer(MeterRegistry registry, String name, Tags tags, String description) {
        if (closed) return null;
        return (Timer) eventMeter(registry, new MetricKey("timer", name, tagList(tags)),
                timerDefinition(name, tags, description));
    }

    /**
     * 业务作用：按精确 meter 身份复用 DistributionSummary。参数说明: 注册表、名称、标签和描述。返回: 唯一摘要；关闭后返回 null。
     */
    private synchronized DistributionSummary summary(MeterRegistry registry,
                                                     String name,
                                                     Tags tags,
                                                     String description) {
        if (closed) return null;
        return (DistributionSummary) eventMeter(registry, new MetricKey("summary", name, tagList(tags)),
                summaryDefinition(name, tags, description));
    }

    /**
     * 业务作用：登记或取得当前桥接器拥有的事件 meter。参数说明: 注册表、身份与构造器。返回: 唯一 meter；关闭后返回 null。
     */
    private Meter eventMeter(MeterRegistry registry,
                             MetricKey key,
                             MeterDefinition definition) {
        Map<MetricKey, Meter> registered = eventMeters.computeIfAbsent(registry, ignored -> new HashMap<>());
        Meter existing = registered.get(key);
        if (existing != null) return existing;
        Set<MetricKey> skipped = skippedEventMeters.computeIfAbsent(registry, ignored -> new LinkedHashSet<>());
        if (skipped.contains(key)) return null;
        Meter created = registerOwnedMeter(registry, definition);
        if (created == null) {
            skipped.add(key);
            return null;
        }
        registered.put(key, created);
        return created;
    }

    /**
     * 业务作用：只接纳本次调用实际创建的 meter，并在 Composite 发布前取得每个初始后端的独立所有权。
     *
     * @param registry   顶层 Composite 或独立 registry
     * @param definition 可按过滤前或顶层 mapped Id 登记同一指标合同的定义
     *                   返回: 当前桥接器拥有且供记录使用的顶层 meter；精确身份已被应用占用、注册被拒绝或桥接器关闭时返回 null。
     */
    private synchronized Meter registerOwnedMeter(
            MeterRegistry registry,
            MeterDefinition definition) {
        if (closed) return null;
        // 登记和终态撤销必须共享 Micrometer 自身的线性化门禁；能力缺失时不能创建随后无法安全收口的对象。
        requireMeterRegistryAccess();
        if (!(registry instanceof CompositeMeterRegistry)) {
            return registerOwnedRootMeter(registry, definition, List.of());
        }

        List<MeterRegistry> transactionRegistries = new ArrayList<>();
        // 顶层在 leaf 之前，与 Micrometer 原生传播取锁方向一致；共享 leaf 再按身份序号排序，避免相反顺序互等。
        transactionRegistries.add(registry);
        transactionRegistries.addAll(initialLeafRegistries.getOrDefault(registry, Set.of()).stream()
                .sorted(Comparator.comparingLong(MicrometerStreamPartitionMetrics::registryLockOrder))
                .toList());
        // 只持有当前传播路径的门禁；跨后端全局锁不能覆盖同步应用回调，否则回调关闭另一桥接器时会形成等待环。
        return withRegistryConfigLocks(
                transactionRegistries,
                0,
                () -> withRegistryMeterLocks(
                        transactionRegistries,
                        0,
                        () -> registerOwnedCompositeMeter(registry, definition)));
    }

    /**
     * 业务作用：为实际后端分配跨桥接器稳定的身份顺序，使共享 leaf 的登记门禁始终按同一方向取得。
     *
     * @param registry 当前实际后端
     *                 返回: 当前对象存活期间不变的正序号；弱引用不延长后端生命周期，序号耗尽时明确拒绝继续赋序。
     */
    private static long registryLockOrder(MeterRegistry registry) {
        // 此临界区只操作本地弱引用和序号，不读取 registry 配置、不取得其锁，也不执行任何应用扩展。
        synchronized (REGISTRY_LOCK_ORDERS) {
            for (int index = REGISTRY_LOCK_ORDERS.size() - 1; index >= 0; index--) {
                RegistryLockOrder order = REGISTRY_LOCK_ORDERS.get(index);
                MeterRegistry existing = order.registry().get();
                if (existing == null) REGISTRY_LOCK_ORDERS.remove(index);
                else if (existing == registry) return order.sequence();
            }
            if (nextRegistryLockOrder == Long.MAX_VALUE) {
                throw new IllegalStateException("MeterRegistry lock order exhausted");
            }
            long sequence = ++nextRegistryLockOrder;
            REGISTRY_LOCK_ORDERS.add(new RegistryLockOrder(new WeakReference<>(registry), sequence));
            return sequence;
        }
    }

    /**
     * 业务作用：在官方门面完成顶层配置后、发布前逐 leaf 取得所有权，保留完整指标定义与过滤顺序。
     *
     * @param registry   顶层 Composite
     * @param definition 当前指标定义
     *                   返回: 当前桥接器拥有的 Composite meter；任一实际后端复用应用对象时返回 null。
     */
    private Meter registerOwnedCompositeMeter(
            MeterRegistry registry,
            MeterDefinition definition) {
        if (closed) return null;
        // 必须先建立逐对象传播门禁，后续创建的门面才具备不误删应用对象的撤销能力。
        ensureCompositeListeners(registry);
        Meter.Id mappedRootId = mappedMeterId(registry, definition.sourceId());
        List<RegisteredMeter> ownedLeaves = new ArrayList<>();
        CompositeRegistration previous = ACTIVE_COMPOSITE_REGISTRATION.get();
        ACTIVE_COMPOSITE_REGISTRATION.set(new CompositeRegistration(
                this, registry, mappedRootId, initialLeafRegistries.getOrDefault(registry, Set.of()),
                ownedLeaves, new ArrayList<>(1)));
        try {
            return registerOwnedRootMeter(registry, definition, ownedLeaves);
        } catch (CompositeRegistrationConflict conflict) {
            // leaf 冲突或同步关闭使新增回调在门面发布前终止；外层登记已按身份回收本次其它 leaf。
            return null;
        } finally {
            if (previous == null) ACTIVE_COMPOSITE_REGISTRATION.remove();
            else ACTIVE_COMPOSITE_REGISTRATION.set(previous);
        }
    }

    /**
     * 业务作用：在门面传播前用 Micrometer 已完成顶层过滤的实际定义取得 leaf 所有权。
     *
     * @param registration 当前 Composite 登记上下文
     * @param meter        Micrometer 已构造但尚未发布的门面
     *                     返回: 无返回值；冲突或同步关闭时在门面发布前抛出内部拒绝信号，已创建坐标由外层登记回收。
     */
    private static void reserveCompositeLeaves(CompositeRegistration registration, Meter meter) {
        // 直接使用官方门面的 child 构造入口，保留 SLO、百分位、窗口、精度和 PauseDetector 的实际传播顺序。
        for (MeterRegistry leaf : registration.leaves()) {
            // leaf 的同步回调可以关闭桥接器；终态出现后禁止继续占用下一后端。
            if (registration.owner().closed) throw new CompositeRegistrationConflict();
            RegistrationResult result = registerOnce(leaf, meter.getId(), target -> registerCompositeChild(meter, target));
            if (result.meter() instanceof NoopMeter) continue;
            // 返回既有对象不构成创建证据，禁止 Composite 随后把事件写入应用的采集项。
            if (!result.created()) throw new CompositeRegistrationConflict();
            registration.ownedLeaves().add(new RegisteredMeter(leaf, result.meter(), result.synthetics()));
        }
        // 最后一个 leaf 也可能同步重入关闭，必须在官方门面传播和发布前终止并交给外层回滚。
        if (registration.owner().closed) throw new CompositeRegistrationConflict();
    }

    /**
     * 业务作用：调用实际 Composite meter 的 child 构造入口，让完整分布定义继续经过 leaf 过滤器。
     *
     * @param meter 已完成顶层配置的 Composite meter
     * @param leaf  实际后端
     *              返回: Micrometer 在 leaf 上创建、复用或拒绝的 meter；不兼容或登记失败时明确抛出异常。
     */
    private static Meter registerCompositeChild(Meter meter, MeterRegistry leaf) {
        try {
            return (Meter) requireMeterRegistryAccess().registerChildMethod().invoke(meter, leaf);
        } catch (ReflectiveOperationException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtimeFailure) throw runtimeFailure;
            if (cause instanceof Error error) throw error;
            throw new IllegalStateException("Unable to register a Micrometer Composite child safely", failure);
        }
    }

    /**
     * 业务作用：为官方 Composite 内置传播回调安装一次按当前调用对象限定的所有权门禁。
     *
     * @param registry 当前顶层 Composite
     *                 返回: 无返回值；无法确认官方回调身份时拒绝登记；应用回调的身份、顺序和参数保持原样。
     */
    @SuppressWarnings("unchecked")
    private static void ensureCompositeListeners(MeterRegistry registry) {
        MeterRegistryAccess access = requireMeterRegistryAccess();
        try {
            List<Consumer<Meter>> added = (List<Consumer<Meter>>) access.addedListenersField().get(registry);
            List<Consumer<Meter>> removed = (List<Consumer<Meter>>) access.removedListenersField().get(registry);
            // 两个入口都验证后才发布适配，未知实现不能留下只保护新增或只保护撤销的半完成状态。
            Consumer<Meter> addGuard = compositeListener(registry, added.getFirst(), true);
            Consumer<Meter> removeGuard = compositeListener(registry, removed.getFirst(), false);
            if (added.getFirst() != addGuard) added.set(0, addGuard);
            if (removed.getFirst() != removeGuard) removed.set(0, removeGuard);
        } catch (IllegalAccessException failure) {
            throw new IllegalStateException("Unable to guard Micrometer Composite propagation", failure);
        }
    }

    /**
     * 业务作用：验证并包装官方内置传播回调，只限制当前桥接器正在操作的门面。
     *
     * @param registry 回调所属 Composite
     * @param listener 当前首个内置回调
     * @param addition 是否为新增传播
     *                 返回: 已安装的门禁或新门禁；回调来源不兼容时拒绝使用未知传播行为。
     */
    private static Consumer<Meter> compositeListener(
            MeterRegistry registry, Consumer<Meter> listener, boolean addition) {
        if (listener instanceof CompositeListener guard
                && guard.registry() == registry && guard.addition() == addition) return listener;
        if (!listener.getClass().isSynthetic()
                || listener.getClass().getNestHost() != CompositeMeterRegistry.class) {
            throw new IllegalStateException("Unsupported Micrometer Composite propagation listener");
        }
        return new CompositeListener(registry, listener, addition);
    }

    /**
     * 业务作用：创建顶层 meter 并在关闭与拓扑门禁复验后提交完整所有权坐标。
     *
     * @param registry    独立 registry 或在新增回调中执行 leaf 预占的 Composite
     * @param definition  当前指标定义
     * @param ownedLeaves 当前流程的 leaf 创建清单，Composite 新增回调在发布前填充该清单
     *                    返回: 已提交所有权的顶层 meter；注册被拒绝、返回既有对象或桥接器关闭时返回 null。
     */
    private Meter registerOwnedRootMeter(
            MeterRegistry registry,
            MeterDefinition definition,
            List<RegisteredMeter> ownedLeaves) {
        RegistrationResult rootResult;
        try {
            rootResult = registerOnce(
                    registry, definition.sourceId(),
                    target -> definition.register(target, definition.sourceId()));
        } catch (RuntimeException | Error failure) {
            removeRegisteredMetersSafely(ownedLeaves);
            throw failure;
        }
        if (rootResult.meter() instanceof NoopMeter || !rootResult.created()) {
            removeRegisteredMetersSafely(ownedLeaves);
            return null;
        }

        OwnedRegistration owned = new OwnedRegistration(
                new RegisteredMeter(registry, rootResult.meter(), rootResult.synthetics()),
                List.copyOf(ownedLeaves));
        if (closed) {
            // register 的同步回调可以重入 close；返回后只能撤销由本次原子登记证明确认的实例。
            removeOwnedRegistrationSafely(owned);
            return null;
        }
        try {
            requireTopologyStable();
        } catch (RuntimeException failure) {
            // 拓扑复验关闭桥接器时，本次坐标尚未进入统一关闭清单，必须在传播异常前独立收口。
            removeOwnedRegistrationSafely(owned);
            throw failure;
        }
        ownedRegistrations.add(owned);
        return rootResult.meter();
    }

    /**
     * 业务作用：在全部相关 Config 门禁内执行 Composite 映射与登记，防止 mapped Id 在事务中途改变。
     *
     * @param registries 当前 Composite 及其初始 leaf
     * @param index      下一项待取得门禁的索引
     * @param action     全部门禁取得后执行的登记事务
     *                   返回: 登记事务结果。
     */
    private static Meter withRegistryConfigLocks(
            List<MeterRegistry> registries,
            int index,
            Supplier<Meter> action) {
        if (index == registries.size()) return action.get();
        synchronized (registries.get(index).config()) {
            return withRegistryConfigLocks(registries, index + 1, action);
        }
    }

    /**
     * 业务作用：锁定 Composite 与全部初始 leaf 的 Micrometer 创建域，使预占、门面传播和失败回滚不可被同 Id 登记穿插。
     *
     * @param registries 当前 Composite 及其初始 leaf
     * @param index      下一项待取得创建门禁的索引
     * @param action     全部门禁取得后执行的登记事务
     *                   返回: 登记事务结果；运行时 Micrometer 不提供所需创建门禁时明确拒绝 Composite 登记。
     */
    private static Meter withRegistryMeterLocks(
            List<MeterRegistry> registries,
            int index,
            Supplier<Meter> action) {
        if (index == registries.size()) return action.get();
        synchronized (meterMapLock(registries.get(index))) {
            return withRegistryMeterLocks(registries, index + 1, action);
        }
    }

    /**
     * 业务作用：读取顶层 registry 按当前全部 MeterFilter 计算出的真实 Id，作为 Composite 向 leaf 传播时的 pre-filter 坐标。
     *
     * @param registry 顶层 Composite registry
     * @param sourceId 业务指标的过滤前 Id
     *                 返回: 顶层 MeterFilter 处理后的 Id；运行时 Micrometer 不兼容时明确失败，禁止带着未知删除坐标继续登记。
     */
    private static Meter.Id mappedMeterId(MeterRegistry registry, Meter.Id sourceId) {
        MeterRegistryAccess access = requireMeterRegistryAccess();
        try {
            Object mapped = access.mappedIdMethod().invoke(registry, sourceId);
            if (mapped instanceof Meter.Id mappedId) return mappedId;
            throw new IllegalStateException("Micrometer returned an invalid mapped meter Id");
        } catch (ReflectiveOperationException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtimeFailure) throw runtimeFailure;
            if (cause instanceof Error error) throw error;
            throw new IllegalStateException("Unable to map a Micrometer meter Id safely", failure);
        }
    }

    /**
     * 业务作用：取得 registry 与自身 register/remove 共用的创建门禁，保证所有权检查和状态变更处于同一线性化域。
     *
     * @param registry 需要锁定的 registry
     *                 返回: Micrometer 内部创建门禁；运行时结构不兼容时明确失败。
     */
    private static Object meterMapLock(MeterRegistry registry) {
        MeterRegistryAccess access = requireMeterRegistryAccess();
        try {
            Object lock = access.meterMapLockField().get(registry);
            if (lock != null) return lock;
            throw new IllegalStateException("Micrometer returned an invalid meter creation lock");
        } catch (IllegalAccessException failure) {
            throw new IllegalStateException("Unable to access the Micrometer meter creation lock", failure);
        }
    }

    /**
     * 业务作用：确认当前 Micrometer 运行时提供对象所有权协议依赖的映射、完整定义传播、派生关联与创建门禁。
     * <p>
     * 参数说明: 无。
     * 返回: 已验证可调用的兼容访问器；缺少任一能力时抛出 IllegalStateException。
     */
    private static MeterRegistryAccess requireMeterRegistryAccess() {
        if (METER_REGISTRY_ACCESS.available()) return METER_REGISTRY_ACCESS;
        throw new IllegalStateException(
                "The Micrometer runtime does not expose the meter ownership primitives required for safe Composite registration",
                METER_REGISTRY_ACCESS.unavailableReason());
    }

    /**
     * 业务作用：在类初始化时解析映射、child 定义、派生关联和传播回调入口，避免部分可用状态进入业务登记。
     * <p>
     * 参数说明: 无。
     * 返回: 所有能力均可访问的适配器；不兼容时保存失败原因，由登记路径按安全边界拒绝。
     */
    private static MeterRegistryAccess resolveMeterRegistryAccess() {
        try {
            Method mappedIdMethod = MeterRegistry.class.getDeclaredMethod("getMappedId", Meter.Id.class);
            Field meterMapLockField = MeterRegistry.class.getDeclaredField("meterMapLock");
            Class<?> compositeMeter = Class.forName(
                    "io.micrometer.core.instrument.composite.AbstractCompositeMeter", false,
                    CompositeMeterRegistry.class.getClassLoader());
            Method registerChildMethod = compositeMeter.getDeclaredMethod("registerNewMeter", MeterRegistry.class);
            Field addedListenersField = MeterRegistry.class.getDeclaredField("meterAddedListeners");
            Field removedListenersField = MeterRegistry.class.getDeclaredField("meterRemovedListeners");
            Field syntheticAssociationsField = MeterRegistry.class.getDeclaredField("syntheticAssociations");
            if (!mappedIdMethod.trySetAccessible() || !meterMapLockField.trySetAccessible()
                    || !registerChildMethod.trySetAccessible() || !addedListenersField.trySetAccessible()
                    || !removedListenersField.trySetAccessible() || !syntheticAssociationsField.trySetAccessible()) {
                return MeterRegistryAccess.unavailable(
                        new IllegalAccessException("Micrometer meter ownership primitives are not accessible"));
            }
            return new MeterRegistryAccess(mappedIdMethod, meterMapLockField, registerChildMethod,
                    addedListenersField, removedListenersField, syntheticAssociationsField, null);
        } catch (ReflectiveOperationException | RuntimeException | LinkageError failure) {
            return MeterRegistryAccess.unavailable(failure);
        }
    }

    /**
     * 业务作用：在应用新增回调前保存创建证据，并在父对象移除后、应用撤销回调前清理自有派生对象。
     *
     * @param registry 需要建立创建与撤销证据的注册表
     *                 返回: 无返回值；同一 registry 最多安装一次，不改变应用回调之间的顺序。
     */
    @SuppressWarnings("unchecked")
    private static void ensureOwnershipListener(MeterRegistry registry) {
        synchronized (OWNERSHIP_LISTENERS) {
            for (int index = OWNERSHIP_LISTENERS.size() - 1; index >= 0; index--) {
                MeterRegistry installed = OWNERSHIP_LISTENERS.get(index).get();
                if (installed == null) OWNERSHIP_LISTENERS.remove(index);
                else if (installed == registry) return;
            }
            MeterRegistryAccess access = requireMeterRegistryAccess();
            try {
                List<Consumer<Meter>> added = (List<Consumer<Meter>>) access.addedListenersField().get(registry);
                List<Consumer<Meter>> removed = (List<Consumer<Meter>>) access.removedListenersField().get(registry);
                int index = registry instanceof CompositeMeterRegistry ? 1 : 0;
                // Composite 首个内置回调仍负责传播；创建证据必须早于可能抛出的应用回调，覆盖尚未发布父对象的部分完成状态。
                added.add(index, meter -> {
                    RegistrationAttempt attempt = ACTIVE_REGISTRATION.get();
                    if (attempt != null && attempt.registry() == registry) attempt.addedMeters().add(meter);
                });
                removed.add(index, meter -> {
                    RegisteredMeter removal = ACTIVE_METER_REMOVAL.get();
                    if (removal != null && removal.registry() == registry && removal.meter() == meter) {
                        // 此时父对象已退出注册表；先清空旧派生坐标，应用父回调才可按相同 Id 登记新的完整分布指标。
                        removeRegisteredMetersSafely(removal.synthetics());
                    }
                });
                OWNERSHIP_LISTENERS.add(new WeakReference<>(registry));
            } catch (IllegalAccessException failure) {
                throw new IllegalStateException("Unable to observe Micrometer meter ownership", failure);
            }
        }
    }

    /**
     * 业务作用：保留一次登记创建的父对象及其实际派生对象证据，失败时回收已发布的部分派生坐标。
     *
     * @param registry 本次登记目标
     * @param sourceId 本次父指标的过滤前身份
     * @param factory  使用官方定义登记父对象的构造函数
     *                 返回: 父对象、创建事实和派生对象清单；登记异常原样传播，应用独立创建的指标不进入撤销范围。
     */
    private static RegistrationResult registerOnce(
            MeterRegistry registry,
            Meter.Id sourceId,
            Function<MeterRegistry, Meter> factory) {
        synchronized (registry.config()) {
            synchronized (meterMapLock(registry)) {
                ensureOwnershipListener(registry);
                Meter.Id expectedId = mappedMeterId(registry, sourceId);
                RegistrationAttempt previous = ACTIVE_REGISTRATION.get();
                List<Meter> addedMeters = new ArrayList<>();
                ACTIVE_REGISTRATION.set(new RegistrationAttempt(registry, addedMeters));
                try {
                    Meter meter = factory.apply(registry);
                    return new RegistrationResult(meter,
                            addedMeters.stream().anyMatch(added -> added == meter),
                            registeredSynthetics(registry, meter.getId(), addedMeters));
                } catch (RuntimeException | Error failure) {
                    // 派生 Gauge 可在父对象返回前发布；异常不能丢弃这些创建证据，也不能把回滚回调新建的应用对象并入证据。
                    ACTIVE_REGISTRATION.remove();
                    Meter.Id parentId = addedMeters.stream()
                            .map(added -> added.getId().syntheticAssociation())
                            .filter(association -> association != null && association.equals(expectedId))
                            .findFirst().orElse(null);
                    if (parentId != null) {
                        List<RegisteredMeter> synthetics = registeredSynthetics(registry, parentId, addedMeters);
                        detachFailedSyntheticAssociations(registry, parentId, synthetics);
                        removeRegisteredMetersSafely(synthetics);
                    }
                    throw failure;
                } finally {
                    if (previous == null) ACTIVE_REGISTRATION.remove();
                    else ACTIVE_REGISTRATION.set(previous);
                }
            }
        }
    }

    /**
     * 业务作用：用新增事件和真实父 Id 对象共同确认派生归属，不以名称或相同坐标推断应用对象所有权。
     *
     * @param registry    创建派生对象的实际后端
     * @param parentId    后端构造父对象时使用的真实 Id 对象
     * @param addedMeters 本次同步新增事件保存的对象
     *                    返回: 属于该父对象的派生对象树；预先存在、普通同名 Gauge 和其它父指标的派生项均不纳入。
     */
    private static List<RegisteredMeter> registeredSynthetics(
            MeterRegistry registry, Meter.Id parentId, List<Meter> addedMeters) {
        List<RegisteredMeter> result = new ArrayList<>();
        for (Meter added : addedMeters) {
            if (added.getId().syntheticAssociation() == parentId) {
                result.add(new RegisteredMeter(registry, added,
                        registeredSynthetics(registry, added.getId(), addedMeters)));
            }
        }
        return List.copyOf(result);
    }

    /**
     * 业务作用：取得 Micrometer 按父 Id 保存的派生级联关系，使撤销可在原生创建门禁内转为逐对象处理。
     *
     * @param registry 当前实际后端
     *                 返回: 仅允许在 meterMapLock 内读写的派生关系表；不兼容时明确拒绝。
     */
    @SuppressWarnings("unchecked")
    private static Map<Meter.Id, Set<Meter.Id>> syntheticAssociations(MeterRegistry registry) {
        try {
            return (Map<Meter.Id, Set<Meter.Id>>) requireMeterRegistryAccess()
                    .syntheticAssociationsField().get(registry);
        } catch (IllegalAccessException failure) {
            throw new IllegalStateException("Unable to access Micrometer synthetic ownership", failure);
        }
    }

    /**
     * 业务作用：解除失败父对象留下的已观察派生关系，避免未发布坐标影响后续同 Id 登记的撤销。
     *
     * @param registry   当前实际后端
     * @param parentId   未完成登记的父对象身份
     * @param synthetics 本次创建事件证明的派生对象
     *                   返回: 无返回值；仅移除本次已观察的关联，未知派生关系保持原样。
     */
    private static void detachFailedSyntheticAssociations(
            MeterRegistry registry, Meter.Id parentId, List<RegisteredMeter> synthetics) {
        Map<Meter.Id, Set<Meter.Id>> associations = syntheticAssociations(registry);
        Set<Meter.Id> ids = associations.get(parentId);
        if (ids == null) return;
        // 回调可以登记另一个同 Id 父对象；其已发布权威不能被失败外层登记解除。
        if (registry.getMeters().stream().anyMatch(meter -> meter.getId().equals(parentId))) return;
        for (RegisteredMeter synthetic : synthetics) ids.remove(synthetic.meter().getId());
        if (ids.isEmpty()) associations.remove(parentId);
    }

    /**
     * 业务作用：撤销一组已经证明由当前登记流程创建的后端或派生对象。
     *
     * @param registeredMeters 需要撤销的精确 registry 与 meter 坐标
     *                         返回: 无返回值；单个 registry 回调失败不阻断其余坐标。
     */
    private static void removeRegisteredMetersSafely(List<RegisteredMeter> registeredMeters) {
        for (RegisteredMeter registered : registeredMeters) {
            removeMeterSafely(registered);
        }
    }

    /**
     * 业务作用：让顶层门面撤销携带完整 leaf 创建证据，在应用顶层撤销回调前完成按身份清理。
     *
     * @param owned 当前桥接器实际创建的一项完整登记
     *              返回: 无返回值；保留应用替代对象，同 Id 新门面可在顶层撤销回调中绑定新后端；脱图及未触发回调的坐标仍按身份收口。
     */
    private static void removeOwnedRegistrationSafely(OwnedRegistration owned) {
        // 回调重入其它桥接器时必须保存并恢复完整创建证据，避免把另一门面的 leaf 归入当前撤销。
        OwnedRegistration previous = ACTIVE_COMPOSITE_REMOVAL.get();
        ACTIVE_COMPOSITE_REMOVAL.set(owned);
        try {
            removeMeterSafely(owned.root());
        } finally {
            if (previous == null) ACTIVE_COMPOSITE_REMOVAL.remove();
            else ACTIVE_COMPOSITE_REMOVAL.set(previous);
        }
        // 顶层对象可能已被应用撤销而不再触发回调；兜底只复验旧对象身份，不触及回调中新建的 leaf。
        removeRegisteredMetersSafely(owned.leaves());
    }

    /**
     * 业务作用：冻结 Tags 的确定顺序用于 meter 身份比较。参数说明: 标签集。返回: 不可变标签列表。
     */
    private static List<Tag> tagList(Tags tags) {
        List<Tag> result = new ArrayList<>();
        for (Tag tag : tags) result.add(tag);
        return List.copyOf(result);
    }

    /**
     * 业务作用：按对象身份撤销父对象与自有派生对象，阻止原生按 Id 级联删除应用替代对象。
     *
     * @param registered 当前桥接器登记的对象及其派生创建证据
     *                   返回: 无返回值；对象已被替换时保留当前对象，应用撤销回调异常不阻断其余自有坐标清理。
     */
    private static void removeMeterSafely(RegisteredMeter registered) {
        MeterRegistry registry = registered.registry();
        Meter meter = registered.meter();
        RegisteredMeter previous = ACTIVE_METER_REMOVAL.get();
        try {
            synchronized (meterMapLock(registry)) {
                // 只有父对象仍归本次登记，才能解除它的按 Id 级联关系；随后由移除回调逐个复验旧派生对象。
                if (containsMeterIdentity(registry, meter)) {
                    syntheticAssociations(registry).remove(meter.getId());
                    ACTIVE_METER_REMOVAL.set(registered);
                    registry.remove(meter);
                }
            }
        } catch (Throwable ignored) {
            // 指标注册表不是消费权威，单个扩展回调不能阻断其余终态清理。
        } finally {
            if (previous == null) ACTIVE_METER_REMOVAL.remove();
            else ACTIVE_METER_REMOVAL.set(previous);
            // 父对象可能已被应用移除，或某个撤销回调中途抛出；兜底仍只收口原对象，不触及同 Id 替代项。
            removeRegisteredMetersSafely(registered.synthetics());
        }
    }

    /**
     * 业务作用：确认关闭坐标中的对象仍是 registry 当前登记实例，避免按相同 Id 撤销后来由应用登记的替代对象。
     *
     * @param registry meter 所属注册表
     * @param meter    当前桥接器创建时保存的对象
     *                 返回: registry 当前仍持有同一对象身份时为 true；读取失败或对象已被替换时为 false。
     */
    private static boolean containsMeterIdentity(MeterRegistry registry, Meter meter) {
        try {
            for (Meter candidate : registry.getMeters()) {
                if (candidate == meter) return true;
            }
        } catch (Throwable ignored) {
            // 无法证明对象仍归当前桥接器时必须保守跳过，避免越权撤销应用资源。
        }
        return false;
    }

    /**
     * 业务作用：从全部注册表移除本桥接器创建的 meter，释放对已销毁 RedisProxy 的引用。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；关闭终态不可逆且先于任何外部撤销回调发布，重复关闭保持幂等，单个注册表回调失败不会阻断其它 meter。
     */
    @Override
    public synchronized void close() {
        if (closed) return;
        // 必须先发布不可逆终态；remove 的同步回调及等待 monitor 的并发入口此后都不能再创建 meter。
        closed = true;
        for (OwnedRegistration owned : List.copyOf(ownedRegistrations)) {
            removeOwnedRegistrationSafely(owned);
        }
        ownedRegistrations.clear();
        eventMeters.clear();
        skippedEventMeters.clear();
        boundGroups.clear();
        boundPlans.clear();
        partition = null;
        runtimeBound = false;
    }

    /**
     * 业务作用：保存父对象与实际派生对象的创建证据，关闭时逐对象复验身份。
     */
    private record RegisteredMeter(MeterRegistry registry, Meter meter, List<RegisteredMeter> synthetics) {
    }

    /**
     * 业务作用：以弱引用保存实际后端的稳定取锁顺序，不依赖 registry 的 equals、hashCode 或外部回调。
     */
    private record RegistryLockOrder(WeakReference<MeterRegistry> registry, long sequence) {
    }

    /**
     * 业务作用：保存一次顶层登记及其在初始 leaf 上由当前桥接器实际创建的对象坐标。
     */
    private record OwnedRegistration(RegisteredMeter root, List<RegisteredMeter> leaves) {
    }

    /**
     * 业务作用：保存当前线程一次 register 调用栈内的同步新增对象集合。
     */
    private record RegistrationAttempt(MeterRegistry registry, List<Meter> addedMeters) {
    }

    /**
     * 业务作用：保存返回父对象的创建事实及其派生对象树，使登记提交和异常后的关闭使用同一份证据。
     */
    private record RegistrationResult(Meter meter, boolean created, List<RegisteredMeter> synthetics) {
    }

    /**
     * 业务作用：限定一次顶层登记的 leaf 预占范围，保存回滚坐标并隔离应用回调重入。
     */
    private record CompositeRegistration(MicrometerStreamPartitionMetrics owner, MeterRegistry registry,
                                         Meter.Id mappedId,
                                         Set<MeterRegistry> leaves, List<RegisteredMeter> ownedLeaves,
                                         List<Meter> preparedRoots) {
    }

    /**
     * 业务作用：在发布 Composite 门面前中止已遇到应用坐标冲突或桥接器关闭的登记。
     */
    private static final class CompositeRegistrationConflict extends RuntimeException {
    }

    /**
     * 业务作用：只对当前线程、当前 registry 和当前门面应用所有权约束，不改变应用 meter 的原有传播行为。
     */
    private record CompositeListener(MeterRegistry registry, Consumer<Meter> delegate,
                                     boolean addition) implements Consumer<Meter> {

        /**
         * 业务作用：新增时以实际完整定义预占 leaf，撤销时在顶层应用回调前按创建证据清理当前门面的 leaf。
         *
         * @param meter Micrometer 同步回调提供的实际对象
         *              返回: 无返回值；当前门面完成 leaf 清理后才继续顶层应用回调，其它对象原样交给官方回调，新增冲突在发布前拒绝。
         */
        @Override
        public void accept(Meter meter) {
            if (addition) {
                CompositeRegistration registration = ACTIVE_COMPOSITE_REGISTRATION.get();
                if (registration != null && registration.registry() == registry
                        && registration.mappedId().equals(meter.getId())
                        && registration.preparedRoots().isEmpty()) {
                    // 先标记本次门面，leaf 同步回调中的重入登记不能冒用同一次预占上下文。
                    registration.preparedRoots().add(meter);
                    reserveCompositeLeaves(registration, meter);
                }
            } else {
                OwnedRegistration removal = ACTIVE_COMPOSITE_REMOVAL.get();
                // 只有当前线程正在撤销的精确门面可以使用这份证据，应用新门面及其它对象仍走官方传播。
                if (removal != null && removal.root().registry() == registry && removal.root().meter() == meter) {
                    // 旧门面已从 registry 移除；先按身份撤销旧 leaf，再让后续顶层应用回调登记同 Id 新门面，避免复用待删除的后端。
                    // 此处替代内置按 Id 广播，应用已替换的 leaf 和后加入后端中归属未知的对象仍须保留。
                    removeRegisteredMetersSafely(removal.leaves());
                    return;
                }
            }
            delegate.accept(meter);
        }
    }

    /**
     * 业务作用：保存一个指标的过滤前身份与按任意 pre-filter Id 重建相同业务 meter 的逻辑。
     *
     * @param sourceId  业务指标的过滤前身份
     * @param registrar 按指定 pre-filter Id 向目标 registry 登记 meter 的函数
     */
    private record MeterDefinition(
            Meter.Id sourceId,
            BiFunction<MeterRegistry, Meter.Id, Meter> registrar) {

        /**
         * 业务作用：使用调用方给定的 pre-filter Id 登记当前业务指标，使顶层与 leaf 采用同一传播语义。
         *
         * @param registry 目标 registry
         * @param id       本次登记的 pre-filter Id
         *                 返回: Micrometer 创建、复用或拒绝后返回的 meter。
         */
        private Meter register(MeterRegistry registry, Meter.Id id) {
            return registrar.apply(registry, id);
        }
    }

    /**
     * 业务作用：封装当前 Micrometer 的映射、完整 child 定义、传播回调、派生关联与 meter 创建门禁。
     *
     * @param mappedIdMethod             MeterRegistry 的映射方法
     * @param meterMapLockField          MeterRegistry 的创建门禁字段
     * @param registerChildMethod        Composite 使用实际 meter 定义创建 child 的方法
     * @param addedListenersField        MeterRegistry 的新增回调集合
     * @param removedListenersField      MeterRegistry 的撤销回调集合
     * @param syntheticAssociationsField 父 Id 到派生 Id 的级联关系
     * @param unavailableReason          运行时不兼容原因
     */
    private record MeterRegistryAccess(
            Method mappedIdMethod,
            Field meterMapLockField,
            Method registerChildMethod,
            Field addedListenersField,
            Field removedListenersField,
            Field syntheticAssociationsField,
            Throwable unavailableReason) {

        /**
         * 业务作用：保存运行时不兼容原因并禁止 Composite 继续登记。参数说明: 失败原因。返回: 不可用适配器。
         */
        private static MeterRegistryAccess unavailable(Throwable reason) {
            return new MeterRegistryAccess(null, null, null, null, null, null, reason);
        }

        /**
         * 业务作用：判断所有权协议需要的全部入口是否可访问。参数说明: 无。返回: 全部能力均存在时为 true。
         */
        private boolean available() {
            return mappedIdMethod != null && meterMapLockField != null && registerChildMethod != null
                    && addedListenersField != null && removedListenersField != null && syntheticAssociationsField != null;
        }
    }

    /**
     * 业务作用：以 meter 类型、名称和稳定标签标识一个动态事件指标。
     */
    private record MetricKey(String type, String name, List<Tag> tags) {
    }

    /**
     * 业务作用：冻结有效操作目标、Composite 成员与初始叶子快照，用于登记、拓扑复验及脱图后撤销。
     */
    private record RegistryTopology(List<MeterRegistry> registries,
                                    Map<CompositeMeterRegistry, Set<MeterRegistry>> compositeMembers,
                                    Map<MeterRegistry, Set<MeterRegistry>> initialLeafRegistries) {
    }
}
