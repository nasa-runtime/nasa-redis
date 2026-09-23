package io.github.nasaruntime.redis.cache.redis.partition;

import java.nio.charset.StandardCharsets;
import java.util.*;
import io.github.nasaruntime.redis.cache.redis.partition.RedisPartitionProperties.ExecutorScope;

/** 业务作用：在任何执行器启动前证明完整物理拓扑和固定数量份额可用。 */
final class PartitionExecutionPlan {
    final ExecutorScope scope;
    final int slots;
    final List<DomainSpec> domains;
    final Map<String, GroupSpec> groups;

    /** 业务作用：描述解析后的物理分区组。参数说明: name 为逻辑名，count 为物理数量，batch 为读取批量。 */
    record GroupSpec(String name, int count, int batch) { }
    /** 业务作用：冻结域身份及其不可借出的容量。参数说明: id 为稳定索引，group/partition 为物理身份，quota 为源总额份额。 */
    record DomainSpec(int id, String group, Integer partition, Quota quota) { }
    /** 业务作用：保存域的独立数量上限。参数说明: 各字段分别限制读取、执行、确认及恢复责任。 */
    record Quota(int records, int tasks, int attempts, int commits, int retries, int unordered, int routeBlocked) { }

    /**
     * 业务作用：按完整拓扑排序、核对最坏批次并均分源级余量，不产生外部副作用。
     * @param qualifier 当前源诊断名称
     * @param config 源分区配置
     * @param topology 已解析的完整物理拓扑
     * @param actualSlots 已启动显式 Runner 的槽数；零表示使用当前 JVM 配置
     * 返回: 全部最低需求可满足的不可变执行计划；非法配置立即拒绝。
     */
    PartitionExecutionPlan(String qualifier, RedisPartitionProperties config,
                           Collection<GroupSpec> topology, int actualSlots) {
        var executor = config.getExecutor();
        scope = Objects.requireNonNull(executor.getScope(), "executor.scope");
        var ordered = new TreeMap<String, GroupSpec>((a, b) -> Arrays.compareUnsigned(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8)));
        for (GroupSpec group : topology) {
            if (group.count() < 1 || group.batch() < 1) throw invalid(qualifier, "count/batch", 0, 1, 0);
            GroupSpec old = ordered.putIfAbsent(group.name(), group);
            if (old != null && !old.equals(group)) throw new IllegalArgumentException("conflicting execution group: " + group.name());
        }
        groups = Collections.unmodifiableMap(ordered);
        long count = switch (scope) {
            case SOURCE -> ordered.isEmpty() ? 0L : 1L;
            case GROUP -> ordered.size();
            case STREAM -> ordered.values().stream().mapToLong(GroupSpec::count).sum();
        };
        if (count < 1 || executor.getMaxRunners() < 1 || executor.getMaxRunners() > 4096
                || count > executor.getMaxRunners()) {
            throw invalid(qualifier, "max-runners", executor.getMaxRunners(), count, count);
        }
        slots = actualSlots > 0 ? actualSlots : configuredSlots();
        long totalSlots = Math.multiplyExact(count, slots);
        if (executor.getMaxTotalPartitions() < 1 || totalSlots > executor.getMaxTotalPartitions()) {
            throw invalid(qualifier, "max-total-partitions", executor.getMaxTotalPartitions(), totalSlots, count);
        }
        var identities = new ArrayList<Map.Entry<String, Integer>>();
        var batches = new ArrayList<Integer>();
        if (scope == ExecutorScope.SOURCE) {
            identities.add(new AbstractMap.SimpleImmutableEntry<>(null, null));
            batches.add(ordered.values().stream().mapToInt(GroupSpec::batch).max().orElseThrow());
        } else {
            for (GroupSpec group : ordered.values()) {
                int repeats = scope == ExecutorScope.GROUP ? 1 : group.count();
                for (int partition = 0; partition < repeats; partition++) {
                    identities.add(new AbstractMap.SimpleImmutableEntry<>(group.name(),
                            scope == ExecutorScope.STREAM ? partition : null));
                    batches.add(group.batch());
                }
            }
        }
        int[] batchMinimum = batches.stream().mapToInt(Integer::intValue).toArray();
        int[] singleMinimum = new int[batches.size()];
        Arrays.fill(singleMinimum, 1);
        var local = config.getLocalConsumer();
        int[] records = allocate(qualifier, "max-in-flight-records", local.getMaxInFlightRecords(), batchMinimum);
        // source 保留按实际解码 Task 数判断的容量合同，新隔离模式必须保证每域能处理一批独立键。
        int[] taskMinimum = scope == ExecutorScope.SOURCE ? singleMinimum : batchMinimum;
        int[] tasks = allocate(qualifier, "max-in-flight-tasks", local.getMaxInFlightTasks(), taskMinimum);
        int[] attempts = allocate(qualifier, "max-pending-commit-attempts", local.getMaxPendingCommitAttempts(), singleMinimum);
        int[] commits = allocate(qualifier, "max-pending-commit-records", local.getMaxPendingCommitRecords(), batchMinimum);
        int[] retries = allocate(qualifier, "max-in-flight-retries", local.getMaxInFlightRetries(), singleMinimum);
        int[] unordered = allocate(qualifier, "max-pending-unordered-retries", local.getMaxPendingUnorderedRetries(), batchMinimum);
        // source 未在读前预留后继位置，整批路由恢复必须仍能承接全部已取得的 raw 责任。
        int[] routeMinimum = scope == ExecutorScope.SOURCE ? records : batchMinimum;
        int[] blocked = allocate(qualifier, "max-route-blocked-records", local.getMaxRouteBlockedRecords(), routeMinimum);
        var specs = new ArrayList<DomainSpec>();
        for (int id = 0; id < identities.size(); id++) {
            var identity = identities.get(id);
            specs.add(new DomainSpec(id, identity.getKey(), identity.getValue(), new Quota(
                    records[id], tasks[id], attempts[id], commits[id], retries[id], unordered[id], blocked[id])));
        }
        domains = List.copyOf(specs);
    }

    /**
     * 业务作用：把 JVM 本地槽数按底层 Runner 相同规则归一化，防止预检低估启动资源。
     * 参数说明: 无。
     * @return 一到二的三十次幂之间的二次幂槽数；越界配置拒绝
     */
    static int configuredSlots() {
        int requested = Math.max(1, Integer.getInteger("nasa.partition.partitions", Runtime.getRuntime().availableProcessors() << 1));
        if (requested > (1 << 30)) throw new IllegalArgumentException("nasa.partition.partitions exceeds capacity");
        return requested == 1 ? 1 : Integer.highestOneBit(requested - 1) << 1;
    }

    /**
     * 业务作用：为每域保留最低批次再均分余量，防止繁忙域占用其它域的保证份额。
     * @param qualifier 源名称
     * @param resource 配置键
     * @param total 源级总额
     * @param minimum 各域最低需求
     * @return 与域顺序对应且合计等于总额的份额；不足时拒绝
     */
    private int[] allocate(String qualifier, String resource, int total, int[] minimum) {
        long required = Arrays.stream(minimum).asLongStream().sum();
        if (total < 1 || required > total) throw invalid(qualifier, resource, total, required, minimum.length);
        long spare = total - required;
        int[] quotas = new int[minimum.length];
        for (int i = 0; i < quotas.length; i++) {
            quotas[i] = Math.toIntExact(minimum[i] + spare / quotas.length + (i < spare % quotas.length ? 1 : 0));
        }
        return quotas;
    }

    /**
     * 业务作用：提供可定位的配置拒绝信息，避免节点在不完整预算下开放消费。
     * @param qualifier 源名称
     * @param resource 资源配置键
     * @param total 配置总额
     * @param required 最低需求
     * @param count 完整域数量
     * @return 不包含消息数据的配置异常
     */
    private IllegalArgumentException invalid(String qualifier, String resource, long total, long required, long count) {
        return new IllegalArgumentException("partition executor qualifier=" + qualifier + " scope=" + scope
                + " resource=" + resource + " configured=" + total + " required=" + required + " domains=" + count);
    }

    /**
     * 业务作用：根据物理来源查找冻结域，拒绝未声明组或越界分区。
     * @param group 逻辑组名
     * @param partition 组内物理编号
     * @return 不随 claim 生命周期改变的域编号
     */
    int domain(String group, int partition) {
        GroupSpec physical = groups.get(group);
        if (physical == null || partition < 0 || partition >= physical.count()) throw new IllegalStateException("unknown physical execution source");
        if (scope == ExecutorScope.SOURCE) return 0;
        for (DomainSpec domain : domains) {
            if (domain.group().equals(group) && (domain.partition() == null || domain.partition() == partition)) return domain.id();
        }
        throw new IllegalStateException("missing execution domain");
    }
}
