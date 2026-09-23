package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.redis.cache.redis.NasaLettuceConfig.Stream;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 业务作用：承载分区认领、本地执行、PEL 恢复和确认的配置，保持各来源的容量与权威边界一致。
 * <p>
 * 这里只放分区独有的运行参数, 拉取相关 (pollTimeout / batchSize) 仍走 {@link Stream} 全局配置。
 * {@code executor.scope} 默认 source；group/stream 按完整拓扑拆分 {@code local-consumer} 的源级数量总额，
 * 每域保留最低批次需求后均分余量，域间不借用份额。该设置不改变 Redis 键布局，也不影响普通 PROXY 订阅。
 * <p>
 * <b>命名空间约定</b>: {@link #defaultGroup} 是所有分区组的命名空间前缀, 也是默认共享组本身的 group 名。
 * <ul>
 *   <li>默认共享组: stream = {@code {defaultGroup}:0..count-1}, consumer group 名 = {@code {defaultGroup}}</li>
 *   <li>隔离组 (yml 中以 {@code groups.<逻辑名>} 配置): stream = {@code {defaultGroup}:<逻辑名>:0..count-1},
 *       consumer group 名 = {@code {defaultGroup}:<逻辑名>}</li>
 * </ul>
 * 业务在 yml 和代码里只用 "逻辑名" (短名), 实际 Redis key 由框架在 {@link #defaultGroup} 命名空间下自动拼接。
 * 上述布局适用于单机 AUTO；Cluster 或显式 COLOCATED 使用共享 hash tag，使分区锁与 Stream 位于同一 slot。
 * <p>
 * yml 配置示例:
 * <pre>
 * nasa:
 *   redis:
 *     properties:
 *       primary:
 *         stream:
 *           pollTimeout: 500          # 与普通 stream 消费共享
 *           batchSize: 100            # 与普通 stream 消费共享
 *           partition:
 *             enabled: true           # 分区消费总开关
 *             default-group: SINGLE-CONSUME  # 命名空间前缀, 也是默认共享组的 group 名
 *             count: 64               # 默认共享组的分区数
 *             executor:
 *               scope: source         # group 按逻辑组、stream 按物理 Stream 建立独立域
 *               max-runners: 256
 *               max-total-partitions: 4096  # Runner 本地槽数总和，与 Redis count 不同
 *             rebalance-ms: 3000
 *             min-idle-ms: 30000
 *             holds-check-interval-ms: 5000
 *             drain-timeout-ms: 5000
 *             groups:                 # 隔离组配置, key 用业务逻辑短名 (不要带 default-group 前缀)
 *               contract:settlement:  # → 实际 stream = SINGLE-CONSUME:contract:settlement:0..63
 *                 count: 64
 *                 min-idle-ms: 60000
 *               spot:settlement:      # → 实际 stream = SINGLE-CONSUME:spot:settlement:0..31
 *                 count: 32
 *                 batch-size: 200
 * </pre>
 */
@Getter
@Setter
public class RedisPartitionProperties {

    /* 分区消费总开关. false → RedisPartition.init 静默返回, 不启 rebalance / consumer task, 节省启动开销 */
    private boolean enabled = false;
    /**
     * 命名空间前缀, 同时也是默认共享组的 stream/consumer group 名。
     * 默认 SINGLE-CONSUME (全大写做命名空间标识, 与业务 key 视觉上分离)。
     * <p>
     * 实际 Redis key 命名规则:
     * <ul>
     *   <li>默认共享组 stream: {@code {defaultGroup}:0..count-1}, consumer group 名 = {defaultGroup}</li>
     *   <li>隔离组 stream: {@code {defaultGroup}:{逻辑名}:0..count-1}, consumer group 名 = {defaultGroup}:{逻辑名}</li>
     *   <li>分区锁 key: {@code DISTRIBUTED-LOCK:{stream前缀}:lock:{partition}}</li>
     * </ul>
     */
    private String defaultGroup = "SINGLE-CONSUME";
    /* 默认共享组的分区数. 业务调 RedisPartition.init() 不传 count 时用这个 */
    private int count = 64;
    /* 再平衡周期 ms；每节点持有上限为 max(1, ceil(count / aliveNodes))，实际认领仍须取得分区锁。 */
    private long rebalanceMs = 3_000;
    /* XAUTOCLAIM 的最小 idle 时间 ms；它只决定接管资格，当前 consumer 的耗时业务仍须经过本地执行权复验。 */
    private long minIdleMs = 30_000;
    /* holds() 自检最小间隔 ms (防长 GC / 业务长跑后锁丢失). 默认 5s, 远小于 lease=30s */
    private long holdsCheckIntervalMs = 5_000;
    /* Claim 排干预算 ms；耗尽后撤销来源权威并保留未决 PEL，迟到确认由 fencing 拒绝，最终停机仍等待本地资源实际结束。 */
    private long drainTimeoutMs = 5_000;
    /* Redis Cluster 自动选择同 slot 布局；单机保持现有 key，显式 COLOCATED 用于已完成迁移的部署。 */
    private PartitionKeyLayout keyLayout = PartitionKeyLayout.AUTO;
    /* 本地 Partition 执行、确认与 PEL 恢复的硬容量。 */
    private final LocalConsumer localConsumer = new LocalConsumer();
    /** 业务作用：选择本地执行域及完整拓扑的资源上限，不改变 Redis 物理布局。 */
    private final Executor executor = new Executor();

    /** 业务作用：决定同一 Redis 源内哪些物理来源共享本地队列与容量。 */
    public enum ExecutorScope {
        /** 当前源共用一个执行域。 */
        SOURCE,
        /** 每个逻辑组使用一个执行域。 */
        GROUP,
        /** 每个物理 Stream 使用一个执行域。 */
        STREAM
    }

    /** 业务作用：在消费启动前限制 Runner 总数和本地槽数，防止随拓扑无界扩张。 */
    @Getter
    @Setter
    public static class Executor {
        /** 本地资源共享范围；仅 SOURCE 允许 listener 提供显式 Runner。 */
        private ExecutorScope scope = ExecutorScope.SOURCE;
        /** 完整拓扑的 Runner 数量上限，允许 1..4096；未订阅的配置组仍计入 GROUP/STREAM 预算。 */
        private int maxRunners = 256;
        /** 全部 Runner 本地槽数总上限，槽数按 JVM 配置归一化后计入，超出时拒绝消费启动。 */
        private int maxTotalPartitions = 4096;
    }
    /**
     * 隔离组配置表。key = 业务逻辑短名 (不要带 defaultGroup 前缀, 框架会自动拼)。
     * 例如 key="contract:settlement" → 实际 stream 前缀 = "{defaultGroup}:contract:settlement"。
     * <p>
     * 默认共享组无需在这里配置，它的参数由 RedisPartitionProperties 顶层字段决定。
     */
    private final Map<String, PartitionGroup> groups = new LinkedHashMap<>();

    /**
     * 业务作用：声明物理 Stream 与分区锁的键布局，确保 Cluster 下的 holder-fenced Lua 不跨 slot。
     */
    public enum PartitionKeyLayout {
        /** 按连接模式选择：单机使用现有布局，Cluster 使用同 slot 布局。 */
        AUTO,
        /** 强制使用带共享 hash tag 的同 slot 布局。 */
        COLOCATED
    }

    /**
     * 业务作用：声明无法解析消费路由时的可靠性策略。
     * 仅允许阻断当前来源并保留 PEL，避免无法证明顺序归属的消息被丢弃或降级为非保序。
     */
    public enum PoisonPolicy {
        BLOCK_CLAIM
    }

    /**
     * 业务作用：为本地 Partition bridge、确认不确定态和 PEL 恢复设置统一硬容量与退避边界。
     * 容量不足时暂停新读取，已经进入 PEL 的坐标不得被驱逐。
     */
    @Getter
    @Setter
    public static class LocalConsumer {
        /** 业务作用：拒绝不再支持的普通来源账本配置，避免旧配置被静默忽略。@param value 旧配置值；返回: 总是抛出迁移诊断。 */
        @Deprecated public void setMaxProxyLedgerRecords(int value) { throw removed("max-proxy-ledger-records"); }
        /** 业务作用：拒绝不再支持的普通来源字段配置。@param value 旧配置值；返回: 总是抛出迁移诊断。 */
        @Deprecated public void setMaxProxyFieldsPerRecord(int value) { throw removed("max-proxy-fields-per-record"); }
        /** 业务作用：拒绝不再支持的普通来源接管配置。@param value 旧配置值；返回: 总是抛出迁移诊断。 */
        @Deprecated public void setProxyPendingMinIdleMs(long value) { throw removed("proxy-pending-min-idle-ms"); }
        /** 业务作用：提供明确的旧配置错误，不建立旧消费路径。@param key 已删除配置键 @return 不支持配置异常 */
        private static IllegalArgumentException removed(String key) {
            return new IllegalArgumentException("BOTH is unsupported; remove stream.partition.local-consumer." + key
                    + " and select PROXY or PARTITION explicitly");
        }


        private int maxInFlightRecords = 8_192;
        private int maxInFlightTasks = 4_096;
        private int maxPendingCommitAttempts = 2_048;
        private int maxPendingCommitRecords = 8_192;
        private int maxInFlightRetries = 256;
        private int maxBlockedKeysPerClaim = 4_096;
        private int maxDeferredIdsPerKey = 1_024;
        private int maxRouteBlockedRecords = 8_192;
        private int maxPendingUnorderedRetries = 8_192;
        private long retryInitialDelayMs = 1_000;
        private long retryMaxDelayMs = 30_000;
        private long ackReconcileInitialDelayMs = 200;
        private long ackReconcileMaxDelayMs = 5_000;
        private PoisonPolicy poisonPolicy = PoisonPolicy.BLOCK_CLAIM;
    }

    /**
     * 业务作用：承载单个分区组的覆盖配置，允许隔离组使用与默认组不同的参数，
     * 例如高频低耗时的 settlement 用大 batchSize, 慢任务的某 topic 用更长 minIdleMs。
     */
    @Getter
    @Setter
    public static class PartitionGroup {

        /* 分区数，必填；发布侧按稳定键及其类型归一化后取模，生产和消费实例必须采用相同数量。 */
        private int count;
        /* 覆盖父级 rebalanceMs；null 时使用 RedisPartitionProperties.rebalanceMs。 */
        private Long rebalanceMs;
        /* 覆盖父级 minIdleMs, null = 用父级 */
        private Long minIdleMs;
        /* 覆盖父级 holdsCheckIntervalMs, null = 用父级 */
        private Long holdsCheckIntervalMs;
        /* 覆盖父级 drainTimeoutMs, null = 用父级 */
        private Long drainTimeoutMs;
        /* 覆盖父级 Stream.batchSize, null = 用 Stream 全局 */
        private Integer batchSize;
        /* 覆盖父级 Stream.pollTimeout, null = 用 Stream 全局 */
        private Integer pollTimeout;
        /**
         * 该隔离组接收哪些业务 topic 的消息。
         * <p>
         * 不配 (空列表) → 默认 = [logical name]: yml key 同时作为 topic 名 (1 隔离组 1 topic 简化场景)。
         * 配了非空 → 框架对每个 topic 调 {@code RedisPartition.isolate(topic, logicalName, count)},
         * 让多个 topic 共享同一个隔离组的 stream/lock 命名空间。
         * <p>
         * 例如 yml:
         * <pre>
         * groups:
         *   high-freq-settle:                        # logical name
         *     count: 128
         *     topics: [contract:settlement, spot:settlement]   # 两个 topic 共享 high-freq-settle 组
         * </pre>
         */
        private final List<String> topics = new ArrayList<>();
    }

}
