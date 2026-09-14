package io.github.nasaruntime.redis.cache.redis.job;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 业务作用：集中承载 RedisJob 的分片、租约、背压、Fanout 和保留参数。
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "nasa.redis.job")
public class RedisJobProperties {

    /**
     * 总开关。关闭时不装配调度器，注解方法也不会被登记，等同于本组件不存在。
     */
    private boolean enabled;

    /**
     * 任务隔离与 Redis Cluster hash tag 路由前缀。同一 Redis 上不同命名空间的任务互不可见。
     * 已写入数据后改名等同于启用一套全新任务集，旧数据不再被任何扫描器读取。
     */
    private String namespace = "default";

    /**
     * 派发消息携带的框架协议代次。执行器读到高于自身支持范围的消息时拒绝启动 Handler 并保留该 Run
     * 等待兼容节点，用于混合版本滚动发布期间避免按错误语义执行。
     */
    private int protocolVersion = 1;

    /**
     * 调度分片数，决定任务名映射到哪个 hash tag。
     * 路由按当前值实时计算，因此**已有数据时不可改变**：改动会让存量任务的键留在旧分片，
     * 而调度到新分片查找，表现为任务集体失联。需要调整时新建命名空间迁移。
     */
    private int shardCount = 64;

    /**
     * Fanout 固定桶数，决定 fanoutId 定址到哪个 hash tag。与分片数同理，已有数据时不可改变。
     */
    private int fanoutBucketCount = 32;

    /**
     * 单次能力快照最多选入的执行器数，同时限制一次 Fanout 的分片总数。
     */
    private int fanoutMaxMembers = 512;

    /**
     * 整批 Fanout 参数的字节上限，防止根任务把 Redis 当作大对象存储。
     * 必须不小于单分片上限 {@code maxParameterBytes}，否则任何分片都无法通过校验。
     */
    private int fanoutMaxTotalParameterBytes = 16 * 1024 * 1024;

    /**
     * 从根 intent 建立到桶内提交完成的截止时间。超时仍未提交的批次由看门狗作废，
     * 保证半完成批次不会被任何执行器看到。必须小于 {@code fanoutMaxWaitMs}。
     */
    private long fanoutCreateTimeoutMs = 60_000L;

    /**
     * 根任务等待全部分片进入终态的上限，超时按失败策略收敛。
     * 必须大于 {@code fanoutCreateTimeoutMs} 且小于 {@code registryGcGraceMs}，
     * 否则仍被等待的目标节点身份可能先被注册表回收。
     */
    private long fanoutMaxWaitMs = 1_800_000L;

    /**
     * 单个分片因真实故障可以建立的 assignment 上限。容量路由不消耗这份故障额度，
     * 但以该值减一作为单轮容量迁移突发上限，并在静默窗口后恢复有限探测。
     */
    private int fanoutMaxAssignments = 5;

    /**
     * 单段 Lua 一次投递的分片数，限制脚本占用 Redis 单线程的时长。
     * 不得大于 {@code fanoutMaxMembers}；调大会让同 master 上其它命令的排队时间变长。
     */
    private int fanoutDeliveryBatchSize = 64;

    /**
     * 已确认接收但迟迟没有启动时的普通唤醒次数上限。达到上限后先复验当前目标是否仍在兼容存活快照：
     * 仍存活时进入独立容量窗口，快照不可读时继续等待，只有确认目标离开才进入失败策略。
     */
    private int readyMaxWakeups = 3;

    /**
     * Fanout shard 在当前目标节点连续等待本地执行容量的时间窗口。
     * 窗口内保留当前 assignment；超过后只在存在其它兼容节点时尝试有界容量路由，
     * 没有候选、能力快照暂时不可读或路由处于静默期时继续原地等待。
     */
    private long fanoutCapacityWaitMs = 10_000L;

    /**
     * 把一个节点整体降级为 FANOUT_UNREADY 所需的失联证据条数，且证据必须来自不同的 Fanout。
     * 必须大于 1：单次抖动就摘掉健康节点会让后续快照的成员数来回震荡。
     */
    private int nodeUnreadyEvidenceCount = 3;

    /**
     * Fanout 终态记录的保留期。到期后分片、参数块与执行认领一并回收，
     * 同一 {@code executionKey} 的框架去重证据随之结束，更长的去重需求由业务自己的存储承担。
     */
    private long fanoutRetentionMs = 604_800_000L;

    /**
     * 单轮回收的分片数上限，限制清理脚本的单次工作量，避免一次删光整批阻塞 Redis。
     */
    private int fanoutCleanupBatchSize = 100;

    /**
     * 定向通知使用分片 Pub/Sub 还是普通广播。
     * SHARDED 只在目标 slot 内传播，需要 Redis 7+；BROADCAST 在 Cluster 上会把每条通知
     * 经集群总线扩散到全部节点，节点越多放大越明显，仅作 Redis 6.2 的降级选项。
     */
    private RedisJobPubSubMode pubsubMode = RedisJobPubSubMode.SHARDED;

    /**
     * 有到期成员时的最短扫描间隔，决定各持久索引的最快恢复速度。
     */
    private long minScanIntervalMs = 50L;

    /**
     * 索引空闲时的最长退避。各索引还会按自身业务期限进一步压低这个上界
     * （例如接收回执按 fanoutReceiptTimeoutMs、租约按 leaseMs/3）。
     * 调大省资源，代价是拉长「其它节点写入的近期截止点」被本节点发现的时延。
     */
    private long maxScanIntervalMs = 30_000L;

    /**
     * 判定误触发之前允许的 Redis 往返延迟，避免把正常网络抖动当成错过了调度时刻。
     */
    private long scheduleRttAllowanceMs = 100L;

    /**
     * 逻辑时刻被其它扫描器抢先推进后，单轮重新计算候选时刻的次数上限。
     */
    private int needRecomputeMaxRetries = 3;

    /**
     * 单轮索引扫描取回的候选数量上限。
     */
    private int scanBatchSize = 100;

    /**
     * Dispatch Stream 的消费组名。同一命名空间内所有执行器必须使用同一个值，
     * 不一致会让同一条派发消息被多个消费组各领取一次。
     */
    private String dispatchGroup = "redis-job-executor";

    /**
     * QUEUED Run 的兜底重投窗口。Stream 消息被裁剪、消费组丢失或写入回滚后靠它自愈。
     * 必须至少为 {@code leaseMs} 的两倍，否则兜底重投会与正常执行窗口重叠。
     */
    private long visibilityTimeoutMs = 60_000L;

    /**
     * 一个 Run 连续重建派发消息的次数阈值。超过后置为 NO_CAPABLE_EXECUTOR，按最长扫描间隔降频并告警，
     * 但保留低频重建入口；兼容执行器成功取得执行权时计数归零。
     */
    private int maxDispatchAttempts = 20;

    /**
     * 本执行器登记到注册表的总容量，供路由提示与观测使用。
     */
    private int executorCapacity = 128;

    /**
     * 普通任务中单个 Worker 在本进程内同时运行的 Handler 上限；Fanout 通道把同一个值作为
     * 全部 Fanout Worker 共享的独立总上限。该值不得大于 {@code executorCapacity}。
     */
    private int handlerCapacity = 8;

    /**
     * attempt 租约时长。执行器失联后须等租约到期，再由恢复扫描取得新执行权；
     * 实际恢复延迟还受扫描间隔、Redis 可用性与新执行器容量影响。
     * 必须覆盖两个续期周期加上允许的进程停顿，否则正常运行的任务会被误判失权重跑。
     */
    private long leaseMs = 30_000L;

    /**
     * 续期周期。缩短可以更早发现失权，代价是控制面调用量上升。
     */
    private long leaseRenewMs = 10_000L;

    /**
     * 计算本地保守持权截止点时扣除的续期往返预算，保证本地判定失权早于 Redis 侧租约到期。
     */
    private long renewRttAllowanceMs = 1_000L;

    /**
     * 计算本地保守持权截止点时扣除的时钟偏差预算。
     * 与 {@code renewRttAllowanceMs} 之和必须小于 {@code leaseMs} 的三分之一。
     */
    private long clockDriftAllowanceMs = 1_000L;

    /**
     * Handler 协作式取消阈值、重试退避和首次有界停机等待的全局上界；
     * callback 与 close 的最终资源等待没有由此字段限定的截止时间。
     * 到点后请求取消但不强制中断业务线程；Handler 必须在安全点检查信号并主动返回。
     */
    private long maxRunDurationMs = 3_600_000L;

    /**
     * 单个串行任务的等待队列长度上限，超限按溢出策略丢弃并告警。
     */
    private int maxSerialBacklog = 1_000;

    /**
     * 串行队列超限时丢弃队首最旧项还是拒绝最新触发。
     * 对账清算类任务通常最新的逻辑时刻更有价值，默认丢最旧。
     */
    private RedisJobSerialOverflowPolicy serialOverflowPolicy = RedisJobSerialOverflowPolicy.SKIP_OLDEST;

    /**
     * 执行器心跳周期，也是启动后健康状态收敛为 UP 的最坏等待时间。
     */
    private long heartbeatMs = 10_000L;

    /**
     * 心跳失效期限，至少覆盖两个心跳周期，避免一次抖动就把存活节点判为过期。
     */
    private long executorExpireMs = 30_000L;

    /**
     * 过期执行器身份的回收宽限。必须大于 {@code fanoutMaxWaitMs}，
     * 否则可能回收掉仍被某个 Fanout 等待的稳定节点身份，让该批次再也等不到目标。
     */
    private long registryGcGraceMs = 3_600_000L;

    /**
     * PEL 接管要求的最小空闲时间。必须大于扫描间隔且小于可见性窗口：
     * 太小会和正常的领取路径抢同一条消息，太大则消费者退出后的接管过于迟缓。
     */
    private long xautoclaimMinIdleMs = 30_000L;

    /**
     * 终态 Run 记录的保留时长，由终态转移的同一段脚本直接设为键 TTL。
     * 它同时是确定性 runId 与手工 requestId 的去重窗口：保留期一过，同一 requestId 会再次执行。
     */
    private long runRetentionMs = 604_800_000L;

    /**
     * 预留的完成事件保留期；当前 Completion Stream 由近似长度上限裁剪，实现不读取此值。
     */
    private long completionRetentionMs = 604_800_000L;

    /**
     * 删除定义后墓碑的保留期。必须长于允许的最长滚动发布窗口，
     * 否则尚未下线的旧节点会把已删除的定义重新登记回来。
     */
    private long tombstoneRetentionMs = 604_800_000L;

    /**
     * 单个 Run 或单个 Fanout 分片的参数字节上限，不得大于整批上限。
     */
    private int maxParameterBytes = 65_536;

    /**
     * 持久化结果摘要的 UTF-8 字节上限，按字节边界截断；完整业务日志不进 Run 记录。
     */
    private int maxResultSummaryBytes = 4_096;

    /**
     * CATCH_UP 的补偿总量上限，不是每轮上限。更早的逻辑时刻会被一次性跳过，
     * 防止长时间停机后瞬间生成大量 Run。
     */
    private int maxCatchUpRuns = 50;

    /**
     * 补偿窗口长度，早于该窗口的逻辑时刻一律不再补偿。
     */
    private long maxCatchUpWindowMs = 3_600_000L;

    /**
     * 参与租约安全关系计算的最大进程停顿预算，用于确定 {@code leaseMs} 的下界。
     */
    private long maxToleratedGcPauseMs = 1_000L;

    /**
     * 跨语言线协议的安全配置分组。
     */
    private Wire wire = new Wire();

    /**
     * 执行器所属应用名，写入注册表并进入 Fanout 快照的成员描述。
     */
    private String applicationName = "application";

    /**
     * 跨重启保持稳定的节点身份，Fanout 的 STRICT_SNAPSHOT 绑定与 inbox 命名都依赖它。
     * 留空时回退到主机名；容器平台需要自行确认该主机名在重启后是否仍然稳定。
     */
    private String instanceIdentity = "";

    /**
     * 业务作用：在启动任务线程和写入 Redis 前校验全部安全边界及时间关系。
     *
     * @return 当前配置，便于构造器链式使用。
     */
    public RedisJobProperties validate() {
        RedisJobNames.requireName(namespace, "namespace");
        if (protocolVersion <= 0 || shardCount <= 0 || fanoutBucketCount <= 0) {
            throw new IllegalArgumentException("protocolVersion and shard counts must be greater than zero");
        }
        if (fanoutMaxMembers <= 0 || fanoutDeliveryBatchSize <= 0
                || fanoutDeliveryBatchSize > fanoutMaxMembers || fanoutMaxAssignments <= 0) {
            throw new IllegalArgumentException("fanout member, batch and assignment limits are invalid");
        }
        if (fanoutMaxTotalParameterBytes <= 0 || maxParameterBytes <= 0
                || maxParameterBytes > fanoutMaxTotalParameterBytes) {
            throw new IllegalArgumentException("fanout parameter byte limits are invalid");
        }
        if (readyMaxWakeups < 0 || fanoutCapacityWaitMs <= 0
                || nodeUnreadyEvidenceCount <= 1 || fanoutCleanupBatchSize <= 0) {
            throw new IllegalArgumentException("fanout retry and cleanup limits are invalid");
        }
        if (pubsubMode == null) throw new IllegalArgumentException("pubsubMode must not be null");
        if (fanoutCreateTimeoutMs <= 0 || fanoutMaxWaitMs <= fanoutCreateTimeoutMs
                || fanoutRetentionMs <= 0 || registryGcGraceMs <= fanoutMaxWaitMs) {
            throw new IllegalArgumentException("fanout timeout and retention relationship is invalid");
        }
        if (minScanIntervalMs <= 0 || maxScanIntervalMs < minScanIntervalMs || scanBatchSize <= 0
                || scheduleRttAllowanceMs < 0 || needRecomputeMaxRetries <= 0) {
            throw new IllegalArgumentException("scan intervals and batch size are invalid");
        }
        if (maxToleratedGcPauseMs < 0 || leaseMs <= 0 || leaseRenewMs <= 0
                || leaseMs <= 2 * leaseRenewMs + maxToleratedGcPauseMs) {
            throw new IllegalArgumentException("leaseMs must cover two renew intervals and tolerated pause");
        }
        if (renewRttAllowanceMs < 0 || clockDriftAllowanceMs < 0
                || renewRttAllowanceMs + clockDriftAllowanceMs >= leaseMs / 3) {
            throw new IllegalArgumentException("renew allowances must be smaller than one third of leaseMs");
        }
        if (visibilityTimeoutMs < 2 * leaseMs) {
            throw new IllegalArgumentException("visibilityTimeoutMs must be at least twice leaseMs");
        }
        if (executorCapacity <= 0 || handlerCapacity <= 0 || handlerCapacity > executorCapacity) {
            throw new IllegalArgumentException("executor capacity limits are invalid");
        }
        if (heartbeatMs <= 0 || executorExpireMs < 2 * heartbeatMs) {
            throw new IllegalArgumentException("executorExpireMs must cover at least two heartbeats");
        }
        if (xautoclaimMinIdleMs <= minScanIntervalMs || xautoclaimMinIdleMs >= visibilityTimeoutMs) {
            throw new IllegalArgumentException("xautoclaimMinIdleMs must be between scan and visibility intervals");
        }
        if (maxRunDurationMs <= 0 || runRetentionMs <= 0 || completionRetentionMs <= 0
                || maxDispatchAttempts <= 0 || maxSerialBacklog <= 0 || maxResultSummaryBytes <= 0
                || maxCatchUpRuns <= 0 || maxCatchUpWindowMs <= 0 || tombstoneRetentionMs <= 0) {
            throw new IllegalArgumentException("run limits and retention must be greater than zero");
        }
        if (serialOverflowPolicy == null) throw new IllegalArgumentException("serialOverflowPolicy must not be null");
        if (wire == null || wire.json == null) {
            throw new IllegalArgumentException("nasa.redis.job.wire.json must be configured");
        }
        if (wire.json.defaultTyping) {
            throw new IllegalArgumentException("nasa.redis.job.wire.json.default-typing must remain false");
        }
        return this;
    }

    /**
     * 业务作用：承载跨语言线协议的安全配置分组。
     */
    @Getter
    @Setter
    public static final class Wire {
        /**
         * JSON 线协议分组，承载跨语言解码的安全门禁。
         */
        private Json json = new Json();
    }

    /**
     * 业务作用：把 Jackson Default Typing 暴露为只能保持 false 的启动门禁。
     */
    @Getter
    @Setter
    public static final class Json {
        /**
         * 只能保持 false 的固定安全值。置为 true 会让任务参数带上 JVM 类型元数据，
         * Go 与 Rust 无法按同一 Schema 解码，且反序列化会依据消息内容加载类型；
         * 因此配置为 true 时调度器直接拒绝启动，不做降级。
         */
        private boolean defaultTyping;
    }
}
