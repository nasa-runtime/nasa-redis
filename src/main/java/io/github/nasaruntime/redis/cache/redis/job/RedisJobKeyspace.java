package io.github.nasaruntime.redis.cache.redis.job;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 业务作用：集中生成 Redis Cluster hash tag 与任务键，保证每段 Lua 的键位于同一 slot。
 */
public final class RedisJobKeyspace {

    private final String qualifier;
    private final String namespace;
    private final int shardCount;
    private final int fanoutBucketCount;
    private final String[] shardPrefixes;
    private final String[] scheduleKeys;
    private final String[] jobsKeys;
    private final String[] controlKeys;
    private final String[] visibleKeys;
    private final String[] leaseKeys;
    private final String[] waitingKeys;
    private final String[] runningKeys;
    private final String[] fenceKeys;
    private final String[] completionKeys;
    private final String registryPrefix;
    private final String[] fanoutPrefixes;
    private final String[] fanoutRootsKeys;
    private final String[] fanoutGcKeys;
    private final String[] fanoutReceiptKeys;
    private final String[] fanoutReadyKeys;
    private final String[] fanoutLeaseKeys;

    /**
     * 业务作用：建立不可变键路由器并冻结调度分片数与 Fanout 桶数。
     *
     * <p>全部键前缀都以 {@code <qualifier>:<namespace>} 开头。两个 qualifier 即使指向同一台 Redis、
     * 又用了相同 namespace，控制面键、Stream、执行器注册表与 Fanout inbox 也完全不相交；
     * 否则一个数据源的消费者会读到另一个数据源的派发消息并抢走执行权。
     *
     * @param qualifier         语言无关的 source id
     * @param namespace         调度命名空间
     * @param shardCount        调度分片数
     * @param fanoutBucketCount Fanout 桶数
     */
    public RedisJobKeyspace(String qualifier, String namespace, int shardCount, int fanoutBucketCount) {
        this.qualifier = RedisJobNames.requireName(qualifier, "qualifier");
        this.namespace = RedisJobNames.requireName(namespace, "namespace");
        // qualifier 内禁止分隔符, 否则 (a:b, c) 与 (a, b:c) 会拼出同一个 scope, 两套 Scheduler 的定义、
        // Run、注册表和 Fanout 全部重合, (qualifier, namespace, jobName) 的唯一身份就不成立。
        // 只约束 qualifier 即可让第一个冒号成为无歧义的分界点, namespace 仍可自由使用冒号分层。
        if (this.qualifier.indexOf(':') >= 0) {
            throw new IllegalArgumentException("RedisJob qualifier must not contain ':' : " + this.qualifier);
        }
        String scope = this.qualifier + ":" + this.namespace;
        if (shardCount <= 0) throw new IllegalArgumentException("shardCount must be greater than zero");
        if (fanoutBucketCount <= 0) throw new IllegalArgumentException("fanoutBucketCount must be greater than zero");
        this.shardCount = shardCount;
        this.fanoutBucketCount = fanoutBucketCount;
        this.shardPrefixes = new String[shardCount];
        this.scheduleKeys = new String[shardCount];
        this.jobsKeys = new String[shardCount];
        this.controlKeys = new String[shardCount];
        this.visibleKeys = new String[shardCount];
        this.leaseKeys = new String[shardCount];
        this.waitingKeys = new String[shardCount];
        this.runningKeys = new String[shardCount];
        this.fenceKeys = new String[shardCount];
        this.completionKeys = new String[shardCount];
        for (int shard = 0; shard < shardCount; shard++) {
            String prefix = "rjob:{" + scope + ":" + paddedIndex(shard) + "}:";
            shardPrefixes[shard] = prefix;
            scheduleKeys[shard] = prefix + "schedule";
            jobsKeys[shard] = prefix + "jobs";
            controlKeys[shard] = prefix + "control";
            visibleKeys[shard] = prefix + "visible";
            leaseKeys[shard] = prefix + "leases";
            waitingKeys[shard] = prefix + "waiting";
            runningKeys[shard] = prefix + "running";
            fenceKeys[shard] = prefix + "fences";
            completionKeys[shard] = prefix + "completion";
        }
        this.registryPrefix = "rjob:{" + scope + ":registry}:";
        this.fanoutPrefixes = new String[fanoutBucketCount];
        this.fanoutRootsKeys = new String[fanoutBucketCount];
        this.fanoutGcKeys = new String[fanoutBucketCount];
        this.fanoutReceiptKeys = new String[fanoutBucketCount];
        this.fanoutReadyKeys = new String[fanoutBucketCount];
        this.fanoutLeaseKeys = new String[fanoutBucketCount];
        for (int bucket = 0; bucket < fanoutBucketCount; bucket++) {
            String prefix = "rjob:{" + scope + ":fanout:" + paddedIndex(bucket) + "}:";
            fanoutPrefixes[bucket] = prefix;
            fanoutRootsKeys[bucket] = prefix + "roots";
            fanoutGcKeys[bucket] = prefix + "gc";
            fanoutReceiptKeys[bucket] = prefix + "receipts";
            fanoutReadyKeys[bucket] = prefix + "ready";
            fanoutLeaseKeys[bucket] = prefix + "lease";
        }
    }

    /**
     * 业务作用：读取本键空间绑定的语言无关 source id。 @return source id。
     */
    public String qualifier() {
        return qualifier;
    }

    /**
     * 业务作用：读取命名空间；不含 qualifier，供上下文与业务按纯命名空间使用。 @return 命名空间。
     */
    public String namespace() {
        return namespace;
    }

    /**
     * 业务作用：读取调度分片数。 @return 分片数。
     */
    public int shardCount() {
        return shardCount;
    }

    /**
     * 业务作用：读取 Fanout 桶数。 @return 桶数。
     */
    public int fanoutBucketCount() {
        return fanoutBucketCount;
    }

    /**
     * 业务作用：向需要处理运行时索引成员的同 slot 脚本提供权威分片前缀，避免脚本解析业务名称猜测键边界。
     *
     * @param shard 分片下标
     * @return 带 hash tag 的分片键前缀。
     */
    String shardKeyPrefix(int shard) {
        return shardPrefix(shard);
    }

    /**
     * 业务作用：向需要处理注册表索引成员的同 slot 脚本提供权威前缀，避免脚本从具体键名反推命名空间。
     *
     * @return 带 hash tag 的注册表键前缀。
     */
    String registryKeyPrefix() {
        return registryPrefix;
    }

    /**
     * 业务作用：生成 Fanout 分片的跨语言业务幂等键，是该值在产品代码中的唯一拼装入口。
     *
     * <p>executionKey 在通知重发、节点重分配和执行重试期间保持不变，业务用它做数据库唯一键或外部
     * idempotency key。qualifier 必须参与：两个数据源的同一分片不能算出同一个值。
     *
     * @param fanoutId Fanout 标识
     * @param seq      分片稳定序号
     * @return executionKey。
     */
    String executionKey(String fanoutId, long seq) {
        return qualifier + ":" + namespace + ":" + fanoutId + ":" + seq;
    }

    /**
     * 业务作用：给出承载不可变布局指纹的 marker 键。
     *
     * <p>放在 registry slot 而不是调度分片下：分片数本身就是被比对的对象之一，
     * marker 不能依赖一个尚未确认的布局来定位。
     *
     * <p>参数说明: 无。
     *
     * @return layout marker 键。
     */
    String layoutMarker() {
        return registryPrefix + "layout";
    }

    /**
     * 业务作用：给出承载"已删除、存量待收敛"任务名的分片级 SET 键。
     *
     * <p>删除动作 O(1) 登记，后台按批终态化 waitq 存量；tombstone 清理凭该 SET 判断收敛是否完成。
     *
     * @param shard 分片下标
     * @return reaping SET 键。
     */
    String reaping(int shard) {
        return shardPrefix(shard) + "reaping";
    }

    /**
     * 业务作用：按稳定 FNV-1a 哈希为首次注册任务选择调度分片。
     *
     * @param jobName 任务名
     * @return 分片下标。
     */
    public int scheduleShard(String jobName) {
        return Math.floorMod(stableHash(jobName), shardCount);
    }

    /**
     * 业务作用：按稳定标识把 Fanout 的全部记录定址到同一个固定桶。
     *
     * @param fanoutId Fanout 标识
     * @return 桶下标。
     */
    public int fanoutBucket(String fanoutId) {
        return Math.floorMod(stableHash(fanoutId), fanoutBucketCount);
    }

    /**
     * 业务作用：生成调度 ZSET 键。 @param shard 分片 @return Redis 键。
     */
    public String schedule(int shard) {
        return fixedShardKey(scheduleKeys, shard);
    }

    /**
     * 业务作用：生成任务枚举 SET 键。 @param shard 分片 @return Redis 键。
     */
    public String jobs(int shard) {
        return fixedShardKey(jobsKeys, shard);
    }

    /**
     * 业务作用：生成分片控制 HASH 键。 @param shard 分片 @return Redis 键。
     */
    public String control(int shard) {
        return fixedShardKey(controlKeys, shard);
    }

    /**
     * 业务作用：生成任务定义 HASH 键。 @param shard 分片 @param jobName 任务名 @return Redis 键。
     */
    public String job(int shard, String jobName) {
        return shardPrefix(shard) + "job:" + jobName;
    }

    /**
     * 业务作用：生成普通 Run HASH 键。 @param shard 分片 @param runId Run 标识 @return Redis 键。
     */
    public String run(int shard, String runId) {
        return shardPrefix(shard) + "run:" + runId;
    }

    /**
     * 业务作用：生成 Worker 派发 Stream 键。 @param shard 分片 @param workerName Worker 名 @return Redis 键。
     */
    public String dispatch(int shard, String workerName) {
        return dispatchByWorkerKey(shard, RedisJobIdentifiers.workerKey(workerName));
    }

    /**
     * 业务作用：使用任务定义已经缓存的 Worker 摘要生成派发键。 @param shard 分片 @param workerKey Worker 摘要 @return Redis 键。
     */
    String dispatchByWorkerKey(int shard, String workerKey) {
        return shardPrefix(shard) + "dispatch:" + Objects.requireNonNull(workerKey, "workerKey must not be null");
    }

    /**
     * 业务作用：生成统一可见性 ZSET 键。 @param shard 分片 @return Redis 键。
     */
    public String visible(int shard) {
        return fixedShardKey(visibleKeys, shard);
    }

    /**
     * 业务作用：生成租约 ZSET 键。 @param shard 分片 @return Redis 键。
     */
    public String leases(int shard) {
        return fixedShardKey(leaseKeys, shard);
    }

    /**
     * 业务作用：生成 Fanout 根等待 ZSET 键。 @param shard 分片 @return Redis 键。
     */
    public String waiting(int shard) {
        return fixedShardKey(waitingKeys, shard);
    }

    /**
     * 业务作用：生成任务单活槽 HASH 键。 @param shard 分片 @return Redis 键。
     */
    public String running(int shard) {
        return fixedShardKey(runningKeys, shard);
    }

    /**
     * 业务作用：生成 attempt fencing HASH 键。 @param shard 分片 @return Redis 键。
     */
    public String fences(int shard) {
        return fixedShardKey(fenceKeys, shard);
    }

    /**
     * 业务作用：生成串行等待 ZSET 键。 @param shard 分片 @param jobName 任务名 @return Redis 键。
     */
    public String waitq(int shard, String jobName) {
        return shardPrefix(shard) + "waitq:" + jobName;
    }

    /**
     * 业务作用：生成普通完成 Stream 键。 @param shard 分片 @return Redis 键。
     */
    public String completion(int shard) {
        return fixedShardKey(completionKeys, shard);
    }

    /**
     * 业务作用：生成执行器存活 ZSET 键。 @return Redis 键。
     */
    public String executors() {
        return registryPrefix + "executors";
    }

    /**
     * 业务作用：生成执行器元数据 HASH 键。 @param executorId 执行器标识 @return Redis 键。
     */
    public String executor(String executorId) {
        return registryPrefix + "executor:" + executorId;
    }

    /**
     * 业务作用：生成 Worker 能力 ZSET 键。 @param workerName Worker 名 @return Redis 键。
     */
    public String capability(String workerName) {
        return registryPrefix + "capability:" + workerName;
    }

    /**
     * 业务作用：生成 Worker 能力元数据 HASH 键。 @param workerName Worker 名 @return Redis 键。
     */
    public String capabilityMeta(String workerName) {
        return registryPrefix + "capability-meta:" + workerName;
    }

    /**
     * 业务作用：生成规范契约 HASH 键。 @param workerName Worker 名 @param revision 契约修订号 @return Redis 键。
     */
    public String contract(String workerName, long revision) {
        return registryPrefix + "contract:" + workerName + ":" + revision;
    }

    /**
     * 业务作用：生成 Worker 摘要绑定键。 @param workerName Worker 名 @return Redis 键。
     */
    public String workerKeyBinding(String workerName) {
        return registryPrefix + "worker-key:" + RedisJobIdentifiers.workerKey(workerName);
    }

    /**
     * 业务作用：生成稳定节点的跨 Fanout 失联证据 HASH。 @param nodeIdentity 节点身份 @return Redis 键。
     */
    public String fanoutEvidence(String nodeIdentity) {
        return registryPrefix + "fanout-evidence:" + nodeIdentity;
    }

    /**
     * 业务作用：生成注册表待回收执行器 ZSET 键。 @return Redis 键。
     */
    public String registryGc() {
        return registryPrefix + "gc";
    }

    /**
     * 业务作用：生成 Fanout root HASH 键。 @param fanoutId Fanout 标识 @return Redis 键。
     */
    public String fanoutRoot(String fanoutId) {
        return fanoutPrefix(fanoutId) + "root:" + fanoutId;
    }

    /**
     * 业务作用：生成 Fanout shard HASH 键。 @param fanoutId Fanout 标识 @param seq 分片序号 @return Redis 键。
     */
    public String fanoutShard(String fanoutId, long seq) {
        return fanoutPrefix(fanoutId) + "shard:" + fanoutId + ":" + seq;
    }

    /**
     * 业务作用：生成稳定节点 inbox Stream 键。 @param fanoutId Fanout 标识 @param nodeIdentity 节点身份 @return Redis 键。
     */
    public String fanoutInbox(String fanoutId, String nodeIdentity) {
        return fanoutPrefix(fanoutId) + "inbox:" + nodeIdentity;
    }

    /**
     * 业务作用：生成接收截止 ZSET 键。 @param fanoutId Fanout 标识 @return Redis 键。
     */
    public String fanoutReceipts(String fanoutId) {
        return fanoutPrefix(fanoutId) + "receipts";
    }

    /**
     * 业务作用：生成启动可见性 ZSET 键。 @param fanoutId Fanout 标识 @return Redis 键。
     */
    public String fanoutReady(String fanoutId) {
        return fanoutPrefix(fanoutId) + "ready";
    }

    /**
     * 业务作用：生成 Fanout 租约 ZSET 键。 @param fanoutId Fanout 标识 @return Redis 键。
     */
    public String fanoutLeases(String fanoutId) {
        return fanoutPrefix(fanoutId) + "lease";
    }

    /**
     * 业务作用：生成 Fanout 完成 Stream 键。 @param fanoutId Fanout 标识 @return Redis 键。
     */
    public String fanoutCompletion(String fanoutId) {
        return fanoutPrefix(fanoutId) + "completion";
    }

    /**
     * 业务作用：生成 Fanout 非终态看门狗 ZSET 键。 @param fanoutId Fanout 标识 @return Redis 键。
     */
    public String fanoutRoots(String fanoutId) {
        return fanoutPrefix(fanoutId) + "roots";
    }

    /**
     * 业务作用：生成 Fanout 清理 ZSET 键。 @param fanoutId Fanout 标识 @return Redis 键。
     */
    public String fanoutGc(String fanoutId) {
        return fanoutPrefix(fanoutId) + "gc";
    }

    /**
     * 业务作用：生成稳定节点的 Fanout 通知频道。 @param fanoutId Fanout 标识 @param nodeIdentity 节点身份 @return Pub/Sub 频道。
     */
    public String fanoutNotifyChannel(String fanoutId, String nodeIdentity) {
        return fanoutPrefix(fanoutId) + "notify:" + nodeIdentity;
    }

    /**
     * 业务作用：生成发起执行器的 Fanout 回执频道。 @param fanoutId Fanout 标识 @param executorId 执行器身份 @return Pub/Sub 频道。
     */
    public String fanoutReceiptChannel(String fanoutId, String executorId) {
        return fanoutPrefix(fanoutId) + "receipt:" + executorId;
    }

    /**
     * 业务作用：按固定桶生成发起执行器的回执频道。 @param bucket 桶下标 @param executorId 执行器身份 @return Pub/Sub 频道。
     */
    public String fanoutReceiptChannel(int bucket, String executorId) {
        return fanoutBucketPrefix(bucket) + "receipt:" + executorId;
    }

    /**
     * 业务作用：按固定桶生成本节点的 Fanout 通知频道，供启动时预先订阅。 @param bucket 桶下标 @param nodeIdentity 节点身份 @return Pub/Sub 频道。
     */
    public String fanoutNotifyChannel(int bucket, String nodeIdentity) {
        return fanoutBucketPrefix(bucket) + "notify:" + nodeIdentity;
    }

    /**
     * 业务作用：按固定桶生成 Fanout roots 看门狗索引。 @param bucket 桶下标 @return Redis 键。
     */
    public String fanoutRoots(int bucket) {
        return fixedFanoutKey(fanoutRootsKeys, bucket);
    }

    /**
     * 业务作用：按固定桶生成 Fanout gc 索引。 @param bucket 桶下标 @return Redis 键。
     */
    public String fanoutGc(int bucket) {
        return fixedFanoutKey(fanoutGcKeys, bucket);
    }

    /**
     * 业务作用：按固定桶生成 receipt 截止索引。 @param bucket 桶下标 @return Redis 键。
     */
    public String fanoutReceipts(int bucket) {
        return fixedFanoutKey(fanoutReceiptKeys, bucket);
    }

    /**
     * 业务作用：按固定桶生成 ready 截止索引。 @param bucket 桶下标 @return Redis 键。
     */
    public String fanoutReady(int bucket) {
        return fixedFanoutKey(fanoutReadyKeys, bucket);
    }

    /**
     * 业务作用：按固定桶生成 Fanout 租约索引。 @param bucket 桶下标 @return Redis 键。
     */
    public String fanoutLeases(int bucket) {
        return fixedFanoutKey(fanoutLeaseKeys, bucket);
    }

    /**
     * 业务作用：生成调度分片前缀并校验下标，防止把脚本键路由到意外 slot。
     *
     * @param shard 分片下标
     * @return 带 hash tag 的键前缀。
     */
    private String shardPrefix(int shard) {
        if (shard < 0 || shard >= shardCount) throw new IllegalArgumentException("invalid schedule shard: " + shard);
        return shardPrefixes[shard];
    }

    /**
     * 业务作用：读取预生成的固定分片键并统一校验下标。
     *
     * @param values 固定键表
     * @param shard  分片下标
     * @return Redis 键。
     */
    private String fixedShardKey(String[] values, int shard) {
        if (shard < 0 || shard >= shardCount) throw new IllegalArgumentException("invalid schedule shard: " + shard);
        return values[shard];
    }

    /**
     * 业务作用：读取预生成的固定 Fanout 桶键并统一校验下标。
     *
     * @param values 固定键表
     * @param bucket 桶下标
     * @return Redis 键。
     */
    private String fixedFanoutKey(String[] values, int bucket) {
        if (bucket < 0 || bucket >= fanoutBucketCount)
            throw new IllegalArgumentException("invalid fanout bucket: " + bucket);
        return values[bucket];
    }

    /**
     * 业务作用：生成 Fanout 桶同 slot 前缀。
     *
     * @param fanoutId Fanout 标识
     * @return 带 hash tag 的键前缀。
     */
    private String fanoutPrefix(String fanoutId) {
        Objects.requireNonNull(fanoutId, "fanoutId must not be null");
        return fanoutBucketPrefix(fanoutBucket(fanoutId));
    }

    /**
     * 业务作用：按固定桶下标生成 Fanout 同 slot 前缀。
     *
     * @param bucket 桶下标
     * @return 带 hash tag 的键前缀。
     */
    private String fanoutBucketPrefix(int bucket) {
        if (bucket < 0 || bucket >= fanoutBucketCount)
            throw new IllegalArgumentException("invalid fanout bucket: " + bucket);
        return fanoutPrefixes[bucket];
    }

    /**
     * 业务作用：保持既有两位最小宽度的跨语言 slot 编号格式，同时避免运行期格式化器分配。
     *
     * @param value 非负分片或桶下标
     * @return 至少两位的十进制编号。
     */
    private static String paddedIndex(int value) {
        return value < 10 ? "0" + value : Integer.toString(value);
    }

    /**
     * 业务作用：计算跨语言稳定的 FNV-1a 32 位哈希。
     *
     * @param value 协议字符串
     * @return 有符号整数哈希。
     */
    private static int stableHash(String value) {
        int hash = 0x811c9dc5;
        for (byte valueByte : Objects.requireNonNull(value, "value must not be null").getBytes(StandardCharsets.UTF_8)) {
            hash ^= valueByte & 0xff;
            hash *= 0x01000193;
        }
        return hash;
    }
}
