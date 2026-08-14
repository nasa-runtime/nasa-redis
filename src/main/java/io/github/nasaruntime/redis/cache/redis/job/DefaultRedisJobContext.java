package io.github.nasaruntime.redis.cache.redis.job;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 业务作用：实现 Handler 的本地失权门禁、参数解码和一次性 Fanout 创建约束。
 */
final class DefaultRedisJobContext implements RedisJobContext {

    private final String namespace;
    private final RedisJobRepository.RunData data;
    private final int attempt;
    private final long attemptToken;
    private final RedisJobJsonCodec jsonCodec;
    private final RedisJobFanoutService fanoutService;
    private final Optional<RedisJobFanoutContext> fanoutContext;
    private final AtomicLong ownershipDeadlineNanos;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicBoolean fanoutPrepared = new AtomicBoolean();

    /**
     * 业务作用：建立当前 attempt 的本地执行视图，并把服务端租约换算为保守单调时钟截止点。
     *
     * @param namespace         调度命名空间
     * @param data              Run 数据
     * @param attempt           当前 attempt
     * @param attemptToken      当前 fencing token
     * @param leaseMs           服务端租约时长
     * @param safetyAllowanceMs RTT 与时钟安全余量
     * @param jsonCodec         Job 专用 JSON Codec
     * @param fanoutService     Fanout 服务
     * @param fanoutContext     Fanout shard 元数据
     */
    DefaultRedisJobContext(String namespace, RedisJobRepository.RunData data, int attempt, long attemptToken,
                           long leaseMs, long safetyAllowanceMs, RedisJobJsonCodec jsonCodec,
                           RedisJobFanoutService fanoutService, RedisJobFanoutContext fanoutContext) {
        this.namespace = namespace;
        this.data = Objects.requireNonNull(data, "data must not be null");
        this.attempt = attempt;
        this.attemptToken = attemptToken;
        this.jsonCodec = Objects.requireNonNull(jsonCodec, "jsonCodec must not be null");
        this.fanoutService = fanoutService;
        this.fanoutContext = Optional.ofNullable(fanoutContext);
        this.ownershipDeadlineNanos = new AtomicLong(deadline(leaseMs, safetyAllowanceMs));
    }

    /**
     * 业务作用：读取调度命名空间。 @return 命名空间。
     */
    @Override
    public String namespace() {
        return namespace;
    }

    /**
     * 业务作用：读取任务名。 @return 任务名。
     */
    @Override
    public String jobName() {
        return data.run().jobName();
    }

    /**
     * 业务作用：读取稳定 Run 标识。 @return Run 标识。
     */
    @Override
    public String runId() {
        return data.run().runId();
    }

    /**
     * 业务作用：读取逻辑触发时刻。 @return UTC 毫秒时刻。
     */
    @Override
    public long logicalFireAt() {
        return data.run().logicalFireAt();
    }

    /**
     * 业务作用：读取实际触发时刻。 @return UTC 毫秒时刻。
     */
    @Override
    public long triggeredAt() {
        return data.run().triggeredAt();
    }

    /**
     * 业务作用：读取当前 attempt。 @return attempt 序号。
     */
    @Override
    public int attempt() {
        return attempt;
    }

    /**
     * 业务作用：读取当前 fencing token。 @return token。
     */
    @Override
    public long attemptToken() {
        return attemptToken;
    }

    /**
     * 业务作用：按已登记类型解码 JSON，原始和 Protobuf 参数只允许读取 byte[]。
     *
     * @param type 目标类型
     * @param <T>  目标类型
     * @return 解码参数。
     */
    @Override
    public <T> T parameter(Class<T> type) {
        if (data.codec() == RedisJobWireCodec.JSON) return jsonCodec.decode(data.payload(), type);
        if (type == byte[].class) return type.cast(data.payload().clone());
        throw new IllegalArgumentException("non-JSON RedisJob payload can only be read as byte[]");
    }

    /**
     * 业务作用：读取参数原始字节副本。 @return 参数副本。
     */
    @Override
    public byte[] rawParameter() {
        return data.payload().clone();
    }

    /**
     * 业务作用：读取参数 Schema。 @return Schema 标识。
     */
    @Override
    public String schemaId() {
        return data.schemaId();
    }

    /**
     * 业务作用：读取参数编码。 @return 线编码。
     */
    @Override
    public RedisJobWireCodec wireCodec() {
        return data.codec();
    }

    /**
     * 业务作用：读取协作式取消信号。 @return 已请求取消返回 true。
     */
    @Override
    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    /**
     * 业务作用：使用本地单调时钟判定当前 attempt 是否仍在保守持权窗口内。
     *
     * @return 未取消且未越过截止点时返回 true。
     */
    @Override
    public boolean stillOwnsExecution() {
        return !cancellationRequested.get() && System.nanoTime() < ownershipDeadlineNanos.get();
    }

    /**
     * 业务作用：在业务安全点拒绝取消后或失权后的新副作用。
     * <p>
     * 返回：门禁通过时正常返回，否则抛出停止异常。
     */
    @Override
    public void checkpoint() {
        if (cancellationRequested.get()) throw new RedisJobExecutionStoppedException("RedisJob cancellation requested");
        if (System.nanoTime() >= ownershipDeadlineNanos.get()) {
            throw new RedisJobExecutionStoppedException("RedisJob execution authority expired");
        }
    }

    /**
     * 业务作用：创建绑定当前根 attempt 的 Fanout 构建器并拒绝 Worker 递归 Fanout。
     *
     * @param workerName Worker 能力名
     * @return Fanout 构建器。
     */
    @Override
    public RedisJobFanoutBuilder fanout(String workerName) {
        if (fanoutContext.isPresent()) {
            throw new IllegalStateException("FANOUT_ONLY worker cannot create another fanout");
        }
        if (fanoutService == null) throw new IllegalStateException("RedisJob fanout service is unavailable");
        return fanoutService.builder(this, RedisJobNames.requireName(workerName, "workerName"));
    }

    /**
     * 业务作用：读取当前 Fanout shard 元数据。 @return 普通 Run 为空。
     */
    @Override
    public Optional<RedisJobFanoutContext> fanoutContext() {
        return fanoutContext;
    }

    /**
     * 业务作用：在续期成功后推进本地保守截止点，Redis 不可达时旧值不会被延长。
     *
     * @param leaseMs           新租约时长
     * @param safetyAllowanceMs 安全余量
     *                          返回：无返回值。
     */
    void renewed(long leaseMs, long safetyAllowanceMs) {
        ownershipDeadlineNanos.set(deadline(leaseMs, safetyAllowanceMs));
    }

    /**
     * 业务作用：记录控制面取消信号，使下一业务安全点停止副作用。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void requestCancellation() {
        cancellationRequested.set(true);
    }

    /**
     * 业务作用：在 Fanout prepare 成功后不可逆转移根 Handler 的完成权。
     *
     * @return 首次转移返回 true，重复调用返回 false。
     */
    boolean markFanoutPrepared() {
        return fanoutPrepared.compareAndSet(false, true);
    }

    /**
     * 业务作用：判定完成权是否已经转移给 Fanout 聚合。 @return 已转移返回 true。
     */
    boolean fanoutPrepared() {
        return fanoutPrepared.get();
    }

    /**
     * 业务作用：读取当前 Redis 执行 owner，供 Fanout prepare 复验。 @return 执行器身份。
     */
    String owner() {
        return data.run().owner();
    }

    /**
     * 业务作用：用单调时钟计算早于 Redis 租约边界的本地截止点。
     *
     * @param leaseMs     租约毫秒数
     * @param allowanceMs 安全余量毫秒数
     * @return 单调时钟纳秒截止点。
     */
    private static long deadline(long leaseMs, long allowanceMs) {
        return System.nanoTime() + Math.max(1L, leaseMs - allowanceMs) * 1_000_000L;
    }
}
