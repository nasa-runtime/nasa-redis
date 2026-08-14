package io.github.nasaruntime.redis.cache.redis;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 业务作用：提供不绑定特定指标库的 nonce 幂等计数快照，应用可按 ttlMode 与 RedisProxy qualifier 桥接到观测系统。
 */
public final class RedisIdempotentCounterMetrics {

    public static final String APPLIED_TOTAL = "redis_idempotent_counter_applied_total";
    public static final String DUPLICATE_TOTAL = "redis_idempotent_counter_duplicate_total";
    public static final String REJECTED_TOTAL = "redis_idempotent_counter_rejected_total";
    public static final String TTL_MISSING_TOTAL = "redis_idempotent_counter_ttl_missing_total";
    public static final String PROBE_FAILURES_TOTAL = "redis_idempotent_counter_probe_failures_total";
    public static final String RESOLVED_TTL_MODE = "redis_idempotent_counter_resolved_ttl_mode";
    public static final String LAYOUT_MARKER = "redis_idempotent_counter_layout_marker";

    private final LongAdder applied = new LongAdder();
    private final LongAdder duplicate = new LongAdder();
    private final LongAdder ttlMissing = new LongAdder();
    private final LongAdder probeFailures = new LongAdder();
    private final LongAdder[] rejected = new LongAdder[RedisIdempotentCounterResultCode.values().length];
    private volatile RedisIdempotentCounterTtlMode ttlMode = RedisIdempotentCounterTtlMode.AUTO;
    private volatile String layoutMarker = "";

    /**
     * 业务作用：预建固定返回码的拒绝计数器，使资金热路径不因首次出现某个原因而扩容映射。
     *
     * <p>参数说明: 无。
     */
    RedisIdempotentCounterMetrics() {
        for (int i = 0; i < rejected.length; i++) rejected[i] = new LongAdder();
    }

    /**
     * 业务作用：读取一个累计计数。
     *
     * @param name 指标名
     * @return 当前累计值；未知指标为零。
     */
    public long counter(String name) {
        return switch (name) {
            case APPLIED_TOTAL -> applied.sum();
            case DUPLICATE_TOTAL -> duplicate.sum();
            case REJECTED_TOTAL -> rejectedTotal();
            case TTL_MISSING_TOTAL -> ttlMissing.sum();
            case PROBE_FAILURES_TOTAL -> probeFailures.sum();
            default -> 0;
        };
    }

    /**
     * 业务作用：按结构化拒绝原因读取累计计数，供观测系统映射 reason 标签。
     *
     * @param reason 拒绝返回码
     * @return 当前累计值；成功类返回码为零。
     */
    public long rejected(RedisIdempotentCounterResultCode reason) {
        if (reason == null || !reason.name().startsWith("REJECTED_")) return 0;
        return rejected[reason.ordinal()].sum();
    }

    /**
     * 业务作用：取得已由共享 layout marker 固定的 TTL 模式，供所有指标附加 ttl_mode 标签。
     *
     * <p>参数说明: 无。
     *
     * @return 首次使用前为 AUTO，布局固定后为 HASH_FIELD 或 HASH_BUCKET。
     */
    public RedisIdempotentCounterTtlMode ttlMode() {
        return ttlMode;
    }

    /**
     * 业务作用：取得 Redis 中已经固定的布局指纹，供运维核对不同应用节点是否服从同一账本合同。
     *
     * <p>参数说明: 无。
     *
     * @return 首次使用前为空串，布局固定后为共享 marker 原文。
     */
    public String layoutMarker() {
        return layoutMarker;
    }

    /**
     * 业务作用：读取 HPEXPIRE 能力探测未能取得完整结论的累计次数，区分正常不支持与探测链路异常。
     *
     * <p>参数说明: 无。
     *
     * @return 当前累计探测失败次数。
     */
    public long probeFailures() {
        return probeFailures.sum();
    }

    /**
     * 业务作用：判断凭证回收是否仍可信；任一次 TTL 未确认都会持续报告不健康，避免内存风险被瞬时成功掩盖。
     *
     * <p>参数说明: 无。
     *
     * @return 尚未观察到 TTL 未确认时为 true。
     */
    public boolean healthy() {
        return ttlMissing.sum() == 0;
    }

    /**
     * 业务作用：生成一致的只读观测快照，计数、已解析 TTL 模式与共享布局指纹一次返回，
     * 使运维无需读取 Redis 或根据键形态反推当前账本布局。
     *
     * <p>参数说明: 无。
     *
     * @return 观测项到当前值的不可变映射；计数为 Long，布局项为 String。
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(APPLIED_TOTAL, applied.sum());
        result.put(DUPLICATE_TOTAL, duplicate.sum());
        result.put(REJECTED_TOTAL, rejectedTotal());
        for (RedisIdempotentCounterResultCode code : RedisIdempotentCounterResultCode.values()) {
            if (code.name().startsWith("REJECTED_")) {
                result.put(REJECTED_TOTAL + "{reason=" + code.name() + "}", rejected[code.ordinal()].sum());
            }
        }
        result.put(TTL_MISSING_TOTAL, ttlMissing.sum());
        result.put(PROBE_FAILURES_TOTAL, probeFailures.sum());
        result.put(RESOLVED_TTL_MODE, ttlMode.name());
        result.put(LAYOUT_MARKER, layoutMarker);
        return Map.copyOf(result);
    }

    /**
     * 业务作用：累计一次真正产生计数变化的请求。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    void recordApplied() {
        applied.increment();
    }

    /**
     * 业务作用：累计一次返回首次结果且未产生第二次变化的 nonce 重放。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    void recordDuplicate() {
        duplicate.increment();
    }

    /**
     * 业务作用：累计一次未产生目标与凭证副作用的结构化拒绝。
     *
     * @param reason 拒绝返回码
     * 返回: 无返回值。
     */
    void recordRejected(RedisIdempotentCounterResultCode reason) {
        rejected[reason.ordinal()].increment();
    }

    /**
     * 业务作用：累计一次凭证已登记但回收 TTL 未确认的内存风险事件。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    void recordTtlMissing() {
        ttlMissing.increment();
    }

    /**
     * 业务作用：发布当前 RedisProxy 已固定的 TTL 模式与共享 marker，确保观测状态与真实账本布局一致。
     *
     * @param mode   已解析模式
     * @param marker Redis 中确认的共享布局指纹
     * 返回: 无返回值。
     */
    void resolveLayout(RedisIdempotentCounterTtlMode mode, String marker) {
        ttlMode = mode;
        layoutMarker = marker;
    }

    /**
     * 业务作用：累计一次无法完整判断服务端命令能力的探测；服务端明确不支持 HPEXPIRE 不计入失败。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    void recordProbeFailure() {
        probeFailures.increment();
    }

    /**
     * 业务作用：汇总全部结构化拒绝原因。
     *
     * <p>参数说明: 无。
     *
     * @return 拒绝总数。
     */
    private long rejectedTotal() {
        long total = 0;
        for (RedisIdempotentCounterResultCode code : RedisIdempotentCounterResultCode.values()) {
            if (code.name().startsWith("REJECTED_")) total += rejected[code.ordinal()].sum();
        }
        return total;
    }
}
