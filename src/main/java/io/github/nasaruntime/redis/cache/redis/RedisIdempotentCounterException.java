package io.github.nasaruntime.redis.cache.redis;

/**
 * 业务作用：报告幂等计数在产生任何目标或凭证副作用前被拒绝的确定原因。
 */
public class RedisIdempotentCounterException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final RedisIdempotentCounterResultCode code;

    /**
     * 业务作用：建立带结构化返回码的拒绝异常。
     *
     * @param code   脚本返回的拒绝码
     * @param detail Redis 返回的诊断信息
     */
    public RedisIdempotentCounterException(RedisIdempotentCounterResultCode code, String detail) {
        super(detail == null || detail.isBlank() ? code.name() : code.name() + ": " + detail);
        this.code = code;
    }

    /**
     * 业务作用：取得可稳定分支处理的拒绝码，避免调用方匹配异常文本。
     *
     * <p>参数说明: 无。
     *
     * @return 脚本返回的拒绝码。
     */
    public RedisIdempotentCounterResultCode getCode() {
        return code;
    }
}
