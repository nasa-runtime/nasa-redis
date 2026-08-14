package io.github.nasaruntime.redis.cache.redis;

/**
 * 业务作用：表达幂等计数脚本的确定结果，使调用方不依赖 Redis 错误文本判断是否产生资金变化。
 */
public enum RedisIdempotentCounterResultCode {
    APPLIED,
    APPLIED_TTL_MISSING,
    DUPLICATE,
    REJECTED_LEDGER_TYPE,
    REJECTED_OPERATION,
    REJECTED_COMMAND
}
