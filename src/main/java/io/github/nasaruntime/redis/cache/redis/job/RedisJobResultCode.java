package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：把 Handler 结果转换为持久状态机能够稳定识别的结果码。
 */
public enum RedisJobResultCode {
    SUCCESS,
    RETRY,
    FAIL_PERMANENT,
    CANCELLED,
    TIMEOUT
}
