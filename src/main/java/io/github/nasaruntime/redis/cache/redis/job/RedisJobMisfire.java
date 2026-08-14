package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：定义调度器发现已经错过逻辑时刻时如何生成 Run。
 */
public enum RedisJobMisfire {
    DO_NOTHING,
    FIRE_ONCE_NOW,
    CATCH_UP
}
