package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：标识任务下一逻辑时刻的计算方式。
 */
public enum RedisJobScheduleType {
    CRON,
    FIXED_RATE,
    FIXED_DELAY,
    MANUAL,
    FANOUT_ONLY
}
