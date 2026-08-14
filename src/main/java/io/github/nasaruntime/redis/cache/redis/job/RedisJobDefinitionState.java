package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：表示任务定义能否继续生成新的运行实例。
 */
public enum RedisJobDefinitionState {
    DISABLED,
    ENABLED,
    PAUSED,
    CONFLICT,
    DELETED
}
