package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：定义同一任务多个 Run 竞争执行权时的业务语义。
 */
public enum RedisJobConcurrency {
    SERIAL_QUEUE,
    DISCARD_IF_RUNNING,
    PARALLEL
}
