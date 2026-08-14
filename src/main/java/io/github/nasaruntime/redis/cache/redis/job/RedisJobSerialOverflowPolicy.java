package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：定义串行等待队列达到容量上限时保留早期任务还是接纳新任务。
 */
public enum RedisJobSerialOverflowPolicy {
    SKIP_OLDEST,
    SKIP_NEWEST
}
