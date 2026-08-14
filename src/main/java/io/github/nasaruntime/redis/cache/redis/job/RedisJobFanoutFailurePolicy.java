package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：定义 Fanout 目标无法接收或完成分片时的集群收敛方式。
 */
public enum RedisJobFanoutFailurePolicy {
    REASSIGN_ON_FAILURE,
    STRICT_SNAPSHOT,
    BEST_EFFORT
}
