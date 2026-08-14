package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：声明 Fanout 低延迟通知使用 Redis Sharded Pub/Sub 还是显式广播降级模式。
 */
public enum RedisJobPubSubMode {
    SHARDED,
    BROADCAST
}
