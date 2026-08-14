package io.github.nasaruntime.redis.cache.redis;

/**
 * 业务作用：选择 nonce 幂等凭证的回收方式。
 * AUTO 会按 Redis 全部 master 的 HPEXPIRE 能力确定一次固定布局；另外两个值用于显式约束部署能力。
 */
public enum RedisIdempotentCounterTtlMode {
    AUTO,
    HASH_FIELD,
    HASH_BUCKET
}
