package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：区分可由调度器生成普通 Run 的任务与只能接收 Fanout 分片的 Worker。
 */
public enum RedisJobTrigger {
    SCHEDULED,
    FANOUT_ONLY
}
