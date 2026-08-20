package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：定义同一任务多个 Run 竞争执行权时的业务语义。
 */
public enum RedisJobConcurrency {
    /** 后续 Run 进入有界队列，当前 attempt 释放 Redis 串行槽后再恢复派发。 */
    SERIAL_QUEUE,
    /** 已有同名 Run 持有串行槽时，把当前 Run 记为 SKIPPED，不保留待执行积压。 */
    DISCARD_IF_RUNNING,
    /** 不占用同名任务串行槽，各 Run 可以分别取得执行权并行运行。 */
    PARALLEL
}
