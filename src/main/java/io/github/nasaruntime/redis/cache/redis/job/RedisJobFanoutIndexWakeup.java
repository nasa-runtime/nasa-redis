package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：通知本进程某个 Fanout 桶已经产生根记录，使空桶长退避不会掩盖新的短期限索引。
 */
@FunctionalInterface
interface RedisJobFanoutIndexWakeup {
    /**
     * 业务作用：立即重排目标桶的索引扫描，并记录本次根任务允许的回执期限。
     *
     * @param fanoutId Fanout 标识
     * @param receiptTimeoutMs 根任务接收回执期限
     * 返回：无返回值。
     */
    void wake(String fanoutId, long receiptTimeoutMs);
}
