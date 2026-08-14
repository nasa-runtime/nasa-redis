package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：把普通 Run 距离可见的剩余时长通知给本进程扫描器，避免墙上时钟偏差影响唤醒。
 */
@FunctionalInterface
interface RedisJobVisibleWakeup {
    /**
     * 业务作用：经过服务端计算的剩余时长后推进目标分片的可见索引。
     *
     * @param shard   调度分片
     * @param delayMs 距离可见的毫秒数；零表示立即推进
     *                返回：无返回值。
     */
    void wake(int shard, long delayMs);
}
