package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：把持久索引的下一个 Redis score 转为带节点错峰的有界扫描间隔。
 */
final class RedisJobScanInterval {

    static final int SCHEDULE = 1;
    static final int VISIBLE = 2;
    static final int LEASE = 3;
    static final int WAITING = 4;
    static final int FANOUT_ROOT = 5;
    static final int FANOUT_RECEIPT = 6;
    static final int FANOUT_READY = 7;
    static final int FANOUT_LEASE = 8;
    static final int FANOUT_GC = 9;

    /**
     * 业务作用：禁止实例化纯扫描间隔策略类。
     *
     * <p>参数说明: 无。
     */
    private RedisJobScanInterval() {}

    /**
     * 业务作用：有到期成员时立即维持最短恢复周期，空闲时按下一 score 退避并错开不同节点。
     *
     * @param properties       扫描上下限
     * @param executorId       当前进程执行器身份
     * @param redisNow         本次扫描的 Redis 时间
     * @param nextScore        索引当前最小 score；空索引为零
     * @param hadDue           本轮是否读取到到期成员
     * @param indexType        索引类型稳定编号
     * @param slot             调度分片或 Fanout 桶
     * @param requestedMaximum 当前索引业务期限允许的最长发现间隔
     * @return 落在配置上下限内的下一扫描间隔。
     */
    static long delay(RedisJobProperties properties, String executorId, long redisNow, long nextScore,
                      boolean hadDue, int indexType, int slot, long requestedMaximum) {
        long minimum = properties.getMinScanIntervalMs();
        long maximum = Math.max(minimum, Math.min(properties.getMaxScanIntervalMs(), requestedMaximum));
        if (hadDue) return minimum;
        long base = nextScore == 0L ? maximum : Math.max(minimum, Math.min(maximum, nextScore - redisNow));
        long jitterWindow = Math.max(1L, Math.min(maximum - minimum, Math.max(minimum, maximum / 10L)));
        int hash = 31 * (31 * executorId.hashCode() + indexType) + slot;
        long jitter = Math.floorMod(hash, jitterWindow + 1L);
        // 抖动只把访问提前，不能把已知业务截止点继续向后推迟。
        return Math.max(minimum, base - jitter);
    }
}
