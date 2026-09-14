package io.github.nasaruntime.redis.cache.redis.partition;

/**
 * 业务作用：冻结一个逻辑分组的低基数集群观测值，meter scrape 只读本地缓存而不在采集线程执行 Redis I/O。
 *
 * @param nodeAlive             当前实例是否仍运行
 * @param claimedPartitions     当前实例真实持有的分区锁数
 * @param fairTargetPartitions  最近一次心跳计算的公平目标
 * @param claimGeneration       当前分组累计取得控制权的代次
 * @param pelPending            最近一次完整采样的全分组 PEL 数
 * @param pelOldestIdleMillis   最近一次完整采样的最大 PEL idle
 * @param ownerConvergenceMillis 最近一次 owner 数达到公平目标的耗时
 * @param pelTakeoverMillis     最近一次 Claim 接管历史 PEL 的耗时
 * @param lockSelfChecks        当前分组累计执行的 holder 自检次数
 * @param wakeSignals           当前分组累计收到的跨节点 wake 信号
 * @param rebalanceFallbacks    当前分组累计执行的周期再平衡兜底次数
 * @param drainTimeouts         当前分组累计发生的排干超时次数
 */
record RedisPartitionMetricSnapshot(long nodeAlive,
                                    long claimedPartitions,
                                    long fairTargetPartitions,
                                    long claimGeneration,
                                    long pelPending,
                                    long pelOldestIdleMillis,
                                    long ownerConvergenceMillis,
                                    long pelTakeoverMillis,
                                    long lockSelfChecks,
                                    long wakeSignals,
                                    long rebalanceFallbacks,
                                    long drainTimeouts) { }
