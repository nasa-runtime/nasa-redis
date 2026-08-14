package io.github.nasaruntime.redis.cache.redis;

/**
 * Stream 消费模式. 决定 listener 注册到哪条消费路径。
 * <p>
 * 业务方在 {@link StreamSubscribe} (含 Batch / Single 子接口) 里通过 {@code mode()} 字段表态选哪条路径。
 *
 * @see StreamSubscribe
 * @see RedisPartition
 */
public enum ConsumeMode {

    /**
     * 仅走 RedisProxy.subscribe — 普通流共享 group, 多 consumer 并行 (高吞吐)。
     * 默认模式, 与改造前行为完全一致。
     */
    PROXY,

    /**
     * 仅走 RedisPartition — per-partition 串行, 集群级同 partition 单节点 (强一致)。
     * <p>
     * 业务侧 publish 时必须用 {@link RedisPartition#publish(String, String, long, Object)}
     * (或 String partition 重载), 而不是 RedisProxy.publish/xAdd 直发 stream key。
     */
    PARTITION,

    /**
     * 双边注册 — 同一 listener / 订阅既走 subscribe 又走 partition。
     * <p>
     * 罕用. 适合"主流走 partition 保证串行, 同时挂监控走 subscribe 多节点并发统计"等场景。
     * 业务侧需自己明白消息源的两个出口 (subscribe 走 stream key 直发, partition 走 partition publish)。
     */
    BOTH
}
