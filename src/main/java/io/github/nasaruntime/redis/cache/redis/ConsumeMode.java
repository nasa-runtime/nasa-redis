package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.redis.cache.redis.partition.RedisPartition;

/**
 * 业务作用：决定 listener 的 Stream 消费来源、顺序范围与确认权威。
 * <p>
 * 业务方在 {@link StreamSubscribe} 中通过 {@code mode()} 选择路径；PROXY 接受 Batch/Single，
 * PARTITION 只接受 Single listener。
 *
 * @see StreamSubscribe
 * @see RedisPartition
 */
public enum ConsumeMode {

    /**
     * 仅走 RedisProxy.subscribe — 普通流共享 group, 多 consumer 并行 (高吞吐)。
     * 默认模式，不建立 RedisPartition 分区认领运行时。
     */
    PROXY,

    /**
     * 仅走 RedisPartition；集群中同一分区只有一个有效消费 owner，本地按计划和有效 hash 执行顺序 Task。
     * <p>
     * 业务侧 publish 时必须用 {@link RedisPartition#publish(String, String, long, Object)}
     * (或 String partition 重载), 而不是 RedisProxy.publish/xAdd 直发 stream key。
     * 历史记录在逐 record 执行权内复验 PEL 和 holder，成功业务交给确认链后不因 ACK 未决重新执行。
     * {@code stream.partition.executor.scope} 选择 source/group/stream 本地资源域，默认 source；
     * 域间有固定数量份额，同计划同有效 hash 的顺序门禁仍共享。
     */
    PARTITION
}
