package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.redis.cache.redis.partition.RedisPartition;

/**
 * 业务作用：决定 listener 的 Stream 消费来源、顺序范围与确认权威。
 * <p>
 * 业务方在 {@link StreamSubscribe} 中通过 {@code mode()} 选择路径；PROXY 接受 Batch/Single，
 * PARTITION 与 BOTH 只接受 Single listener。
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
     */
    PARTITION,

    /**
     * 声明同一 listener 同时服务普通 Stream 与 RedisPartition 两种消息来源。
     * <p>
     * 两侧复用同一不可变订阅计划和 Partition dispatcher。普通 Stream 侧使用独立的单 consumer、
     * 手工确认容器，并以 PEL consumer fencing 保护迟到确认；这里只提供 JVM 内 local_ordered，
     * 需要跨节点全局顺序的权威消费仍应使用 PARTITION。
     * 两侧独立持有来源代次；普通 Stream 的多 field 成功证据只在当前 consumer epoch 内有效，业务仍须幂等。
     */
    BOTH
}
