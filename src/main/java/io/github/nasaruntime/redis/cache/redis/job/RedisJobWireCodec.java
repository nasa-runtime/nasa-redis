package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：声明任务参数在不同语言运行时之间使用的稳定编码。
 */
public enum RedisJobWireCodec {
    JSON,
    PROTOBUF,
    RAW
}
