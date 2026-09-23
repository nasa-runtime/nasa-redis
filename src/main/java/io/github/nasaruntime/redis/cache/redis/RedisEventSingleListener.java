package io.github.nasaruntime.redis.cache.redis;

/**
 * 业务作用：以单条消息为业务处理边界，供普通 Stream 与持权分区消费使用。
 * <p>
 * PARTITION 通过稳定 {@code partitionKey(data)} 约束本地 Task 顺序；该模式下，
 * {@code onEvent} 正常返回才计为该消息业务成功，异常保留 PEL 并登记恢复责任。
 * PROXY 的确认与重投能力由普通消费组配置和调用方的 ACK 方式决定。
 * 回调自行派发的异步工作不属于框架 Task Future 的完成条件，不能在业务尚未结束时提前返回。
 * <p>
 * PARTITION 按至少一次交付处理，业务必须以稳定事件键处理重复调用；
 * PROXY 使用 NOACK 时不进入 PEL，回调失败不能依靠待处理消息恢复。
 *
 * @param <T> 单条消息解码后的业务类型
 */
public interface RedisEventSingleListener<T> extends StreamSubscribe<T, T> {

}
