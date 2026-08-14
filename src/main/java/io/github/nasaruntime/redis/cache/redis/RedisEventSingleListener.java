package io.github.nasaruntime.redis.cache.redis;

/**
 * Nasa
 * redis 事件数据逐条消费接口
 */
public interface RedisEventSingleListener<T> extends StreamSubscribe<T, T> {

}
