package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.Initialization;

/**
 * Nasa
 * redis订阅接口，反序列化依赖RedisTemplate配置
 * @see io.github.nasaruntime.redis.cache.redis.Subscriber
 */
public interface Subscribe<T> extends Initialization {

    /**
     * 业务作用：服务于哪个redis server
     *
     * @return 见上述说明。
     */
    default String[] qualifier() {
        return null;
    }

    /**
     * 业务作用：订阅的 channel
     *
     * @return 见上述说明。
     */
    String[] channels();

    /**
     * 业务作用：消费
     *
     * @param t 见上述说明
     */
    void consume(T t);

}
