package io.github.nasaruntime.redis.cache.redis;

import java.util.List;

/**
 * Nasa
 * redis 事件数据批量消费接口
 */
public interface RedisEventBatchListener<T> extends StreamSubscribe<T, List<T>> {

}
