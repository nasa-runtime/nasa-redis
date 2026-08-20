package io.github.nasaruntime.redis.cache;

import io.github.nasaruntime.core.cache.SimpleCache;
import io.github.nasaruntime.core.utils.ContextUtils;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Nasa
 * 默认redis简单缓存
 */
@SuppressWarnings("unchecked")
public class DefaultRedisSimpleCache<V> implements SimpleCache<String, V> {

    private final RedisSerializer<V> valueSerializer;
    private RedisTemplate<String, V> redisTemplate;
    private HashOperations<String, String, V> hashOperations;

    /**
     * 业务作用：以默认序列化方式建出简单缓存。
     *
     * <p>参数说明: 无。
     */
    public DefaultRedisSimpleCache() {
        // Jackson2JsonRedisSerializer 对 byte[] 序列化反序列化有问题，这里只能用Jdk序列化
        this((RedisSerializer<V>) new JdkSerializationRedisSerializer());
    }

    /**
     * 业务作用：以指定的值序列化方式建出简单缓存，用于需要与既有数据格式对齐的场景。
     *
     * @param valueSerializer 值的序列化方式
     */
    public DefaultRedisSimpleCache(RedisSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    private final java.util.concurrent.locks.ReentrantLock initLock = new java.util.concurrent.locks.ReentrantLock();

    /**
     * 业务作用：惰性取出模板并缓存，避免每次操作都查一次容器。
     *
     * <p>参数说明: 无。
     *
     * @return 操作模板。
     */
    private RedisTemplate<String, V> getRedisTemplate() {
        if (Objects.nonNull(redisTemplate)) return redisTemplate;
        initLock.lock();
        try {
            if (Objects.nonNull(redisTemplate)) return redisTemplate;
            RedisTemplate<String, V> bean = (RedisTemplate<String, V>) ContextUtils.getBean("redisTemplate");
            if (bean.getValueSerializer().getClass() == valueSerializer.getClass()) {
                return this.redisTemplate = bean;
            }
            RedisTemplate<String, V> template = new RedisTemplate<>();
            template.setConnectionFactory(bean.getConnectionFactory());
            template.setKeySerializer(bean.getKeySerializer());
            template.setHashKeySerializer(bean.getHashKeySerializer());
            template.setValueSerializer(this.valueSerializer);
            template.setHashValueSerializer(this.valueSerializer);
            template.afterPropertiesSet();
            template.setEnableTransactionSupport(false);
            this.hashOperations = template.opsForHash();
            return this.redisTemplate = template;
        } finally {
            initLock.unlock();
        }
    }

    /**
     * 业务作用：惰性取出哈希操作入口并缓存。
     *
     * <p>参数说明: 无。
     *
     * @return 哈希操作入口。
     */
    private HashOperations<String, String, V> getHashOperations() {
        if (Objects.nonNull(hashOperations)) return hashOperations;
        initLock.lock();
        try {
            if (Objects.nonNull(hashOperations)) return hashOperations;
            getRedisTemplate();
            return this.hashOperations;
        } finally {
            initLock.unlock();
        }
    }
    
    /**
     * 业务作用：读取字符串值。
     * 键不存在时返回空。
     *
     * @param key 缓存键
     * @return 缓存值；键不存在时返回 null。
     */
    @Override
    public V get(String key) {
        return getRedisTemplate().opsForValue().get(key);
    }

    /**
     * 业务作用：写入一个缓存值。
     *
     * @param key   缓存键
     * @param value 缓存值
     * 返回: 无返回值。
     */
    @Override
    public void put(String key, V value) {
        getRedisTemplate().opsForValue().set(key, value);
    }

    /**
     * 业务作用：读取缓存值，未命中时用给定函数计算并写回。
     * <p>
     * 读取与写回<b>不是原子的</b>：并发未命中时多个调用方都会执行计算函数，
     * 最后一个写回的生效。计算函数应当无副作用且幂等。
     *
     * @param k               缓存键
     * @param mappingFunction 未命中时的计算函数
     * @return 缓存值或本次计算结果。
     */
    @Override
    public V computeIfAbsent(String k, Function<String, V> mappingFunction) {
        V v = this.get(k);
        if (Objects.nonNull(v)) return v;
        v = mappingFunction.apply(k);
        this.put(k, v);
        return v;
    }

    /**
     * 业务作用：判断缓存键是否存在。
     *
     * @param k 缓存键
     * @return 存在返回 true。
     */
    @Override
    public boolean containsKey(String k) {
        Boolean b = getRedisTemplate().hasKey(k);
        return Objects.nonNull(b) && b;
    }

    /**
     * 业务作用：判断哈希缓存中的字段是否存在。
     *
     * @param k  缓存键
     * @param hk 哈希字段名
     * @return 存在返回 true。
     */
    @Override
    public boolean containsKey(String k, String hk) {
        return getHashOperations().hasKey(k, hk);
    }

    /**
     * 业务作用：写入一个哈希缓存字段。
     *
     * @param k  缓存键
     * @param hk 哈希字段名
     * @param v  缓存值
     * 返回: 无返回值。
     */
    @Override
    public void put(String k, String hk, V v) {
        getHashOperations().put(k, hk, v);
    }

    /**
     * 业务作用：读取哈希缓存字段，未命中时计算并写回；同样不保证原子。
     *
     * @param k               缓存键
     * @param hk              哈希字段名
     * @param mappingFunction 未命中时的计算函数
     * @return 缓存值或本次计算结果。
     */
    @Override
    public V computeIfAbsent(String k, String hk, BiFunction<String, String, V> mappingFunction) {
        V v = hGet(k, hk);
        if (Objects.nonNull(v)) return v;
        v = mappingFunction.apply(k, hk);
        this.put(k, hk, v);
        return v;
    }

    /**
     * 业务作用：读取哈希字段。
     * 字段不存在时返回空。
     *
     * @param k  哈希缓存键
     * @param hk 哈希字段名
     * @return 字段值；键或字段不存在时返回 null。
     */
    @Override
    public V hGet(String k, String hk) {
        return getHashOperations().get(k, hk);
    }

    /**
     * 业务作用：读取哈希字段。
     * 字段不存在时返回空。
     *
     * @param k 哈希缓存键
     * @return 全部字段及其值；键不存在时返回空 Map。
     */
    @Override
    public Map<String, V> hGet(String k) {
        return getHashOperations().entries(k);
    }

    /**
     * 业务作用：读取哈希的全部字段名。
     * 字段极多时会一次性返回全部内容，应评估返回体量。
     *
     * @param s 哈希缓存键
     * @return 全部字段名；键不存在时返回空 Set。
     */
    @Override
    public Set<String> hKeys(String s) {
        return getHashOperations().keys(s);
    }

    /**
     * 业务作用：删除键。
     * 键不存在时不报错，因此可安全用于幂等清理。
     *
     * @param ks 见方法语义
     * 返回: 无返回值。
     */
    @Override
    public void del(String... ks) {
        for (String k : ks) {
            getRedisTemplate().delete(k);
        }
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     *
     * @param k 见方法语义
     * @param timeout 超时时长
     * 返回: 无返回值。
     */
    @Override
    public void expire(String k, Duration timeout) {
        getRedisTemplate().expire(k, timeout);
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     *
     * @param k 见方法语义
     * @param timeout 超时时长
     * @param hks 见方法语义
     * 返回: 无返回值。
     */
    @Override
    public void expire(String k, Duration timeout, String... hks) {
        getHashOperations().expire(k, timeout, Arrays.asList(hks));
    }

}
