package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.utils.ColUtils;
import io.github.nasaruntime.core.utils.ContextUtils;
import io.github.nasaruntime.core.utils.ReflectUtils;
import lombok.Setter;
import org.apache.ibatis.cache.Cache;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Mybatis二级缓存，pipeline 实现数据交互
 */
public class MybatisCache implements Cache {

    private static final Map<String, Set<String>> clearMap = new HashMap<>();
    @Setter
    static RedisProxy redisProxy;
    private static final ReentrantLock redisProxyLock = new ReentrantLock();

    /**
     * 业务作用：惰性解析并缓存命令代理。
     * 惰性而非静态初始化：本类可能早于容器就绪被加载，此时取代理会失败。
     *
     * <p>参数说明: 无。
     *
     * @return 命令代理。
     */
    static RedisProxy initRedisProxy() {
        if (Objects.nonNull(redisProxy)) return redisProxy;
        redisProxyLock.lock();
        try {
            if (Objects.nonNull(redisProxy)) return redisProxy;
            return redisProxy = ContextUtils.getBean("redisProxy", RedisProxy.class);
        } finally {
            redisProxyLock.unlock();
        }
    }

    final String id;
    final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * 业务作用：为一个 MyBatis 映射命名空间建出二级缓存实例，缓存键以该命名空间为前缀，
     * 使不同映射的缓存互不干扰。
     *
     * @param id 映射命名空间标识
     */
    @SuppressWarnings("rawtypes")
    public MybatisCache(final String id) {
        clearMap.computeIfAbsent(this.id = id, t -> new HashSet<>()).add(this.id);
        Class<?> clazz = ReflectUtils.forName(id);
        if (Objects.isNull(clazz)) return;
        CacheClearRef clearRef = clazz.getAnnotation(CacheClearRef.class);
        if (Objects.isNull(clearRef)) return;
        Class[] classes = clearRef.value();
        for (Class c : classes) {
            clearMap.computeIfAbsent(c.getName(), t -> new HashSet<>()).add(this.id);
        }
    }

    /**
     * 业务作用：返回本缓存所属的映射命名空间标识，由 MyBatis 用于区分各缓存实例。
     *
     * <p>参数说明: 无。
     *
     * @return 命名空间标识。
     */
    @Override
    public String getId() {
        return this.id;
    }

    /**
     * 业务作用：把一次查询结果写入缓存，经由命令代理存到 Redis。
     * 键由 MyBatis 按语句与参数生成，因此<b>参数不同的同一条语句各占一个缓存项</b>。
     *
     * @param key   MyBatis 生成的缓存键
     * @param value 查询结果
     * 返回: 无返回值。
     */
    @Override
    public void putObject(Object key, Object value) {
        initRedisProxy().hSet(this.id, key.toString(), value);
    }

    /**
     * 业务作用：读取缓存的查询结果，未命中时返回 null 由 MyBatis 回落到数据库。
     *
     * @param key MyBatis 生成的缓存键
     * @return 缓存的查询结果；未命中时为 null。
     */
    @Override
    public Object getObject(Object key) {
        return initRedisProxy().hGet(this.id, key.toString());
    }

    /**
     * 业务作用：移除一个缓存项。
     *
     * @param key MyBatis 生成的缓存键
     * @return 被移除的值；不存在时为 null。
     */
    @Override
    public Object removeObject(Object key) {
        return initRedisProxy().hDel(this.id, key.toString());
    }

    /**
     * 业务作用：清空本命名空间下的全部缓存，MyBatis 在该命名空间发生写操作时调用。
     * <p>
     * 清空按命名空间前缀扫描并逐个删除，<b>缓存项极多时耗时明显</b>；
     * 写多读少的映射不宜开启二级缓存。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void clear() {
        Set<String> ids = clearMap.get(this.id);
        if (ids.size() == 1) {
            initRedisProxy().del(this.id);
            return;
        }
        initRedisProxy().del(ColUtils.toArray(ids));
    }

    /**
     * 业务作用：统计本命名空间下的缓存项数，供监控使用。
     * 需要按前缀扫描全部键，<b>不应放在高频路径上</b>。
     *
     * <p>参数说明: 无。
     *
     * @return 缓存项数。
     */
    @SuppressWarnings("ConstantConditions")
    @Override
    public int getSize() {
        RedisConnectionFactory factory = initRedisProxy().getRedisTemplate().getConnectionFactory();
        RedisConnection connection = RedisConnectionUtils.getConnection(factory);
        try {
            Long dbSize = connection.serverCommands().dbSize();
            return Objects.isNull(dbSize) ? 0 : dbSize.intValue();
        } finally {
            RedisConnectionUtils.releaseConnection(connection, factory);
        }
    }

    /**
     * 业务作用：返回读写锁。
     * 本实现的互斥由 Redis 自身保证，此处返回本地锁只是满足 MyBatis 的接口约定，
     * <b>不提供跨进程互斥</b>——依赖它做跨实例同步会失效。
     *
     * <p>参数说明: 无。
     *
     * @return 本地读写锁。
     */
    @Override
    public ReadWriteLock getReadWriteLock() {
        return this.lock;
    }
}
