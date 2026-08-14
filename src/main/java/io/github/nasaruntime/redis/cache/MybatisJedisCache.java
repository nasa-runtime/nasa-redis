package io.github.nasaruntime.redis.cache;

import io.github.nasaruntime.core.utils.ObjMprUtils;
import io.github.nasaruntime.core.utils.ReflectUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.cache.Cache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.DatabaseCommands;
import redis.clients.jedis.commands.JedisBinaryCommands;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.exceptions.JedisException;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 业务作用：为 MyBatis 的二级缓存提供 Redis 后端，使查询结果跨进程共享而不是各实例各存一份。
 * 连接由使用方在启动时注入，本类不自行建连——连接参数与生命周期归应用管理。
 */
@Deprecated
@SuppressWarnings("unused")
@Slf4j
public class MybatisJedisCache implements Cache {

    protected static final String Not_get_resource_err = "Could not get a resource from the pool";

    /* redis的数据库序号 */
    protected static int index = 1;
    /* redis单机连接池 */
    protected static JedisPool jedisPool;
    /* redis集群连接 */
    protected static JedisCluster jedisCluster;

    protected final String id;
    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * 业务作用：为一个 MyBatis 映射命名空间建出二级缓存实例，缓存键以该命名空间为前缀，
     * 使不同映射的缓存互不干扰。
     *
     * @param id 映射命名空间标识
     */
    public MybatisJedisCache(final String id) {
        this.id = id;
    }

    /**
     * 业务作用：注入单机模式的连接池，是使用本缓存前的必要装配步骤。
     * 与集群连接<b>二选一</b>：两者都注入时集群优先，单机池不会被使用。
     *
     * @param jedisPool 单机连接池
     * 返回: 无返回值。
     */
    public static void setJedisPool(JedisPool jedisPool) {
        MybatisJedisCache.jedisPool = jedisPool;
    }

    /**
     * 业务作用：注入集群模式的连接，是集群部署下使用本缓存的必要装配步骤。
     * 注入后<b>优先于单机连接池</b>被使用。
     *
     * @param jedisCluster 集群连接
     * 返回: 无返回值。
     */
    public static void setJedisCluster(JedisCluster jedisCluster) {
        MybatisJedisCache.jedisCluster = jedisCluster;
    }

    /**
     * 业务作用：取一条可用连接，并在连接池暂时耗尽时按次数退避重试。
     * <p>
     * 只对「池中取不到连接」这一种异常重试：它通常是瞬时争抢，重试大概率能成功；
     * 其余异常直接抛出，因为重试改变不了结果。
     * <p>
     * 按重试次数分级记日志：前几次不记，中段只记摘要，接近上限才记完整堆栈——
     * 瞬时争抢是常态，每次都打完整堆栈会把日志淹没，而真正耗尽时又需要堆栈定位。
     * <p>
     * 超过上限仍取不到即抛出，避免无限重试把调用方长期挂住。
     *
     * @param m 当前是第几次尝试，用于控制退避与日志级别
     * @return 可用连接；未注入任何连接时返回 null。
     */
    private static JedisCommands getResource(int m) {
        if (Objects.nonNull(jedisCluster)) {
            return jedisCluster;
        }
        if (Objects.isNull(jedisPool)) {
            return null;
        }
        try {
            return jedisPool.getResource();
        } catch (JedisException e) {
            if (!Not_get_resource_err.equals(e.getMessage()) || m >= 10) {
                throw e;
            }
            // 当发生 Could not get a resource from the pool 异常时，当前休眠毫秒数，再继续获取resource
            if (m > 3 && m < 7) {
                log.error(e.getMessage());
            }
            else if (m >= 7) {
                log.error(e.getMessage(), e);
            }
            // 休眠毫秒数
            // m = 1 => 20   ms
            // m = 2 => 40   ms
            // m = 3 => 80   ms
            // m = 4 => 160  ms
            // m = 5 => 320  ms
            // m = 6 => 640  ms
            // m = 7 => 1280 ms
            // m = 8 => 2560 ms
            // m = 9 => 5120 ms
            ReflectUtils.sleep(10L << m);
            // 再次获取resource
            return getResource(++m);
        }
    }

    /**
     * 业务作用：取一条连接并按调用方期望的类型返回，屏蔽单机与集群两种实现的差异。
     *
     * @param clazz 期望的连接类型
     * @param <T>   连接类型
     * @return 连接实例；未注入任何连接时返回 null。
     */
    @SuppressWarnings("unchecked")
    public static <T> T getResource(Class<T> clazz) {
        return (T) getResource(1);
    }

    /**
     * 业务作用：取一条连接并切到指定的数据库序号。
     * 仅单机连接支持切库；集群连接会忽略该参数——Redis 集群模式只有一个数据库。
     *
     * @param index 数据库序号
     * @param clazz 期望的连接类型
     * @param <T>   连接类型
     * @return 已切库的连接；未注入任何连接时返回 null。
     */
    public static <T> T getResource(int index, Class<T> clazz) {
        T commands = getResource(clazz);
        if (Objects.nonNull(commands) && commands instanceof DatabaseCommands) {
            ((DatabaseCommands) commands).select(index);
        }
        return commands;
    }

    /**
     * 业务作用：取一条单机连接并切到指定数据库，供需要直接使用原生命令的场景。
     * 未注入单机连接池时直接失败而非返回 null：调用方拿到 null 也无法继续，
     * 把配置缺失暴露在调用点比让它变成一个空指针更容易定位。
     *
     * @param index 数据库序号
     * @return 已切库的单机连接。
     * @throws NullPointerException 未注入单机连接池
     */
    public static Jedis getJedis(int index) {
        if (Objects.isNull(jedisPool)) {
            throw new NullPointerException();
        }
        return getResource(index, Jedis.class);
    }

    /**
     * 业务作用：获取一个JedisCluster实例
     *
     * @return 见上述说明。
     */
    public static JedisCluster getJedisCluster() {
        if (Objects.isNull(jedisCluster)) {
            throw new NullPointerException();
        }
        return jedisCluster;
    }

    /**
     * 业务作用：取一条可用连接，屏蔽单机与集群两种部署的差异。
     *
     * <p>参数说明: 无。
     *
     * @return 可用连接；未注入任何连接时返回 null。
     */
    private static JedisBinaryCommands getResource() {
        return getResource(index, JedisBinaryCommands.class);
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
     * 业务作用：把一次查询结果写入缓存，经由原生连接存到 Redis。
     * 键由 MyBatis 按语句与参数生成，因此<b>参数不同的同一条语句各占一个缓存项</b>。
     *
     * @param key   MyBatis 生成的缓存键
     * @param value 查询结果
     * 返回: 无返回值。
     */
    @Override
    public void putObject(Object key, Object value) {
        JedisBinaryCommands commands = null;
        try {
            commands = getResource();
            if (Objects.isNull(commands)) {
                return;
            }
            commands.hset(this.id.getBytes(), key.toString().getBytes(), ObjMprUtils.toBytes((Serializable) value));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (Objects.nonNull(commands) && commands instanceof Jedis jedis) {
                jedis.close();
            }
        }
    }

    /**
     * 业务作用：读取缓存的查询结果，未命中时返回 null 由 MyBatis 回落到数据库。
     *
     * @param key MyBatis 生成的缓存键
     * @return 缓存的查询结果；未命中时为 null。
     */
    @Override
    public Object getObject(Object key) {
        Object obj = null;
        JedisBinaryCommands commands = null;
        try {
            commands = getResource();
            if (Objects.isNull(commands)) {
                return obj;
            }
            byte[] value = commands.hget(this.id.getBytes(), key.toString().getBytes());
            obj = ObjMprUtils.toObject(value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (Objects.nonNull(commands) && commands instanceof Jedis jedis) {
                jedis.close();
            }
        }
        return obj;
    }

    /**
     * 业务作用：移除一个缓存项。
     *
     * @param key MyBatis 生成的缓存键
     * @return 被移除的值；不存在时为 null。
     */
    @Override
    public Object removeObject(Object key) {
        Object obj = null;
        JedisBinaryCommands commands = null;
        try {
            commands = getResource();
            if (Objects.isNull(commands)) {
                return obj;
            }
            obj = commands.hdel(this.id.getBytes(), key.toString().getBytes());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (Objects.nonNull(commands) && commands instanceof Jedis jedis) {
                jedis.close();
            }
        }
        return obj;
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
        JedisBinaryCommands commands = null;
        try {
            commands = getResource();
            if (Objects.isNull(commands)) {
                return;
            }
            commands.del(this.id.getBytes());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (Objects.nonNull(commands) && commands instanceof Jedis jedis) {
                jedis.close();
            }
        }
    }

    /**
     * 业务作用：统计本命名空间下的缓存项数，供监控使用。
     * 需要按前缀扫描全部键，<b>不应放在高频路径上</b>。
     *
     * <p>参数说明: 无。
     *
     * @return 缓存项数。
     */
    @Override
    public int getSize() {
        int size = 0;
        JedisBinaryCommands commands = null;
        try {
            commands = getResource();
            if (Objects.isNull(commands)) {
                return size;
            }
            if (commands instanceof DatabaseCommands) {
                size = (int) ((DatabaseCommands) commands).dbSize();
            } else {
                size = (int) ((JedisCluster) commands).dbSize();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (Objects.nonNull(commands) && commands instanceof Jedis jedis) {
                jedis.close();
            }
        }
        return size;
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
        this.lock.readLock().lock();
        return this.lock;
    }
}
