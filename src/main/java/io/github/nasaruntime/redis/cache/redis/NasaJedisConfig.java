package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.IdGenerate;
import io.github.nasaruntime.redis.cache.MybatisJedisCache;
import io.github.nasaruntime.core.utils.ReflectUtils;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.util.ClassUtils;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Nasa
 * Jedis扩展配置
 */
@Deprecated
@Setter
@Getter
@ConfigurationProperties(prefix = "nasa.redis")
public class NasaJedisConfig {

    /* redis启动环境：single、cluster */
    private String mode = "single";
    /* 分布式ID的redis的lua脚本返回代码 */
    private String evalSha = "d4e188e31102185124a8987b8ea8ef8eda13ab66";
    /* 资源池中资源最小空闲时间，单位为毫秒，默认值：30分钟，当达到该值后空闲资源将被移除，建议根据业务自身设定 */
    private Long minEvictableIdleTimeMillis;
    /* 做空闲资源检测时，每次的采样数，默认值：3，可根据自身应用连接数进行微调，如果设置为 -1，表示对所有连接做空闲监测 */
    private Integer numTestsPerEvictionRun;
    /* 创建新的资源连接后，是否做连接有效性检测，无效连接会被移除，默认值：false ，业务量很大时建议为false，因为会多一次ping的开销 */
    private Boolean testOnCreate;
    /* 向资源池借用连接时，是否做连接有效性检测，无效连接会被移除，默认值：false ，业务量很大时建议为false，因为会多一次ping的开销 */
    private Boolean testOnBorrow;
    /* 向资源池归还连接时，是否做连接有效性检测，无效连接会被移除，默认值：false，业务量很大时建议为false，因为会多一次ping的开销 */
    private Boolean testOnReturn;
    /* 是否开启空闲资源监测，默认值：false */
    private Boolean testWhileIdle;
    /* 为false解决报错 MXBean already registered with name org.apache.commons.pool2 */
    private boolean jmxEnabled = false;

    /**
     * 业务作用：按配置组装连接池参数，供单机模式的客户端建池使用。
     *
     * @param properties Redis 连接配置
     * @return 组装完毕的连接池配置。
     */
    public JedisPoolConfig jedisPoolConfig(RedisProperties properties) {
        RedisProperties.Pool pool = properties.getJedis().getPool();
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(pool.getMaxActive());
        config.setMaxIdle(pool.getMaxIdle());
        config.setMinIdle(pool.getMinIdle());
        config.setMaxWait(pool.getMaxWait());
        // 空闲资源的检测周期，单位为毫秒，默认值：-1，表示不检测，建议设置一个合理的值，周期性运行监测任务
        config.setTimeBetweenEvictionRuns(pool.getTimeBetweenEvictionRuns());
        if (Objects.nonNull(minEvictableIdleTimeMillis)) {
            config.setMinEvictableIdleTime(Duration.ofMillis(minEvictableIdleTimeMillis));
        }
        if (Objects.nonNull(numTestsPerEvictionRun)) {
            config.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        }
        if (Objects.nonNull(testOnBorrow)) {
            config.setTestOnBorrow(testOnBorrow);
        }
        if (Objects.nonNull(testOnReturn)) {
            config.setTestOnReturn(testOnReturn);
        }
        if (Objects.nonNull(testOnCreate)) {
            config.setTestOnCreate(testOnCreate);
        }
        if (Objects.nonNull(testWhileIdle)) {
            config.setTestWhileIdle(testWhileIdle);
        }
        config.setJmxEnabled(jmxEnabled);
        return config;
    }

    /**
     * 业务作用：Jedis连接redis单节点工厂
     *
     * @param properties 见上述说明
     * @return 见上述说明。
     */
    @ConditionalOnMissingBean(RedisConnectionFactory.class)
    @Bean
    public RedisConnectionFactory redisConnectionFactory(RedisProperties properties) {

        JedisClientConfiguration jedisClientConfiguration = JedisClientConfiguration
                .builder()
                .usePooling().poolConfig(this.jedisPoolConfig(properties))
                .build();

        // GraalVM native-image 不支持 @ConditionalOnProperty 注解
        if ("cluster".equals(this.mode)) {
            RedisProperties.Cluster cluster = properties.getCluster();
            Integer maxRedirects = cluster.getMaxRedirects();
            List<String> nodes = cluster.getNodes();

            RedisClusterConfiguration configuration = new RedisClusterConfiguration();
            configuration.setUsername(properties.getUsername());
            configuration.setPassword(properties.getPassword());
            if (Objects.nonNull(maxRedirects)) {
                configuration.setMaxRedirects(maxRedirects);
            }
            for (String node : nodes) {
                configuration.addClusterNode(RedisNode.fromString(node));
            }
            return new JedisConnectionFactory(configuration, jedisClientConfiguration);
        }

        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
        configuration.setHostName(properties.getHost());
        configuration.setPort(properties.getPort());
        configuration.setUsername(properties.getUsername());
        configuration.setPassword(properties.getPassword());
        return new JedisConnectionFactory(configuration, jedisClientConfiguration);
    }

//    @ConditionalOnProperty(prefix = "nasa.redis", name = "mode", havingValue = "single", matchIfMissing = true)

    /**
     * 业务作用：Jedis连接redis单节点
     *
     * @param connectionFactory 见上述说明
     * @param idGenerate        见上述说明
     * @return 见上述说明。
     */
    @ConditionalOnMissingBean(JedisPool.class)
    @Bean
    public JedisPool jedisPool(RedisConnectionFactory connectionFactory, IdGenerate idGenerate) {
        // GraalVM native-image 不支持 @ConditionalOnProperty 注解
        if ("cluster".equals(this.mode)) {
            return null;
        }
        JedisPool jedisPool = (JedisPool) ReflectUtils.fieldGet(connectionFactory.getConnection(), "pool");
        if (ClassUtils.isPresent("org.apache.ibatis.cache.Cache", this.getClass().getClassLoader())) {
            MybatisJedisCache.setJedisPool(jedisPool);
        }
        if (idGenerate instanceof JedisSnowflake rsf) {
            rsf.setJedisPool(jedisPool);
            rsf.scriptLoad(jedisPool.getResource());
        }
        return jedisPool;
    }

//    @ConditionalOnProperty(prefix = "nasa.redis", name = "mode", havingValue = "cluster")

    /**
     * 业务作用：Jedis连接redis集群
     *
     * @param connectionFactory 见上述说明
     * @param idGenerate        见上述说明
     * @return 见上述说明。
     */
    @ConditionalOnMissingBean(JedisCluster.class)
    @Bean
    public JedisCluster jedisCluster(RedisConnectionFactory connectionFactory, IdGenerate idGenerate) {
        // GraalVM native-image 不支持 @ConditionalOnProperty 注解
        if ("single".equals(this.mode)) {
            return null;
        }
        JedisCluster jedisCluster = (JedisCluster) connectionFactory.getConnection().getNativeConnection();
        if (ClassUtils.isPresent("org.apache.ibatis.cache.Cache", this.getClass().getClassLoader())) {
            MybatisJedisCache.setJedisCluster(jedisCluster);
        }
        if (idGenerate instanceof JedisSnowflake rsf) {
            rsf.setJedisCluster(jedisCluster);
            rsf.scriptLoad(jedisCluster);
        }
        return jedisCluster;
    }

    /**
     * 业务作用：分布式id生成器
     *
     * @return 见上述说明。
     */
    @ConditionalOnMissingBean(IdGenerate.class)
    @Bean
    public IdGenerate idGenerate() {
        JedisSnowflake jedisSnowflake = new JedisSnowflake();
        jedisSnowflake.setEvalSha(evalSha);
        return jedisSnowflake;
    }

}
