package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.DistributedLock;
import io.github.nasaruntime.core.utils.ColUtils;
import io.github.nasaruntime.core.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * 业务作用：承载 Redisson 客户端的配置，供需要其高级数据结构的场景使用。
 * 本组件的分布式锁默认走自带的 Lua 实现，不依赖 Redisson；
 * 仅在显式启用时才需要这份配置。
 */
@Setter
@Getter
public class NasaRedissonConfig {

    /* redisson分布锁设置key过期时间 */
    @Value("${nasa.redis.lock-watchdog-timeout:30000}")
    private int lockWatchdogTimeout = 30000;
    /* redis启动环境：single、cluster */
    @Value("${nasa.redis.mode:single}")
    private String mode;

    /**
     * 业务作用：按配置装配 Redisson 客户端，供需要其高级数据结构的场景使用。
     *
     * @param properties 连接配置
     * @return Redisson 客户端。
     */
    @ConditionalOnMissingBean(RedissonClient.class)
    @Bean
    public RedissonClient redissonClient(RedisProperties properties) {
        Config config = new Config();
        config.setLockWatchdogTimeout(lockWatchdogTimeout);
        Duration timeout = properties.getConnectTimeout();
        String username = properties.getUsername();
        String password = properties.getPassword();

        // GraalVM native-image 不支持 @ConditionalOnProperty 注解
        if ("cluster".equals(this.mode)) {
            ClusterServersConfig clusterServersConfig = config.useClusterServers();
            clusterServersConfig.setUsername(username);
            clusterServersConfig.setPassword(password);
            if (Objects.nonNull(timeout)) {
                clusterServersConfig.setConnectTimeout((int) timeout.toMillis());
            }
            List<String> nodes = properties.getCluster().getNodes();
            clusterServersConfig.addNodeAddress(ColUtils.toArray(nodes, t -> true, t -> "redis://" + t));
            return Redisson.create(config);
        }

        String host = properties.getHost();
        int database = properties.getDatabase();

        SingleServerConfig singleServerConfig = config.useSingleServer();
        // Redisson连接redis必须要在地址前面添加这个前缀，不然要报错
        singleServerConfig.setAddress(StringUtils.concat("redis://", host, ":", properties.getPort()));
        singleServerConfig.setUsername(username);
        singleServerConfig.setPassword(password);
        if (Objects.nonNull(timeout)) {
            singleServerConfig.setConnectTimeout((int) timeout.toMillis());
        }
        singleServerConfig.setDatabase(database);
        return Redisson.create(config);
    }

    /**
     * 业务作用：装配基于 Redisson 的分布式锁入口。
     * 与本组件自带的 Lua 实现<b>二选一</b>：两者的锁键格式不同，混用会让互斥失效。
     *
     * @param redissonClient Redisson 客户端
     * @return 分布式锁入口。
     */
    @ConditionalOnMissingBean(DistributedLock.class)
    @Bean
    public DistributedLock distributedLock(RedissonClient redissonClient) {
        return redissonClient::getLock;
    }

}
