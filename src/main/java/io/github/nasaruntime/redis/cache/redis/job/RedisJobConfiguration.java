package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;

/**
 * 业务作用：在显式启用 RedisJob 时装配调度门面和注解方法登记器。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RedisJobProperties.class)
@ConditionalOnProperty(prefix = "nasa.redis.job", name = "enabled", havingValue = "true")
@ImportRuntimeHints(RedisJobRuntimeHints.class)
public class RedisJobConfiguration {

    /**
     * 业务作用：建立唯一 RedisJobScheduler，并由其 Spring 生命周期控制任务领取。
     *
     * @param redisProxy 默认 Redis 命令代理
     * @param properties Job 配置
     * @return 调度门面。
     */
    @Bean
    public RedisJobScheduler redisJobScheduler(RedisProxy redisProxy, RedisJobProperties properties) {
        return new RedisJobScheduler(redisProxy, properties);
    }

    /**
     * 业务作用：装配注解方法扫描器，在全部单例创建后登记 Handler。
     *
     * @param context 应用上下文
     * @param scheduler 调度门面
     * @return 注解登记器。
     */
    @Bean
    public RedisJobAnnotationRegistrar redisJobAnnotationRegistrar(
            ApplicationContext context, RedisJobScheduler scheduler) {
        return new RedisJobAnnotationRegistrar(context, scheduler);
    }
}
