package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.core.env.Environment;

/**
 * 业务作用：在业务显式开启 RedisJob 后建立共享管理器与注解登记器，不提供任何默认数据源 Scheduler Bean。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RedisJobProperties.class)
@ConditionalOnProperty(prefix = "nasa.redis.job", name = "enabled", havingValue = "true")
@ImportRuntimeHints(RedisJobRuntimeHints.class)
class RedisJobInfrastructureConfiguration {

    /**
     * 业务作用：建立进程内唯一的多数据源调度器管理器，由它统一驱动实际被引用 source 的生命周期。
     *
     * <p>显式依赖默认 {@link RedisProxy} 只用于保证多数据源代理已经全部完成登记，不会因此创建 primary
     * Scheduler，也不会在 source 缺失时回退到 primary。
     *
     * @param environment 配置环境，用于读取 {@code nasa.redis.job.sources.<id>} 逐源覆盖
     * @param properties  Job 根级默认参数
     * @param redisProxy  默认数据源命令代理，仅承担 Bean 创建顺序约束
     * @return 多数据源调度器管理器。
     */
    @Bean(destroyMethod = "close")
    RedisJobSchedulers redisJobSchedulers(Environment environment, RedisJobProperties properties,
                                           RedisProxy redisProxy) {
        return new RedisJobSchedulers(environment, properties, redisProxy);
    }

    /**
     * 业务作用：在全部单例就绪后发现注解任务，并按每个任务显式声明的 source 登记 Handler。
     *
     * @param context    应用上下文
     * @param schedulers 多数据源调度器管理器
     * @return 注解登记器。
     */
    @Bean
    RedisJobAnnotationRegistrar redisJobAnnotationRegistrar(
            ApplicationContext context, RedisJobSchedulers schedulers) {
        return new RedisJobAnnotationRegistrar(context, schedulers);
    }
}
