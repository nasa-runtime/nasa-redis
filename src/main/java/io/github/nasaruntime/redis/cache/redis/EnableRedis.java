package io.github.nasaruntime.redis.cache.redis;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * 业务作用：启用多数据源 RedisTemplate、RedisProxy 与分布式锁装配。
 * RedisJob 具有独立的生命周期与数据源选择入口，需要业务显式使用
 * {@link io.github.nasaruntime.redis.cache.redis.job.EnableRedisJob} 开启。
 */
@SuppressWarnings("unused")
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(RedisImportBeanDefinitionRegistrar.class)
public @interface EnableRedis {

    /**
     * 业务作用：选择 Redis 交互实现。
     *
     * @return {@code lettuce} 或 {@code jedis}。
     */
    String value() default "lettuce";

}
