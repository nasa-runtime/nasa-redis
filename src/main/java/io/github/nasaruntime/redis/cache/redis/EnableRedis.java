package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.redis.cache.redis.job.RedisJobConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * 业务作用：启用多数据源 RedisTemplate、RedisProxy、分布式锁及按配置开启的 RedisJob 装配。
 */
@SuppressWarnings("unused")
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import({RedisImportBeanDefinitionRegistrar.class, RedisJobConfiguration.class})
public @interface EnableRedis {

    /**
     * 业务作用：选择 Redis 交互实现。
     *
     * @return {@code lettuce} 或 {@code jedis}。
     */
    String value() default "lettuce";

}
