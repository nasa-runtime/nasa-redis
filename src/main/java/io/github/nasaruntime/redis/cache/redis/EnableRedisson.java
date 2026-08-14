package io.github.nasaruntime.redis.cache.redis;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * 启动RedissonClient装配
 */
@SuppressWarnings("unused")
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(NasaRedissonConfig.class)
public @interface EnableRedisson {

}
