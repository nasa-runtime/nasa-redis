package io.github.nasaruntime.redis.cache.redis;

import java.lang.annotation.*;

/**
 * Nasa
 * mybatis多表联查时，当关联的表发生事务操作需要clear时，需要指定是否clear当前缓存
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface CacheClearRef {

    Class[] value() default {};

}
