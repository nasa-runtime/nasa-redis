package io.github.nasaruntime.redis.cache.redis;

import java.lang.annotation.*;

/**
 * 业务作用：声明 MyBatis 联表查询缓存所依赖的其它缓存类型，使关联表发生事务写入时能够同步清理当前缓存。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface CacheClearRef {

    /**
     * 业务作用：列出会使当前缓存失效的关联缓存类型。
     *
     * <p>参数说明: 无。
     *
     * @return 关联缓存类型；空数组表示没有额外关联。
     */
    Class[] value() default {};

}
