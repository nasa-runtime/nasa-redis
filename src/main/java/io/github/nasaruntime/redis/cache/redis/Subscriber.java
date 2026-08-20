package io.github.nasaruntime.redis.cache.redis;

import java.lang.annotation.*;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 业务作用：声明方法需要订阅的 Redis Pub/Sub 频道及其数据源，消息反序列化沿用对应 RedisTemplate 配置。
 *
 * @see io.github.nasaruntime.redis.cache.redis.Subscribe
 */
@Target(METHOD)
@Retention(RUNTIME)
@Repeatable(Subscriber.List.class)
@Documented
@Inherited
public @interface Subscriber {

    /**
     * 业务作用：订阅的 channel
     *
     * @return 见上述说明。
     */
    String[] value();

    /**
     * 业务作用：限定订阅使用的 Redis 数据源，避免同一频道在无关数据源上重复注册。
     *
     * <p>参数说明: 无。
     *
     * @return RedisProxy qualifier 列表；空数组表示采用框架约定的默认选择规则。
     */
    String[] qualifier() default {};

    /**
     * 1.8多注解复用
     */
    @Target(METHOD)
    @Retention(RUNTIME)
    @Documented
    @Inherited
    @interface List {
        /**
         * 业务作用：承载同一处重复标注的多个订阅声明，使一个类能同时订阅多个来源。
         * 由编译器在展开重复注解时自动使用，业务代码不直接书写。
         *
         * <p>参数说明: 无。
         *
         * @return 该处的全部订阅声明。
         */
        Subscriber[] value();
    }
}
