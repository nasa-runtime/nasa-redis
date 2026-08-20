package io.github.nasaruntime.redis.cache.redis.job;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 业务作用：显式开启 RedisJob 的配置绑定、注解任务发现和多数据源 Scheduler 生命周期。
 *
 * <p>本入口本身不创建任何具体数据源的 Scheduler。只有 {@link RedisJob#qualifier()} 实际引用某个
 * source，或业务调用 {@link RedisJobSchedulers#scheduler(String)} 时，才为该 source 惰性建立独立运行时。
 * 仅声明本注解但没有设置 {@code nasa.redis.job.enabled=true} 时不建立 RedisJob 基础设施。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(RedisJobInfrastructureConfiguration.class)
public @interface EnableRedisJob {
}
