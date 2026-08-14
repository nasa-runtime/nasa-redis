package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：为编程式注册提供与注解方法一致的 Handler 调用合同。
 */
@FunctionalInterface
public interface RedisJobHandler {

    /**
     * 业务作用：在框架成功取得当前 attempt 执行权后处理任务。
     *
     * @param context 当前执行上下文
     * @return 业务结果；返回 null 按成功处理。
     * @throws Exception 业务异常，框架按定义的重试策略收敛
     */
    RedisJobResult handle(RedisJobContext context) throws Exception;
}
