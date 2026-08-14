package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：在普通根上下文与 Fanout 持久协议之间提供受控创建入口。
 */
interface RedisJobFanoutService {

    /**
     * 业务作用：创建绑定根 attempt 和目标 Worker 的 Fanout 构建器。
     *
     * @param rootContext 根执行上下文
     * @param workerName 目标 Worker
     * @return Fanout 构建器。
     */
    RedisJobFanoutBuilder builder(DefaultRedisJobContext rootContext, String workerName);
}
