package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：在取消或失权后立即终止 Handler 的后续业务副作用。
 */
public final class RedisJobExecutionStoppedException extends RuntimeException {
    /**
     * 业务作用：建立带稳定停止原因的运行异常。
     *
     * @param message 停止原因
     */
    public RedisJobExecutionStoppedException(String message) {
        super(message);
    }
}
