package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：统一表示普通 Run 与 Fanout shard 的持久状态。
 */
public enum RedisJobState {

    CREATED,
    QUEUED,
    BLOCKED,
    RUNNING,
    RETRY_WAIT,
    FANOUT_CREATING,
    WAITING_CHILDREN,
    AWAITING_CAPABILITY,
    AWAITING_RECEIPT,
    RECEIVED,
    SUCCEEDED,
    FAILED,
    DEAD,
    SKIPPED,
    CANCELLED;

    /**
     * 业务作用：判定记录是否已经关闭执行权并可进入保留期。
     *
     * @return 进入终态返回 true。
     */
    public boolean terminal() {
        return this == SUCCEEDED || this == FAILED || this == DEAD
                || this == SKIPPED || this == CANCELLED;
    }
}
