package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：向查询调用方返回普通 Run 的持久权威状态与当前 attempt 信息。
 *
 * @param runId         Run 标识
 * @param jobName       任务名
 * @param workerName    Worker 名
 * @param state         当前状态
 * @param logicalFireAt 逻辑触发时刻
 * @param triggeredAt   实际触发时刻
 * @param attempt       当前 attempt
 * @param attemptToken  当前 fencing token
 * @param owner         当前执行 owner
 * @param leaseUntil    当前租约截止时刻
 * @param resultCode    结果码
 * @param resultSummary 有界结果摘要
 * @param errorType     错误类型
 * @param errorSummary  有界错误摘要
 */
public record RedisJobRun(
        String runId,
        String jobName,
        String workerName,
        RedisJobState state,
        long logicalFireAt,
        long triggeredAt,
        int attempt,
        long attemptToken,
        String owner,
        long leaseUntil,
        String resultCode,
        String resultSummary,
        String errorType,
        String errorSummary
) {
}
