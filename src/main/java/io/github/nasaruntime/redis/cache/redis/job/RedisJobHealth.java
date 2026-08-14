package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：汇总调度生命周期和最近控制面成功时间，供应用健康检查直接消费。
 *
 * @param status UP、DRAINING、DEGRADED 或 DOWN
 * @param running 调度生命周期是否运行
 * @param draining 是否停止领取新任务
 * @param lastHeartbeatAt 最近成功心跳的应用观测时间
 * @param lastScheduleScanAt 最近成功调度扫描的应用观测时间
 */
public record RedisJobHealth(String status, boolean running, boolean draining,
                             long lastHeartbeatAt, long lastScheduleScanAt) {
}
