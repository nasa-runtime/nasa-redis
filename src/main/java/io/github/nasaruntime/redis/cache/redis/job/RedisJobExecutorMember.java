package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：描述冻结能力快照中的一个兼容执行器及其稳定节点身份。
 *
 * @param nodeIdentity 跨重启稳定的节点身份
 * @param executorId 当前进程执行器身份
 * @param startupId 当前进程启动标识
 * @param applicationName 应用名
 * @param runtime 语言运行时
 * @param implementationDigest 具体实现摘要
 * @param heartbeatRevision 心跳修订号
 */
public record RedisJobExecutorMember(
        String nodeIdentity,
        String executorId,
        String startupId,
        String applicationName,
        String runtime,
        String implementationDigest,
        long heartbeatRevision
) {
}
