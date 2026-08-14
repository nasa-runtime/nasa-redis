package io.github.nasaruntime.redis.cache.redis.job;

import java.util.List;

/**
 * 业务作用：冻结某个 Worker 在一个 Redis 时刻的兼容执行器集合和规范摘要。
 *
 * @param snapshotId 快照标识
 * @param workerName Worker 名
 * @param selectedAt Redis 选择时刻
 * @param members 有序成员列表
 * @param snapshotDigest 规范成员摘要
 */
public record RedisJobClusterSnapshot(
        String snapshotId,
        String workerName,
        long selectedAt,
        List<RedisJobExecutorMember> members,
        String snapshotDigest
) {
    /**
     * 业务作用：复制成员列表，防止能力快照在 Fanout 提交期间被调用方改写。
     *
     * @param snapshotId 快照标识
     * @param workerName Worker 名
     * @param selectedAt Redis 选择时刻
     * @param members 成员列表
     * @param snapshotDigest 快照摘要
     */
    public RedisJobClusterSnapshot {
        members = List.copyOf(members);
    }
}
