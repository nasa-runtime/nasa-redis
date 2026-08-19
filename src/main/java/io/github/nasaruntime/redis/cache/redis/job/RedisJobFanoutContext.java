package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：向 Fanout Worker 暴露稳定分片身份与当前 assignment 权威信息。
 */
public interface RedisJobFanoutContext {

    /**
     * 业务作用：读取 Fanout 根标识。 @return Fanout 根标识。
     */
    String fanoutId();

    /**
     * 业务作用：读取普通根 Run 标识。 @return 根 Run 标识。
     */
    String rootRunId();

    /**
     * 业务作用：读取冻结能力快照标识。 @return 快照标识。
     */
    String snapshotId();

    /**
     * 业务作用：读取当前分片下标。 @return 从零开始的分片下标。
     */
    int shardIndex();

    /**
     * 业务作用：读取冻结的分片总数。 @return 分片总数。
     */
    int shardTotal();

    /**
     * 业务作用：读取跨重发保持不变的执行序号。 @return 执行序号。
     */
    long seq();

    /**
     * 业务作用：读取供外部副作用去重的稳定键。 @return 执行业务幂等键。
     */
    String executionKey();

    /**
     * 业务作用：读取本次 assignment 的目标节点身份。 @return 稳定节点身份。
     */
    String targetNodeIdentity();

    /**
     * 业务作用：读取每次 assignment 重建时单调增加的代次，用于拒绝旧目标或旧启动实例的迟到提交。
     *
     * @return assignment 代次；换节点以及原稳定节点凭新启动或心跳证据恢复时都会增加。
     */
    long assignmentEpoch();
}
