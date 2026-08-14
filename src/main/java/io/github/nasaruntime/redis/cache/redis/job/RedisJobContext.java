package io.github.nasaruntime.redis.cache.redis.job;

import java.util.Optional;

/**
 * 业务作用：向 Handler 暴露稳定运行身份、取消信号、失权门禁和 Fanout 创建入口。
 */
public interface RedisJobContext {

    /**
     * 业务作用：读取调度命名空间。 @return 命名空间。
     */
    String namespace();

    /**
     * 业务作用：读取任务名。 @return 任务名。
     */
    String jobName();

    /**
     * 业务作用：读取跨重试稳定的 Run 标识。 @return Run 标识。
     */
    String runId();

    /**
     * 业务作用：读取计划执行的逻辑时刻。 @return UTC 毫秒时刻。
     */
    long logicalFireAt();

    /**
     * 业务作用：读取 Redis 实际生成 Run 的时刻。 @return UTC 毫秒时刻。
     */
    long triggeredAt();

    /**
     * 业务作用：读取当前实际启动次数。 @return attempt 序号。
     */
    int attempt();

    /**
     * 业务作用：读取当前持权代次令牌。 @return attempt token。
     */
    long attemptToken();

    /**
     * 业务作用：按 Worker 登记的确定类型解码参数，不根据消息内容动态加载类型。
     *
     * @param type 目标类型
     * @param <T>  目标类型
     * @return 解码后的参数。
     */
    <T> T parameter(Class<T> type);

    /**
     * 业务作用：读取线协议原始字节，供 Protobuf 或自定义 Codec 使用。 @return 参数副本。
     */
    byte[] rawParameter();

    /**
     * 业务作用：读取参数 Schema 标识。 @return Schema 标识。
     */
    String schemaId();

    /**
     * 业务作用：读取参数编码方式。 @return 编码方式。
     */
    RedisJobWireCodec wireCodec();

    /**
     * 业务作用：判定控制面是否已经请求协作式取消。 @return 已请求取消返回 true。
     */
    boolean isCancellationRequested();

    /**
     * 业务作用：依据本地保守截止点判定当前 attempt 是否仍有副作用权威。 @return 仍持权返回 true。
     */
    boolean stillOwnsExecution();

    /**
     * 业务作用：在业务安全点同时执行取消与失权门禁。 返回：门禁通过时正常返回，否则抛出停止异常。
     */
    void checkpoint();

    /**
     * 业务作用：创建只面向声明兼容 Worker 节点的 Fanout 构建器。
     *
     * @param workerName Worker 能力名
     * @return Fanout 构建器。
     */
    RedisJobFanoutBuilder fanout(String workerName);

    /**
     * 业务作用：在定向 Worker 中读取当前分片元数据。 @return 普通任务为空。
     */
    Optional<RedisJobFanoutContext> fanoutContext();
}
