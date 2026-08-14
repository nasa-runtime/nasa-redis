package io.github.nasaruntime.redis.cache.redis.job;

import java.util.Collection;

/**
 * 业务作用：收集一次 Fanout 的契约、分片输入和失败策略，并在 dispatch 时冻结能力快照。
 */
public interface RedisJobFanoutBuilder {

    /**
     * 业务作用：选择本次 Fanout 使用的跨语言契约修订号。
     *
     * @param revision 契约修订号
     * @return 当前构建器。
     */
    RedisJobFanoutBuilder contractRevision(long revision);

    /**
     * 业务作用：选择本次分片参数的 Schema。
     *
     * @param schemaId Schema 标识
     * @return 当前构建器。
     */
    RedisJobFanoutBuilder schema(String schemaId);

    /**
     * 业务作用：选择本次分片参数的线编码。
     *
     * @param codec 编码方式
     * @return 当前构建器。
     */
    RedisJobFanoutBuilder codec(RedisJobWireCodec codec);

    /**
     * 业务作用：保存待切分业务数据和分片算法，实际分片数由 dispatch 时冻结的能力快照决定。
     *
     * @param items       待切分业务数据
     * @param partitioner 分片算法
     * @param <T>         数据类型
     * @return 当前构建器。
     */
    <T> RedisJobFanoutBuilder partition(Collection<T> items, RedisJobPartitioner<T> partitioner);

    /**
     * 业务作用：直接提供已经按目标成员顺序编码的分片参数。
     *
     * @param payloads 分片参数
     * @return 当前构建器。
     */
    RedisJobFanoutBuilder shards(Collection<RedisJobPayload> payloads);

    /**
     * 业务作用：选择目标失联或执行失败时的收敛策略。
     *
     * @param policy 失败策略
     * @return 当前构建器。
     */
    RedisJobFanoutBuilder failurePolicy(RedisJobFanoutFailurePolicy policy);

    /**
     * 业务作用：冻结兼容执行器快照并以确定性 Fanout intent 提交全部分片。
     *
     * @return 已接受 Fanout intent 时返回成功；提交前校验失败时抛出异常。
     */
    RedisJobResult dispatch();
}
