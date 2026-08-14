package io.github.nasaruntime.redis.cache.redis.job;

import java.util.List;

/**
 * 业务作用：按冻结的兼容执行器数量把业务数据切成一一对应的参数分片。
 *
 * @param <T> 业务数据类型
 */
@FunctionalInterface
public interface RedisJobPartitioner<T> {
    /**
     * 业务作用：生成与成员数完全相同且顺序稳定的分片，空分片也必须显式保留。
     *
     * @param items 不可变输入列表
     * @param memberCount 冻结快照成员数
     * @return 与成员数相同的有序分片列表。
     */
    List<List<T>> partition(List<T> items, int memberCount);
}
