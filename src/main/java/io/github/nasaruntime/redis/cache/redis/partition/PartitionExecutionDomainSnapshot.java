package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.Map;

/**
 * 业务作用：提供不访问 Redis 的冻结域身份、份额和当前责任快照；数值容量不代表内存字节上限。
 * @param scope 执行域类型
 * @param domainId 完整拓扑中的稳定索引
 * @param logicalGroup source 为 null，默认逻辑组为空字符串
 * @param partition 仅 stream 模式包含物理编号
 * @param quotas 固定资源份额
 * @param usage 当前数量使用情况，包含读取等待、Task 预留、重试、确认和 ACK UNKNOWN
 * @param runnerHealthy Runner 健康且本域未锁存故障
 * @param timingWheelHealthy 同名时间轮健康
 * @param stopped Runner 和同名时间轮均已停止
 */
public record PartitionExecutionDomainSnapshot(RedisPartitionProperties.ExecutorScope scope, int domainId,
        String logicalGroup, Integer partition, Map<String, Long> quotas, Map<String, Long> usage,
        boolean runnerHealthy, boolean timingWheelHealthy, boolean stopped) {
    /** 业务作用：保护快照集合不被调用方改变。参数说明: 组件字段与 record 声明一致。返回: 不可变观测值。 */
    public PartitionExecutionDomainSnapshot { quotas = Map.copyOf(quotas); usage = Map.copyOf(usage); }
}
