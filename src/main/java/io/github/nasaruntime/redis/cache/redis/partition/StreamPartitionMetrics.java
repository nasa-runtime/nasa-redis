package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;

/**
 * 业务作用：隔离可选指标后端与 Stream Partition 核心状态机；后端不存在时消费语义不发生变化。
 */
public interface StreamPartitionMetrics extends AutoCloseable {

    StreamPartitionMetrics NOOP = new StreamPartitionMetrics() {};

    /**
     * 业务作用：在分区入口建立或后绑定指标后端时登记现有运行时和后续分组。
     *
     * @param partition 当前 RedisProxy 的分区入口
     *                  返回: 无返回值。
     */
    default void partitionAvailable(RedisPartition partition) {}

    /**
     * 业务作用：为新发布的逻辑分组登记固定低基数指标，不把业务 key 或 record id 带入标签。
     *
     * @param partition 当前 RedisProxy 的分区入口
     * @param groupName 逻辑分组名；默认组为空字符串
     *                  返回: 无返回值。
     */
    default void groupAvailable(RedisPartition partition, String groupName) {}

    /**
     * 业务作用：为已激活计划预登记稳定订阅、模式、顺序结果与 Runner 健康指标。
     *
     * @param partition 当前分区入口
     * @param plan      已发布且生命周期稳定的订阅计划
     *                  返回: 无返回值；相同计划重复通知保持幂等。
     */
    default void planAvailable(RedisPartition partition, StreamSubscriptionPlan plan) {}

    /**
     * 业务作用：记录 partitionKey 计算耗时、粗类型与成功结果。参数说明: 计划、类型、结果与耗时。返回: 无返回值。
     */
    default void partitionKey(StreamSubscriptionPlan plan, String kind, String result, long elapsedNanos) {}

    /**
     * 业务作用：记录 Redis 批量大小或拆分 Task 数。参数说明: 来源、阶段、恢复标志与数值。返回: 无返回值。
     */
    default void batch(StreamRecordSource source, String stage, boolean recovery, long value) {}

    /**
     * 业务作用：记录 ordered bucket 大小。参数说明: 计划与 bucket 数量。返回: 无返回值。
     */
    default void orderedBucket(StreamSubscriptionPlan plan, long size) {}

    /**
     * 业务作用：按稳定计划维度累计 Partition Task 终态。参数说明: 计划、顺序属性与终态。返回: 无返回值。
     */
    default void taskOutcome(StreamSubscriptionPlan plan, boolean ordered, ConsumeStatus outcome) {}

    /**
     * 业务作用：记录 Task 排队、listener 或 Future 等待阶段耗时。参数说明: 计划、顺序属性、阶段与耗时。返回: 无返回值。
     */
    default void taskLatency(StreamSubscriptionPlan plan, boolean ordered, String stage, long elapsedNanos) {}

    /**
     * 业务作用：累计 Partition Submission 的明确取消结果。参数说明: 计划、顺序属性与结果。返回: 无返回值。
     */
    default void submission(StreamSubscriptionPlan plan, boolean ordered, String result) {}

    /**
     * 业务作用：累计 PEL exact、route recovery 与 XAUTOCLAIM 的固定结果。参数说明: 来源、类型与结果。返回: 无返回值。
     */
    default void recovery(StreamRecordSource source, String type, String result) {}

    /**
     * 业务作用：累计 fencing ACK 与 XPENDING 复验的固定结果。参数说明: 来源、阶段与结果。返回: 无返回值。
     */
    default void ack(StreamRecordSource source, String stage, String result) {}

    /**
     * 业务作用：撤销当前代理登记的全部 meter，使 Spring 上下文重建不会遗留旧实例引用。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值。
     */
    @Override
    default void close() {}

    /**
     * 业务作用：在所有 Spring 单例就绪后探测 MeterRegistry，并为每个 RedisProxy 装配内部指标桥接。
     *
     * @param context Spring 应用上下文
     *                返回: 无返回值；没有 Micrometer 或 MeterRegistry 时保持无操作。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static void bindAvailable(ApplicationContext context) {
        Class<?> registryType;
        try {
            registryType = Class.forName("io.micrometer.core.instrument.MeterRegistry");
        } catch (ClassNotFoundException absent) {
            return;
        }
        Map<String, ?> registryBeans = context.getBeansOfType((Class) registryType);
        if (registryBeans.isEmpty()) return;
        try {
            Class<?> implementation = Class.forName(
                    "io.github.nasaruntime.redis.cache.redis.partition.MicrometerStreamPartitionMetrics");
            Constructor<?> constructor = implementation.getDeclaredConstructor(Collection.class);
            constructor.setAccessible(true);
            for (RedisProxy redisProxy : context.getBeansOfType(RedisProxy.class).values()) {
                StreamPartitionMetrics metrics = (StreamPartitionMetrics) constructor.newInstance(
                        registryBeans.values());
                redisProxy.installStreamPartitionMetrics(metrics);
            }
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Stream Partition metrics binding failed", failure);
        }
    }
}
