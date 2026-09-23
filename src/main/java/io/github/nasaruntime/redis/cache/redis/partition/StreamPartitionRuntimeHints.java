package io.github.nasaruntime.redis.cache.redis.partition;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;

/**
 * 业务作用：为 native-image 保留分区运行时、嵌套配置与可选指标后端需要的反射入口。
 */
final class StreamPartitionRuntimeHints implements RuntimeHintsRegistrar {

    /**
     * 业务作用：登记分区配置绑定与运行时反射类型，并按字符串保留可选指标实现，避免缺少 Micrometer 时产生类加载依赖。
     *
     * @param hints      当前 native-image 提示集合
     * @param classLoader 构建期类加载器
     * 返回: 无返回值。
     */
    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        // 配置由 Spring 反射绑定，嵌套类型也须显式保留，确保容量与权威参数在原生镜像中生效。
        for (Class<?> type : new Class<?>[]{
                RedisPartitionProperties.class,
                RedisPartitionProperties.PartitionGroup.class,
                RedisPartitionProperties.LocalConsumer.class,
                RedisPartitionProperties.Executor.class,
                RedisPartitionProperties.ExecutorScope.class,
                RedisPartitionProperties.PartitionKeyLayout.class,
                RedisPartitionProperties.PoisonPolicy.class,
                RedisPartition.class,
                RedisPartition.PartitionGroup.class,
                RedisPartition.Claim.class}) {
            hints.reflection().registerType(type, MemberCategory.values());
        }
        hints.reflection().registerType(
                TypeReference.of(
                        "io.github.nasaruntime.redis.cache.redis.partition.MicrometerStreamPartitionMetrics"),
                builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS));
        hints.reflection().registerType(
                TypeReference.of("io.micrometer.core.instrument.MeterRegistry"),
                builder -> builder.withMembers(
                        MemberCategory.DECLARED_FIELDS,
                        MemberCategory.INVOKE_DECLARED_METHODS));
        hints.reflection().registerType(
                TypeReference.of("io.micrometer.core.instrument.composite.AbstractCompositeMeter"),
                builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_METHODS));
    }
}
