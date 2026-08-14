package io.github.nasaruntime.redis.cache;

import io.github.nasaruntime.redis.cache.redis.EnableRedis;
import io.github.nasaruntime.core.feature.NasaNativeFeature;
import io.github.nasaruntime.redis.cache.redis.RedisImportBeanDefinitionRegistrar;
import org.springframework.aot.hint.*;
import org.springframework.lang.NonNull;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * Nasa
 * 基础注解的RuntimeHints
 * 用于GraalVM的native-image原生镜像打包
 */
class NasaCacheRuntimeHints implements RuntimeHintsRegistrar {

    /**
     * 业务作用：为原生镜像登记本组件在运行期通过反射触达的类型。
     * 原生镜像的构建期闭包分析看不到只被反射使用的类型，不登记会在运行期才报找不到类。
     *
     * @param hints       构建期收集提示的容器
     * @param classLoader 用于解析类名的类加载器
     * 返回: 无返回值。
     */
    @Override
    public void registerHints(@NonNull RuntimeHints hints, ClassLoader classLoader) {

        ReflectionHints reflectionHints = hints.reflection();
        ResourceHints resourceHints = hints.resources();

        reflectionHints.registerType(EnableRedis.class, MemberCategory.values());
        reflectionHints.registerType(RedisImportBeanDefinitionRegistrar.class, MemberCategory.values());

        resourceHints.registerPatternIfPresent(classLoader, "lua", (hint) -> hint.includes("lua/*"));

        try {
            if (ClassUtils.isPresent("org.redisson.api.RedissonClient", classLoader)) {
                reflectionHints.registerType(ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.NasaRedissonConfig", classLoader), MemberCategory.values());
            }
            if (ClassUtils.isPresent("io.lettuce.core.api.async.BaseRedisAsyncCommands", classLoader)) {
                // 是Lettuce
                // RedisTemplate.execute() 在 cluster 模式下创建 RedisClusterConnection 的 JDK 代理
                if (ClassUtils.isPresent("org.springframework.data.redis.connection.RedisClusterConnection", classLoader)) {
                    hints.proxies().registerJdkProxy(
                            ClassUtils.forName("org.springframework.data.redis.connection.RedisClusterConnection", classLoader),
                            ClassUtils.forName("org.springframework.data.redis.connection.DefaultedRedisClusterConnection", classLoader),
                            ClassUtils.forName("org.springframework.data.redis.connection.RedisConnection", classLoader)
                    );
                }
                reflectionHints.registerTypes(TypeReference.listOf(
                        ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.NasaLettuceConfig.RedisProperties", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.NasaLettuceConfig.Stream", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.NasaLettuceConfig.Group", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.NasaLettuceConfig.DistributedLockProperties", classLoader)
                        // Partition 配置 (Spring @ConfigurationProperties 嵌套反射注入)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.NasaLettuceConfig.Partition", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.NasaLettuceConfig.PartitionGroup", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.LettuceFuture", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.LettucePipeline", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.LettucePipeline.Actuator", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.LettucePipeline.CmdBuffer", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.RedisProxy", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.Subscriber", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.LettuceDistributedLock", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.LettuceDistributedLock.RedisLock", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.LettuceDistributedLock.LockSubscriptionRegistry", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.LettuceDistributedLock.Subscription", classLoader)
                        // 消费模式枚举 (yml 反射 setter + Enum.valueOf)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.ConsumeMode", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.PipelineConnectionPool", classLoader)
                        // RedisPartition 体系 (框架核心 + Jackson 反序列化 PooledEvtData 反射 getter/setter)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.RedisPartition", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.RedisPartition.PartitionGroup", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.RedisPartition.Claim", classLoader)
                        // 本地维护的 stream container 体系 + lifecycle hook + managed runner
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.stream.BatchStreamMessageListenerContainer", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.stream.BatchStreamMessageListenerContainer.ManagedRunner", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.stream.BatchStreamMessageListenerContainer.TaskSubscription", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.stream.BatchStreamPollTask", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.stream.BatchStreamPollTask.PollState", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.stream.PollLifecycle", classLoader)
                ), TypeHint.builtWith(MemberCategory.values()));
                if (ClassUtils.isPresent("org.apache.ibatis.cache.Cache", classLoader)) {
                    reflectionHints.registerType(ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.MybatisCache", classLoader), MemberCategory.values());
                }
            }
            else if (ClassUtils.isPresent("redis.clients.jedis.JedisPool", classLoader)) {
                // 是Jedis
                Class<?> jedisConnection = ClassUtils.forName("org.springframework.data.redis.connection.jedis.JedisConnection", classLoader);

                Field pool = ReflectionUtils.findField(jedisConnection, "pool");
                if (Objects.nonNull(pool)) {
                    reflectionHints.registerField(pool);
                }
                reflectionHints.registerTypes(TypeReference.listOf(
                        ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.NasaJedisConfig", classLoader)
                        , ClassUtils.forName("io.github.nasaruntime.redis.cache.redis.JedisSnowflake", classLoader)
                ), TypeHint.builtWith(MemberCategory.values()));
                if (ClassUtils.isPresent("org.apache.ibatis.cache.Cache", classLoader)) {
                    reflectionHints.registerType(ClassUtils.forName("io.github.nasaruntime.redis.cache.MybatisJedisCache", classLoader), MemberCategory.values());
                }
            }

        } catch (Exception e) {
            throw new Error(e.getMessage(), e);
        }

        // Caffeine: LocalCacheFactory 通过 Class.forName 动态加载实现类 (如 SSWR/SSMSA 等)
        if (ClassUtils.isPresent("com.github.benmanes.caffeine.cache.Caffeine", classLoader)) {
            NasaNativeFeature.allClassesIfPresent(
                    "com.github.benmanes.caffeine.cache",
                    io.github.nasaruntime.core.feature.NasaNativeFeature::isNotInterface,
                    io.github.nasaruntime.core.feature.NasaNativeFeature::isNotAbstract
            ).forEach(clazz -> reflectionHints.registerType(clazz, MemberCategory.values()));
        }
    }

}
