package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.IdGenerate;
import io.github.nasaruntime.core.config.Graceful;
import io.github.nasaruntime.core.utils.StringUtils;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Objects;

/**
 * Redis workerId 分配与纯 JDK 雪花算法的集成入口。
 * <p>
 * ID 位布局、时钟回拨处理和并发生成逻辑由
 * {@link io.github.nasaruntime.core.base.JdkSnowflake} 提供；本类保留原包名兼容入口，
 * 并通过 {@link EnableSnowflake} 把 Redis workerId 池接入 Spring 应用。
 */
public class Snowflake extends io.github.nasaruntime.core.base.JdkSnowflake {

    /**
     * 业务作用：使用 Redis 集成层分配完成的 workerId 创建雪花 ID 生成器，同时保留原有公共类型兼容性。
     *
     * @param workerId     机器码，范围为 {@code [0, 2^workerIdBits)}
     * @param baseTime     基础时间戳，单位为毫秒
     * @param workerIdBits 机器码位长，范围为 {@code [1, 15]}
     * @param seqBits      序列号位长，范围为 {@code [3, 21]}
     */
    public Snowflake(long workerId, long baseTime, int workerIdBits, int seqBits) {
        super(workerId, baseTime, workerIdBits, seqBits);
    }

    // ==================== 自动配置 ====================

    /**
     * 启用雪花 ID 自动配置。
     * 通过 Redis Sorted Set 自动分配 workerId, 停机时归还。
     * <pre>
     * &#64;EnableSnowflake
     * &#64;boot frameworkApplication
     * public class App { }
     * </pre>
     */
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @Import(JdkSnowflakeConfig.class)
    public @interface EnableSnowflake {

    }

    /**
     * 雪花 ID 自动配置。
     * <p>
     * 通过 Redis Sorted Set ({@code nasa.snowflake.key}) 管理 workerId 池:
     * <ol>
     *   <li>启动时: ZPOPMIN 原子取出并删除最小 workerId</li>
     *   <li>首次启动: 池为空则初始化 [0, workers - 1] 到 Sorted Set</li>
     *   <li>停机时: Graceful shutdown 回调将 workerId 归还到 Sorted Set</li>
     * </ol>
     * 配置项 (application.yml):
     * <pre>
     * nasa:
     *   snowflake:
     *     qualifier: ""                # Redis 源 qualifier, 空则用默认
     *     key: "SNOWFLAKE-WORKERS"     # Redis Sorted Set key
     *     base-time: 1704038400000     # 基础时间戳 (2024-01-01 UTC)
     *     worker-id-bits: 6            # 机器码位长, 最大实例数 = 2^6 = 64
     *     seq-bits: 6                  # 序列号位长, 每毫秒 ID 数 = 2^6 = 64
     * </pre>
     */
    @SuppressWarnings("all")
    @Setter
    @ConfigurationProperties(prefix = "nasa.snowflake")
    static class JdkSnowflakeConfig {

        /* Redis 源 qualifier, 为空使用默认 */
        private String qualifier;
        /* Redis Sorted Set 的 key, 存放可用的 workerId 池 */
        private String key = "SNOWFLAKE-WORKERS";
        /* 基础时间戳, 默认 2024-01-01 00:00:00 UTC */
        private long baseTime = 1704038400000L;
        /* 机器码位长, 默认 6, 最大实例数 = 2^workerIdBits */
        private int workerIdBits = 6;
        /* 序列号位长, 默认 6 */
        private int seqBits = 6;

        /**
         * 业务作用：装配标识生成器。
         * 节点编号从 Redis 申请以保证各节点互不相同——人工配置节点编号一旦重复，
         * 两个节点会产出<b>相同的标识</b>，且冲突要到数据落库时才暴露。
         * Redis 不可用时按本地兜底方式生成，代价是极端情形下的唯一性不再由集中分配保证。
         *
         * @param redisTemplate 用于申请节点编号的模板，可为 null
         * @return 标识生成器。
         */
        @Bean
        public IdGenerate idGenerate(@Autowired(required = false) RedisTemplate<String, Object> redisTemplate) {
            RedisProxy redisProxy = RedisProxy.load();
            if (StringUtils.isNotBlank(qualifier)) {
                redisProxy = RedisProxy.load(qualifier);
                if (Objects.isNull(redisProxy)) {
                    throw new IllegalArgumentException("this conf nasa.snowflake.qualifier=" + qualifier + " is not exists.");
                }
            }
            // 无 Redis 时 workerId 默认 1 (单节点模式)
            long workerId = Objects.isNull(redisProxy) ? 1 : worker(redisProxy);
            return new Snowflake(workerId, baseTime, workerIdBits, seqBits);
        }

        /**
         * 业务作用：从 Redis Sorted Set 中原子获取一个 workerId。
         * 使用 ZPOPMIN 一次性取最小并删除, 避免并发启动时多个实例拿到同一个 workerId。
         * 池为空时自动初始化 {@code [0, 2^workerIdBits)} 后重试。
         *
         * @param redisProxy 命令代理，决定连接与序列化方式
         * @return 已从可用池原子摘除、供当前实例独占使用的 workerId。
         */
        private long worker(RedisProxy redisProxy) {
            RedisSerializer<Object> valueSerializer = redisProxy.getValueSerializer();

            // ZPOPMIN 将 workerId 从可用池中原子摘除，避免正常分配阶段多个存活实例共享同一编号。
            LettucePipeline.Actuator actuator = LettucePipeline.open(redisProxy, null);
            LettuceFuture<RedisFuture<ScoredValue<byte[]>>> lf = LettuceFuture.of();
            actuator.zPopMinAsync(key, lf);
            actuator.pipeline();

            ScoredValue<byte[]> rs = lf.getFinally();
            Integer worker = rs == null || !rs.hasValue() ? null : (Integer) valueSerializer.deserialize(rs.getValue());
            if (Objects.nonNull(worker)) {
                // 仅在正常停机时归还编号；编号归池前始终代表当前实例的全局唯一性边界。
                Graceful.registry(() -> redisProxy.zAdd(key, worker, worker));
                return worker;
            }
            // 首次启动需要建立完整编号池，后续调用再通过 ZPOPMIN 取得独占编号。
            int maxWorkers = 1 << workerIdBits;
            for (int i = 0; i < maxWorkers; i++) {
                actuator.zAdd(key, i, i);
            }
            actuator.pipeline();
            return worker(redisProxy);
        }
    }
}
