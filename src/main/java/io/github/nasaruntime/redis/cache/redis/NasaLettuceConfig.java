package io.github.nasaruntime.redis.cache.redis;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.nasaruntime.core.base.DistributedLock;
import io.github.nasaruntime.core.config.Graceful;
import io.github.nasaruntime.core.utils.ReflectUtils;
import io.github.nasaruntime.core.utils.StringUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.*;
import java.util.function.Function;

/**
 * 自定义多redis数据源配置：
 * nasa:
 *   redis:
 *     properties:
 *       primary:
 *         mode: single
 *         host: 127.0.0.1
 *         port: 6379
 *         password: a123456
 *       test:
 *         mode: cluster
 *         password: a123456
 *         cluster:
 *           nodes:
 *             - 192.168.2.11:7001
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "nasa.redis")
@EnableConfigurationProperties(RedisIdempotentCounterProperties.class)
public class NasaLettuceConfig {

    private final Map<String, RedisProperties> properties = new LinkedHashMap<>();

    /* 所有 RedisProxy 实例共享同一套 nonce 账本合同，避免同一应用内因数据源装配路径不同形成两种布局。 */
    @Autowired
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private RedisIdempotentCounterProperties idempotentCounterProperties = new RedisIdempotentCounterProperties();

    // 主数据源 primary 在装配循环里建出的实例暂存于此, 由下方三个 @Bean 方法回传给容器完成注册。
    // 不能像非主源那样在循环里 registerSingleton: 这三个 Bean 的名字与 @Bean 方法同名,
    // Spring 之后仍会用方法返回值注册一次同名单例, 两次注册撞名会让整个应用启动失败。
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private RedisTemplate<String, Object> primaryRedisTemplate;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private RedisProxy primaryRedisProxy;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private DistributedLock primaryDistributedLock;

    private final Function<Boolean, Jackson2JsonRedisSerializer<Object>> jacksonMapper = b -> {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        objectMapper.registerModule(new JavaTimeModule());
        if (b) {
            // 让redis序列化后的json带上类信息，反序列化时才知道反序列化成什么
            objectMapper.activateDefaultTyping(objectMapper.getPolymorphicTypeValidator(), ObjectMapper.DefaultTyping.NON_FINAL);
        }
        return new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);
    };

    /**
     * 业务作用：装配值的 JSON 序列化方式。
     * 容器内已有自定义序列化器时优先沿用，使业务能统一各处的 JSON 行为而不被本组件覆盖。
     *
     * @param context                容器上下文，用于探测已有的序列化配置
     * @param redisConnectionFactory 连接工厂，可为 null
     * @return 值的序列化方式。
     */
    Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer(ApplicationContext context, RedisConnectionFactory redisConnectionFactory) {
        HashMap<Boolean, Jackson2JsonRedisSerializer<Object>> map = new HashMap<>();
        // 手动注入自定义 redisTemplate 和 redisProxy
        ConfigurableListableBeanFactory beanRegistry = ((AbstractApplicationContext) context).getBeanFactory();
        properties.forEach((pre, p) -> {
            RedisSerializer<Object> valueSerializer = map.computeIfAbsent(p.isActivateDefaultTyping(), jacksonMapper);
            boolean primary = RedisProxy.PRIMARY.equals(pre);
            // 统一由框架创建 factory, 不用 Spring 自动配置的 (避免停机时 Spring 先 stop factory 导致报错)
            RedisConnectionFactory factory = redisConnectionFactory(p);

            // 为 RedisProperties 初始化 pipeline 连接池 (幂等, 已有则跳过) primary 源和非 primary 源统一走此方法。
            Pool pool = p.getLettuce().getPool();
            int maxActive = pool != null && pool.getMaxActive() > 0 ? pool.getMaxActive() : 8;
            int maxIdle = pool != null && pool.getMaxIdle() >= 0 ? pool.getMaxIdle() : maxActive;
            int minIdle = pool != null && pool.getMinIdle() >= 0 ? pool.getMinIdle() : 0;
            long maxWait = pool != null && pool.getMaxWait() != null ? pool.getMaxWait().toMillis() : 0;
            // pipeline awaitAll 复用 Redis 命令超时（timeout: 10000）；未配置时 0 表示无限等待。
            long awaitTimeoutMs = p.getTimeout() != null ? p.getTimeout().toMillis() : 0;
            boolean cluster = !"single".equals(p.mode);
            PipelineConnectionPool pipelinePool = new PipelineConnectionPool(
                    ((LettuceConnectionFactory) factory).getNativeClient(), cluster, maxActive, maxIdle, minIdle, maxWait, awaitTimeoutMs);
            // 连接池最后关闭
            Graceful.registry(Integer.MAX_VALUE, pipelinePool::shutdown);
            p.setPipelinePool(pipelinePool);

            RedisTemplate<String, Object> redisTemplate = redisTemplate(factory, valueSerializer);
            String qualifier = primary ? "redisProxy" : pre + "RedisProxy";
            RedisProxy redisProxy = new RedisProxy(qualifier, redisTemplate);
            redisProxy.configureIdempotentCounter(idempotentCounterProperties);
            redisProxy.refreshProperties(p);
            DistributedLock lock = LettuceDistributedLock.initialize(redisProxy, p.getDistributedLock());
            if (primary) {
                // 主源三件套走 @Bean 返回值这条路注册, 此处只暂存, 见字段上的说明。
                primaryRedisTemplate = redisTemplate;
                primaryRedisProxy = redisProxy;
                primaryDistributedLock = lock;
                return;
            }
            // 非主源没有对应的 @Bean 方法, 只能在这里按 "<源名>RedisTemplate" 之类的名字手工注册,
            // 业务侧用 @Qualifier 按名取用。
            beanRegistry.registerSingleton(pre + "RedisTemplate", redisTemplate);
            beanRegistry.registerSingleton(qualifier, redisProxy);
            // 手工登记的单例不会自动推断销毁回调，必须显式纳入上下文生命周期才能释放进程级 qualifier。
            if (!(beanRegistry instanceof DefaultSingletonBeanRegistry lifecycleRegistry)) {
                redisProxy.destroy();
                throw new IllegalStateException("Spring BeanFactory does not support disposable singleton registration");
            }
            lifecycleRegistry.registerDisposableBean(qualifier, redisProxy);
            beanRegistry.registerSingleton(pre + "DistributedLock", lock);
        });
        if (properties.containsKey(RedisProxy.PRIMARY)) {
            return map.get(properties.get(RedisProxy.PRIMARY).isActivateDefaultTyping());
        }
        return map.computeIfAbsent(Boolean.TRUE, jacksonMapper);
    }

    /**
     * 业务作用：按配置的运行模式装配连接工厂：单机与集群建出的工厂类型不同。
     *
     * @param properties 连接配置
     * @return 连接工厂。
     */
    RedisConnectionFactory redisConnectionFactory(RedisProperties properties) {
        // 非池模式: 普通操作走 Lettuce 共享连接 (Netty 多路复用, 天然线程安全, 不走 commons-pool2)
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder().build();
        boolean cluster = !"single".equals(properties.mode);
        LettuceConnectionFactory factory;
        if (!cluster) {
            RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
            config.setHostName(properties.getHost());
            config.setPort(properties.getPort());
            config.setDatabase(properties.getDatabase());
            config.setUsername(properties.getUsername());
            if (StringUtils.isNotBlank(properties.getPassword())) {
                config.setPassword(RedisPassword.of(properties.getPassword()));
            }
            factory = new LettuceConnectionFactory(config, clientConfig);
        } else {
            RedisClusterConfiguration config = new RedisClusterConfiguration(properties.getCluster().getNodes());
            Integer maxRedirects = properties.getCluster().getMaxRedirects();
            if (Objects.nonNull(maxRedirects)) {
                config.setMaxRedirects(maxRedirects);
            }
            config.setUsername(properties.getUsername());
            if (StringUtils.isNotBlank(properties.getPassword())) {
                config.setPassword(RedisPassword.of(properties.getPassword()));
            }
            factory = new LettuceConnectionFactory(config, clientConfig);
        }
        this.start(factory);
        return factory;
    }

    /**
     * 业务作用：显式初始化连接工厂，使连接问题在启动期暴露而非等到首次使用。
     *
     * @param factory 待初始化的连接工厂
     * @return 已初始化的连接工厂。
     */
    LettuceConnectionFactory start(LettuceConnectionFactory factory) {
        factory.start();
        // 优雅停机关闭连接
        Graceful.registry(Integer.MAX_VALUE, factory::stop);
        while (!factory.isRunning()) ReflectUtils.sleep(5);
        return factory;
    }

    /**
     * 业务作用：装配操作模板，绑定键与值各自的序列化方式。
     * 键固定按字符串序列化：键在运维排查时需要可读，二进制键在客户端里无法辨认。
     *
     * @param redisConnectionFactory 连接工厂
     * @param valueSerializer        值的序列化方式
     * @return 操作模板。
     */
    RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory, RedisSerializer<Object> valueSerializer) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);

        template.setKeySerializer(RedisSerializer.string());
        template.setValueSerializer(valueSerializer);
        // 外层 Redis key 与 hash field 是两类协议字段，分别设置序列化器可避免后续配置改动使二者意外耦合。
        template.setHashKeySerializer(RedisSerializer.string());
        template.setHashValueSerializer(valueSerializer);

        template.afterPropertiesSet();
        template.setEnableTransactionSupport(false);
        return template;
    }

    /**
     * 业务作用：装配名为 {@code redisTemplate} 的主操作模板，它同时是本组件所有其它 Bean 的装配触发点。
     * <p>
     * 方法体第一步会走完多数据源装配：连接工厂、pipeline 连接池、各数据源的模板与代理都在那一步建出。
     * 配了 {@code nasa.redis.properties.primary} 时直接回传那一步为主源建好的实例，
     * 使主源实例经由本方法的返回值完成注册——主源不能在装配循环里手工注册，否则与本方法的注册撞名。
     *
     * @param context                容器上下文，装配非主源 Bean 时需要向其注册单例
     * @param redisConnectionFactory 容器内已有的连接工厂，可为 null；仅在未配置主源时用于兜底装配
     * @return 主操作模板。未配置主源时按容器内已有的连接工厂兜底装配一个。
     */
    @Primary
    @Bean
    public RedisTemplate<String, Object> redisTemplate(ApplicationContext context,
                                                       @Autowired(required = false) RedisConnectionFactory redisConnectionFactory) {
        RedisSerializer<Object> valueSerializer = jackson2JsonRedisSerializer(context, redisConnectionFactory);
        return primaryRedisTemplate != null ? primaryRedisTemplate : redisTemplate(redisConnectionFactory, valueSerializer);
    }

    /**
     * 业务作用：装配命令代理，它是本组件对外的主入口。
     * <p>
     * 入参强制容器先建出 {@code redisTemplate}，从而保证多数据源装配已经走完、主源实例可直接回传。
     * 未配置主源时退回按传入模板新建，此实例没有 pipeline 连接池，只能用直通命令。
     *
     * @param redisTemplate 主操作模板
     * @return 主命令代理。未配置主源时为按传入模板新建、不带 pipeline 连接池的实例。
     */
    @Primary
    @Bean
    public RedisProxy redisProxy(RedisTemplate<String, Object> redisTemplate) {
        if (primaryRedisProxy != null) return primaryRedisProxy;
        RedisProxy proxy = new RedisProxy("redisProxy", redisTemplate);
        proxy.configureIdempotentCounter(idempotentCounterProperties);
        return proxy;
    }

    /**
     * 业务作用：装配分布式锁入口。
     * 配了主源时回传主源装配阶段建好的实例，它带着 {@code nasa.redis.properties.primary.distributed-lock} 下的参数；
     * 未配主源时按默认参数兜底装配。
     *
     * @param redisProxy 命令代理，可为 null；为 null 表示未启用 Redis，锁实现自行降级而不使应用启动失败
     * @return 分布式锁入口。
     */
    @Primary
    @Bean
    public DistributedLock distributedLock(@Autowired(required = false) RedisProxy redisProxy) {
        return primaryDistributedLock != null ? primaryDistributedLock
                : LettuceDistributedLock.initialize(redisProxy, new DistributedLockProperties());
    }

    /**
     * 业务作用：承载本组件在 Spring Boot 原生 Redis 配置之上扩展的参数：
     * 运行模式、走原生接口还是批次的并发阈值、以及采集该并发度的时间窗口。
     * 继承原生配置类使连接地址、密码等标准项无需重复声明。
     */
    @Getter
    @Setter
    static class RedisProperties extends org.springframework.boot.autoconfigure.data.redis.RedisProperties {
        /* redis启动环境：single、cluster */
        private String mode = "single";
        /* 并发数低于这个值，走RedisTemplate原生API，否则走pipeline */
        private int lower = 50;
        /* 并发数采集时间窗口大小，ms */
        private int lowerMillis = 50;

        /* redis stream 相关配置 */
        private final Stream stream = new Stream();

        /* 让redis序列化后的json带上类信息 */
        private boolean activateDefaultTyping = true;

        /* 分布式锁配置 (qualifier 级, 覆盖全局) */
        private final DistributedLockProperties distributedLock = new DistributedLockProperties();

        /* pipeline 专用连接池实例 (运行时由 redisConnectionFactory 初始化, 非 yml 配置) */
        @Setter
        private transient PipelineConnectionPool pipelinePool;
    }

    /**
     * 业务作用：承载 Stream 消费的拉取参数：单次阻塞等待上限与单批最大条数。
     * 两者共同决定消费的延迟与吞吐取舍——等待越久空转越少但停机响应越慢，批越大吞吐越高但延迟毛刺越大。
     */
    @Getter
    @Setter
    public static class Stream {

        /* XREADGROUP 拉取消息超时时间ms，无消息时阻塞等待的上限 */
        private int pollTimeout = 500;
        /* XREADGROUP 每次拉取的最大消息数 */
        private int batchSize = 100;
        /* 异步删除stream record-id的周期时间 */
        private long asyncDelRecordPeriod = 5000;
        /* 是否由每个应用实例定时发起幂等裁剪；默认关闭，避免未配置保留期时误删消息 */
        private boolean autoTrimEnabled = false;
        /* 将stream自动裁剪交给第三服务，这是第三服务监听的stream */
        private String autoTrimToTopic;
        /* 自动裁剪执行频率，默认60s执行一次 */
        private long autoTrimRate = 60000;
        /* stream数据过期时间，默认1小时 */
        private long dataExpireMillis = 1000 * 60 * 60;
        /* 不需要自动裁剪的stream名 */
        private final Set<String> autoTrimExcludes = new HashSet<>();
        /* (stream, (group, Group)) 相关配置 */
        private final Map<String, Map<String, Group>> group = new LinkedHashMap<>();
        /* event 异步线程池在飞任务上限 (所有 listener/consumer 共享), 超限时由当前 consumer 线程同步执行实现背压 */
        private int eventExecutorInflightMax = 1000;
        /* 非group时是否启用线程池并发处理 */
        private boolean nonGroupExecutorEnable = true;
        /* 分区消费 (RedisPartition) 配置. 拉取参数 (pollTimeout/batchSize) 复用上面 Stream 全局配置, 这里只放分区独有的参数 */
        private final Partition partition = new Partition();

    }

    /**
     * 业务作用：承载单个消费组的参数：消费者名、起始位点、空闲接管阈值等。
     * 消费者名不填时按节点序号生成，使各节点在同一组内自动获得互不相同的消费者身份。
     */
    @Getter
    @Setter
    public static class Group {

        public static final Group DEFAULT = new Group();

        /* consumer注册的name，不填写默认是 ME.sequence() */
        private String consumerName;
        /* 同一组内consumer的节点数 */
        private int consumers = 1;
        /* 多事件时是否启用线程池并发处理不同事件 (与 consumers 数量无关) */
        private boolean eventExecutorEnable = false;
        /* 是否用 XREADGROUP NOACK；开启后消息读取时不进入 PEL，业务异常也无法依靠待处理消息重投 */
        private boolean autoAcknowledge = true;
        /**
         * 覆盖父级 Stream.batchSize, null = 用全局。
         * <p>
         * 配了非 null → RedisProxy.subscribe 会为该 (stream, group) 单独建一个独立 container
         * (走 RedisProxy.dedicatedContainers 缓存), 与全局共享 container 隔离, 该 group 的
         * XREADGROUP COUNT 真正按这里取值。
         * 没配 → 走全局共享 container, 拉取参数与其他订阅一致。
         */
        private Integer batchSize;
        /**
         * 覆盖父级 Stream.pollTimeout, null = 用全局。
         * <p>
         * 配了非 null → 该 (stream, group) 单独建独立 container, XREADGROUP BLOCK 按这里取值。
         * 没配 → 走全局共享 container。
         */
        private Integer pollTimeout;

    }

    /**
     * 分区消费 (RedisPartition) 配置。
     * <p>
     * 这里只放分区独有的运行参数, 拉取相关 (pollTimeout / batchSize) 仍走 {@link Stream} 全局配置。
     * <p>
     * <b>命名空间约定</b>: {@link #defaultGroup} 是所有分区组的命名空间前缀, 也是默认共享组本身的 group 名。
     * <ul>
     *   <li>默认共享组: stream = {@code {defaultGroup}:0..count-1}, consumer group 名 = {@code {defaultGroup}}</li>
     *   <li>隔离组 (yml 中以 {@code groups.<逻辑名>} 配置): stream = {@code {defaultGroup}:<逻辑名>:0..count-1},
     *       consumer group 名 = {@code {defaultGroup}:<逻辑名>}</li>
     * </ul>
     * 业务在 yml 和代码里只用 "逻辑名" (短名), 实际 Redis key 由框架在 {@link #defaultGroup} 命名空间下自动拼接。
     * <p>
     * yml 配置示例:
     * <pre>
     * nasa:
     *   redis:
     *     properties:
     *       primary:
     *         stream:
     *           pollTimeout: 500          # 与普通 stream 消费共享
     *           batchSize: 100            # 与普通 stream 消费共享
     *           partition:
     *             enabled: true           # 分区消费总开关
     *             default-group: SINGLE-CONSUME  # 命名空间前缀, 也是默认共享组的 group 名
     *             count: 64               # 默认共享组的分区数
     *             rebalance-ms: 3000
     *             min-idle-ms: 30000
     *             holds-check-interval-ms: 5000
     *             drain-timeout-ms: 5000
     *             groups:                 # 隔离组配置, key 用业务逻辑短名 (不要带 default-group 前缀)
     *               contract:settlement:  # → 实际 stream = SINGLE-CONSUME:contract:settlement:0..63
     *                 count: 64
     *                 min-idle-ms: 60000
     *               spot:settlement:      # → 实际 stream = SINGLE-CONSUME:spot:settlement:0..31
     *                 count: 32
     *                 batch-size: 200
     * </pre>
     */
    @Getter
    @Setter
    public static class Partition {

        /* 分区消费总开关. false → RedisPartition.init 静默返回, 不启 rebalance / consumer task, 节省启动开销 */
        private boolean enabled = false;
        /**
         * 命名空间前缀, 同时也是默认共享组的 stream/consumer group 名。
         * 默认 SINGLE-CONSUME (全大写做命名空间标识, 与业务 key 视觉上分离)。
         * <p>
         * 实际 Redis key 命名规则:
         * <ul>
         *   <li>默认共享组 stream: {@code {defaultGroup}:0..count-1}, consumer group 名 = {defaultGroup}</li>
         *   <li>隔离组 stream: {@code {defaultGroup}:{逻辑名}:0..count-1}, consumer group 名 = {defaultGroup}:{逻辑名}</li>
         *   <li>分区锁 key: {@code DISTRIBUTED-LOCK:{stream前缀}:lock:{partition}}</li>
         * </ul>
         */
        private String defaultGroup = "SINGLE-CONSUME";
        /* 默认共享组的分区数. 业务调 RedisPartition.init() 不传 count 时用这个 */
        private int count = 64;
        /* 再平衡周期 ms (按 fair = count / aliveNodes 重新均摊持有的分区数) */
        private long rebalanceMs = 3_000;
        /* XAUTOCLAIM 接管 pending 的最小 idle 时间 ms, 与 LettuceDistributedLock 的 leaseTime 对齐, 默认 30s */
        private long minIdleMs = 30_000;
        /* holds() 自检最小间隔 ms (防长 GC / 业务长跑后锁丢失). 默认 5s, 远小于 lease=30s */
        private long holdsCheckIntervalMs = 5_000;
        /* drain 超时 ms（主动 stop 后等待 in-flight listener / recoverPending 的最大时长）。默认 5s，超时强制 exit，迟到的 XACK 由 fencing 拒绝。 */
        private long drainTimeoutMs = 5_000;
        /**
         * 隔离组配置表。key = 业务逻辑短名 (不要带 defaultGroup 前缀, 框架会自动拼)。
         * 例如 key="contract:settlement" → 实际 stream 前缀 = "{defaultGroup}:contract:settlement"。
         * <p>
         * 默认共享组无需在这里配置, 它的参数由 Partition 顶层字段直接决定。
         */
        private final Map<String, PartitionGroup> groups = new LinkedHashMap<>();
    }

    /**
     * 单个分区组的覆盖配置。允许隔离组用与默认组不同的参数,
     * 例如高频低耗时的 settlement 用大 batchSize, 慢任务的某 topic 用更长 minIdleMs。
     */
    @Getter
    @Setter
    public static class PartitionGroup {

        /* 分区数, 必填 (决定 key.hashCode() % count 的取模基数) */
        private int count;
        /* 覆盖父级 rebalanceMs, null = 用父级 Partition.rebalanceMs */
        private Long rebalanceMs;
        /* 覆盖父级 minIdleMs, null = 用父级 */
        private Long minIdleMs;
        /* 覆盖父级 holdsCheckIntervalMs, null = 用父级 */
        private Long holdsCheckIntervalMs;
        /* 覆盖父级 drainTimeoutMs, null = 用父级 */
        private Long drainTimeoutMs;
        /* 覆盖父级 Stream.batchSize, null = 用 Stream 全局 */
        private Integer batchSize;
        /* 覆盖父级 Stream.pollTimeout, null = 用 Stream 全局 */
        private Integer pollTimeout;
        /**
         * 该隔离组接收哪些业务 topic 的消息。
         * <p>
         * 不配 (空列表) → 默认 = [logical name]: yml key 同时作为 topic 名 (1 隔离组 1 topic 简化场景)。
         * 配了非空 → 框架对每个 topic 调 {@code RedisPartition.isolate(topic, logicalName, count)},
         * 让多个 topic 共享同一个隔离组的 stream/lock 命名空间。
         * <p>
         * 例如 yml:
         * <pre>
         * groups:
         *   high-freq-settle:                        # logical name
         *     count: 128
         *     topics: [contract:settlement, spot:settlement]   # 两个 topic 共享 high-freq-settle 组
         * </pre>
         */
        private final List<String> topics = new ArrayList<>();
    }

    /**
     * 业务作用：承载分布式锁的参数：键前缀与租期。
     * 前缀用于把锁键与业务键隔开，避免误删；租期决定看门狗的续期节奏，
     * 设得过短会让业务尚未做完锁就过期，设得过长则持锁节点宕机后其他节点要等更久才能接管。
     */
    @Getter
    @Setter
    public static class DistributedLockProperties {
        /* 锁 key 前缀, 避免与其他 Redis key 类型冲突 */
        private String prefix;
        /* 锁过期时间 ms, 看门狗每 leaseTime/3 自动续期 */
        private Long leaseTime;
    }

}
