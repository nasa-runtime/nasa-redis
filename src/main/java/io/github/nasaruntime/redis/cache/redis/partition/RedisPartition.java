package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.*;
import io.github.nasaruntime.redis.cache.redis.*;
import io.github.nasaruntime.redis.cache.redis.stream.BatchStreamListener;
import io.github.nasaruntime.redis.cache.redis.stream.BatchStreamMessageListenerContainer;
import io.github.nasaruntime.redis.cache.redis.stream.PollLifecycle;
import io.github.nasaruntime.core.config.Graceful;
import io.github.nasaruntime.core.evt.PooledEvtData;
import io.github.nasaruntime.core.function.ActionRecycler;
import io.github.nasaruntime.core.utils.ContextUtils;
import io.github.nasaruntime.core.utils.StringUtils;
import org.springframework.data.redis.connection.stream.ReadOffset;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.models.stream.ClaimedMessages;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 业务作用：按业务分区键把 Redis Stream 消息稳定路由到独占消费者，在节点变化后重新分配分区并接管未确认消息。
 * <p>
 * N 个分区 Stream 共享一组消费者组，每个分区通过 {@link LettuceDistributedLock} 独占；取得锁的节点执行
 * XAUTOCLAIM 与 XREADGROUP。在分区数、命名空间和键类型不变时，同一个发布键落入同一分区；
 * 当前持锁节点按单条消息的计划与有效 hash 建立本地顺序，不同分区之间不保证顺序。
 *
 * <h2>架构特性</h2>
 * <ul>
 *   <li><b>消费容器</b>：每个分区组使用独立的 {@link BatchStreamMessageListenerContainer}，
 *       复用对应 {@link RedisProxy} 的连接工厂、序列化方式与业务执行基础设施，并允许单组覆盖
 *       pollTimeout 和 batchSize。</li>
 *   <li><b>分区认领</b>：启动与周期再平衡都以非阻塞 tryLock 认领分区，通知通道只负责缩短收敛延迟。</li>
 *   <li><b>崩溃恢复</b>：原 owner 的锁租期失效后，新 owner 用 XAUTOCLAIM 接管达到空闲阈值的 PEL。</li>
 *   <li><b>本地执行</b>：Redis 保持批量读取，Single listener 按稳定 {@code partitionKey} 进入 Partition Task。</li>
 *   <li><b>执行交接</b>：正常读取、历史接管与精确重试共用逐 record 执行权，句柄覆盖 Task 与重试或确认责任的交接。</li>
 *   <li><b>历史复验</b>：取得执行权后重新查询 PEL；缺席正文不提交，已有执行或确认责任交给原驱动力。</li>
 *   <li><b>确认责任</b>：Task Future 真实结束后登记确认；ACK 结果不确定时只复验 PEL 或重试确认，不重新调用成功 listener。</li>
 *   <li><b>失权门禁</b>：恢复正文和 PEL 读取后复验实际 holder，最终确认由 holder-fenced Lua 原子完成。</li>
 *   <li><b>容量与排干</b>：证据不明确时保留完整恢复页与容量；停止先封闭准入，再等待在途 Task、确认及恢复责任结束。</li>
 *   <li><b>topic 路由</b>：默认所有 topic 共享分区组，高吞吐或慢 topic 可以隔离到独立组。</li>
 * </ul>
 * <p>
 * 执行句柄与成功证据只在当前来源代次内有效。进程退出或来源移交仍可能重放业务，
 * 回调必须以稳定事件键实现幂等；fencing 不能撤销已经进入外部系统的副作用。
 *
 * <h3>使用方式 (yml 驱动 + listener 声明式, 业务侧零样板)</h3>
 * <pre>{@code
 * // 1. yml 配置。分区组的创建由 RedisProxy.initialize 按容器内 PARTITION 模式 listener 的 topic 自动完成,
 * //    业务侧不需要显式调 init / isolate。
 * //    nasa.redis.properties.primary.stream.partition:
 * //      enabled: true
 * //      count: 64
 * //      groups:
 * //        settlement: { count: 64, topics: [contract:settlement], batch-size: 200 }
 *
 * // 2. 业务侧实现 RedisEventSingleListener, mode = PARTITION，并从消息体声明本地 Partition key
 * @Component
 * public class OpenPositionListener implements RedisEventSingleListener<Order> {
 *     // 业务作用：声明结算来源。参数说明: 无。返回: 结算 topic。
 *     public String[] topics() { return new String[]{"contract:settlement"}; }
 *     // 业务作用：选择开仓事件。参数说明: 无。返回: 当前计划的 event。
 *     public String event() { return "open-position"; }
 *     // 业务作用：选择持权分区入口。参数说明: 无。返回: PARTITION 模式。
 *     public ConsumeMode mode() { return ConsumeMode.PARTITION; }
 *     // 业务作用：声明订单解码类型。参数说明: 无。返回: 精确订单类型。
 *     public TypeReference<Order> paramType() { return new TypeReference<>() {}; }
 *     // 业务作用：按账户建立执行顺序。参数说明: order 为当前订单。返回: 稳定账户键。
 *     public Object partitionKey(Order order) { return order.uid(); }
 *     // 业务作用：处理一条订单事件。参数说明: order 为当前订单。返回: 正常返回后可确认，异常保留重试。
 *     public void onEvent(Order order) { ... }
 * }
 *
 * // 3. 业务发布 — 分区数与命名空间不变时，同 uid 路由到同一分区的单一持权处理路径
 * RedisPartition.load(redisProxy).publish("contract:settlement", "open-position", order.uid(), order);
 * }</pre>
 *
 * <h3>命名空间</h3>
 * <ul>
 *   <li>默认共享组 stream: {@code SINGLE-CONSUME:0..63} (前缀来自 yml {@code partition.defaultGroup})</li>
 *   <li>隔离组 stream: {@code SINGLE-CONSUME:settlement:0..63} (拼 defaultGroup + 逻辑名)</li>
 *   <li>consumer group 名 = stream 前缀 (默认组 = SINGLE-CONSUME, 隔离组 = SINGLE-CONSUME:settlement)</li>
 *   <li>分区锁业务 key = {@code {stream前缀}:lock:{partition}}，最终 Redis key 还会加分布式锁配置前缀</li>
 * </ul>
 *
 * <h3>配置 (yml)</h3>
 * <pre>
 * nasa:
 *   redis:
 *     properties:
 *       primary:
 *         stream:
 *           pollTimeout: 500
 *           batchSize: 100
 *           partition:
 *             enabled: true
 *             default-group: SINGLE-CONSUME    # 命名空间前缀, 也是默认组的 group 名
 *             count: 64                         # 默认组分区数
 *             rebalance-ms: 3000
 *             min-idle-ms: 30000
 *             holds-check-interval-ms: 5000     # holds 自检最小间隔, 时间限流防止 NOBLOCK 空轮询打爆 EVAL
 *             drain-timeout-ms: 5000
 *             groups:                           # 隔离组配置 (key 用业务逻辑短名)
 *               settlement:                     # → stream = SINGLE-CONSUME:settlement:0..63
 *                 count: 64
 *                 topics: [contract:settlement, spot:settlement]
 *                 batch-size: 200
 * </pre>
 *
 * @see LettuceDistributedLock
 * @see RedisProxy
 * @see PollLifecycle
 */
@SuppressWarnings("all")
@Slf4j
public class RedisPartition {

    /**
     * 默认共享组的逻辑名: 用空串表示。
     * <p>
     * 实际 stream/group 名由 {@code RedisPartitionProperties.defaultGroup} 决定 (yml 可配, 默认 "SINGLE-CONSUME"),
     * 这里只是 {@link RedisPartition#groups} 这个内部 Map 的 key 约定 — 默认组用 "" 占位,
     * 隔离组用业务逻辑短名 ("contract:settlement" 等)。
     */
    public static final String DEFAULT_GROUP_NAME = "";

    /**
     * 消息体写入 stream 的 hash field
     */
    public static final String DATA_FIELD = "data";

    /**
     * 首个节点原子写入分区协议，后续节点只能读取并复验。
     */
    private static final String PARTITION_CONTRACT_LUA = """
            local current = redis.call('get', KEYS[1])
            if not current then
                redis.call('set', KEYS[1], ARGV[1])
                return ARGV[1]
            end
            return current
            """;

    /**
     * 只读取得已有组合同，使已经完成全命名空间证明的部署无需在每次启动时重复扫描。
     */
    private static final String PARTITION_CONTRACT_READ_LUA = "return redis.call('get', KEYS[1])";

    /**
     * 一次原子复验并登记同一 listener 或发布入口声明的全部 topic 路由，冲突时不写入任何 field。
     */
    private static final String TOPIC_ROUTE_CONTRACT_LUA = """
            for i = 1, #ARGV, 2 do
                local current = redis.call('hget', KEYS[1], ARGV[i])
                if current and current ~= ARGV[i + 1] then
                    return {0, i, current}
                end
            end
            for i = 1, #ARGV, 2 do
                if redis.call('hexists', KEYS[1], ARGV[i]) == 0 then
                    redis.call('hset', KEYS[1], ARGV[i], ARGV[i + 1])
                end
            end
            return {1}
            """;

    /**
     * 只读复验同一 listener 声明的全部 topic 路由，使已知冲突在代理独占 Runner 启动前返回。
     */
    private static final String TOPIC_ROUTE_CONTRACT_PREFLIGHT_LUA = """
            for i = 1, #ARGV, 2 do
                local current = redis.call('hget', KEYS[1], ARGV[i])
                if current and current ~= ARGV[i + 1] then
                    return {0, i, current}
                end
            end
            return {1}
            """;

    /**
     * 使用 Redis 服务端时钟续约并清理节点，消除不同主机墙上时钟偏差对公平份额的影响。
     */
    private static final String PARTITION_HEARTBEAT_LUA = """
            local now = redis.call('time')
            local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
            redis.call('zadd', KEYS[1], now_ms + tonumber(ARGV[1]), ARGV[2])
            redis.call('zremrangebyscore', KEYS[1], '-inf', now_ms)
            return redis.call('zcard', KEYS[1])
            """;

    /**
     * 优雅下线必须在发布 offline 之前同步移除成员，接管节点才能立即计算出新的公平份额。
     */
    private static final String PARTITION_UNREGISTER_LUA = "return redis.call('zrem', KEYS[1], ARGV[1])";

    /**
     * 业务作用：在一个 Redis 原子执行单元内复验 Claim holder、确认 PEL，并只删除本次实际确认的正文。
     */
    private static final String HOLDER_FENCED_ACK_LUA = """
            if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
                return {-1}
            end
            local result = {1}
            for i = 4, #ARGV do
                local id = ARGV[i]
                local acked = redis.call('xack', KEYS[2], ARGV[2], id)
                if acked == 1 then
                    if ARGV[3] == '1' then redis.call('xdel', KEYS[2], id) end
                    result[#result + 1] = 1
                else
                    result[#result + 1] = 0
                end
            end
            return result
            """;

    /**
     * 业务作用：以两个独立字段表达消息路由身份，避免 topic 或 event 自身包含分隔符时映射到同一监听器。
     *
     * @param topic 消息主题
     * @param event 事件名称
     */
    private record TopicEventKey(String topic, String event) {
    }

    /**
     * 业务作用：冻结一个 topic 在共享 HASH 中的 field 与完整物理路由正文，供批量原子复验。
     */
    private record TopicRouteContract(String topic, String field, String expected) {
    }

    /**
     * recoverPending 异步提交的 Consumer 策略, 无状态全局共享, 配合 {@link ActionRecycler} 零 GC 派发.
     * ref(0)=Claim，ref(1)=提交前登记的恢复责任。实际执行委托给 {@link Claim#doRecoverPendingAsync(StreamRuntimeStatus.DrainToken)}。
     */
    private static final Consumer<ActionRecycler> RECOVER_PENDING_CON = ar -> {
        Claim claim = ar.ref(0);
        claim.doRecoverPendingAsync(ar.ref(1));
    };

    private static final Map<RedisProxy, RedisPartition> CACHE = new ConcurrentHashMap<>();
    private static final Graceful.Shutdown SHUTDOWN_ALL = RedisPartition::shutdownAll;

    static {
        // 只登记一个进程级入口，避免每次重建 Spring 上下文都把实例方法引用永久留在停机链中。
        Graceful.registry(Integer.MAX_VALUE - 10000, SHUTDOWN_ALL);
    }

    /**
     * 业务作用：获取/创建一个 RedisProxy 对应的 RedisPartition (按 RedisProxy 单例)
     *
     * @param redisProxy 命令代理，决定连接与序列化方式
     * @return 见上述说明。
     */
    public static RedisPartition load(RedisProxy redisProxy) {
        if (redisProxy == null) return null;
        RedisPartition partition = CACHE.compute(redisProxy, (proxy, current) -> {
            // 分区实例的创建与代理销毁使用同一个缓存键串行化；代理终态之后不能再生成新的运行时。
            if (!proxy.partitionAdmissionOpen()) {
                throw new IllegalStateException("RedisProxy is already destroyed: " + proxy.getQualifier());
            }
            return current == null ? new RedisPartition(proxy) : current;
        });
        redisProxy.streamPartitionMetrics().partitionAvailable(partition);
        return partition;
    }

    /**
     * 业务作用：供代理生命周期和内部指标桥接只读查询已存在的分区入口，不因观测动作创建消费状态机。
     *
     * @param redisProxy 目标命令代理
     * @return 已存在的分区入口；尚未使用分区能力时为 null
     */
    public static RedisPartition current(RedisProxy redisProxy) {
        return CACHE.get(redisProxy);
    }

    /**
     * 业务作用：取默认实例的分区消费入口。
     *
     * <p>参数说明: 无。
     *
     * @return 分区消费入口。
     */
    public static RedisPartition load() {
        return load(RedisProxy.load());
    }

    /**
     * 业务作用：按实例名取分区消费入口。
     *
     * @param qualifier 实例名
     * @return 该实例的分区消费入口；未登记时为 null。
     */
    public static RedisPartition load(String qualifier) {
        RedisProxy redisProxy = RedisProxy.load(qualifier);
        return redisProxy == null ? null : load(redisProxy);
    }

    /**
     * 业务作用：原子摘除命令代理关联的分区消费入口，把后续停机责任交给仍可接收真实生命周期回调的 RedisProxy。
     * 先摘除静态登记可允许相同 qualifier 在代理资源关闭后安全重建，同时返回旧实例以保留未完成执行域的收敛入口。
     *
     * @param redisProxy 即将销毁的命令代理
     * @return 已摘除且 dispatcher 准入已关闭的分区入口；没有关联实例时返回 null，已有责任仍待后续停机排干。
     */
    public static RedisPartition detachForShutdown(RedisProxy redisProxy) {
        AtomicReference<RedisPartition> removed = new AtomicReference<>();
        // 与 load 的 compute 共用缓存键原子交接，销毁标志发布后不存在“移除为空、随后新建”的窗口。
        CACHE.compute(redisProxy, (proxy, current) -> {
            removed.set(current);
            return null;
        });
        // 摘除只转移生命周期归属；交回调用方前先封闭消费准入，延后排干期间不能继续提交新任务。
        if (removed.get() != null) removed.get().partitionRuntime.closeAdmission();
        return removed.get();
    }

    /**
     * 业务作用：在 JVM 优雅停机阶段关闭当时仍存活的全部分区消费入口。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回: 无返回值；每个实例独立保证关闭幂等。
     */
    private static void shutdownAll() {
        for (RedisPartition partition : List.copyOf(CACHE.values())) partition.shutdown();
    }

    private final RedisProxy redisProxy;
    private final LettuceDistributedLock distributedLock;
    private final StreamSubscriptionRegistry subscriptionRegistry;
    private final StreamPartitionRuntime partitionRuntime;
    private final boolean colocatedKeyLayout;

    /**
     * 节点级唯一 consumer name, 写入 XREADGROUP / XAUTOCLAIM 的 consumer 字段。
     * 每次进程启动新生成 UUID — pending list 上旧 consumer 名下的消息会 idle 30s+ 后被
     * 新节点 XAUTOCLAIM 接管, 不会出现 "进程重启后误以为自己仍持有上次未 ACK 的消息"。
     */
    private final String nodeId;

    /**
     * 已初始化的分区组: groupName → group。
     * <p>
     * key 是逻辑短名 (默认共享组用 {@link #DEFAULT_GROUP_NAME} = "", 隔离组用业务逻辑名);
     * 实际 Redis stream/lock/consumer-group 命名由 PartitionGroup.streamPrefix 决定。
     */
    private final Map<String, PartitionGroup> groups = new ConcurrentHashMap<>();

    /**
     * topic → 所属分区组逻辑名. 未配置的 topic 默认走 {@link #DEFAULT_GROUP_NAME}
     */
    private final Map<String, String> topicToGroupName = new ConcurrentHashMap<>();

    /**
     * 已完成远端复验的组合同，value 是包含物理布局的不可变合同正文。
     */
    private final Map<String, String> partitionContractCache = new ConcurrentHashMap<>();

    /**
     * 发布路径的路由缓存: topic → [streamPrefix, count, roundRobin, colocated]。
     * 只有组合同与 topic 路由合同均完成远端复验的结果才会进入缓存。
     */
    private final Map<String, Object[]> publishRouteCache = new ConcurrentHashMap<>();
    /**
     * 缓存 resolvePublishRoute 方法引用, 避免 computeIfAbsent 热路径分配
     */
    private final Function<String, Object[]> resolvePublishRouteRef = this::resolvePublishRoute;

    private volatile boolean running = true;
    /**
     * 来源已封闭并完成退出请求；由 shutdownLock 保护，迟到确认和恢复责任仍须由运行时单独排干。
     */
    private boolean sourcesShutdownComplete;
    /**
     * 本代理独占的 Runner 与 TimingWheel 已完成依赖顺序停机；由 shutdownLock 保护。
     */
    private boolean ownedRunnerShutdownComplete;
    /**
     * 发布准入独立于消费运行时，关闭时同时覆盖同步 XADD 与调用方持有的 pipeline 命令。
     */
    private final PublisherCoordinator publisherCoordinator = new PublisherCoordinator();
    /* startup-only lock for initGroup/startAllGroups, set to null after startup for GC */
    private final ReentrantLock initLock = new ReentrantLock();
    /* shutdown lock */
    private final ReentrantLock shutdownLock = new ReentrantLock();

    /**
     * 业务作用：绑定分区消费入口与其命令代理。
     *
     * @param redisProxy 承载分区命令的命令代理
     *                   返回: 构造出的代理级分区入口；分布式锁尚未登记时拒绝构造。
     */
    private RedisPartition(RedisProxy redisProxy) {
        this.redisProxy = Objects.requireNonNull(redisProxy, "redisProxy must not be null");
        // 复用 RedisProxy 对应的分布式锁实例. 必须在 NasaLettuceConfig 已经为该 RedisProxy
        // initialize 过 LettuceDistributedLock 之后才能用 (一般是 Spring 启动后期)
        this.distributedLock = LettuceDistributedLock.load(redisProxy);
        if (this.distributedLock == null) {
            throw new IllegalStateException(
                    "LettuceDistributedLock not initialized for RedisProxy[" + redisProxy.getQualifier()
                            + "], make sure NasaLettuceConfig has registered it");
        }
        this.subscriptionRegistry = new StreamSubscriptionRegistry(redisProxy);
        this.partitionRuntime = new StreamPartitionRuntime(redisProxy, subscriptionRegistry);
        RedisPartitionProperties partitionConfig = redisProxy.getStream().getPartition();
        boolean clusterConnection = redisProxy.getRedisTemplate().getConnectionFactory()
                instanceof org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory factory
                && factory.getClusterConfiguration() != null;
        this.colocatedKeyLayout = partitionConfig.getKeyLayout() == RedisPartitionProperties.PartitionKeyLayout.COLOCATED
                || (partitionConfig.getKeyLayout() == RedisPartitionProperties.PartitionKeyLayout.AUTO && clusterConnection);
        // 每个运行时实例都使用新的会话标识。ME.sequence() 可能被同一主机上的多个进程共享，
        // 单独使用会让 ZSET 把多个消费者误判成一个节点，进而计算出错误的 fair 值。
        this.nodeId = ContextUtils.getPropertySafe("spring.application.name", "RedisPartition")
                + "/" + ME.sequence() + "/" + UUID.randomUUID();
    }

    /**
     * 业务作用：限定分区协议使用 Spring UTF-8 字符串键编码，使不同节点共享同一物理合同，并让 SCAN glob 与逻辑前缀等价。
     *
     * @param redisProxy 待启用分区能力的命令代理
     *                   返回: 无返回值；键序列化器类型或编码不符合协议时抛出 IllegalStateException。
     */
    private static void validatePartitionKeySerializer(RedisProxy redisProxy) {
        RedisSerializer<String> serializer = redisProxy.getKeySerializer();
        String probe = "nasa-redis:{partition-key}:*?[]\\中文";
        byte[] expected = probe.getBytes(StandardCharsets.UTF_8);
        byte[] actual;
        try {
            actual = serializer == null ? null : serializer.serialize(probe);
        } catch (RuntimeException failure) {
            throw new IllegalStateException(
                    "RedisPartition requires the UTF-8 StringRedisSerializer for Redis keys", failure);
        }
        if (serializer == null
                || serializer.getClass() != StringRedisSerializer.class
                || !Arrays.equals(expected, actual)) {
            String configured = serializer == null ? "<null>" : serializer.getClass().getName();
            String encoding = serializer != null && serializer.getClass() == StringRedisSerializer.class
                    ? ", configured StringRedisSerializer encoding is not UTF-8"
                    : "";
            throw new IllegalStateException(
                    "RedisPartition requires the UTF-8 StringRedisSerializer for Redis keys, configured="
                            + configured + encoding);
        }
    }

    /**
     * 业务作用：线性化分区发布准入与关闭，取消尚未派发的 pipeline 命令，并等待所有在途 Redis 写入终结。
     */
    private static final class PublisherCoordinator {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition drained = lock.newCondition();
        private final Set<PublisherPermit> permits = Collections.newSetFromMap(new IdentityHashMap<>());
        private boolean open = true;

        /**
         * 业务作用：登记一条立即执行的同步发布，使关闭方等待到调用方明确完成 Redis XADD。
         *
         * <p>参数说明: 无。
         *
         * @return 已处于在途状态的发布许可；准入关闭后抛出 IllegalStateException。
         */
        PublisherPermit admitDirect() {
            return admit(false);
        }

        /**
         * 业务作用：登记一条尚未 flush 的 pipeline 发布，关闭方可在实际派发前将其取消。
         *
         * <p>参数说明: 无。
         *
         * @return 处于排队状态的发布许可；准入关闭后抛出 IllegalStateException。
         */
        PublisherPermit admitQueued() {
            return admit(true);
        }

        /**
         * 业务作用：在同一临界区复验发布准入并登记许可，避免关闭检查与许可发布之间出现写入窗口。
         *
         * @param queued true 表示命令尚可在派发前取消，false 表示调用方已承担同步写入责任
         * @return 新登记的发布许可。
         */
        private PublisherPermit admit(boolean queued) {
            lock.lock();
            try {
                if (!open) throw new IllegalStateException("RedisPartition is already shut down");
                PublisherPermit permit = new PublisherPermit(this, queued);
                permits.add(permit);
                return permit;
            } finally {
                lock.unlock();
            }
        }

        /**
         * 业务作用：封闭发布准入、取消仍在排队的命令，并等待已经开始 Redis 写入的许可全部归零。
         * 中断只延后到排干完成后恢复，确保 shutdown 返回时不遗留迟到 XADD。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；返回时所有登记过的发布均已取消或到达 Redis 终态。
         */
        void closeAndDrain() {
            boolean interrupted = false;
            lock.lock();
            try {
                open = false;
                Iterator<PublisherPermit> iterator = permits.iterator();
                while (iterator.hasNext()) {
                    PublisherPermit permit = iterator.next();
                    if (permit.state != PublisherPermit.QUEUED) continue;
                    permit.state = PublisherPermit.COMPLETED;
                    iterator.remove();
                }
                while (!permits.isEmpty()) {
                    try {
                        drained.await();
                    } catch (InterruptedException signal) {
                        interrupted = true;
                    }
                }
            } finally {
                lock.unlock();
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 业务作用：表示单条分区发布从排队、派发到 Redis 终态的所有权，并为 pipeline 提供关闭复验。
     */
    private static final class PublisherPermit implements LettucePipeline.BufferedCommandLifecycle {
        private static final int QUEUED = 0;
        private static final int ACTIVE = 1;
        private static final int COMPLETED = 2;

        private final PublisherCoordinator coordinator;
        private int state;
        private boolean futureBound;

        /**
         * 业务作用：创建由指定协调器持有的发布许可，并设置初始排队或在途状态。
         *
         * @param coordinator 发布准入协调器
         * @param queued      true 表示等待 pipeline 派发，false 表示同步调用已开始
         *                    返回: 构造出的发布许可。
         */
        private PublisherPermit(PublisherCoordinator coordinator, boolean queued) {
            this.coordinator = coordinator;
            this.state = queued ? QUEUED : ACTIVE;
        }

        /**
         * 业务作用：在发布准入锁内把 pipeline 命令从排队转为在途；关闭已先发生时拒绝派发。
         *
         * <p>参数说明: 无。
         *
         * @return true 表示调用方取得派发责任；false 表示命令已取消或已经结束。
         */
        @Override
        public boolean beginDispatch() {
            coordinator.lock.lock();
            try {
                if (state != QUEUED || !coordinator.open) return false;
                state = ACTIVE;
                return true;
            } finally {
                coordinator.lock.unlock();
            }
        }

        /**
         * 业务作用：把在途发布绑定到 Redis future，只有 future 成功或失败后才允许关闭边界通过。
         *
         * @param future 当前 XADD 的 Redis future；null 表示没有形成可执行命令
         *               返回: 无返回值；null future 立即撤销，其余 future 在终态回收许可。
         */
        @Override
        public void bind(RedisFuture<?> future) {
            if (future == null) {
                abort();
                return;
            }
            coordinator.lock.lock();
            try {
                if (state != ACTIVE) return;
                futureBound = true;
                future.whenComplete((result, failure) -> complete());
            } finally {
                coordinator.lock.unlock();
            }
        }

        /**
         * 业务作用：撤销尚未取得派发权或尚未形成 Redis future 的发布；已绑定 future 时由其终态负责收口。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；可安全取消时从关闭等待集合移除。
         */
        @Override
        public void abort() {
            coordinator.lock.lock();
            try {
                if (state == QUEUED || (state == ACTIVE && !futureBound)) completeLocked();
            } finally {
                coordinator.lock.unlock();
            }
        }

        /**
         * 业务作用：在同步发布返回或 Redis future 终态后释放本条发布责任并唤醒关闭等待者。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；重复调用保持幂等。
         */
        private void complete() {
            coordinator.lock.lock();
            try {
                completeLocked();
            } finally {
                coordinator.lock.unlock();
            }
        }

        /**
         * 业务作用：在已持有协调器锁时完成许可移除，保证状态迁移与排干通知不可分割。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；许可首次完成时从登记集合移除。
         */
        private void completeLocked() {
            if (state == COMPLETED) return;
            state = COMPLETED;
            if (coordinator.permits.remove(this) && coordinator.permits.isEmpty()) {
                coordinator.drained.signalAll();
            }
        }
    }

    // ==================== 公开 API ====================

    /**
     * 业务作用：初始化默认共享分区组, 分区数从 yml {@code partition.count} 读取 (默认 64)。
     * <p>
     * 实际 stream 名 = {@code {partition.defaultGroup}:0..count-1} (默认 SINGLE-CONSUME:0..63)。
     * 如果配置 {@code stream.partition.enabled=false}, 静默返回不启动。
     */
    public RedisPartition init() {
        return this.init(redisProxy.getStream().getPartition().getCount());
    }

    /**
     * 业务作用：初始化默认共享分区组, 显式指定分区数 (覆盖 yml partition.count)。
     *
     * @param count 分区数，确定后不可更改——改变它会让同一分区键落到不同分区
     * @return 见上述说明。
     */
    public RedisPartition init(int count) {
        return this.initGroup(DEFAULT_GROUP_NAME, count);
    }

    /**
     * 业务作用：把一个 topic 隔离到独立分区组, 隔离组的逻辑名默认与 topic 同名。
     * <p>
     * 例如 {@code isolate("contract:settlement", 64)} → 实际 stream 名为
     * {@code {defaultGroup}:contract:settlement:0..63} (默认 SINGLE-CONSUME:contract:settlement:0..63)。
     *
     * @param topic 主题名
     * @param count 该主题独立使用的分区数
     * @return 见上述说明。
     */
    public RedisPartition isolate(String topic, int count) {
        return this.isolate(topic, topic, count);
    }

    /**
     * 业务作用：把一个 topic 隔离到指定逻辑名的分区组。隔离组的慢消费不会阻塞默认组的其他 topic。
     * <p>
     * 多个 topic 可以共享同一个隔离组 (传同一个 groupName), 它们会一起摊到该组的 stream 上消费。
     *
     * @param topic     业务 topic
     * @param groupName 隔离组逻辑名 (短名, 不含 defaultGroup 前缀)。框架会自动拼成
     *                  {@code {defaultGroup}:{groupName}} 作为实际 stream 前缀
     * @param count     该隔离组的分区数
     * @return 见上述说明。
     */
    public RedisPartition isolate(String topic, String groupName, int count) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("isolate topic must not be blank");
        }
        // 空白 (含 "   ") groupName 会污染 topicToGroupName / groups 映射, fail-fast
        if (StringUtils.isBlank(groupName)) {
            throw new IllegalArgumentException("isolate groupName must not be blank (use init() for default group)");
        }
        initLock.lock();
        try {
            requirePartitionOpen();
            // 分区组与 topic 路由必须在同一生命周期临界区发布，shutdown 不能只看到其中一半。
            this.initGroup(groupName, count, topic);
            topicToGroupName.put(topic, groupName);
            // 路由声明变化后必须丢弃此前验证的 topic 结果；下一次发布会用新映射复验共享合同。
            publishRouteCache.remove(topic);
            return this;
        } finally {
            initLock.unlock();
        }
    }

    /**
     * 业务作用：拒绝代理销毁或分区停机后的基础设施变更与消息发布，任一终态结论之后不得再产生本地或 Redis 副作用。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；所属代理或当前分区已进入终态时抛出 IllegalStateException。
     */
    private void requirePartitionOpen() {
        if (!redisProxy.partitionAdmissionOpen()) {
            throw new IllegalStateException("RedisPartition is already shut down because RedisProxy ["
                    + redisProxy.getQualifier() + "] is destroyed");
        }
        if (!running) throw new IllegalStateException("RedisPartition is already shut down");
    }

    /**
     * 业务作用：内部统一初始化入口。groupName 用 {@link #DEFAULT_GROUP_NAME} ("") 表示默认共享组,
     * 用业务逻辑短名表示隔离组。
     * <p>
     * <b>总开关</b>: {@code stream.partition.enabled=false} 时静默返回, 业务侧不用包 if-else。
     *
     * @param groupName 见上述说明
     * @param count     数量上限
     * @return 当前分区入口；已初始化或分区功能关闭时保持幂等。
     */
    private RedisPartition initGroup(String groupName, int count) {
        return initGroup(groupName, count, null);
    }

    /**
     * 业务作用：初始化一个分区组，并可在 XGROUP、container 启动和本地组发布前先固定声明该组的 topic 路由。
     *
     * @param groupName  默认组为空串，隔离组使用业务逻辑短名
     * @param count      该组不可变的分区数
     * @param routeTopic 与本次隔离组同时声明的 topic；普通默认组初始化时为 null
     * @return 当前分区入口；功能关闭或组已按相同合同初始化时保持幂等。
     */
    private RedisPartition initGroup(String groupName, int count, String routeTopic) {
        initLock.lock();
        try {
            requirePartitionOpen();
            if (count <= 0) throw new IllegalArgumentException("count must be > 0, got " + count);
            PartitionGroup existing = groups.get(groupName);
            if (existing != null) {
                if (existing.count != count) {
                    throw new IllegalStateException("partition group count mismatch for ["
                            + (groupName.isEmpty() ? "<default>" : groupName)
                            + "]: initialized=" + existing.count + ", requested=" + count);
                }
                if (routeTopic != null) {
                    validateTopicRouteContract(routeTopic,
                            redisProxy.getStream().getPartition().getDefaultGroup(),
                            existing.streamPrefix, existing.count);
                }
                return this;
            }
            if (!redisProxy.getStream().getPartition().isEnabled()) {
                log.info("[{}] partition disabled by config, init({}, {}) skipped",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName, count);
                return this;
            }
            PartitionGroup group = new PartitionGroup(groupName, count);
            // 仅 prepare (创建 stream/group/container, 不 tryClaim) — 避免 listener 还没 register 完
            // 就启 task 拉消息找不到 listener 而丢消息。tryClaim 由后续 startAllGroups() 统一启动。
            try {
                group.prepare(routeTopic);
            } catch (RuntimeException | Error failure) {
                // 准备结论尚未发布；先撤销可能已启动的本地 container，失败组不能留在后台继续运行。
                group.discardAfterPrepareFailure();
                throw failure;
            }
            // 只有远端协议复验和本地资源准备都成功后才发布本地组，失败配置不能留下半初始化入口。
            groups.put(groupName, group);
            redisProxy.streamPartitionMetrics().groupAvailable(this, groupName);
            return this;
        } finally {
            initLock.unlock();
        }
    }

    /**
     * 业务作用：启动所有已注册分区组的消费 — tryClaim 贪心 + 注册 rebalance timer。
     * <p>
     * 调用时机: 必须在所有 {@link #registerListener} 完成之后；缺少精确路由的消息会按
     * BLOCK_CLAIM 保留 PEL 并暂停来源，不能依赖运行期补注册。
     * <p>
     * 框架在 {@link RedisProxy#initialize()} 末尾自动调用 (autoPreparePartitions → loadStreamSubscribe 扫描 → 本方法),
     * 业务方一般不需要手动调; 但如果业务方手动 init/isolate + register, 必须自己调本方法启动消费。
     */
    public void startAllGroups() {
        initLock.lock();
        try {
            requirePartitionOpen();
            // BOTH dedicated container 与物理分区必须在全部路由发布后再统一开放，避免启动期读到半注册 record。
            partitionRuntime.startProxySources();
            for (PartitionGroup g : groups.values()) {
                // rebalanceTimerName != null 表示已 startConsuming 过, 幂等跳过 (防多次调用重复启 timer)
                if (g.rebalanceTimerName != null) continue;
                g.startConsuming();
            }
        } finally {
            initLock.unlock();
        }
    }

    /**
     * 业务作用：注册单条 listener。框架扫描 {@link StreamSubscribe} bean 后把 PARTITION/BOTH 模式转交本方法；
     * Batch 在计划准备期明确拒绝，BOTH 的普通 Stream 侧由共享运行时另建 consumer-fenced dedicated 路径。
     * <p>
     * 业务方一般不需要直接调用，实现 StreamSubscribe bean 并声明 PARTITION 或 BOTH 即可。
     * <p>
     * 注册逻辑: 遍历 listener.topics() 每一个 topic, 按 topicToGroupName 决定落入默认共享组
     * 还是隔离组 (yml partition.groups.{logicalName}.topics 配过的走隔离组), 然后按 (topic, event)
     * 注册到该组的 listenersByTopicEvent 表。
     *
     * @param listener 见上述说明
     * @return 见上述说明。
     */
    public RedisPartition registerListener(StreamSubscribe<?, ?> listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        StreamSubscriptionPlan plan = null;
        boolean activated = false;
        initLock.lock();
        try {
            // 代理与分区终态门禁必须先于 Runner 选择和永久归属；此锁同时阻止 shutdown 穿越本次准备。
            requirePartitionOpen();
            plan = subscriptionRegistry.prepare(listener);
            List<Map.Entry<PartitionGroup, TopicEventKey>> staged = new ArrayList<>();
            for (String topic : plan.topics()) {
                String groupName = topicToGroupName.getOrDefault(topic, DEFAULT_GROUP_NAME);
                PartitionGroup group = groups.get(groupName);
                if (group == null) {
                    throw new IllegalStateException(
                            "partition group [" + (groupName.isEmpty() ? "<default>" : groupName)
                                    + "] not initialized, configure yml stream.partition.groups before registerListener");
                }
                if (group.rebalanceTimerName != null) {
                    throw new IllegalStateException("不能在 RedisPartition 消费启动后追加 listener: "
                            + listener.getClass().getName());
                }
                TopicEventKey key = new TopicEventKey(topic, plan.event());
                if (group.listenersByTopicEvent.containsKey(key)) {
                    throw new IllegalStateException("partition listener route already registered topic="
                            + topic + " event=" + plan.event());
                }
                staged.add(Map.entry(group, key));
            }

            List<TopicRouteContract> routeContracts = new ArrayList<>(staged.size());
            String defaultPrefix = redisProxy.getStream().getPartition().getDefaultGroup();
            for (Map.Entry<PartitionGroup, TopicEventKey> entry : staged) {
                PartitionGroup group = entry.getKey();
                routeContracts.add(topicRouteContract(
                        entry.getValue().topic(), group.streamPrefix, group.count));
            }
            // 先用只读原子快照拒绝已经存在的冲突，避免为确定失败的注册启动代理独占执行域。
            preflightTopicRouteContracts(defaultPrefix, routeContracts);
            // 先完成 Runner、路由和 taskType 的全部可失败就绪动作，远端合同不能指向未激活的本地计划。
            try (StreamSubscriptionRegistry.Activation activation =
                         subscriptionRegistry.prepareActivation(plan)) {
                // 只读预检后仍可能出现跨节点竞争，最终 Lua 必须再次整体复验并登记全部 topic。
                validateTopicRouteContracts(defaultPrefix, routeContracts);
                activation.commit();
                for (Map.Entry<PartitionGroup, TopicEventKey> entry : staged) {
                    entry.getKey().listenersByTopicEvent.put(entry.getValue(), plan);
                    topicToGroupName.putIfAbsent(entry.getValue().topic(), entry.getKey().groupName);
                }
                activated = true;
            }
            return this;
        } catch (Throwable failure) {
            if (plan != null && !activated) subscriptionRegistry.abandon(plan);
            throw failure;
        } finally {
            initLock.unlock();
        }
    }

    // ==================== publish: (topic, event, partition, data) 重载 ====================

    /**
     * 业务作用：按字符串分区键把事件持久写入对应分区 Stream，事件名默认使用 topic。
     *
     * @param topic     主题名
     * @param partition 参与稳定分区计算的业务键；null 时使用轮转分区
     * @param data      业务数据
     * @return Redis Stream 消息 ID。
     */
    public String publish(String topic, String partition, Object data) {
        return this.publish(topic, topic, partition, data);
    }

    /**
     * 业务作用：按长整型分区键把事件持久写入对应分区 Stream，事件名默认使用 topic。
     *
     * @param topic     主题名
     * @param partition 参与稳定分区计算的业务键
     * @param data      业务数据
     * @return Redis Stream 消息 ID。
     */
    public String publish(String topic, long partition, Object data) {
        return this.publish(topic, topic, partition, data);
    }

    /**
     * 业务作用：按字符串分区键把指定事件持久写入对应分区 Stream。
     *
     * @param topic     主题名
     * @param event     事件名
     * @param partition 参与稳定分区计算的业务键；null 时使用轮转分区
     * @param data      业务数据
     * @return Redis Stream 消息 ID。
     */
    public String publish(String topic, String event, String partition, Object data) {
        return this.doPublish(topic, event, partition == null ? -1 : (partition.hashCode() & Integer.MAX_VALUE), partition == null, data);
    }

    /**
     * 业务作用：按长整型分区键把指定事件持久写入对应分区 Stream。
     *
     * @param topic     主题名
     * @param event     事件名
     * @param partition 参与稳定分区计算的业务键
     * @param data      业务数据
     * @return Redis Stream 消息 ID。
     */
    public String publish(String topic, String event, long partition, Object data) {
        return this.doPublish(topic, event, Long.hashCode(partition) & Integer.MAX_VALUE, false, data);
    }

    /**
     * 业务作用：内部统一 publish 入口。
     * <p>
     * 关键不变量: <b>分区数、key 类型和值及 topic 组映射不变时，同一 partition 落到同一分区</b>。
     * 该分区由集群中唯一节点持锁消费，同 partition 的消息进入单一持权处理路径。
     *
     * @param hashAbs    已经屏蔽符号位的 hash (用于 % count 取模)
     * @param topic      主题名
     * @param event      事件名
     * @param data       业务数据
     * @param roundRobin true → 忽略 hashAbs, 用本组 round-robin 均摊到各分区
     * @return 见上述说明。
     */
    private String doPublish(String topic, String event, int hashAbs, boolean roundRobin, Object data) {
        requirePartitionOpen();
        // 高层语义守门 (在 resolveStream / borrow 前): 空白 topic/event 或 null data 会污染路由 + 写入无效 entry
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(event) || data == null) return null;
        // 发布许可把入口复验与真实 XADD 终态连接起来；shutdown 封闭准入后要等待本许可释放。
        PublisherPermit permit = publisherCoordinator.admitDirect();
        try {
            String stream = this.resolveStream(topic, hashAbs, roundRobin);
            PooledEvtData pm = PooledEvtData.of(topic, event, data);
            RecycleLinkedMap<String, Object> pt = RedisProxyHolder.passthrough();
            try {
                pm.setPassthrough(pt);
                return redisProxy.xAdd(stream, DATA_FIELD, pm);
            } finally {
                pm.recycle();
                // PooledEvtData.restore 不再 cascade, caller 显式归还 passthrough 到池
                if (pt != null) pt.recycle();
            }
        } finally {
            permit.complete();
        }
    }

    // ==================== pipeline: 批量分区发布 (嵌入 LettucePipeline.Actuator) ====================
    //
    // 业务场景: settlement 等批处理用 LettucePipeline.Actuator 攒一批 pipeline 操作一次提交,
    // 其中部分消息需要走分区路由。此系列方法让业务侧把分区消息也嵌入同一个 pipeline 批次,
    // 而非每条 publish 独立 xAdd, 保持 pipeline 批量提交的性能优势。
    //
    // 调用链: actuator.partitionAsync → RedisPartition.pipelineAsync → actuator.xAddPartition
    //         (Actuator 是入口, RedisPartition 做路由, 最终写回同一个 Actuator 的 pipeline 队列)

    /**
     * 业务作用：同步 pipeline 分区发布 (event 默认 = topic, partition = long)。
     * <p>
     * 按 partitionKey 路由到固定分区 stream, 消息写入 actuator 的 pipeline 批次,
     * 随 {@link LettucePipeline.Actuator#pipeline(Object)} 一起提交。
     *
     * @param actuator     当前 pipeline 上下文
     * @param topic        业务 topic, 消费端按 (topic, event) 路由 listener
     * @param partitionKey 路由 key (uid / orderId 等), 同 key 同分区同节点串行
     * @param data         实际业务消息体
     */
    public void pipeline(LettucePipeline.Actuator actuator, String topic, long partitionKey, Object data) {
        this.pipeline(actuator, topic, topic, partitionKey, data);
    }

    /**
     * 业务作用：同步 pipeline 分区发布 (event 默认 = topic, partition = String)
     *
     * @param actuator     见上述说明
     * @param topic        主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data         业务数据
     * @see #pipeline(LettucePipeline.Actuator, String, long, Object)
     */
    public void pipeline(LettucePipeline.Actuator actuator, String topic, String partitionKey, Object data) {
        this.pipeline(actuator, topic, topic, partitionKey, data);
    }

    /**
     * 业务作用：同步 pipeline 分区发布 (partition = long)
     *
     * @param event        事件名, 透传到 listener
     * @param actuator     见上述说明
     * @param topic        主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data         业务数据
     * @see #pipeline(LettucePipeline.Actuator, String, long, Object)
     */
    public void pipeline(LettucePipeline.Actuator actuator, String topic, String event, long partitionKey, Object data) {
        this.doPipeline(actuator, topic, event, Long.hashCode(partitionKey) & Integer.MAX_VALUE, false, data, false);
    }

    /**
     * 业务作用：同步 pipeline 分区发布 (partition = String)
     *
     * @param actuator     见上述说明
     * @param topic        主题名
     * @param event        事件名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data         业务数据
     * @see #pipeline(LettucePipeline.Actuator, String, long, Object)
     */
    public void pipeline(LettucePipeline.Actuator actuator, String topic, String event, String partitionKey, Object data) {
        this.doPipeline(actuator, topic, event, partitionKey == null ? -1 : (partitionKey.hashCode() & Integer.MAX_VALUE), partitionKey == null, data, false);
    }

    /**
     * 业务作用：异步 pipeline 分区发布 (event 默认 = topic, partition = long)。
     * <p>
     * 与 {@link #pipeline(LettucePipeline.Actuator, String, long, Object)} 行为一致,
     * 区别在底层用 {@code xAddAsync} (fire-and-forget) 而非 {@code xAdd} (等 future)。
     * 高吞吐场景优先用 async。
     *
     * @param actuator     见上述说明
     * @param topic        主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data         业务数据
     * @see #pipeline(LettucePipeline.Actuator, String, long, Object)
     */
    public void pipelineAsync(LettucePipeline.Actuator actuator, String topic, long partitionKey, Object data) {
        this.pipelineAsync(actuator, topic, topic, partitionKey, data);
    }

    /**
     * 业务作用：异步 pipeline 分区发布 (event 默认 = topic, partition = String)
     *
     * @param actuator     见上述说明
     * @param topic        主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data         业务数据
     * @see #pipelineAsync(LettucePipeline.Actuator, String, long, Object)
     */
    public void pipelineAsync(LettucePipeline.Actuator actuator, String topic, String partitionKey, Object data) {
        this.pipelineAsync(actuator, topic, topic, partitionKey, data);
    }

    /**
     * 业务作用：异步 pipeline 分区发布 (partition = long)
     *
     * @param event        事件名, 透传到 listener
     * @param actuator     见上述说明
     * @param topic        主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data         业务数据
     * @see #pipelineAsync(LettucePipeline.Actuator, String, long, Object)
     */
    public void pipelineAsync(LettucePipeline.Actuator actuator, String topic, String event, long partitionKey, Object data) {
        this.doPipeline(actuator, topic, event, Long.hashCode(partitionKey) & Integer.MAX_VALUE, false, data, true);
    }

    /**
     * 业务作用：异步 pipeline 分区发布 (partition = String)
     *
     * @param actuator     见上述说明
     * @param topic        主题名
     * @param event        事件名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data         业务数据
     * @see #pipelineAsync(LettucePipeline.Actuator, String, long, Object)
     */
    public void pipelineAsync(LettucePipeline.Actuator actuator, String topic, String event, String partitionKey, Object data) {
        this.doPipeline(actuator, topic, event, partitionKey == null ? -1 : (partitionKey.hashCode() & Integer.MAX_VALUE), partitionKey == null, data, true);
    }

    /**
     * 业务作用：pipeline 分区发布统一入口。
     * <p>
     * 路由计算 ({@link #resolveStream}) + {@link PooledEvtData} 包装 +
     * actuator.xAddPartition 写入当前 pipeline 批次并转交发布许可。
     * <p>
     * PooledEvtData 在序列化完成后立即 recycle — Actuator.xAddPartition 内部
     * 先 serialize 成 byte[] 再加入 pipeline 命令队列, 序列化完 pm 对象即无引用。
     *
     * @param actuator   见上述说明
     * @param topic      主题名
     * @param event      事件名
     * @param hashAbs    见上述说明
     * @param roundRobin 见上述说明
     * @param data       业务数据
     * @param async      true → fire-and-forget，false → flush 时等待 Redis future
     *                   返回: 无返回值；命令已交给 Actuator 时由其负责到取消或 Redis 终态，交接失败时撤销发布许可。
     */
    private void doPipeline(LettucePipeline.Actuator actuator, String topic, String event,
                            int hashAbs, boolean roundRobin, Object data, boolean async) {
        requirePartitionOpen();
        // 高层语义守门 (与 doPublish 一致): 空白 topic/event 或 null data 直接 return
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(event) || data == null) return;
        Objects.requireNonNull(actuator, "actuator must not be null");
        // 入队前登记可取消许可；Actuator 接管后，clear、派发异常或 Redis future 终态负责归还。
        PublisherPermit permit = publisherCoordinator.admitQueued();
        boolean handedOff = false;
        try {
            String stream = this.resolveStream(topic, hashAbs, roundRobin);
            PooledEvtData pm = PooledEvtData.of(topic, event, data);
            RecycleLinkedMap<String, Object> pt = RedisProxyHolder.passthrough();
            // try/finally 兜底 (与 doPublish 一致): XADD 序列化 pm 时若抛异常, 仍归还池化的 pm/pt
            try {
                pm.setPassthrough(pt);
                actuator.xAddPartition(stream, DATA_FIELD, pm, async, permit);
                handedOff = true;
            } finally {
                pm.recycle();
                // PooledEvtData.restore 不再 cascade, caller 显式归还 passthrough 到池
                if (pt != null) pt.recycle();
            }
        } finally {
            if (!handedOff) permit.abort();
        }
    }

    /**
     * 业务作用：解析 topic + hash → 目标分区 stream key。
     * <p>
     * {@link #doPublish} 和 {@link #doPipeline} 共用。
     * topic 先按显式 isolate 或 yml 配置确定组，再使用已经通过共享合同复验的前缀、分区数和物理布局。
     *
     * @param hashAbs    已屏蔽符号位的 hash (用于 % count 取模)
     * @param roundRobin true → 忽略 hashAbs, 用本组 round-robin 计数均摊
     * @param topic      主题名
     * @return 目标分区 stream key, 如 "SINGLE-CONSUME:order:detail-batch:17"
     */
    private String resolveStream(String topic, int hashAbs, boolean roundRobin) {
        Objects.requireNonNull(topic, "topic must not be null");
        // 消费组是否已初始化不改变 topic 的配置归属；统一解析并复验后，热路径只读取不可变缓存。
        Object[] route = publishRouteCache.computeIfAbsent(topic, resolvePublishRouteRef);
        String prefix = (String) route[0];
        int count = (int) route[1];
        AtomicInteger routeRoundRobin = (AtomicInteger) route[2];
        int slot = roundRobin ? (routeRoundRobin.getAndIncrement() & Integer.MAX_VALUE) : hashAbs;
        boolean colocated = (boolean) route[3];
        return colocated ? "{" + prefix + "}:" + (slot % count) : prefix + ":" + (slot % count);
    }

    /**
     * 业务作用：解析 topic 对应的物理 Stream 前缀与分区数，并在返回前复验组合同及共享 topic 路由合同。
     * 显式 isolate 映射优先；否则无论本地是否已初始化默认消费组，都按 yml 隔离组配置确定归属。
     *
     * @param topic 主题名
     * @return 已完成远端合同复验的 [streamPrefix, count, roundRobin, colocated] 路由。
     */
    private Object[] resolvePublishRoute(String topic) {
        RedisPartitionProperties pc = redisProxy.getStream().getPartition();
        String defaultPrefix = pc.getDefaultGroup();
        if (StringUtils.isBlank(defaultPrefix)) {
            throw new IllegalStateException("stream.partition.default-group must not be blank");
        }
        String explicitGroup = topicToGroupName.get(topic);
        String configuredGroup = null;
        RedisPartitionProperties.PartitionGroup configuredGroupConfig = null;
        for (Map.Entry<String, RedisPartitionProperties.PartitionGroup> e : pc.getGroups().entrySet()) {
            String logicalName = e.getKey();
            RedisPartitionProperties.PartitionGroup gc = e.getValue();
            List<String> topics = gc.getTopics();
            if ((topics == null || topics.isEmpty()) ? logicalName.equals(topic) : topics.contains(topic)) {
                if (configuredGroup != null && !configuredGroup.equals(logicalName)) {
                    throw new IllegalStateException("[" + redisProxy.getQualifier()
                            + "] topic belongs to multiple partition groups: topic=" + topic
                            + " groups=[" + configuredGroup + ", " + logicalName + "]");
                }
                configuredGroup = logicalName;
                configuredGroupConfig = gc;
            }
        }
        String groupName = explicitGroup != null
                ? explicitGroup
                : configuredGroup == null ? DEFAULT_GROUP_NAME : configuredGroup;
        PartitionGroup initializedGroup = groups.get(groupName);
        String prefix;
        int count;
        AtomicInteger routeRoundRobin;
        if (initializedGroup != null) {
            prefix = initializedGroup.streamPrefix;
            count = initializedGroup.count;
            routeRoundRobin = initializedGroup.roundRobin;
        } else if (!groupName.isEmpty() && groupName.equals(configuredGroup)) {
            prefix = defaultPrefix + ":" + groupName;
            count = configuredGroupConfig.getCount();
            routeRoundRobin = new AtomicInteger();
        } else {
            prefix = defaultPrefix;
            count = pc.getCount();
            routeRoundRobin = new AtomicInteger();
        }
        // count 必须在任何取模、合同写入或 XADD 前确定为有效值。
        if (count <= 0) {
            throw new IllegalStateException("[" + redisProxy.getQualifier() + "] partition count must be > 0, topic=" + topic
                    + " group=" + (groupName.isEmpty() ? "<default>" : groupName)
                    + " count=" + count + " (check yml stream.partition configuration)");
        }
        // 先固定组的物理布局，再固定 topic 到该组的映射；任一冲突都发生在 XADD 入队之前。
        validatePartitionContract(prefix, count);
        validateTopicRouteContract(topic, defaultPrefix, prefix, count);
        return new Object[]{prefix, count, routeRoundRobin, colocatedKeyLayout};
    }

    /**
     * 业务作用：在 Redis 中建立并复验分区组的消息映射与锁权威合同，发布者和消费者共用同一门禁。
     * 已存在但缺少物理布局字段的旧合同会明确拒绝，不能把未知布局解释为当前节点布局。
     *
     * @param streamPrefix 分区组的物理 Stream 前缀
     * @param count        决定业务键取模结果的分区数
     *                     返回: 无返回值；合同首次建立或完全一致时返回，不一致时在 Stream、锁或 XADD 副作用前抛出异常。
     */
    private void validatePartitionContract(String streamPrefix, int count) {
        // 只有真实进入建组或发布协议时才约束物理键；关闭的消费初始化仍保持无副作用返回。
        validatePartitionKeySerializer(redisProxy);
        String lockPrefix = encodeContractValue(distributedLock.keyPrefix());
        String expected = "schema=5;count=" + count
                + ";layout=" + resolvedKeyLayout()
                + ";lock-prefix-b64=" + lockPrefix
                + ";hash=java-hash-sign-mask-mod";
        validateColocatedLockSlot(streamPrefix);
        String cached = partitionContractCache.get(streamPrefix);
        if (expected.equals(cached)) return;
        if (cached != null) {
            throw new IllegalStateException("local partition contract mismatch for " + streamPrefix
                    + ": expected [" + expected + "] but this node already validated [" + cached + "]");
        }
        String key = streamPrefix + ":partition-contract";
        String existing = redisProxy.evalDirectConnection(
                PARTITION_CONTRACT_READ_LUA, String.class, new String[]{key});
        if (expected.equals(existing)) {
            partitionContractCache.putIfAbsent(streamPrefix, expected);
            return;
        }
        if (existing != null) throw partitionContractMismatch(streamPrefix, expected, existing);
        // 新合同把当前 count 作为初始权威，因此两种布局都必须为空，不能从稀疏历史键推断原分区数。
        validatePhysicalPartitionNamespace(streamPrefix, count);
        String actual = redisProxy.evalDirectConnection(
                PARTITION_CONTRACT_LUA, String.class, new String[]{key}, expected);
        if (!expected.equals(actual)) throw partitionContractMismatch(streamPrefix, expected, actual);
        String raced = partitionContractCache.putIfAbsent(streamPrefix, expected);
        if (raced != null && !expected.equals(raced)) {
            throw new IllegalStateException("local partition contract mismatch for " + streamPrefix
                    + ": expected [" + expected + "] but this node already validated [" + raced + "]");
        }
    }

    /**
     * 业务作用：构造组合同不一致异常，并为缺少当前安全证明的旧 schema 给出封闭迁移要求。
     *
     * @param streamPrefix 分区组的物理 Stream 前缀
     * @param expected     当前节点要求的完整合同
     * @param actual       Redis 已有合同
     * @return 包含合同差异与必要迁移边界的异常。
     */
    private IllegalStateException partitionContractMismatch(
            String streamPrefix, String expected, String actual) {
        String migration = "";
        if (actual != null && actual.startsWith("schema=1;")) {
            migration = "; schema=1 does not identify the physical key layout, drain and migrate the old Stream, PEL and lock namespace before replacing this contract";
        } else if (actual != null && actual.startsWith("schema=2;")) {
            migration = "; schema=2 did not prove the entire opposite-layout namespace empty, inspect every master and migrate all old Stream and PEL data before replacing this contract";
        } else if (actual != null && actual.startsWith("schema=3;")) {
            migration = "; schema=3 did not prove the current-layout namespace contains no partition index outside count, inspect every master and migrate all unreachable Stream and PEL data before replacing this contract";
        } else if (actual != null && actual.startsWith("schema=4;")) {
            migration = "; schema=4 did not prove the current-layout namespace empty before adopting count, inspect every master and migrate all existing Stream and PEL data before replacing this contract";
        }
        return new IllegalStateException("partition contract mismatch for " + streamPrefix
                + ": expected [" + expected + "] but Redis contains [" + actual + "]" + migration);
    }

    /**
     * 业务作用：复验 COLOCATED Stream 与加前缀后的真实锁 key 属于同一 Redis Cluster slot。
     * 直接比较最终序列化键，比按字符拒绝花括号更准确，也允许不会改变 slot 的合法前缀。
     *
     * @param streamPrefix 分区组的物理 Stream 前缀
     *                     返回: 无返回值；最终键异槽时在合同、Stream 和锁副作用前拒绝配置。
     */
    private void validateColocatedLockSlot(String streamPrefix) {
        if (!colocatedKeyLayout) return;
        String streamKey = "{" + streamPrefix + "}:0";
        String lockKey = distributedLock.redisKey("{" + streamPrefix + "}:lock:0");
        byte[] serializedStream = Objects.requireNonNull(
                redisProxy.serializeKey(streamKey), "serialized partition Stream key must not be null");
        byte[] serializedLock = Objects.requireNonNull(
                redisProxy.serializeKey(lockKey), "serialized partition lock key must not be null");
        if (SlotHash.getSlot(serializedStream) != SlotHash.getSlot(serializedLock)) {
            throw new IllegalArgumentException("distributed lock prefix places the final partition lock key in a different Redis Cluster slot: stream="
                    + streamKey + " lock=" + lockKey);
        }
    }

    /**
     * 业务作用：在首次建立组合同前扫描全部 Redis master，证明两种布局都不存在历史分区 Stream。
     * 任一节点无法完成两种布局的分页扫描都按证据不足拒绝，不能写入宣称迁移完成的新合同。
     *
     * @param streamPrefix 分区组的物理 Stream 前缀
     * @param count        新合同允许访问的分区数
     *                     返回: 无返回值；发现任一布局的分区 Stream，或无法完整扫描拓扑时抛出异常。
     */
    private void validatePhysicalPartitionNamespace(String streamPrefix, int count) {
        if (!(redisProxy.getRedisTemplate().getConnectionFactory() instanceof LettuceConnectionFactory factory)) {
            throw new IllegalStateException("partition namespace migration requires a Lettuce connection factory");
        }
        String currentPrefix = colocatedKeyLayout
                ? "{" + streamPrefix + "}:"
                : streamPrefix + ":";
        String incompatiblePrefix = colocatedKeyLayout
                ? streamPrefix + ":"
                : "{" + streamPrefix + "}:";
        byte[] currentPattern = Objects.requireNonNull(
                redisProxy.serializeKey(escapeRedisGlob(currentPrefix) + "*"),
                "serialized current partition scan pattern must not be null");
        byte[] incompatiblePattern = Objects.requireNonNull(
                redisProxy.serializeKey(escapeRedisGlob(incompatiblePrefix) + "*"),
                "serialized incompatible partition scan pattern must not be null");
        try {
            if (factory.getNativeClient() instanceof RedisClusterClient client) {
                try (StatefulRedisClusterConnection<byte[], byte[]> connection =
                             client.connect(ByteArrayCodec.INSTANCE)) {
                    List<RedisClusterNode> masters = new ArrayList<>();
                    for (RedisClusterNode node : connection.getPartitions()) {
                        if (node.getRole() != null && node.getRole().isUpstream() && !node.hasNoSlots()) {
                            masters.add(node);
                        }
                    }
                    if (masters.isEmpty()) {
                        throw new IllegalStateException("partition namespace scan found no Redis Cluster master");
                    }
                    for (RedisClusterNode node : masters) {
                        RedisClusterCommands<byte[], byte[]> commands =
                                connection.sync().getConnection(node.getNodeId());
                        validatePartitionNamespaceOnMaster(
                                commands, streamPrefix, count,
                                currentPattern, currentPrefix,
                                incompatiblePattern, incompatiblePrefix);
                    }
                    return;
                }
            }
            if (factory.getNativeClient() instanceof RedisClient client) {
                try (StatefulRedisConnection<byte[], byte[]> connection =
                             client.connect(ByteArrayCodec.INSTANCE)) {
                    validatePartitionNamespaceOnMaster(
                            connection.sync(), streamPrefix, count,
                            currentPattern, currentPrefix,
                            incompatiblePattern, incompatiblePrefix);
                    return;
                }
            }
            throw new IllegalStateException("partition namespace scan requires a Lettuce Redis client");
        } catch (IllegalStateException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new IllegalStateException("partition namespace scan could not prove layout and count compatibility for "
                    + streamPrefix + " count=" + count, failure);
        }
    }

    /**
     * 业务作用：在一个确定 Redis master 上证明相反布局和当前布局都没有历史分区 Stream。
     * 当前布局即使只有低索引 Stream 也不能证明原分区数，必须拒绝静默采纳新 count。
     *
     * @param commands            节点级键命令入口
     * @param streamPrefix        分区组的逻辑 Stream 前缀
     * @param count               新合同允许访问的分区数
     * @param currentPattern      当前布局的序列化 SCAN pattern
     * @param currentPrefix       当前布局的精确文本前缀
     * @param incompatiblePattern 相反布局的序列化 SCAN pattern
     * @param incompatiblePrefix  相反布局的精确文本前缀
     *                            返回: 无返回值；任一命名空间存在不兼容 Stream 时抛出迁移异常。
     */
    private void validatePartitionNamespaceOnMaster(
            RedisKeyCommands<byte[], byte[]> commands,
            String streamPrefix,
            int count,
            byte[] currentPattern,
            String currentPrefix,
            byte[] incompatiblePattern,
            String incompatiblePrefix) {
        String incompatible = findPartitionStream(
                commands, incompatiblePattern, incompatiblePrefix);
        if (incompatible != null) throw incompatibleLayout(streamPrefix, incompatible);
        String existing = findPartitionStream(
                commands, currentPattern, currentPrefix);
        if (existing != null) throw unprovenPartitionCount(streamPrefix, count, existing);
    }

    /**
     * 业务作用：在一个确定 Redis master 上分页扫描候选键，只返回严格数字后缀的真实分区 Stream。
     *
     * @param commands    节点级键命令入口
     * @param pattern     已按当前 key serializer 编码的 SCAN pattern
     * @param exactPrefix 待检查布局的精确文本前缀
     * @return 首个合法分区名对应的 Stream；完整扫描未发现时返回 null。
     */
    private String findPartitionStream(
            RedisKeyCommands<byte[], byte[]> commands,
            byte[] pattern,
            String exactPrefix) {
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = new ScanArgs().match(pattern).limit(256);
        do {
            KeyScanCursor<byte[]> page = commands.scan(cursor, args);
            for (byte[] rawKey : page.getKeys()) {
                String key = redisProxy.deserializeKey(rawKey);
                String indexSuffix = partitionIndexSuffix(key, exactPrefix);
                if (indexSuffix != null
                        && "stream".equalsIgnoreCase(commands.type(rawKey))) {
                    return key;
                }
            }
            cursor = page;
        } while (!cursor.isFinished());
        return null;
    }

    /**
     * 业务作用：严格识别框架生成的非负十进制分区 Stream 名，排除相似业务前缀和控制面键。
     *
     * @param key         SCAN 返回并按配置反序列化的键
     * @param exactPrefix 待识别布局的精确文本前缀
     * @return 后缀是规范非负十进制整数时返回该后缀，否则返回 null。
     */
    private static String partitionIndexSuffix(String key, String exactPrefix) {
        if (key == null || !key.startsWith(exactPrefix)) return null;
        String suffix = key.substring(exactPrefix.length());
        if (suffix.isEmpty() || (suffix.length() > 1 && suffix.charAt(0) == '0')) return null;
        for (int index = 0; index < suffix.length(); index++) {
            char current = suffix.charAt(index);
            if (current < '0' || current > '9') return null;
        }
        return suffix;
    }

    /**
     * 业务作用：把命名空间转成 Redis glob 的字面量，确保配置中的通配字符不会缩小或改写扫描范围。
     *
     * @param value 待作为固定前缀使用的文本
     * @return 可安全追加通配后缀的 Redis glob 字面量。
     */
    private static String escapeRedisGlob(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '\\' || current == '*' || current == '?' || current == '[' || current == ']') {
                escaped.append('\\');
            }
            escaped.append(current);
        }
        return escaped.toString();
    }

    /**
     * 业务作用：构造发现相反布局 Stream 时的封闭迁移异常。
     *
     * @param streamPrefix 分区组的物理 Stream 前缀
     * @param incompatible 已存在的相反布局 Stream
     * @return 阻止新合同建立的异常。
     */
    private IllegalStateException incompatibleLayout(String streamPrefix, String incompatible) {
        return new IllegalStateException("partition key layout mismatch for " + streamPrefix
                + ": resolved layout=" + resolvedKeyLayout()
                + " but incompatible Stream exists [" + incompatible + "]");
    }

    /**
     * 业务作用：构造当前布局已有 Stream、无法证明其原分区数时的封闭迁移异常。
     *
     * @param streamPrefix 分区组的物理 Stream 前缀
     * @param count        新合同允许访问的分区数
     * @param existing     无权威合同可解释原分区数的现存 Stream
     * @return 阻止静默采纳未知历史分区数的异常。
     */
    private IllegalStateException unprovenPartitionCount(
            String streamPrefix, int count, String existing) {
        return new IllegalStateException("partition count adoption requires an empty namespace for " + streamPrefix
                + ": requested count=" + count
                + " but current-layout Stream exists [" + existing + "] and its original count cannot be proven");
    }

    /**
     * 业务作用：原子固定一个 topic 到物理分区组的完整发布路由，阻止不同进程把同一业务键拆到独立锁域。
     *
     * @param topic         业务主题
     * @param defaultPrefix topic 路由合同所在的数据源分区命名空间
     * @param streamPrefix  该 topic 实际写入的分区组前缀
     * @param count         该 topic 使用的分区数
     *                      返回: 无返回值；首次登记或路由完全一致时返回，不一致时在 XADD 入队前抛出异常。
     */
    private void validateTopicRouteContract(
            String topic, String defaultPrefix, String streamPrefix, int count) {
        validateTopicRouteContracts(defaultPrefix,
                List.of(topicRouteContract(topic, streamPrefix, count)));
    }

    /**
     * 业务作用：冻结一个 topic 的完整发布路由，供单条发布与 listener 批量登记共用同一正文格式。
     *
     * @param topic        业务主题
     * @param streamPrefix 该主题实际使用的 Stream 前缀
     * @param count        该主题实际使用的分区数
     * @return 包含 HASH field 和稳定合同正文的不可变路由。
     */
    private TopicRouteContract topicRouteContract(String topic, String streamPrefix, int count) {
        String expected = "schema=1;stream-prefix-b64=" + encodeContractValue(streamPrefix)
                + ";count=" + count
                + ";layout=" + resolvedKeyLayout()
                + ";hash=java-hash-sign-mask-mod";
        return new TopicRouteContract(topic, encodeContractValue(topic), expected);
    }

    /**
     * 业务作用：在一个 Redis 原子单元中先复验全部 topic 路由，再补写全部缺失 field。
     * 任一后项冲突时不写入前项，listener 本地激活与远端合同保持全有或全无。
     *
     * @param defaultPrefix 路由合同 HASH 所在的分区命名空间
     * @param contracts     同一声明中需要整体登记的 topic 路由
     *                      返回: 无返回值；全部兼容时完成缺失登记，任一冲突时不写入并抛出异常。
     */
    private void validateTopicRouteContracts(
            String defaultPrefix, List<TopicRouteContract> contracts) {
        if (contracts.isEmpty()) return;
        String key = defaultPrefix + ":partition-topic-contracts";
        List<?> result = redisProxy.evalDirectConnection(
                TOPIC_ROUTE_CONTRACT_LUA, List.class, new String[]{key},
                topicRouteContractArguments(contracts));
        requireCompatibleTopicRouteContracts(contracts, result);
    }

    /**
     * 业务作用：在启动代理独占 Runner 前原子读取全部 topic 合同，提前拒绝当前已经确定的路由冲突。
     * 最终登记仍会再次原子复验，不能把本次只读结果作为跨节点提交权威。
     *
     * @param defaultPrefix 路由合同 HASH 所在的分区命名空间
     * @param contracts     同一 listener 准备声明的全部 topic 路由
     *                      返回: 无返回值；当前快照兼容时返回，任一冲突时在 Runner 启动前抛出异常。
     */
    private void preflightTopicRouteContracts(
            String defaultPrefix, List<TopicRouteContract> contracts) {
        if (contracts.isEmpty()) return;
        String key = defaultPrefix + ":partition-topic-contracts";
        List<?> result = redisProxy.evalDirectConnection(
                TOPIC_ROUTE_CONTRACT_PREFLIGHT_LUA, List.class, new String[]{key},
                topicRouteContractArguments(contracts));
        requireCompatibleTopicRouteContracts(contracts, result);
    }

    /**
     * 业务作用：把一组 topic 合同编码为 Lua 依次读取的 field、正文参数对。
     *
     * @param contracts 待编码的 topic 路由合同
     * @return 与合同顺序一致且长度为合同数两倍的参数数组。
     */
    private static Object[] topicRouteContractArguments(List<TopicRouteContract> contracts) {
        Object[] arguments = new Object[contracts.size() * 2];
        for (int index = 0; index < contracts.size(); index++) {
            TopicRouteContract contract = contracts.get(index);
            arguments[index * 2] = contract.field();
            arguments[index * 2 + 1] = contract.expected();
        }
        return arguments;
    }

    /**
     * 业务作用：解释 topic 合同 Lua 的稳定结果，并把冲突位置转换为包含业务 topic 的拒绝异常。
     *
     * @param contracts 本次按顺序提交给 Lua 的合同
     * @param result    Redis 返回的状态、冲突位置与已有正文
     *                  返回: 无返回值；兼容状态返回，协议异常或路由冲突时抛出 IllegalStateException。
     */
    private static void requireCompatibleTopicRouteContracts(
            List<TopicRouteContract> contracts, List<?> result) {
        if (result != null && !result.isEmpty()
                && result.getFirst() instanceof Number status && status.longValue() == 1L) {
            return;
        }
        if (result == null || result.size() < 3 || !(result.get(1) instanceof Number position)) {
            throw new IllegalStateException("partition topic route contract returned an invalid result");
        }
        int contractIndex = (position.intValue() - 1) / 2;
        if (contractIndex < 0 || contractIndex >= contracts.size()) {
            throw new IllegalStateException("partition topic route contract returned an invalid conflict index");
        }
        TopicRouteContract conflict = contracts.get(contractIndex);
        throw new IllegalStateException("partition topic route mismatch for " + conflict.topic()
                + ": expected [" + conflict.expected() + "] but Redis contains [" + result.get(2) + "]");
    }

    /**
     * 业务作用：把合同中的任意 UTF-8 标识转换为无分隔符歧义的稳定文本。
     *
     * @param value 待编码的锁前缀、Stream 前缀或 topic
     * @return 不带填充的 URL-safe Base64 文本。
     */
    private static String encodeContractValue(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 业务作用：返回当前连接与配置共同解析出的物理键布局标识，供组合同与 topic 合同使用同一事实。
     *
     * <p>参数说明: 无。
     *
     * @return {@code colocated} 表示共享 hash tag，{@code plain} 表示普通前缀键。
     */
    private String resolvedKeyLayout() {
        return colocatedKeyLayout ? "colocated" : "plain";
    }

    /**
     * 业务作用：优雅停机：封闭发布与消费入口，排干在途任务并释放分区锁，最后停止本代理独占的 Partition 执行域。
     * 由进程级 {@link Graceful} 入口或 RedisProxy 生命周期调用，一般不需要业务侧手动执行。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回: 无返回值；重复调用不重做已完成的来源关闭，并继续收敛尚未归零的运行时责任和独占执行域；普通 shutdown 保留终态门面，
     * 只有命令代理销毁才解除静态登记。
     */
    public void shutdown() {
        shutdownLock.lock();
        try {
            initLock.lock();
            try {
                // 终态标志与全部初始化入口共用临界区；此前进入 prepare 的调用先完成，之后的调用在外部副作用前拒绝。
                running = false;
            } finally {
                initLock.unlock();
            }

            if (!sourcesShutdownComplete) {
                // 先封闭发布入口：排队命令就地取消，在途命令到达 Redis 终态后才允许继续释放消费者。
                publisherCoordinator.closeAndDrain();
                // 先在线性化边界关闭新批次并取消未运行 Submission，运行中的 listener 仍保留 holder 到 drain 完成。
                partitionRuntime.beginShutdown();
                for (PartitionGroup g : groups.values()) {
                    try {
                        g.shutdown();
                    } catch (Throwable t) {
                        log.error("[{}] partition group shutdown failed group={} streamPrefix={}",
                                redisProxy.getQualifier(), g.groupName.isEmpty() ? "<default>" : g.groupName, g.streamPrefix, t);
                    }
                }
                groups.clear();
                // 来源关闭只代表不再产生新读取；迟到 ACK 与恢复调用仍由运行时持有责任，不能据此发布停机完成。
                sourcesShutdownComplete = true;
            }

            // 所有来源封闭后启动运行时关闭；排干预算只限制本轮等待，重复入口必须继续保留尚未真正终止的责任。
            partitionRuntime.close();
            if (!partitionRuntime.shutdownComplete()) return;

            if (ownedRunnerShutdownComplete) return;
            try {
                // Redis 来源与确认路径均已收口，此时停止独占 Runner 不会截断业务任务，也不会影响其它 RedisProxy。
                ownedRunnerShutdownComplete = subscriptionRegistry.stopOwnedRunner();
                if (!ownedRunnerShutdownComplete) {
                    log.error("[{}] owned PartitionRunner did not reach stopped state; its TimingWheel remains running",
                            redisProxy.getQualifier());
                }
            } catch (Throwable error) {
                log.error("[{}] owned PartitionRunner shutdown failed", redisProxy.getQualifier(), error);
            }
        } finally {
            shutdownLock.unlock();
        }
    }

    /**
     * 业务作用：复验来源封闭、运行时排干和独占执行域的共同终态，供所属 RedisProxy 判断能否解除最后的停机责任引用。
     *
     * <p>参数说明: 无。
     *
     * @return 来源已关闭、运行时全部责任归零、等待执行器实际终止且 Runner 与 TimingWheel 均已停止时返回 true。
     */
    public boolean shutdownComplete() {
        shutdownLock.lock();
        try {
            return sourcesShutdownComplete && partitionRuntime.shutdownComplete() && ownedRunnerShutdownComplete;
        } finally {
            shutdownLock.unlock();
        }
    }

    /**
     * 业务作用：按消费组返回当前节点持有的分区索引，供运行监控与故障诊断使用；默认组使用 "&lt;default&gt;"。
     *
     * <p>参数说明: 无。
     *
     * @return 见上述说明。
     */
    public Map<String, List<Integer>> claimedPartitions() {
        Map<String, List<Integer>> r = new LinkedHashMap<>();
        for (Map.Entry<String, PartitionGroup> e : groups.entrySet()) {
            String key = e.getKey().isEmpty() ? "<default>" : e.getKey();
            List<Integer> held = new ArrayList<>();
            for (Map.Entry<Integer, Claim> claim : e.getValue().claims.entrySet()) {
                Claim value = claim.getValue();
                if (value.lock != null && !value.lockLost) held.add(claim.getKey());
            }
            held.sort(Integer::compareTo);
            r.put(key, held);
        }
        return r;
    }

    /**
     * 业务作用：列出已发布逻辑分组供内部 meter 补绑定。参数说明: 无。返回: 不可变分组名集合。
     */
    Set<String> metricGroupNames() {
        return Set.copyOf(groups.keySet());
    }

    /**
     * 业务作用：向内部指标桥接提供当前已发布计划的不可变快照，不暴露内部 planId 作为标签。
     *
     * <p>参数说明: 无。
     *
     * @return 按注册顺序冻结的计划列表
     */
    List<StreamSubscriptionPlan> metricPlans() {
        return partitionRuntime.plans();
    }

    /**
     * 业务作用：取得指标标签使用的 RedisProxy qualifier。参数说明: 无。返回: 稳定实例限定名。
     */
    String metricQualifier() {
        return redisProxy.getQualifier();
    }

    /**
     * 业务作用：取得本进程分区会话身份。参数说明: 无。返回: 仅用于节点级标签的 instanceId。
     */
    String metricInstanceId() {
        return nodeId;
    }

    /**
     * 业务作用：读取共享 dispatcher 的低基数运行快照。参数说明: 无。返回: 固定指标名到当前值的映射。
     */
    Map<String, Long> runtimeMetrics() {
        return partitionRuntime.metrics();
    }

    /**
     * 业务作用：读取一个逻辑分组最近完成的集群观测，不在 meter scrape 线程执行 Redis 命令。
     *
     * @param groupName 逻辑分组名；默认组为空字符串
     * @return 分组不存在时返回零值快照，否则返回本地 owner 与最近 PEL/收敛采样
     */
    RedisPartitionMetricSnapshot metricSnapshot(String groupName) {
        PartitionGroup group = groups.get(groupName == null ? DEFAULT_GROUP_NAME : groupName);
        if (group == null) {
            return new RedisPartitionMetricSnapshot(
                    0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }
        int fair = group.currentFair == Integer.MAX_VALUE ? group.count : group.currentFair;
        return new RedisPartitionMetricSnapshot(
                running ? 1L : 0L,
                group.realLockCount.get(),
                fair,
                group.claimGeneration.get(),
                group.pelPending,
                group.pelOldestIdleMillis,
                group.ownerConvergenceMillis,
                group.pelTakeoverMillis,
                group.lockSelfChecks.get(),
                group.wakeSignals.get(),
                group.rebalanceFallbacks.get(),
                group.drainTimeouts.get());
    }

    // ==================== PartitionGroup ====================

    /**
     * 一个分区组的运行时状态。每组一套 stream/group/lock, 一个独立的 rebalance timer。
     * 默认共享组与隔离组并存, 互不影响。
     * <p>
     * <b>命名空间分离</b>: {@link #groupName} 是业务逻辑短名 (默认组="", 隔离组="contract:settlement"),
     * {@link #streamPrefix} 是真实 Redis key 命名空间 ({defaultGroup}[:{groupName}]).
     * stream/lock/consumer-group 名都以 streamPrefix 拼接, business code 只感知 groupName。
     */
    final class PartitionGroup {

        /**
         * 业务逻辑短名: "" = 默认共享组, 非空 = 隔离组的 yml key
         */
        final String groupName;
        /**
         * 真实 Redis 命名空间. 默认组 = defaultGroup, 隔离组 = defaultGroup + ":" + groupName
         */
        final String streamPrefix;
        final int count;

        // -- 运行时参数: 优先 per-group 覆盖 → RedisPartitionProperties 父级 → Stream 全局 (在构造时一次解析定型) --

        /**
         * 再平衡周期 ms
         */
        final long rebalancePeriodMs;
        /**
         * XAUTOCLAIM 最小空闲阈值，默认 30s；只决定 PEL 接管资格，不代替锁权威或本地执行权。
         */
        final long minIdleMs;
        /**
         * holds() 自检最小间隔 ms。runner 每个轮询周期都可能触发 beforePoll,
         * 但只有距上次 holds 间隔 &gt;= 本字段才真正发 EVAL, 避免 NOBLOCK 模式下空轮询打爆 holds()。
         */
        final long holdsCheckIntervalMs;
        /**
         * Claim 持权排干预算 ms。主动 stop 后允许在途 listener 与恢复任务在预算内完成确认；
         * 预算耗尽后撤销来源代次并交回未决 PEL，ACK fencing 拒绝迟到 XACK，本地任务仍须实际结束。
         * <p>
         * 来源: yml partition.groups.{groupName}.drainTimeoutMs 覆盖 → RedisPartitionProperties.drainTimeoutMs 父级.
         */
        final long drainTimeoutMs;
        /**
         * 拉取批量大小. 同时作为:
         * (1) 本组独立 container 的 XREADGROUP COUNT (per-group 真正生效);
         * (2) XAUTOCLAIM 单页 count.
         * 来源: yml partition.groups.{groupName}.batchSize 覆盖 → Stream.batchSize 全局。
         */
        final int batchSize;
        /**
         * 拉取阻塞超时 ms. 本组独立 container 的 XREADGROUP BLOCK。
         * 来源: yml partition.groups.{groupName}.pollTimeout 覆盖 → Stream.pollTimeout 全局。
         */
        final int pollTimeout;

        /**
         * 本组独立的 stream listener container。
         * <p>
         * <b>per-group 隔离的关键</b>: 各组独立 container 持有各自的 readOptions (BLOCK / COUNT),
         * 这是 DI 容器层的限制 — readOptions 是 container 启动时定型的,
         * 同一 container 内的所有 task 共用。N 个分区组 = N 个 container, 每组按自己的
         * batchSize/pollTimeout 跑。
         * <p>
         * 每组容器共享 RedisProxy 装配的 connectionFactory、serializer 与执行基础设施；
         * 具体线程和连接占用由底层容器、连接工厂与业务执行器配置共同决定。
         */
        final BatchStreamMessageListenerContainer<String, MapRecord<String, Object, Object>> container;

        /**
         * (topic, event) → listener。两个字段保持独立，不能用可出现在业务值中的字符拼接。
         * <p>
         * 由 {@link #registerListener(StreamSubscribe)} 在计划激活后写入，dispatcher 按两个字段精确路由。
         * 同一 (topic, event) 只能注册一个 listener，冲突在任何路由发布前拒绝。
         */
        final Map<TopicEventKey, StreamSubscriptionPlan> listenersByTopicEvent = new ConcurrentHashMap<>();
        /**
         * 已认领的分区: 索引 → claim. 占位语义: 防止同 partition 重复 tryClaim.
         * <p>
         * <b>注意</b>: rebalance release 会先从 claims 移除 claim 再 markStop, 此时 claim 仍 live in ManagedRunner.
         * 不能用 claims 同时表达 "分区占位" 和 "runner 中 live task 集合" — shutdown 等待用 {@link #liveClaims}.
         */
        final Map<Integer, Claim> claims = new ConcurrentHashMap<>();
        /**
         * 当前真正 live 在 ManagedRunner 中的 Claim 集合 (与 partition 索引解耦).
         * <p>
         * 与 {@link #claims} 的区别:
         * <ul>
         *   <li>{@link #claims} 按 partition 索引, rebalance release 时被 remove, 但 Claim 实例仍在 runner 内 drain</li>
         *   <li>{@link #liveClaims} 在 tryClaim 注册到 container 后加入, afterExit 完整跑完后移除</li>
         * </ul>
         * shutdown 用本字段 snapshot 算 latch 容量, 确保已脱离 claims 但仍 live 的 release task 也被等到.
         */
        final Set<Claim> liveClaims = ConcurrentHashMap.newKeySet();
        /**
         * null key 时的 round-robin 计数
         */
        final AtomicInteger roundRobin = new AtomicInteger();
        /**
         * rebalance 可能同时由周期定时器和 wake-up 通道触发, 同一组内只允许一个在跑。
         */
        final AtomicBoolean rebalancing = new AtomicBoolean();

        String rebalanceTimerName;

        /**
         * 当前真锁数, tryLock 成功 +1, unlock/lockLost -1, 原子操作避免热路径遍历
         */
        final AtomicInteger realLockCount = new AtomicInteger();

        /**
         * 每次成功取得真实分区锁后单调递增，节点级指标不使用 partition 作为标签。
         */
        final AtomicLong claimGeneration = new AtomicLong();

        /**
         * holder 自检、通知加速、周期兜底和排干超时的分组级低基数累计值。
         */
        final AtomicLong lockSelfChecks = new AtomicLong();
        final AtomicLong wakeSignals = new AtomicLong();
        final AtomicLong rebalanceFallbacks = new AtomicLong();
        final AtomicLong drainTimeouts = new AtomicLong();

        /**
         * 最近一次完整 PEL 采样结果；meter scrape 只读缓存，不直接访问 Redis。
         */
        volatile long pelPending;
        volatile long pelOldestIdleMillis;

        /**
         * 最近一次 owner 目标收敛与历史 PEL 接管耗时。
         */
        volatile long ownerConvergenceMillis;
        volatile long pelTakeoverMillis;
        private volatile long ownerConvergenceStartNanos;
        private volatile int ownerConvergenceTarget = Integer.MIN_VALUE;
        private volatile long lastPelSampleNanos;

        /**
         * 当前 fair 值 (每轮 rebalance 更新, volatile 跨线程可见)。
         * Claim.beforeStart 用此值判断是否已持有足够真锁, 防止 zombie retry 与 rebalance 释放对冲。
         * 初始 MAX_VALUE: 首轮 rebalance 之前不限制, 允许启动贪心抢占。
         */
        volatile int currentFair = Integer.MAX_VALUE;

        /**
         * rebalance 释放倒计数: step2 设为释放数, 每个 afterExit -1 (仅当 Claim.releaseGen 匹配当前 generation),
         * 最后一个到 0 时 pub wake.
         */
        final AtomicInteger pendingReleaseCount = new AtomicInteger();
        /**
         * release batch 代号 (每次新一轮 release 或保险阀 reset 时递增).
         * Claim 记录自己被计入的 generation, afterExit decrement 只对自己 batch 计数,
         * 防止跨 batch 污染 (例如保险阀 reset 后旧 Claim 迟到 afterExit 减掉新 batch 的计数).
         */
        final AtomicInteger releaseGeneration = new AtomicInteger();
        /**
         * 本轮 rebalance release set(N) 的时间戳, 给"卡死保险阀"用.
         * <p>
         * 如果某轮 release 因边界竞争 (例如 rebalance step 与独立 lockLost 同时发生) 造成
         * pendingReleaseCount 永远不归零, 后续 rebalance 会被 {@code pendingReleaseCount.get() > 0}
         * 永久挡住。超过排干时限与再平衡调度余量后，强制重置计数，让 release 路径自行恢复。
         * <p>
         * volatile: rebalance (TimingWheel 线程) 写, 后续 rebalance 读, 跨线程可见.
         */
        volatile long releaseStartNanos;

        /**
         * 跨节点 wake-up 通道. 任一节点释放 partition 后 pub 一个空消息, 其它节点收到后立即 rebalance,
         * 不必等下个周期 (默认 3s) 才收敛. 自己也会收到自己的 pub, handler 里会重复跑一次 rebalance,
         * 因为已经平衡过 (claims.size=fair) 这次会是空跑, 可接受.
         */
        final String wakeChannel;

        /**
         * shutdown 期间临时启用的全组 latch，容量 = shutdown 时的 claims.size。
         * <p>
         * 所有 Claim.afterExit 共享 countDown 同一个 latch，shutdown 只执行一次有上限的 await
         * （默认 3s），使总等待时长与 task 数量解耦。runner finally 并行执行 exitManaged。
         * <p>
         * 非 shutdown 阶段（rebalance 释放等）latch=null，afterExit 跳过 countDown。
         */
        volatile CountDownLatch shutdownLatch;

        /**
         * 业务作用：描述一个分区组：组名与分区数。
         * <p>
         * 分区数<b>确定后不可更改</b>：分区键到分区的映射依赖它，改变分区数会让同一个键落到不同分区，
         * 该键的历史消息与新消息不再由同一个消费者按序处理。
         *
         * @param groupName 分区组名
         * @param count     分区数
         *                  返回: 构造出的分区组；配置非法时拒绝创建任何消费任务。
         */
        PartitionGroup(String groupName, int count) {
            this.groupName = groupName;
            this.count = count;
            // 配置取值: 优先 per-group 覆盖 → RedisPartitionProperties 父级 → Stream 全局
            // 这里一次性 resolve 定型, 后续运行不再读配置, 避免反复 lookup
            NasaLettuceConfig.Stream sc = redisProxy.getStream();
            RedisPartitionProperties pc = sc.getPartition();
            // defaultGroup 是所有分区组的命名空间前缀, 必须非空 — yml 写 null 或空串会让所有 stream/lock key 退化成奇怪形态
            String defaultGroup = pc.getDefaultGroup();
            if (defaultGroup == null || defaultGroup.isBlank()) {
                throw new IllegalStateException(
                        "yml stream.partition.default-group must be a non-empty string "
                                + "(it is the namespace prefix for all partition stream/lock keys, default 'SINGLE-CONSUME')");
            }
            // 拼真实 Redis 命名空间: 默认组直接用 defaultGroup, 隔离组 = defaultGroup:groupName
            this.streamPrefix = groupName.isEmpty() ? defaultGroup : defaultGroup + ":" + groupName;
            this.wakeChannel = streamPrefix + ":wake";
            // per-group 覆盖配置用 groupName (短名) 查 yml partition.groups; 默认组没有 per-group 覆盖, 直接用父级
            RedisPartitionProperties.PartitionGroup gc = groupName.isEmpty() ? null : pc.getGroups().get(groupName);
            this.rebalancePeriodMs = (gc != null && gc.getRebalanceMs() != null) ? gc.getRebalanceMs() : pc.getRebalanceMs();
            this.minIdleMs = (gc != null && gc.getMinIdleMs() != null) ? gc.getMinIdleMs() : pc.getMinIdleMs();
            this.holdsCheckIntervalMs = (gc != null && gc.getHoldsCheckIntervalMs() != null) ? gc.getHoldsCheckIntervalMs() : pc.getHoldsCheckIntervalMs();
            this.drainTimeoutMs = (gc != null && gc.getDrainTimeoutMs() != null) ? gc.getDrainTimeoutMs() : pc.getDrainTimeoutMs();
            this.batchSize = (gc != null && gc.getBatchSize() != null) ? gc.getBatchSize() : sc.getBatchSize();
            this.pollTimeout = (gc != null && gc.getPollTimeout() != null) ? gc.getPollTimeout() : sc.getPollTimeout();
            if (rebalancePeriodMs < 1 || rebalancePeriodMs > Long.MAX_VALUE / 3) {
                throw new IllegalStateException("stream.partition rebalance-ms must be between 1 and "
                        + (Long.MAX_VALUE / 3) + ": " + rebalancePeriodMs);
            }
            if (minIdleMs < 0) {
                throw new IllegalStateException("stream.partition min-idle-ms must not be negative: " + minIdleMs);
            }
            if (holdsCheckIntervalMs < 1 || drainTimeoutMs < 1 || batchSize < 1 || pollTimeout < 1) {
                throw new IllegalStateException("stream.partition holds-check-interval-ms, drain-timeout-ms, "
                        + "batch-size and poll-timeout must be greater than zero");
            }
            // 创建本组独立 container. 参数仅 pollTimeout/batchSize 不同, 其他 (executor/serializer/connectionFactory)
            // 全部复用 RedisProxy. 工厂方法 createListenerContainer 由 RedisProxy 负责装配, 与 subscribe 路径
            // 的 dedicated container 共用同一个工厂。
            this.container = redisProxy.createListenerContainer(
                    pollTimeout, batchSize, partitionRuntime.waitExecutor());
        }

        /**
         * 业务作用：按连接与显式配置生成物理 Stream key，Cluster 布局用共享 hash tag 与锁键同 slot。
         *
         * @param partition 物理分区索引
         * @return 当前部署合同下的 Stream key
         */
        String streamKey(int partition) {
            return colocatedKeyLayout
                    ? "{" + streamPrefix + "}:" + partition
                    : streamPrefix + ":" + partition;
        }

        /**
         * 业务作用：生成与对应物理 Stream 同 slot 的分布式锁业务 key，供 holder-fenced Lua 使用。
         *
         * @param partition 物理分区索引
         * @return 交给 LettuceDistributedLock 添加统一前缀的业务 lock key
         */
        String lockKey(int partition) {
            return colocatedKeyLayout
                    ? "{" + streamPrefix + "}:lock:" + partition
                    : streamPrefix + ":lock:" + partition;
        }

        /**
         * 业务作用：准备阶段: 创建 stream consumer group + auto-trim + 启动本组独立 container。
         * <p>
         * <b>不调 tryClaim</b> — 此时 listener 可能还没 register 完, 不能让 task 拉到消息时
         * 找不到 listener 而丢消息。tryClaim 推迟到 {@link #startConsuming()} 阶段, 由框架在
         * listener 全部 register 完成后统一调用。
         * <p>
         * 调用时机: 业务方 / 框架 init/isolate 阶段, 早于 listener 扫描注册。
         *
         * @param routeTopic 与新隔离组同时声明的 topic；普通组初始化时为 null
         *                   返回: 无返回值；合同不一致或本地资源准备失败时抛出异常且不发布本地分区组。
         */
        void prepare(String routeTopic) {
            // 分区数、物理布局、锁命名空间与散列规则共同决定消息和所有权映射；任何不一致都先于建组拒绝。
            RedisPartition.this.validatePartitionContract(streamPrefix, count);
            if (routeTopic != null) {
                // 已知 topic 冲突必须先于 XGROUP、container 启动和本地组发布返回失败。
                RedisPartition.this.validateTopicRouteContract(routeTopic,
                        redisProxy.getStream().getPartition().getDefaultGroup(), streamPrefix, count);
            }
            // step 1: 准备每个分区 stream 的运行时元数据
            // - xGroupCreate: 给每个 stream 建消费者组；RedisProxy 只把 BUSYGROUP 归为并发幂等成功
            // - streamAutoTrim: 注册到 RedisProxy 的 stream 裁剪 cache, 由现有定时任务批量 XTRIM
            // - consumer group 名 = streamPrefix (跟 stream key 命名空间一致, 多节点共用一个 group)
            for (int i = 0; i < count; i++) {
                String stream = streamKey(i);
                // 从 0-0 创建组：publisher 先写、consumer 后建组时仍须读取历史；其它 Redis 失败必须阻止本地组发布。
                redisProxy.xGroupCreate(stream, streamPrefix, ReadOffset.from("0-0"));
                try {
                    redisProxy.streamAutoTrim(stream);
                } catch (Exception ignored) {
                    // cluster 未启用时 streamAutoTrim 直接 return; 兜底捕获以防未来变更
                }
            }
            // step 2: 启动本组独立 container
            // BatchStreamMessageListenerContainer.isAutoStartup() = false, 必须手动 start.
            // start() 后 register 进来的 task 才会被 executor.execute (但本阶段不 register, 由 startConsuming 阶段做)。
            this.container.start();
        }

        /**
         * 业务作用：准备失败时停止尚未发布的本地 container，避免部分启动的拉取任务脱离分区组生命周期。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；清理异常只记录诊断，原始准备失败继续交给调用方。
         */
        void discardAfterPrepareFailure() {
            try {
                this.container.stop();
            } catch (Throwable cleanupFailure) {
                log.warn("[{}] partition container cleanup failed group={} streamPrefix={}",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName,
                        streamPrefix, cleanupFailure);
            }
        }

        /**
         * 业务作用：以 Redis 服务端时间续约当前节点并清理过期成员，返回本组一致可见的存活节点数。
         *
         * <p>参数说明: 无。
         *
         * @return 清理过期成员后的节点数量，最小按一处理以保持分区份额可计算。
         */
        private int heartbeat() {
            Long alive = redisProxy.evalDirectConnection(
                    PARTITION_HEARTBEAT_LUA,
                    Long.class,
                    new String[]{streamPrefix + ":nodes"},
                    3 * rebalancePeriodMs,
                    nodeId);
            return Math.max(1, alive == null ? 1 : alive.intValue());
        }

        /**
         * 业务作用：记录最近公平 owner 目标及其收敛起点，真实锁数达到目标后发布耗时。
         *
         * @param target 最近一次心跳计算的本节点公平份额
         *               返回: 无返回值。
         */
        private synchronized void observeOwnerTarget(int target) {
            long now = System.nanoTime();
            if (ownerConvergenceTarget != target) {
                ownerConvergenceTarget = target;
                ownerConvergenceStartNanos = now;
            } else if (ownerConvergenceStartNanos == 0L && realLockCount.get() != target) {
                ownerConvergenceStartNanos = now;
            }
            if (realLockCount.get() == target && ownerConvergenceStartNanos != 0L) {
                ownerConvergenceMillis = TimeUnit.NANOSECONDS.toMillis(now - ownerConvergenceStartNanos);
                ownerConvergenceStartNanos = 0L;
            }
        }

        /**
         * 业务作用：在 Claim 取得或释放真实锁后复验当前公平目标，异步 owner 变化也能结束收敛计时。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值。
         */
        private void observeOwnerCount() {
            int target = ownerConvergenceTarget;
            if (target != Integer.MIN_VALUE) observeOwnerTarget(target);
        }

        /**
         * 业务作用：在组件控制执行域内完整分页采集本逻辑组全部物理 Stream 的 PEL 数量与最大 idle。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；采样失败时发布 -1，避免把局部结果冒充全组健康。
         */
        private void samplePelIfDue() {
            long now = System.nanoTime();
            long intervalMillis = Math.max(10_000L, rebalancePeriodMs);
            if (lastPelSampleNanos != 0L
                    && now - lastPelSampleNanos < TimeUnit.MILLISECONDS.toNanos(intervalMillis)) return;
            lastPelSampleNanos = now;
            long pendingTotal = 0L;
            long oldestIdleMillis = 0L;
            try {
                for (int partition = 0; partition < count; partition++) {
                    String stream = streamKey(partition);
                    PendingMessagesSummary summary = redisProxy.xPending(stream, streamPrefix);
                    long pending = summary == null ? 0L : summary.getTotalPendingMessages();
                    pendingTotal += pending;
                    String afterId = null;
                    long scanned = 0L;
                    while (scanned < pending) {
                        PendingMessages page = redisProxy.xPendingPage(
                                stream, streamPrefix, afterId, Math.min(512L, pending - scanned));
                        if (page == null || page.isEmpty()) break;
                        for (org.springframework.data.redis.connection.stream.PendingMessage message : page) {
                            oldestIdleMillis = Math.max(
                                    oldestIdleMillis,
                                    message.getElapsedTimeSinceLastDelivery().toMillis());
                            afterId = message.getIdAsString();
                            scanned++;
                        }
                    }
                }
                pelPending = pendingTotal;
                pelOldestIdleMillis = oldestIdleMillis;
            } catch (Throwable failure) {
                pelPending = -1L;
                pelOldestIdleMillis = -1L;
                log.warn("[{}] partition PEL metrics collection failed group={} streamPrefix={}",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName,
                        streamPrefix, failure);
            }
        }

        /**
         * 业务作用：启动消费阶段: 贪心 tryClaim 各分区 + 注册周期 rebalance 任务。
         * <p>
         * 必须在 listener 全部 register 完成后调用，否则无精确计划的消息会进入 routeBlocked 并暂停来源。
         *
         * <p>参数说明: 无。
         * <p>
         * 返回: 无返回值；启动心跳、通知通道、初始认领和周期再平衡，单次通知失败由周期任务兜底。
         */
        void startConsuming() {
            // step 3: 立即 ZADD 自己进 alive 注册, 让其它节点的下一次 rebalance 立刻看到我.
            // 不能等到第一次 rebalance 才注册 — 那要等 rebalancePeriodMs (默认 3s), 中间窗口我在线但隐身.
            try {
                this.heartbeat();
            } catch (Exception e) {
                log.warn("[{}] partition initial heartbeat failed group={} streamPrefix={}",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName, streamPrefix, e);
            }
            // step 4: 订阅 wake-up channel. 任一节点释放 partition → pub → 所有节点 (含自己) 立即 rebalance,
            // pub/sub 只发出轻量时间轮信号，实际 Redis 控制操作由组件专用执行域承接。
            try {
                redisProxy.subscribe(wakeChannel, (msg) -> {
                    if (!running) return;
                    String body = msg == null ? "" : msg.toString();
                    // nodeId 的应用名部分允许包含冒号，不能只截取最后一段比较；按完整后缀识别自己发出的通知。
                    if (body.endsWith(":" + nodeId)) return;
                    wakeSignals.incrementAndGet();
                    // 打印上线/下线通知 (release 不打, 频率高靠状态摘要体现)
                    if (body.startsWith("online:")) {
                        log.info("[{}] 收到分区节点上线通知: {} streamPrefix={}", redisProxy.getQualifier(), body, streamPrefix);
                    } else if (body.startsWith("offline:")) {
                        log.info("[{}] 收到分区节点下线通知: {} streamPrefix={}", redisProxy.getQualifier(), body, streamPrefix);
                    }
                    TimingWheel.platform(this::rebalance);
                });
            } catch (Exception e) {
                log.warn("[{}] partition wake-up subscribe failed group={} streamPrefix={}",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName, streamPrefix, e);
            }
            // step 5: 启动时贪心扫一遍
            // 第一个起来的节点拿到所有分区, 后续节点起来时通过 rebalance 公式 (count/aliveNodes) 均摊
            for (int i = 0; i < count && running; i++) this.tryClaim(i);
            // step 6: 注册周期再平衡任务
            // unique name 包含 qualifier + streamPrefix, 避免多 RedisProxy / 多隔离组互相覆盖
            this.rebalanceTimerName = "redis-partition:rebalance:" + redisProxy.getQualifier() + ":" + streamPrefix;
            TimingWheel.platform(rebalancePeriodMs, rebalancePeriodMs, rebalanceTimerName, () -> {
                rebalanceFallbacks.incrementAndGet();
                rebalance();
            });
            // (分区锁定的逐条日志已在 beforeStart 中打印, 不需要额外快照)

            Map<String, Long> runtimeMetrics = partitionRuntime.metrics();
            log.info("[{}] 分区组启动: group={} streamPrefix={} 总分区={} batchSize={} pollTimeout={} "
                            + "lockLeaseMs={} minIdleMs={} proxyPendingMinIdleMs={} drainTimeoutMs={} "
                            + "listenerP99Ms={} listenerObservations={} 初始claim={} listener数={}",
                    redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName,
                    streamPrefix, count, batchSize, pollTimeout,
                    redisProxy.distributedLockLeaseMillis(), minIdleMs,
                    redisProxy.getStream().getPartition().getLocalConsumer().getProxyPendingMinIdleMs(), drainTimeoutMs,
                    runtimeMetrics.getOrDefault("listener_p99_ms", -1L),
                    runtimeMetrics.getOrDefault("listener_observations", 0L),
                    claims.size(), listenersByTopicEvent.size());

            // step 7: 通知其它节点立刻 rebalance, 不必等下个周期.
            // 只是加速提示 (有周期 rebalance 兜底), pub 临时失败不应让启动流程失败, 故 try/catch 只 warn.
            try {
                redisProxy.pub(wakeChannel, "online:" + nodeId);
            } catch (Exception e) {
                log.warn("[{}] partition online wake pub failed group={} streamPrefix={} (依赖周期 rebalance 兜底)",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName, streamPrefix, e);
            }
        }

        /**
         * 业务作用：注册分区 task 到 container。非阻塞 — task 进入 ManagedRunner 的 pending 队列,
         * runner 线程异步 initManaged 内做 tryLock, 成功与否由 task.managedSuccess 跟踪。
         * <p>
         * 流程:
         * <ol>
         *   <li>幂等检查: claims 已占位 直接 return (启动期贪心扫 / 周期 rebalance 重复调用都会幂等跳过)</li>
         *   <li>占位: putIfAbsent 原子, 防同节点并发 tryClaim 同分区</li>
         *   <li>{@link Claim#start()} 注册 subscription 到 container, 进入 ManagedRunner.pending 队列</li>
         * </ol>
         * <p>
         * tryLock 由 runner 线程 initManaged → beforeStart 异步完成, 失败的 task 由 runner 每
         * {@code RETRY_INTERVAL_MS} 自动重试, 不需要 tryClaim 关心后续。
         *
         * @param partition 见上述说明
         */
        void tryClaim(int partition) {
            if (!running) return;
            if (claims.containsKey(partition)) return;
            String lockKey = lockKey(partition);
            Claim claim = new Claim(this, partition, lockKey);
            if (claims.putIfAbsent(partition, claim) != null) return;
            // liveClaims 在 start 之前加入: start 成功后 task 进入 ManagedRunner pending, 自此 live.
            // start 失败的回滚分支会同步从 liveClaims 移除, 不会泄漏.
            liveClaims.add(claim);
            try {
                claim.start();
            } catch (Throwable t) {
                // start (container register / executor) 失败必须移除占位, 否则该 claim 既没进 runner 又永久占住 claims,
                // 后续 rebalance 因 claims.containsKey 跳过 → 该分区在本节点永久僵死, shutdown latch 也会等它超时.
                claims.remove(partition, claim);
                liveClaims.remove(claim);
                // start 失败 → afterExit 永不执行. 若本 claim 已被并发 shutdown 纳入 latch 计数 (shutdownCounted 在
                // shutdown 的 !liveClaims.contains 补偿检查"之后"才被本 catch 的 liveClaims.remove 影响, 那条补偿漏判),
                // 这里用同一 shutdownCountedDown CAS 补一次 countDown (与 afterExit / shutdown 补偿互斥, exactly-once),
                // 否则 shutdown await 等到 timeout.
                CountDownLatch sl = shutdownLatch;
                if (sl != null && claim.shutdownCounted && claim.shutdownCountedDown.compareAndSet(false, true)) {
                    sl.countDown();
                }
                log.error("[{}] partition claim.start failed, removed placeholder streamPrefix={} partition={}",
                        redisProxy.getQualifier(), streamPrefix, partition, t);
            }
        }

        /**
         * 业务作用：周期性再平衡: 让本节点持有的分区数趋近 fair = count / aliveNodes。
         * <p>
         * 本入口跑在 {@link TimingWheel} 工作线程上，只合并并发信号并投递控制任务；心跳、释放与抢占
         * 在组件专用控制域执行，避免 Redis I/O 阻塞共享时间轮平台线程。
         * <p>
         * <b>alive 自治</b>: 每组独立维护 ZSET ({@code {streamPrefix}:nodes}), member=nodeId,
         * score=过期时间戳 (now + 3*period, 容忍 1-2 次心跳丢失). 本方法每次跑都 ZADD 自己续约 +
         * ZREMRANGEBYSCORE 清过期节点 + ZCARD 数活节点. 完全不依赖外部 cluster 开关.
         * 死节点 (kill -9 / 网络分区) 在 3*rebalancePeriodMs 内自动剔除.
         *
         * <p>参数说明: 无。
         * <p>
         * 返回: 无返回值；单轮 Redis 异常只记录并由后续周期重试。
         */
        void rebalance() {
            if (!running) return;
            if (!rebalancing.compareAndSet(false, true)) return;
            // 时间轮只合并并发信号；心跳和锁操作转交组件专用控制域，不能阻塞共享 TimingWheel 平台线程。
            if (!partitionRuntime.submitRedisControl(this::rebalanceNow)) rebalancing.set(false);
        }

        /**
         * 业务作用：在组件专用控制域执行一轮真实 Redis 心跳与分区份额收敛。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；单轮异常只影响本次收敛，周期时间轮信号仍会继续补偿。
         */
        private void rebalanceNow() {
            try {
                if (!running) return;
                String q = redisProxy.getQualifier();

                // === step 1: 心跳续约 + 估算存活节点 ===
                int alive = this.heartbeat();

                // 向上取整: 64 分区 3 节点 → fair=22, 允许部分节点多持 1 个, 避免余数分区成孤儿
                int fair = Math.max(1, (count + alive - 1) / alive);
                this.currentFair = fair;
                this.observeOwnerTarget(fair);
                // my 只统计真正持有锁的 claim, 不算 zombie (tryLock 未成功的)
                int my = realLockCount.get();

                // === step 2: 真锁持有过多 → 释放 ===
                // zombie (tryLock 未成功) 不清除, 由 ManagedRunner retry 自然重试;
                // 对方释放锁后 zombie 的 retry 会成功变真锁.
                //
                // 防覆盖: 上一波 release 尚未 afterExit 完成时跳过本轮 step2 (不调 pendingReleaseCount.set,
                // 不 markStop 新一批). wake channel 异步触发的 rebalance 不等周期, 必须显式守门,
                // 否则 pendingReleaseCount 被覆盖导致 wake pub 时机错乱.
                // step 3 (claim 不足时抢锁) 不受影响, 因为不读写 pendingReleaseCount.
                //
                // 卡死保险阀: 边界 race (rebalance step 与独立 lockLost 微秒级撞上) 可能让 pendingReleaseCount
                // 永远卡 > 0, 后续 release 被永久挡住. 若 elapsed > stuckThreshold 强制 reset 0 自愈.
                // threshold 取 drainTimeoutMs + rebalancePeriodMs/2（默认 5s+1.5s=6.5s），
                // 略大于 drainTimeoutMs 留出调度余量，并让后续周期能尽快解除释放门禁。
                int pending = pendingReleaseCount.get();
                if (my > fair && pending > 0) {
                    long releaseStarted = releaseStartNanos;
                    long elapsed = releaseStarted <= 0 ? 0
                            : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - releaseStarted);
                    long stuckThreshold = drainTimeoutMs + rebalancePeriodMs / 2;
                    if (releaseStarted > 0 && elapsed > stuckThreshold) {
                        log.warn("[{}] 再平衡 release 卡死保险阀触发: streamPrefix={} pendingReleaseCount={} elapsed={}ms threshold={}ms, 强制 reset",
                                q, streamPrefix, pending, elapsed, stuckThreshold);
                        // increment generation 让旧 batch 的 Claim 迟到 afterExit 不再 decrement 当前 count (跨 batch 污染防御).
                        releaseGeneration.incrementAndGet();
                        pendingReleaseCount.set(0);
                        // 落入下面的独立 if 走正常 release 路径 — 重置 pending 让条件成立
                        pending = 0;
                    } else {
                        log.debug("[{}] 再平衡跳过本轮释放: streamPrefix={} pendingReleaseCount={} elapsed={}ms",
                                q, streamPrefix, pending, elapsed);
                    }
                }
                if (my > fair && pending == 0) {
                    int excess = my - fair;
                    // 高编号先释放 (保留低编号正在处理消息), 只释放真锁 (lock != null), 跳过 zombie
                    List<Integer> owned = new ArrayList<>();
                    for (Map.Entry<Integer, Claim> e : claims.entrySet()) {
                        if (e.getValue().lock != null) owned.add(e.getKey());
                    }
                    owned.sort((p1, p2) -> Integer.compare(p2, p1));
                    int toRelease = Math.min(excess, owned.size());
                    // 先收集要释放的 claim (remove + 暂存), 统计实际数量后设倒计数, 最后才 markStop
                    // 时序: set(N) → markStop → runner 检测 → afterExit → decrement
                    // 必须 set 在 markStop 之前, 否则 afterExit 先跑会把初始 0 减成负数提前触发 pub
                    List<Claim> toStop = new ArrayList<>(toRelease);
                    for (int i = 0; i < toRelease; i++) {
                        Claim c = claims.remove(owned.get(i));
                        if (c != null) toStop.add(c);
                    }
                    int released = toStop.size();
                    // 记录 release 起点, 给卡死保险阀算 elapsed.
                    // 注意写入顺序: releaseStartNanos 在 pendingReleaseCount.set 之前,
                    // 否则后续 rebalance 可能读到 pendingReleaseCount>0 但 releaseStartNanos=0, elapsed 错乱.
                    releaseStartNanos = System.nanoTime();
                    // 新一轮 generation: 旧 batch 的 Claim 迟到 afterExit 不再 decrement 本 batch 计数 (防跨 batch 污染).
                    int gen = releaseGeneration.incrementAndGet();
                    pendingReleaseCount.set(released);
                    // 时序: releaseGen / releaseCounted 必须在 markStop 之前, 否则 afterExit 先跑会看 releaseCounted=false 不 decrement
                    for (Claim c : toStop) {
                        c.releaseGen = gen;
                        c.releaseCounted = true;
                    }
                    for (Claim c : toStop) c.markStop();
                    // 立即让 runner 观察 stop 门禁并开始排干，避免空闲分区继续等到下一次 pollTimeout。
                    container.wakeManagedRunners();
                    log.info("[{}] 再平衡释放: streamPrefix={} 释放 {} 个 (持有 {} → 目标 {}, 节点数 {})",
                            q, streamPrefix, released, my, fair, alive);
                }

                // === step 3: 持有不足 → 扫剩余分区抢锁 ===
                // break 条件用 realLockCount (真锁数), 不用 claims.size()
                // (zombie 占位不算真锁, 用 claims.size 会导致 zombie 过多时提前 break, 丢失分区)
                // containsKey 防同分区重复 claim (已有真锁或 zombie 的不重复尝试)
                int claimed = 0;
                boolean retrySignaled = false;
                if (my < fair) {
                    for (int i = 0; i < count && running; i++) {
                        if (realLockCount.get() >= fair) break;
                        Claim existing = claims.get(i);
                        if (existing == null) {
                            this.tryClaim(i);
                            claimed++;
                        } else if (existing.lock == null && existing.requestImmediateRetry()) {
                            retrySignaled = true;
                        }
                    }
                    if (claimed > 0) {
                        log.info("[{}] 再平衡抢占: streamPrefix={} 发起 {} 个 claim (持有 {} → 目标 {}, 节点数 {})",
                                q, streamPrefix, claimed, my, fair, alive);
                    }
                }
                // 释放事件已经证明集群状态发生变化，唤醒失败占位立即重做锁校验；
                // 若锁尚未真正可用，原有固定退避仍会继续限制后续请求频率。
                if (retrySignaled) container.wakeManagedRunners();
                // === step 4: wake 通知由 afterExit 在锁真正释放后发出, 不在这里提前发 ===
                // (markStop 到 unlock 之间有延迟, 提前通知会导致对方 tryLock 失败白跑)

                // PEL 全组采样在专用控制域执行并限频，Prometheus scrape 线程只读取已完成快照。
                this.samplePelIfDue();

                // (分区锁定/释放的逐条日志已在 beforeStart/afterExit 中打印, 这里不再重复摘要)

            } catch (Exception e) {
                log.error("[{}] partition rebalance failed group={} streamPrefix={}",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName, streamPrefix, e);
            } finally {
                rebalancing.set(false);
            }
        }

        /**
         * 业务作用：关闭当前物理分区组的成员登记、唤醒订阅和 Claim，按一次共享预算等待锁生命周期退出。
         * <p>
         * 等待集合包含已因再平衡脱离 claims、但仍在排干的 liveClaims；每个 Claim 只计一次退出，
         * 避免并发退出使等待过早完成。等待预算为 drainTimeoutMs 加各 Claim 收尾余量，
         * 超时或中断后仍发送下线通知，未释放的锁继续按实际持权状态限制接管。
         * 该方法返回不表示 Task、确认或恢复执行域全部终止；外层运行时继续保留并等待这些责任。
         *
         * <p>参数说明: 无。
         * <p>返回: 完成本轮组关闭与有界等待；未结束的本地工作由所属运行时继续收口，锁租期并非固定值。
         */
        void shutdown() {
            if (rebalanceTimerName != null) TimingWheel.cancel(rebalanceTimerName);
            // step 0a: 优雅退出 alive 注册表, 让其它节点立即看到我下线 (不必等 3*period TTL 过期)
            try {
                redisProxy.evalDirectConnection(
                        PARTITION_UNREGISTER_LUA,
                        Long.class,
                        new String[]{streamPrefix + ":nodes"},
                        nodeId);
            } catch (Exception ignored) {
            }
            // step 0b: 取消 wake-up 订阅
            try {
                redisProxy.unsubscribe(wakeChannel);
            } catch (Exception ignored) {
            }
            // step 1: 快照 live task 集合 (与 claims map 解耦): 用 liveClaims 是为了把已脱离 claims 但仍 live
            // 的 rebalance release task 也纳入 latch, 否则 shutdown 可能提前 await 完成而旧 task 仍在 drain.
            List<Claim> liveSnapshot = new ArrayList<>(liveClaims);
            int n = liveSnapshot.size();
            log.info("[{}] 分区组停机: streamPrefix={} 持有 live task={} 开始释放", redisProxy.getQualifier(), streamPrefix, n);
            this.shutdownLatch = new CountDownLatch(n);
            // step 2: 给本批 live task 设 shutdownCounted=true (在 markStop 之前), 之后 afterExit finally 看此标志才 countDown.
            // 防止 shutdown 启动后 race 触发的新 task 误 countDown 让 latch 提前归零.
            for (Claim c : liveSnapshot) c.shutdownCounted = true;
            // step 2.5: 主动补救 ns 级 race window.
            // 如果某 task 在 snapshot 之后 / setShutdownCounted 设到自己之前 已经走完 afterExit finally
            // (此时它读 shutdownCounted=false 没 countDown), 这里查 liveClaims 看它是否已 remove,
            // 已 remove 则主动用同一 CAS 补 countDown — exactly-once 保证不会与 afterExit 自己 countDown 重复.
            // 这样 race 命中也能精确 await, 不依赖 timeout 兜底.
            for (Claim c : liveSnapshot) {
                if (!liveClaims.contains(c) && c.shutdownCountedDown.compareAndSet(false, true)) {
                    shutdownLatch.countDown();
                }
            }
            // step 3: 并行 markStop 所有 live task → runner 主循环进入 drain (持锁等在途业务跑完/超时) → exitManaged → afterExit countDown
            for (Claim c : liveSnapshot) c.markStop();
            // step 4: 触发 runner.running=false, runner finally 并行跑 exitManaged
            try {
                container.stop();
            } catch (Exception e) {
                log.warn("[{}] partition container stop failed group={} streamPrefix={}",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName, streamPrefix, e);
            }
            // 共享等待预算覆盖 Claim 排干与逐个收尾余量；超时不证明锁已释放，也不撤销运行时对迟到任务的责任。
            if (n > 0) {
                long awaitMs = drainTimeoutMs + Math.max(0, n - 1) * 50L + 1_000L;
                try {
                    if (!shutdownLatch.await(awaitMs, TimeUnit.MILLISECONDS)) {
                        drainTimeouts.incrementAndGet();
                        log.warn("[{}] partition shutdown timed out group={} streamPrefix={} total={} remaining={} awaitMs={}, fallback to lease expire",
                                redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName,
                                streamPrefix, n, shutdownLatch.getCount(), awaitMs);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            claims.clear();
            liveClaims.clear();
            shutdownLatch = null;
            // 有界等待后通知其它节点重新计算份额；未结束的 Claim 仍可能持锁，接收者必须重新竞争真实锁权威。
            try {
                redisProxy.pub(wakeChannel, "offline:" + nodeId);
            } catch (Exception ignored) {
            }
            log.info("[{}] 分区组停机完成: streamPrefix={} 已释放全部分区, 通知其他节点接管", redisProxy.getQualifier(), streamPrefix);
        }
    }

    // ==================== Claim ====================

    /**
     * 一个被本节点持有的分区。
     * <p>
     * 同时实现 {@link io.github.nasaruntime.redis.cache.redis.stream.PollLifecycle} (锁的 tryLock/holds/unlock) 和 {@link io.github.nasaruntime.redis.cache.redis.stream.BatchStreamListener}
     * (消息 dispatch)。tryLock/holds/unlock 全部在 ManagedRunner 线程内调用 (initManaged →
     * beforeStart, checkAlive → beforePoll, exitManaged → afterExit), threadId 一致。
     */
    @SuppressWarnings("rawtypes")
    final class Claim implements PollLifecycle,
            BatchStreamListener<String, MapRecord<String, Object, Object>>,
            StreamPartitionRuntime.PartitionSource {

        final PartitionGroup group;
        final int partition;
        final String lockKey;
        final String stream;
        final byte[] groupBytes;
        final byte[] consumerBytes;
        final io.lettuce.core.Consumer<byte[]> autoclaimConsumer;
        final StreamSourceAuthority sourceAuthority = new StreamSourceAuthority();
        final PartitionSourceRecordState recordState;
        final StreamPendingRecovery pendingRecovery;
        /**
         * 只控制 holder 的本地发布与停止状态先后关系，Redis 网络往返不得占用该锁。
         */
        final ReentrantLock lifecycleLock = new ReentrantLock();

        /**
         * 本 task 线程持有的锁实例. beforeStart 内 tryLock 后赋值.
         * <p>
         * volatile: rebalance (TimingWheel 线程) 通过它判断 zombie vs 真持有, 与 beforeStart (runner 线程) 跨线程, 必须保可见.
         */
        volatile Lock lock;
        /**
         * 锁丢失标记. afterExit 看到 true 就跳过 unlock.
         * volatile 同上, rebalance 跨线程读.
         */
        volatile boolean lockLost;
        /**
         * runner 线程 tryLock 成功后写入的 holder 标识 (含 runnerThreadId)。确认路径冻结该值并交给
         * holder-fenced Lua 原子复验；XAUTOCLAIM 的业务执行前检查也用它区分失权与 Redis 异常。
         * <p>
         * volatile: runner 写入，专用等待执行域与确认控制任务跨线程读取，必须保持可见。
         */
        volatile String lockHolder;
        /**
         * 上次 holds() 自检的单调时钟快照。与 group.holdsCheckIntervalMs 配合做时间限流,
         * 防止 NOBLOCK 模式下空轮询每秒打几千次 holds() EVAL。
         */
        long lastHoldsCheckNanos;
        /**
         * rebalance/shutdown 设 false → runner 主循环 {@link #isStopRequested} 命中 → 进入 drain
         * (持锁等在途业务跑完/超时) → 移除 task。锁丢失另走立即 exit, 不经 drain。
         */
        volatile boolean active = true;
        /**
         * route 事务未能完整分类时只暂停新读取，当前 holder 继续承担整批 PEL 恢复责任。
         */
        volatile boolean routeBlocked;
        /**
         * markStop() 的单调时钟快照, 给 drainTimedOut 用. volatile 防 32-bit JVM 撕裂 + 防后续重构把 markStop 内
         * "先写 stopNanos 再写 active" 的顺序搞反.
         */
        volatile long stopNanos;

        /**
         * 异步 recoverPending 期间 = true, runner 通过 {@link #isPollReady} 看到后跳过 batch poll,
         * 但仍调 checkAlive 维持 holds 自检与 markStop 响应。完成时由 doRecoverPendingAsync 的 finally 设回 false。
         * <p>
         * 批次边界保证: 不进 batch list → 没有新 XREADGROUP 批次进入 dispatcher；recoverPending 在
         * 专用虚拟等待执行域完成 Task/Future 与确认决策后才重新开放新读取。独立本地执行槽仍可按 Partition
         * 规则并行，不把物理 transport batch 变成单线程业务事务。
         */
        volatile boolean recovering = false;
        /**
         * 首次取得 Claim 后的恢复轮用于记录 takeover 收敛耗时，后续补扫不覆盖该次观测。
         */
        volatile boolean initialRecovery = true;
        /**
         * 标记本 Claim 是否被本轮 rebalance 计入 release 范围. rebalance step 2 markStop 前设 true,
         * 让 afterExit 知道本次退出必须 decrement {@link PartitionGroup#pendingReleaseCount}.
         * <p>
         * 不被本轮 release 的 task (普通 lockLost / cancel 路径) 不 decrement, 避免把 0 减成负数发出错误 wake.
         * 被本轮 release 但 drain 期间锁真丢 (lockLost) 的 task 也必须 decrement, 否则 release 计数卡住,
         * 后续 rebalance 永远被 pendingReleaseCount.get() &gt; 0 挡住.
         */
        volatile boolean releaseCounted = false;
        /**
         * 本 Claim 被计入的 release batch generation (与 {@link PartitionGroup#releaseGeneration} 配对).
         * afterExit decrement 时仅当 releaseGen 与当前 generation 一致才计数, 防止跨 batch 污染:
         * 保险阀 reset 后旧 Claim 迟到 afterExit 不能减掉新 batch 的计数.
         */
        volatile int releaseGen = 0;
        /**
         * 标记本 Claim 还有 pending 未 recover, beforePoll 内择机重新 submit. 避免旧 PEL 卡到下次 rebalance/重启.
         * <p>
         * 字段默认 <b>false</b>: 首次 recover 由 {@link #beforeStart} 调用 {@link #submitRecoverPending}
         * 无条件触发 (不依赖此标志). 标志只在"需要后续 retry"的场景下置 true:
         * <ul>
         *   <li>submitRecoverPending 内 executor 抛任何 Throwable</li>
         *   <li>recoverPending 暂时无法取得 raw record 容量</li>
         *   <li>recoverPending 遇到未归类的命令异常而结束本轮</li>
         *   <li>recoverPending 内 maxLoops 耗尽但 cursor 未到 "0-0"</li>
         * </ul>
         * 这些路径在 finally 兜底设 true (只在 active &amp;&amp; running &amp;&amp; !lockLost 时), 主循环 beforePoll 检测后 retry.
         * 已识别的 Redis 瞬时异常和 holder UNKNOWN 在原恢复任务内退避，不丢弃本轮游标或已知页面。
         * 本次游标正常结束后还会按外部 consumer PEL 的真实 idle 登记定时补扫；主消费 XREADGROUP &gt;
         * 不读其它 consumer 的 PEL, 必须由这两类重试完成接管.
         */
        volatile boolean recoverPendingNeeded = false;
        /**
         * 外部 consumer 的 PEL 尚未达到 minIdle 时，从该单调时钟快照起等待精确剩余时长。
         */
        volatile long recoverPendingFollowUpStartNanos;
        /**
         * 与 recoverPendingFollowUpStartNanos 配对的等待时长。
         */
        volatile long recoverPendingFollowUpDelayNanos;
        /**
         * 标记本 Claim 是否被本次 shutdown 纳入 latch 计数. shutdown 内对 liveClaims snapshot 设 true 后才 markStop.
         * afterExit finally 内仅当 shutdownCounted=true 时 countDown, 防止已脱离 latch 范围的 task (例如
         * rebalance release task 与 shutdown 并发) 误 countDown 让 latch 提前归零.
         */
        volatile boolean shutdownCounted = false;
        /**
         * exactly-once countDown 守门. shutdown 内 snapshot 与 task afterExit finally 的 ns 级 race 中,
         * 同一 task 可能被 afterExit 自己 countDown, 也可能被 shutdown 主动补 countDown — CAS 保证只成功一次.
         * 单调一次性: 设置 true 后不再 reset (Claim 生命周期内 shutdown 只发生一次).
         */
        final AtomicBoolean shutdownCountedDown = new AtomicBoolean(false);
        /**
         * 资源释放事件对应的一次性重试信号。多个并发事件合并为一个状态位，避免形成无界任务队列。
         */
        final AtomicBoolean immediateRetryRequested = new AtomicBoolean(false);

        /**
         * 业务作用：描述本节点对一个分区的占用：所属组、分区号与其锁键。
         * 占用以分布式锁表达，因此<b>同一分区在任一时刻只会被一个节点消费</b>，这是分区内保序的前提。
         *
         * @param group     所属分区组
         * @param partition 分区号
         * @param lockKey   该分区的占用锁键
         *                  返回: 尚未取得 holder 的 Claim，包含受同一容量与权威约束的恢复上下文。
         */
        Claim(PartitionGroup group, int partition, String lockKey) {
            this.group = group;
            this.partition = partition;
            this.lockKey = lockKey;
            this.stream = group.streamKey(partition);
            this.groupBytes = redisProxy.getKeySerializer().serialize(group.streamPrefix);
            this.consumerBytes = redisProxy.getKeySerializer().serialize(nodeId);
            this.autoclaimConsumer = io.lettuce.core.Consumer.from(groupBytes, consumerBytes);
            this.recordState = partitionRuntime.newRecordState(this, () -> {
            });
            this.pendingRecovery = new StreamPendingRecovery(redisProxy, partitionRuntime, this, recordState, group.batchSize);
        }

        /**
         * 业务作用：注册 task 到 container。ManagedRunner 模式下不阻塞 — task 进入 runner 的 pending 队列,
         * runner 线程异步调 initManaged (tryLock), 成功与否由 managedSuccess 标识跟踪。
         */
        void start() {
            StreamMessageListenerContainer.ConsumerStreamReadRequest<String> req =
                    StreamMessageListenerContainer.StreamReadRequest
                            .builder(StreamOffset.create(stream, ReadOffset.lastConsumed()))
                            // 全限定: 与新 import 的 Consumer 同名冲突, 不得不用全限定 (与 RedisProxy 同处理).
                            .consumer(org.springframework.data.redis.connection.stream.Consumer.from(group.streamPrefix, nodeId))
                            .autoAcknowledge(false)
                            .cancelOnError(t -> false)
                            .build();
            group.container.register(req, this, this);
        }

        /**
         * 业务作用：标记停止. 非阻塞 — 不立即解锁, 而是进入 <b>drain</b>: runner 主循环见 active=false 后,
         * 仍持锁等在途 batch 业务跑完 (或 {@link #drainTimedOut} 超时) 才 exitManaged + 从 tasks
         * 列表移除 → afterExit 解锁。期间锁保持持有, 避免在途消息被其它节点重复消费。
         * 锁真丢失 (holdsStatus==0 → lockLost=true) 时跳过 drain 走立即 exit。
         * <p>
         * 写入顺序: 先 stopNanos (drainTimedOut 读取依赖) 再 active=false (drain 入口标志).
         * active 是 volatile, 根据 JMM happens-before, 后续 reader 读到 active=false 之后再读 stopNanos
         * 一定能看到这里写入的值. 同时 stopNanos 自身也是 volatile, 防 32-bit JVM 撕裂.
         *
         * <p>参数说明: 无。
         * <p>
         * 返回: 无返回值；后续轮询只能排干或退出，不能再拉取新消息。
         */
        void markStop() {
            lifecycleLock.lock();
            try {
                stopNanos = System.nanoTime();
                // 先关闭 poll/submit admission；共享执行权威保留到 drain 完成，使已受理 Task 仍可完成 fenced ACK。
                active = false;
            } finally {
                lifecycleLock.unlock();
            }
            // recovery admission 与 retry 注册共用线性化门禁，停止返回后不会再留下尚未开始的恢复责任。
            partitionRuntime.sourceStopping(sourceAuthority);
            // submit 发布与取消共用运行时线性化边界，返回后本来源没有尚未发布句柄的提交。
            partitionRuntime.cancelPendingSubmissions(sourceAuthority);
        }

        /**
         * 业务作用：为尚未取得分区锁的占位登记一次立即重试请求。
         * 请求只会唤醒权威锁校验，不会直接改变持有状态；停止中的 Claim 拒绝新请求。
         *
         * <p>参数说明: 无。
         *
         * @return 本次是否把信号从无切换为有。
         */
        boolean requestImmediateRetry() {
            return active && lock == null && immediateRetryRequested.compareAndSet(false, true);
        }

        /**
         * 业务作用：由 ManagedRunner 原子消费立即重试请求，确保一个释放事件至多触发一次额外锁校验。
         *
         * <p>参数说明: 无。
         *
         * @return 存在未消费请求时返回 true。
         */
        @Override
        public boolean consumeImmediateRetry() {
            return immediateRetryRequested.getAndSet(false);
        }

        /**
         * 业务作用：task 是否已就绪可以加入下一轮 batch poll. 三层守门:
         * <ol>
         *   <li>{@code !recovering}: 异步 recoverPending 进行中 → false, 防 PEL dispatch 与新消息 dispatch 并发</li>
         *   <li>{@code !lockLost}: recoverPending 已设 lockLost=true 后, recovering 在 finally 已被清 false,
         *       如果不在此 short-circuit, 主循环会进 complete-isPollReady ready 分支, checkAlive 可能因 holds 限流
         *       窗口(默认 5s)返回 true, task 加入 batch poll 读新消息 — 锁已被新 owner 抢, 重复消费</li>
         *   <li>{@code (!recoverPendingNeeded || !active || !running)}: recover 失败 / rejected 后保留 retry 标志,
         *       正常运行时 (active &amp;&amp; running) 强制走 in-flight 分支触发 beforePoll 重新 submitRecoverPending,
         *       但<b>不让本轮 task 加入 batch poll</b> (防 retry 的 recoverPending 与 batch poll 并发).
         *       drain / shutdown 时 (!active || !running) 不让此标志阻挠 drain 收尾 — drain 状态机只看 recovering 完成.</li>
         * </ol>
         */
        @Override
        public boolean isPollReady() {
            if (recovering) return false;
            if (lockLost) return false;
            if (routeBlocked) return false;
            if (recoverPendingNeeded && active && running) return false;
            return sourceAuthority.isActive();
        }

        /**
         * 业务作用：停止阶段等待当前 Claim 的 PEL 恢复、Task、重试、路由恢复与确认责任，全部收敛前保持 holder。参数说明: 无。返回: 本来源全部责任结束时为 true。
         */
        @Override
        public boolean isDrainComplete() {
            return !recovering && partitionRuntime.sourceDrained(sourceAuthority);
        }

        /**
         * 业务作用：在 XREADGROUP 前取得本来源 batch-size 对应的公平 raw record 容量。
         *
         * <p>参数说明: 无。
         *
         * @return 已持有本轮读取容量时返回 true
         */
        @Override
        public boolean tryAcquirePollPermit() {
            return recordState.tryAcquire(group.batchSize);
        }

        /**
         * 业务作用：把 Redis 实际返回数量交接给活动 batch，空结果立即归还全部预留。
         *
         * @param recordCount 当前来源本轮返回数量
         *                    返回: 无返回值；缺少读取前预留时拒绝继续消费。
         */
        @Override
        public void onPollResult(int recordCount) {
            recordState.onPollResult(recordCount);
        }

        /**
         * 业务作用：在 Task outcome 与 ACK/PEL 决策完成后归还活动 batch 的 raw record 容量。参数说明: 无。返回: 无返回值。
         */
        @Override
        public void afterBatchComplete() {
            recordState.afterBatchComplete();
        }

        /**
         * 业务作用：读取或派发异常时归还尚未交接的 raw record 容量。参数说明: 无。返回: 无返回值。
         */
        @Override
        public void afterPollFailure() {
            recordState.afterPollFailure();
        }

        // ==================== PollLifecycle ====================

        /**
         * 业务作用：钩子 1/3: ManagedRunner 的 initManaged 调用 (在 runner 线程内)。
         * <p>
         * 流程:
         * <ol>
         *   <li>tryLock 拿锁. 失败 → 返回 false, runner 记录单调时钟并按 RETRY_INTERVAL_MS 重试</li>
         *   <li>拿到锁 → 设 {@link #recovering}=true 让 {@link #isPollReady} 返回 false, runner 暂不
         *       发起 batch poll。把 recoverPending 异步提交到专用虚拟等待执行域，否则崩溃恢复期
         *       PEL 几千条 XAUTOCLAIM 多页拉取会饿死 runner 内其他 partition (动辄几秒甚至几十秒不调度)</li>
         *   <li>recoverPending 完成时 finally 设 recovering=false, runner 下次发现 isPollReady=true
         *       自然恢复 batch poll</li>
         * </ol>
         * <p>
         * <b>批次边界保证</b>: recovering=true 期间 runner 不进 batch list，没有新 XREADGROUP 批次与
         * pending 恢复批次重叠；消息进入同一 dispatcher 后仍按本地 Partition key 规则决定串行与并行。
         *
         * <p>参数说明: 无。
         *
         * @return 取得分区锁并提交 pending 恢复时为 true；公平门禁、锁竞争或初始化失败时为 false。
         */
        @Override
        public boolean beforeStart() {
            // Redis 往返不占用生命周期锁，停止线程可以立即发布 active=false 并开始排干计时。
            if (!running || !active) return false;
            if (group.realLockCount.get() >= group.currentFair) return false;
            Lock candidate = distributedLock.getLock(lockKey);
            boolean acquired;
            try {
                acquired = candidate.tryLock();
            } catch (Throwable t) {
                log.error("[{}] partition tryLock threw streamPrefix={} partition={}",
                        redisProxy.getQualifier(), group.streamPrefix, partition, t);
                return false;
            }
            if (!acquired) return false;

            boolean published = false;
            lifecycleLock.lock();
            try {
                if (!running || !active) {
                    return false;
                }

                this.lock = candidate;
                // 在取得控制权的 runner 线程冻结 holder，后续确认 Lua 只能使用这一代权威。
                // 必须在 runner 线程内执行 (initManaged 已保证), 否则 holder 含的 threadId 错位.
                this.lockHolder = LettuceDistributedLock.currentHolder();
                // holder 写入完成后才发布新 generation，Task 快照不会观察到缺少 Redis 身份的半代次。
                this.sourceAuthority.activate(this.lockHolder);
                this.recoverPendingFollowUpStartNanos = 0;
                this.recoverPendingFollowUpDelayNanos = 0;
                int locked = group.realLockCount.incrementAndGet();
                group.claimGeneration.incrementAndGet();
                group.observeOwnerCount();
                log.info("[{}] 分区锁定: streamPrefix={} partition={} 持有={}/{}", redisProxy.getQualifier(), group.streamPrefix, partition, locked, group.count);
                // 异步 recoverPending: 不阻塞 runner 线程, 期间 runner 用 checkAlive 维持 holds 自检。
                // recovering=true 期间 isPollReady 返回 false, runner 跳过 batch poll 但保 holds 检查;
                // recovering=false 后 runner 自然恢复 batch poll。
                this.submitRecoverPending();
                published = true;
                return true;
            } catch (Throwable t) {
                log.error("[{}] partition beforeStart failed streamPrefix={} partition={}",
                        redisProxy.getQualifier(), group.streamPrefix, partition, t);
                return false;
            } finally {
                lifecycleLock.unlock();
                if (!published) {
                    // 停止先完成线性化时，对称释放尚未发布的 Redis holder；本地状态不会暴露这次短暂取得。
                    try {
                        candidate.unlock();
                    } catch (Throwable releaseFailure) {
                        log.warn("[{}] partition unpublished holder release failed streamPrefix={} partition={}",
                                redisProxy.getQualifier(), group.streamPrefix, partition, releaseFailure);
                    }
                }
            }
        }

        /**
         * 业务作用：钩子 2/3: ManagedRunner 在以下时机调用 (全部通过 BatchStreamPollTask.checkAlive 转入):
         * <ul>
         *   <li>就绪 task 进 batch 之前 — 防止用已丢锁的 consumer 发 xReadGroup</li>
         *   <li>业务 in-flight ({@code !complete}) 或异步初始化中 ({@code !isPollReady}) 期间 —
         *       周期性存活自检, 不实际 poll</li>
         * </ul>
         * 两条路径都需要 markStop 检测 + holds 自检, 共用本方法。
         * <p>
         * <b>时间限流</b>: NOBLOCK 批量模式下空 stream 每个 cycle 都可能进 beforePoll,
         * 用 lastHoldsCheckNanos + holdsCheckIntervalMs (默认 5s) 限流真实 holds() EVAL,
         * 防止打爆 redis pipeline。lease=30s, 5s 间隔留 6 次重试机会, 足够安全。
         *
         * <p>参数说明: 无。
         *
         * @return 见上述说明。
         */
        @Override
        public boolean beforePoll() {
            // 外部 markStop. 不读 running — 全局 shutdown 会调用 g.shutdown → markStop 所有 claim,
            // 走 active=false 路径与本地 stop 统一. 不能因 !running 提前短路, 否则 drain 路径下 task
            // 会被 checkAlive=false 立即移除绕过 drain.
            if (!active) return false;
            // 锁已丢失 (可能由 recoverPending 内 fence 或 beforePoll 上一次 holds 设置) — 立即 short-circuit,
            // 防 holds 限流窗口 (默认 5s) 内 beforePoll 仍返回 true 让 task 加入 batch poll 读新消息.
            // 主循环看到 checkAlive=false 后再判 isLockLost=true 走 removeAndExit 立即 exit.
            if (lockLost) return false;
            if (!partitionRuntime.partitionSourceHealthy(group.listenersByTopicEvent.values())) {
                // Runner 健康是读取新消息的前置权威；先关 admission 并排干当前来源，再释放 holder 给后续健康代次。
                pause(new IllegalStateException("PartitionRunner unhealthy for stream group " + group.streamPrefix));
                return false;
            }
            // tryLock 从未成功 (lock == null): 跳过 holds 检查, 让 retry 路径重试 tryLock
            if (lock == null) return true;
            // recover retry: rejected / 网络异常导致 recoverPending 未完整跑完时, 在 holds 自检之前重试 submit.
            // 不能让 PEL 卡到下次 rebalance/重启 — 主消费 XREADGROUP > 不读旧 consumer 的 PEL.
            // 只在 recovering=false 时 retry (recovering=true 表示上次 submit 还在跑).
            long followUpStart = recoverPendingFollowUpStartNanos;
            boolean followUpDue = followUpStart != 0
                    && System.nanoTime() - followUpStart >= recoverPendingFollowUpDelayNanos;
            if ((recoverPendingNeeded || followUpDue) && !recovering) {
                if (followUpDue) {
                    recoverPendingFollowUpStartNanos = 0;
                    recoverPendingFollowUpDelayNanos = 0;
                }
                this.submitRecoverPending();
            }
            // 时间限流: 距上次 holds 不到 holdsCheckIntervalMs → 跳过本次, 与正常 poll 同进度
            long nowNanos = System.nanoTime();
            long intervalNanos = TimeUnit.MILLISECONDS.toNanos(group.holdsCheckIntervalMs);
            if (lastHoldsCheckNanos != 0 && nowNanos - lastHoldsCheckNanos < intervalNanos) return true;
            lastHoldsCheckNanos = nowNanos;
            // 三态: null = Redis 瞬时异常, 不当锁丢 (否则一次抖动就误判丢锁停消费), 当作仍持有, 下个周期重判; 1 = 持有
            group.lockSelfChecks.incrementAndGet();
            Long holdsSt = distributedLock.holdsStatus(lockKey, lockHolder);
            if (holdsSt == null || holdsSt == 1L) return true;
            // holdsSt == 0: 锁真丢. 区分: 锁被其他节点正常接管 (rebalance) vs 锁真的过期丢失 (看门狗故障)
            // TTL 接近满值表示其它节点刚获得租约，属于正常 rebalance，只记录低级别诊断日志。
            // TTL 快到期 / key 不存在 → 看门狗没续上, warn 级别
            try {
                long ttl = redisProxy.pttl(distributedLock.redisKey(lockKey));
                // ttl > 10s 或 key 不存在(-2) 或 无过期(-1) → 别人持有或已释放, 正常 rebalance
                // ttl 在 [0, 10s] → 锁快过期了, 看门狗可能没续上, warn
                if (ttl > 10000 || ttl < 0) {
                    log.debug("[{}] partition rebalance: lock taken by another node streamPrefix={} partition={} redisTTL={}",
                            redisProxy.getQualifier(), group.streamPrefix, partition, ttl);
                } else {
                    log.warn("[{}] partition lock expired streamPrefix={} partition={} holder={} redisTTL={}, stop consuming",
                            redisProxy.getQualifier(), group.streamPrefix, partition,
                            LettuceDistributedLock.currentHolder(), ttl);
                }
            } catch (Exception ignored) {
                log.warn("[{}] partition lock lost streamPrefix={} partition={}, stop consuming",
                        redisProxy.getQualifier(), group.streamPrefix, partition);
            }
            lockLost = true;
            sourceAuthority.loseAuthority();
            return false;
        }

        /**
         * 业务作用：钩子 3/3: ManagedRunner 退出时调 exitManaged → afterExit。
         * <p>
         * 三件事 (try-finally 保证 latch.countDown 一定走):
         * <ul>
         *   <li>从 claims 移除自己 (rebalance/shutdown 通常已经移过, putIfAbsent 比较移除幂等兜底)</li>
         *   <li>unlock (除非锁已丢失)，完整释放后结束本轮稳定 Lock 所有权</li>
         *   <li>group.shutdownLatch 非 null 时 countDown — 给 PartitionGroup.shutdown 的封顶 await
         *       发信号. rebalance 释放路径 latch=null, 跳过</li>
         * </ul>
         *
         * @param normal 见上述说明
         *               返回: 无返回值；所有退出分支都会完成本地引用、释放计数与停机闩锁收口。
         */
        @Override
        public void afterExit(boolean normal) {
            try {
                group.claims.remove(partition, this);
                // 从未持锁 → 没什么好释放的, 直接走 finally countDown
                if (lock == null) return;
                int remaining = group.realLockCount.decrementAndGet();
                group.observeOwnerCount();
                // 锁已丢失 → 跳 unlock (锁不归我, Redis 端发 UNLOCK_LUA 也没意义)，但本地续租权威仍要撤销。
                // release 计数收尾交给 finally 统一处理, 不在这里 early return 跳过.
                if (lockLost) {
                    this.disposeLocalLock();
                    return;
                }
                try {
                    lock.unlock();
                    log.info("[{}] 分区释放: streamPrefix={} partition={} 持有={}/{}", redisProxy.getQualifier(), group.streamPrefix, partition, remaining, group.count);
                } catch (LettuceDistributedLock.OwnershipLostException lost) {
                    // 本地 holder 仍是取得锁的 runner，但 Redis 已不再承认这次所有权。
                    // RedisLock 在抛出该异常前已经停止 watchdog 并结束本地所有权，不能再次改变同一状态。
                    // 这是租期或接管语义，不归因于业务代码跨线程调用，消息留在 PEL 由当前 owner 接续。
                    log.warn("[{}] 分区释放时所有权已丢失 streamPrefix={} partition={}，跳过 Redis 解锁并等待当前 owner 接管",
                            redisProxy.getQualifier(), group.streamPrefix, partition);
                } catch (IllegalMonitorStateException wrongThread) {
                    // beforeStart 与 afterExit 按约束应在同一 runner 线程；走到这里表示本地线程身份不变量被破坏。
                    log.error("[{}] 分区释放线程与加锁线程不一致 streamPrefix={} partition={} expectedHolder={} actualHolder={}",
                            redisProxy.getQualifier(), group.streamPrefix, partition,
                            lockHolder, LettuceDistributedLock.currentHolder());
                    this.disposeLocalLock();
                } catch (Exception e) {
                    // owner 的 unlock 遇到 Redis 网络异常时，RedisLock 已停止 watchdog 并结束本地所有权；
                    // Redis 端所有权结局未知，依靠 leaseTime 自然过期。
                    log.error("[{}] 分区 unlock 失败 streamPrefix={} partition={}", redisProxy.getQualifier(), group.streamPrefix, partition, e);
                }
            } finally {
                // 先让所有迟到 Task 的本地权威复验失败，再释放 holder 引用与远端锁生命周期。
                sourceAuthority.loseAuthority();
                partitionRuntime.authorityLost(sourceAuthority);
                recordState.close();
                // 同时清除 holder 与 Lock 引用；迟到 Task 已先观察到失效 generation，确认路径也不能再取得旧 holder。
                this.lockHolder = null;
                this.lock = null;
                // 2) liveClaims 维护: 不论何种退出路径都要从 live 集合移除, 让 shutdown live 计数准确.
                group.liveClaims.remove(this);
                // 3) shutdown latch countDown: 仅当本 Claim 被本次 shutdown 纳入计数 (shutdownCounted=true) 时才递减.
                //    用 shutdownCountedDown CAS 保证 exactly-once — shutdown 内主动补 countDown 路径与本路径互斥,
                //    避免同一 task 被双 countDown 让 latch 提前归零.
                CountDownLatch l = group.shutdownLatch;
                if (l != null && shutdownCounted && shutdownCountedDown.compareAndSet(false, true)) {
                    l.countDown();
                }
                // 4) release 计数 exactly-once 收尾: rebalance release 范围内的 claim 不论 lockLost / 正常 unlock /
                //    unlock 抛异常都必须递减 pendingReleaseCount, 否则计数卡住后续 rebalance 被永久挡住.
                //    非 release 路径 (普通 lockLost / cancel) releaseCounted=false 不递减.
                //    跨 batch 防御: 仅当 releaseGen 与当前 generation 一致才 decrement (保险阀 reset 会 increment generation
                //    让旧 batch 失效, 旧 Claim 迟到 afterExit 不能减掉新 batch 计数).
                if (releaseCounted && l == null && running && releaseGen == group.releaseGeneration.get()) {
                    if (group.pendingReleaseCount.decrementAndGet() == 0) {
                        try {
                            redisProxy.pub(group.wakeChannel, "release:" + nodeId);
                        } catch (Exception e) {
                            log.error("[{}] 分区释放 wake pub 失败 channel={} streamPrefix={} partition={}",
                                    redisProxy.getQualifier(), group.wakeChannel, group.streamPrefix, partition, e);
                        }
                    }
                }
            }
        }

        // ==================== drain-aware hooks ====================

        /**
         * 业务作用：主动 stop 标志. markStop 写 active=false, 这里取反给 ManagedRunner drain 分流用.
         *
         * @return 见上述说明。
         */
        @Override
        public boolean isStopRequested() {
            return !active;
        }

        /**
         * 业务作用：锁真丢失标志 (watchdog 失败 / 被新 owner 抢走). beforePoll 检测到 holds 失败时设置.
         *
         * @return 见上述说明。
         */
        @Override
        public boolean isLockLost() {
            return lockLost;
        }

        /**
         * 业务作用：drain 超时判定. 4 个 short-circuit + 时间封顶:
         * <ol>
         *   <li>active=true → 未 markStop, 不算 drain timeout</li>
         *   <li>lock==null → zombie, 不走 drain 路径</li>
         *   <li>lockLost=true → 走立即 exit 路径, 不算 drain timeout</li>
         *   <li>stopNanos &lt;= 0 → markStop 写入顺序异常, 防御性 return false</li>
         * </ol>
         * 最终: 当前时间距 markStop 已超 {@code group.drainTimeoutMs} 即视为超时, 调用方走强制 exit.
         * <p>
         * <b>注意</b>: "drain 已完成"({@code task.complete=true &amp;&amp; !recovering}) 的短路判定属于
         * BatchStreamPollTask + Claim 跨字段, 由 ManagedRunner 主循环的 {@code !task.complete || !task.isPollReady()}
         * 分支天然守门 — drain 完成时根本不会进这个分支调本方法, 本方法内不重复 guard.
         *
         * @param now runner 的本轮毫秒时间戳；本实现使用单调时钟计算耗时，不依赖该墙上时钟值
         * @return 见上述说明。
         */
        @Override
        public boolean drainTimedOut(long now) {
            if (active) return false;
            if (lock == null) return false;
            if (lockLost) return false;
            long st = stopNanos;
            if (st <= 0) return false;
            return System.nanoTime() - st >= TimeUnit.MILLISECONDS.toNanos(group.drainTimeoutMs);
        }

        /**
         * 业务作用：drain 等待期间的 holds 自检. 与 {@link #beforePoll()} 区别: <b>不读 active / running</b>,
         * 让 drain 路径不被"主动 stop"或"全局 running=false"立即终止. 锁真丢失时设 {@code lockLost=true} +
         * return false, 与 beforePoll 同 lockLost 设置行为, 主循环看到 lockLost=true 走立即 exit 分支.
         * <p>
         * 时间限流复用 {@link #lastHoldsCheckNanos} + {@link PartitionGroup#holdsCheckIntervalMs},
         * 与 beforePoll 共享一个限流时钟, 避免 drain 期间 holds EVAL 翻倍.
         *
         * <p>参数说明: 无。
         *
         * @return 见上述说明。
         */
        @Override
        public boolean checkAliveHoldsOnly() {
            if (lock == null) return true;
            if (lockLost) return false;
            long nowNanos = System.nanoTime();
            long intervalNanos = TimeUnit.MILLISECONDS.toNanos(group.holdsCheckIntervalMs);
            if (lastHoldsCheckNanos != 0 && nowNanos - lastHoldsCheckNanos < intervalNanos) return true;
            lastHoldsCheckNanos = nowNanos;
            // 三态: null = Redis 异常 (瞬时抖动), 不能当锁丢提前解锁 — 当作仍持有继续 drain, 由 drainTimedOut 封顶兜底.
            // 0 = 真锁丢 (key 不在/被新 owner 抢), 设 lockLost 走立即 exit. 1 = 持有.
            group.lockSelfChecks.incrementAndGet();
            Long st = distributedLock.holdsStatus(lockKey, lockHolder);
            if (st == null || st == 1L) return true;
            lockLost = true;
            sourceAuthority.loseAuthority();
            log.warn("[{}] partition lock lost during drain streamPrefix={} partition={} holder={}",
                    redisProxy.getQualifier(), group.streamPrefix, partition, lockHolder);
            return false;
        }

        // ==================== BatchStreamListener ====================

        /**
         * 业务作用：一批消息到达, container 已经反序列化好 (用 RedisProxy 的 hashValueSerializer)。
         * <p>
         * 注: 用 raw type 是为了对齐 RedisProxy 现有 streamListenerNonGroup 风格 — activateDefaultTyping=true
         * 时 deserializer 行为有变, raw 跳 checkcast 容错。
         *
         * @param batch 见上述说明
         *              返回: 无返回值；成功处理的消息被确认，失败或非法消息保留在 PEL 并暂停来源。
         */
        @Override
        public void onMessage(RecycleLinkedList<MapRecord<String, Object, Object>> batch) {
            partitionRuntime.dispatchPartition(this, batch);
        }

        /**
         * 业务作用：fallback: 单条投递 (理论上 BatchStreamPollTask 一定走批量, 这里只是接口兜底)
         *
         * @param message 消息体
         */
        @Override
        public void onMessage(MapRecord<String, Object, Object> message) {
            RecycleLinkedList<MapRecord<String, Object, Object>> one = RecycleLinkedList.of();
            try {
                one.add(message);
                this.onMessage(one);
            } finally {
                one.recycle();
            }
        }

        /**
         * 业务作用：向 dispatcher 暴露本 Claim 的共享权威引用。参数说明: 无。返回: 来源权威对象。
         */
        @Override
        public StreamSourceAuthority authority() {
            return sourceAuthority;
        }

        /**
         * 业务作用：复验 Claim、组件与 holder 身份仍接受新 Task 提交。参数说明: 无。返回: admission 完整开放时为 true。
         */
        @Override
        public boolean allowsAdmission() {
            return running && active && !routeBlocked && !lockLost
                    && lock != null && lockHolder != null && sourceAuthority.isActive();
        }

        /**
         * 业务作用：允许已进入 PEL 的恢复责任在 routeBlocked 时继续使用当前 holder。参数说明: 无。返回: holder 仍有效时为 true。
         */
        @Override
        public boolean allowsRecovery() {
            return running && active && !lockLost
                    && lock != null && lockHolder != null && sourceAuthority.isActive();
        }

        /**
         * 业务作用：为精确重试、整批重试和接管补扫统一查询实际 Redis holder，拒绝仅凭本地状态提交迟到正文。
         * 参数说明: 无。
         *
         * @return holder 明确有效且本地仍可恢复时为 true；UNKNOWN 保留权威交由调用方退避，明确失权时撤销本地执行权威。
         */
        @Override
        public boolean revalidateRecoveryAuthority() {
            // 停止或已失权的来源没有重新取得执行权的资格，不能以迟到 Redis 证据恢复准入。
            if (!allowsRecovery() || !partitionRuntime.admissionOpen()) return false;
            Long held = fencedForRecover();
            // UNKNOWN 不能证明仍持锁，也不能证明已失权；原恢复责任必须保留并继续退避。
            if (held == null) return false;
            if (!Long.valueOf(1L).equals(held)) {
                // 远端明确不再承认原 holder，立即停止本地业务提交，PEL 留给后续合法 owner。
                lockLost = true;
                sourceAuthority.loseAuthority();
                return false;
            }
            return allowsRecovery() && partitionRuntime.admissionOpen();
        }

        /**
         * 业务作用：只阻断本 Claim 新读取并保留 holder，使原批次 route recovery 不被后继消息越过。参数说明: 失败原因。返回: 无返回值。
         */
        @Override
        public void blockRoute(Throwable failure) {
            routeBlocked = true;
            partitionRuntime.sourceRouteBlocked();
            group.container.wakeManagedRunners();
        }

        /**
         * 业务作用：整批 route recovery 收敛后重新开放当前 Claim。参数说明: 无。返回: 无返回值。
         */
        @Override
        public void clearRouteBlock() {
            routeBlocked = false;
            group.container.wakeManagedRunners();
        }

        /**
         * 业务作用：把当前 poll raw batch 容量转交给 route recovery，外层 batch finally 不再释放。参数说明: 无。返回: retained Permit。
         */
        @Override
        public PartitionRecordCapacity.Permit retainActiveBatch() {
            return recordState.retainActiveBatch();
        }

        /**
         * 业务作用：遇到无法安全分类、推进或依赖失健康时先关闭新读取，再让 Claim 在 holder 保护下排干并保留 PEL。
         *
         * @param failure 触发来源暂停的原因
         *                返回: 无返回值；重复暂停保持幂等。
         */
        @Override
        public void pause(Throwable failure) {
            if (!active) return;
            log.error("[{}] partition source paused stream={} partition={} reason={}",
                    redisProxy.getQualifier(), stream, partition, failure == null ? null : failure.getMessage(), failure);
            // 当前已受理 Task 仍需要原 holder 完成 fenced ACK；本地 gate 与重试责任在 afterExit 撤销，不能提前开放同 key。
            markStop();
        }

        /**
         * 业务作用：在 Redis 内原子复验实际 lock holder、XACK 精确 id，并按计划选择性 XDEL 本次确认正文。
         *
         * @param ids        已由 listener 成功处理的 record id
         * @param autoDelete 本次实际 XACK 后是否删除正文
         * @return 逐 id 的明确确认、失权或不确定结论
         */
        @Override
        public Map<String, StreamCommitCoordinator.AckDisposition> ack(
                List<String> ids, boolean autoDelete) {
            if (ids == null || ids.isEmpty()) return Map.of();
            String holder = lockHolder;
            if (holder == null || lockLost || lock == null || !sourceAuthority.isActive()) {
                LinkedHashMap<String, StreamCommitCoordinator.AckDisposition> lost = new LinkedHashMap<>();
                for (String id : ids) lost.put(id, StreamCommitCoordinator.AckDisposition.LOST_AUTHORITY);
                return Map.copyOf(lost);
            }
            Object[] args = new Object[3 + ids.size()];
            args[0] = holder;
            args[1] = group.streamPrefix;
            args[2] = autoDelete ? "1" : "0";
            for (int index = 0; index < ids.size(); index++) args[index + 3] = ids.get(index);

            List<?> result = executeHolderFencedAck(args);
            if (result == null || result.isEmpty()) {
                throw new IllegalStateException("holder-fenced ACK returned empty result");
            }
            long marker = ((Number) result.get(0)).longValue();
            if (marker == -1L) {
                lockLost = true;
                sourceAuthority.loseAuthority();
                partitionRuntime.authorityLost(sourceAuthority);
                LinkedHashMap<String, StreamCommitCoordinator.AckDisposition> lost = new LinkedHashMap<>();
                for (String id : ids) lost.put(id, StreamCommitCoordinator.AckDisposition.LOST_AUTHORITY);
                return Map.copyOf(lost);
            }
            if (marker != 1L) {
                throw new IllegalStateException("holder-fenced ACK returned invalid marker: " + marker);
            }
            if (result.size() != ids.size() + 1) {
                throw new IllegalStateException("holder-fenced ACK result size mismatch");
            }
            LinkedHashMap<String, StreamCommitCoordinator.AckDisposition> observations = new LinkedHashMap<>();
            for (int index = 1; index < result.size(); index++) {
                long idStatus = ((Number) result.get(index)).longValue();
                if (idStatus != 0L && idStatus != 1L) {
                    throw new IllegalStateException("holder-fenced ACK returned invalid status: " + idStatus);
                }
                observations.put(ids.get(index - 1), StreamCommitCoordinator.AckDisposition.CONFIRMED);
            }
            return Map.copyOf(observations);
        }

        /**
         * 业务作用：精确复验一个物理分区 id 是否仍在 group PEL，holder 路径只要未失权即可确认任意 consumer 成员。
         *
         * @param id record id
         * @return PEL 缺席时为 ABSENT，holder 有效时为 OWNED，失权时为 MOVED
         */
        @Override
        public StreamCommitCoordinator.PendingDisposition pending(String id) {
            if (!redisProxy.xPendingExact(stream, group.streamPrefix, id)) {
                return StreamCommitCoordinator.PendingDisposition.ABSENT;
            }
            return lockLost || lock == null || lockHolder == null || !sourceAuthority.isActive()
                    ? StreamCommitCoordinator.PendingDisposition.MOVED
                    : StreamCommitCoordinator.PendingDisposition.OWNED;
        }

        /**
         * 业务作用：标识本 Claim 为带分布式 holder 的物理分区来源。参数说明: 无。返回: REDIS_PARTITION。
         */
        @Override
        public StreamRecordSource sourceKind() {
            return StreamRecordSource.REDIS_PARTITION;
        }

        /**
         * 业务作用：返回当前 Claim 的物理 Stream key。参数说明: 无。返回: Stream key。
         */
        @Override
        public String stream() {
            return stream;
        }

        /**
         * 业务作用：返回物理分区共享的 consumer group。参数说明: 无。返回: streamPrefix group。
         */
        @Override
        public String group() {
            return group.streamPrefix;
        }

        /**
         * 业务作用：返回本进程物理分区 consumer name。参数说明: 无。返回: 节点会话身份。
         */
        @Override
        public String consumer() {
            return nodeId;
        }

        /**
         * 业务作用：用 RedisProxy serializer 精确读取一条物理分区正文，供 blocked/null-key 重试重建。
         *
         * @param id record id
         * @return 正文存在时返回 MapRecord，被裁剪或删除时返回 null
         */
        @Override
        public MapRecord<String, Object, Object> exact(String id) {
            return redisProxy.xRangeExactRecord(stream, id);
        }

        /**
         * 业务作用：执行一次 holder-fenced ACK Lua，调用方负责解释失权、未知结果与 PEL 复验。
         *
         * @param args holder、group、删除策略与精确 id 参数
         * @return Lua marker 与逐 id 状态
         */
        private List<?> executeHolderFencedAck(Object[] args) {
            // holder 比较与 XACK 必须在同一 Lua 原子单元内，禁止 check-then-act 窗口确认新 owner 的 PEL。
            return redisProxy.evalDirectConnection(
                    HOLDER_FENCED_ACK_LUA,
                    List.class,
                    new String[]{distributedLock.redisKey(lockKey), stream},
                    args);
        }

        // ==================== 内部: XAUTOCLAIM + dispatch + ack ====================

        /**
         * 业务作用：XAUTOCLAIM: 把 stream consumer group 中 idle &gt; minIdleMs 的 pending 消息
         * 转移到当前 consumer name 下, 然后正常 dispatch 处理。
         * <p>
         * minIdleMs 只约束 Redis 接管资格；当前 holder 的在途消费也可能达到该阈值。
         * dispatcher 按 record 协调本地执行与确认责任，并在取得执行权后复验 PEL，避免迟到正文重复提交。
         * <p>
         * 多页拉取，当前页路由未知时保留游标并等待整批恢复建立顺序责任，再迁移后续页。
         * cursor 返回 "0-0" 或不变时退出，单轮页数上限防止异常游标形成无界循环。
         *
         * <p>参数说明: 无。
         *
         * <p>返回：游标正常结束或停止、失权时退出；接管响应未知时保留屏障并补扫当前 consumer，未完整扫描会登记后续重试。
         */
        void recoverPending() {
            String start = "0-0";
            int loops = 0;
            int maxLoops = Math.max(group.count * 2, 100);
            // fullyDrained 只表示本次 XAUTOCLAIM 游标正常走到 "0-0"，不表示所有 PEL 都已达到 minIdle。
            // 任何中途退出都登记立即重试；游标正常结束则按外部 consumer PEL 的真实 idle 登记延迟补扫，
            // 避免尚未满足 minIdle 的记录卡到下一次 rebalance 或重启。
            boolean fullyDrained = false;
            try {
                // lockLost 加入循环条件: 防止 runner 已设 lockLost 但本 lambda 还在 queue 排队, 后续仍 XAUTOCLAIM
                // 把 PEL 消息 claim 到旧 consumer 名下影响新 owner 接管.
                while (active && running && !lockLost && loops++ < maxLoops) {
                    // 一来源只持有一个未知路由批次；前页收口前禁止迁移后页，避免已接管的 PEL 没有恢复者。
                    while (routeBlocked) {
                        if (!allowsRecovery()) return;
                        try {
                            Thread.sleep(10L);
                        } catch (InterruptedException signal) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    // 停止可能同时撤销路由责任；等待结束不代表 holder 仍允许发起新的接管副作用。
                    if (!allowsRecovery()) return;
                    if (pendingRecovery.isUncertain()
                            && !pendingRecovery.reconcileOwned(this::awaitRecoveryFence)) return;
                    // XAUTOCLAIM 会迁移 PEL，必须用真实 holder 证据覆盖主循环自检间隔；UNKNOWN 保持屏障并退避。
                    if (!awaitRecoveryFence()) return;
                    if (!recordState.tryAcquire(group.batchSize)) {
                        recoverPendingNeeded = true;
                        return;
                    }
                    try {
                        // 容量取得与 holder 往返之后仍须复验本地停止状态，不能因迟到响应再发起接管。
                        if (!allowsRecovery() || !partitionRuntime.admissionOpen()) return;
                        XAutoClaimArgs<byte[]> args = new XAutoClaimArgs<byte[]>()
                                .consumer(autoclaimConsumer)
                                .minIdleTime(Duration.ofMillis(group.minIdleMs))
                                .startId(start)
                                .count(group.batchSize);
                        ClaimedMessages<byte[], byte[]> claimed = redisProxy.xAutoClaim(stream, args);
                        if (claimed == null) {
                            // 缺少响应不能排除服务端已经迁移 PEL，必须走当前 consumer 补扫。
                            throw new QueryTimeoutException("XAUTOCLAIM returned no response");
                        }
                        List<StreamMessage<byte[], byte[]>> msgs = claimed.getMessages();
                        redisProxy.streamPartitionMetrics().recovery(
                                StreamRecordSource.REDIS_PARTITION, "xautoclaim",
                                msgs == null || msgs.isEmpty() ? "empty" : "claimed");
                        recordState.onPollResult(msgs == null ? 0 : msgs.size());
                        try {
                            if (pendingRecovery.isUncertain()) {
                                // 当前返回页可能晚于响应丢失的页面，先按当前 consumer PEL 的原始顺序完成全部交接。
                                recordState.afterBatchComplete();
                                if (!pendingRecovery.reconcileOwned(this::awaitRecoveryFence)) return;
                            } else if (msgs != null && !msgs.isEmpty()) {
                                // XAUTOCLAIM 返回后 dispatch 前再判一次: stop / lockLost 在 XAUTOCLAIM 期间也可能发生.
                                // 已 claim 的消息 idle 已重置, 留 PEL 给新 owner 等 minIdleMs 后再接管, 不在本节点投递业务.
                                if (!active || !running || lockLost) {
                                    log.warn("[{}] partition recoverPending stopped between xautoclaim and dispatch streamPrefix={} partition={} claimedCount={}",
                                            redisProxy.getQualifier(), group.streamPrefix, partition, msgs.size());
                                    return;
                                }
                                // dispatch 前二次 Redis fence: XAUTOCLAIM 一次 RTT 几 ms - 几十 ms, 期间锁可能在 Redis 端
                                // 过期被新 owner 抢走, 本地 lockLost 还没刷新 (holds 自检 5s 一次). 不做二次 fence 直接 dispatch
                                // 会让旧 owner 跑业务 listener；最终 holder-fenced Lua 虽会拒绝确认，却不能撤回外部副作用。
                                // 复验不确定时继续持有已知整页、raw 容量和恢复令牌，不能丢弃这一页再受 minIdle 过滤。
                                if (!awaitRecoveryFence()) return;
                                // byte[] StreamMessage → MapRecord, 走主 dispatch 路径
                                // (反序列化 + 分桶 + flush 与 onMessage 完全一致, 不再有第二条 byte[] 路径)
                                // toMapRecord 始终非 null，空或失败正文也保留精确 id，让 dispatcher 进入 BLOCK_CLAIM。
                                RecycleLinkedList<MapRecord<String, Object, Object>> batch = RecycleLinkedList.of();
                                try {
                                    for (StreamMessage<byte[], byte[]> msg : msgs) {
                                        batch.add(this.toMapRecord(msg));
                                    }
                                    try {
                                        partitionRuntime.dispatchPartitionClaimed(this, batch);
                                    } catch (StreamPartitionRuntime.PartitionDispatchException retained) {
                                        // 已登记的页内恢复不能替代剩余分页；保留当前游标，下一页仍在本轮读取屏障内推进。
                                        redisProxy.streamPartitionMetrics().recovery(
                                                StreamRecordSource.REDIS_PARTITION, "xautoclaim", "retained");
                                    }
                                } finally {
                                    batch.recycle();
                                }
                            }
                            String next = Objects.requireNonNull(claimed.getId(), "XAUTOCLAIM cursor");
                            if ("0-0".equals(next)) {
                                // 游标正常结束；仍未达到 minIdle 的外部 consumer PEL 由 finally 登记延迟补扫。
                                fullyDrained = true;
                                pendingRecovery.completeScan();
                                return;
                            }
                            if (next.equals(start)) {
                                // 游标不前进 — 防死循环的兜底, 也不代表本次扫描正常结束.
                                // 可能 Redis 客户端异常游标 / cluster 切换等. 不标 fullyDrained, 让 finally 设 retry 让下轮接管.
                                log.warn("[{}] partition recoverPending cursor stuck (next == start), will retry streamPrefix={} partition={} cursor={}",
                                        redisProxy.getQualifier(), group.streamPrefix, partition, next);
                                return;
                            }
                            start = next;
                        } finally {
                            recordState.afterBatchComplete();
                        }
                    } catch (Exception e) {
                        recordState.afterPollFailure();
                        if (StreamPendingRecovery.isTransient(e)) {
                            // 服务端可能已迁移本页；仍由本轮在新读取屏障内核对当前 consumer，不依赖 foreign PEL 补扫。
                            pendingRecovery.markUncertain();
                            if (!pendingRecovery.backoff()) return;
                            continue;
                        }
                        redisProxy.streamPartitionMetrics().recovery(
                                StreamRecordSource.REDIS_PARTITION, "xautoclaim", "failure");
                        // 未归类命令失败不能证明扫描结束；后续扫描继续复验，XREADGROUP > 仍保持关闭。
                        log.error("[{}] xautoclaim failed, will retry streamPrefix={} partition={}",
                                redisProxy.getQualifier(), group.streamPrefix, partition, e);
                        return;
                    }
                }
                // while 退出但没 break/return: loops 耗尽 (maxLoops 兜底).
                // 此时 cursor 可能还没扫到 "0-0", 留 finally 兜底 set retry.
            } finally {
                // 兜底重试触发条件: 没扫完 + task 仍 live (active/running/!lockLost). lockLost / 主动 stop / shutdown 路径
                // 即将退出, 不应触发 retry. 已显式 set 过 recoverPendingNeeded=true 的路径在这里重设也无害 (等价操作).
                if (!fullyDrained && active && running && !lockLost) {
                    this.recoverPendingNeeded = true;
                    if (loops >= maxLoops) {
                        log.warn("[{}] partition recoverPending hit maxLoops without draining PEL, will retry streamPrefix={} partition={} loops={}",
                                redisProxy.getQualifier(), group.streamPrefix, partition, loops);
                    }
                }
                if (fullyDrained) this.scheduleForeignPendingFollowUpIfNeeded();
            }
        }

        /**
         * 业务作用：扫描结束后检查其它 consumer 遗留的 PEL，并按其真实 idle 登记下一次安全接管时间。
         * 已被当前 consumer 接管但业务处理失败的记录不会进入该调度，因此不会形成同 owner 无限重试。
         *
         * <p>参数说明: 无。
         * <p>
         * 返回: 无返回值；不存在外部 PEL 时取消补扫，查询异常时按最多一秒退避后重新检查。
         */
        private void scheduleForeignPendingFollowUpIfNeeded() {
            if (!active || !running || lockLost) return;
            long delayMs = -1;
            try {
                PendingMessagesSummary summary = redisProxy.xPending(stream, group.streamPrefix);
                if (summary != null) {
                    for (Map.Entry<String, Long> entry : summary.getPendingMessagesPerConsumer().entrySet()) {
                        if (entry.getValue() == null || entry.getValue() <= 0 || nodeId.equals(entry.getKey()))
                            continue;
                        org.springframework.data.redis.connection.stream.Consumer foreignConsumer =
                                org.springframework.data.redis.connection.stream.Consumer.from(group.streamPrefix, entry.getKey());
                        PendingMessages pending = redisProxy.xPending(stream, foreignConsumer, 1);
                        if (pending == null || pending.isEmpty()) continue;
                        long idleMs = pending.get(0).getElapsedTimeSinceLastDelivery().toMillis();
                        long remainingMs = Math.max(1L, group.minIdleMs - idleMs);
                        if (delayMs < 0 || remainingMs < delayMs) delayMs = remainingMs;
                    }
                }
            } catch (Exception e) {
                delayMs = Math.max(1L, Math.min(1_000L, group.minIdleMs));
                log.warn("[{}] partition pending detail failed, schedule guarded follow-up streamPrefix={} partition={}",
                        redisProxy.getQualifier(), group.streamPrefix, partition, e);
            }
            if (delayMs < 0) {
                recoverPendingFollowUpStartNanos = 0;
                recoverPendingFollowUpDelayNanos = 0;
                return;
            }
            recoverPendingFollowUpDelayNanos = TimeUnit.MILLISECONDS.toNanos(delayMs);
            recoverPendingFollowUpStartNanos = System.nanoTime();
        }

        /**
         * 业务作用：把 Lettuce 的 byte[] {@link StreamMessage} 包装成 框架的 {@link MapRecord},
         * 让 XAUTOCLAIM 路径下游可以走与主消费循环完全一样的 Partition dispatcher。
         * <p>
         * 反序列化用 RedisProxy 的 hashKeySerializer / hashValueSerializer, 与 publish 端 + 主消费
         * 路径 (container deserializer) 一致, 保证两条路径出来的 PooledEvtData 类型完全相同。
         * <p>
         * <b>始终返回非 null</b>: 空 body / 反序列化失败时返回带空 Map 的 MapRecord，
         * dispatcher 会按 BLOCK_CLAIM 策略暂停来源并保留 PEL，不能把未知顺序归属静默丢弃。
         *
         * @param msg 见上述说明
         */
        private MapRecord<String, Object, Object> toMapRecord(StreamMessage<byte[], byte[]> msg) {
            String id = msg.getId();
            Map<byte[], byte[]> body = msg.getBody();
            if (body == null || body.isEmpty()) {
                // 空 body 仍保留 record id，dispatcher 据此暂停来源并把该坐标留在 PEL。
                return MapRecord.create(stream, Collections.emptyMap()).withId(RecordId.of(id));
            }
            try {
                Map<Object, Object> map = new LinkedHashMap<>(body.size());
                for (Map.Entry<byte[], byte[]> e : body.entrySet()) {
                    Object hk = redisProxy.getHashKeySerializer().deserialize(e.getKey());
                    Object hv = redisProxy.getHashValueSerializer().deserialize(e.getValue());
                    map.put(hk, hv);
                }
                return MapRecord.create(stream, map).withId(RecordId.of(id));
            } catch (Exception e) {
                log.error("[{}] partition deserialize failed (autoclaim) stream={} id={}",
                        redisProxy.getQualifier(), stream, id, e);
                // 反序列化失败仍返回精确坐标，后续按 BLOCK_CLAIM 保留 PEL。
                return MapRecord.create(stream, Collections.emptyMap()).withId(RecordId.of(id));
            }
        }

        /**
         * 业务作用：提交物理 Claim 的历史 PEL 接管，在提交前登记恢复责任，防止排队或 Redis 响应停顿穿透停机边界。
         * 执行器拒绝必须在持锁 Claim 内处理，避免启动失败重试覆盖尚未释放的 acquisition。
         * <p>
         * 参数说明: 无。
         * 返回: 无返回值；关闭后不再提交，提交失败归还令牌并保留后续恢复标志，执行成功由任务退出时归还令牌。
         */
        private void submitRecoverPending() {
            StreamRuntimeStatus.DrainToken drain = partitionRuntime.beginSourceRecovery();
            // 已持有分区锁不代表仍有全局恢复准入；未取得令牌时让现有 Claim 按停机流程释放权威。
            if (drain == null) return;
            this.recovering = true;
            this.recoverPendingNeeded = false;
            ActionRecycler ar = null;
            try {
                ar = ActionRecycler.ofRecycle(RECOVER_PENDING_CON).ref(0, this).ref(1, drain);
                partitionRuntime.waitExecutor().execute(ar);
            } catch (Throwable t) {
                // 未被执行器接纳的恢复不会自行退出，提交方必须归还池化动作与排干责任，且不能丢失 Claim 的释放入口。
                try {
                    if (ar != null) ar.recycle();
                } finally {
                    this.recoverPendingNeeded = true;
                    this.recovering = false;
                    drain.close();
                }
                log.warn("[{}] partition recoverPending submit failed, will retry in beforePoll streamPrefix={} partition={}",
                        redisProxy.getQualifier(), group.streamPrefix, partition, t);
            }
        }

        /**
         * 业务作用：在专用等待执行域完成历史 PEL 接管，并在 Redis 调用和上下文清理结束后交还恢复责任。
         *
         * @param drain 提交前登记的恢复令牌，覆盖排队、XAUTOCLAIM、后续投递与退出清理
         *              返回: 无返回值；失败记录诊断并保留 PEL，所有退出路径均交还恢复责任。
         */
        void doRecoverPendingAsync(StreamRuntimeStatus.DrainToken drain) {
            long startedAt = System.nanoTime();
            boolean measureTakeover = initialRecovery;
            try (drain) {
                try {
                    this.recoverPending();
                } catch (Throwable t) {
                    log.error("[{}] partition recoverPending async failed streamPrefix={} partition={}",
                            redisProxy.getQualifier(), group.streamPrefix, partition, t);
                } finally {
                    if (measureTakeover) {
                        group.pelTakeoverMillis = TimeUnit.NANOSECONDS.toMillis(
                                System.nanoTime() - startedAt);
                        initialRecovery = false;
                    }
                    // 恢复路径不经过主消费循环的 finally，必须在归还排干令牌之前清理线程上下文和本地运行标志。
                    AnyHolder.clear();
                    this.recovering = false;
                }
            }
        }

        /**
         * 业务作用：在未进入 owner 解锁流程时结束 RedisLock 当前 acquisition，不发送 Redis 命令。
         * 用于已确认 lockLost 或检测到本地线程身份不变量被破坏的退出路径；RedisLock 自身抛出的
         * owner 失权与网络异常已经完成本地处置，调用方不得再次回收。
         *
         * <p>参数说明: 无。
         * <p>
         * 返回: 无返回值；底层处置异常只记录诊断，不阻断 Claim 的最终收口。
         */
        private void disposeLocalLock() {
            Lock l = this.lock;
            String h = this.lockHolder;
            if (l != null && h != null) {
                try {
                    LettuceDistributedLock.disposeLocal(l, h);
                } catch (Throwable t) {
                    log.warn("[{}] partition disposeLocal failed streamPrefix={} partition={}",
                            redisProxy.getQualifier(), group.streamPrefix, partition, t);
                }
            }
        }

        /**
         * 业务作用：recoverPending 路径的 fencing 校验 (三态): 区分真锁丢 vs 网络异常.
         * <p>
         * <ul>
         *   <li>{@code 1L} = 持有, 可继续 XAUTOCLAIM</li>
         *   <li>{@code 0L} = 不持有 (锁真丢, 已被新 owner 接管或过期), 调用方应设 lockLost=true 退出</li>
         *   <li>{@code null} = Redis 异常 (eval 抛 / 超时 / 网络断开), 调用方应跳过本轮但不污染 lockLost,
         *       下次 beforeStart / 主循环 holds 自检会重新评估</li>
         * </ul>
         * 这条区分是为了避免单次网络抖动把 lockLost 误标 true, 让锁等 30s lease 才释放.
         *
         * @return EVAL 原始结果, 调用方按三态语义处理
         */
        private Long fencedForRecover() {
            String h = lockHolder;
            if (h == null) return 0L;
            return distributedLock.holdsStatus(lockKey, h);
        }

        /**
         * 业务作用：在已知接管页或当前 consumer 补扫投递前等待明确的 holder 证据，UNKNOWN 不释放页面责任。
         * 参数说明: 无。
         *
         * @return holder 明确有效时为 true；停止、失权或中断时关闭本次投递并返回 false。
         */
        private boolean awaitRecoveryFence() {
            while (allowsRecovery() && partitionRuntime.admissionOpen()) {
                if (revalidateRecoveryAuthority()) return true;
                // 明确失权或停止已经关闭本地权威，不得把它降级为可继续等待的 UNKNOWN。
                if (!allowsRecovery() || !partitionRuntime.admissionOpen()) return false;
                // 缺少 holder 证据时不投递业务，也不把不确定的已迁移页视为完成。
                partitionRuntime.recoveryUncertain(sourceAuthority, true);
                if (!pendingRecovery.backoff()) return false;
            }
            return false;
        }

    }

}
