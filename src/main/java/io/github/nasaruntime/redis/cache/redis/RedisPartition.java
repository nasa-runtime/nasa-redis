package io.github.nasaruntime.redis.cache.redis;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.nasaruntime.core.base.*;
import io.github.nasaruntime.redis.cache.redis.stream.BatchStreamListener;
import io.github.nasaruntime.redis.cache.redis.stream.BatchStreamMessageListenerContainer;
import io.github.nasaruntime.redis.cache.redis.stream.PollLifecycle;
import io.github.nasaruntime.core.config.Graceful;
import io.github.nasaruntime.core.evt.PooledEvtData;
import io.github.nasaruntime.core.function.ActionRecycler;
import io.github.nasaruntime.core.utils.ContextUtils;
import io.github.nasaruntime.core.utils.MapUtils;
import io.github.nasaruntime.core.utils.ObjMprUtils;
import io.github.nasaruntime.core.utils.StringUtils;
import org.springframework.data.redis.connection.stream.ReadOffset;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.models.stream.ClaimedMessages;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 业务作用：按业务分区键把 Redis Stream 消息稳定路由到独占消费者，在节点变化后重新分配分区并接管未确认消息。
 * <p>
 * N 个分区 Stream 共享一组消费者组，每个分区通过 {@link LettuceDistributedLock} 独占；取得锁的节点执行
 * XAUTOCLAIM 与 XREADGROUP。同一个分区键始终落入同一分区，并由当前持锁节点串行处理；不同分区之间不保证顺序。
 *
 * <h2>架构特性</h2>
 * <ul>
 *   <li><b>消费容器</b>：每个分区组使用独立的 {@link BatchStreamMessageListenerContainer}，
 *       复用对应 {@link RedisProxy} 的连接工厂、序列化方式与业务执行基础设施，并允许单组覆盖
 *       pollTimeout 和 batchSize。</li>
 *   <li><b>分区认领</b>：启动与周期再平衡都以非阻塞 tryLock 认领分区，通知通道只负责缩短收敛延迟。</li>
 *   <li><b>崩溃恢复</b>：原 owner 的锁租期失效后，新 owner 用 XAUTOCLAIM 接管达到空闲阈值的 PEL。</li>
 *   <li><b>失权门禁</b>：周期 holds 自检与 ACK 前复验共同阻止旧 owner 确认新 owner 应接管的消息。</li>
 *   <li><b>topic 路由</b>：默认所有 topic 共享分区组，高吞吐或慢 topic 可以隔离到独立组。</li>
 * </ul>
 *
 * <h3>使用方式 (yml 驱动 + listener 声明式, 业务侧零样板)</h3>
 * <pre>{@code
 * // 1. yml 配置。分区组的创建由 RedisProxy.initialize 按容器内 PARTITION 模式 listener 的 topic 自动完成,
 * //    业务侧不需要显式调 init / isolate。
 * //    nasa.redis.properties.primary.stream.partition:
 * //      enabled: true
 * //      count: 64
 * //      groups:
 * //        contract:settlement: { count: 64, batch-size: 200 }
 *
 * // 2. 业务侧实现 RedisEventBatchListener (或 Single), mode = PARTITION
 * @Component
 * public class OpenPositionListener implements RedisEventBatchListener<TestOrder> {
 *     public String[] topics() { return new String[]{"contract:settlement"}; }
 *     public String event() { return "open-position"; }
 *     public ConsumeMode mode() { return ConsumeMode.PARTITION; }      // 标记走 partition
 *     public TypeReference<TestOrder> paramType() { return TestOrder.REFERENCE; }
 *     public void onEvent(List<TestOrder> orders) { ... }              // 同 uid 同分区串行
 * }
 *
 * // 3. 业务发布 — 分区数与命名空间不变时，同 uid 路由到同一分区的单一持权处理路径
 * RedisPartition.load().publish("contract:settlement", "open-position", uid, TestOrder);
 * }</pre>
 *
 * <h3>命名空间</h3>
 * <ul>
 *   <li>默认共享组 stream: {@code SINGLE-CONSUME:0..63} (前缀来自 yml {@code partition.defaultGroup})</li>
 *   <li>隔离组 stream: {@code SINGLE-CONSUME:contract:settlement:0..63} (拼 defaultGroup + 逻辑名)</li>
 *   <li>consumer group 名 = stream 前缀 (默认组 = SINGLE-CONSUME, 隔离组 = SINGLE-CONSUME:contract:settlement)</li>
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
     * 实际 stream/group 名由 {@code Partition.defaultGroup} 决定 (yml 可配, 默认 "SINGLE-CONSUME"),
     * 这里只是 {@link RedisPartition#groups} 这个内部 Map 的 key 约定 — 默认组用 "" 占位,
     * 隔离组用业务逻辑短名 ("contract:settlement" 等)。
     */
    public static final String DEFAULT_GROUP_NAME = "";

    /**
     * 消息体写入 stream 的 hash field
     */
    public static final String DATA_FIELD = "data";

    /**
     * dispatch 热路径 computeIfAbsent 用的工厂常量, 提到字段避免每批次 new lambda
     */
    private static final Function<TopicEventKey, RecycleLinkedList<Object>> DATA_BUCKET = k -> RecycleLinkedList.of();
    private static final Function<TopicEventKey, RecycleLinkedList<String>> ID_BUCKET = k -> RecycleLinkedList.of();

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
     * 业务作用：以两个独立字段表达消息路由身份，避免 topic 或 event 自身包含分隔符时映射到同一监听器。
     *
     * @param topic 消息主题
     * @param event 事件名称
     */
    private record TopicEventKey(String topic, String event) {}

    /**
     * recoverPending 异步提交的 Consumer 策略, 无状态全局共享, 配合 {@link ActionRecycler} 零 GC 派发.
     * ref(0)=Claim. 实际执行委托给 {@link Claim#doRecoverPendingAsync()}.
     */
    private static final Consumer<ActionRecycler> RECOVER_PENDING_CON = ar -> {
        Claim claim = ar.ref(0);
        claim.doRecoverPendingAsync();
    };

    /**
     * holder 内 traceIds 按 (topic, event) 分桶 map 的固定 key.
     * dispatch 阶段把整批的 traceId 按 teKey 累积成 RecycleLinkedMap&lt;teKey, traceIds 拼接&gt; set 进 holder,
     * flush 的 batch 路径再按当前 teKey 取本桶 traceIds 染色批日志.
     */
    private static final String KEY_TRACE_IDS_BY_TE = "traceIdsByTE";

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
        return CACHE.computeIfAbsent(redisProxy, RedisPartition::new);
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
     * 业务作用：销毁一个命令代理关联的分区消费入口，并解除静态登记，供 Spring 上下文安全重建。
     * 分区必须在命令代理和业务执行器仍可用时排干，否则关闭中的消费者会继续投递业务任务。
     *
     * @param redisProxy 即将销毁的命令代理
     *                   返回: 无返回值；没有关联实例时保持幂等。
     */
    static void destroy(RedisProxy redisProxy) {
        RedisPartition partition = CACHE.remove(redisProxy);
        if (partition != null) partition.shutdown();
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
     * 纯 publisher 路径的路由缓存: topic → [streamPrefix, count]。
     * resolveStream 热路径上避免每次遍历 yml groups 匹配 topic。
     */
    private final Map<String, Object[]> publishRouteCache = new ConcurrentHashMap<>();
    /**
     * 缓存 resolvePublishRoute 方法引用, 避免 computeIfAbsent 热路径分配
     */
    private final Function<String, Object[]> resolvePublishRouteRef = this::resolvePublishRoute;

    private volatile boolean running = true;
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
        this.redisProxy = redisProxy;
        // 复用 RedisProxy 对应的分布式锁实例. 必须在 NasaLettuceConfig 已经为该 RedisProxy
        // initialize 过 LettuceDistributedLock 之后才能用 (一般是 Spring 启动后期)
        this.distributedLock = LettuceDistributedLock.load(redisProxy);
        if (this.distributedLock == null) {
            throw new IllegalStateException(
                    "LettuceDistributedLock not initialized for RedisProxy[" + redisProxy.getQualifier()
                            + "], make sure NasaLettuceConfig has registered it");
        }
        // 每个运行时实例都使用新的会话标识。ME.sequence() 可能被同一主机上的多个进程共享，
        // 单独使用会让 ZSET 把多个消费者误判成一个节点，进而计算出错误的 fair 值。
        this.nodeId = ContextUtils.getPropertySafe("spring.application.name", "RedisPartition")
                + "/" + ME.sequence() + "/" + UUID.randomUUID();
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
        // 先复验或创建分区组，再发布 topic 路由；count 冲突时不能留下指向错误组的半完成映射。
        this.initGroup(groupName, count);
        topicToGroupName.put(topic, groupName);
        return this;
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
        initLock.lock();
        try {
            if (count <= 0) throw new IllegalArgumentException("count must be > 0, got " + count);
            PartitionGroup existing = groups.get(groupName);
            if (existing != null) {
                if (existing.count != count) {
                    throw new IllegalStateException("partition group count mismatch for ["
                            + (groupName.isEmpty() ? "<default>" : groupName)
                            + "]: initialized=" + existing.count + ", requested=" + count);
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
            group.prepare();
            // 只有远端协议复验和本地资源准备都成功后才发布本地组，失败配置不能留下半初始化入口。
            groups.put(groupName, group);
            return this;
        } finally {
            initLock.unlock();
        }
    }

    /**
     * 业务作用：启动所有已注册分区组的消费 — tryClaim 贪心 + 注册 rebalance timer。
     * <p>
     * 调用时机: 必须在所有 {@link #registerListener} 完成之后, 否则 task 拉到的消息找不到
     * listener 会被 ack 丢弃。
     * <p>
     * 框架在 {@link RedisProxy#initialize()} 末尾自动调用 (autoPreparePartitions → loadStreamSubscribe 扫描 → 本方法),
     * 业务方一般不需要手动调; 但如果业务方手动 init/isolate + register, 必须自己调本方法启动消费。
     */
    public void startAllGroups() {
        initLock.lock();
        try {
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
     * 业务作用：注册 listener. 框架自动调用 — 由 {@link RedisProxy#before()} 阶段的 loadStreamSubscribe 扫描所有
     * {@link StreamSubscribe} bean (含 {@link RedisEventBatchListener} / {@link RedisEventSingleListener}), 按 mode = PARTITION/BOTH 转交本方法。
     * <p>
     * 业务方一般不需要直接调用, 实现 StreamSubscribe bean + 标记 mode() = PARTITION 即可。
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
        if (!redisProxy.getStream().getPartition().isEnabled()) {
            throw new IllegalStateException(
                    "RedisPartition is disabled (yml stream.partition.enabled=false). "
                            + "Enable it before registering listeners.");
        }
        String[] topics = listener.topics();
        if (topics == null || topics.length == 0) {
            throw new IllegalArgumentException("listener topics() must not be empty: " + listener.getClass().getName());
        }
        String event = listener.event();
        if (StringUtils.isBlank(event)) {
            throw new IllegalArgumentException("listener event() must not be blank: " + listener.getClass().getName());
        }
        for (String topic : topics) {
            // 空白 topic 会污染 (topic, event) 路由 key 和 partition group 映射, 配置期 fail-fast
            if (StringUtils.isBlank(topic)) {
                throw new IllegalArgumentException("listener topic must not be blank: " + listener.getClass().getName());
            }
            String groupName = topicToGroupName.getOrDefault(topic, DEFAULT_GROUP_NAME);
            PartitionGroup group = groups.get(groupName);
            if (group == null) {
                throw new IllegalStateException(
                        "partition group [" + (groupName.isEmpty() ? "<default>" : groupName) + "] not initialized, "
                                + "configure yml stream.partition.groups before registerListener");
            }
            TopicEventKey key = new TopicEventKey(topic, event);
            StreamSubscribe<?, ?> prev = group.listenersByTopicEvent.put(key, listener);
            if (prev != null) {
                log.warn("[{}] partition listener overridden topic={} event={} prev={} new={}",
                        redisProxy.getQualifier(), topic, event,
                        prev.getClass().getName(), listener.getClass().getName());
            }
            topicToGroupName.putIfAbsent(topic, groupName);
        }
        return this;
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
        // 高层语义守门 (在 resolveStream / borrow 前): 空白 topic/event 或 null data 会污染路由 + 写入无效 entry
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(event) || data == null) return null;
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
    }

    // ==================== pipeline: 批量分区发布 (嵌入 LettucePipeline.Actuator) ====================
    //
    // 业务场景: settlement 等批处理用 LettucePipeline.Actuator 攒一批 pipeline 操作一次提交,
    // 其中部分消息需要走分区路由。此系列方法让业务侧把分区消息也嵌入同一个 pipeline 批次,
    // 而非每条 publish 独立 xAdd, 保持 pipeline 批量提交的性能优势。
    //
    // 调用链: actuator.partitionAsync → RedisPartition.pipelineAsync → actuator.xAddAsync
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
     * actuator.xAdd / xAddAsync 写入当前 pipeline 批次。
     * <p>
     * PooledEvtData 在序列化完成后立即 recycle — Actuator.xAdd/xAddAsync 内部
     * 先 serialize 成 byte[] 再加入 pipeline 命令队列, 序列化完 pm 对象即无引用。
     *
     * @param actuator   见上述说明
     * @param topic      主题名
     * @param event      事件名
     * @param hashAbs    见上述说明
     * @param roundRobin 见上述说明
     * @param data       业务数据
     * @param async      true → xAddAsync (fire-and-forget), false → xAdd (等 future)
     */
    private void doPipeline(LettucePipeline.Actuator actuator, String topic, String event,
                            int hashAbs, boolean roundRobin, Object data, boolean async) {
        // 高层语义守门 (与 doPublish 一致): 空白 topic/event 或 null data 直接 return
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(event) || data == null) return;
        String stream = this.resolveStream(topic, hashAbs, roundRobin);
        PooledEvtData pm = PooledEvtData.of(topic, event, data);
        RecycleLinkedMap<String, Object> pt = RedisProxyHolder.passthrough();
        // try/finally 兜底 (与 doPublish 一致): xAdd/xAddAsync 序列化 pm 时若抛异常, 仍归还池化的 pm/pt
        try {
            pm.setPassthrough(pt);
            if (async) {
                actuator.xAddAsync(stream, DATA_FIELD, pm);
            } else {
                actuator.xAdd(stream, DATA_FIELD, pm);
            }
        } finally {
            pm.recycle();
            // PooledEvtData.restore 不再 cascade, caller 显式归还 passthrough 到池
            if (pt != null) pt.recycle();
        }
    }

    /**
     * 业务作用：解析 topic + hash → 目标分区 stream key。
     * <p>
     * {@link #doPublish} 和 {@link #doPipeline} 共用。
     * topic → groupName (topicToGroupName 映射, 未配置走默认组) → group.streamPrefix + ":" + partition。
     *
     * @param hashAbs    已屏蔽符号位的 hash (用于 % count 取模)
     * @param roundRobin true → 忽略 hashAbs, 用本组 round-robin 计数均摊
     * @param topic      主题名
     * @return 目标分区 stream key, 如 "SINGLE-CONSUME:order:detail-batch:17"
     */
    private String resolveStream(String topic, int hashAbs, boolean roundRobin) {
        Objects.requireNonNull(topic, "topic must not be null");
        String groupName = topicToGroupName.getOrDefault(topic, DEFAULT_GROUP_NAME);
        PartitionGroup group = groups.get(groupName);
        if (group != null) {
            // 消费端已初始化, 直接用 group 的 streamPrefix + count
            int slot = roundRobin ? (group.roundRobin.getAndIncrement() & Integer.MAX_VALUE) : hashAbs;
            return group.streamPrefix + ":" + (slot % group.count);
        }
        // 纯 publisher 场景: 消费端未初始化 (enabled=false 或无 PARTITION listener),
        // 从 yml 配置直接计算 stream 名, 不需要 group 基础设施。缓存路由结果避免热路径重复遍历。
        Object[] route = publishRouteCache.computeIfAbsent(topic, resolvePublishRouteRef);
        String prefix = (String) route[0];
        int count = (int) route[1];
        AtomicInteger routeRoundRobin = (AtomicInteger) route[2];
        int slot = roundRobin ? (routeRoundRobin.getAndIncrement() & Integer.MAX_VALUE) : hashAbs;
        return prefix + ":" + (slot % count);
    }

    /**
     * 业务作用：纯 publisher 路由解析: 从 yml 配置计算 topic 对应的 streamPrefix + count。
     * 结果缓存到 {@link #publishRouteCache}, 热路径只查一次 ConcurrentHashMap。
     *
     * @param topic 主题名
     * @return 见上述说明。
     */
    private Object[] resolvePublishRoute(String topic) {
        NasaLettuceConfig.Partition pc = redisProxy.getStream().getPartition();
        String prefix = pc.getDefaultGroup();
        int count = pc.getCount();
        for (Map.Entry<String, NasaLettuceConfig.PartitionGroup> e : pc.getGroups().entrySet()) {
            String logicalName = e.getKey();
            NasaLettuceConfig.PartitionGroup gc = e.getValue();
            List<String> topics = gc.getTopics();
            if ((topics == null || topics.isEmpty()) ? logicalName.equals(topic) : topics.contains(topic)) {
                // count 必须 > 0: 否则 resolveStream 的 hashAbs % count 在 publish 热路径抛 ArithmeticException, 提前 fail-fast
                if (gc.getCount() <= 0) {
                    throw new IllegalStateException("[" + redisProxy.getQualifier() + "] partition count must be > 0, topic=" + topic
                            + " group=" + logicalName + " count=" + gc.getCount() + " (check yml stream.partition.groups)");
                }
                return new Object[]{prefix + ":" + logicalName, gc.getCount(), new AtomicInteger()};
            }
        }
        if (count <= 0) {
            throw new IllegalStateException("[" + redisProxy.getQualifier() + "] partition count must be > 0, topic=" + topic
                    + " count=" + count + " (check yml stream.partition.count)");
        }
        return new Object[]{prefix, count, new AtomicInteger()};
    }

    /**
     * 业务作用：优雅停机: cancel 所有 subscription + 等消费线程退出 + 释放分区锁。
     * 由进程级 {@link Graceful} 入口或 RedisProxy 生命周期调用，一般不需要业务侧手动执行。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回: 无返回值；重复调用保持幂等。
     */
    public void shutdown() {
        shutdownLock.lock();
        try {
            if (!running) return;
            // running=false 提前: 让 tryClaim、wake handler 与 rebalance 立即守门，
            // 防止 shutdown 期间 wake 异步触发的 rebalance 创建新 claim 进 container.pending 死掉.
            // 注意: Claim.beforePoll 已去掉 "!running" 短路, 不会因本字段误绕过 drain;
            // drain 路径靠 active/lockLost/stopNanos + checkAliveHoldsOnly 单独驱动.
            running = false;
            for (PartitionGroup g : groups.values()) {
                try {
                    g.shutdown();
                } catch (Throwable t) {
                    log.error("[{}] partition group shutdown failed group={} streamPrefix={}",
                            redisProxy.getQualifier(), g.groupName.isEmpty() ? "<default>" : g.groupName, g.streamPrefix, t);
                }
            }
            groups.clear();
        } finally {
            CACHE.remove(redisProxy, this);
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

        // -- 运行时参数: 优先 per-group 覆盖 → Partition 父级 → Stream 全局 (在构造时一次解析定型) --

        /**
         * 再平衡周期 ms
         */
        final long rebalancePeriodMs;
        /**
         * XAUTOCLAIM min-idle-time, 与 lock leaseTime 对齐 (默认 30s)
         */
        final long minIdleMs;
        /**
         * holds() 自检最小间隔 ms。runner 每个轮询周期都可能触发 beforePoll,
         * 但只有距上次 holds 间隔 &gt;= 本字段才真正发 EVAL, 避免 NOBLOCK 模式下空轮询打爆 holds()。
         */
        final long holdsCheckIntervalMs;
        /**
         * drain 超时 ms。主动 stop (rebalance/shutdown) 后等待 in-flight listener / recoverPending 的最大时长.
         * 超过该时长视为业务卡死并强制 exit；ACK fencing 会拒绝失权后的迟到 XACK。
         * <p>
         * 来源: yml partition.groups.{groupName}.drainTimeoutMs 覆盖 → Partition.drainTimeoutMs 全局.
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
         * 由 {@link #registerListener(StreamSubscribe)} 写入, dispatch 端按 (topic, event) 路由调用。
         * 同一 (topic, event) 只能注册一个 listener, 重复注册时旧的被覆盖并打 warn。
         */
        final Map<TopicEventKey, StreamSubscribe<?, ?>> listenersByTopicEvent = new ConcurrentHashMap<>();
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
            // 配置取值: 优先 per-group 覆盖 → Partition 父级 → Stream 全局
            // 这里一次性 resolve 定型, 后续运行不再读配置, 避免反复 lookup
            NasaLettuceConfig.Stream sc = redisProxy.getStream();
            NasaLettuceConfig.Partition pc = sc.getPartition();
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
            NasaLettuceConfig.PartitionGroup gc = groupName.isEmpty() ? null : pc.getGroups().get(groupName);
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
            this.container = redisProxy.createListenerContainer(pollTimeout, batchSize);
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
         * <p>参数说明: 无。
         * <p>
         * 返回: 无返回值；合同不一致或本地资源准备失败时抛出异常且不发布本地分区组。
         */
        void prepare() {
            // 分区数、锁命名空间与散列规则共同决定消息和所有权映射；集群节点不一致时必须在消费前拒绝启动。
            this.validatePartitionContract();
            // step 1: 准备每个分区 stream 的运行时元数据
            // - xGroupCreate: 给每个 stream 建消费者组. 多节点同时启动时, 第二个会因
            //   "BUSYGROUP" 抛异常, RedisProxy.xGroupCreate 内部已 containGroup 预检过滤, catch 兜底
            // - streamAutoTrim: 注册到 RedisProxy 的 stream 裁剪 cache, 由现有定时任务批量 XTRIM
            // - consumer group 名 = streamPrefix (跟 stream key 命名空间一致, 多节点共用一个 group)
            for (int i = 0; i < count; i++) {
                String stream = streamPrefix + ":" + i;
                try {
                    // 从 0-0 创建组: 分区是可靠队列语义, publisher 先写、consumer 后建组时不能跳历史.
                    // (仅首次创建生效; 已存在组 no-op, 故对现存部署无影响)
                    redisProxy.xGroupCreate(stream, streamPrefix, ReadOffset.from("0-0"));
                } catch (Exception e) {
                    log.debug("[{}] xGroupCreate skip stream={} err={}",
                            redisProxy.getQualifier(), stream, e.getMessage());
                }
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
         * 业务作用：在 Redis 中建立并复验分区映射协议，阻止同一 stream 命名空间下的节点使用不同分区数或锁域。
         * 租期和接管空闲时间属于运行调优参数，不写入硬协议，以便集群滚动调整而不改变消息映射与互斥身份。
         *
         * <p>参数说明: 无。
         * <p>
         * 返回: 无返回值；首次启动持久写入协议，后续配置不一致时抛出异常且不启动本组消费。
         */
        private void validatePartitionContract() {
            String lockPrefix = Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(distributedLock.keyPrefix().getBytes(StandardCharsets.UTF_8));
            String expected = "schema=1;count=" + count
                    + ";lock-prefix-b64=" + lockPrefix
                    + ";hash=java-hash-sign-mask-mod";
            String key = streamPrefix + ":partition-contract";
            String actual = redisProxy.evalDirectConnection(
                    PARTITION_CONTRACT_LUA, String.class, new String[]{key}, expected);
            if (!expected.equals(actual)) {
                throw new IllegalStateException("partition contract mismatch for " + streamPrefix
                        + ": expected [" + expected + "] but Redis contains [" + actual + "]");
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
         * 业务作用：启动消费阶段: 贪心 tryClaim 各分区 + 注册周期 rebalance 任务。
         * <p>
         * 必须在 listener 全部 register 完成后调用, 否则 task 拉到消息找不到 listener 会被丢弃。
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
            // 把消息处理推到 TimingWheel 工作线程, 不阻塞 pub/sub 监听线程.
            try {
                redisProxy.subscribe(wakeChannel, (msg) -> {
                    if (!running) return;
                    String body = msg == null ? "" : msg.toString();
                    // nodeId 的应用名部分允许包含冒号，不能只截取最后一段比较；按完整后缀识别自己发出的通知。
                    if (body.endsWith(":" + nodeId)) return;
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
            TimingWheel.platform(rebalancePeriodMs, rebalancePeriodMs, rebalanceTimerName, this::rebalance);
            // (分区锁定的逐条日志已在 beforeStart 中打印, 不需要额外快照)

            log.info("[{}] 分区组启动: group={} streamPrefix={} 总分区={} batchSize={} pollTimeout={} 初始claim={} listener数={}",
                    redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName,
                    streamPrefix, count, batchSize, pollTimeout, claims.size(), listenersByTopicEvent.size());

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
            String lockKey = streamPrefix + ":lock:" + partition;
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
         * 跑在 {@link TimingWheel} 工作线程上, 必须非阻塞 — 释放只调 {@link Claim#markStop()}
         * (设 active=false, runner 主循环进入 drain: 持锁等在途业务跑完或超时后 afterExit 解锁); 抢占由
         * {@link #tryClaim} 投递新 task 给 runner 完成。整个 rebalance 不持锁不阻塞 IO。
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
            try {
                String q = redisProxy.getQualifier();

                // === step 1: 心跳续约 + 估算存活节点 ===
                int alive = this.heartbeat();

                // 向上取整: 64 分区 3 节点 → fair=22, 允许部分节点多持 1 个, 避免余数分区成孤儿
                int fair = Math.max(1, (count + alive - 1) / alive);
                this.currentFair = fair;
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

                // (分区锁定/释放的逐条日志已在 beforeStart/afterExit 中打印, 这里不再重复摘要)

            } catch (Exception e) {
                log.error("[{}] partition rebalance failed group={} streamPrefix={}",
                        redisProxy.getQualifier(), groupName.isEmpty() ? "<default>" : groupName, streamPrefix, e);
            } finally {
                rebalancing.set(false);
            }
        }

        /**
         * 业务作用：优雅停机: 单次封顶 await, 总耗时与 N 无关 (但与 drainTimeoutMs 相关).
         * <p>
         * 流程:
         * <ol>
         *   <li>cancel rebalance timer + zRem alive 注册 + unsubscribe wakeChannel, 停止再平衡产生新的 claim</li>
         *   <li>快照 liveClaims 集合 (不是 claims.size — 后者不含 rebalance release 后已脱离但仍 live 的 task).
         *       latch 容量 = liveClaims snapshot.size, 在 markStop 之前快照</li>
         *   <li>给本批 live task 设 shutdownCounted=true, 让 afterExit finally 看此标志才 countDown,
         *       防止 race 触发的新 task 误 countDown 让 latch 提前归零</li>
         *   <li>主动补 countDown: 若某 task 在 snapshot 之后 / setShutdownCounted 之前已 afterExit 完成
         *       (此时它未 countDown), 这里用 shutdownCountedDown CAS exactly-once 补 countDown</li>
         *   <li>markStop 所有 live task → 主循环 drain 路径 → afterExit → CAS countDown</li>
         *   <li>container.stop 通知所有 runner.running=false, runner finally 顺序跑 drainAndExitOne</li>
         *   <li>latch.await(drainTimeoutMs + n*50ms + 1s): 单 task drain 上限 drainTimeoutMs,
         *       N task 顺序处理每个 ~50ms 开销, 加 1s buffer 防边界抖动</li>
         * </ol>
         * <p>
         * latch 超时仍未清完 → log.warn + 走 lease (30s) 兜底, 不再死等.
         *
         * <p>参数说明: 无。
         *
         * <p>返回：已停止再平衡与订阅并完成本轮有界排空后返回；超时任务交由锁租约阻止旧节点继续持权。
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
            } catch (Exception ignored) {}
            // step 0b: 取消 wake-up 订阅
            try {
                redisProxy.unsubscribe(wakeChannel);
            } catch (Exception ignored) {}
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
            // step 5: 单次封顶等待. N 个 task 顺序在 ManagedRunner finally 里 drainAndExitOne 处理.
            // 第一个 drain timeout 后, 后续 task drainTimedOut 立即 true, 但每个仍有 50ms parkNanos + exitManaged 开销.
            // worst awaitMs = drainTimeoutMs + (n-1) * 50ms + 1s buffer.
            // 卡死或超时则 fallback 到 leaseTime (30s) 自然过期, 新 owner XAUTOCLAIM 接管.
            if (n > 0) {
                long awaitMs = drainTimeoutMs + Math.max(0, n - 1) * 50L + 1_000L;
                try {
                    if (!shutdownLatch.await(awaitMs, TimeUnit.MILLISECONDS)) {
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
            // step 6: 主动通知其它节点立即 rebalance 接管. 必须在 latch.await 完成后 pub, 此时锁才在 Redis 里真的 DEL,
            // 其它节点 tryLock 才能成功; 提前 pub 其它节点会徒劳地撞失败. zRem (step 0a) 已让 ZCARD -1, 配合 wake
            // 让其它节点 ms 级感知 + 抢锁, 不必等 10s 周期.
            try {
                redisProxy.pub(wakeChannel, "offline:" + nodeId);
            } catch (Exception ignored) {}
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
    final class Claim implements PollLifecycle, BatchStreamListener<String, MapRecord<String, Object, Object>> {

        final PartitionGroup group;
        final int partition;
        final String lockKey;
        final String stream;
        final byte[] groupBytes;
        final byte[] consumerBytes;
        final io.lettuce.core.Consumer<byte[]> autoclaimConsumer;

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
         * runner 线程 tryLock 成功后写入的 holder 标识 (含 runnerThreadId). 给 businessExecutor 线程
         * 在 ACK / XDEL 前做 fencing 校验用 — businessExecutor 自身的 {@link LettuceDistributedLock#holds(String)}
         * 因 threadId 不同会一律返回 false, 必须用 runner 当时记录的 holder 走三态的
         * {@link LettuceDistributedLock#holdsStatus(String, String)} (1=持有放行, 0=丢失+lockLost, null=Redis 异常不误判).
         * <p>
         * volatile: runner (写) 与 businessExecutor (读) 跨线程, 必须保可见.
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
         * markStop() 的单调时钟快照, 给 drainTimedOut 用. volatile 防 32-bit JVM 撕裂 + 防后续重构把 markStop 内
         * "先写 stopNanos 再写 active" 的顺序搞反.
         */
        volatile long stopNanos;

        /**
         * 异步 recoverPending 期间 = true, runner 通过 {@link #isPollReady} 看到后跳过 batch poll,
         * 但仍调 checkAlive 维持 holds 自检 + markStop 响应。完成时由 beforeStart 内 finally 设回 false。
         * <p>
         * 串行性保证: 不进 batch list → 没有新 XREADGROUP 批次进 dispatch; recoverPending 在
         * businessExecutor 内独占 dispatch pending → 同 partition listener 调用严格串行,
         * 不会出现 "新消息 dispatch 与 pending dispatch 并发" 竞态。
         */
        volatile boolean recovering = false;
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
         *   <li>recoverPending 内 fence eval 返回 null (Redis 网络异常)</li>
         *   <li>recoverPending 内 xAutoClaim 抛异常</li>
         *   <li>recoverPending 内 maxLoops 耗尽但 cursor 未到 "0-0"</li>
         * </ul>
         * 这些路径在 finally 兜底设 true (只在 active &amp;&amp; running &amp;&amp; !lockLost 时), 主循环 beforePoll 检测后 retry.
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
         */
        Claim(PartitionGroup group, int partition, String lockKey) {
            this.group = group;
            this.partition = partition;
            this.lockKey = lockKey;
            this.stream = group.streamPrefix + ":" + partition;
            this.groupBytes = redisProxy.getKeySerializer().serialize(group.streamPrefix);
            this.consumerBytes = redisProxy.getKeySerializer().serialize(nodeId);
            this.autoclaimConsumer = io.lettuce.core.Consumer.from(groupBytes, consumerBytes);
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
            stopNanos = System.nanoTime();
            active = false;
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
            if (recoverPendingNeeded && active && running) return false;
            return true;
        }

        // ==================== PollLifecycle ====================

        /**
         * 业务作用：钩子 1/3: ManagedRunner 的 initManaged 调用 (在 runner 线程内)。
         * <p>
         * 流程:
         * <ol>
         *   <li>tryLock 拿锁. 失败 → 返回 false, runner 记录单调时钟并按 RETRY_INTERVAL_MS 重试</li>
         *   <li>拿到锁 → 设 {@link #recovering}=true 让 {@link #isPollReady} 返回 false, runner 暂不
         *       发起 batch poll. 把 recoverPending 异步提交到 businessExecutor — 否则崩溃恢复期
         *       PEL 几千条 XAUTOCLAIM 多页拉取会饿死 runner 内其他 partition (动辄几秒甚至几十秒不调度)</li>
         *   <li>recoverPending 完成时 finally 设 recovering=false, runner 下次发现 isPollReady=true
         *       自然恢复 batch poll</li>
         * </ol>
         * <p>
         * <b>串行性保证</b>: recovering=true 期间 runner 不进 batch list → 没有新 XREADGROUP 批次
         * 进 dispatch; recoverPending 在 businessExecutor 内串行 dispatch pending →
         * 同 partition 内 listener 调用严格串行, 不会出现 "新消息 dispatch 与 pending dispatch 并发" 竞态。
         *
         * <p>参数说明: 无。
         *
         * @return 取得分区锁并提交 pending 恢复时为 true；公平门禁、锁竞争或初始化失败时为 false。
         */
        @Override
        public boolean beforeStart() {
            // 本组真锁数已达 fair 上限, 不再抢新锁, 防止与 rebalance 释放对冲
            if (group.realLockCount.get() >= group.currentFair) return false;
            try {
                Lock l = distributedLock.getLock(lockKey);
                boolean acquired;
                try {
                    acquired = l.tryLock();
                } catch (Throwable t) {
                    log.error("[{}] partition tryLock threw streamPrefix={} partition={}",
                            redisProxy.getQualifier(), group.streamPrefix, partition, t);
                    return false;
                }
                if (!acquired) return false;

                this.lock = l;
                // 记录 runner 线程当下的 holder, 给 businessExecutor 后续 ACK fencing 用.
                // 必须在 runner 线程内执行 (initManaged 已保证), 否则 holder 含的 threadId 错位.
                this.lockHolder = LettuceDistributedLock.currentHolder();
                this.recoverPendingFollowUpStartNanos = 0;
                this.recoverPendingFollowUpDelayNanos = 0;
                int locked = group.realLockCount.incrementAndGet();
                log.info("[{}] 分区锁定: streamPrefix={} partition={} 持有={}/{}", redisProxy.getQualifier(), group.streamPrefix, partition, locked, group.count);
                // 异步 recoverPending: 不阻塞 runner 线程, 期间 runner 用 checkAlive 维持 holds 自检。
                // recovering=true 期间 isPollReady 返回 false, runner 跳过 batch poll 但保 holds 检查;
                // recovering=false 后 runner 自然恢复 batch poll。
                this.submitRecoverPending();
                return true;
            } catch (Throwable t) {
                log.error("[{}] partition beforeStart failed streamPrefix={} partition={}",
                        redisProxy.getQualifier(), group.streamPrefix, partition, t);
                return false;
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
                // 1) 同时清除 holder 与 Lock 引用。残留的 businessExecutor lambda 读到 null 后 fencing 失败，
                //    不会再用旧 holder 发 stale XACK；Claim 也不能继续引用已结束的 acquisition。
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
            Long st = distributedLock.holdsStatus(lockKey, lockHolder);
            if (st == null || st == 1L) return true;
            lockLost = true;
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
         *              返回: 无返回值；成功处理的消息被确认，失败消息保留在 PEL，非法消息按丢弃策略确认。
         */
        @Override
        public void onMessage(RecycleLinkedList<MapRecord<String, Object, Object>> batch) {
            this.dispatch(batch);
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

        // ==================== 内部: XAUTOCLAIM + dispatch + ack ====================

        /**
         * 业务作用：XAUTOCLAIM: 把 stream consumer group 中 idle &gt; minIdleMs 的 pending 消息
         * 转移到当前 consumer name 下, 然后正常 dispatch 处理。
         * <p>
         * 关键: <b>minIdleTime 与 lock leaseTime 对齐 (30s)</b> — 锁过期 30s 才被新节点抢, 这时候
         * pending 消息也至少 idle 30s, 可以接管, 不会抢正在被合法 owner 处理的消息。
         * <p>
         * 多页拉取, cursor 返回 "0-0" 或不变就退出。兜底循环上限 count*2 防边界 case 死循环。
         *
         * <p>参数说明: 无。
         *
         * <p>返回：游标正常排空或出现失权、连接异常时结束本轮；未完整收敛会登记后续补扫。
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
                    // XAUTOCLAIM 前做 holder fence: 长 GC / watchdog 失败导致锁已被新 owner 抢走时,
                    // runner 那侧 lockLost 还没刷新 (holds 自检 5s 一次), 这里走真实 Redis 校验提前感知.
                    // 用三态版本: 区分"真锁丢"和"网络异常". 单次网络抖动不把 lockLost 误标 true,
                    // 避免锁等 30s lease 才释放 — 下一轮 beforeStart / 主循环自检会重新评估.
                    Long fc = this.fencedForRecover();
                    if (fc == null) {
                        // Redis 异常: 保守跳过本轮 recover, 不污染 lockLost. finally 会设 recoverPendingNeeded=true 触发 retry.
                        log.warn("[{}] partition recoverPending fence eval error, pause this round, will retry streamPrefix={} partition={}",
                                redisProxy.getQualifier(), group.streamPrefix, partition);
                        return;
                    }
                    if (fc != 1L) {
                        // 真锁丢（返回 0）后主循环必须立即 exit；ACK fencing 会拒绝失权后的迟到确认，且该节点不得重新续租。
                        lockLost = true;
                        log.warn("[{}] partition recoverPending fence rejected (lock lost), streamPrefix={} partition={}",
                                redisProxy.getQualifier(), group.streamPrefix, partition);
                        return;
                    }
                    try {
                        XAutoClaimArgs<byte[]> args = new XAutoClaimArgs<byte[]>()
                                .consumer(autoclaimConsumer)
                                .minIdleTime(Duration.ofMillis(group.minIdleMs))
                                .startId(start)
                                .count(group.batchSize);
                        ClaimedMessages<byte[], byte[]> claimed = redisProxy.xAutoClaim(stream, args);
                        if (claimed == null) {
                            // Redis 返回 null (边界 case): 保守不算 fullyDrained, finally 兜底 set retry.
                            return;
                        }
                        List<StreamMessage<byte[], byte[]>> msgs = claimed.getMessages();
                        if (msgs != null && !msgs.isEmpty()) {
                            // XAUTOCLAIM 返回后 dispatch 前再判一次: stop / lockLost 在 XAUTOCLAIM 期间也可能发生.
                            // 已 claim 的消息 idle 已重置, 留 PEL 给新 owner 等 minIdleMs 后再接管, 不在本节点投递业务.
                            if (!active || !running || lockLost) {
                                log.warn("[{}] partition recoverPending stopped between xautoclaim and dispatch streamPrefix={} partition={} claimedCount={}",
                                        redisProxy.getQualifier(), group.streamPrefix, partition, msgs.size());
                                return;
                            }
                            // dispatch 前二次 Redis fence: XAUTOCLAIM 一次 RTT 几 ms - 几十 ms, 期间锁可能在 Redis 端
                            // 过期被新 owner 抢走, 本地 lockLost 还没刷新 (holds 自检 5s 一次). 不做二次 fence 直接 dispatch
                            // 会让旧 owner 跑业务 listener, 非幂等场景双执行污染数据. ack 由 fencedOk 拦, 但业务副作用已发生.
                            Long fc2 = this.fencedForRecover();
                            if (fc2 == null) {
                                // Redis 异常 — 跳过本批 dispatch, 留 PEL. finally 兜底 set retry.
                                log.warn("[{}] partition recoverPending fence eval error after xautoclaim, skip dispatch streamPrefix={} partition={} claimedCount={}",
                                        redisProxy.getQualifier(), group.streamPrefix, partition, msgs.size());
                                return;
                            }
                            if (fc2 != 1L) {
                                // 真锁丢 — 设 lockLost, 留 PEL 给新 owner. finally 因 lockLost 不 set retry.
                                lockLost = true;
                                log.warn("[{}] partition recoverPending fence rejected after xautoclaim (lock lost), skip dispatch streamPrefix={} partition={} claimedCount={}",
                                        redisProxy.getQualifier(), group.streamPrefix, partition, msgs.size());
                                return;
                            }
                            // byte[] StreamMessage → MapRecord, 走主 dispatch 路径
                            // (反序列化 + 分桶 + flush 与 onMessage 完全一致, 不再有第二条 byte[] 路径)
                            // toMapRecord 始终非 null (空/失败的也包成空 Map 让 dispatch 走毒消息丢弃)
                            RecycleLinkedList<MapRecord<String, Object, Object>> batch = RecycleLinkedList.of();
                            try {
                                for (StreamMessage<byte[], byte[]> msg : msgs) {
                                    batch.add(this.toMapRecord(msg));
                                }
                                this.dispatch(batch);
                            } finally {
                                batch.recycle();
                            }
                        }
                        String next = claimed.getId();
                        if (next == null || "0-0".equals(next)) {
                            // 游标正常结束；仍未达到 minIdle 的外部 consumer PEL 由 finally 登记延迟补扫。
                            fullyDrained = true;
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
                    } catch (Exception e) {
                        // XAUTOCLAIM 抛异常 (Redis 网络/超时等): 本次扫描未正常结束, finally 兜底登记重试.
                        // 旧 PEL 在 XREADGROUP > 路径下读不到, 必须靠后续 recoverPending retry 接管.
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
         * 让 XAUTOCLAIM 路径下游可以走与主消费循环完全一样的 dispatch 路径。
         * <p>
         * 反序列化用 RedisProxy 的 hashKeySerializer / hashValueSerializer, 与 publish 端 + 主消费
         * 路径 (container deserializer) 一致, 保证两条路径出来的 PooledEvtData 类型完全相同。
         * <p>
         * <b>始终返回非 null</b>: 空 body / 反序列化失败时返回带空 Map 的 MapRecord,
         * 让 dispatch 内的毒消息识别(map.get(DATA_FIELD) == null) 自然走 dropIds → ackSafe 丢弃,
         * 避免毒消息留在 PEL 永远不被 ack 而堆积。
         *
         * @param msg 见上述说明
         */
        private MapRecord<String, Object, Object> toMapRecord(StreamMessage<byte[], byte[]> msg) {
            String id = msg.getId();
            Map<byte[], byte[]> body = msg.getBody();
            if (body == null || body.isEmpty()) {
                // 空 body → 包空 Map, dispatch 端 map.get(DATA_FIELD)=null → 毒消息丢弃
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
                // 反序列化失败 → 包空 Map, 同样走 dispatch 毒消息路径丢弃
                return MapRecord.create(stream, Collections.emptyMap()).withId(RecordId.of(id));
            }
        }

        /**
         * 业务作用：毒消息丢弃链表懒初始化 + add. 大多数 batch 没毒消息, dropIds 保持 null 不分配 RecycleLinkedList.
         *
         * @param dropIds 见上述说明
         * @param id      条目标识
         * @return 见上述说明。
         */
        private static RecycleLinkedList<String> drop(RecycleLinkedList<String> dropIds, String id) {
            if (dropIds == null) dropIds = RecycleLinkedList.of();
            dropIds.add(id);
            return dropIds;
        }

        /**
         * 业务作用：主消费路径 (从 {@link #onMessage} 进来, 也供 {@link #recoverPending} XAUTOCLAIM 复用)。
         * <p>
         * 反序列化由 container 的 deserializer 完成, 这里按 (topic, event) 分桶后调 listener.onEvent。
         * <p>
         * 失败语义: handler 抛异常 → 该批次的 id 不 ACK → 留在 PEL → 等本分区下次发生节点迁移
         * (本节点崩溃 lease 过期 / rebalance 调度) 时, 接手节点的 {@link #recoverPending} XAUTOCLAIM
         * 把这些 idle &gt; minIdleMs (默认 30s) 的消息接管过来重新投递。同节点持续持锁期间这些消息
         * 不会自动重试。业务方必须保证 handler 幂等, 极端长生命周期失败建议自行落库 + 死信队列处理。
         *
         * <p>返回：无返回值；成功分桶交付的消息会确认，处理失败的消息保留在 PEL。
         *
         * @param batch 见上述说明
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        void dispatch(RecycleLinkedList batch) {
            // 与 PROXY 路径对齐: 设置当前 redisProxy, 让 listener 内部可通过 RedisProxyHolder.getRedisProxy() 拿到
            RedisProxyHolder.setRedisProxy(redisProxy);
            // topic 与 event 必须作为两个字段参与判等，分隔符拼接会让不同业务路由互相覆盖。
            RecycleLinkedMap<TopicEventKey, RecycleLinkedList<Object>> dataByTE = RecycleLinkedMap.of();
            RecycleLinkedMap<TopicEventKey, RecycleLinkedList<String>> idsByTE = RecycleLinkedMap.of();
            // 反序列化失败 / 找不到 listener → 直接 ACK 丢弃, 防止 pending 堆积
            RecycleLinkedList<String> dropIds = null;

            // traceId 必须按 (topic, event) 隔离聚合，避免无关事件共享日志上下文并造成错误归因。
            RecycleLinkedMap<TopicEventKey, HashSet<String>> traceIdsByTE = null;
            // PooledEvtData 路径 Jackson 经 RecycleModule 路由把 passthrough 池借出 RecycleLinkedMap;
            // listener 执行完后必须显式归还, 否则池借出不还稳态后退化到每次 new.
            // 这里聚合所有池借的 passthrough 实例, finally 统一 recycle (rawMap 路径产生的 LinkedHashMap 跳过).
            RecycleLinkedList<RecycleLinkedMap<String, Object>> ptToRecycle = null;
            try {
                for (Object m : batch) {
                    // m 通常是 MapRecord, 也可能是 activateDefaultTyping 路径下的原始对象
                    if (!(m instanceof Record record)) {
                        log.warn("[{}] partition unexpected message type stream={} type={}",
                                redisProxy.getQualifier(), stream, m == null ? null : m.getClass());
                        continue;
                    }
                    String id = ((Record<String, ?>) record).getId().getValue();
                    Object value = record.getValue();
                    if (!(value instanceof Map map)) {
                        dropIds = drop(dropIds, id);
                        continue;
                    }
                    Object o = map.get(DATA_FIELD);
                    // activateDefaultTyping=true → Jackson 反序列化为 PooledEvtData
                    // activateDefaultTyping=false → Jackson 反序列化为 LinkedHashMap, 需手动提取字段
                    String pmTopic, pmEvent;
                    Object pmData;
                    Map<String, Object> passthrough;
                    if (o instanceof PooledEvtData pm) {
                        pmTopic = pm.getTopic();
                        pmEvent = pm.getEvent();
                        pmData = pm.getData();
                        // PooledEvtData.restore 不再 cascade recycle passthrough, pm.recycle 后 passthrough 引用仍有效.
                        // passthrough 是 Jackson 经 RecycleModule 路由产生的池借 RecycleLinkedMap (@class type tag 命中),
                        // finally 走 ptToRecycle 统一归还.
                        passthrough = pm.getPassthrough();
                        pm.recycle();
                    } else if (o instanceof Map<?, ?> rawMap) {
                        Object ft = rawMap.get(PooledEvtData.FIELD_TOPIC);
                        Object fe = rawMap.get(PooledEvtData.FIELD_EVENT);
                        if (!(ft instanceof String) || !(fe instanceof String)) {
                            log.warn("[{}] partition unexpected data type stream={} id={} type={} msg={}",
                                    redisProxy.getQualifier(), stream, id, o.getClass(), ObjMprUtils.toString(o));
                            dropIds = drop(dropIds, id);
                            continue;
                        }
                        pmTopic = (String) ft;
                        pmEvent = (String) fe;
                        pmData = rawMap.get(PooledEvtData.FIELD_DATA);
                        passthrough = MapUtils.getObject(rawMap, PooledEvtData.FIELD_PASSTHROUGH);
                    } else {
                        log.warn("[{}] partition unexpected data type stream={} id={} type={} msg={}",
                                redisProxy.getQualifier(), stream, id, o == null ? null : o.getClass(), ObjMprUtils.toString(o));
                        dropIds = drop(dropIds, id);
                        continue;
                    }
                    // 池化 passthrough 一经取出便纳入统一回收范围，后续毒消息和无监听器分支也不能遗漏。
                    if (passthrough instanceof RecycleLinkedMap rlm) {
                        if (ptToRecycle == null) ptToRecycle = RecycleLinkedList.of();
                        ptToRecycle.add(rlm);
                    }
                    // 毒消息: topic/event 空白或 data 缺失 (recoverPending / 历史 / 外部写入可能产生) — 不投业务 listener, 直接 ACK 丢弃.
                    // publish 入口虽已禁 null data, 但 recover/历史消息绕过入口校验.
                    if (StringUtils.isBlank(pmTopic) || StringUtils.isBlank(pmEvent) || pmData == null) {
                        log.warn("[{}] partition poison message (blank topic/event or null data) stream={} id={} topic={} event={}",
                                redisProxy.getQualifier(), stream, id, pmTopic, pmEvent);
                        dropIds = drop(dropIds, id);
                        continue;
                    }
                    TopicEventKey teKey = new TopicEventKey(pmTopic, pmEvent);
                    if (log.isDebugEnabled()) {
                        log.debug("[partition-dispatch] stream={} topic={} event={} id={} data={}",
                                stream, pmTopic, pmEvent, id, ObjMprUtils.toString(pmData));
                    }
                    if (group.listenersByTopicEvent.get(teKey) == null) {
                        log.warn("[{}] no listener for topic-event={} stream={} id={}", redisProxy.getQualifier(), teKey, stream, id);
                        dropIds = drop(dropIds, id);
                        continue;
                    }

                    dataByTE.computeIfAbsent(teKey, DATA_BUCKET).add(pmData);
                    idsByTE.computeIfAbsent(teKey, ID_BUCKET).add(id);
                    // 无透传直接跳过, 避免空 set 进 holder
                    if (passthrough == null) continue;
                    // 用 pmData 对象引用做 key 存 passthrough, single 路径下逐条按 item 反查
                    RedisProxyHolder.set(pmData, passthrough);
                    String traceId = MapUtils.getString(passthrough, AnyHolder.TRACE_ID);
                    if (traceId == null) continue;
                    // 按 teKey 累积本桶 traceId, batch 路径整桶 onEvent 时拿来一行染色
                    if (traceIdsByTE == null) traceIdsByTE = RecycleLinkedMap.of();
                    traceIdsByTE.computeIfAbsent(teKey, k -> new HashSet<>()).add(traceId);
                }

                // 整张 traceIds map set 进 holder, 供 batch listener 一行染色用; finally 显式 recycle 还池
                if (traceIdsByTE != null) {
                    RedisProxyHolder.set(KEY_TRACE_IDS_BY_TE, traceIdsByTE);
                }

                // exec onEvent
                this.flush(dataByTE, idsByTE, dropIds);

            } finally {
                if (dropIds != null) dropIds.recycle();
                dataByTE.forEach((key, values) -> values.recycle());
                dataByTE.recycle();
                idsByTE.forEach((key, values) -> values.recycle());
                idsByTE.recycle();
                // listener 已执行完, 池借的 passthrough / traceIdsByTE 不再被引用, 归还到池
                // (AnyHolder.clear 只 recycle 外层 RecycleLinkedMap, 内层 LinkedHashMap 的 value 不会级联)
                if (ptToRecycle != null) {
                    for (RecycleLinkedMap<String, Object> p : ptToRecycle) p.recycle();
                    ptToRecycle.recycle();
                }
                if (traceIdsByTE != null) traceIdsByTE.recycle();
            }
        }

        /**
         * 业务作用：提交分桶后的消息: 毒消息整批 ACK 丢弃, 正常消息按 (topic, event) 调 listener 成功才 ACK。
         * <p>
         * listener 区分 batch / single 调用 (与 {@link RedisProxy#before()} 内 loadStreamSubscribe
         * 走 PROXY 路径时的语义一致):
         * <ul>
         *   <li>{@link RedisEventBatchListener}: 一次性给 List, 业务可批量处理 — 整批失败则全部留 PEL</li>
         *   <li>{@link RedisEventSingleListener}: 框架内部 for 循环逐条调 — 单条失败仅该条留 PEL,
         *       其他条照常 ACK, 防一条毒消息卡死整批</li>
         * </ul>
         * paramType 处理: activateDefaultTyping=true 时反序列化已经还原具体类型, 直接传; false 时
         * data 是 LinkedHashMap, 用 listener.paramType() 二次反序列化。
         *
         * @param dataByTE 见上述说明
         * @param idsByTE  见上述说明
         * @param dropIds  见上述说明
         *                 返回: 无返回值；各路由桶按 listener 类型提交，只有通过 fencing 且处理成功的记录才确认。
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        private void flush(RecycleLinkedMap<TopicEventKey, RecycleLinkedList<Object>> dataByTE,
                           RecycleLinkedMap<TopicEventKey, RecycleLinkedList<String>> idsByTE,
                           RecycleLinkedList<String> dropIds) {
            // 毒消息整批 ACK (即使 ack 失败也不致命, 下次 XAUTOCLAIM 还会拿到, dispatch 会再判定为毒消息丢弃)
            if (dropIds != null) this.ackSafe(dropIds, "drop");
            boolean activateDefaultTyping = redisProxy.isActivateDefaultTyping();
            // 逐 (topic, event) 调对应 listener
            for (Map.Entry<TopicEventKey, RecycleLinkedList<Object>> e : dataByTE.entrySet()) {
                TopicEventKey teKey = e.getKey();
                RecycleLinkedList<Object> data = e.getValue();
                RecycleLinkedList<String> ids = idsByTE.get(teKey);
                StreamSubscribe listener = group.listenersByTopicEvent.get(teKey);
                if (listener == null) {
                    this.ackSafe(ids, "no-listener-for-" + teKey);
                    continue;
                }
                // activateDefaultTyping=false 时 data 是裸 LinkedHashMap, 必须用 paramType 二次反序列化才能拿到具体类型
                // (与 RedisProxy.streamListener 行为对齐: hashValueSerializer 不带类信息时回退到 paramType)
                TypeReference reference = listener.paramType();
                if (reference != null && !activateDefaultTyping) {
                    ListIterator<Object> dataIt = data.listIterator();
                    ListIterator<String> idIt = ids.listIterator();
                    while (dataIt.hasNext()) {
                        Object raw = dataIt.next();
                        String correspondingId = idIt.next();
                        try {
                            Object newData = ObjMprUtils.deserialize(ObjMprUtils.toString(raw), reference);
                            // 二次反序列化把 raw 替换成 newData 实例, 同步把 holder 里以 raw 为 key 的 passthrough 迁到 newData,
                            // 否则 single 路径下 RedisProxyHolder.passthrough(item) 用新对象查不到, traceId 丢失
                            // (旧 raw key 残留无所谓, BatchStreamPollTask 派发完成后 AnyHolder.clear() 兜底)
                            Map<String, Object> pt = RedisProxyHolder.get(raw);
                            if (pt != null) RedisProxyHolder.set(newData, pt);
                            dataIt.set(newData);
                        } catch (Throwable t) {
                            log.error("[{}] partition data 2nd deserialize failed teKey={} stream={}", redisProxy.getQualifier(), teKey, stream, t);
                            // 同步移除 data 和对应 id, 保持两个 list 长度一致
                            dataIt.remove();
                            idIt.remove();
                            // 反序列化失败的消息立即 ACK 丢弃, 防 pending 堆积
                            this.ackSafe(correspondingId, "deserialize-fail");
                        }
                    }
                }
                // 二次反序列化把整组都剪光了 (全部失败已逐条 ACK): data 空则跳过 listener, 不发空批 / 不调空集合
                if (data.isEmpty()) continue;
                // batch listener: 整批处理, 成功才 ACK; 失败全部留 pending 等 XAUTOCLAIM 重投
                // single listener: 逐条处理 + 逐条 ACK, 某条失败不影响后续, 防单条毒消息卡死整批
                if (listener instanceof RedisEventBatchListener bl) {
                    // 批量处理: 取本 (topic, event) 桶累积的 traceIds 染色批日志, 一行能定位本桶所有上游 trace
                    RecycleLinkedMap<TopicEventKey, HashSet<String>> traceIdsByTE = RedisProxyHolder.get(KEY_TRACE_IDS_BY_TE);
                    HashSet<String> teTraceIds = traceIdsByTE == null ? null : traceIdsByTE.get(teKey);
                    String traceIds = teTraceIds == null ? null : String.join(",", teTraceIds);
                    // 精确设当前 teKey 的 trace: 有则设, 无则清 — 否则会继承上一个 teKey 的 trace (同批内串链路)
                    if (traceIds != null) AnyHolder.set(AnyHolder.TRACE_ID, traceIds);
                    else AnyHolder.remove(AnyHolder.TRACE_ID);
                    // 与 PROXY 路径对齐: 设置批量 recordId
                    RedisProxyHolder.setStreamRecordId(ids);
                    try {
                        bl.onEvent(data);
                    } catch (Throwable t) {
                        log.error("{} [{}] partition batch listener failed teKey={} stream={} count={} listener={}"
                                , AnyHolder.getTraceId(), redisProxy.getQualifier(), teKey, stream, ids.size(), listener.getClass().getName(), t);
                        // listener 失败留 pending 等 XAUTOCLAIM 重投, 跳过 ack 进入下一 teKey
                        continue;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("{} [partition-flush] stream={} teKey={} count={} listener={}"
                                , AnyHolder.getTraceId(), stream, teKey, ids.size(), listener.getClass().getSimpleName());
                    }
                    // fence + ack + 条件 XDEL 共用一次 fence 结果, fence 失败两边都跳, 留 PEL.
                    this.ackSafe(ids, listener.autoDelete(), teKey.toString());
                    continue;
                }
                // single listener: 逐条处理 + 逐条 ACK, 某条失败不影响后续, 防单条毒消息卡死整批
                Iterator<Object> dataIt = data.iterator();
                Iterator<String> idIt = ids.iterator();
                while (dataIt.hasNext()) {
                    Object item = dataIt.next();
                    String itemId = idIt.next();
                    // 逐条处理: 按 item 对象引用反查 holder 取本条对应上游 traceId 染色单条日志.
                    // 有则设, 无则清 — 否则会继承上一条 item 的 trace (同批内串链路)
                    String traceId = MapUtils.getString(RedisProxyHolder.get(item), AnyHolder.TRACE_ID);
                    if (traceId != null) AnyHolder.set(AnyHolder.TRACE_ID, traceId);
                    else AnyHolder.remove(AnyHolder.TRACE_ID);
                    // 与 PROXY 路径对齐: 设置单条 recordId
                    RedisProxyHolder.setStreamRecordId(itemId);
                    try {
                        listener.onEvent(item);
                    } catch (Throwable t) {
                        log.error("{} [{}] partition single listener failed teKey={} stream={} id={} listener={}"
                                , AnyHolder.getTraceId(), redisProxy.getQualifier(), teKey, stream, itemId, listener.getClass().getName(), t);
                        // 该条不 ACK, 留 pending 等 XAUTOCLAIM 重投
                        continue;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("{} [partition-flush] stream={} teKey={} id={} listener={}"
                                , AnyHolder.getTraceId(), stream, teKey, itemId, listener.getClass().getSimpleName());
                    }
                    // fence + ack + 条件 XDEL 共用一次 fence 结果.
                    this.ackSafe(itemId, listener.autoDelete(), teKey.toString());
                }
            }
        }


        /**
         * 业务作用：ACK / XDEL 前的 fencing 校验 (二态): 任何非"持有"都拒 ACK, 消息留 PEL.
         * <p>
         * ackSafe 不需要区分"真锁丢"和"Redis 异常" — 都按 fence 失败处理留 PEL.
         * 真锁丢由主循环 holds 自检触发 lockLost; 网络异常 ack 重试由新 owner XAUTOCLAIM 接管.
         *
         * @return true = 持有可 ACK / XDEL; false = 必须跳过
         */
        private boolean fencedOk() {
            String h = lockHolder;
            if (h == null) return false;
            Long r = distributedLock.holdsStatus(lockKey, h);
            if (r != null && r == 1L) return true;
            // r == 0: 锁真丢 (已被新 owner 接管/过期). fencedOk 已证锁不在自己手里, 主动设 lockLost,
            // 让 runner 下个 tick 立即停止拉新消息, 缩短"旧 owner 继续消费产生重复业务副作用"的窗口
            // (否则要等下次 5s holds 自检才发现). lockLost volatile, businessExecutor 写 runner 读可见.
            // r == null: Redis 瞬时异常, 只跳过本次 ACK, 不污染 lockLost (避免抖动误停).
            if (r != null && r == 0L) lockLost = true;
            return false;
        }

        /**
         * 业务作用：提交 recoverPending 异步任务到 businessExecutor.
         * <p>
         * 入口设 recovering=true(让 isPollReady=false, runner 暂停 batch poll, 防 PEL 与新消息并发 dispatch).
         * 异步任务体由 {@link #doRecoverPendingAsync()} 承接, finally 内清 recovering=false. doRecoverPendingAsync
         * 内 recoverPending 自身负责在网络异常 / xAutoClaim 失败 / maxLoops 耗尽时设 recoverPendingNeeded=true.
         * <p>
         * <b>catch Throwable 而非 RejectedExecutionException</b>: 调用本方法时锁已经被 beforeStart 成功获取
         * (lock / lockHolder 已设, realLockCount 已 incrementAndGet). 一旦 executor.execute
         * 抛<b>任何</b>异常 (包装 executor / 任务装饰器 / 关闭态适配器 / native-image 实现可能抛非 Rejected 的 Throwable),
         * 异常如果冒泡到 beforeStart 外层 catch return false, ManagedRunner 把 false 当 zombie retry, 不调 afterExit,
         * 持锁状态完整保留 → 下一轮 retry 再进 beforeStart, 同线程 holder 会重入并覆盖当前 Lock 引用，
         * 本地持有深度与退出时的单次 unlock 不再对称。必须在本方法内部处理提交异常，不让它逃出.
         * <p>
         * catch Throwable 后: 回收 ar + 保留 recoverPendingNeeded=true 让 beforePoll 周期重试 +
         * 清 recovering=false 让主消费恢复拉新消息. 锁仍在本节点, 旧 PEL 等下次 retry 接管.
         * <p>
         * 用 {@link ActionRecycler} 池化 lambda 实例, 与 RedisProxy 内其它 executor.execute 路径风格统一.
         */
        private void submitRecoverPending() {
            this.recovering = true;
            // 进入 recover 前清标志, recoverPending 内部如果网络异常 / 失败会再 set true 触发 retry.
            // 提交前清: 提交失败 catch 会重设 true.
            this.recoverPendingNeeded = false;
            ActionRecycler ar = ActionRecycler.ofRecycle(RECOVER_PENDING_CON).ref(0, this);
            try {
                redisProxy.getExecutor().execute(ar);
            } catch (Throwable t) {
                // 在锁已获取的前提下, 任何 executor 异常都必须本地处理 — 详见方法 API docs.
                // 不能让异常冒泡导致 beforeStart 返回 false 触发 zombie retry, 否则锁泄漏 + 重入覆盖旧 lock.
                ar.recycle();
                log.warn("[{}] partition recoverPending submit failed, will retry in beforePoll streamPrefix={} partition={}",
                        redisProxy.getQualifier(), group.streamPrefix, partition, t);
                this.recoverPendingNeeded = true;
                this.recovering = false;
            }
        }

        /**
         * 业务作用：recoverPending 异步任务的实际执行体, 由 {@link #RECOVER_PENDING_CON} 调用 (在 businessExecutor 线程内).
         * 与正常 handleBatchResult 路径对称: recoverPending → dispatch 写了 trace/passthrough/recordId (均基于 AnyHolder),
         * 此路径不经 BatchStreamPollTask 的 finally, 必须就地清, 否则 businessExecutor 复用线程会串上一批 holder/trace + 持已回收 passthrough 引用.
         */
        void doRecoverPendingAsync() {
            try {
                this.recoverPending();
            } catch (Throwable t) {
                log.error("[{}] partition recoverPending async failed streamPrefix={} partition={}",
                        redisProxy.getQualifier(), group.streamPrefix, partition, t);
            } finally {
                AnyHolder.clear();
                this.recovering = false;
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
            if (l instanceof LettuceDistributedLock.RedisLock rl && h != null) {
                try {
                    rl.disposeLocal(h);
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
         * 业务作用：安全 ack: ack 失败不抛, 只记日志带上 source 便于排障。
         * source 区分多种丢弃路径 (drop / no-handler / topic 名), 让日志能定位是哪批消息没 ack。
         * <p>
         * 旧签名转发到 {@link #ackSafe(RecycleLinkedList, boolean, String)} 传 autoDelete=false,
         * 用于丢弃路径 (毒消息 / 无 listener / 反序列化失败).
         *
         * @param ids    条目标识集合
         * @param source 见上述说明
         */
        private void ackSafe(RecycleLinkedList<String> ids, String source) {
            this.ackSafe(ids, false, source);
        }

        /**
         * 业务作用：单条 ack 重载, 供 single listener 逐条确认.
         * 旧签名转发到 {@link #ackSafe(String, boolean, String)} 传 autoDelete=false.
         *
         * @param id     条目标识
         * @param source 见上述说明
         */
        private void ackSafe(String id, String source) {
            this.ackSafe(id, false, source);
        }

        /**
         * 业务作用：fence + ack + 条件 XDEL 三合一. fence 失败时 ACK 和 XDEL 都跳, 消息留 PEL 给新 owner 重投.
         * <p>
         * <b>共用同一次 fence 结果</b>: 防止"ACK 被拦截但 XDEL 仍执行"导致 stream record 物理删除
         * 但 PEL 仍标记 pending 的不一致状态.
         * <p>
         * <b>ACK 成功才 XDEL</b>: 用 acked 标志显式跟踪 ack 是否真正成功. ack 抛异常 → acked=false → 跳过 xDel,
         * 否则 PEL 仍有 pending 但 stream record 被物理删除, 接管方只能看到 deleted entry, 可靠重投语义破坏.
         *
         * @param ids        条目标识集合
         * @param autoDelete 见上述说明
         * @param source     见上述说明
         */
        private void ackSafe(RecycleLinkedList<String> ids, boolean autoDelete, String source) {
            if (ids == null || ids.isEmpty()) return;
            if (!this.fencedOk()) {
                log.warn("[{}] partition ack fence rejected source={} stream={} count={} lockKey={}",
                        redisProxy.getQualifier(), source, stream, ids.size(), lockKey);
                return;
            }
            boolean acked = false;
            try {
                redisProxy.ack(stream, group.streamPrefix, ids.toArray(new String[0]));
                acked = true;
            } catch (Throwable t) {
                log.error("[{}] partition ack failed source={} stream={} count={}",
                        redisProxy.getQualifier(), source, stream, ids.size(), t);
            }
            if (!acked || !autoDelete) return;
            for (String id : ids) redisProxy.xDelAsync(stream, id);
        }

        /**
         * 业务作用：单条 fence + ack + 条件 XDEL 三合一, 供 single listener 路径调用. acked 含义同上.
         *
         * @param id         条目标识
         * @param autoDelete 见上述说明
         * @param source     见上述说明
         */
        private void ackSafe(String id, boolean autoDelete, String source) {
            if (!this.fencedOk()) {
                log.warn("[{}] partition ack fence rejected source={} stream={} id={} lockKey={}",
                        redisProxy.getQualifier(), source, stream, id, lockKey);
                return;
            }
            boolean acked = false;
            try {
                redisProxy.ack(stream, group.streamPrefix, id);
                acked = true;
            } catch (Throwable t) {
                log.error("[{}] partition ack failed source={} stream={} id={}",
                        redisProxy.getQualifier(), source, stream, id, t);
            }
            if (!acked || !autoDelete) return;
            redisProxy.xDelAsync(stream, id);
        }

    }

}
