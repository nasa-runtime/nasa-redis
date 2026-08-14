package io.github.nasaruntime.redis.cache.redis;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.nasaruntime.core.base.*;
import io.github.nasaruntime.redis.cache.redis.search.RediSearch;
import io.github.nasaruntime.redis.cache.redis.search.RedisCallbackRecycler;
import io.github.nasaruntime.redis.cache.redis.stream.BatchStreamListener;
import io.github.nasaruntime.redis.cache.redis.stream.BatchStreamMessageListenerContainer;
import io.github.nasaruntime.core.concurrent.ConcurrentHashSet;
import io.github.nasaruntime.core.config.Graceful;
import io.github.nasaruntime.core.evt.PooledEvtData;
import io.github.nasaruntime.core.exception.FileException;
import io.github.nasaruntime.core.function.Action;
import io.github.nasaruntime.core.function.ActionRecycler;
import io.github.nasaruntime.core.function.FactorConsumer2;
import io.github.nasaruntime.core.function.FunctionRecycler;
import io.github.nasaruntime.core.socket.cloud.EventMessage;
import io.github.nasaruntime.core.utils.*;
import io.lettuce.core.*;
import io.lettuce.core.models.stream.ClaimedMessages;
import lombok.Getter;
import lombok.Setter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConverters;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.*;
import org.springframework.util.ClassUtils;
import org.springframework.util.ErrorHandler;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Nasa
 * redis api proxy
 */
@SuppressWarnings("all")
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE + 5)
@ConfigurationProperties(prefix = "nasa.redis-proxy")
public class RedisProxy extends OPS implements Initialization, DisposableBean {

    static final Map<String, RedisProxy> CACHE = new ConcurrentHashMap<>();
    public static final String PRIMARY = "primary";
    private static final Graceful.Shutdown SHUTDOWN_ALL = RedisProxy::shutdownAll;

    static {
        Graceful.registry(Integer.MAX_VALUE - 5000, SHUTDOWN_ALL);
    }

    /**
     * stream event 异步线程池在飞任务计数 (全局共享, 所有 RedisProxy / listener / consumer 共用)
     * 与 NasaLettuceConfig.Stream.eventExecutorInflightMax 配合实现背压
     */
    static final AtomicInteger STREAM_EVENT_INFLIGHT = new AtomicInteger(0);

    /**
     * 业务作用：列出已登记的全部 Redis 实例名，供多实例部署下遍历处理。
     *
     * <p>参数说明: 无。
     *
     * @return 实例名数组。
     */
    public static String[] qualifiers() {
        return ColUtils.toArray(CACHE.keySet());
    }

    /**
     * 业务作用：按实例名取命令代理。
     *
     * @param qualifier 实例名
     * @return 该实例的命令代理；未登记时为 null。
     */
    public static RedisProxy load(String qualifier) {
        return CACHE.get(qualifierFormat(qualifier));
    }

    /**
     * 业务作用：取默认实例的命令代理，供单实例部署直接使用。
     *
     * <p>参数说明: 无。
     *
     * @return 默认实例的命令代理。
     */
    public static RedisProxy load() {
        return load(PRIMARY);
    }

    /**
     * 业务作用：归一实例名的书写形式，使配置里的大小写与空白差异不影响匹配。
     *
     * @param qualifier 原始实例名
     * @return 归一后的实例名。
     */
    static String qualifierFormat(String qualifier) {
        qualifier = Objects.requireNonNull(qualifier, "qualifier must not be null").trim();
        if (qualifier.isEmpty()) {
            throw new IllegalArgumentException("qualifier must not be blank");
        }
        if (PRIMARY.equalsIgnoreCase(qualifier) || "redisProxy".equalsIgnoreCase(qualifier)) {
            qualifier = "redisProxy";
        } else {
            String suffix = "RedisProxy";
            if (qualifier.length() >= suffix.length()
                    && qualifier.regionMatches(true, qualifier.length() - suffix.length(), suffix, 0, suffix.length())) {
                qualifier = qualifier.substring(0, qualifier.length() - suffix.length()) + suffix;
            } else {
                qualifier = qualifier + suffix;
            }
        }
        return qualifier;
    }

    /**
     * 业务作用：按操作模板反查其对应的命令代理。
     *
     * @param redisTemplate 操作模板
     * @return 对应的命令代理；未登记时为 null。
     */
    public static RedisProxy load(RedisTemplate<String, Object> redisTemplate) {
        for (Map.Entry<String, RedisProxy> entry : CACHE.entrySet()) {
            RedisProxy redisProxy = entry.getValue();
            if (redisProxy.redisTemplate == redisTemplate) return redisProxy;
        }
        return null;
    }


    /**
     * pipeline 命令缓冲队列 (单 MPSC CLQ).
     * <p>
     * N 个业务线程并发 offer (lock-free CAS), 单消费者 (TimingWheel 信号线程, 由 pipelineLock 互斥)
     * 用 poll() 排干；CLQ 的 offer 与 poll 可在生产者和单消费者之间安全并发。
     */
    final ConcurrentLinkedQueue<PipelineTask> cqueue = new ConcurrentLinkedQueue<>();


    /* RedisProxy 在framework容器里的名称 */
    @Getter
    private final String qualifier;
    @Getter
    private final RedisTemplate<String, Object> redisTemplate;
    @Getter
    private final RedisSerializer<String> keySerializer;
    @Getter
    private final RedisSerializer<Object> valueSerializer;
    @Getter
    private final HashOperations<String, String, Object> hashOperations;
    @Getter
    private final RedisSerializer<String> hashKeySerializer;
    @Getter
    private final RedisSerializer<Object> hashValueSerializer;
    private final ConcurrentMap<String, String> scriptShaCache = new ConcurrentHashMap<>();
    private final RedisIdempotentCounter idempotentCounter;
    private final Lock executorLock = new ReentrantLock();
    private volatile Executor taskExecutor;
    private volatile boolean propertiesInitialized;
    private final String pipelineTaskName;
    private final String streamTrimTaskName;
    private final String streamDeleteTaskName;
    private final Lock pipelineDrainLock = new ReentrantLock();
    private final AtomicBoolean destroyed = new AtomicBoolean();

    private static final BiFunction<String, FunctionRecycler<String, String>, String> SCRIPT_SHA_LOADER =
            (script, fn) -> {
                RedisProxy proxy = fn.ref(0);
                return proxy.loadScriptSha(script);
            };

    private static final BiFunction<RedisConnection, RedisCallbackRecycler<String>, String> SCRIPT_LOAD_CB =
            (connection, cb) -> {
                byte[] script = cb.ref(0);
                return connection.commands().scriptLoad(script);
            };

    private static final BiFunction<RedisConnection, RedisCallbackRecycler<Object>, Object> EVAL_DIRECT_CB =
            (connection, cb) -> {
                byte[] script = cb.ref(0);
                ReturnType returnType = cb.ref(1);
                byte[][] kas = cb.ref(2);
                return connection.commands().eval(script, returnType, cb.intVal(0), kas);
            };

    private static final BiFunction<RedisConnection, RedisCallbackRecycler<Object>, Object> EVALSHA_DIRECT_CB =
            (connection, cb) -> {
                String sha1 = cb.ref(0);
                ReturnType returnType = cb.ref(1);
                byte[][] kas = cb.ref(2);
                return connection.commands().evalSha(sha1, returnType, cb.intVal(0), kas);
            };

    private static final BiFunction<RedisConnection, RedisCallbackRecycler<Object>, Object> SCRIPT_FLUSH_CB =
            (connection, cb) -> {
                connection.commands().scriptFlush();
                return null;
            };

    private static final BiFunction<RedisConnection, RedisCallbackRecycler<Object>, Object> SCRIPT_KILL_CB =
            (connection, cb) -> {
                connection.commands().scriptKill();
                return null;
            };

    private static final BiFunction<RedisConnection, RedisCallbackRecycler<List<Boolean>>, List<Boolean>> SCRIPT_EXISTS_CB =
            (connection, cb) -> {
                String[] scripts = cb.ref(0);
                return connection.commands().scriptExists(scripts);
            };
    /* redis stream 相关配置 */
    @Getter
    private final NasaLettuceConfig.Stream stream = new NasaLettuceConfig.Stream();
    /* pipeline 专用连接池 (Semaphore + ConcurrentLinkedQueue, 虚拟线程友好) */
    @Getter
    @Setter
    private PipelineConnectionPool pipelinePool;

    /**
     * 业务作用：建出一个实例的命令代理，绑定其实例名与操作模板。
     * 实例名是多实例部署下区分归属的唯一依据，也是各类订阅按实例过滤的依据。
     *
     * @param qualifier     实例名
     * @param redisTemplate 操作模板
     */
    public RedisProxy(String qualifier, RedisTemplate<String, Object> redisTemplate) {
        this.qualifier = qualifierFormat(qualifier);
        this.pipelineTaskName = "redis-proxy-pipeline-" + this.qualifier;
        this.streamTrimTaskName = "redis-proxy-stream-trim-" + this.qualifier;
        this.streamDeleteTaskName = "redis-proxy-stream-delete-" + this.qualifier;
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
        this.keySerializer = (RedisSerializer<String>) redisTemplate.getKeySerializer();
        this.valueSerializer = (RedisSerializer<Object>) redisTemplate.getValueSerializer();
        this.hashOperations = redisTemplate.opsForHash();
        this.hashKeySerializer = (RedisSerializer<String>) redisTemplate.getHashKeySerializer();
        this.hashValueSerializer = (RedisSerializer<Object>) redisTemplate.getHashValueSerializer();
        this.idempotentCounter = new RedisIdempotentCounter(this, this::evalIdempotentCounterScript);
        RedisProxy previous = CACHE.putIfAbsent(this.qualifier, this);
        if (previous != null) {
            throw new IllegalStateException("RedisProxy qualifier already registered: " + this.qualifier);
        }
        try {
            LettucePipeline.initialize(this);
            RediSearch.initialize(this);
        } catch (RuntimeException | Error e) {
            CACHE.remove(this.qualifier, this);
            LettucePipeline.destroy(this);
            RediSearch.destroy(this);
            throw e;
        }

    }

    /**
     * 业务作用：销毁当前命令代理，先停止新增后台动作并排干已接收批次，再撤销全部进程级登记。
     * 同一实例可能同时经过 Spring 容器关闭和 JVM 优雅停机，本方法保证清理只执行一次。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；单项清理失败会记录并继续释放其它资源。
     */
    @Override
    public void destroy() {
        if (!destroyed.compareAndSet(false, true)) return;

        // 先关闭会产生 Redis 外部副作用的周期入口，避免排干期间又有新批次进入。
        try {
            TimingWheel.cancel(this.pipelineTaskName);
            TimingWheel.cancel(this.streamTrimTaskName);
            TimingWheel.cancel(this.streamDeleteTaskName);
        } catch (Throwable error) {
            log.warn("[{}] background task cancellation failed", this.qualifier, error);
        }

        // 连接仍可用时完成已接收批次；同步调用的 future 不能因上下文关闭而永久等待。
        try {
            while (!this.cqueue.isEmpty()) this.drainPipelineQueue(true);
        } catch (Throwable error) {
            log.warn("[{}] pipeline drain during shutdown failed", this.qualifier, error);
        }

        // 只关闭本代理创建的共享容器；外部注入的容器仍由所属 Spring 上下文管理。
        if (ownsRedisMessageListenerContainer && redisMessageListenerContainer != null
                && redisMessageListenerContainer.isRunning()) {
            try {
                redisMessageListenerContainer.stop();
            } catch (Throwable error) {
                log.warn("[{}] pub/sub container stop failed", this.qualifier, error);
            }
        }
        if (ownsStreamMessageListenerContainer && streamMessageListenerContainer != null
                && streamMessageListenerContainer.isRunning()) {
            try {
                streamMessageListenerContainer.stop();
            } catch (Throwable error) {
                log.warn("[{}] stream container stop failed", this.qualifier, error);
            }
        }
        dedicatedContainers.values().forEach(container -> {
            try {
                container.stop();
            } catch (Throwable error) {
                log.warn("[{}] dedicated stream container stop failed", this.qualifier, error);
            }
        });
        dedicatedContainers.clear();

        // 只有资源入口停止后才能开放相同 qualifier 的新上下文，避免新旧代理短暂并存并共享静态门面。
        try {
            removeSafely(null);
        } catch (Throwable error) {
            log.warn("[{}] OPS registration removal failed", this.qualifier, error);
        } finally {
            CACHE.remove(this.qualifier, this);
            try {
                LettucePipeline.destroy(this);
            } catch (Throwable error) {
                log.warn("[{}] pipeline registration removal failed", this.qualifier, error);
            }
            try {
                RediSearch.destroy(this);
            } catch (Throwable error) {
                log.warn("[{}] RediSearch registration removal failed", this.qualifier, error);
            }
        }
    }

    /**
     * 业务作用：在 JVM 优雅停机阶段销毁当时仍存活的全部代理，不保留已结束 Spring 上下文的实例引用。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；各实例内部独立保证清理幂等。
     */
    private static void shutdownAll() {
        for (RedisProxy redisProxy : List.copyOf(CACHE.values())) redisProxy.destroy();
    }

    /**
     * 业务作用：按配置的序列化方式把键转成字节，供需要直接拼装命令的调用方使用。
     *
     * @param key 待序列化的键
     * @return 序列化后的字节。
     */
    public byte[] serializeKey(String key) {
        return this.keySerializer.serialize(key);
    }

    /**
     * 业务作用：按配置的序列化方式把值转成字节，供需要直接拼装命令的调用方使用。
     *
     * @param value 待序列化的值
     * @return 序列化后的字节。
     */
    public byte[] serializeValue(Object value) {
        return this.valueSerializer.serialize(value);
    }

    /**
     * 业务作用：按配置的序列化方式把哈希字段名转成字节，供需要直接拼装命令的调用方使用。
     *
     * @param hashKey 待序列化的哈希字段名
     * @return 序列化后的字节。
     */
    public byte[] serializeHKey(String hashKey) {
        return this.hashKeySerializer.serialize(hashKey);
    }

    /**
     * 业务作用：按配置的序列化方式把哈希字段值转成字节，供需要直接拼装命令的调用方使用。
     *
     * @param value 待序列化的哈希字段值
     * @return 序列化后的字节。
     */
    public byte[] serializeHValue(Object value) {
        return this.hashValueSerializer.serialize(value);
    }

    /**
     * 业务作用：value 序列化 helper: byte[] 直通, 非 byte[] 才走 serializer (与 LettucePipeline.Actuator serVal/serHVal 语义一致).
     * 业务方传已序列化好的 raw bytes 时, 不再二次编码, 保证 RedisProxy 与 LettucePipeline 写入字节一致.
     *
     * @param val 见上述说明
     * @return 见上述说明。
     */
    private byte[] serVal(Object val) {
        return val instanceof byte[] bs ? bs : valueSerializer.serialize(val);
    }

    /**
     * 业务作用：序列化哈希字段值的内部快捷入口。
     *
     * @param val 待序列化的值
     * @return 序列化后的字节。
     */
    private byte[] serHVal(Object val) {
        return val instanceof byte[] bs ? bs : hashValueSerializer.serialize(val);
    }

    /**
     * (stream, group) → 独立 container 缓存。
     * <p>
     * 仅当 yml 给该 (stream, group) 显式配了 batchSize / pollTimeout 覆盖时才会有条目 —
     * 没配覆盖的 (stream, group) 都共用 {@link #streamMessageListenerContainer} 全局共享 container。
     * <p>
     * key = {@code stream + "/" + group}, 与 {@link #streamSubscriptionMap} 的 key 约定一致。
     */
    @SuppressWarnings("rawtypes")
    private final Map<String, StreamMessageListenerContainer> dedicatedContainers = new ConcurrentHashMap<>();

    /* 让redis序列化后的json带上类信息 */
    @Getter
    private boolean activateDefaultTyping = true;
    /* 并发数低于这个值，走RedisTemplate原生API，否则走pipeline */
    @Setter
    private int lower = 50;
    /* 并发数采集时间窗口大小，ms */
    private int lowerMillis = 50;

    /**
     * 业务作用：声明本代理所绑定序列化器是否携带默认类型信息。
     * 序列化器在连接模板创建时已经定型，初始化完成后改变此标志会让消费端按错误格式解码，因此拒绝变更。
     *
     * @param activateDefaultTyping 是否携带默认类型信息
     * 返回: 无返回值；初始化完成后的不同取值会被拒绝。
     */
    public void setActivateDefaultTyping(boolean activateDefaultTyping) {
        if (propertiesInitialized && this.activateDefaultTyping != activateDefaultTyping) {
            throw new IllegalStateException("activateDefaultTyping cannot change after RedisProxy initialization");
        }
        this.activateDefaultTyping = activateDefaultTyping;
    }

    /**
     * 业务作用：设置并发采样窗口，窗口必须为正数才能保证采样任务可调度。
     *
     * @param lowerMillis 采样窗口毫秒数
     * 返回: 无返回值；非正数会被拒绝。
     */
    public void setLowerMillis(int lowerMillis) {
        if (lowerMillis < 1) throw new IllegalArgumentException("lowerMillis must be greater than zero");
        this.lowerMillis = lowerMillis;
    }

    /**
     * 业务作用：为当前 Redis 实例装入 nonce 幂等计数布局，并在布局启用后拒绝不一致的运行期变更。
     *
     * @param properties 全局幂等计数配置
     * 返回: 无返回值；配置与已启用布局不一致时拒绝。
     */
    void configureIdempotentCounter(RedisIdempotentCounterProperties properties) {
        this.idempotentCounter.configure(properties);
    }

    /**
     * 业务作用：取得 nonce 幂等计数的本地观测快照入口，用于桥接 Micrometer、Prometheus 或应用健康检查。
     *
     * <p>参数说明: 无。
     *
     * @return 当前 RedisProxy 长期复用的线程安全指标容器。
     */
    public RedisIdempotentCounterMetrics idempotentCounterMetrics() {
        return this.idempotentCounter.metrics();
    }
    /**
     * 业务作用：把一个数据源的配置复制到本代理，供初始装配及后续订阅读取。
     * 已创建的连接、监听容器和周期任务不会在这里重建；它们的建造参数变化只影响后续新建实例或订阅。
     * 序列化方式在模板创建时已经固化，初始化完成后不允许改变其类型信息标志。
     *
     * @param p 新的配置
     * 返回: 无返回值。
     */
    public void refreshProperties(NasaLettuceConfig.RedisProperties p) {
        Objects.requireNonNull(p, "redis properties must not be null");
        NasaLettuceConfig.Stream sourceStream = Objects.requireNonNull(p.getStream(), "stream properties must not be null");
        if (p.getLowerMillis() < 1) {
            throw new IllegalArgumentException("lowerMillis must be greater than zero");
        }
        if (sourceStream.getPollTimeout() < 1 || sourceStream.getBatchSize() < 1
                || sourceStream.getAsyncDelRecordPeriod() < 1) {
            throw new IllegalArgumentException("stream pollTimeout, batchSize and asyncDelRecordPeriod must be greater than zero");
        }
        if (sourceStream.isAutoTrimEnabled()
                && (sourceStream.getAutoTrimRate() < 1 || sourceStream.getDataExpireMillis() < 1)) {
            throw new IllegalArgumentException("autoTrimRate and dataExpireMillis must be greater than zero when auto trim is enabled");
        }
        if (sourceStream.getEventExecutorInflightMax() < 1) {
            throw new IllegalArgumentException("eventExecutorInflightMax must be greater than zero");
        }
        sourceStream.getGroup().forEach((streamName, groups) -> groups.forEach((groupName, group) -> {
            if (group.getBatchSize() != null && group.getBatchSize() < 1) {
                throw new IllegalArgumentException("stream group batchSize must be greater than zero: "
                        + streamName + "/" + groupName);
            }
            if (group.getPollTimeout() != null && group.getPollTimeout() < 1) {
                throw new IllegalArgumentException("stream group pollTimeout must be greater than zero: "
                        + streamName + "/" + groupName);
            }
        }));
        this.setActivateDefaultTyping(p.isActivateDefaultTyping());
        this.lower = p.getLower();
        this.setLowerMillis(p.getLowerMillis());
        this.stream.setPollTimeout(sourceStream.getPollTimeout());
        this.stream.setBatchSize(sourceStream.getBatchSize());
        this.stream.setAsyncDelRecordPeriod(sourceStream.getAsyncDelRecordPeriod());
        this.stream.setAutoTrimEnabled(sourceStream.isAutoTrimEnabled());
        this.stream.setAutoTrimToTopic(sourceStream.getAutoTrimToTopic());
        this.stream.setAutoTrimRate(sourceStream.getAutoTrimRate());
        this.stream.setDataExpireMillis(sourceStream.getDataExpireMillis());
        this.stream.getAutoTrimExcludes().clear();
        this.stream.getAutoTrimExcludes().addAll(sourceStream.getAutoTrimExcludes());
        this.stream.getGroup().clear();
        this.stream.getGroup().putAll(sourceStream.getGroup());
        this.stream.setEventExecutorInflightMax(sourceStream.getEventExecutorInflightMax());
        this.stream.setNonGroupExecutorEnable(sourceStream.isNonGroupExecutorEnable());
        // 集合成员按当前配置整体替换，避免已删除的排除项或分区组继续生效。
        NasaLettuceConfig.Partition partFrom = sourceStream.getPartition();
        NasaLettuceConfig.Partition partTo = this.stream.getPartition();
        partTo.setEnabled(partFrom.isEnabled());
        partTo.setDefaultGroup(partFrom.getDefaultGroup());
        partTo.setCount(partFrom.getCount());
        partTo.setRebalanceMs(partFrom.getRebalanceMs());
        partTo.setMinIdleMs(partFrom.getMinIdleMs());
        partTo.setHoldsCheckIntervalMs(partFrom.getHoldsCheckIntervalMs());
        partTo.getGroups().clear();
        partTo.getGroups().putAll(partFrom.getGroups());
        // pipeline 专用连接池
        if (p.getPipelinePool() != null) {
            this.pipelinePool = p.getPipelinePool();
        }
        this.propertiesInitialized = true;
    }

    /**
     * 业务作用：启动早期完成本代理的自检与基础设施准备：探测服务端版本、预热连接、载入脚本。
     * <p>
     * 版本探测决定后续走原生接口还是批次路径——低版本缺少批次所需的命令，
     * 不探测会在首次使用时才失败。
     * <p>
     * 脚本在此预先载入并缓存其摘要，使运行期按摘要执行时不必每次重传脚本内容。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void before() {
        super.startSafely(this.lowerMillis);
        if (!TimingWheel.isStarted()) TimingWheel.startTimingWheel();
        // 时间轮只负责发出排干信号，实例锁保证同一代理任一时刻只有一个消费者操作批次状态。
        TimingWheel.exec(1, 1, this.pipelineTaskName, () -> this.drainPipelineQueue(false));

    }

    /**
     * 业务作用：在单消费者约束下排干一段 pipeline 队列，并把每条命令的完成结果传回等待线程。
     * 周期任务使用非阻塞加锁避免信号线程堆积；停机使用阻塞加锁，确保连接关闭前处理完已接收命令。
     *
     * @param waitForLock 是否等待正在执行的排干任务释放控制权
     * 返回: 无返回值；单次最多处理一个批次上限，剩余命令由下一轮继续处理。
     */
    private void drainPipelineQueue(boolean waitForLock) {
        if (waitForLock) pipelineDrainLock.lock();
        else if (!pipelineDrainLock.tryLock()) return;
        LettucePipeline.Actuator actuator = null;
        try {
            final int drainLimit = LettucePipeline.PIPELINE_LENGTH;
            int drained = 0;
            PipelineTask task;
            while (drained < drainLimit && (task = cqueue.poll()) != null) {
                try {
                    if (actuator == null) actuator = LettucePipeline.open(this, null);
                    actuator.drainTask(task);
                } catch (Throwable e) {
                    // 单条命令失败不能阻断后续命令；先完成其 future，避免调用线程永久等待。
                    LettuceFuture<?> future = task.lf;
                    if (future != null) future.completeExceptionally(e);
                    log.error("[pipeline] drain task failed op={}", task.op, e);
                } finally {
                    task.recycle();
                }
                drained++;
            }
            if (actuator != null) actuator.pipeline();
        } finally {
            // 即使发送阶段失败也清理本线程的批次会话，避免残留命令混入下一轮。
            if (actuator != null) {
                try {
                    actuator.pipelineForce();
                } catch (Throwable e) {
                    log.error("[pipeline] pipelineForce failed", e);
                }
            }
            pipelineDrainLock.unlock();
        }
    }

    /**
     * 业务作用：容器就绪后接线各类订阅：先整体扫描收集分区消费的主题全集并建组，再注册各订阅开始消费。
     * <p>
     * 两趟的先后不可颠倒：分区 stream 与消费组必须在任何订阅开始读取之前就位，
     * 边扫边建会让先注册的订阅在后续主题的组还没建好时就开始读取而报错。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void initialize() {
        // 先整体扫描收集 PARTITION 模式 listener 的 topic 全集，再建组。
        // 必须先收齐才能建：分区 stream 与 consumer group 要在任何 listener 开始消费之前就位，
        // 边扫边建会让先注册的 listener 在后续 topic 的组还没建好时就开始 XREADGROUP，读到 NOGROUP。
        Collection<Object> values = ContextUtils.getBeansOfType(Object.class).values();
        Set<String> partitionTopics = new HashSet<>();
        for (Object o : values) {
            if (o instanceof StreamSubscribe ss) {
                ConsumeMode m = ss.mode();
                if (m == ConsumeMode.PARTITION || m == ConsumeMode.BOTH) {
                    // qualifier 过滤: 只有归本 RedisProxy 服务的 listener 才算
                    String[] qfr = ss.qualifiers();
                    if (qfr == null || !ColUtils.notContains(qfr, RedisProxy::qualifierFormat, this.qualifier)) {
                        String[] topics = ss.topics();
                        if (topics != null) Collections.addAll(partitionTopics, topics);
                    }
                }
            }
        }

        // === partition 自动初始化 (prepare 阶段) ===
        // 仅在有 PARTITION 模式的 listener 时才初始化, 按 listener 实际 topic 按需创建组,
        // 避免无分区消费需求的服务白白创建 64 个分区 stream + consumer group。
        if (!partitionTopics.isEmpty()) {
            this.autoPreparePartitions(partitionTopics);
        }

        // 分区已就位，此时才注册 listener 并开始消费。
        for (Object o : values) {
            // 加载 channel 接口
            if (o instanceof Subscribe s) {
                String[] qfr = s.qualifier();
                if (Objects.nonNull(qfr) && ColUtils.notContains(qfr, RedisProxy::qualifierFormat, this.qualifier)) {
                    continue;
                }
                for (String channel : s.channels()) {
                    if (StringUtils.isBlank(channel)) {
                        throw new NullPointerException("channel must not be null or empty");
                    }
                    this.subscribe(channel, (Consumer) s::consume);
                }
                continue;
            }
            // === 加载 stream 订阅接口 ===
            // StreamSubscribe<T,TS> 是 redis stream 订阅根接口, Batch / Single 都继承它,
            // 所以一个 instanceof 就覆盖所有 stream listener bean。
            if (o instanceof StreamSubscribe ss) {
                this.loadStreamSubscribe(ss);
                continue;
            }

            // 加载 channel 注解 (stream 端只通过 StreamSubscribe 接口扫描, 不再有方法注解)
            List<Method> methods = ReflectUtils.allMethod(AopUtils.getTargetClass(o));
            methods.forEach(method -> {
                Subscriber subscriber = method.getAnnotation(Subscriber.class);
                if (Objects.isNull(subscriber)) return;
                String[] qfr = subscriber.qualifier();
                if (Objects.nonNull(qfr) && ColUtils.notContains(qfr, RedisProxy::qualifierFormat, this.qualifier)) {
                    return;
                }
                for (String channel : subscriber.value()) {
                    if (StringUtils.isBlank(channel)) {
                        throw new NullPointerException("channel must not be null or empty");
                    }
                    this.subscribe(channel, (Consumer) t -> ReflectUtils.invoke(o, method, t));
                }
            });
        }

        // === partition 启动消费阶段 ===
        // listener 已全部 registerListener 完成, 统一启 tryClaim 拉消息。
        // 仅在有 PARTITION listener 时启动 (与上面 autoPreparePartitions 对称)。
        if (!partitionTopics.isEmpty() && this.stream.getPartition().isEnabled()) {
            RedisPartition.load(this).startAllGroups();
        }
    }

    /**
     * 业务作用：按 listener 实际 topic 按需 prepare PartitionGroup (创建 stream/group/container, 不 tryClaim)。
     * <p>
     * 行为:
     * <ul>
     *   <li>partition.enabled=false → noop</li>
     *   <li>遍历 yml partition.groups: 仅当 listener 的 topic 命中该组时才 isolate</li>
     *   <li>有 listener topic 不属于任何隔离组 → init() 创建默认共享组</li>
     * </ul>
     * tryClaim 推迟到 listener 扫描完成后, 由 RedisPartition.load(this).startAllGroups() 统一启动。
     *
     * @param partitionTopics 容器中所有 PARTITION 模式 listener 的 topic 集合
     */
    private void autoPreparePartitions(Set<String> partitionTopics) {
        NasaLettuceConfig.Partition pc = this.stream.getPartition();
        if (!pc.isEnabled()) return;

        // 收集被隔离组覆盖的 topic, 用于判断哪些 topic 需要走共享组
        Set<String> isolatedTopics = new HashSet<>();

        // 隔离组: 仅当 listener 的 topic 命中该组才初始化
        pc.getGroups().forEach((logicalName, gc) -> {
            List<String> topics = gc.getTopics();
            // topics 没配 → 默认 logical name 同时是 topic
            List<String> effectiveTopics = ColUtils.isEmpty(topics) ? List.of(logicalName) : topics;
            // 检查是否有 listener topic 命中
            boolean hasMatch = false;
            for (String topic : effectiveTopics) {
                if (partitionTopics.contains(topic)) {
                    hasMatch = true;
                    isolatedTopics.add(topic);
                }
            }
            if (hasMatch) {
                for (String topic : effectiveTopics) {
                    RedisPartition.load(this).isolate(topic, logicalName, gc.getCount());
                }
            }
        });

        // 共享组: 有 listener topic 不属于任何隔离组时才初始化
        for (String topic : partitionTopics) {
            if (!isolatedTopics.contains(topic)) {
                RedisPartition.load(this).init();
                break;
            }
        }
    }

    /**
     * 业务作用：声明本代理在统计注册表中的场景名，使多实例的采样数据互不混淆。
     *
     * <p>参数说明: 无。
     *
     * @return 场景名。
     */
    @Override
    public String scene() {
        return qualifierFormat(qualifier);
    }

    /**
     * 本 RedisProxy 已订阅的 topic 集合 (PROXY 模式 = stream key, PARTITION 模式 = 业务 topic)。
     * loadStreamSubscribe 内维护, 由 subscribedTopics() 对外暴露, 供监控与第三方裁剪服务读取。
     */
    private final Set<String> subscribedTopics = new ConcurrentHashSet<>();

    /**
     * 业务作用：已订阅的所有 topic. 用于监控 / 触发第三方裁剪服务
     *
     * @return 见上述说明。
     */
    public Set<String> getSubscribedTopics() {
        return subscribedTopics;
    }

    /**
     * 业务作用：处理一个 {@link StreamSubscribe} bean 的注册 (含 {@link RedisEventBatchListener} / {@link RedisEventSingleListener})。
     * <p>
     * 流程:
     * <ol>
     *   <li>qualifier 过滤 — 跳过不归本 RedisProxy 服务的 listener</li>
     *   <li>mode 分流 — PROXY/BOTH 走 redisProxy.subscribe; PARTITION/BOTH 走 RedisPartition.load(this).registerListener</li>
     *   <li>PROXY 路径用 FactorConsumer2 包装 listener.onEvent + autoDelete + EventMessage 回收</li>
     * </ol>
     * 一个 instanceof StreamSubscribe 就覆盖了所有 stream listener bean (Batch / Single 都继承 StreamSubscribe)。
     *
     * @param ss 见上述说明
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void loadStreamSubscribe(StreamSubscribe ss) {
        // step 1: qualifier 过滤
        String[] qfr = ss.qualifiers();
        if (Objects.nonNull(qfr) && ColUtils.notContains(qfr, RedisProxy::qualifierFormat, this.qualifier)) {
            return;
        }
        String[] topics = ss.topics();
        if (ColUtils.isEmpty(topics)) return;
        String event = ss.event();
        if (StringUtils.isBlank(event)) {
            throw new NullPointerException("StreamSubscribe.event() must not be null/empty: " + ss.getClass().getName());
        }
        TypeReference reference = ss.paramType();
        ConsumeMode mode = ss.mode();

        // step 2: PARTITION/BOTH → 注册到 RedisPartition (per-partition 串行消费)
        // StreamSubscribe 直接走 registerListener;
        // 纯 StreamSubscribe (非 Batch/Single) 走 registerConsumer 包装路径
        if (mode != ConsumeMode.PROXY) {
            if (this.stream.getPartition().isEnabled()) {
                RedisPartition.load(this).registerListener(ss);
            } else {
                log.warn("StreamSubscribe {} mode={} but RedisProxy[{}] partition.enabled=false, skip partition registration",
                        ss.getClass().getName(), mode, this.qualifier);
            }
            // PARTITION 不走 subscribe; BOTH 继续往下
            if (mode == ConsumeMode.PARTITION) return;
        }

        // step 3: PROXY/BOTH → 走 redisProxy.subscribe (普通流共享 group)
        // FactorConsumer2 把 listener 暴露给框架内部用 (识别 batch / autoDelete 等)
        String group = ss.group();
        for (String topic : topics) {
            if (StringUtils.isBlank(topic)) {
                throw new NullPointerException("StreamSubscribe.topics() contains blank: " + ss.getClass().getName());
            }
            subscribedTopics.add(topic);
            this.subscribe(topic, group, event, reference, new FactorConsumer2<String, Object, StreamSubscribe>() {
                @Override
                public StreamSubscribe factor() {
                    // 给框架内部的 isBatchConsumer 检测用
                    return ss;
                }

                @Override
                public void accept(String e, Object m) {
                    try {
                        ss.onEvent(m);
                    } finally {
                        if (m instanceof EventMessage em) em.recycle();
                    }
                    // autoDelete: 处理后自动 XDEL stream record
                    if (!ss.autoDelete()) return;

                    if (ss instanceof RedisEventSingleListener) {
                        Object rec = RedisProxyHolder.getStreamRecordId();
                        if (rec != null) RedisProxy.this.xDelAsync(topic, rec.toString());
                    } else if (ss instanceof RedisEventBatchListener) {
                        Object rec = RedisProxyHolder.getStreamRecordId();
                        if (rec instanceof Collection col) {
                            for (Object o : col) RedisProxy.this.xDelAsync(topic, o.toString());
                        }
                    }
                }
            });
        }
    }

    /**
     * 业务作用：获取 pipeline 命令缓冲队列, 业务方 offer 池化的 {@link PipelineTask}, TimingWheel 1ms tick 单消费者 drain.
     * <p>
     * MPSC 模式: N 个业务线程并发 offer (CLQ lock-free CAS), 单消费者 (signal 线程) 在 {@link #before()} tick
     * 内 poll() 排干 + recycle task. task 字段引用直传到 CmdBuffer, 全程零 lambda 零 capturing.
     *
     * @return 见上述说明。
     */
    public ConcurrentLinkedQueue<PipelineTask> pipeline() {
        return this.cqueue;
    }

    /**
     * 业务作用：判断时间窗内并发数是否较低
     * 低：走RedisTemplate原生API
     * 高：走pipeline
     *
     * @return 见上述说明。
     */
    private boolean isLower() {
        return super.signal() <= lower;
    }

    /**
     * 业务作用：把 Lettuce 的区间边界无损转换为 Spring Data 区间，保留开闭与无界语义。
     * 直接取边界值会把无界转换成 null，也会把排除边界误当成包含边界。
     *
     * @param range Lettuce 区间
     * @param <T>   边界值类型
     * @return 语义等价的 Spring Data 区间。
     */
    private static <T> org.springframework.data.domain.Range<T> toSpringRange(Range<T> range) {
        Objects.requireNonNull(range, "range must not be null");
        Range.Boundary<T> sourceLower = range.getLower();
        Bound<T> lower = sourceLower.isUnbounded()
                ? Bound.unbounded()
                : sourceLower.isIncluding() ? Bound.inclusive(sourceLower.getValue()) : Bound.exclusive(sourceLower.getValue());
        Range.Boundary<T> sourceUpper = range.getUpper();
        Bound<T> upper = sourceUpper.isUnbounded()
                ? Bound.unbounded()
                : sourceUpper.isIncluding() ? Bound.inclusive(sourceUpper.getValue()) : Bound.exclusive(sourceUpper.getValue());
        return org.springframework.data.domain.Range.of(lower, upper);
    }

    /**
     * 业务作用：从 DI 容器中获取一个线程池。
     * <p>
     * 选择策略:
     * <ol>
     *   <li>遍历所有 Executor bean, 提交探测任务测 {@link Thread#isVirtual()},
     *       发现任何一个是虚拟线程池就优先返回</li>
     *   <li>没有虚拟线程池 → 按 Spring 标准 bean 名 fallback:
     *       taskExecutor → asyncTaskExecutor → applicationTaskExecutor</li>
     *   <li>仍未命中 → 取容器里第一个 Executor bean</li>
     *   <li>容器里没有任何 Executor → 退到 TimingWheel 自带的 work executor</li>
     * </ol>
     * <b>设计原则</b>: 框架不替业务决定线程池模型 —— 业务方注入了虚拟线程池就是想用它,
     * 框架感知并优先选用; 业务什么都不配就用 Spring 默认 (可能是平台线程池)。
     * 业务发现框架性能瓶颈在线程池, 主动注入虚拟线程池即可, 无需改框架代码。
     */
    Executor getExecutor() {
        Executor resolved = taskExecutor;
        if (resolved != null) return resolved;
        executorLock.lock();
        try {
            if (taskExecutor != null) return taskExecutor;
            return taskExecutor = resolveExecutor();
        } finally {
            executorLock.unlock();
        }
    }

    /**
     * 业务作用：按容器配置选择 Stream 与订阅回调共用的执行器。
     * 探测只在首次解析时发生，避免每批消息重复向业务线程池提交探测任务。
     *
     * <p>参数说明: 无。
     *
     * @return 可用的业务执行器；容器未提供时返回时间轮自带执行器。
     */
    private Executor resolveExecutor() {
        // 1. 优先找虚拟线程池: 业务侧只要注入了一个 isVirtual 的 Executor bean, 这里就会命中
        Map<String, Executor> all = ContextUtils.getBeansOfType(Executor.class);
        if (!MapUtils.isEmpty(all)) {
            for (Executor e : all.values()) {
                if (isVirtualThreadExecutor(e)) return e;
            }
        }
        // 2. 没有虚拟线程池 → 按 Spring 习惯 bean 名 fallback
        Executor executor = ContextUtils.getBeanOrNull("taskExecutor", Executor.class);
        if (Objects.nonNull(executor)) return executor;

        executor = ContextUtils.getBeanOrNull("asyncTaskExecutor", Executor.class);
        if (Objects.nonNull(executor)) return executor;

        executor = ContextUtils.getBeanOrNull("applicationTaskExecutor", Executor.class);
        if (Objects.nonNull(executor)) return executor;

        executor = ContextUtils.getBeanFirstOrNull(Executor.class);
        return Objects.isNull(executor) ? TimingWheel.of().start().getVirtualExecutor() : executor;
    }

    /**
     * 业务作用：探测一个 Executor 是否承载虚拟线程: 提交一个任务到它上面跑 {@link Thread#isVirtual()}, 2s 超时兜底。
     * <p>
     * 没用 instanceof / 类名匹配, 是因为虚拟线程池在 JDK 里对应的是
     * {@code JVM.util.concurrent.ThreadPerTaskExecutor} (jdk-internal class), 不稳;
     * 而框架的虚拟线程包装类型也不固定。提交任务并读取 {@link Thread#isVirtual()} 能直接反映执行器的线程模型，getExecutor 在生命周期内
     * 只调一次, 一次探测开销可以接受。
     *
     * @param executor 见上述说明
     * @return 见上述说明。
     */
    private static boolean isVirtualThreadExecutor(Executor executor) {
        if (executor == null) return false;
        try {
            CompletableFuture<Boolean> probe = new CompletableFuture<>();
            executor.execute(() -> probe.complete(Thread.currentThread().isVirtual()));
            return Boolean.TRUE.equals(probe.get(2, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Throwable t) {
            // 提交失败或超时视为非虚拟线程池，不影响后续 fallback。
            return false;
        }
    }

    /* NOTE ------------------- common start ------------------------------------------------------------------------ */

    /**
     * 业务作用：删除键。
     * 键不存在时不报错，因此可安全用于幂等清理。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param keys 缓存键集合
     * @return 命令的执行结果。
     */
    public Long del(String... keys) {
        if (ColUtils.isEmpty(keys)) {
            return 0L;
        }
        if (isLower()) {
            return redisTemplate.delete(ColUtils.toSet(keys));
        }
        byte[][] ks = ColUtils.toArray(byte[].class, keys, StringUtils::isNotBlank, keySerializer::serialize);
        if (ColUtils.isEmpty(ks)) {
            return 0L;
        }
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_DEL_MULTI).extras(ks).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：导出键的序列化内容。
     * 内容格式与 Redis 版本绑定，跨版本恢复可能失败。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public byte[] dump(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return redisTemplate.dump(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_DUMP).arg1(k).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：判断键是否存在。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public boolean exists(String key) {
        if (StringUtils.isBlank(key)) {
            return false;
        }
        if (isLower()) {
            return redisTemplate.hasKey(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_EXISTS).arg1(k).future(lf));
        Long r = lf.getFinally();
        return Objects.nonNull(r) && r > 0;
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param unit 时长单位
     * @return 命令的执行结果。
     */
    public boolean expire(String key, long timeout, TimeUnit unit) {
        return expire(key, unit.toMillis(timeout));
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * @return 命令的执行结果。
     */
    public boolean expire(String key, long millis) {
        if (StringUtils.isBlank(key)) {
            return false;
        }
        if (isLower()) {
            return redisTemplate.expire(key, millis, TimeUnit.MILLISECONDS);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Boolean>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_EXPIRE).arg1(k).longArg(millis).future(lf));
        Boolean r = lf.getFinally();
        return Objects.nonNull(r) && r;
    }

    /**
     * 业务作用：把键的过期时刻设为绝对时间。
     * 依赖服务端时钟，与客户端时钟不一致时过期时刻会偏移。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * @return 命令的执行结果。
     */
    public boolean expireAt(String key, long millis) {
        if (StringUtils.isBlank(key)) {
            return false;
        }
        if (isLower()) {
            Boolean r = redisTemplate.expireAt(key, Instant.ofEpochMilli(millis));
            return Objects.nonNull(r) && r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Boolean>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_EXPIRE_AT).arg1(k).longArg(millis).future(lf));
        Boolean r = lf.getFinally();
        return Objects.nonNull(r) && r;
    }

    /**
     * 业务作用：移除键的存活时长使其长期保留。
     * 此后该键不再自动回收，需由业务显式删除，否则会持续占用内存。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public boolean persist(String key) {
        if (StringUtils.isBlank(key)) {
            return false;
        }
        if (isLower()) {
            return redisTemplate.persist(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Boolean>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_PERSIST).arg1(k).future(lf));
        Boolean r = lf.getFinally();
        return Objects.nonNull(r) && r;
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * @param hashKeys 哈希字段名集合
     * 返回: 无返回值。
     */
    public void expire(String key, long millis, String... hashKeys) {
        expire(key, millis, TimeUnit.MILLISECONDS, hashKeys);
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param unit 时长单位
     * @param hashKeys 哈希字段名集合
     * 返回: 无返回值。
     */
    public void expire(String key, long timeout, TimeUnit unit, String... hashKeys) {
        expire(key, Duration.ofMillis(unit.toMillis(timeout)), hashKeys);
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param hashKeys 哈希字段名集合
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void expire(String key, Duration timeout, String... hashKeys) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(hashKeys)) return;
        if (isLower()) {
            hashOperations.expire(key, timeout, Arrays.asList(hashKeys));
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[][] hks = ColUtils.toArray(byte[].class, hashKeys, StringUtils::isNotBlank, hashKeySerializer::serialize);
        // 过滤后全空: 不发空 HEXPIRE
        if (hks.length == 0) return;
        LettuceFuture<RedisFuture<List<Long>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HEXPIRE_MULTI).arg1(k).longArg(timeout.toMillis()).extras(hks).future(lf));
        lf.getFinally();
    }

    /**
     * 业务作用：查询键的剩余存活毫秒数。
     * 键不存在或未设存活时长时返回负值，两种情形取值不同。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public long pttl(String key) {
        if (StringUtils.isBlank(key)) {
            return 0L;
        }
        if (isLower()) {
            Long ttl = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
            return ttl == null ? 0L : ttl;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_PTTL).arg1(k).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0L : r;
    }

    /**
     * 业务作用：按模式匹配键名。
     * 该命令会遍历整个键空间，<b>在大实例上会阻塞服务端</b>，生产环境应改用游标扫描。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param pattern 匹配模式
     * @return 命令的执行结果。
     */
    public Set<String> keys(String pattern) {
        if (StringUtils.isBlank(pattern)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return redisTemplate.keys(pattern);
        }
        byte[] k = keySerializer.serialize(pattern);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_KEYS).arg1(k).future(lf));
        List<byte[]> rs = lf.getFinally();
        if (ColUtils.isEmpty(rs)) {
            return Collections.emptySet();
        }
        Set<String> r = new LinkedHashSet<>(rs.size());
        for (byte[] b : rs) {
            r.add(keySerializer.deserialize(b));
        }
        return r;
    }

    /* NOTE ------------------- String start ------------------------------------------------------------------------ */

    /**
     * 业务作用：读取字符串值。
     * 键不存在时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public <T> T get(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return (T) redisTemplate.opsForValue().get(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_GET).arg1(k).future(lf));
        return lf.getFinally(valueSerializer);
    }

    /**
     * 业务作用：读取字符串的指定字节区间。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * @return 命令的执行结果。
     */
    public String getRange(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return redisTemplate.opsForValue().get(key, start, end);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_GETRANGE).arg1(k).longArg(start).longArg2(end).future(lf));
        return lf.getFinally(RedisSerializer.string());
    }

    /**
     * 业务作用：写入新值并取回旧值，两步在服务端原子完成，可用于取走并重置一个标记。
     *
     * @param key 缓存键
     * @param val 新值
     * @param <T> 值类型
     * @return 旧值；键此前不存在时为 null。
     */
    public <T> T getAndSet(String key, T val) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return (T) redisTemplate.opsForValue().getAndSet(key, val);
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_GETSET).arg1(k).arg3(v).future(lf));
        return lf.getFinally(valueSerializer);
    }

    /**
     * 业务作用：读取并随即删除，两步在服务端原子完成，适合一次性取走的令牌类数据。
     *
     * @param key 缓存键
     * @param <T> 值类型
     * @return 被取走的值；键不存在时为 null。
     */
    public <T> T getAndDel(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return (T) redisTemplate.opsForValue().getAndDelete(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_GETDEL).arg1(k).future(lf));
        return lf.getFinally(valueSerializer);
    }

    /**
     * 业务作用：批量读取多个键值。
     * 结果与入参顺序一一对应，不存在或名称为空白的键对应位置为空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param keys 缓存键集合
     * @return 命令的执行结果。
     */
    public <T> List<T> multiGet(Collection<String> keys) {
        if (ColUtils.isEmpty(keys)) {
            return Collections.emptyList();
        }
        List<String> validKeys = new ArrayList<>(keys.size());
        List<Integer> validIndexes = new ArrayList<>(keys.size());
        List<T> result = new ArrayList<>(Collections.nCopies(keys.size(), null));
        int index = 0;
        for (String key : keys) {
            if (StringUtils.isNotBlank(key)) {
                validKeys.add(key);
                validIndexes.add(index);
            }
            index++;
        }
        if (validKeys.isEmpty()) return result;

        List<T> values;
        if (isLower()) {
            values = (List<T>) redisTemplate.opsForValue().multiGet(validKeys);
        } else {
            byte[][] ks = ColUtils.toArray(byte[].class, validKeys, key -> true, keySerializer::serialize);
            LettuceFuture<RedisFuture<List<KeyValue<byte[], byte[]>>>> lf = LettuceFuture.of();
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_MGET).extras(ks).future(lf));
            List<KeyValue<byte[], byte[]>> rs = lf.getFinally();
            values = new ArrayList<>(rs.size());
            for (KeyValue<byte[], byte[]> kv : rs) {
                values.add(kv.hasValue() ? (T) valueSerializer.deserialize(kv.getValue()) : null);
            }
        }
        if (values == null) return result;
        for (int i = 0; i < validIndexes.size() && i < values.size(); i++) {
            result.set(validIndexes.get(i), values.get(i));
        }
        return result;
    }

    /**
     * 业务作用：批量读取多个键并组装成保持入参顺序的映射。
     * <p>
     * 键名为空白的项在发命令前被剔除，不出现在结果里；<b>其余键无论是否命中都会进结果</b>，
     * 未命中的对应值为 null，因此遍历结果时仍需判空。
     * <p>
     * 剔除空白键是必要的：批量读取路径会过滤空白键，导致返回条数少于入参条数，
     * 若仍按原始入参顺序回填，从空白键之后的每个键都会拿到<b>属于别人的值</b>。
     *
     * @param keys 缓存键集合
     * @param <T>  值类型
     * @return 键到值的映射，保持入参顺序。
     */
    public <T> LinkedHashMap<String, T> multiGetToMap(Collection<String> keys) {
        LinkedHashMap<String, T> map = new LinkedHashMap<>();
        if (ColUtils.isEmpty(keys)) return map;
        // 只对有效 (非 blank) key 发 MGET, 结果与有效 key 一一对齐:
        // multiGet 的 pipeline 路径会过滤 blank key 导致结果数 < 原始 keys 数, 直接按原始 keys 回填会越界/错位.
        List<String> validKeys = new ArrayList<>(keys.size());
        for (String key : keys) if (StringUtils.isNotBlank(key)) validKeys.add(key);
        if (validKeys.isEmpty()) return map;
        List<T> list = this.multiGet(validKeys);
        int i = 0;
        for (String key : validKeys) {
            map.put(key, i < list.size() ? list.get(i++) : null);
        }
        return map;
    }

    /**
     * 业务作用：写入字符串值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void set(String key, Object val) {
        if (StringUtils.isBlank(key) || Objects.isNull(val)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForValue().set(key, val);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SET).arg1(k).arg3(v));
    }

    /**
     * 业务作用：写入字符串值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param timeout 超时时长
     * @param unit 时长单位
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void set(String key, Object val, int timeout, TimeUnit unit) {
        if (StringUtils.isBlank(key) || Objects.isNull(val)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForValue().set(key, val, timeout, unit);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SET_EX).arg1(k).arg3(v).longArg(unit.toMillis(timeout)));
    }

    /**
     * 业务作用：写入字符串值。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param millis 毫秒数
     * 返回: 无返回值。
     */
    public void set(String key, Object val, int millis) {
        this.set(key, val, millis, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：仅在键不存在时写入。
     * 写入与判存在服务端原子完成，常用于抢占型的互斥标记。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public boolean setIfAbsent(String key, Object val) {
        if (StringUtils.isBlank(key) || Objects.isNull(val)) {
            return false;
        }
        if (isLower()) {
            Boolean r = redisTemplate.opsForValue().setIfAbsent(key, val);
            return Objects.nonNull(r) && r;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Boolean>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SET_NX).arg1(k).arg3(v).future(lf));
        Boolean r = lf.getFinally();
        return Objects.nonNull(r) && r;
    }

    /**
     * 业务作用：从指定偏移开始覆写字符串的一段。
     * 偏移超出原长度时中间以零字节填充，可能一次分配大量内存。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param offset 偏移量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void setRange(String key, Object val, long offset) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForValue().set(key, val, offset);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SETRANGE).arg1(k).arg3(v).longArg(offset));
    }

    /**
     * 业务作用：查询字符串值的字节长度。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public long strLen(String key) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long size = redisTemplate.opsForValue().size(key);
            return Objects.isNull(size) ? 0 : size;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_STRLEN).arg1(k).future(lf));
        Long size = lf.getFinally();
        return Objects.isNull(size) ? 0 : size;
    }

    /**
     * 业务作用：批量写入多个键值。
     * 有效键值通过一条 MSET 原子写入；空键与空值被忽略，全部无效时不发命令。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param map 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    @SuppressWarnings("rawtypes")
    public void multiSet(Map map) {
        if (MapUtils.isEmpty(map)) {
            return;
        }
        Map<String, Object> valid = new LinkedHashMap<>(map.size());
        map.forEach((key, value) -> {
            if (key == null || value == null) return;
            String stringKey = key.toString();
            if (StringUtils.isNotBlank(stringKey)) valid.put(stringKey, value);
        });
        if (valid.isEmpty()) return;
        if (isLower()) {
            redisTemplate.opsForValue().multiSet(valid);
            return;
        }
        Map<byte[], byte[]> m = new LinkedHashMap<>(valid.size());
        valid.forEach((key, value) -> m.put(keySerializer.serialize(key), valueSerializer.serialize(value)));
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_MSET).extras(m));
    }

    /**
     * 业务作用：按增量原子自增。
     * 自增在服务端完成，多个客户端并发调用不会丢更新。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public long increment(String key) {
        Objects.requireNonNull(key);
        if (isLower()) {
            Long r = redisTemplate.opsForValue().increment(key);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_INCR).arg1(k).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按增量原子自增。
     * 自增在服务端完成，多个客户端并发调用不会丢更新。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param delta 增减量
     * @return 命令的执行结果。
     */
    public long increment(String key, long delta) {
        Objects.requireNonNull(key);
        if (isLower()) {
            Long r = redisTemplate.opsForValue().increment(key, delta);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_INCRBY).arg1(k).longArg(delta).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按减量原子自减。
     * 结果可为负数，需要下界约束的场景应改用脚本。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public long decrement(String key) {
        Objects.requireNonNull(key);
        if (isLower()) {
            Long r = redisTemplate.opsForValue().decrement(key);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_DECR).arg1(k).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按减量原子自减。
     * 结果可为负数，需要下界约束的场景应改用脚本。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param delta 增减量
     * @return 命令的执行结果。
     */
    public long decrement(String key, long delta) {
        Objects.requireNonNull(key);
        if (isLower()) {
            Long r = redisTemplate.opsForValue().decrement(key, delta);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_DECRBY).arg1(k).longArg(delta).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：以业务 nonce 原子自增字符串计数器；默认 7 天窗口内重复调用返回首次结果，不会再次改变值。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；窗口可由全局配置调整。
     *
     * @param key   目标键
     * @param nonce 稳定业务事件的幂等标识
     * @return 该 nonce 首次自增后的精确 int64 值。
     */
    public long incrementIdempotent(String key, String nonce) {
        return incrementIdempotent(key, 1, nonce);
    }

    /**
     * 业务作用：以业务 nonce 原子增加字符串计数器；默认 7 天窗口内参数或方向漂移也只返回首次结果。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；窗口可由全局配置调整。
     *
     * @param key   目标键
     * @param delta 增量
     * @param nonce 稳定业务事件的幂等标识
     * @return 该 nonce 首次执行后的精确 int64 值。
     */
    public long incrementIdempotent(String key, long delta, String nonce) {
        return idempotentCounter.increment(key, delta, nonce);
    }

    /**
     * 业务作用：以业务 nonce 原子自减字符串计数器；默认 7 天窗口内重复调用返回首次结果，不会再次改变值。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；窗口可由全局配置调整。
     *
     * @param key   目标键
     * @param nonce 稳定业务事件的幂等标识
     * @return 该 nonce 首次自减后的精确 int64 值。
     */
    public long decrementIdempotent(String key, String nonce) {
        return decrementIdempotent(key, 1, nonce);
    }

    /**
     * 业务作用：以业务 nonce 原子减少字符串计数器；默认 7 天窗口内参数或方向漂移也只返回首次结果。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；窗口可由全局配置调整。
     *
     * @param key   目标键
     * @param delta 减量
     * @param nonce 稳定业务事件的幂等标识
     * @return 该 nonce 首次执行后的精确 int64 值。
     */
    public long decrementIdempotent(String key, long delta, String nonce) {
        return idempotentCounter.decrement(key, delta, nonce);
    }

    /**
     * 业务作用：在字符串末尾追加内容。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void append(String key, String val) {
        if (StringUtils.isBlank(key) || Objects.isNull(val)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForValue().append(key, val);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_APPEND).arg1(k).arg3(v));
    }

    /* NOTE ------------------- Hash start -------------------------------------------------------------------------- */

    /**
     * 业务作用：删除哈希字段。
     * 字段不存在时不报错。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKeys 哈希字段名集合
     * @return 命令的执行结果。
     */
    public long hDel(String key, String... hashKeys) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(hashKeys)) {
            return 0;
        }
        if (isLower()) {
            return hashOperations.delete(key, (Object[]) hashKeys);
        }
        byte[] k = keySerializer.serialize(key);
        byte[][] hks = ColUtils.toArray(byte[].class, hashKeys, StringUtils::isNotBlank, hashKeySerializer::serialize);
        // 过滤后全空: 不发空 HDEL, 返回 0 (无字段被删)
        if (hks.length == 0) return 0;
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HDEL_MULTI).arg1(k).extras(hks).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：判断哈希字段是否存在。
     *
     * @param key     缓存键
     * @param hashKey 哈希字段名
     * @return 存在返回 true。
     */
    public boolean hasKey(String key, String hashKey) {
        if (StringUtils.isBlank(key) || StringUtils.isBlank(hashKey)) {
            return false;
        }
        if (isLower()) {
            return hashOperations.hasKey(key, hashKey);
        }
        byte[] k = keySerializer.serialize(key);
        byte[] hk = hashKeySerializer.serialize(hashKey);
        LettuceFuture<RedisFuture<Boolean>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HEXISTS).arg1(k).arg2(hk).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：读取哈希字段。
     * 字段不存在时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @return 命令的执行结果。
     */
    public <T> T hGet(String key, String hashKey) {
        if (StringUtils.isBlank(key) || StringUtils.isBlank(hashKey)) {
            return null;
        }
        if (isLower()) {
            return (T) hashOperations.get(key, hashKey);
        }
        byte[] k = keySerializer.serialize(key);
        byte[] hk = hashKeySerializer.serialize(hashKey);
        LettuceFuture<RedisFuture<?>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HGET).arg1(k).arg2(hk).future(lf));
        return lf.getFinally(hashValueSerializer);
    }

    /**
     * 业务作用：读取哈希字段。
     * 字段不存在时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public <T> Map<String, T> hGet(String key) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptyMap();
        }
        if (isLower()) {
            return (Map<String, T>) hashOperations.entries(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Map<byte[], byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HGETALL).arg1(k).future(lf));
        Map<byte[], byte[]> rm = lf.getFinally();
        if (rm == null || rm.isEmpty()) return Collections.emptyMap();
        Map<String, T> map = new LinkedHashMap<>(rm.size());
        rm.forEach((ks, vs) -> map.put(hashKeySerializer.deserialize(ks), (T) hashValueSerializer.deserialize(vs)));
        return map;
    }

    /**
     * 业务作用：按增量原子自增哈希字段。
     * 自增在服务端完成，并发调用不会丢更新。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * @return 命令的执行结果。
     */
    public long hIncrBy(String key, String hashKey, long delta) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(hashKey);
        if (isLower()) {
            return hashOperations.increment(key, hashKey, delta);
        }
        byte[] k = keySerializer.serialize(key);
        byte[] hk = hashKeySerializer.serialize(hashKey);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HINCRBY).arg1(k).arg2(hk).longArg(delta).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：按增量原子自增哈希字段。
     * 自增在服务端完成，并发调用不会丢更新。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @return 命令的执行结果。
     */
    public long hIncrBy(String key, String hashKey) {
        return hIncrBy(key, hashKey, 1);
    }

    /**
     * 业务作用：按减量原子自减哈希字段。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * @return 命令的执行结果。
     */
    public long hDecrBy(String key, String hashKey, long delta) {
        return hIncrBy(key, hashKey, 0 - delta);
    }

    /**
     * 业务作用：按减量原子自减哈希字段。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @return 命令的执行结果。
     */
    public long hDecrBy(String key, String hashKey) {
        return hDecrBy(key, hashKey, 1);
    }

    /**
     * 业务作用：以业务 nonce 原子自增 HASH 字段；默认 7 天窗口内重复调用返回首次结果，不会再次改变余额。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；窗口可由全局配置调整。
     *
     * @param key     目标 HASH 键
     * @param hashKey 目标字段
     * @param nonce   稳定业务事件的幂等标识
     * @return 该 nonce 首次自增后的精确 int64 值。
     */
    public long hIncrByIdempotent(String key, String hashKey, String nonce) {
        return hIncrByIdempotent(key, hashKey, 1, nonce);
    }

    /**
     * 业务作用：以业务 nonce 原子增加 HASH 字段；默认 7 天窗口内参数或方向漂移也只返回首次结果。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；窗口可由全局配置调整。
     *
     * @param key     目标 HASH 键
     * @param hashKey 目标字段
     * @param delta   增量
     * @param nonce   稳定业务事件的幂等标识
     * @return 该 nonce 首次执行后的精确 int64 值。
     */
    public long hIncrByIdempotent(String key, String hashKey, long delta, String nonce) {
        return idempotentCounter.hashIncrement(key, hashKey, delta, nonce);
    }

    /**
     * 业务作用：以业务 nonce 原子自减 HASH 字段；默认 7 天窗口内重复调用返回首次结果，不会再次改变余额。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；窗口可由全局配置调整。
     *
     * @param key     目标 HASH 键
     * @param hashKey 目标字段
     * @param nonce   稳定业务事件的幂等标识
     * @return 该 nonce 首次自减后的精确 int64 值。
     */
    public long hDecrByIdempotent(String key, String hashKey, String nonce) {
        return hDecrByIdempotent(key, hashKey, 1, nonce);
    }

    /**
     * 业务作用：以业务 nonce 原子减少 HASH 字段；默认 7 天窗口内参数或方向漂移也只返回首次结果。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；窗口可由全局配置调整。
     *
     * @param key     目标 HASH 键
     * @param hashKey 目标字段
     * @param delta   减量
     * @param nonce   稳定业务事件的幂等标识
     * @return 该 nonce 首次执行后的精确 int64 值。
     */
    public long hDecrByIdempotent(String key, String hashKey, long delta, String nonce) {
        return idempotentCounter.hashDecrement(key, hashKey, delta, nonce);
    }

    static volatile byte[] HASH_HDECRIBY_DEL_BYTE;
    private static final String LPOP_ALL_SCRIPT =
            "local values = redis.call('LRANGE', KEYS[1], 0, -1); redis.call('DEL', KEYS[1]); return values";
    private static final Lock hDecrByAndDelLock = new ReentrantLock();

    /**
     * 业务作用：产出「自减到零即删字段」脚本的字节形式，供载入与执行使用。
     * 预先转成字节并缓存，避免每次执行都重复编码。
     *
     * <p>参数说明: 无。
     *
     * @return 脚本内容的字节形式。
     */
    static byte[] hDecrByAndDelScriptBytes() {
        if (Objects.nonNull(HASH_HDECRIBY_DEL_BYTE)) return HASH_HDECRIBY_DEL_BYTE;
        hDecrByAndDelLock.lock();
        try {
            if (Objects.nonNull(HASH_HDECRIBY_DEL_BYTE)) return HASH_HDECRIBY_DEL_BYTE;
            InputStream inputStream = RedisProxy.class.getResourceAsStream("/lua/hash_hdecriby_del.lua");
            if (Objects.isNull(inputStream)) {
                throw new FileException("lua script not found: {}", "/lua/hash_hdecriby_del.lua");
            }
            try (BufferedInputStream bis = new BufferedInputStream(inputStream)) {
                return HASH_HDECRIBY_DEL_BYTE = RedisSerializer.string()
                        .serialize(new String(bis.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new FileException(e.getMessage(), e);
            }
        } finally {
            hDecrByAndDelLock.unlock();
        }
    }

    /**
     * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
     * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值。
     */
    public void hDecrByAndDel(String key, String hashKey) {
        hDecrByAndDelEval(key, new byte[][]{hashKeySerializer.serialize(hashKey)});
    }

    /**
     * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
     * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * 返回: 无返回值。
     */
    public void hDecrByAndDel(String key, String hashKey, long delta) {
        hDecrByAndDelEval(key, new byte[][]{hashKeySerializer.serialize(hashKey), keySerializer.serialize(String.valueOf(delta))});
    }

    /**
     * 业务作用：执行自减并在减到零时删除字段的脚本。
     * 自减与删除必须在服务端原子完成——分两步会在两者之间留出窗口，使字段被并发写回而残留一个零值。
     *
     * @param key 缓存键
     * @param as  脚本参数
     * 返回: 无返回值。
     */
    private void hDecrByAndDelEval(String key, byte[][] as) {
        byte[] s = hDecrByAndDelScriptBytes();
        byte[][] ks = {keySerializer.serialize(key)};
        // 脚本固定返回自减后的余值(整数), 必须按整数解析响应:
        // 交给 ReturnType.fromJavaType(Object.class) 会落到 VALUE, 而 VALUE 对应的 ValueOutput 不接受整数响应,
        // 解析阶段直接抛 UnsupportedOperationException, 本方法在批次直通路径上会必然失败。
        ReturnType returnType = ReturnType.INTEGER;
        if (isLower()) {
            byte[][] kas = new byte[ks.length + as.length][];
            System.arraycopy(ks, 0, kas, 0, ks.length);
            System.arraycopy(as, 0, kas, ks.length, as.length);
            redisTemplate.execute(
                    RedisCallbackRecycler.<Object>ofRecycle(EVAL_DIRECT_CB)
                            .ref(0, s)
                            .ref(1, returnType)
                            .ref(2, kas)
                            .val(0, ks.length), true);
        } else {
            LettuceFuture<RedisFuture<?>> lf = LettuceFuture.of();
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_EVAL_ASYNC).extras(new Object[]{s, ks, as}).future(lf));
            lf.getFinally();
        }
    }

    /**
     * 业务作用：读取哈希的全部字段名。
     * 字段极多时会一次性返回全部内容，应评估返回体量。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public Set<String> hKeys(String key) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return hashOperations.keys(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HKEYS).arg1(k).future(lf));
        List<byte[]> rs = lf.getFinally();
        if (ColUtils.isEmpty(rs)) {
            return Collections.emptySet();
        }
        Set<String> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add(hashKeySerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：查询哈希的字段数。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public long hLen(String key) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            return hashOperations.size(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HLEN).arg1(k).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：批量读取哈希字段。
     * 结果与入参顺序一一对应，不存在或名称为空白的字段对应位置为空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKeys 哈希字段名集合
     * @return 命令的执行结果。
     */
    public <T> List<T> hMGet(String key, Collection<String> hashKeys) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(hashKeys)) {
            return Collections.emptyList();
        }
        List<String> validHashKeys = new ArrayList<>(hashKeys.size());
        List<Integer> validIndexes = new ArrayList<>(hashKeys.size());
        List<T> result = new ArrayList<>(Collections.nCopies(hashKeys.size(), null));
        int index = 0;
        for (String hashKey : hashKeys) {
            if (StringUtils.isNotBlank(hashKey)) {
                validHashKeys.add(hashKey);
                validIndexes.add(index);
            }
            index++;
        }
        if (validHashKeys.isEmpty()) return result;

        List<T> values;
        if (isLower()) {
            values = (List<T>) hashOperations.multiGet(key, validHashKeys);
        } else {
            byte[] k = keySerializer.serialize(key);
            byte[][] hks = ColUtils.toArray(byte[].class, validHashKeys, ignored -> true, hashKeySerializer::serialize);
            LettuceFuture<RedisFuture<List<KeyValue<byte[], byte[]>>>> lf = LettuceFuture.of();
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_HMGET).arg1(k).extras(hks).future(lf));
            List<KeyValue<byte[], byte[]>> rs = lf.getFinally();
            values = new ArrayList<>(rs.size());
            for (KeyValue<byte[], byte[]> kv : rs) {
                values.add(kv.hasValue() ? (T) hashValueSerializer.deserialize(kv.getValue()) : null);
            }
        }
        if (values == null) return result;
        for (int i = 0; i < validIndexes.size() && i < values.size(); i++) {
            result.set(validIndexes.get(i), values.get(i));
        }
        return result;
    }

    /**
     * 业务作用：批量读取哈希字段并组装成映射；不存在的字段不进结果。
     * 结果由池化容器承载，<b>调用方用完必须归还</b>，否则池会逐渐借空。
     *
     * @param key      缓存键
     * @param hashKeys 哈希字段名集合
     * @param <T>      值类型
     * @return 字段到值的映射，由调用方归还。
     */
    public <T> RecycleLinkedMap<String, T> hMGetToMap(String key, Collection<String> hashKeys) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(hashKeys)) return RecycleLinkedMap.of();
        RecycleLinkedMap<String, Collection<String>> ps = RecycleLinkedMap.of(key, hashKeys);
        try {
            RecycleLinkedMap<String, RecycleLinkedMap<String, T>> rm = this.hMGetToMap(ps);
            RecycleLinkedMap<String, T> data = rm.remove(key);
            if (data == null) data = RecycleLinkedMap.of();
            rm.recycle();
            return data;
        } finally {
            ps.recycle();
        }
    }

    /**
     * 业务作用：跨多个哈希键批量读取字段，一次批次完成，避免逐键往返。
     * 结果为两层池化映射，<b>外层与各内层都需要归还</b>。
     *
     * @param pm  各缓存键到其待读字段名集合的映射
     * @param <T> 值类型
     * @return 缓存键到「字段到值」映射的两层映射，由调用方归还。
     */
    public <T> RecycleLinkedMap<String, RecycleLinkedMap<String, T>> hMGetToMap(Map<String, ? extends Collection<String>> pm) {
        if (MapUtils.isEmpty(pm)) {
            return RecycleLinkedMap.of();
        }
        // 嵌套 (外层已 open) 用 openIsolated 独立执行器避免破坏外层, 否则复用单例 (零分配); 都在 finally clearSession 兜底
        LettucePipeline.Actuator actuator = LettucePipeline.openNested(this);
        // 只对有效 (过滤后非空 field) 的 entry 入队, 用并行 list 记录 (key, lf) 一一对齐:
        // 跳过空 field entry (否则发空 HMGET → Redis 参数错误), 也不能再按原始 pm 顺序+index 回填 (会错位).
        List<String> keys = new ArrayList<>(pm.size());
        List<LettuceFuture<RedisFuture<List<KeyValue<byte[], byte[]>>>>> lfs = new ArrayList<>(pm.size());
        // 手动 open 必须 try/finally 兜底: 序列化/入队/flush 前半段抛异常时清理 ThreadLocal, 防 open/CMD/LF 残留污染后续调用
        Throwable failure = null;
        boolean pipelineAttempted = false;
        try {
            pm.forEach((key, hashKeys) -> {
                if (StringUtils.isBlank(key) || ColUtils.isEmpty(hashKeys)) return;
                byte[][] hks = ColUtils.toArray(byte[].class, hashKeys, StringUtils::isNotBlank, hashKeySerializer::serialize);
                if (hks.length == 0) return;
                byte[] k = keySerializer.serialize(key);
                LettuceFuture<RedisFuture<List<KeyValue<byte[], byte[]>>>> lf = LettuceFuture.of();
                actuator.hmgetAsync(k, hks, lf);
                keys.add(key);
                lfs.add(lf);
            });
            pipelineAttempted = true;
            actuator.pipelineForce();
        } catch (RuntimeException | Error e) {
            failure = e;
            if (!pipelineAttempted && !lfs.isEmpty()) {
                try {
                    // 已入队的都是只读命令；发出它们可使对应 future 正常完成并归还对象池。
                    actuator.pipelineForce();
                } catch (RuntimeException | Error flushFailure) {
                    failure.addSuppressed(flushFailure);
                }
            }
        } finally {
            actuator.clearSession();
        }
        RecycleLinkedMap<String, RecycleLinkedMap<String, T>> map = RecycleLinkedMap.of();
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            try {
                List<KeyValue<byte[], byte[]>> rs = lfs.get(i).getFinally();
                for (KeyValue<byte[], byte[]> kv : rs) {
                    String k = hashKeySerializer.deserialize(kv.getKey());
                    T v = kv.hasValue() ? (T) hashValueSerializer.deserialize(kv.getValue()) : null;
                    RecycleLinkedMap<String, T> rlm = map.get(key);
                    if (rlm == null) {
                        map.put(key, rlm = RecycleLinkedMap.of());
                    }
                    rlm.put(k, v);
                }
            } catch (RuntimeException | Error e) {
                if (failure == null) failure = e;
            }
        }
        if (failure != null) {
            // 失败后仍等待了全部 future，确保池化占位均已归还；已组装的池化结果也必须一并归还。
            map.values().forEach(RecycleLinkedMap::recycle);
            map.recycle();
            if (failure instanceof RuntimeException runtimeException) throw runtimeException;
            throw (Error) failure;
        }
        return map;
    }

    /**
     * 业务作用：批量写入哈希字段。
     * 有效字段通过一条 HMSET 写入；空字段名与空值被忽略。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param pm 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void hMSet(String key, Map<String, ?> pm) {
        if (StringUtils.isBlank(key) || MapUtils.isEmpty(pm)) {
            return;
        }
        Map<String, Object> valid = new LinkedHashMap<>(pm.size());
        boolean[] containsRawBytes = {false};
        pm.forEach((hashKey, value) -> {
            if (StringUtils.isBlank(hashKey) || value == null) return;
            valid.put(hashKey, value);
            if (value instanceof byte[]) containsRawBytes[0] = true;
        });
        if (valid.isEmpty()) return;
        if (isLower() && !containsRawBytes[0]) {
            hashOperations.putAll(key, valid);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        Map<byte[], byte[]> m = new LinkedHashMap<>(valid.size());
        // 只要包含原始字节就统一走底层命令，确保并发阈值变化不会改变 byte[] 的落盘格式。
        valid.forEach((hashKey, value) -> m.put(hashKeySerializer.serialize(hashKey), serHVal(value)));
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HMSET).arg1(k).extras(m));
    }

    /**
     * 业务作用：写入哈希字段。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void hSet(String key, String hashKey, Object val) {
        if (StringUtils.isBlank(key) || StringUtils.isBlank(hashKey) || Objects.isNull(val)) {
            return;
        }
        if (isLower() && !(val instanceof byte[])) {
            hashOperations.put(key, hashKey, val);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] hk = hashKeySerializer.serialize(hashKey);
        // val 是 byte[] 时直通 (与 Actuator serHVal 一致): 上面专门把 byte[] 分流到 pipeline 就是为了写原始字节,
        // 若再 hashValueSerializer.serialize 会二次编码, 同一份 raw bytes 经 RedisProxy/LettucePipeline 写入会不一致
        byte[] v = val instanceof byte[] bs ? bs : hashValueSerializer.serialize(val);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HSET).arg1(k).arg2(hk).arg3(v));

    }

    /**
     * 业务作用：仅在哈希字段不存在时写入。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public boolean hSetNx(String key, String hashKey, Object val) {
        if (StringUtils.isBlank(key) || StringUtils.isBlank(hashKey) || Objects.isNull(val)) {
            return false;
        }
        if (isLower()) {
            return hashOperations.putIfAbsent(key, hashKey, val);
        }
        byte[] k = keySerializer.serialize(key);
        byte[] hk = hashKeySerializer.serialize(hashKey);
        // byte[] 直通, 对齐 Actuator
        byte[] v = serHVal(val);
        LettuceFuture<RedisFuture<Boolean>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_HSET_NX).arg1(k).arg2(hk).arg3(v).future(lf));
        return lf.getFinally();
    }

    /* NOTE ------------------- List start -------------------------------------------------------------------------- */

    /**
     * 业务作用：移出并获取列表的第一个元素， 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止
     *
     * @param key     缓存键
     * @param timeout 超时时长
     * @param unit    时长单位
     * @return 见上述说明。
     */
    public <T> T blPop(String key, long timeout, TimeUnit unit) {
        return (T) redisTemplate.opsForList().leftPop(key, timeout, unit);
    }

    /**
     * 业务作用：移出并获取列表的最后一个元素， 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止
     *
     * @param key     缓存键
     * @param timeout 超时时长
     * @param unit    时长单位
     * @return 见上述说明。
     */
    public <T> T brPop(String key, long timeout, TimeUnit unit) {
        return (T) redisTemplate.opsForList().rightPop(key, timeout, unit);
    }

    /**
     * 业务作用：按下标读取列表元素。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param index 下标
     * @return 命令的执行结果。
     */
    public <T> T lIndex(String key, long index) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return (T) redisTemplate.opsForList().index(key, index);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LINDEX).arg1(k).longArg(index).future(lf));
        return lf.getFinally(valueSerializer);
    }

    /**
     * 业务作用：查询列表长度。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public long lLen(String key) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForList().size(key);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LLEN).arg1(k).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：从列表头部插入元素。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param vals 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void lPush(String key, Object... vals) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(vals)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForList().leftPushAll(key, vals);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[][] vs = ColUtils.toArray(byte[].class, vals, ColUtils.nonNull(), valueSerializer::serialize);
        // 过滤后全空: 不发空 LPUSH (Redis 报参数错误)
        if (vs.length == 0) return;
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LPUSH_MULTI).arg1(k).extras(vs));
    }

    /**
     * 业务作用：仅在列表已存在时从头部插入。
     * 列表不存在时不创建，用于避免凭空建出空列表。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public long lPushIfAbsent(String key, Object val) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForList().leftPushIfPresent(key, val);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LPUSH_X).arg1(k).arg3(v).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：从列表头部弹出元素。
     * 列表为空时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public <T> T lPop(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return (T) redisTemplate.opsForList().leftPop(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LPOP).arg1(k).future(lf));
        return lf.getFinally(valueSerializer);
    }

    /**
     * 业务作用：从列表头部弹出元素。
     * 列表为空时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <T> List<T> lPop(String key, int count) {
        if (StringUtils.isBlank(key) || count <= 0) {
            return Collections.emptyList();
        }
        if (isLower()) {
            return (List<T>) redisTemplate.opsForList().leftPop(key, count);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LPOP_COUNT).arg1(k).longArg(count).future(lf));
        List<byte[]> rs = lf.getFinally();
        List<T> list = new ArrayList<>(rs.size());
        for (byte[] r : rs) {
            list.add((T) valueSerializer.deserialize(r));
        }
        return list;
    }

    /**
     * 业务作用：移除并获取列表所有的数据
     *
     * @param key 缓存键
     * @return 见上述说明。
     */
    public <T> List<T> lPopAll(String key) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptyList();
        }
        byte[][] keys = {Objects.requireNonNull(keySerializer.serialize(key))};
        // 读取与删除必须由同一个服务端脚本原子完成，避免并发写入落在两条命令之间后被删除却未返回。
        Object raw = evalRaw(LPOP_ALL_SCRIPT, ReturnType.MULTI, keys, LettucePipeline.EMPTY_BYTE2);
        if (!(raw instanceof List<?> rs) || rs.isEmpty()) return Collections.emptyList();
        List<T> list = new ArrayList<>(rs.size());
        for (Object r : rs) {
            if (!(r instanceof byte[] bytes)) {
                throw new IllegalStateException("LRANGE script returned an unexpected element type");
            }
            list.add((T) valueSerializer.deserialize(bytes));
        }
        return list;
    }

    /**
     * 业务作用：按下标区间读取列表元素。
     * 下标支持负数表示从尾部倒数。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * @return 命令的执行结果。
     */
    public <T> List<T> lRange(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptyList();
        }
        if (isLower()) {
            return (List<T>) redisTemplate.opsForList().range(key, start, end);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LRANGE).arg1(k).longArg(start).longArg2(end).future(lf));
        List<byte[]> rs = lf.getFinally();
        List<T> list = new ArrayList<>(rs.size());
        for (byte[] r : rs) {
            list.add((T) valueSerializer.deserialize(r));
        }
        return list;
    }

    /**
     * 业务作用：按值删除列表中的元素。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param count 数量上限
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public long lRem(String key, long count, Object val) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForList().remove(key, count, val);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LREM).arg1(k).arg3(v).longArg(count).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按下标覆盖列表元素。
     * 下标越界时报错。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param index 下标
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void lSet(String key, long index, Object val) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForList().set(key, index, val);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<String>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LSET).arg1(k).arg3(v).longArg(index).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
    }

    /**
     * 业务作用：把列表裁剪到指定下标区间。
     * 区间之外的元素被永久删除，不可撤销。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void lTrim(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForList().trim(key, start, end);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<String>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_LTRIM).arg1(k).longArg(start).longArg2(end).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
    }

    /**
     * 业务作用：从列表尾部插入元素。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param vals 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void rPush(String key, Object... vals) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(vals)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForList().rightPushAll(key, vals);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[][] vs = ColUtils.toArray(byte[].class, vals, ColUtils.nonNull(), valueSerializer::serialize);
        // 过滤后全空: 不发空 RPUSH (Redis 报参数错误)
        if (vs.length == 0) return;
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_RPUSH_MULTI).arg1(k).extras(vs));
    }

    /**
     * 业务作用：仅在列表已存在时从尾部插入。
     * 列表不存在时不创建。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public long rPushIfAbsent(String key, Object val) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForList().rightPushIfPresent(key, val);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_RPUSH_X).arg1(k).arg3(v).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：从列表尾部弹出元素。
     * 列表为空时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public <T> T rPop(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return (T) redisTemplate.opsForList().rightPop(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_RPOP).arg1(k).future(lf));
        return lf.getFinally(valueSerializer);
    }

    /**
     * 业务作用：从列表尾部弹出元素。
     * 列表为空时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <T> List<T> rPop(String key, int count) {
        if (StringUtils.isBlank(key) || count <= 0) {
            return Collections.emptyList();
        }
        if (isLower()) {
            return (List<T>) redisTemplate.opsForList().rightPop(key, count);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_RPOP_COUNT).arg1(k).longArg(count).future(lf));
        List<byte[]> rs = lf.getFinally();
        List<T> list = new ArrayList<>(rs.size());
        for (byte[] r : rs) {
            list.add((T) valueSerializer.deserialize(r));
        }
        return list;
    }

    /* NOTE ------------------- Set start --------------------------------------------------------------------------- */

    /**
     * 业务作用：向集合添加成员。
     * 已存在的成员不会重复加入。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param vals 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void sAdd(String key, Object... vals) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(vals)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForSet().add(key, vals);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[][] vs = ColUtils.toArray(byte[].class, vals, ColUtils.nonNull(), valueSerializer::serialize);
        // 过滤后全空: 不发空 SADD (Redis 报参数错误)
        if (vs.length == 0) return;
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SADD_MULTI).arg1(k).extras(vs).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
    }

    /**
     * 业务作用：查询集合的成员数。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public long sCard(String key) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForSet().size(key);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SCARD).arg1(k).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：求集合差集。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param keys 缓存键集合
     * @return 命令的执行结果。
     */
    public <T> Set<T> sDiff(String... keys) {
        if (ColUtils.isEmpty(keys)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForSet().difference(ColUtils.toSet(keys));
        }
        byte[][] ks = ColUtils.toArray(byte[].class, keys, StringUtils::isNotBlank, keySerializer::serialize);
        // 过滤后全空: 不发空 SDIFF
        if (ks.length == 0) return Collections.emptySet();
        LettuceFuture<RedisFuture<Set<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SDIFF).extras(ks).future(lf));
        Set<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：求集合交集。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param keys 缓存键集合
     * @return 命令的执行结果。
     */
    public <T> Set<T> sInter(String... keys) {
        if (ColUtils.isEmpty(keys)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForSet().intersect(ColUtils.toSet(keys));
        }
        byte[][] ks = ColUtils.toArray(byte[].class, keys, StringUtils::isNotBlank, keySerializer::serialize);
        // 过滤后全空: 不发空 SINTER
        if (ks.length == 0) return Collections.emptySet();
        LettuceFuture<RedisFuture<Set<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SINTER).extras(ks).future(lf));
        Set<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：判断元素是否是集合 key 的成员
     *
     * @param key 缓存键
     * @param val 见上述说明
     * @return 见上述说明。
     */
    public boolean sisMember(String key, Object val) {
        if (StringUtils.isBlank(key) || Objects.isNull(val)) {
            return false;
        }
        if (isLower()) {
            Boolean r = redisTemplate.opsForSet().isMember(key, val);
            return Objects.nonNull(r) && r;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Boolean>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SISMEMBER).arg1(k).arg3(v).future(lf));
        Boolean r = lf.getFinally();
        return Objects.nonNull(r) && r;
    }

    /**
     * 业务作用：判断元素是否是集合 key 的成员
     *
     * @param key  缓存键
     * @param vals 见上述说明
     * @return 见上述说明。
     */
    public Map<Object, Boolean> sMisMember(String key, Object... vals) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(vals)) {
            return Collections.emptyMap();
        }
        List<Object> valid = new ArrayList<>(vals.length);
        Map<Object, Boolean> result = new LinkedHashMap<>(vals.length);
        for (Object val : vals) {
            if (val == null) result.put(null, false);
            else valid.add(val);
        }
        if (valid.isEmpty()) return result;
        Map<Object, Boolean> membership;
        if (isLower()) {
            membership = redisTemplate.opsForSet().isMember(key, valid.toArray());
        } else {
            byte[] k = keySerializer.serialize(key);
            byte[][] vs = ColUtils.toArray(byte[].class, valid, ignored -> true, valueSerializer::serialize);
            LettuceFuture<RedisFuture<List<Boolean>>> lf = LettuceFuture.of();
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_SMISMEMBER).arg1(k).extras(vs).future(lf));
            List<Boolean> rs = lf.getFinally();
            membership = new LinkedHashMap<>(valid.size());
            for (int i = 0; i < valid.size(); i++) membership.put(valid.get(i), rs.get(i));
        }
        for (Object val : vals) result.put(val, val == null ? false : Boolean.TRUE.equals(membership.get(val)));
        return result;
    }

    /**
     * 业务作用：读取集合全部成员。
     * 成员极多时会一次性返回全部内容，应评估返回体量。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public <T> Set<T> sMembers(String key) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForSet().members(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Set<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SMEMBERS).arg1(k).future(lf));
        Set<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：随机弹出集合成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public <T> T sPop(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return (T) redisTemplate.opsForSet().pop(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SPOP).arg1(k).future(lf));
        return lf.getFinally(valueSerializer);
    }

    /**
     * 业务作用：随机弹出集合成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <T> Set<T> sPop(String key, int count) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForSet().pop(key, count);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Set<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SPOP_COUNT).arg1(k).longArg(count).future(lf));
        Set<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：随机读取集合成员但不移除。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public <T> T sRandMember(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (isLower()) {
            return (T) redisTemplate.opsForSet().randomMember(key);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<byte[]>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SRANDMEMBER).arg1(k).future(lf));
        return lf.getFinally(valueSerializer);
    }

    /**
     * 业务作用：随机读取集合成员但不移除。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <T> List<T> sRandMember(String key, int count) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptyList();
        }
        if (isLower()) {
            return (List<T>) redisTemplate.opsForSet().randomMembers(key, count);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SRANDMEMBER_COUNT).arg1(k).longArg(count).future(lf));
        List<byte[]> rs = lf.getFinally();
        List<T> list = new ArrayList<>(rs.size());
        for (byte[] r : rs) {
            list.add((T) valueSerializer.deserialize(r));
        }
        return list;
    }

    /**
     * 业务作用：从集合移除成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param vals 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void sRem(String key, Object... vals) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(vals)) {
            return;
        }
        if (isLower()) {
            redisTemplate.opsForSet().remove(key, vals);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        byte[][] vs = ColUtils.toArray(byte[].class, vals, ColUtils.nonNull(), valueSerializer::serialize);
        // 过滤 null 后全空: 不发空 SREM (SREM 只看删除数量, 过滤 null 不破坏结果对齐, 与 sAdd/zRem 一致)
        if (vs.length == 0) return;
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SREM_MULTI).arg1(k).extras(vs).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
    }

    /**
     * 业务作用：求集合并集。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param keys 缓存键集合
     * @return 命令的执行结果。
     */
    public <T> Set<T> sUnion(String... keys) {
        if (ColUtils.isEmpty(keys)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForSet().union(ColUtils.toSet(keys));
        }
        byte[][] ks = ColUtils.toArray(byte[].class, keys, StringUtils::isNotBlank, keySerializer::serialize);
        // 过滤后全空: 不发空 SUNION
        if (ks.length == 0) return Collections.emptySet();
        LettuceFuture<RedisFuture<Set<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SUNION).extras(ks).future(lf));
        Set<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /* NOTE ------------------- sorted set start -------------------------------------------------------------------- */

    /**
     * 业务作用：向有序集合添加成员并设定分值。
     * 成员已存在时更新其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param score 有序集合分值
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public boolean zAdd(String key, double score, Object val) {
        if (StringUtils.isBlank(key) || Objects.isNull(val)) {
            return false;
        }
        if (isLower()) {
            Boolean r = redisTemplate.opsForZSet().add(key, val, score);
            return Objects.nonNull(r) && r;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZADD).arg1(k).arg3(v).longArg(Double.doubleToRawLongBits(score)).future(lf));
        Long r = lf.getFinally();
        return Objects.nonNull(r) && r > 0;
    }

    /**
     * 业务作用：向有序集合添加成员并设定分值。
     * 成员已存在时更新其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param scoreAndVals 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void zAdd(String key, Object... scoreAndVals) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        if (scoreAndVals == null || scoreAndVals.length == 0) {
            return;
        }
        int pair = scoreAndVals.length >> 1;
        if (pair << 1 != scoreAndVals.length) {
            throw new IllegalArgumentException("ScoresAndValues.length must be a multiple of 2 and contain a sequence of score1, value1, score2, value2, scoreN, valueN");
        }
        if (isLower()) {
            Set<ZSetOperations.TypedTuple<Object>> svs = new LinkedHashSet<>(pair);
            for (int i = 0; i < pair; i++) {
                int index = i << 1;
                Object val = scoreAndVals[index + 1];
                // null value 跳过 (与 Actuator 多值版一致); score 支持 Number/数字字符串, 不强转 Double (避免 Integer/Long/BigDecimal CCE)
                if (val == null) continue;
                Object score = scoreAndVals[index];
                double s = (score instanceof Number n) ? n.doubleValue() : Double.parseDouble(String.valueOf(score));
                svs.add(ZSetOperations.TypedTuple.of(val, s));
            }
            if (svs.isEmpty()) return;
            redisTemplate.opsForZSet().add(key, svs);
            return;
        }
        byte[] k = keySerializer.serialize(key);
        // pipeline 路径走 lettuce c.zadd(K, Object...) 底层 ByteArrayCodec, val 必须是 byte[], score 为 Number.
        // 过滤 null val + score 归一为 double, 过滤后为空不发空 ZADD.
        List<Object> sv = new ArrayList<>(scoreAndVals.length);
        for (int i = 0; i < pair; i++) {
            int scoreIdx = i << 1;
            Object val = scoreAndVals[scoreIdx + 1];
            if (val == null) continue;
            Object score = scoreAndVals[scoreIdx];
            double s = (score instanceof Number n) ? n.doubleValue() : Double.parseDouble(String.valueOf(score));
            sv.add(s);
            sv.add(valueSerializer.serialize(val));
        }
        if (sv.isEmpty()) return;
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZADD_MULTI).arg1(k).extras(sv.toArray()).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
    }

    /**
     * 业务作用：从有序集合移除成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param vals 见方法语义
     * @return 命令的执行结果。
     */
    public long zRem(String key, Object... vals) {
        if (StringUtils.isBlank(key) || ColUtils.isEmpty(vals)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForZSet().remove(key, vals);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        byte[][] vs = ColUtils.toArray(byte[].class, vals, ColUtils.nonNull(), valueSerializer::serialize);
        // 过滤后全空: 不发空 ZREM (Redis 报参数错误), 返回 0
        if (vs.length == 0) return 0;
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREM_MULTI).arg1(k).extras(vs).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按字典序区间批量移除成员。
     * 仅当集合内所有成员分值相同时结果才有意义。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @return 命令的执行结果。
     */
    public long zRemRangeByLex(String key, String min, String max) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForZSet().removeRangeByLex(key, org.springframework.data.domain.Range.closed(min, max));
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        Range<byte[]> range = Range.create(
                Objects.requireNonNull(valueSerializer.serialize(min))
                , Objects.requireNonNull(valueSerializer.serialize(max)));
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREMRANGE_BY_LEX_RANGE).arg1(k).extras(range).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按排名区间批量移除成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * @return 命令的执行结果。
     */
    public long zRemRange(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForZSet().removeRange(key, start, end);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREMRANGE).arg1(k).longArg(start).longArg2(end).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按分值区间批量移除成员。
     * 常用于按时间戳分值裁剪过期数据。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @return 命令的执行结果。
     */
    public long zRemRangeByScore(String key, double min, double max) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForZSet().removeRangeByScore(key, min, max);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        Range<Double> rang = Range.create(min, max);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREMRANGE_BY_SCORE_RANGE).arg1(k).extras(rang).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按分值区间批量移除成员。
     * 常用于按时间戳分值裁剪过期数据。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param rang 见方法语义
     * @return 命令的执行结果。
     */
    public long zRemRangeByScore(String key, Range<Double> rang) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (Objects.isNull(rang)) {
            rang = Range.unbounded();
        }
        if (isLower()) {
            byte[] rawKey = keySerializer.serialize(key);
            org.springframework.data.domain.Range<Double> springRange = toSpringRange(rang);
            Long r = redisTemplate.execute(connection ->
                    connection.zSetCommands().zRemRangeByScore(rawKey, springRange), true);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        Range<Double> rg = rang;
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREMRANGE_BY_SCORE_RANGE).arg1(k).extras(rg).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：查询有序集合的成员数。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @return 命令的执行结果。
     */
    public long zCard(String key) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForZSet().zCard(key);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZCARD).arg1(k).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：统计分值落在给定区间内的成员数。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @return 命令的执行结果。
     */
    public long zCount(String key, double min, double max) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForZSet().count(key, min, max);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        Range<Double> range = Range.create(min, max);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZCOUNT_SCORE).arg1(k).extras(range).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：统计分值落在给定区间内的成员数。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param range 见方法语义
     * @return 命令的执行结果。
     */
    public long zCount(String key, Range<Double> range) {
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        if (Objects.isNull(range)) {
            range = Range.unbounded();
        }
        if (isLower()) {
            byte[] rawKey = keySerializer.serialize(key);
            org.springframework.data.domain.Range<Double> springRange = toSpringRange(range);
            Long r = redisTemplate.execute(connection -> connection.zSetCommands().zCount(rawKey, springRange), true);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        Range<Double> rg = range;
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZCOUNT_SCORE).arg1(k).extras(rg).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：按增量原子调整成员分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param delta 增减量
     * @return 命令的执行结果。
     */
    public double zIncrBy(String key, Object val, double delta) {
        if (StringUtils.isBlank(key) || Objects.isNull(val)) {
            return 0;
        }
        if (isLower()) {
            Double r = redisTemplate.opsForZSet().incrementScore(key, val, delta);
            return Objects.isNull(r) ? 0 : r;
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Double>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZINCRBY).arg1(k).arg3(v).longArg(Double.doubleToRawLongBits(delta)).future(lf));
        Double r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /**
     * 业务作用：以业务 nonce 原子调整 ZSET 成员分值；默认 7 天窗口内重复调用返回首次结果，不会再次改变分值。
     * nonce 必须由订单、成交等稳定业务事件推导，不能在每次重试时重新生成；成员沿用当前 valueSerializer，
     * 因此幂等入口与普通 zIncrBy 操作的是同一成员字节。窗口可由全局配置调整。
     *
     * @param key   目标 ZSET 键
     * @param val   目标成员
     * @param delta 分值增量
     * @param nonce 稳定业务事件的幂等标识
     * @return 该 nonce 首次执行后的分值。
     */
    public double zIncrByIdempotent(String key, Object val, double delta, String nonce) {
        return idempotentCounter.zsetIncrement(key, val, delta, nonce);
    }

    /**
     * 业务作用：按排名区间正序读取成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRange(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().range(key, start, end);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGE).arg1(k).longArg(start).longArg2(end).future(lf));
        List<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：按字典序区间读取成员。
     * 仅当集合内所有成员分值相同时结果才有意义。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRangeByLex(String key, String min, String max) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().rangeByLex(key
                    , org.springframework.data.domain.Range.closed(min, max));
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        Range<byte[]> range = Range.create(
                Objects.requireNonNull(valueSerializer.serialize(min))
                , Objects.requireNonNull(valueSerializer.serialize(max)));
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGEBYLEX).arg1(k).extras(range).future(lf));
        List<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：按字典序区间读取成员。
     * 仅当集合内所有成员分值相同时结果才有意义。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @param offset 偏移量
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRangeByLex(String key, String min, String max, int offset, int count) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().rangeByLex(key
                    , org.springframework.data.domain.Range.closed(min, max)
                    , org.springframework.data.redis.connection.Limit.limit().offset(offset).count(count));
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        Range<byte[]> range = Range.create(
                Objects.requireNonNull(valueSerializer.serialize(min))
                , Objects.requireNonNull(valueSerializer.serialize(max)));
        Limit limit = Limit.create(offset, count);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGEBYLEX_LIMIT).arg1(k).arg2(limit).extras(range).future(lf));
        List<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：按分值区间正序读取成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @param offset 偏移量
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRangeByScore(String key, double min, double max, int offset, int count) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().rangeByScore(key, min, max, offset, count);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        Range<Double> range = Range.create(min, max);
        Limit limit = Limit.create(offset, count);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGEBYSCORE_LIMIT).arg1(k).arg2(limit).extras(range).future(lf));
        List<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：按分值区间正序读取成员。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRangeByScore(String key, double min, double max) {
        return zRangeByScore(key, Range.create(min, max), null);
    }

    /**
     * 业务作用：按分值区间正序读取成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param range 见方法语义
     * @param limit 数量上限
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRangeByScore(String key, Range<Double> range, Limit limit) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (Objects.isNull(range)) {
            range = Range.unbounded();
        }
        if (isLower()) {
            byte[] rawKey = keySerializer.serialize(key);
            org.springframework.data.domain.Range<Double> springRange = toSpringRange(range);
            org.springframework.data.redis.connection.Limit springLimit = limit == null
                    ? org.springframework.data.redis.connection.Limit.unlimited()
                    : org.springframework.data.redis.connection.Limit.limit()
                    .offset(Math.toIntExact(limit.getOffset())).count(Math.toIntExact(limit.getCount()));
            Set<byte[]> values = redisTemplate.execute(connection ->
                    connection.zSetCommands().zRangeByScore(rawKey, springRange, springLimit), true);
            if (ColUtils.isEmpty(values)) return Collections.emptySet();
            Set<T> result = new LinkedHashSet<>(values.size());
            for (byte[] value : values) result.add((T) valueSerializer.deserialize(value));
            return result;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        Range<Double> rg = range;
        if (Objects.isNull(limit)) {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGEBYSCORE).arg1(k).extras(rg).future(lf));
        } else {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGEBYSCORE_LIMIT).arg1(k).arg2(limit).extras(rg).future(lf));
        }
        List<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：查询成员的正序排名。
     * 成员不存在时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public Long zRank(String key, Object val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);
        if (isLower()) {
            return redisTemplate.opsForZSet().rank(key, val);
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANK).arg1(k).arg3(v).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：按排名区间倒序读取成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRevRange(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().reverseRange(key, start, end);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANGE).arg1(k).longArg(start).longArg2(end).future(lf));
        List<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：按分值区间倒序读取成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @param offset 偏移量
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRevRangeByScore(String key, double min, double max, int offset, int count) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().reverseRangeByScore(key, min, max, offset, count);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        Range<Double> range = Range.create(min, max);
        Limit limit = Limit.create(offset, count);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANGEBYSCORE_LIMIT).arg1(k).arg2(limit).extras(range).future(lf));
        List<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：按分值区间倒序读取成员。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRevRangeByScore(String key, double min, double max) {
        return zRevRangeByScore(key, Range.create(min, max), null);
    }

    /**
     * 业务作用：按分值区间倒序读取成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param range 见方法语义
     * @param limit 数量上限
     * @return 命令的执行结果。
     */
    public <T> Set<T> zRevRangeByScore(String key, Range<Double> range, Limit limit) {
        if (StringUtils.isBlank(key)) {
            return Collections.emptySet();
        }
        if (Objects.isNull(range)) {
            range = Range.unbounded();
        }
        if (isLower()) {
            byte[] rawKey = keySerializer.serialize(key);
            org.springframework.data.domain.Range<Double> springRange = toSpringRange(range);
            org.springframework.data.redis.connection.Limit springLimit = limit == null
                    ? org.springframework.data.redis.connection.Limit.unlimited()
                    : org.springframework.data.redis.connection.Limit.limit()
                    .offset(Math.toIntExact(limit.getOffset())).count(Math.toIntExact(limit.getCount()));
            Set<byte[]> values = redisTemplate.execute(connection ->
                    connection.zSetCommands().zRevRangeByScore(rawKey, springRange, springLimit), true);
            if (ColUtils.isEmpty(values)) return Collections.emptySet();
            Set<T> result = new LinkedHashSet<>(values.size());
            for (byte[] value : values) result.add((T) valueSerializer.deserialize(value));
            return result;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<byte[]>>> lf = LettuceFuture.of();
        Range<Double> rg = range;
        // limit==null 走无 LIMIT op (与 zRangeByScore 一致): 否则把 null Limit 传给 lettuce → NPE/非法命令
        if (Objects.isNull(limit)) {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANGEBYSCORE).arg1(k).extras(rg).future(lf));
        } else {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANGEBYSCORE_LIMIT).arg1(k).arg2(limit).extras(rg).future(lf));
        }
        List<byte[]> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (byte[] r : rs) {
            set.add((T) valueSerializer.deserialize(r));
        }
        return set;
    }

    /**
     * 业务作用：查询成员的倒序排名。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public Long zRevRank(String key, Object val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);
        if (isLower()) {
            return redisTemplate.opsForZSet().reverseRank(key, val);
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANK).arg1(k).arg3(v).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：读取成员的分值。
     * 成员不存在时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @return 命令的执行结果。
     */
    public Double zScore(String key, Object val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);
        if (isLower()) {
            return redisTemplate.opsForZSet().score(key, val);
        }
        byte[] k = keySerializer.serialize(key);
        byte[] v = valueSerializer.serialize(val);
        LettuceFuture<RedisFuture<Double>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZSCORE).arg1(k).arg3(v).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：按排名区间正序读取成员及其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * @return 命令的执行结果。
     */
    public <E, T extends ZSetOperations.TypedTuple<E>> Set<T> zRangeWithScores(String key, long start, long end) {
        if (StringUtils.isEmpty(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().rangeWithScores(key, start, end);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGE_WITHSCORES).arg1(k).longArg(start).longArg2(end).future(lf));
        List<ScoredValue<byte[]>> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (ScoredValue<byte[]> r : rs) {
            set.add((T) new DefaultTypedTuple<>((E) valueSerializer.deserialize(r.getValue()), r.getScore()));
        }
        return set;
    }

    /**
     * 业务作用：按排名区间倒序读取成员及其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * @return 命令的执行结果。
     */
    public <E, T extends ZSetOperations.TypedTuple<E>> Set<T> zRevRangeWithScores(String key, long start, long end) {
        if (StringUtils.isEmpty(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().reverseRangeWithScores(key, start, end);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANGE_WITHSCORES).arg1(k).longArg(start).longArg2(end).future(lf));
        List<ScoredValue<byte[]>> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (ScoredValue<byte[]> r : rs) {
            set.add((T) new DefaultTypedTuple<>((E) valueSerializer.deserialize(r.getValue()), r.getScore()));
        }
        return set;
    }

    /**
     * 业务作用：按分值区间正序读取成员及其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @param offset 偏移量
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <E, T extends ZSetOperations.TypedTuple<E>> Set<T> zRangeByScoreWithScores(
            String key, double min, double max, int offset, int count) {
        if (StringUtils.isEmpty(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().rangeByScoreWithScores(key, min, max, offset, count);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf = LettuceFuture.of();
        Range<Double> range = Range.create(min, max);
        Limit limit = Limit.create(offset, count);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGEBYSCORE_WITHSCORES_LIMIT).arg1(k).arg2(limit).extras(range).future(lf));
        List<ScoredValue<byte[]>> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (ScoredValue<byte[]> r : rs) {
            set.add((T) new DefaultTypedTuple<>((E) valueSerializer.deserialize(r.getValue()), r.getScore()));
        }
        return set;
    }


    /**
     * 业务作用：按分值区间正序读取成员及其分值。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @return 命令的执行结果。
     */
    public <E, T extends ZSetOperations.TypedTuple<E>> Set<T> zRangeByScoreWithScores(String key, double min, double max) {
        return zRangeByScoreWithScores(key, Range.create(min, max), null);
    }

    /**
     * 业务作用：按分值区间正序读取成员及其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param range 见方法语义
     * @param limit 数量上限
     * @return 命令的执行结果。
     */
    public <E, T extends ZSetOperations.TypedTuple<E>> Set<T> zRangeByScoreWithScores(
            String key, Range<Double> range, Limit limit) {
        if (StringUtils.isEmpty(key)) {
            return Collections.emptySet();
        }
        if (Objects.isNull(range)) {
            range = Range.unbounded();
        }
        if (isLower()) {
            byte[] rawKey = keySerializer.serialize(key);
            org.springframework.data.domain.Range<Double> springRange = toSpringRange(range);
            org.springframework.data.redis.connection.Limit springLimit = limit == null
                    ? org.springframework.data.redis.connection.Limit.unlimited()
                    : org.springframework.data.redis.connection.Limit.limit()
                    .offset(Math.toIntExact(limit.getOffset())).count(Math.toIntExact(limit.getCount()));
            Set<Tuple> tuples = redisTemplate.execute(connection ->
                    connection.zSetCommands().zRangeByScoreWithScores(rawKey, springRange, springLimit), true);
            if (ColUtils.isEmpty(tuples)) return Collections.emptySet();
            Set<T> result = new LinkedHashSet<>(tuples.size());
            for (Tuple tuple : tuples) {
                result.add((T) new DefaultTypedTuple<>((E) valueSerializer.deserialize(tuple.getValue()), tuple.getScore()));
            }
            return result;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf = LettuceFuture.of();
        Range<Double> rg = range;
        if (Objects.isNull(limit)) {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGEBYSCORE_WITHSCORES).arg1(k).extras(rg).future(lf));
        } else {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZRANGEBYSCORE_WITHSCORES_LIMIT).arg1(k).arg2(limit).extras(rg).future(lf));
        }
        List<ScoredValue<byte[]>> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (ScoredValue<byte[]> r : rs) {
            set.add((T) new DefaultTypedTuple<>((E) valueSerializer.deserialize(r.getValue()), r.getScore()));
        }
        return set;
    }

    /**
     * 业务作用：按分值区间倒序读取成员及其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @param offset 偏移量
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <E, T extends ZSetOperations.TypedTuple<E>> Set<T> zRevRangeByScoreWithScores(
            String key, double min, double max, int offset, int count) {
        if (StringUtils.isEmpty(key)) {
            return Collections.emptySet();
        }
        if (isLower()) {
            return (Set<T>) redisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, min, max, offset, count);
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf = LettuceFuture.of();
        Range<Double> range = Range.create(min, max);
        Limit limit = Limit.create(offset, count);
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANGEBYSCORE_WITHSCORES_LIMIT).arg1(k).arg2(limit).extras(range).future(lf));
        List<ScoredValue<byte[]>> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (ScoredValue<byte[]> r : rs) {
            set.add((T) new DefaultTypedTuple<>((E) valueSerializer.deserialize(r.getValue()), r.getScore()));
        }
        return set;
    }

    /**
     * 业务作用：按分值区间倒序读取成员及其分值。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * @return 命令的执行结果。
     */
    public <E, T extends ZSetOperations.TypedTuple<E>> Set<T> zRevRangeByScoreWithScores(String key, double min, double max) {
        return zRevRangeByScoreWithScores(key, Range.create(min, max), null);
    }

    /**
     * 业务作用：按分值区间倒序读取成员及其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param range 见方法语义
     * @param limit 数量上限
     * @return 命令的执行结果。
     */
    public <E, T extends ZSetOperations.TypedTuple<E>> Set<T> zRevRangeByScoreWithScores(String key, Range<Double> range, Limit limit) {
        if (StringUtils.isEmpty(key)) {
            return Collections.emptySet();
        }
        if (Objects.isNull(range)) {
            range = Range.unbounded();
        }
        if (isLower()) {
            byte[] rawKey = keySerializer.serialize(key);
            org.springframework.data.domain.Range<Double> springRange = toSpringRange(range);
            org.springframework.data.redis.connection.Limit springLimit = limit == null
                    ? org.springframework.data.redis.connection.Limit.unlimited()
                    : org.springframework.data.redis.connection.Limit.limit()
                    .offset(Math.toIntExact(limit.getOffset())).count(Math.toIntExact(limit.getCount()));
            Set<Tuple> tuples = redisTemplate.execute(connection ->
                    connection.zSetCommands().zRevRangeByScoreWithScores(rawKey, springRange, springLimit), true);
            if (ColUtils.isEmpty(tuples)) return Collections.emptySet();
            Set<T> result = new LinkedHashSet<>(tuples.size());
            for (Tuple tuple : tuples) {
                result.add((T) new DefaultTypedTuple<>((E) valueSerializer.deserialize(tuple.getValue()), tuple.getScore()));
            }
            return result;
        }
        byte[] k = keySerializer.serialize(key);
        LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf = LettuceFuture.of();
        Range<Double> rg = range;
        if (Objects.isNull(limit)) {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANGEBYSCORE_WITHSCORES).arg1(k).extras(rg).future(lf));
        } else {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_ZREVRANGEBYSCORE_WITHSCORES_LIMIT).arg1(k).arg2(limit).extras(rg).future(lf));
        }
        List<ScoredValue<byte[]>> rs = lf.getFinally();
        Set<T> set = new LinkedHashSet<>(rs.size());
        for (ScoredValue<byte[]> r : rs) {
            set.add((T) new DefaultTypedTuple<>((E) valueSerializer.deserialize(r.getValue()), r.getScore()));
        }
        return set;
    }

    /* NOTE ------------------- script start ------------------------------------------------------------------------ */

    /**
     * 业务作用：把 Lua 脚本载入服务端缓存并取回其摘要。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param script Lua 脚本
     * @return 命令的执行结果。
     */
    public String scriptLoad(String script) {
        byte[] sc = keySerializer.serialize(script);
        Objects.requireNonNull(sc);
        if (isLower()) {
            return redisTemplate.execute(
                    RedisCallbackRecycler.<String>ofRecycle(SCRIPT_LOAD_CB)
                            .ref(0, sc), true);
        }
        LettuceFuture<RedisFuture<String>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SCRIPT_LOAD).arg1(sc).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：清空服务端的 Lua 脚本缓存。
     * 此后所有按摘要执行都会失败，直至脚本重新载入。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void scriptFlush() {
        if (isLower()) {
            redisTemplate.execute(RedisCallbackRecycler.<Object>ofRecycle(SCRIPT_FLUSH_CB), true);
            this.scriptShaCache.clear();
            return;
        }
        LettuceFuture<RedisFuture<String>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SCRIPT_FLUSH).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
        this.scriptShaCache.clear();
    }

    /**
     * 业务作用：终止正在执行的 Lua 脚本。
     * 已产生写入的脚本无法被终止，只能等待其完成或重启实例。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void scriptKill() {
        if (isLower()) {
            redisTemplate.execute(RedisCallbackRecycler.<Object>ofRecycle(SCRIPT_KILL_CB), true);
            return;
        }
        LettuceFuture<RedisFuture<String>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SCRIPT_KILL).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
    }

    /**
     * 业务作用：校验指定的脚本是否已经被保存在缓存当中
     * <a href="https://www.runoob.com/redis/scripting-script-exists.html">https://www.runoob.com/redis/scripting-script-exists.html</a>
     *
     * @param scripts lua脚本
     * @return 见上述说明。
     */
    public List<Boolean> scriptExists(String... scripts) {
        if (ColUtils.isEmpty(scripts)) {
            return Collections.emptyList();
        }
        if (isLower()) {
            return redisTemplate.execute(
                    RedisCallbackRecycler.<List<Boolean>>ofRecycle(SCRIPT_EXISTS_CB)
                            .ref(0, scripts), true);
        }
        LettuceFuture<RedisFuture<List<Boolean>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_SCRIPT_EXISTS).extras(scripts).future(lf));
        return lf.getFinally();
    }

    /**
     * 业务作用：执行 Lua 脚本。
     * 脚本在服务端原子执行，期间不会有其它命令插入，是实现复合原子操作的手段。
     *
     * @param script Lua 脚本
     * @param clazz 反序列化目标类型
     * @param args 脚本参数
     * @return 命令的执行结果。
     */
    public <T> T eval(String script, Class<?> clazz, Object... args) {
        return this.eval(script, clazz, null, args);
    }

    /**
     * 业务作用：执行 Lua 脚本。
     * 脚本在服务端原子执行，期间不会有其它命令插入，是实现复合原子操作的手段。
     *
     * @param script Lua 脚本
     * @param clazz 反序列化目标类型
     * @param keys 缓存键集合
     * @param args 脚本参数
     * @return 命令的执行结果。
     */
    public <T> T eval(String script, Class<?> clazz, String[] keys, Object... args) {
        Objects.requireNonNull(script);
        byte[][] ks = this.serializeScriptKeys(keys);
        byte[][] as = this.serializeScriptArgs(args);
        ReturnType returnType = ReturnType.fromJavaType(clazz);
        return this.decodeScriptResult(this.evalRaw(script, returnType, ks, as), clazz);
    }

    /**
     * 业务作用：以原始字节执行 nonce 幂等脚本，使 ZSET 成员和 HASH 字段保持与普通命令完全相同的序列化身份。
     * 该入口只交给内部幂等计数器，避免业务绕过 key 数量与同 slot 派生门禁。
     *
     * @param script Lua 文本
     * @param keys   已序列化目标键与账本键
     * @param args   已序列化的凭证、操作和数值参数
     * @return 已解码的三元素脚本结果。
     */
    private List<Object> evalIdempotentCounterScript(String script, byte[][] keys, byte[][] args) {
        return this.decodeScriptResult(this.evalRaw(script, ReturnType.MULTI, keys, args), List.class);
    }

    /**
     * 业务作用：在当前调用线程立即执行控制面 Lua，绕开业务命令批处理队列及其流量等待时间。
     * 该入口供租约、fencing 和调度状态迁移使用，脚本摘要失效时在同一连接路径回退到 EVAL。
     *
     * @param script Lua 脚本
     * @param clazz 反序列化目标类型
     * @param keys 缓存键集合
     * @param args 脚本参数
     * @param <T> 返回类型
     * @return 命令的执行结果。
     */
    public <T> T evalDirectConnection(String script, Class<?> clazz, String[] keys, Object... args) {
        Objects.requireNonNull(script);
        byte[][] ks = this.serializeScriptKeys(keys);
        byte[][] as = this.serializeScriptArgs(args);
        ReturnType returnType = ReturnType.fromJavaType(clazz);
        return this.decodeScriptResult(this.evalRawDirectConnection(script, returnType, ks, as), clazz);
    }

    /**
     * 业务作用：使用直发连接完成带 SHA 缓存的脚本调用，不让控制面延迟受业务批次排队影响。
     *
     * @param script Lua 脚本内容
     * @param returnType 服务端返回形态
     * @param ks 已序列化的键参数
     * @param as 已序列化的其它参数
     * @return 服务端原始结果；无返回值时为 null。
     */
    private Object evalRawDirectConnection(String script, ReturnType returnType, byte[][] ks, byte[][] as) {
        String sha = this.scriptShaCache.get(script);
        if (sha == null) {
            String loaded = this.scriptLoadDirectConnection(script);
            String existing = this.scriptShaCache.putIfAbsent(script, loaded);
            sha = existing == null ? loaded : existing;
        }
        try {
            return this.evalShaOnDirectConnection(sha, returnType, ks, as);
        } catch (Exception error) {
            if (!this.isNoScript(error)) throw error;
            // Cluster 每个节点维护自己的脚本缓存，目标 slot 首次命中时用 EVAL 在该节点补齐。
            return this.evalOnDirectConnection(script, returnType, ks, as);
        }
    }

    /**
     * 业务作用：通过 RedisTemplate 直发通道装载脚本摘要，不进入 RedisProxy 的业务批处理队列。
     *
     * @param script Lua 脚本内容
     * @return Redis 返回的脚本摘要。
     */
    private String scriptLoadDirectConnection(String script) {
        byte[] bytes = Objects.requireNonNull(RedisSerializer.string().serialize(script));
        String sha = redisTemplate.execute(
                RedisCallbackRecycler.<String>ofRecycle(SCRIPT_LOAD_CB).ref(0, bytes), true);
        if (StringUtils.isBlank(sha)) throw new IllegalStateException("SCRIPT LOAD returned blank sha");
        return sha;
    }

    /**
     * 业务作用：按脚本内容在直发通道执行状态迁移，作为目标 Redis 节点缺少摘要时的安全回退。
     *
     * @param script Lua 脚本内容
     * @param returnType 服务端返回形态
     * @param ks 已序列化的键参数
     * @param as 已序列化的其它参数
     * @return 服务端原始结果。
     */
    private Object evalOnDirectConnection(String script, ReturnType returnType, byte[][] ks, byte[][] as) {
        byte[] bytes = Objects.requireNonNull(RedisSerializer.string().serialize(script));
        return redisTemplate.execute(
                RedisCallbackRecycler.<Object>ofRecycle(EVAL_DIRECT_CB)
                        .ref(0, bytes)
                        .ref(1, returnType)
                        .ref(2, mergeScriptKeysAndArgs(ks, as))
                        .val(0, ks.length), true);
    }

    /**
     * 业务作用：按摘要在直发通道执行控制面状态迁移，服务端缺少脚本时由调用方回退到脚本内容。
     *
     * @param sha1 脚本摘要
     * @param returnType 服务端返回形态
     * @param ks 已序列化的键参数
     * @param as 已序列化的其它参数
     * @return 服务端原始结果。
     */
    private Object evalShaOnDirectConnection(String sha1, ReturnType returnType, byte[][] ks, byte[][] as) {
        return redisTemplate.execute(
                RedisCallbackRecycler.<Object>ofRecycle(EVALSHA_DIRECT_CB)
                        .ref(0, sha1)
                        .ref(1, returnType)
                        .ref(2, mergeScriptKeysAndArgs(ks, as))
                        .val(0, ks.length), true);
    }

    /**
     * 业务作用：执行脚本并保留服务端返回的原始字节形态，供需要自行选择反序列化器的复合命令使用。
     * 摘要失效时回退到脚本内容执行，保证服务端重启或清空脚本缓存后仍可继续工作。
     *
     * @param script     Lua 脚本内容
     * @param returnType 服务端返回形态
     * @param ks         已序列化的键参数
     * @param as         已序列化的其它参数
     * @return 服务端原始结果；无返回值时为 null。
     */
    private Object evalRaw(String script, ReturnType returnType, byte[][] ks, byte[][] as) {
        String sha = this.scriptShaCache.get(script);
        Object o;
        try {
            if (sha == null) {
                sha = this.loadScriptShaByCache(script);
            }
            o = this.evalShaDirect(sha, returnType, ks, as);
        } catch (Exception e) {
            if (!this.isNoScript(e)) {
                if (sha != null || this.scriptShaCache.containsKey(script)) {
                    throw e;
                }
                o = this.evalDirect(script, returnType, ks, as);
            } else {
                if (sha != null) {
                    this.scriptShaCache.remove(script, sha);
                }
                o = this.evalDirect(script, returnType, ks, as);
            }
        }
        return o;
    }

    /**
     * 业务作用：按脚本摘要执行已缓存的 Lua 脚本。
     * 服务端未缓存该脚本时报错，调用方需回退到按内容执行。
     *
     * @param sha1 见方法语义
     * @param clazz 反序列化目标类型
     * @param args 脚本参数
     * @return 命令的执行结果。
     */
    public <T> T evalSha(String sha1, Class<?> clazz, Object... args) {
        return this.evalSha(sha1, clazz, null, args);
    }

    /**
     * 业务作用：按脚本摘要执行已缓存的 Lua 脚本。
     * 服务端未缓存该脚本时报错，调用方需回退到按内容执行。
     *
     * @param sha1 见方法语义
     * @param clazz 反序列化目标类型
     * @param keys 缓存键集合
     * @param args 脚本参数
     * @return 命令的执行结果。
     */
    public <T> T evalSha(String sha1, Class<?> clazz, String[] keys, Object... args) {
        Objects.requireNonNull(sha1);
        ReturnType returnType = ReturnType.fromJavaType(clazz);
        Object o = this.evalShaDirect(sha1, returnType, this.serializeScriptKeys(keys), this.serializeScriptArgs(args));
        return this.decodeScriptResult(o, clazz);
    }

    /**
     * 业务作用：把脚本载入服务端并取回其摘要，此后按摘要执行可省去重传脚本内容。
     *
     * @param script 脚本内容
     * @return 脚本摘要。
     */
    private String loadScriptSha(String script) {
        String sha = this.scriptLoad(script);
        if (StringUtils.isBlank(sha)) {
            throw new IllegalStateException("SCRIPT LOAD returned blank sha");
        }
        return sha;
    }

    /**
     * 业务作用：取脚本摘要，本地无缓存时载入一次并缓存。
     * 缓存必要：同一段脚本在每次执行前都重新载入会白白多一次往返。
     *
     * @param script 脚本内容
     * @return 脚本摘要。
     */
    private String loadScriptShaByCache(String script) {
        FunctionRecycler<String, String> loader = FunctionRecycler
                .<String, String>of(SCRIPT_SHA_LOADER)
                .ref(0, this);
        try {
            return this.scriptShaCache.computeIfAbsent(script, loader);
        } finally {
            loader.recycle();
        }
    }

    /**
     * 业务作用：按脚本内容直接执行，用于摘要不可用时的回退路径。
     *
     * @param script     脚本内容
     * @param returnType 期望的返回形态
     * @param ks         键参数
     * @param as         其余参数
     * @return 脚本执行结果。
     */
    private Object evalDirect(String script, ReturnType returnType, byte[][] ks, byte[][] as) {
        byte[] s = Objects.requireNonNull(RedisSerializer.string().serialize(script));
        Object o;
        if (isLower()) {
            o = redisTemplate.execute(
                    RedisCallbackRecycler.<Object>ofRecycle(EVAL_DIRECT_CB)
                            .ref(0, s)
                            .ref(1, returnType)
                            .ref(2, mergeScriptKeysAndArgs(ks, as))
                            .val(0, ks.length), true);
        } else {
            ScriptOutputType type = LettuceConverters.toScriptOutputType(returnType);
            LettuceFuture<RedisFuture<Object>> lf = LettuceFuture.of();
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_EVAL).arg1(s).arg3(type).extras(new Object[]{ks, as}).future(lf));
            o = lf.getFinally();
        }
        return o;
    }

    /**
     * 业务作用：按摘要执行已缓存的脚本。
     * 服务端未缓存时抛出特定错误，由调用方识别并回退到按内容执行——
     * 服务端重启或清过脚本缓存后必然走到这条回退路径。
     *
     * @param sha1       脚本摘要
     * @param returnType 期望的返回形态
     * @param ks         键参数
     * @param as         其余参数
     * @return 脚本执行结果。
     */
    private Object evalShaDirect(String sha1, ReturnType returnType, byte[][] ks, byte[][] as) {
        Object o;
        if (isLower()) {
            o = redisTemplate.execute(
                    RedisCallbackRecycler.<Object>ofRecycle(EVALSHA_DIRECT_CB)
                            .ref(0, sha1)
                            .ref(1, returnType)
                            .ref(2, mergeScriptKeysAndArgs(ks, as))
                            .val(0, ks.length), true);
        } else {
            ScriptOutputType type = LettuceConverters.toScriptOutputType(returnType);
            LettuceFuture<RedisFuture<Object>> lf = LettuceFuture.of();
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_EVALSHA).arg2(sha1).arg3(type).extras(new Object[]{ks, as}).future(lf));
            o = lf.getFinally();
        }
        return o;
    }

    /**
     * 业务作用：按键的序列化方式转换脚本的键参数。
     *
     * @param keys 键数组
     * @return 序列化后的字节数组。
     */
    private byte[][] serializeScriptKeys(String[] keys) {
        if (ColUtils.isEmpty(keys)) {
            return LettucePipeline.EMPTY_BYTE2;
        }
        byte[][] ks = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            ks[i] = keySerializer.serialize(keys[i]);
        }
        return ks;
    }

    /**
     * 业务作用：按值的序列化方式转换脚本的其余参数。
     * 键与其余参数用不同的序列化方式，混用会让脚本读到与预期不符的内容。
     *
     * @param args 参数数组
     * @return 序列化后的字节数组。
     */
    private byte[][] serializeScriptArgs(Object[] args) {
        if (ColUtils.isEmpty(args)) {
            return LettucePipeline.EMPTY_BYTE2;
        }
        byte[][] as = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            as[i] = RedisSerializer.string().serialize(String.valueOf(args[i]));
        }
        return as;
    }

    /**
     * 业务作用：把键参数与其余参数按脚本执行命令要求的顺序合并成一个数组。
     * 顺序固定为「先全部键、后全部其余参数」，颠倒会让脚本按错误的下标取值。
     *
     * @param ks 键参数
     * @param as 其余参数
     * @return 合并后的参数数组。
     */
    private static byte[][] mergeScriptKeysAndArgs(byte[][] ks, byte[][] as) {
        if (ks.length + as.length == 0) {
            return LettucePipeline.EMPTY_BYTE2;
        }
        if (ks.length == 0) {
            return as;
        }
        if (as.length == 0) {
            return ks;
        }
        byte[][] kas = new byte[ks.length + as.length][];
        System.arraycopy(ks, 0, kas, 0, ks.length);
        System.arraycopy(as, 0, kas, ks.length, as.length);
        return kas;
    }

    /**
     * 业务作用：把脚本返回的原始结果按期望类型还原。
     *
     * @param o     原始结果
     * @param clazz 期望类型
     * @param <T>   期望类型
     * @return 还原后的结果。
     */
    private <T> T decodeScriptResult(Object o, Class<?> clazz) {
        if (Objects.isNull(o) || ClassUtils.isAssignable(Boolean.class, clazz) || ClassUtils.isAssignable(Long.class, clazz)) {
            return (T) o;
        }
        if (ClassUtils.isAssignable(List.class, clazz)) {
            List<Object> rs = (List<Object>) o;
            List<Object> list = new ArrayList<>(rs.size());
            for (Object r : rs) {
                list.add(r instanceof byte[] bs ? RedisSerializer.string().deserialize(bs) : r);
            }
            return (T) list;
        }
        return (T) RedisSerializer.string().deserialize((byte[]) o);
    }

    /**
     * 业务作用：判定异常是否为「服务端未缓存该脚本」，识别出来才能回退到按内容执行。
     *
     * @param t 待判定的异常
     * @return 属于脚本未缓存返回 true。
     */
    private boolean isNoScript(Throwable t) {
        return hasRedisError(t, "NOSCRIPT");
    }

    /**
     * 业务作用：沿异常因果链识别 Redis 错误码或稳定错误片段，避免连接层包装后误判失败类型。
     *
     * @param t      待检查的异常
     * @param marker Redis 错误码或错误片段
     * @return 任一层异常消息包含该标记时返回 true。
     */
    private static boolean hasRedisError(Throwable t, String marker) {
        String expected = marker.toUpperCase(Locale.ROOT);
        while (t != null) {
            String message = t.getMessage();
            if (message != null && message.toUpperCase(Locale.ROOT).contains(expected)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /* NOTE ------------------- publish/subscribe start ------------------------------------------------------------- */

    private volatile RedisMessageListenerContainer redisMessageListenerContainer;
    private volatile boolean ownsRedisMessageListenerContainer;
    /* 缓存频道和消费函数. ConcurrentHashMap + CopyOnWriteArrayList: 运行期并发 subscribe/unsubscribe 安全 (消息投递由 Spring container 处理, 不遍历本 map) */
    private final Map<String, List<KeyValue<Topic, MessageListener>>> topicMap = new ConcurrentHashMap<>();

    private final Lock listenerContainerLock = new ReentrantLock();

    /**
     * 业务作用：注入由外部容器管理的发布订阅监听容器。
     * 外部容器的连接工厂必须与本代理一致，避免多数据源消息被接到错误实例。
     *
     * @param container 已完成基础配置的监听容器
     * 返回: 无返回值；停机时不会由本代理关闭该容器。
     */
    public void setRedisMessageListenerContainer(RedisMessageListenerContainer container) {
        Objects.requireNonNull(container, "container must not be null");
        if (container.getConnectionFactory() != redisTemplate.getConnectionFactory()) {
            throw new IllegalArgumentException("RedisMessageListenerContainer connection factory does not match RedisProxy");
        }
        listenerContainerLock.lock();
        try {
            if (this.redisMessageListenerContainer != null && this.redisMessageListenerContainer != container) {
                throw new IllegalStateException("RedisMessageListenerContainer must be configured before first use");
            }
            this.redisMessageListenerContainer = container;
            this.ownsRedisMessageListenerContainer = false;
        } finally {
            listenerContainerLock.unlock();
        }
    }

    /**
     * 业务作用：惰性建出发布订阅的监听容器并缓存。
     * 容器持有独立连接与线程，惰性建出使不使用发布订阅的应用不承担这份开销。
     *
     * <p>参数说明: 无。
     *
     * @return 发布订阅监听容器。
     */
    @SuppressWarnings("ConstantConditions")
    private RedisMessageListenerContainer redisMessageListenerContainer() {
        if (Objects.nonNull(redisMessageListenerContainer)) return redisMessageListenerContainer;
        listenerContainerLock.lock();
        try {
            if (Objects.nonNull(redisMessageListenerContainer)) return redisMessageListenerContainer;
            RedisMessageListenerContainer bean = ContextUtils.getBeanOrNull(RedisMessageListenerContainer.class);
            RedisMessageListenerContainer container;
            boolean owned;
            if (Objects.nonNull(bean) && bean.getConnectionFactory() == redisTemplate.getConnectionFactory()) {
                container = bean;
                owned = false;
            } else {
                container = new RedisMessageListenerContainer();
                container.setConnectionFactory(redisTemplate.getConnectionFactory());
                container.setTaskExecutor(getExecutor());
                container.afterPropertiesSet();
                owned = true;
            }
            if (!container.isRunning()) {
                container.start();
                for (int i = 0; i < 2000 && !container.isRunning(); i++) {
                    ReflectUtils.sleep(5);
                }
            }
            if (!container.isRunning()) {
                if (owned) container.stop();
                throw new IllegalStateException("RedisMessageListenerContainer did not start");
            }
            this.ownsRedisMessageListenerContainer = owned;
            return this.redisMessageListenerContainer = container;
        } finally {
            listenerContainerLock.unlock();
        }
    }

    /**
     * 业务作用：暴露发布订阅监听容器，供业务自行注册监听。
     *
     * <p>参数说明: 无。
     *
     * @return 发布订阅监听容器。
     */
    public RedisMessageListenerContainer getRedisMessageListenerContainer() {
        return Objects.isNull(redisMessageListenerContainer) ? this.redisMessageListenerContainer() : redisMessageListenerContainer;
    }

    /**
     * 业务作用：订阅频道。
     *
     * @param channel 频道名
     * @param consumer 消费者名
     * 返回: 无返回值。
     */
    public <P> void subscribe(String channel, Consumer<P> consumer) {
        subscribe(true, channel, consumer);
    }

    /**
     * 业务作用：订阅频道。
     *
     * @param enableLog 见方法语义
     * @param channel 频道名
     * @param consumer 消费者名
     * 返回: 无返回值。
     */
    <P> void subscribe(boolean enableLog, String channel, Consumer<P> consumer) {
        if (StringUtils.isBlank(channel)) {
            throw new IllegalArgumentException("channel must not be blank");
        }
        Objects.requireNonNull(consumer, "consumer must not be null");
        Topic topic = ChannelTopic.of(channel);
        MessageListener listener = (message, pattern) -> {
            try {
                P p;
                try {
                    p = (P) valueSerializer.deserialize(message.getBody());
                } catch (Throwable e) {
                    p = (P) RedisSerializer.string().deserialize(message.getBody());
                }
                consumer.accept(p);
            } catch (Throwable t) {
                log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
            }
        };
        topicMap.compute(channel, (key, listeners) -> {
            getRedisMessageListenerContainer().addMessageListener(listener, topic);
            if (listeners == null) listeners = new CopyOnWriteArrayList<>();
            listeners.add(KeyValue.just(topic, listener));
            return listeners;
        });
        if (enableLog) log.info("{} Subscribed to channel {}", this.qualifier, channel);
    }

    /**
     * 业务作用：取消订阅频道。
     *
     * @param channel 频道名
     * 返回: 无返回值。
     */
    public void unsubscribe(String channel) {
        unsubscribe(true, channel);
    }

    /**
     * 业务作用：取消订阅频道。
     *
     * @param enableLog 见方法语义
     * @param channel 频道名
     * 返回: 无返回值。
     */
    void unsubscribe(boolean enableLog, String channel) {
        List<KeyValue<Topic, MessageListener>> kvs = topicMap.remove(channel);
        if (Objects.isNull(kvs)) return;
        kvs.forEach(kv -> {
            Topic topic = kv.getKey();
            MessageListener listener = kv.getValue();
            getRedisMessageListenerContainer().removeMessageListener(listener, topic);
        });
        if (enableLog) log.info("{} Unsubscribed from channel {}", this.qualifier, channel);
    }

    /**
     * 业务作用：向 Stream 发布一条业务事件。
     * 事件由订阅方按消费组读取；Stream 需配合长度裁剪，否则无限增长。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param channel 频道名
     * @param message 消息体
     * @return 命令的执行结果。
     */
    public long pub(String channel, Object message) {
        if (StringUtils.isBlank(channel) || message == null) {
            return 0;
        }
        if (isLower()) {
            return redisTemplate.convertAndSend(channel, message);
        }
        byte[] cnl = RedisSerializer.string().serialize(channel);
        byte[] msg = valueSerializer.serialize(message);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_PUB).arg1(cnl).arg3(msg).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0 : r;
    }

    /* NOTE ------------------- stream start ------------------------------------------------------------------------ */
    /* ------------------- <a href="https://www.runoob.com/redis/redis-stream.html"/> -------------------------- */

    public static final String STREAM_EVENT = "msg";

    private final ErrorHandler errorHandler = e -> {
        Object factory = this.getRedisTemplate().getConnectionFactory();
        if (!(factory instanceof LettuceConnectionFactory lettuceFactory) || lettuceFactory.isRunning()) {
            log.error(e.getMessage(), e);
        }
    };
    /* 缓存stream订阅的Subscription，以便执行unsubscribe */
    private final ConcurrentMap<String, CopyOnWriteArrayList<Subscription>> streamSubscriptionMap = new ConcurrentHashMap<>();
    /* 缓存(stream + / + group, (event, consumer))，同一个stream/group的所有消费，由事件驱动消费 */
    private final ConcurrentMap<String, ConcurrentMap<String, KV<TypeReference, BiConsumer>>> streamGroupEventConsumerMap = new ConcurrentHashMap<>();
    /* 标记已完成底层注册的事件分发订阅，防止同一 stream/group 的多个 event 重复拉取同一批消息。 */
    private final Set<String> eventDispatchSubscriptions = new ConcurrentHashSet<>();
    /* 缓存(stream, (listener, group))，同一个stream/group使用同一个listener */
    private final ConcurrentMap<String, ConcurrentMap<StreamListener, String>> listenerGroupMap = new ConcurrentHashMap<>();

    private volatile Set<String> streamAutoTrimCache;
    private final Lock trimCacheLock = new ReentrantLock();

    /**
     * 业务作用：初始化需要周期裁剪的 Stream 登记集，并只创建一个本实例的裁剪任务。
     * 同一 Stream 重复登记不会重复创建调度任务。
     *
     * <p>参数说明: 无。
     *
     * @return 记录集。
     */
    private Set<String> initTrimCache() {
        if (Objects.nonNull(streamAutoTrimCache)) return streamAutoTrimCache;
        trimCacheLock.lock();
        try {
            if (Objects.nonNull(streamAutoTrimCache)) return streamAutoTrimCache;
            streamAutoTrimCache = new ConcurrentHashSet<>();
            Action action = () -> {
                // 动态关闭后立即停止产生删除副作用，已登记的 Stream 保留，重新开启时可继续使用。
                if (!this.stream.isAutoTrimEnabled()) return;
                // 自动裁剪，自动数据过期
                long st = System.currentTimeMillis();
                long millis = st - this.stream.getDataExpireMillis();
                // 独立批次: openNested — 非嵌套(定时线程常态)复用 CACHE 单例零分配, 万一嵌套则隔离; 仍 try/finally 兜底
                LettucePipeline.Actuator actuator = LettucePipeline.openNested(this);
                try {
                    for (String s : streamAutoTrimCache) {
                        actuator.xTrimMinId(s, millis);
                    }
                    actuator.pipeline();
                } finally {
                    actuator.clearSession();
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}] stream{} exec trim cost time: {}ms", this.qualifier
                            , ObjMprUtils.toString(streamAutoTrimCache), System.currentTimeMillis() - st);
                }
            };
            // 裁剪是幂等外部副作用，各实例各自调度可避免依赖不带 fencing 的应用侧选主。
            TimingWheel.exec(stream.getAutoTrimRate(), stream.getAutoTrimRate(), this.streamTrimTaskName, action);
            return streamAutoTrimCache;
        } finally {
            trimCacheLock.unlock();
        }
    }

    /**
     * 业务作用：stream数据流自动裁剪
     *
     * @param stream 流名
     */
    public void streamAutoTrim(String stream) {
        if (StringUtils.isBlank(stream) || !this.stream.isAutoTrimEnabled()
                || this.stream.getAutoTrimExcludes().contains(stream)) return;
        Set<String> streams = initTrimCache();
        if (streams.contains(stream)) return;
        if (streams.add(stream)) {
            TimeUnit unit = TimeUnit.MILLISECONDS;
            String ms;
            long hours = unit.toHours(this.stream.getDataExpireMillis());
            if (hours > 0) {
                ms = hours + "h";
            } else {
                ms = unit.toMinutes(this.stream.getDataExpireMillis()) + "m";
            }
            log.info("[{}] stream[{}] auto trim started, period {}s, data expire {}", this.qualifier
                    , stream, unit.toSeconds(this.stream.getAutoTrimRate()), ms);
        }
    }

    @SuppressWarnings("rawtypes")
    private volatile StreamMessageListenerContainer streamMessageListenerContainer;
    private volatile boolean ownsStreamMessageListenerContainer;

    /**
     * 业务作用：注入由外部容器管理的 Stream 监听容器。
     * 本代理只负责登记订阅，不在停机时关闭外部容器。
     *
     * @param container 已按本代理序列化方式配置的监听容器
     * 返回: 无返回值；调用方负责容器生命周期。
     */
    @Synchronized
    public void setStreamMessageListenerContainer(StreamMessageListenerContainer container) {
        StreamMessageListenerContainer required = Objects.requireNonNull(container, "container must not be null");
        if (this.streamMessageListenerContainer != null && this.streamMessageListenerContainer != required) {
            throw new IllegalStateException("StreamMessageListenerContainer must be configured before first use");
        }
        this.streamMessageListenerContainer = required;
        this.ownsStreamMessageListenerContainer = false;
    }

    /**
     * 业务作用：惰性建出 Stream 消费的监听容器并缓存。
     *
     * <p>参数说明: 无。
     *
     * @return Stream 监听容器。
     */
    @Synchronized
    @SuppressWarnings({"ConstantConditions", "rawtypes"})
    private StreamMessageListenerContainer streamMessageListenerContainer() {
        if (Objects.nonNull(streamMessageListenerContainer)) return streamMessageListenerContainer;
        var options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                // 拉取消息超时时间
                .pollTimeout(Duration.ofMillis(stream.getPollTimeout()))
                // 批量抓取消息
                .batchSize(stream.getBatchSize())
                // stream反序列化
                .keySerializer(keySerializer)
                // field反序列化
                .hashKeySerializer(hashKeySerializer)
                // 消息反序列化
                .hashValueSerializer(hashValueSerializer)
                // 设置线程池
                .executor(getExecutor())
                .build();
        StreamMessageListenerContainer container = BatchStreamMessageListenerContainer.create(
                redisTemplate.getConnectionFactory(), options);
        container.start();
        for (int i = 0; i < 2000 && !container.isRunning(); i++) {
            ReflectUtils.sleep(5);
        }
        if (!container.isRunning()) {
            container.stop();
            throw new IllegalStateException("StreamMessageListenerContainer did not start");
        }
        this.ownsStreamMessageListenerContainer = true;
        return streamMessageListenerContainer = container;
    }

    /**
     * 业务作用：暴露 Stream 监听容器，供业务自行注册消费。
     *
     * <p>参数说明: 无。
     *
     * @return Stream 监听容器。
     */
    @SuppressWarnings("rawtypes")
    public StreamMessageListenerContainer getStreamMessageListenerContainer() {
        return Objects.isNull(streamMessageListenerContainer) ? this.streamMessageListenerContainer() : streamMessageListenerContainer;
    }

    /**
     * 业务作用：给 {@link #subscribe(String, String, StreamListener)} 选 container:
     * <ul>
     *   <li>yml stream.group.{stream}.{group} 没配 batchSize / pollTimeout 覆盖 → 走全局共享 container</li>
     *   <li>配了任一覆盖 → 走 dedicatedContainers 里该 (stream, group) 独享的 container,
     *       readOptions (BLOCK/COUNT) 按 group 配置真正生效</li>
     * </ul>
     * dedicated container 按需懒加载 (computeIfAbsent), 同一 (stream, group) 多次 subscribe (consumers&gt;1)
     * 共用同一个 dedicated container — 它本就为这个 group 量身定制, 应该共享。
     * @param stream Stream 键
     * @param group  消费组名
     * @return 见上述说明。
     */
    @SuppressWarnings("rawtypes")
    private StreamMessageListenerContainer pickContainer(String stream, String group) {
        NasaLettuceConfig.Group cfg = this.groupConfig(stream, group);
        // 没有专属拉取参数时复用全局容器，避免为每个消费组额外维护调度状态。
        if (cfg.getBatchSize() == null && cfg.getPollTimeout() == null) {
            return this.getStreamMessageListenerContainer();
        }
        // 配了覆盖 → 按 (stream, group) 缓存独立 container
        String key = stream + "/" + group;
        return dedicatedContainers.computeIfAbsent(key, k -> {
            int bs = cfg.getBatchSize() != null ? cfg.getBatchSize() : this.stream.getBatchSize();
            int pt = cfg.getPollTimeout() != null ? cfg.getPollTimeout() : this.stream.getPollTimeout();
            BatchStreamMessageListenerContainer<String, MapRecord<String, Object, Object>> c = createListenerContainer(pt, bs);
            // 注册订阅前先启动专属容器，否则新任务不会进入执行器。
            c.start();
            log.info("[{}] dedicated stream container created for stream={} group={} batchSize={} pollTimeout={}",
                    this.qualifier, stream, group, bs, pt);
            return c;
        });
    }

    /**
     * 业务作用：工厂: 创建一个独立的 stream listener container, 用指定的 pollTimeout / batchSize。
     * <p>
     * 给两类场景使用:
     * <ol>
     *   <li>{@link RedisPartition} per-group container — 每个分区组一个独立 container,
     *       readOptions 按 partition.groups.{group} 配置生效</li>
     *   <li>{@link #subscribe(String, String, StreamListener)} 的 dedicated container —
     *       某个 (stream, group) 在 yml stream.group.{stream}.{group} 配了 batchSize/pollTimeout
     *       覆盖时, 单独走一个 container 而不是全局共享 container</li>
     * </ol>
     * Spring container 的 readOptions (BLOCK/COUNT) 是 container 启动时定型的, 一个 container 内
     * 所有 task 共享同一份, 只能通过多个独立 container 实现 per-(stream,group) 隔离。
     * <p>
     * <b>资源说明</b>: container 自身是 ~1-2KB 的轻量壳子, 共享 RedisProxy 的:
     * <ul>
     *   <li>{@link #getExecutor()} 返回的 executor (优先识别业务方注入的虚拟线程池)</li>
     *   <li>{@link RedisTemplate#getConnectionFactory()} 的 Lettuce 连接</li>
     *   <li>key / hashKey / hashValue 三个 serializer (与 publish 端对称, 反序列化能还原原始类型)</li>
     * </ul>
     * 不引入额外线程池 / 不开新 Lettuce 连接。
     * <p>
     * <b>调用方负责</b>: 拿到 container 后调 {@code container.start()} 启动 task 调度,
     * 调 {@code container.register(req, listener[, lifecycle])} 注册消费, shutdown 时调 {@code container.stop()}。
     *
     * @param pollTimeout XREADGROUP BLOCK 超时 ms (与 RedisProxy 全局 stream.pollTimeout 解耦)
     * @param batchSize   XREADGROUP COUNT (与 RedisProxy 全局 stream.batchSize 解耦)
     * @return 新建的 BatchStreamMessageListenerContainer 实例 (未 start, 由调用方决定启动时机)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public BatchStreamMessageListenerContainer<String, MapRecord<String, Object, Object>> createListenerContainer(
            int pollTimeout, int batchSize) {
        var options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofMillis(pollTimeout))
                .batchSize(batchSize)
                .keySerializer(keySerializer)
                .hashKeySerializer(hashKeySerializer)
                .hashValueSerializer(hashValueSerializer)
                .executor(getExecutor())
                .build();
        BatchStreamMessageListenerContainer<String, MapRecord<String, Object, Object>> c =
                BatchStreamMessageListenerContainer.create(redisTemplate.getConnectionFactory(), options);
        // ManagedRunner 数上限, 普通操作走共享连接不占池, 从 pipelinePool.maxIdle 取参考值
        c.setMaxRunners(this.resolveMaxRunners());
        return c;
    }

    /**
     * 业务作用：ManagedRunner 数量上限。
     * <p>
     * 普通操作 (XREADGROUP / publish / ack 等) 走 Lettuce 共享连接 (Netty 多路复用),
     * 不占 pipeline 连接池, runner 数量不再受连接池约束。
     * 使用 pipelinePool.maxIdle (实际复用连接数) 作为参考上限, 未配置时 fallback 32。
     *
     * @return 见上述说明。
     */
    private int resolveMaxRunners() {
        if (pipelinePool != null) {
            int max = pipelinePool.getMaxIdle();
            if (max > 0) return max;
        }
        return 32;
    }

    static final Function<String, RecycleLinkedList<Object>> FUNC = s -> RecycleLinkedList.of();
    static final Function<String, RecycleLinkedList<KV<String, Object>>> FUNC2 = s -> RecycleLinkedList.of();
    static final Function<String, RecycleLinkedList<String>> MID_FUNC = s -> RecycleLinkedList.of();

    /**
     * streamListener group 路径异步事件分发的 Consumer 策略, 无状态全局共享, 配合 {@link ActionRecycler} 零 GC 派发.
     * ref(0)=bico, ref(1)=event, ref(2)=list, ref(3)=remaining, ref(4)=recycleMaps.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final Consumer<ActionRecycler> STREAM_GROUP_ASYNC_CON = ar -> {
        BiConsumer bico = ar.ref(0);
        String event = ar.ref(1);
        RecycleLinkedList list = ar.ref(2);
        AtomicInteger remaining = ar.ref(3);
        Runnable recycleMaps = ar.ref(4);
        try {
            bico.accept(event, list);
        } finally {
            STREAM_EVENT_INFLIGHT.decrementAndGet();
            if (remaining.decrementAndGet() <= 0) recycleMaps.run();
        }
    };

    /**
     * streamListenerNonGroup batch 路径的 Consumer 策略, 无状态全局共享.
     * ref(0)=proxy, ref(1)=list, ref(2)=kv, ref(3)=mids, ref(4)=stream, ref(5)=event, ref(6)=remaining, ref(7)=recycle.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final Consumer<ActionRecycler> STREAM_NONGROUP_BATCH_CON = ar -> {
        RedisProxy proxy = ar.ref(0);
        RecycleLinkedList list = ar.ref(1);
        KV<TypeReference, BiConsumer> kv = ar.ref(2);
        RecycleLinkedList<String> mids = ar.ref(3);
        String stream = ar.ref(4);
        String event = ar.ref(5);
        AtomicInteger remaining = ar.ref(6);
        Action recycle = ar.ref(7);
        // 反序列化在消费线程做
        proxy.deserializeBatchList(list, kv.getKey(), stream);
        RedisProxyHolder.setRedisProxy(proxy);
        RedisProxyHolder.setStreamRecordId(mids);
        try {
            kv.getValue().accept(event, list);
        } catch (Throwable t) {
            log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
        } finally {
            RedisProxyHolder.clear();
            STREAM_EVENT_INFLIGHT.decrementAndGet();
            if (remaining.decrementAndGet() <= 0) recycle.run();
        }
    };

    /**
     * streamListenerNonGroup single 路径的 Consumer 策略, 无状态全局共享.
     * ref(0)=proxy, ref(1)=cm, ref(2)=task, ref(3)=stream, ref(4)=remaining, ref(5)=recycle (可能为 null, 无 batch 时).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final Consumer<ActionRecycler> STREAM_NONGROUP_SINGLE_CON = ar -> {
        RedisProxy proxy = ar.ref(0);
        ConcurrentMap<String, KV<TypeReference, BiConsumer>> cm = ar.ref(1);
        KV<String, KV<String, Object>> task = ar.ref(2);
        String stream = ar.ref(3);
        AtomicInteger remaining = ar.ref(4);
        Action recycle = ar.ref(5);
        try {
            proxy.consumeSingleTask(cm, task, stream);
        } finally {
            STREAM_EVENT_INFLIGHT.decrementAndGet();
            if (remaining.decrementAndGet() <= 0 && recycle != null) recycle.run();
        }
    };

    /**
     * 业务作用：取 stream/group 的配置, 不存在时返回 DEFAULT
     * group 必须非空, 由调用方保证
     *
     * @param stream Stream 键
     * @param group  消费组名
     */
    private NasaLettuceConfig.Group groupConfig(String stream, String group) {
        return MapUtils.getObject(this.stream.getGroup().get(stream), group, NasaLettuceConfig.Group.DEFAULT);
    }

    /**
     * 业务作用：为一个 Stream 与消费组建出监听器：解码消息、写入线程上下文并投递业务回调。
     * <p>
     * 消费组的确认语义由 {@link NasaLettuceConfig.Group#isAutoAcknowledge()} 决定：开启时使用
     * XREADGROUP NOACK，消息在回调开始前就不会进入 PEL，属于至多一次；关闭时业务必须在成功后显式调用
     * {@link #ack(String, String, String...)}，未确认消息才可由接管流程重新投递。
     * <p>
     * 无论成败都在末尾清理线程上下文——消费线程会被复用，残留会串到下一条消息。
     *
     * @param stream Stream 键
     * @param group  消费组名
     * @return 监听器。
     */
    private StreamListener streamListener(String stream, String group) {
        if (Objects.isNull(group)) group = "";
        ConcurrentMap<StreamListener, String> lgm = listenerGroupMap.get(stream);
        if (Objects.nonNull(lgm)) for (Map.Entry<StreamListener, String> entry : lgm.entrySet()) {
            // 同一个 stream + "/" + group的所有event使用相同的listener
            if (group.equals(entry.getValue())) return entry.getKey();
        }
        // non-group + executor 开启 → 走消息级并行 listener (socket-center 等高吞吐场景)
        boolean nonGroup = group.isEmpty();
        if (nonGroup && this.stream.isNonGroupExecutorEnable()) {
            return streamListenerNonGroup(stream);
        }
        Executor executor = getExecutor();
        String key = stream + "/" + group;
        // 闭包捕获: 多事件时是否启用线程池并发 (与 consumers 数量无关)
        boolean eventExecutorEnable = nonGroup
                ? false
                : groupConfig(stream, group).isEventExecutorEnable();

        BatchStreamListener listener = messages -> {

            if (messages.isEmpty()) return;

            // 同一stream和group，使用同一个监听端点（批量消费）
            ConcurrentMap<String, KV<TypeReference, BiConsumer>> cm = streamGroupEventConsumerMap.get(key);
            if (MapUtils.isEmpty(cm)) return;

            // 就一条消息, 走快速路径: 不分组, 不创建 eventObjMap/eventMidMap, 直接消费
            if (messages.size() == 1) {
                Record record = (Record) messages.getFirst();
                String recordId = record.getId().getValue();
                Map<String, Object> map = (Map<String, Object>) record.getValue();
                RedisProxyHolder.setRedisProxy(this);
                try {
                    for (Map.Entry<String, ?> entry : map.entrySet()) {
                        String event = entry.getKey();
                        KV<TypeReference, BiConsumer> kv = cm.get(event);
                        if (Objects.isNull(kv)) continue;
                        // 反序列化 + 探测拆包 (publish 端 wrap PooledEvtData 的话, 拆包并 set holder)
                        Object o = this.deserializeUnwrap(entry.getValue(), kv.getKey(), stream);
                        if (o == null) continue;
                        // 拆包后, 按 item 反查 holder 取 traceId 染色单条日志 (single 路径)
                        String traceId = MapUtils.getString(RedisProxyHolder.get(o), AnyHolder.TRACE_ID);
                        // 有则设无则清: 防同批内上一条消息的 trace 串到本条
                        if (traceId != null) AnyHolder.set(AnyHolder.TRACE_ID, traceId);
                        else AnyHolder.remove(AnyHolder.TRACE_ID);
                        BiConsumer consumer = kv.getValue();
                        boolean batch = false;
                        if (consumer instanceof FactorConsumer2 factor) {
                            Object f = factor.factor();
                            batch = Objects.nonNull(f) && f instanceof RedisEventBatchListener;
                        }
                        try {
                            if (!batch) {
                                RedisProxyHolder.setStreamRecordId(recordId);
                                consumer.accept(event, o);
                                continue;
                            }
                            RecycleLinkedList<String> recordIds = RecycleLinkedList.of();
                            recordIds.add(recordId);
                            RedisProxyHolder.setStreamRecordId(recordIds);
                            RecycleLinkedList<Object> list = RecycleLinkedList.of();
                            try {
                                list.add(o);
                                consumer.accept(event, list);
                            } finally {
                                list.recycle();
                                recordIds.recycle();
                            }
                        } catch (Throwable t) {
                            log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
                        }
                    }
                } finally {
                    RedisProxyHolder.clear();
                }
                return;
            }

            // 多条消息 ...

            // 按 event/field 分组：Map<event, RecycleLinkedList<KV<recordId, data>>>
            RecycleLinkedMap<String, RecycleLinkedList<KV<String, Object>>> eventRidObjs = RecycleLinkedMap.of();
            // 内联嵌套 for，避免 forEach 每条消息分配捕获 recordId 的 lambda
            for (Object m : messages) {
                Record record = (Record) m;
                String recordId = record.getId().getValue();
                Map<String, Object> map = (Map<String, Object>) record.getValue();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String event = entry.getKey();
                    KV<TypeReference, BiConsumer> kv = cm.get(event);
                    if (Objects.isNull(kv)) continue;
                    // 反序列化推迟到 worker 线程的 bico 里 (deserializeUnwrap), 这里只塞 raw.
                    // 原因: publish 端透传 wrap 后, raw 是 PooledEvtData/LinkedHashMap, 用业务 reference 反序列化会拿到字段全 null 的对象.
                    // 推到 worker 也让反序列化跨多个 worker 并行, dispatch 线程负担减轻.
                    eventRidObjs.computeIfAbsent(event, FUNC2).add(KV.of(recordId, entry.getValue()));
                }
            }
            // 消费一个 event 的全部消息
            BiConsumer<String, RecycleLinkedList<KV<String, Object>>> bico = (event, list) -> {
                KV<TypeReference, BiConsumer> kv = cm.get(event);
                if (Objects.isNull(kv)) return;
                RedisProxyHolder.setRedisProxy(this);

                BiConsumer consumer = kv.getValue();
                TypeReference reference = kv.getKey();
                boolean batch = false;
                if (consumer instanceof FactorConsumer2 factor) {
                    Object f = factor.factor();
                    batch = Objects.nonNull(f) && f instanceof RedisEventBatchListener;
                }

                try {
                    if (batch) {
                        RecycleLinkedList<String> recordIds = RecycleLinkedList.of();
                        RecycleLinkedList<Object> datas = RecycleLinkedList.of();
                        // batch 路径累积本桶 traceIds 拼接, 一次性挂 AnyHolder.TRACE_ID 染色批日志
                        StringBuilder traceIds = null;
                        for (KV<String, Object> rd : list) {
                            // worker 线程内反序列化 + 探测拆包 (passthrough 自动 set 到 holder)
                            Object data = this.deserializeUnwrap(rd.getValue(), reference, stream);
                            if (data == null) {
                                rd.recycle();
                                continue;
                            }
                            recordIds.add(rd.getKey());
                            datas.add(data);
                            String tid = MapUtils.getString(RedisProxyHolder.get(data), AnyHolder.TRACE_ID);
                            if (tid != null) {
                                if (traceIds == null) traceIds = new StringBuilder();
                                if (traceIds.isEmpty()) traceIds.append(tid);
                                else traceIds.append(',').append(tid);
                            }
                            rd.recycle();
                        }
                        // 有则设无则清: 防上一批桶的 trace 串到本桶
                        if (traceIds != null) AnyHolder.set(AnyHolder.TRACE_ID, traceIds.toString());
                        else AnyHolder.remove(AnyHolder.TRACE_ID);
                        RedisProxyHolder.setStreamRecordId(recordIds);
                        try {
                            consumer.accept(event, datas);
                        } catch (Throwable t) {
                            log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
                        } finally {
                            recordIds.recycle();
                            datas.recycle();
                        }
                        return;
                    }
                    // single 路径: 逐条反序列化 + 按 item 反查 traceId 染色单条日志
                    for (KV<String, Object> rd : list) {
                        Object data = this.deserializeUnwrap(rd.getValue(), reference, stream);
                        if (data == null) {
                            rd.recycle();
                            continue;
                        }
                        String tid = MapUtils.getString(RedisProxyHolder.get(data), AnyHolder.TRACE_ID);
                        // 有则设无则清: 防同批内上一条消息的 trace 串到本条
                        if (tid != null) AnyHolder.set(AnyHolder.TRACE_ID, tid);
                        else AnyHolder.remove(AnyHolder.TRACE_ID);
                        RedisProxyHolder.setStreamRecordId(rd.getKey());
                        try {
                            consumer.accept(event, data);
                        } catch (Throwable t) {
                            log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
                        } finally {
                            rd.recycle();
                        }
                    }
                } finally {
                    RedisProxyHolder.clear();
                }
            };

            int taskLen = eventRidObjs.size();
            // 单事件 或 关闭了 event 线程池 → 当前 consumer 线程同步串行执行后回收
            if (taskLen <= 1 || !eventExecutorEnable) {
                try {
                    eventRidObjs.forEach(bico);
                } finally {
                    eventRidObjs.forEach(RecycleLinkedList.RECY_BICON);
                    eventRidObjs.recycle();
                }
                return;
            }

            /*
             * fire-and-forget: 异步提交后 consumer 立即返回去拉下一批
             * 全局背压: 当 STREAM_EVENT_INFLIGHT >= eventExecutorInflightMax 时, 当前 consumer 线程同步执行 (阻塞拉取)
             * 对象池回收交给"最后一个完成的任务"
             */
            int max = this.stream.getEventExecutorInflightMax();

            // remaining 初始 1 = forEach 本身, 异步提交 +1, 异步完成 -1, forEach 结束再 -1
            // 加 1 保证 forEach 期间 remaining ≥ 1, 防止异步任务先完成引发 use-after-recycle
            // (eventMidMap 内的 recordIds 会被异步任务通过 bico 访问, 必须等所有任务结束才能回收)
            AtomicInteger remaining = new AtomicInteger(1);
            Runnable recycleMaps = () -> {
                // list 已在各自任务的 finally 内 recycle, 这里只回收 map 自身和 midMap 内的 recordIds list
                eventRidObjs.forEach(RecycleLinkedList.RECY_BICON);
                eventRidObjs.recycle();
            };

            // 前 N-1 个 event 尝试异步, 最后一个 event 由 consumer 当前线程同步执行
            RingInteger integer = new RingInteger();
            int lastIdx = taskLen - 1;
            eventRidObjs.forEach((event, list) -> {
                // 最后一个 event 或 背压触发 → consumer 线程同步执行 (不计入 remaining)
                if (integer.getAndIncrement() == lastIdx || STREAM_EVENT_INFLIGHT.get() >= max) {
                    bico.accept(event, list);
                    return;
                }
                STREAM_EVENT_INFLIGHT.incrementAndGet();
                remaining.incrementAndGet();
                ActionRecycler ar = ActionRecycler.ofRecycle(STREAM_GROUP_ASYNC_CON)
                        .ref(0, bico)
                        .ref(1, event)
                        .ref(2, list)
                        .ref(3, remaining)
                        .ref(4, recycleMaps);
                try {
                    executor.execute(ar);
                } catch (RejectedExecutionException t) {
                    // executor 拒绝 → 回收 ar 防池泄漏, 当前线程降级, 撤销 inflight 和 remaining 增量
                    ar.recycle();
                    STREAM_EVENT_INFLIGHT.decrementAndGet();
                    try {
                        bico.accept(event, list);
                    } finally {
                        // list 由 recycleMaps 统一回收, 这里只撤销 remaining 增量
                        remaining.decrementAndGet(); // 不会归 0, forEach 的初始 1 还在
                    }
                }
            });
            // forEach 结束, 减掉初始的 1; 谁是最后一个完成的 (forEach 或最后异步任务) 谁触发 recycle
            if (remaining.decrementAndGet() <= 0) recycleMaps.run();
        };
        // 缓存listener
        listenerGroupMap.computeIfAbsent(stream, s -> new ConcurrentHashMap<>()).put(listener, group);
        return listener;
    }

    /**
     * 业务作用：non-group 消息级并行 listener.
     * <p>
     * 适用于 socket-center 等高吞吐广播场景: 1 个 consumer 拉取, 线程池并行消费.
     * 与 group listener 的区别: group 按 event 分组并行 (同 event 内串行保序),
     * non-group 按消息粒度并行 (不保序, 追求吞吐).
     * <p>
     * yml 配置启用 (在对应 redis 数据源的 stream 下):
     * <pre>
     * nasa:
     *   redis:
     *     properties:
     *       primary:              # 或其他 qualifier (websocket 等)
     *         stream:
     *           non-group-executor-enable: true    # 开启后 non-group listener 走消息级并行
     *           event-executor-inflight-max: 1000  # 全局在飞任务背压上限
     * </pre>
     * <p>
     * 执行顺序:
     * <ol>
     *   <li>一遍遍历 messages: 判定 batch/single + 分流收集 (反序列化延迟到消费线程)</li>
     *   <li>batch event 先提交线程池 (每个 event 一个 task, 内含该 event 的全部消息)</li>
     *   <li>single event 按消息粒度提交线程池 (前 N-1 条异步, 最后一条当前线程同步执行做背压)</li>
     * </ol>
     * remaining 计数: batch task + single task 共享, 最后一个完成的负责回收 batch 资源.
     *
     * @param stream Stream 键
     * @return 见上述说明。
     */
    private StreamListener streamListenerNonGroup(String stream) {
        Executor executor = getExecutor();
        String key = stream + "/";
        int max = this.stream.getEventExecutorInflightMax();

        BatchStreamListener listener = messages -> {
            if (messages.isEmpty()) return;

            ConcurrentMap<String, KV<TypeReference, BiConsumer>> cm = streamGroupEventConsumerMap.get(key);
            if (MapUtils.isEmpty(cm)) return;

            // === 1~3. 一遍遍历: 判定 batch/single + 反序列化 + 分流收集 ===
            Map<String, Boolean> batchFlags = Map.of(); // 懒初始化, 空 Map.of() 表示无 batch
            RecycleLinkedMap<String, RecycleLinkedList<Object>> batchData = null;
            RecycleLinkedMap<String, RecycleLinkedList<String>> batchMids = null;
            RecycleLinkedList<KV<String, KV<String, Object>>> singleTasks = RecycleLinkedList.of();

            for (Object m : messages) {
                Record record = (Record) m;
                String recordId = record.getId().getValue();
                Map<String, Object> map = (Map<String, Object>) record.getValue();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String event = entry.getKey();
                    KV<TypeReference, BiConsumer> kv = cm.get(event);
                    if (Objects.isNull(kv)) continue;

                    // 首次遇到某 event 时判定 batch (结果缓存到 batchFlags)
                    boolean isBatch;
                    if (batchFlags.containsKey(event)) {
                        isBatch = true;
                    } else if (batchFlags instanceof HashMap) {
                        // batchFlags 已初始化为 HashMap 但不含此 event → 首次判定
                        isBatch = isBatchConsumer(kv);
                        if (isBatch) batchFlags.put(event, Boolean.TRUE);
                    } else {
                        // batchFlags 还是 Map.of() → 首次判定
                        isBatch = isBatchConsumer(kv);
                        if (isBatch) {
                            batchFlags = new HashMap<>();
                            batchFlags.put(event, Boolean.TRUE);
                        }
                    }

                    // 反序列化延迟到消费线程, 收集阶段只存原始数据
                    if (isBatch) {
                        if (batchData == null) {
                            batchData = RecycleLinkedMap.of();
                            batchMids = RecycleLinkedMap.of();
                        }
                        batchData.computeIfAbsent(event, FUNC).add(entry.getValue());
                        batchMids.computeIfAbsent(event, MID_FUNC).add(recordId);
                    } else {
                        singleTasks.add(KV.of(recordId, KV.of(event, entry.getValue())));
                    }
                }
            }
            boolean hasBatch = batchData != null;

            // remaining: batch task + single task 共享, 初始 1 = forEach 本身
            AtomicInteger remaining = new AtomicInteger(1);
            RecycleLinkedMap<String, RecycleLinkedList<Object>> bd = batchData;
            RecycleLinkedMap<String, RecycleLinkedList<String>> bm = batchMids;
            Action recycle = !hasBatch ? null : () -> {
                bd.forEach(RecycleLinkedList.RECY_BICON);
                bd.recycle();
                bm.forEach(RecycleLinkedList.RECY_BICON);
                bm.recycle();
            };

            // === 4. batch event 先提交线程池 ===
            if (hasBatch) {
                for (var entry : batchData.entrySet()) {
                    String event = entry.getKey();
                    RecycleLinkedList<Object> list = entry.getValue();
                    KV<TypeReference, BiConsumer> kv = cm.get(event);
                    RecycleLinkedList<String> mids = batchMids.get(event);

                    STREAM_EVENT_INFLIGHT.incrementAndGet();
                    remaining.incrementAndGet();
                    ActionRecycler ar = ActionRecycler.ofRecycle(STREAM_NONGROUP_BATCH_CON)
                            .ref(0, this)
                            .ref(1, list)
                            .ref(2, kv)
                            .ref(3, mids)
                            .ref(4, stream)
                            .ref(5, event)
                            .ref(6, remaining)
                            .ref(7, recycle);
                    try {
                        executor.execute(ar);
                    } catch (RejectedExecutionException t) {
                        // executor 拒绝 → 回收 ar 防池泄漏, 当前线程降级
                        ar.recycle();
                        STREAM_EVENT_INFLIGHT.decrementAndGet();
                        this.deserializeBatchList(list, kv.getKey(), stream);
                        RedisProxyHolder.setRedisProxy(this);
                        RedisProxyHolder.setStreamRecordId(mids);
                        try {
                            kv.getValue().accept(event, list);
                        } catch (Throwable te) {
                            log.error("{} {}", AnyHolder.getTraceId(), te.getMessage(), te);
                        } finally {
                            RedisProxyHolder.clear();
                            if (remaining.decrementAndGet() <= 0) recycle.run();
                        }
                    }
                }
            }

            // === 5+6. single task 按条提交线程池, 最后一条自己执行 ===
            int singleSize = singleTasks.size();
            int singleIdx = 0;
            for (KV<String, KV<String, Object>> task : singleTasks) {
                boolean isLast = (++singleIdx == singleSize);

                // 最后一条 或 背压 → 当前线程同步
                if (isLast || STREAM_EVENT_INFLIGHT.get() >= max) {
                    this.consumeSingleTask(cm, task, stream);
                    continue;
                }

                STREAM_EVENT_INFLIGHT.incrementAndGet();
                remaining.incrementAndGet();
                ActionRecycler ar = ActionRecycler.ofRecycle(STREAM_NONGROUP_SINGLE_CON)
                        .ref(0, this)
                        .ref(1, cm)
                        .ref(2, task)
                        .ref(3, stream)
                        .ref(4, remaining)
                        .ref(5, recycle);
                try {
                    executor.execute(ar);
                } catch (RejectedExecutionException t) {
                    // executor 拒绝 → 回收 ar 防池泄漏, 当前线程降级
                    ar.recycle();
                    STREAM_EVENT_INFLIGHT.decrementAndGet();
                    this.consumeSingleTask(cm, task, stream);
                    if (remaining.decrementAndGet() <= 0 && recycle != null) recycle.run();
                }
            }
            // forEach 结束: 回收 singleTasks list (内部 KV 已在各自 consumeSingleTask 中回收)
            singleTasks.recycle();
            // 减掉初始 1
            if (remaining.decrementAndGet() <= 0 && recycle != null) recycle.run();
        };

        listenerGroupMap.computeIfAbsent(stream, s -> new ConcurrentHashMap<>()).put(listener, "");
        return listener;
    }

    /**
     * 业务作用：消费单条 single task: KV&lt;recordId, KV&lt;event, data&gt;&gt;, 消费后回收两层 KV
     *
     * @param cm     见上述说明
     * @param task   见上述说明
     * @param stream Stream 键
     */
    @SuppressWarnings("unchecked")
    private void consumeSingleTask(
            ConcurrentMap<String, KV<TypeReference, BiConsumer>> cm, KV<String, KV<String, Object>> task, String stream) {

        String recordId = task.getKey();
        KV<String, Object> inner = task.getValue();
        String event = inner.getKey();
        KV<TypeReference, BiConsumer> kv = cm.get(event);
        TypeReference reference = kv.getKey();

        RedisProxyHolder.setRedisProxy(this);
        RedisProxyHolder.setStreamRecordId(recordId);
        try {
            // 反序列化 + 探测拆包 (publish 端 wrap PooledEvtData 的话, 拆包并把 passthrough set 到 holder)
            Object data = this.deserializeUnwrap(inner.getValue(), reference, stream);
            if (data == null) return;
            // 按 item 反查 traceId 染色单条日志
            String tid = MapUtils.getString(RedisProxyHolder.get(data), AnyHolder.TRACE_ID);
            // 有则设无则清: 防上一条消息的 trace 串到本条
            if (tid != null) AnyHolder.set(AnyHolder.TRACE_ID, tid);
            else AnyHolder.remove(AnyHolder.TRACE_ID);
            kv.getValue().accept(event, data);
        } catch (Throwable t) {
            log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
        } finally {
            RedisProxyHolder.clear();
            inner.recycle();
            task.recycle();
        }
    }

    /**
     * 业务作用：原地反序列化 batch list + 探测拆包 + batch trace 拼接.
     * <p>
     * 用于 non-group batch task (worker 线程内). 拆包后把 unwrap 业务对象 set 回 list,
     * 同时累积本桶 traceIds 拼接, batch listener 调用前一次性挂 AnyHolder.TRACE_ID 染色批日志.
     *
     * @param list      见上述说明
     * @param reference 带泛型的反序列化目标类型引用
     * @param stream    Stream 键
     */
    private void deserializeBatchList(RecycleLinkedList<Object> list, TypeReference reference, String stream) {
        if (list.isEmpty()) return;
        ListIterator<Object> it = list.listIterator();
        StringBuilder traceIds = null;
        while (it.hasNext()) {
            Object raw = it.next();
            Object data = this.deserializeUnwrap(raw, reference, stream);
            if (data == null) {
                it.remove();
                continue;
            }
            it.set(data);
            String tid = MapUtils.getString(RedisProxyHolder.get(data), AnyHolder.TRACE_ID);
            if (tid == null) continue;
            if (traceIds == null) traceIds = new StringBuilder();
            if (traceIds.isEmpty()) traceIds.append(tid);
            else traceIds.append(',').append(tid);
        }
        // 有则设无则清: 防上一批的 trace 串到本批
        if (traceIds != null) AnyHolder.set(AnyHolder.TRACE_ID, traceIds.toString());
        else AnyHolder.remove(AnyHolder.TRACE_ID);
    }

    /**
     * 业务作用：探测式拆包 publish 端透传包装. 4 条消费分支统一用此 helper 替代裸 ObjMprUtils.deserialize.
     * <p>
     * 探测顺序:
     * <ol>
     *   <li>typing=true → Jackson 还原成 {@link PooledEvtData} 实例, 直接拆包</li>
     *   <li>typing=false → Map 且 {@code FIELD_TOPIC} == 当前 stream 名 → <b>认定为 wrap</b>, 必拆包返回 data (即使 data/passthrough 为 null), 不 fall through 段 3</li>
     *   <li>都不是 → fall through 原路径 (按 reference 反序列化为业务类型)</li>
     * </ol>
     * 拆包路径下, 若 passthrough 非空, 把它用 (业务对象引用) 做 key 存到 holder, single 路径下后续按 item 反查 trace.
     * <p>
     * <b>语义关键</b>: 段 2 一旦认定为 wrap (topic == stream), 必走拆包路径 — 不能 fall through 段 3 用业务 reference
     * 反序列化整个 wrap JSON, 否则会拿到字段全 null 的业务对象 (Jackson 忽略未知字段) 静默业务错.
     * 即使 data 缺失 (NON_EMPTY 序列化让空 message 字段消失) 或 passthrough 缺失 (跨服务/历史消息可能 null),
     * 仍按 wrap 处理, 返回拆包后的 data (可能 null).
     * <p>
     * <b>判定边界</b>: 业务方 publish 一个 Map 类型 message, 该 Map 含 "topic"=当前 stream 名 → 误识别 wrap.
     * 撮合实务下业务 message 不会用 stream 名做字段值 (stream 名是框架路由约定), 概率约 0.
     *
     * @param stream 当前 stream 名 (用于段 2 判定 wrap 标识)
     * @param raw       见上述说明
     * @param reference 带泛型的反序列化目标类型引用
     * @return 业务对象 (可能 null 表示空 message wrap 或反序列化失败, 调用方应跳过)
     */
    @SuppressWarnings("unchecked")
    private Object deserializeUnwrap(Object raw, TypeReference reference, String stream) {
        if (raw == null) return null;
        // 段 1: typing=true → PooledEvtData 实例
        if (raw instanceof PooledEvtData pm) {
            Object data = pm.getData();
            Map<String, Object> pt = pm.getPassthrough();
            pm.recycle();
            if (data != null && pt != null) RedisProxyHolder.set(data, pt);
            return data;
        }
        // 段 2: typing=false → Map 且 FIELD_TOPIC == 当前 stream 名 → 认定 wrap, 必拆包返回 data
        if (raw instanceof Map<?, ?> rawMap && stream.equals(rawMap.get(PooledEvtData.FIELD_TOPIC))) {
            Object dataRaw = rawMap.get(PooledEvtData.FIELD_DATA);
            // pt 可能 null (跨服务/历史消息没 set passthrough, 或 NON_EMPTY 让空 passthrough 字段消失)
            Map<String, Object> pt = MapUtils.getObject(rawMap, PooledEvtData.FIELD_PASSTHROUGH);
            Object data;
            if (reference != null && !isActivateDefaultTyping() && dataRaw != null) {
                try {
                    data = ObjMprUtils.deserialize(ObjMprUtils.toString(dataRaw), reference);
                } catch (Throwable t) {
                    // 反序列化失败前先把 traceId 挂 ThreadLocal, 让 log.error 染色排障
                    // (worker finally 会 AnyHolder.clear() 不污染后续消息)
                    String tid = MapUtils.getString(pt, AnyHolder.TRACE_ID);
                    if (tid != null) AnyHolder.set(AnyHolder.TRACE_ID, tid);
                    log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
                    return null;
                }
            } else {
                data = dataRaw;   // 可能 null (空 message 场景)
            }
            // pt 非空才 set holder (单条反查 trace), pt null 时跳过, 不影响 data 返回
            if (data != null && pt != null) RedisProxyHolder.set(data, pt);
            return data;
        }
        // 段 3: 按 reference 反序列化
        // activateDefaultTyping=true 时，复杂对象已被 hvs 正确反序列化（带类型信息），直接返回；
        // 但标量值（int/String/enum ordinal）无类型信息，hvs 还原为 Integer/String，
        // 与 reference 目标类型不匹配时 fallback 到 reference 反序列化（如 Integer 1 → TrueFalse.TRUE）
        // 段 3: 不是 wrap, 走原路径 (按 reference 反序列化)
        if (reference != null && (!isActivateDefaultTyping() || raw instanceof Number || raw instanceof Boolean || raw instanceof String)) {
            try {
                return ObjMprUtils.deserialize(ObjMprUtils.toString(raw), reference);
            } catch (Throwable t) {
                log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
                return null;
            }
        }
        return raw;
    }

    /**
     * 业务作用：判定 consumer 是否是 batch 消费
     *
     * @param kv 见上述说明
     * @return 见上述说明。
     */
    private static boolean isBatchConsumer(KV<TypeReference, BiConsumer> kv) {
        if (kv.getValue() instanceof FactorConsumer2 factor) {
            Object f = factor.factor();
            return Objects.nonNull(f) && f instanceof RedisEventBatchListener;
        }
        return false;
    }

    /**
     * 业务作用：订阅频道。
     *
     * @param stream Stream 键
     * @param consumer 消费者名
     * 返回: 无返回值。
     */
    public <M> void subscribe(String stream, BiConsumer<String, M> consumer) {
        this.subscribe(stream, STREAM_EVENT, consumer);
    }

    /**
     * 业务作用：订阅频道。
     *
     * @param stream Stream 键
     * @param reference 带泛型的反序列化目标类型引用
     * @param consumer 消费者名
     * 返回: 无返回值。
     */
    public <M> void subscribe(String stream, TypeReference<M> reference, BiConsumer<String, M> consumer) {
        this.subscribe(stream, STREAM_EVENT, reference, consumer);
    }

    /**
     * 业务作用：订阅频道。
     *
     * @param stream Stream 键
     * @param event 事件名
     * @param consumer 消费者名
     * 返回: 无返回值。
     */
    public <M> void subscribe(String stream, String event, BiConsumer<String, M> consumer) {
        this.subscribe(stream, null, event, consumer);
    }

    /**
     * 业务作用：订阅频道。
     *
     * @param stream Stream 键
     * @param event 事件名
     * @param reference 带泛型的反序列化目标类型引用
     * @param consumer 消费者名
     * 返回: 无返回值。
     */
    public <M> void subscribe(String stream, String event, TypeReference<M> reference, BiConsumer<String, M> consumer) {
        this.subscribe(stream, null, event, reference, consumer);
    }

    /**
     * 业务作用：订阅频道。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param event 事件名
     * @param consumer 消费者名
     * 返回: 无返回值。
     */
    public <M> void subscribe(String stream, String group, String event, BiConsumer<String, M> consumer) {
        this.subscribe(stream, group, event, null, consumer);
    }

    /**
     * 业务作用：订阅频道。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param event 事件名
     * @param reference 带泛型的反序列化目标类型引用
     * @param consumer 消费者名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public <M> void subscribe(String stream, String group, String event
            , TypeReference<M> reference, BiConsumer<String, M> consumer) {
        this.subscribe(stream, group, event, reference, consumer, errorHandler);
    }

    /**
     * 业务作用：订阅频道。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param event 事件名
     * @param consumer 消费者名
     * @param errorHandler 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public <M> void subscribe(String stream, String group, String event
            , BiConsumer<String, M> consumer, ErrorHandler errorHandler) {
        this.subscribe(stream, group, event, null, consumer, errorHandler);
    }

    /**
     * 业务作用：按 Stream、消费组与事件登记业务回调，并确保同一 Stream/组只打开一个底层拉取订阅。
     * 回调映射会先于拉取任务发布，防止容器启动后读到消息却找不到对应事件处理器。
     *
     * @param stream       Stream 键
     * @param group        消费组名；空值表示无消费组读取
     * @param event        消息字段对应的事件名
     * @param reference    未携带默认类型信息时使用的反序列化目标
     * @param consumer     业务回调
     * @param errorHandler 拉取或转换阶段的异常处理器
     * 返回: 无返回值；注册失败时回退本次写入的回调与订阅标记。
     */
    public <M> void subscribe(String stream, String group, String event
            , TypeReference<M> reference, BiConsumer<String, M> consumer, ErrorHandler errorHandler) {
        if (StringUtils.isBlank(stream) || StringUtils.isBlank(event)) {
            throw new IllegalArgumentException("stream and event must not be blank");
        }
        Objects.requireNonNull(consumer, "consumer must not be null");
        Objects.requireNonNull(errorHandler, "errorHandler must not be null");
        String key = stream + "/" + (Objects.isNull(group) ? "" : group);
        ConcurrentMap<String, KV<TypeReference, BiConsumer>> consumers =
                streamGroupEventConsumerMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        synchronized (consumers) {
            KV<TypeReference, BiConsumer> previous = consumers.put(event, KV.of(reference, consumer));
            if (!eventDispatchSubscriptions.add(key)) return;
            try {
                // 先发布 consumer 映射再开放拉取，避免容器立即读到消息时尚无可用回调而直接跳过。
                this.subscribe(stream, group, streamListener(stream, group), errorHandler);
            } catch (RuntimeException | Error e) {
                eventDispatchSubscriptions.remove(key);
                if (previous == null) consumers.remove(event);
                else consumers.put(event, previous);
                throw e;
            }
        }
    }

    /**
     * 业务作用：订阅频道。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param listener 见方法语义
     * 返回: 无返回值。
     */
    public <V extends Record<String, ?>> void subscribe(String stream, String group, StreamListener<String, V> listener) {
        this.subscribe(stream, group, listener, errorHandler);
    }

    /**
     * 业务作用：注册原始 Stream listener；消费组不存在时先幂等创建，并按配置建立一个或多个 consumer。
     * 多 consumer 注册中途失败时只撤销本次新增部分，不影响同键下已经生效的订阅。
     *
     * @param stream       Stream 键
     * @param group        消费组名；空值表示无消费组读取
     * @param listener     消息 listener
     * @param errorHandler 拉取或转换阶段的异常处理器
     * 返回: 无返回值；全部 consumer 完成登记后订阅才视为成功。
     */
    public <V extends Record<String, ?>> void subscribe(String stream, String group
            , StreamListener<String, V> listener, ErrorHandler errorHandler) {
        if (StringUtils.isBlank(stream)) throw new IllegalArgumentException("stream must not be blank");
        Objects.requireNonNull(listener, "listener must not be null");
        Objects.requireNonNull(errorHandler, "errorHandler must not be null");
        var requestBuilder = StreamMessageListenerContainer.StreamReadRequest
                // 指定消费最新消息
                .builder(StreamOffset.create(stream, ReadOffset.lastConsumed()))
                // 异常处理
                .errorHandler(errorHandler)
                // 发生异常时，跳过错误继续消费
                .cancelOnError(e -> false);

        String key = stream + "/" + (Objects.isNull(group) ? "" : group);
        var subList = streamSubscriptionMap.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());
        synchronized (subList) {
            int initialSize = subList.size();
            try {
                if (StringUtils.isBlank(group)) {
                    // 无 group: 单 consumer (无 group 概念, 多 consumer 无意义)
                    subList.add(getStreamMessageListenerContainer().register(requestBuilder.build(), listener));
                    if (streamSubscriptionMap.get(key) != subList) {
                        throw new IllegalStateException("stream subscription was cancelled while registering: " + key);
                    }
                    log.info("{} Subscribed to stream {}", this.qualifier, stream);
                    return;
                }

                // 有 group: 创建消费者组 (已存在则直接返回)
                this.xGroupCreate(stream, group);

                NasaLettuceConfig.Group cfg = this.groupConfig(stream, group);
                String baseName = StringUtils.isNotBlank(cfg.getConsumerName()) ? cfg.getConsumerName() : ME.sequence();
                int consumers = Math.max(1, cfg.getConsumers());

                // 专属拉取参数需要独立 container；未配置时复用全局 container。
                @SuppressWarnings("rawtypes")
                StreamMessageListenerContainer container = pickContainer(stream, group);

                // 单节点内可注册多个 consumer (consumer name 加 -i 后缀), 让 Redis stream group 在节点内进一步分摊消息
                for (int i = 0; i < consumers; i++) {
                    String consumerName = consumers == 1 ? baseName : baseName + "-" + i;
                    var req = requestBuilder
                            .consumer(org.springframework.data.redis.connection.stream.Consumer.from(group, consumerName))
                            .autoAcknowledge(cfg.isAutoAcknowledge())
                            .build();
                    subList.add(container.register(req, listener));
                }
                // 取消操作会先摘掉列表；若注册期间失去登记权，必须撤销刚打开的拉取任务，避免留下无法再取消的订阅。
                if (streamSubscriptionMap.get(key) != subList) {
                    throw new IllegalStateException("stream subscription was cancelled while registering: " + key);
                }
                log.info("{} Subscribed to stream {} with group {} ({} consumers)", this.qualifier, stream, group, consumers);
            } catch (RuntimeException | Error e) {
                rollbackSubscriptions(key, subList, initialSize);
                throw e;
            }
        }
    }

    /**
     * 业务作用：撤销一次未完整注册的 Stream 订阅，只回退本次新增部分，不影响同键下先前已生效的订阅。
     *
     * @param key         Stream 与消费组组成的内部键
     * @param subscriptions 当前订阅列表
     * @param initialSize 本次注册前的列表长度
     * 返回: 无返回值；新增订阅均被取消，空列表会从缓存移除。
     */
    private void rollbackSubscriptions(String key, CopyOnWriteArrayList<Subscription> subscriptions, int initialSize) {
        for (int i = subscriptions.size() - 1; i >= initialSize; i--) {
            Subscription subscription = subscriptions.remove(i);
            try {
                subscription.cancel();
            } catch (RuntimeException | Error e) {
                log.warn("[{}] stream subscription rollback failed for {}", qualifier, key, e);
            }
        }
        if (subscriptions.isEmpty()) {
            streamSubscriptionMap.remove(key, subscriptions);
            StreamMessageListenerContainer dedicated = dedicatedContainers.remove(key);
            if (dedicated != null && dedicated.isRunning()) dedicated.stop();
        }
    }

    /**
     * 业务作用：订阅频道。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param streamReadRequest 见方法语义
     * @param event 事件名
     * @param consumer 消费者名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public <M> void subscribe(StreamMessageListenerContainer.StreamReadRequest<String> streamReadRequest
            , String event, BiConsumer<String, M> consumer) {
        this.subscribe(streamReadRequest, event, null, consumer);
    }

    /**
     * 业务作用：按调用方提供的读取请求登记事件回调与底层 Subscription。
     * 回调、订阅标记和 Subscription 必须共同成功；取消与注册交错时会撤销刚建立的拉取任务。
     *
     * @param streamReadRequest 完整的 Stream 读取请求
     * @param event             消息字段对应的事件名
     * @param reference         未携带默认类型信息时使用的反序列化目标
     * @param consumer          业务回调
     * 返回: 无返回值；任一登记步骤失败时不保留半完成订阅。
     */
    public <M> void subscribe(StreamMessageListenerContainer.StreamReadRequest<String> streamReadRequest
            , String event, TypeReference<M> reference, BiConsumer<String, M> consumer) {
        Objects.requireNonNull(streamReadRequest, "streamReadRequest must not be null");
        if (StringUtils.isBlank(event)) throw new IllegalArgumentException("event must not be blank");
        Objects.requireNonNull(consumer, "consumer must not be null");
        String stream = streamReadRequest.getStreamOffset().getKey();
        String group;
        if (streamReadRequest instanceof StreamMessageListenerContainer.ConsumerStreamReadRequest<?> csrr) {
            group = csrr.getConsumer().getGroup();
            log.info("{} Subscribed to stream {} with group {}", this.qualifier, stream, group);
        } else {
            group = null;
            log.info("{} Subscribed to stream {}", this.qualifier, stream);
        }
        String key = stream + "/" + (Objects.isNull(group) ? "" : group);
        ConcurrentMap<String, KV<TypeReference, BiConsumer>> consumers =
                streamGroupEventConsumerMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        synchronized (consumers) {
            KV<TypeReference, BiConsumer> previous = consumers.put(event, KV.of(reference, consumer));
            if (!eventDispatchSubscriptions.add(key)) return;
            CopyOnWriteArrayList<Subscription> subscriptions =
                    streamSubscriptionMap.computeIfAbsent(key, ignored -> new CopyOnWriteArrayList<>());
            try {
                synchronized (subscriptions) {
                    Subscription subscription = getStreamMessageListenerContainer()
                            .register(streamReadRequest, streamListener(stream, group));
                    subscriptions.add(subscription);
                    if (streamSubscriptionMap.get(key) != subscriptions) {
                        subscriptions.remove(subscription);
                        subscription.cancel();
                        throw new IllegalStateException("stream subscription was cancelled while registering: " + key);
                    }
                }
            } catch (RuntimeException e) {
                eventDispatchSubscriptions.remove(key);
                if (previous == null) consumers.remove(event);
                else consumers.put(event, previous);
                if (subscriptions.isEmpty()) streamSubscriptionMap.remove(key, subscriptions);
                throw e;
            }
        }
    }

    /**
     * 业务作用：取消订阅频道。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * 返回: 无返回值。
     */
    public void unsubscribe(String stream, String group) {
        this.unsubscribe(stream, group, null);
    }

    /**
     * 业务作用：取消订阅频道。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param event 事件名
     * 返回: 无返回值。
     */
    public void unsubscribe(String stream, String group, String event) {
        String key = stream + "/" + (Objects.isNull(group) ? "" : group);
        ConcurrentMap<String, KV<TypeReference, BiConsumer>> consumers = streamGroupEventConsumerMap.get(key);
        if (consumers == null) {
            if (StringUtils.isNotBlank(event)) return;
            unsubscribeStreamRegistration(key, stream, group);
            return;
        }
        synchronized (consumers) {
            if (StringUtils.isNotBlank(event)) {
                if (consumers.remove(event) == null) return;
                log.info("{} Unsubscribed from stream {} event {}", this.qualifier, stream, event);
                if (MapUtils.isNotEmpty(consumers)) return;
            } else {
                consumers.clear();
            }
            // 保留空映射作为稳定的并发锁；后续重新订阅会复用它，避免取消与注册交错时把回调写入失联映射。
            unsubscribeStreamRegistration(key, stream, group);
        }
    }

    /**
     * 业务作用：取消一个 Stream/消费组对应的底层拉取任务，并释放该组专属的监听容器。
     * 调用方必须先阻止同键事件注册并发进入，以免刚建立的订阅在取消后失去登记。
     *
     * @param key    Stream 与消费组组成的内部键
     * @param stream Stream 键
     * @param group  消费组名；空值表示无消费组订阅
     * 返回: 无返回值；所有已登记 Subscription 均被取消。
     */
    private void unsubscribeStreamRegistration(String key, String stream, String group) {
        eventDispatchSubscriptions.remove(key);
        List<Subscription> list = streamSubscriptionMap.remove(key);
        boolean hadSubscriptions = ColUtils.isNotEmpty(list);
        if (list != null) {
            synchronized (list) {
                for (Subscription subscription : list) {
                    // Subscription 记录自身所属容器；直接取消可覆盖共享与 dedicated 两种注册来源。
                    try {
                        subscription.cancel();
                    } catch (RuntimeException e) {
                        log.warn("[{}] stream subscription cancel failed for {}", qualifier, key, e);
                    }
                }
                list.clear();
            }
        }
        ConcurrentMap<StreamListener, String> listeners = listenerGroupMap.get(stream);
        if (listeners != null) {
            String normalizedGroup = Objects.isNull(group) ? "" : group;
            listeners.entrySet().removeIf(entry -> normalizedGroup.equals(entry.getValue()));
            if (listeners.isEmpty()) listenerGroupMap.remove(stream, listeners);
        }
        StreamMessageListenerContainer dedicated = dedicatedContainers.remove(key);
        if (dedicated != null && dedicated.isRunning()) dedicated.stop();
        if (!hadSubscriptions) return;
        if (StringUtils.isBlank(group)) {
            log.info("{} Unsubscribed from stream {}", this.qualifier, stream);
        } else {
            log.info("{} Unsubscribed from stream {} with group {}", this.qualifier, stream, group);
        }
    }

    /**
     * 业务作用：向 Stream 追加默认事件消息。
     * 消息会保留在 Stream 中，直至显式删除或被保留期裁剪。
     *
     * @param stream Stream 键
     * @param message 消息体
     * @return 命令的执行结果。
     */
    public String publish(String stream, Object message) {
        return this.publish(stream, STREAM_EVENT, message);
    }

    /**
     * 业务作用：向 Stream 追加指定事件消息。
     * 消息会保留在 Stream 中，直至显式删除或被保留期裁剪。
     *
     * @param stream Stream 键
     * @param event 事件名
     * @param message 消息体
     * @return 命令的执行结果。
     */
    public String publish(String stream, String event, Object message) {
        // 前置校验 (在 borrow PooledEvtData/passthrough 之前, 避免无意义借还), 与 xAdd 单条一致
        if (StringUtils.isBlank(stream) || StringUtils.isBlank(event) || message == null) {
            return null;
        }
        // PROXY 模式下 event 是物理 hash field (xAdd 入参), 不存进 pm; 但 topic 字段存 stream 名做 wrap 标识 —
        // 消费端 deserializeUnwrap 段 2 用 (topic == 当前 stream) 判定确认是 publish 端 wrap 而非业务 Map 误识别.
        PooledEvtData pm = PooledEvtData.of();
        RecycleLinkedMap<String, Object> pt = RedisProxyHolder.passthrough();
        try {
            pm.setTopic(stream);
            pm.setData(message);
            pm.setPassthrough(pt);
            return this.xAdd(stream, event, pm);
        } finally {
            pm.recycle();
            // PooledEvtData.restore 不再 cascade, caller 显式归还 passthrough 到池
            if (pt != null) pt.recycle();
        }
    }

    /**
     * 业务作用：向频道发布消息。
     * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
     *
     * @param stream Stream 键
     * @param eventAndMessages 见方法语义
     * 返回: 无返回值。
     */
    public void publish(String stream, Object... eventAndMessages) {
        if (StringUtils.isBlank(stream)) return;
        if (eventAndMessages == null || eventAndMessages.length == 0) return;
        // 数组奇偶校验(失败抛 IAE), 提前到 wrap 之前避免 borrow 后异常路径泄漏
        int pair = eventAndMessages.length >> 1;
        if (pair << 1 != eventAndMessages.length) {
            throw new IllegalArgumentException("eventAndMessages.length must be a multiple of 2 and contain a sequence of event1, message1, event2, message2, ..., eventN, messageN");
        }
        // 每组 event 非 blank String + message 非 null fail-fast (与单条 publish/多field XADD 一致, 在 borrow 前校验)
        for (int i = 0; i < pair; i++) {
            if (!(eventAndMessages[i << 1] instanceof String es) || StringUtils.isBlank(es) || eventAndMessages[(i << 1) + 1] == null) {
                throw new IllegalArgumentException("publish event must be non-blank String and message non-null at pair " + i);
            }
        }

        PooledEvtData[] pms = new PooledEvtData[pair];
        @SuppressWarnings("unchecked")
        RecycleLinkedMap<String, Object>[] pts = new RecycleLinkedMap[pair];
        Object[] wrapped = new Object[eventAndMessages.length];
        try {
            for (int i = 0; i < pair; i++) {
                String event = (String) eventAndMessages[i << 1];
                Object message = eventAndMessages[(i << 1) + 1];
                // PROXY: event 是 xAdd hash field 不存 pm; topic 存 stream 名做 wrap 标识 (消费端段 2 判定用)
                PooledEvtData pm = PooledEvtData.of();
                RecycleLinkedMap<String, Object> pt = RedisProxyHolder.passthrough();
                // 立即记录到 pms[i] / pts[i], 后续 setter 异常路径下 finally 兜底 recycle 防止 borrow 泄漏
                pms[i] = pm;
                pts[i] = pt;
                pm.setTopic(stream);
                pm.setData(message);
                pm.setPassthrough(pt);
                wrapped[i << 1] = event;
                wrapped[(i << 1) + 1] = pm;
            }
            this.xAdd(stream, wrapped);
        } finally {
            for (PooledEvtData pm : pms) {
                if (pm != null) pm.recycle();
            }
            // PooledEvtData.restore 不再 cascade, caller 显式归还各自的 passthrough 到池
            for (RecycleLinkedMap<String, Object> pt : pts) {
                if (pt != null) pt.recycle();
            }
        }
    }

    // ==================== partition 快速访问 ====================
    // RedisPartition 的 publish 转发器, 业务侧用 redisProxy.partition(...) 不用再写
    // RedisPartition.load(this).of(this).publish(...). 实际调用都委托 RedisPartition 处理。

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     *
     * @param topic 主题名
     * @param partition 见方法语义
     * @param data 业务数据
     * @return 命令的执行结果。
     */
    public String partition(String topic, String partition, Object data) {
        return RedisPartition.load(this).publish(topic, partition, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     *
     * @param topic 主题名
     * @param partition 见方法语义
     * @param data 业务数据
     * @return 命令的执行结果。
     */
    public String partition(String topic, long partition, Object data) {
        return RedisPartition.load(this).publish(topic, partition, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     *
     * @param topic 主题名
     * @param event 事件名
     * @param partition 见方法语义
     * @param data 业务数据
     * @return 命令的执行结果。
     */
    public String partition(String topic, String event, String partition, Object data) {
        return RedisPartition.load(this).publish(topic, event, partition, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     *
     * @param topic 主题名
     * @param event 事件名
     * @param partition 见方法语义
     * @param data 业务数据
     * @return 命令的执行结果。
     */
    public String partition(String topic, String event, long partition, Object data) {
        return RedisPartition.load(this).publish(topic, event, partition, data);
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     *
     * @param stream Stream 键
     * @param message 消息体
     * @return 命令的执行结果。
     */
    public String xAdd(String stream, Object message) {
        return xAdd(stream, STREAM_EVENT, message);
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param field 哈希字段名
     * @param message 消息体
     * @return 命令的执行结果。
     */
    public String xAdd(String stream, String field, Object message) {
        // 前置校验 (与多 field 版一致): Redis Stream 不接受 null/空 field/value, blank stream 也非法
        if (StringUtils.isBlank(stream) || StringUtils.isBlank(field) || message == null) {
            return null;
        }
        if (isLower()) {
            RecycleLinkedMap<String, Object> map = RecycleLinkedMap.of(field, message);
            try {
                return redisTemplate.opsForStream().add(MapRecord.create(stream, map)).getValue();
            } finally {
                map.recycle();
            }
        }
        byte[] s = keySerializer.serialize(stream);
        byte[] f = hashKeySerializer.serialize(field);
        // byte[] message 直通, 对齐 Actuator xAdd
        byte[] v = serHVal(message);
        LettuceFuture<RedisFuture<?>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XADD).arg1(s).arg2(f).arg3(v).future(lf));
        // 如果返回一个错误，这里会抛出异常
        return lf.getFinally();
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param fieldAndMessages 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void xAdd(String stream, Object... fieldAndMessages) {
        if (StringUtils.isBlank(stream)) {
            return;
        }
        // XADD 至少要一组 field/value; 0 个 pair 会构造无效命令 (空 XADD), fail-fast 暴露调用方错误
        if (fieldAndMessages == null || fieldAndMessages.length == 0) {
            throw new IllegalArgumentException("xAdd fieldAndMessages must contain at least one field/value pair");
        }
        int pair = fieldAndMessages.length >> 1;
        if (pair << 1 != fieldAndMessages.length) {
            throw new IllegalArgumentException("fieldAndMessages.length must be a multiple of 2 and contain a sequence of field1, message1, field2, message2, fieldN, messageN");
        }
        // field/message 非 null fail-fast (与 Actuator 一致): XADD 多 field 是同一 entry, 缺字段会改变业务结构, 不静默过滤
        for (int i = 0; i < fieldAndMessages.length; i++) {
            if (fieldAndMessages[i] == null) {
                throw new IllegalArgumentException("xAdd field/message must not be null at index " + i);
            }
        }
        if (isLower()) {
            Map<String, Object> map = new LinkedHashMap<>(pair);
            for (int i = 0; i < pair; i++) {
                int index = i << 1;
                map.put(fieldAndMessages[index++].toString(), fieldAndMessages[index]);
            }
            redisTemplate.opsForStream().add(MapRecord.create(stream, map));
            return;
        }
        byte[] s = keySerializer.serialize(stream);
        Map<byte[], byte[]> map = new LinkedHashMap<>(pair);
        for (int i = 0; i < pair; i++) {
            int index = i << 1;
            byte[] hk = hashKeySerializer.serialize(fieldAndMessages[index++].toString());
            // byte[] message 直通, 对齐 Actuator xAdd
            byte[] v = serHVal(fieldAndMessages[index]);
            map.put(hk, v);
        }
        LettuceFuture<RedisFuture<String>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XADD_MULTI).arg1(s).extras(map).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
    }

    /**
     * 业务作用：按最大条数裁剪 Stream。
     * 被裁掉的消息永久丢失，包括尚未被消费的。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param maxlen 见方法语义
     * @return 命令的执行结果。
     */
    public long xTrimMaxlen(String stream, long maxlen) {
        if (StringUtils.isBlank(stream) || maxlen < 1) {
            return 0L;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForStream().trim(stream, maxlen);
            return Objects.isNull(r) ? 0L : r;
        }
        byte[] s = keySerializer.serialize(stream);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XTRIM_MAXLEN).arg1(s).longArg(maxlen).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0L : r;
    }

    /**
     * 业务作用：按最小条目标识裁剪 Stream。
     * 早于该标识的消息永久丢失。
     *
     * @param stream Stream 键
     * @param millis 毫秒数
     * @return 命令的执行结果。
     */
    public long xTrimMinId(String stream, long millis) {
        return xTrimMinId(stream, millis + "-0");
    }

    /**
     * 业务作用：按最小条目标识裁剪 Stream。
     * 早于该标识的消息永久丢失。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param minId 见方法语义
     * @return 命令的执行结果。
     */
    public long xTrimMinId(String stream, String minId) {
        if (StringUtils.isBlank(stream)) return 0l;
        XTrimArgs args = new XTrimArgs();
        args.minId(minId);
        byte[] s = keySerializer.serialize(stream);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XTRIM_ARGS).arg1(s).extras(args).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0L : r;
    }

    /* 缓存异步删除 stream record-id */
    volatile ConcurrentMap<String, Queue<String>> delStreamRecordIdsMap;
    final Function<String, Queue<String>> delStreamRecordIdsFunc = t -> new ConcurrentLinkedQueue<>();
    private final Lock delStreamRecordIdsLock = new ReentrantLock();

    /**
     * 业务作用：取「待删除条目标识」的暂存表。
     * 确认之后的条目并不立即删除，而是攒起来批量删——逐条删除会让每条消息多一次往返。
     *
     * <p>参数说明: 无。
     *
     * @return Stream 键到待删条目标识队列的映射。
     */
    ConcurrentMap<String, Queue<String>> delStreamRecordIdsMap() {
        if (Objects.nonNull(delStreamRecordIdsMap)) return delStreamRecordIdsMap;
        delStreamRecordIdsLock.lock();
        try {
            if (Objects.nonNull(delStreamRecordIdsMap)) return delStreamRecordIdsMap;
            delStreamRecordIdsMap = new ConcurrentHashMap<>();

            if (!TimingWheel.isStarted()) TimingWheel.startTimingWheel();

            long asyncDelRecordPeriod = this.stream.getAsyncDelRecordPeriod();
            long t = System.currentTimeMillis();
            long delay = t / asyncDelRecordPeriod * asyncDelRecordPeriod + asyncDelRecordPeriod - t;
            TimingWheel.exec(delay, asyncDelRecordPeriod, this.streamDeleteTaskName, () -> {
                if (delStreamRecordIdsMap.isEmpty()) return;

                // 独立批次: openNested — 非嵌套(定时线程常态)复用 CACHE 单例零分配, 万一嵌套则隔离; 仍 try/finally 兜底
                LettucePipeline.Actuator pipelineActuator = LettucePipeline.openNested(this);
                Map<String, List<String>> drained = new LinkedHashMap<>();
                try {
                    delStreamRecordIdsMap.forEach((stram, recordIds) -> {
                        String recordId;
                        while ((recordId = recordIds.poll()) != null) {
                            drained.computeIfAbsent(stram, ignored -> new ArrayList<>()).add(recordId);
                            pipelineActuator.xDel(stram, recordId);
                        }
                    });
                    pipelineActuator.pipeline();
                } catch (RuntimeException | Error e) {
                    // 删除失败时把本轮取出的标识重新入队；XDEL 幂等，重复提交比静默遗失待删项更安全。
                    drained.forEach((streamName, recordIds) ->
                            delStreamRecordIdsMap.computeIfAbsent(streamName, delStreamRecordIdsFunc).addAll(recordIds));
                    throw e;
                } finally {
                    pipelineActuator.clearSession();
                }
            });
            return delStreamRecordIdsMap;
        } finally {
            delStreamRecordIdsLock.unlock();
        }
    }

    /**
     * 业务作用：按条目标识删除 Stream 消息。
     * 已被消费组读取但未确认的条目删除后不会再投递。
     *
     * @param stream Stream 键
     * @param messageId 见方法语义
     * 返回: 无返回值。
     */
    public void xDelAsync(String stream, String messageId) {
        // messageId blank 直接 return: 否则 null 会 ConcurrentLinkedQueue.add(null) NPE, blank 会让定时任务发非法 XDEL
        if (StringUtils.isBlank(stream) || StringUtils.isBlank(messageId)) return;
        delStreamRecordIdsMap().computeIfAbsent(stream, delStreamRecordIdsFunc).add(messageId);
    }

    /**
     * 业务作用：按条目标识删除 Stream 消息。
     * 已被消费组读取但未确认的条目删除后不会再投递。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param messageIds 见方法语义
     * @return 命令的执行结果。
     */
    public long xDel(String stream, String... messageIds) {
        if (StringUtils.isBlank(stream) || ColUtils.isEmpty(messageIds)) {
            return 0L;
        }
        // 过滤 null/blank id, 过滤后为空返回 0 (不发非法 XDEL)
        List<String> valid = new ArrayList<>(messageIds.length);
        for (String id : messageIds) if (StringUtils.isNotBlank(id)) valid.add(id);
        if (valid.isEmpty()) return 0L;
        String[] vids = valid.toArray(new String[0]);
        if (isLower()) {
            Long r = redisTemplate.opsForStream().delete(stream, vids);
            return Objects.isNull(r) ? 0L : r;
        }
        byte[] s = keySerializer.serialize(stream);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XDEL_MULTI).arg1(s).extras(vids).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0L : r;
    }

    /**
     * 业务作用：查询 Stream 的消息条数。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @return 命令的执行结果。
     */
    public long xLen(String stream) {
        if (StringUtils.isBlank(stream)) {
            return 0;
        }
        if (isLower()) {
            Long r = redisTemplate.opsForStream().size(stream);
            return Objects.isNull(r) ? 0L : r;
        }
        byte[] s = keySerializer.serialize(stream);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XLEN).arg1(s).future(lf));
        Long r = lf.getFinally();
        return Objects.isNull(r) ? 0L : r;
    }

    /**
     * 业务作用：按标识区间读取 Stream 消息。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param messageIdRange 见方法语义
     * @param count 数量上限
     * @return 命令的执行结果。
     */
    public <V> List<Map<String, V>> xRange(String stream, Range<String> messageIdRange, Long count) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(messageIdRange);
        if (count != null && count <= 0) return Collections.emptyList();
        int springCount = count == null ? 0 : Math.toIntExact(count);
        if (isLower()) {
            org.springframework.data.domain.Range<String> range = toSpringRange(messageIdRange);
            List<MapRecord<String, Object, Object>> list;
            if (Objects.isNull(count)) {
                list = redisTemplate.opsForStream().range(stream, range);
            } else {
                list = redisTemplate.opsForStream().range(stream, range
                        , org.springframework.data.redis.connection.Limit.limit().count(springCount));
            }
            if (Objects.isNull(list)) {
                return Collections.emptyList();
            }
            List<Map<String, V>> rs = new ArrayList<>(list.size());
            list.forEach(mr -> {
                Map<Object, Object> map = mr.getValue();
                Map<String, V> m = new LinkedHashMap<>(map.size());
                rs.add(m);
                map.forEach((k, v) -> m.put(k.toString(), (V) v));
            });
            return rs;
        }
        byte[] s = keySerializer.serialize(stream);
        LettuceFuture<RedisFuture<List<StreamMessage<byte[], byte[]>>>> lf = LettuceFuture.of();
        if (Objects.isNull(count)) {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_XRANGE).arg1(s).extras(messageIdRange).future(lf));
        } else {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_XRANGE_LIMIT).arg1(s).longArg(count).extras(messageIdRange).future(lf));
        }
        List<StreamMessage<byte[], byte[]>> list = lf.getFinally();
        if (ColUtils.isEmpty(list)) {
            return Collections.emptyList();
        }
        List<Map<String, V>> rs = new ArrayList<>(list.size());
        list.forEach(sm -> {
            Map<byte[], byte[]> map = sm.getBody();
            Map<String, V> m = new LinkedHashMap<>(map.size());
            rs.add(m);
            map.forEach((k, v) -> m.put(hashKeySerializer.deserialize(k), (V) hashValueSerializer.deserialize(v)));
        });
        return rs;
    }

    /**
     * 业务作用：按标识区间读取 Stream 消息。
     *
     * @param stream Stream 键
     * @param messageIdRange 见方法语义
     * @return 命令的执行结果。
     */
    public <V> List<Map<String, V>> xRange(String stream, Range<String> messageIdRange) {
        return xRange(stream, messageIdRange, null);
    }

    /**
     * 业务作用：按标识区间读取 Stream 消息。
     *
     * @param stream Stream 键
     * @param messageIdStart 见方法语义
     * @param messageIdEnd 见方法语义
     * @return 命令的执行结果。
     */
    public <V> List<Map<String, V>> xRange(String stream, String messageIdStart, String messageIdEnd) {
        return xRange(stream, Range.create(messageIdStart, messageIdEnd), null);
    }

    /**
     * 业务作用：获取消息列表，会自动过滤已经删除的消息
     * ID 从大到小，降序返回
     *
     * @param stream         流名
     * @param messageIdRange 消息id范围
     * @param count          返回条数
     */
    public <V> List<Map<String, V>> xRevRange(String stream, Range<String> messageIdRange, Integer count) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(messageIdRange);
        if (count != null && count <= 0) return Collections.emptyList();
        if (isLower()) {
            org.springframework.data.domain.Range<String> range = toSpringRange(messageIdRange);
            List<MapRecord<String, Object, Object>> list;
            if (Objects.isNull(count)) {
                list = redisTemplate.opsForStream().reverseRange(stream, range);
            } else {
                list = redisTemplate.opsForStream().reverseRange(stream, range
                        , org.springframework.data.redis.connection.Limit.limit().count(count));
            }
            if (Objects.isNull(list)) {
                return Collections.emptyList();
            }
            List<Map<String, V>> rs = new ArrayList<>(list.size());
            list.forEach(mr -> {
                Map<Object, Object> map = mr.getValue();
                Map<String, V> m = new LinkedHashMap<>(map.size());
                rs.add(m);
                map.forEach((k, v) -> m.put(k.toString(), (V) v));
            });
            return rs;
        }
        byte[] s = keySerializer.serialize(stream);
        LettuceFuture<RedisFuture<List<StreamMessage<byte[], byte[]>>>> lf = LettuceFuture.of();
        if (Objects.isNull(count)) {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_XREVRANGE).arg1(s).extras(messageIdRange).future(lf));
        } else {
            pipeline().offer(PipelineTask.of(LettucePipeline.OP_XREVRANGE_LIMIT).arg1(s).longArg(count).extras(messageIdRange).future(lf));
        }
        List<StreamMessage<byte[], byte[]>> list = lf.getFinally();
        if (ColUtils.isEmpty(list)) {
            return Collections.emptyList();
        }
        List<Map<String, V>> rs = new ArrayList<>(list.size());
        list.forEach(sm -> {
            Map<byte[], byte[]> map = sm.getBody();
            Map<String, V> m = new LinkedHashMap<>(map.size());
            rs.add(m);
            map.forEach((k, v) -> m.put(hashKeySerializer.deserialize(k), (V) hashValueSerializer.deserialize(v)));
        });
        return rs;
    }

    /**
     * 业务作用：获取消息列表，会自动过滤已经删除的消息
     * ID 从大到小，降序返回
     *
     * @param stream         流名
     * @param messageIdRange 消息id范围
     */
    public <V> List<Map<String, V>> xRevRange(String stream, Range<String> messageIdRange) {
        return this.xRevRange(stream, messageIdRange, null);
    }

    /**
     * 业务作用：获取消息列表，会自动过滤已经删除的消息
     * ID 从大到小，降序返回
     *
     * @param stream         流名
     * @param messageIdStart 消息id开始值
     * @param messageIdEnd   消息id结束值
     */
    public <V> List<Map<String, V>> xRevRange(String stream, String messageIdStart, String messageIdEnd) {
        return xRevRange(stream, Range.create(messageIdStart, messageIdEnd), null);
    }

    /**
     * 业务作用：查看stream流所有的消费者组信息
     *
     * @param stream 流名
     * @return 见上述说明。
     */
    public List<Map<String, Object>> xInfoGroup(String stream) {
        Objects.requireNonNull(stream);
        byte[] s = keySerializer.serialize(stream);
        LettuceFuture<RedisFuture<List<Object>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XINFO_GROUPS).arg1(s).future(lf));
        try {
            List<List<Object>> os = lf.getFinally();
            if (ColUtils.isEmpty(os)) {
                return Collections.emptyList();
            }
            List<Map<String, Object>> rs = new ArrayList<>(os.size());
            for (List<Object> list : os) {
                rs.add(this.group(list));
            }
            return rs;
        } catch (RuntimeException e) {
            if (hasRedisError(e, "no such key")) {
                return Collections.emptyList();
            }
            throw e;
        }
    }

    /**
     * 业务作用：把消费组信息的原始响应整理成映射，屏蔽不同版本响应结构的差异。
     *
     * @param list 原始响应
     * @return 整理后的映射。
     */
    @SuppressWarnings("ConstantConditions")
    private Map<String, Object> group(List<Object> list) {
        // redis返回的是成对的映射
        int len = list.size() >> 1;
        Map<String, Object> map = new LinkedHashMap<>(len);
        for (int i = 0; i < len; i++) {
            int index = i << 1;
            Object key = list.get(index++);
            if (key instanceof byte[]) {
                key = keySerializer.deserialize((byte[]) key);
            }
            Object val = list.get(index);
            if (val instanceof byte[]) {
                val = keySerializer.deserialize((byte[]) val);
            }
            map.put(key.toString(), val);
        }
        return map;
    }

    /**
     * 业务作用：判断消费组是否已存在，供创建前判存以避开「组已存在」的报错。
     * <p>
     * 实现上要拉取该 Stream 的<b>全部</b>消费组信息再比对，因此不适合放在高频路径上。
     *
     * @param stream 流名
     * @param group  消费组名
     * @return 该消费组已存在返回 true。
     */
    public boolean containGroup(String stream, String group) {
        List<Map<String, Object>> maps = this.xInfoGroup(stream);
        return ColUtils.contains(maps, m -> m.get("name").toString(), group);
    }

    /**
     * 业务作用：创建消费组。
     * 组已存在时按幂等成功处理，多个节点可并发初始化同一个消费组。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * 返回: 无返回值。
     */
    @SuppressWarnings("ConstantConditions")
    public void xGroupCreate(String stream, String group) {
        // 默认从 latest ($) 创建: 普通 stream subscribe / PROXY 模式沿用此语义 (只消费组创建后的新消息)
        this.xGroupCreate(stream, group, ReadOffset.latest());
    }

    /**
     * 业务作用：创建消费组。
     * 组已存在时报错，调用方需自行捕获或先判存。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param readOffset 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void xGroupCreate(String stream, String group, ReadOffset readOffset) {
        if (StringUtils.isBlank(stream) || StringUtils.isBlank(group)) {
            throw new IllegalArgumentException("stream and group must not be blank");
        }
        Objects.requireNonNull(readOffset, "readOffset must not be null");
        // 直接创建并识别 BUSYGROUP，避免“先查后建”在多节点并发启动时留下竞争窗口。
        byte[] s = keySerializer.serialize(stream);
        XReadArgs.StreamOffset<byte[]> offset = XReadArgs.StreamOffset.from(s, readOffset.getOffset());
        byte[] g = keySerializer.serialize(group);
        LettuceFuture<RedisFuture<String>> lf = LettuceFuture.of();
        // MKSTREAM: stream 不存在时自动创建, 避免 NOGROUP 错误
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XGROUP_CREATE).arg3(g).extras(offset).future(lf));
        try {
            lf.getFinally();
        } catch (RuntimeException e) {
            if (!hasRedisError(e, "BUSYGROUP")) throw e;
        }
    }

    /**
     * 业务作用：查看消费者组信息
     *
     * @param stream 流名
     * @param group  消费者组
     * @return 见上述说明。
     */
    public Map<String, Object> xInfoConsumers(String stream, String group) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(group);
        byte[] s = keySerializer.serialize(stream);
        byte[] g = keySerializer.serialize(group);
        LettuceFuture<RedisFuture<List<Object>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XINFO_CONSUMERS).arg1(s).arg3(g).future(lf));
        List<List<Object>> os = lf.getFinally();
        if (ColUtils.isEmpty(os)) {
            return null;
        }
        List<Object> list = os.getFirst();
        return ColUtils.isEmpty(list) ? null : this.group(list);
    }

    /**
     * 业务作用：确认消费组已处理某条消息。
     * 不确认的消息会留在待处理列表中，被空闲接管机制重新投递。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param ids 条目标识集合
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public void ack(String stream, String group, String... ids) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(group);
        if (ids == null || ids.length == 0) return;
        // 过滤 null/blank id (与 Actuator 一致), 过滤后为空不发空 XACK
        List<String> valid = new ArrayList<>(ids.length);
        for (String id : ids) if (StringUtils.isNotBlank(id)) valid.add(id);
        if (valid.isEmpty()) return;
        String[] vids = valid.toArray(new String[0]);
        if (isLower()) {
            redisTemplate.opsForStream().acknowledge(stream, group, vids);
            return;
        }
        byte[] s = keySerializer.serialize(stream);
        byte[] g = keySerializer.serialize(group);
        LettuceFuture<RedisFuture<Long>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XACK_MULTI).arg1(s).arg3(g).extras(vids).future(lf));
        // 如果返回一个错误，这里会抛出异常
        lf.getFinally();
    }

    /**
     * 业务作用：删除消费组。
     * <p>
     * 删除会连同该组的<b>消费位点与待处理列表一并丢弃</b>：此后重建同名组会从新指定的起点开始，
     * 原先未确认的消息不再投递。这是不可逆操作。
     *
     * @param stream 流名
     * @param group  消费组名
     * @return 删除成功返回 true；组不存在时返回 false。
     */
    public boolean xGroupDestroy(String stream, String group) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(group);
        if (isLower()) {
            Boolean r = redisTemplate.opsForStream().destroyGroup(stream, group);
            return Objects.nonNull(r) && r;
        }
        byte[] s = keySerializer.serialize(stream);
        byte[] g = keySerializer.serialize(group);
        LettuceFuture<RedisFuture<Boolean>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XGROUP_DESTROY).arg1(s).arg3(g).future(lf));
        Boolean r = lf.getFinally();
        return Objects.nonNull(r) && r;
    }

    /**
     * 业务作用：查询消费组的待处理消息。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @return 命令的执行结果。
     */
    public PendingMessagesSummary xPending(String stream, String group) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(group);
        return redisTemplate.opsForStream().pending(stream, group);
    }

    /**
     * 业务作用：接管其它消费者长时间未确认的消息。
     * 接管是消费者宕机后消息不丢的保障，也意味着<b>同一条消息可能被处理两次，业务必须幂等</b>。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param args 脚本参数
     * @return 命令的执行结果。
     */
    public ClaimedMessages<byte[], byte[]> xAutoClaim(String stream, XAutoClaimArgs<byte[]> args) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(args);
        byte[] s = keySerializer.serialize(stream);
        LettuceFuture<RedisFuture<ClaimedMessages<byte[], byte[]>>> lf = LettuceFuture.of();
        pipeline().offer(PipelineTask.of(LettucePipeline.OP_XAUTOCLAIM).arg1(s).extras(args).future(lf));
        return lf.getFinally();
    }

    /* NOTE ------------------- redis-search ------------------------------------------------------------------------ */

    /**
     * {@link #rediSearch()} 的 lazy 缓存; 无 volatile, race 无害 — 多线程并发首次访问最多重复触发, 但都返回同一个 CACHE 实例.
     */
    private RediSearch rediSearch;

    /**
     * 业务作用：拿到本数据源对应的 {@link io.github.nasaruntime.redis.cache.redis.search.RediSearch} 视图; 按 {@code RedisProxy} 进程内单例.
     * <p>
     * 等价于 {@code RediSearch.load(this)}, 但通过 RedisProxy 链式调用更直观:
     * <pre>{@code RedisProxy.load("match").rediSearch().find(query, TestOrder.class)}</pre>
     *
     * @return 见上述说明。
     */
    public RediSearch rediSearch() {
        return rediSearch != null ? rediSearch : (rediSearch = RediSearch.load(this));
    }

    /**
     * 业务作用：拿到 entity {@code clazz} 绑定的 {@link RediSearch.Actuator}, 业务调用不必再传 Class.
     * <p>
     * 典型用法 (推荐): 在 entity 类里缓存为 {@code static final} 字段:
     * <pre>{@code
     * public class TestOrder {
     *     private static final RediSearch.Actuator<TestOrder> RS =
     *             RedisProxy.load("match").rediSearch(TestOrder.class);
     *     public static RediSearch.Actuator<TestOrder> rediSearch() { return RS; }
     * }
     * // 调用点: TestOrder.rediSearch().save(mo);
     * }</pre>
     *
     * @param clazz 反序列化目标类型
     * @return 见上述说明。
     */
    public <T> RediSearch.Actuator<T> rediSearch(Class<T> clazz) {
        return this.rediSearch().actuator(clazz);
    }

    /* NOTE ------------------- distributed lock -------------------------------------------------------------------- */

    private DistributedLock distributedLock;

    /**
     * 业务作用：取得本代理对应的分布式锁入口，首次调用时惰性建出并此后复用。
     * <p>
     * 按代理缓存而非每次新建：锁实现持有看门狗定时任务与脚本摘要，每次新建会重复注册这些资源。
     * <p>
     * 锁与代理绑定，因此<b>不同代理上的同名锁互不互斥</b>——多套 Redis 共存时要确保互斥双方用同一个代理。
     *
     * <p>参数说明: 无。
     *
     * @return 本代理的分布式锁入口。
     */
    public DistributedLock distributedLock() {
        return distributedLock != null ? distributedLock : (distributedLock = LettuceDistributedLock.load(this));
    }

}
