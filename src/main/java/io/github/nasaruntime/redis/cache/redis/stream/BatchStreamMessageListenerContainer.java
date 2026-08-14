package io.github.nasaruntime.redis.cache.redis.stream;

import io.github.nasaruntime.core.base.RecycleLinkedList;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.lettuce.core.cluster.SlotHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.Cancelable;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/**
 * 业务作用：Stream 批量消费的容器，负责为每个订阅建出拉取任务、管理其生命周期并在异常后重建。
 * <p>
 * 每个订阅独占一条阻塞连接：阻塞读取会长时间占住连接，共用会让同一连接上的其它订阅一起被阻塞住。
 * <p>
 * 容器在停机时确保各任务真正停止后才释放连接，否则正在阻塞读取的任务会在连接关闭时抛出异常刷屏。
 */
public class BatchStreamMessageListenerContainer<K, V extends Record<K, ?>> implements StreamMessageListenerContainer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(BatchStreamMessageListenerContainer.class);

    private static final long STOP_TIMEOUT_MS = Long.getLong("nasa.batch-stream.stop-timeout-ms", 30_000L);

    private final java.util.concurrent.locks.ReentrantLock lifecycleMonitor = new java.util.concurrent.locks.ReentrantLock();

    private final Executor taskExecutor;
    private final ErrorHandler errorHandler;
    private final StreamReadOptions blockingReadOptions;
    /**
     * 当前容器所属 Redis 数据源的连接工厂。
     * 非 group 阻塞订阅用它为每个 poll task 创建一条生命周期独占连接。
     */
    private final RedisConnectionFactory connectionFactory;
    private final RedisTemplate<K, ?> template;
    private final StreamOperations<K, Object, Object> streamOperations;
    private final StreamMessageListenerContainerOptions<K, V> containerOptions;

    private final List<Subscription> subscriptions = new ArrayList<>();
    /**
     * 非阻塞 readOptions (不带 BLOCK), 给托管模式 ManagedRunner 的批量 xReadGroup 多 stream 调用用。
     * 连接瞬时借还 (微秒级), 不像 BLOCK 会长持连接。
     */
    private final StreamReadOptions nonBlockingReadOptions;
    /**
     * pollTimeout ms, 托管模式下冷流等待间隔。
     */
    private final long pollTimeoutMs;
    /**
     * 托管 runner 列表。doRegister 判断 runner 数 &lt; maxRunners 时新建 runner,
     * 否则按 taskCount 升序排序后塞给任务最少的 runner (简单负载均衡)。
     */
    private final List<ManagedRunner<K, V>> runners = new ArrayList<>();
    /**
     * runner 数量上限, 间接由 Lettuce 连接池容量决定 (RedisProxy.resolveMaxRunners 通常给
     * pool maxTotal × 75%, 留 25% 余量给 publish/ack 等共享路径)。默认 32 仅作 fallback。
     */
    private int maxRunners = 32;

    /**
     * 是否 Redis Cluster 模式。决定批量 poll 时是否需要按 slot 分桶 — Cluster 下多 stream xReadGroup
     * 要求所有 key 在同一 slot, 否则 server 直接返回 CROSSLOT 错误; 单节点没有 slot 概念,
     * 全部 task 塞同一个 bucket, 单次 RTT 拿所有 stream, 性能最优。
     */
    private final boolean clusterMode;

    private boolean running = false;

    /**
     * 业务作用：建出批量消费容器，绑定连接工厂与消费选项。
     *
     * @param connectionFactory 连接工厂
     * @param containerOptions   消费选项，含阻塞时长、批量条数与执行器
     */
    BatchStreamMessageListenerContainer(RedisConnectionFactory connectionFactory,
                                        StreamMessageListenerContainerOptions<K, V> containerOptions) {

        Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null");
        Assert.notNull(containerOptions, "StreamMessageListenerContainerOptions must not be null");

        this.taskExecutor = containerOptions.getExecutor();
        this.errorHandler = containerOptions.getErrorHandler();
        this.blockingReadOptions = buildReadOptions(containerOptions, true);
        this.connectionFactory = connectionFactory;
        this.template = createRedisTemplate(connectionFactory, containerOptions);
        this.containerOptions = containerOptions;

        this.nonBlockingReadOptions = buildReadOptions(containerOptions, false);
        this.pollTimeoutMs = containerOptions.getPollTimeout().toMillis();
        // 一次性判定 cluster 模式. 非 LettuceConnectionFactory 一律视为单节点 (Jedis 走另一套, 不走本类)
        this.clusterMode = (connectionFactory instanceof LettuceConnectionFactory lcf)
                && lcf.getClusterConfiguration() != null;

        if (containerOptions.hasHashMapper()) {
            this.streamOperations = this.template.opsForStream(containerOptions.getRequiredHashMapper());
        } else {
            this.streamOperations = this.template.opsForStream();
        }
    }

    /**
     * 业务作用：Create a new {@link BatchStreamMessageListenerContainer}.
     *
     * @param connectionFactory 见上述说明
     * @param containerOptions  见上述说明
     * @return 见上述说明。
     */
    public static <K, V extends Record<K, ?>> BatchStreamMessageListenerContainer<K, V> create(
            RedisConnectionFactory connectionFactory, StreamMessageListenerContainerOptions<K, V> containerOptions) {
        return new BatchStreamMessageListenerContainer<>(connectionFactory, containerOptions);
    }

    /**
     * 业务作用：设置托管 runner 最大数量。建议值: Lettuce 连接池 maxTotal × 0.75, 留 25% 余量给共享路径
     * (publish / ack / holds 等); 调用方未设置时 fallback 默认 32。
     * 超过此数的 task 会被分配到任务最少的已有 runner 中, 不再新建。
     *
     * @param maxRunners 见上述说明
     */
    public void setMaxRunners(int maxRunners) {
        this.maxRunners = maxRunners;
    }

    // ---- lifecycle ----

    /**
     * 业务作用：声明容器是否随应用自动启动。
     *
     * <p>参数说明: 无。
     *
     * @return 自动启动返回 true。
     */
    @Override
    public boolean isAutoStartup() {
        return false;
    }

    /**
     * 业务作用：停止容器并在完成后回调通知调用方，供容器管理框架编排停机顺序。
     *
     * @param callback 停止完成后的回调
     * 返回: 无返回值。
     */
    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    /**
     * 业务作用：启动容器：为已登记的各订阅建出拉取任务并开始消费。
     * 重复启动只生效一次，使容器管理框架的重复调用无副作用。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    @SuppressWarnings("unchecked")
    public void start() {
        lifecycleMonitor.lock();
        try {
            if (this.running) {
                return;
            }
            running = true;
            // 预注册的 task: group → ManagedRunner 托管, 非 group → 独立线程
            subscriptions.stream()
                    .filter(it -> !it.isActive())
                    .filter(it -> it instanceof TaskSubscription)
                    .map(TaskSubscription.class::cast)
                    .map(TaskSubscription::getTask)
                    .forEach(task -> {
                        if (task instanceof BatchStreamPollTask<?, ?> bpt && bpt.isGroupTask()) {
                            this.assignToRunner((BatchStreamPollTask<K, V>) bpt);
                        } else {
                            taskExecutor.execute(task);
                        }
                    });
        } finally {
            lifecycleMonitor.unlock();
        }
    }

    /**
     * 业务作用：将 task 分配到 runner (调用方已持有 lifecycleMonitor lock)
     *
     * @param task 见上述说明
     */
    private void assignToRunner(BatchStreamPollTask<K, V> task) {
        BatchAffinity affinity = BatchAffinity.from(task);
        ManagedRunner<K, V> min = null;
        int minCount = Integer.MAX_VALUE;
        for (int i = 0, n = runners.size(); i < n; i++) {
            ManagedRunner<K, V> r = runners.get(i);
            if (r.isAcceptingTasks() && r.accepts(affinity)) {
                int c = r.taskCount();
                if (c < minCount) {
                    min = r;
                    minCount = c;
                }
            }
        }
        if (min != null && min.addTask(task)) {
            return;
        }
        if (runners.size() >= maxRunners) {
            log.warn("Create extra BatchStream ManagedRunner for {}, maxRunners={} is treated as a soft limit to avoid mixed XREADGROUP batches",
                    affinity, maxRunners);
        }
        ManagedRunner<K, V> runner = new ManagedRunner<>(this, taskExecutor, pollTimeoutMs, errorHandler, affinity);
        if (!runner.addTask(task)) {
            throw new IllegalStateException("New BatchStream ManagedRunner rejected initial task");
        }
        runners.add(runner);
        taskExecutor.execute(runner);
    }

    /**
     * 业务作用：停止容器：先向全部任务发出停止请求，再统一等待它们真正停止，最后释放连接。
     * <p>
     * 先请求后等待而非逐个停止：逐个串行等待会让停机耗时随订阅数线性增长。
     * <p>
     * 必须确认任务确实停止后才释放连接——仍在阻塞读取的任务会在连接关闭时抛异常刷屏。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void stop() {
        List<ManagedRunner<K, V>> runnersToStop = List.of();
        lifecycleMonitor.lock();
        try {
            if (this.running) {
                subscriptions.forEach(Cancelable::cancel);
                running = false;
            }
            runnersToStop = new ArrayList<>(runners);
            for (ManagedRunner<K, V> runner : runners) {
                runner.requestStop();
            }
        } finally {
            lifecycleMonitor.unlock();
        }
        awaitRunnerStop(runnersToStop);
    }

    /**
     * 业务作用：等待一批执行器全部停止，超时后不再等待并记警告。
     * 不无限等待：某个业务回调卡死时会拖住整个停机流程，宁可留下一个警告也不能让进程退不掉。
     *
     * @param runnersToStop 待停止的执行器
     * 返回: 无返回值。
     */
    private void awaitRunnerStop(List<ManagedRunner<K, V>> runnersToStop) {
        if (runnersToStop.isEmpty()) {
            return;
        }
        for (ManagedRunner<K, V> runner : runnersToStop) {
            try {
                if (!runner.awaitStop(STOP_TIMEOUT_MS)) {
                    log.warn("BatchStream ManagedRunner did not stop within {} ms", STOP_TIMEOUT_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting BatchStream ManagedRunner stop", e);
                return;
            }
        }
    }

    /**
     * 业务作用：某个执行器结束时从容器登记中摘除，避免停机时对已结束的执行器重复等待。
     *
     * @param runner 已结束的执行器
     * 返回: 无返回值。
     */
    private void runnerTerminated(ManagedRunner<K, V> runner) {
        lifecycleMonitor.lock();
        try {
            runners.remove(runner);
        } finally {
            lifecycleMonitor.unlock();
        }
    }

    /**
     * 业务作用：判断容器是否处于运行态。
     *
     * <p>参数说明: 无。
     *
     * @return 运行中返回 true。
     */
    @Override
    public boolean isRunning() {
        lifecycleMonitor.lock();
        try {
            return running;
        } finally {
            lifecycleMonitor.unlock();
        }
    }

    /**
     * 业务作用：声明容器在停机顺序中的阶段。
     * 消费应<b>早于</b>其依赖的资源被关闭，阶段值据此设定；设错会让消费在业务组件已销毁时仍在投递消息。
     *
     * <p>参数说明: 无。
     *
     * @return 停机阶段值。
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    // ---- registration (the key override) ----

    /**
     * 业务作用：登记一个订阅并建出其拉取任务，容器已在运行时立即开始消费。
     * <p>
     * 同组同流的订阅会被合并到同一个执行器上，使它们共用一条阻塞连接——
     * 每个订阅各占一条连接会让连接数随订阅数线性增长。
     *
     * @param streamRequest 读取请求
     * @param listener      消息监听器
     * @return 该订阅的句柄，用于查询状态与取消。
     */
    @Override
    public Subscription register(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener) {

        Assert.notNull(streamRequest, "StreamReadRequest must not be null");
        Assert.notNull(listener, "StreamListener must not be null");

        return doRegister(getReadTask(streamRequest, listener));
    }

    /**
     * 业务作用：扩展 register: 接受 {@link PollLifecycle} 钩子, 给独占消费 / 分区消费等需要在 pollTask
     * 关键点上插自定义初始化、自检、清理逻辑的场景使用。
     * <p>
     * <b>限制</b>: lifecycle 仅对 {@link BatchStreamListener} 生效。普通 {@link StreamListener}
     * 没有 batch drain/fencing 语义, 所以传普通 listener 这里会抛 IAE 提示走原 register。
     * 现有调用方走 {@link #register(StreamReadRequest, StreamListener)} 不受影响。
     *
     * @param streamRequest 见上述说明
     * @param listener      见上述说明
     * @param lifecycle     见上述说明
     * @return 见上述说明。
     */
    public Subscription register(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener, PollLifecycle lifecycle) {

        Assert.notNull(streamRequest, "StreamReadRequest must not be null");
        Assert.notNull(listener, "StreamListener must not be null");
        Assert.notNull(lifecycle, "PollLifecycle must not be null");
        if (!(listener instanceof BatchStreamListener)) {
            throw new IllegalArgumentException(
                    "PollLifecycle requires a BatchStreamListener; for plain StreamListener use register(req, listener)");
        }

        return doRegister(getReadTask(streamRequest, listener, lifecycle));
    }


    /**
     * 业务作用：Creates an internal poll task backed only by Spring Data Redis public stream APIs.
     *
     * @param streamRequest 见上述说明
     * @param listener      见上述说明
     * @return 见上述说明。
     */
    private NasaStreamTask getReadTask(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener) {
        return getReadTask(streamRequest, listener, PollLifecycle.NOOP);
    }

    /**
     * 业务作用：task 构造。group 订阅 → 非阻塞 readFunction + ManagedRunner 托管; 非 group → 阻塞 readFunction + 独立线程。
     *
     * @param streamRequest 见上述说明
     * @param listener      见上述说明
     * @param lifecycle     见上述说明
     * @return 见上述说明。
     */
    private NasaStreamTask getReadTask(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener, PollLifecycle lifecycle) {

        boolean isGroup = streamRequest instanceof ConsumerStreamReadRequest;
        Function<ReadOffset, List<ByteRecord>> readFunction = isGroup
                ? getNonBlockingReadFunction(streamRequest)
                : getBlockingReadFunction(streamRequest);
        Function<ByteRecord, V> deserializerToUse = getDeserializer();

        TypeDescriptor targetType = TypeDescriptor
                .valueOf(containerOptions.hasHashMapper() ? containerOptions.getTargetType() : MapRecord.class);

        return new BatchStreamPollTask<>(streamRequest, listener, errorHandler, targetType, readFunction, deserializerToUse, lifecycle);
    }

    // ---- internal helpers ----

    /**
     * 业务作用：产出把原始记录转成业务对象的函数，按容器配置的序列化方式构造。
     *
     * <p>参数说明: 无。
     *
     * @return 解码函数。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Function<ByteRecord, V> getDeserializer() {

        Function<ByteRecord, MapRecord<K, Object, Object>> deserializer = streamOperations::deserializeRecord;

        if (containerOptions.getHashMapper() == null) {
            return (Function) deserializer;
        }

        return source -> {
            MapRecord<K, Object, Object> intermediate = deserializer.apply(source);
            return (V) streamOperations.map(intermediate, this.containerOptions.getTargetType());
        };
    }

    /**
     * 业务作用：按容器选项组装读取参数：阻塞时长与单批条数。
     * 阻塞时长同时决定空转开销与停机响应速度——停机要等当前这次阻塞读返回才能退出。
     *
     * @param options 容器消费选项
     * @param block   单次阻塞等待时长
     * @return 读取参数。
     */
    private static StreamReadOptions buildReadOptions(StreamMessageListenerContainerOptions<?, ?> options, boolean block) {
        StreamReadOptions readOpts = StreamReadOptions.empty();
        if (options.getBatchSize().isPresent()) {
            readOpts = readOpts.count(options.getBatchSize().getAsInt());
        }
        if (block && !options.getPollTimeout().isZero()) {
            readOpts = readOpts.block(options.getPollTimeout());
        }
        return readOpts;
    }

    /**
     * 业务作用：构建阻塞 readFunction (带 BLOCK), 给非 group 订阅的独立线程 doLoop 用。
     * <p>
     * <b>为什么非 group 必须用阻塞模式:</b>
     * <p>
     * 非 group 的 XREAD 用 {@code $} 作为 offset, 含义是"从当前 stream 最新 ID 之后开始读"。
     * <ul>
     *   <li><b>阻塞模式</b>: {@code XREAD BLOCK 500 STREAMS match $} — Redis server 解析 {@code $}
     *       为当前最新 ID (如 100), 将该客户端连接挂到 stream 的等待队列上。消息 101 到达时,
     *       server <b>主动推送</b>数据到这个连接, 客户端立即收到。本质是 <b>server-push over long-polling</b>,
     *       Lettuce 与 Redis server 之间维持一个长连接, {@code $} 只在命令发起时解析一次,
     *       后续新消息由 server 推送, 不会丢失。
     * <p>
     *       500ms 内有消息 → server 推送, 返回消息, 框架的 {@code PollState.updateReadOffset}
     *       将 offset 更新为最后一条消息的 ID (如 105)。下一轮 doLoop 发起
     *       {@code XREAD BLOCK 500 STREAMS match 105}, 从 105 之后继续等待, 不丢不重。
     * <p>
     *       500ms 内无消息 → server 返回 nil，但任务保留同一条 dedicated connection。下一轮
     *       doLoop 在同一连接上重新发起 {@code XREAD BLOCK 500 STREAMS match $}，
     *       {@code $} 再次解析为当前最新 ID，并重新挂到等待队列。周而复始。</li>
     *   <li><b>非阻塞模式</b>: {@code XREAD STREAMS match $} — 每次调用都重新解析 {@code $} 为"当前最新 ID",
     *       如果两次 poll 之间有新消息到达, 下次 poll 的 {@code $} 已经跳过它们。
     *       例如: poll1 时 {@code $}=100, 返回空; 消息 101-105 到达; poll2 时 {@code $}=105, 返回空。
     *       中间的 101-105 <b>全部丢失</b>。</li>
     * </ul>
     * <p>
     * group 订阅 (XREADGROUP) 没有此问题: {@code >} 含义是"该 consumer 尚未 deliver 的消息",
     * 由 Redis server 端的 consumer group 状态机跟踪, 不依赖客户端 offset, 非阻塞也能正确读取。
     * <p>
     * <b>连接生命周期</b>: 阻塞模式每个非 group task 独占一条连接。连接在第一次 poll 时懒加载，
     * 后续轮次持续复用；连接故障时废弃并在下一轮重建，取消订阅或容器停机时关闭。
     * 该连接不进入 {@code PipelineConnectionPool}，也不与普通命令的共享连接混用。
     *
     * @param streamRequest 见上述说明
     */
    @SuppressWarnings("unchecked")
    private Function<ReadOffset, List<ByteRecord>> getBlockingReadFunction(StreamReadRequest<K> streamRequest) {
        byte[] rawKey = ((RedisSerializer<K>) template.getKeySerializer())
                .serialize(streamRequest.getStreamOffset().getKey());
        return new PersistentBlockingStreamReadFunction(connectionFactory, blockingReadOptions, rawKey);
    }

    /**
     * 业务作用：构建不带 BLOCK 的非阻塞 readFunction。ManagedRunner 通过 {@link #batchPollAndDemux} 一次拉取多个 stream，
     * 本方法用于以下单任务读取场景：
     * <ul>
     *   <li>BatchStreamPollTask 构造时需要 readFunction 字段；</li>
     *   <li>降级为单 task NOBLOCK 时继续读取。</li>
     * </ul>
     * <p>
     * group 订阅 (XREADGROUP) 的 {@code >} offset 由 Redis server 端的 consumer group 状态机跟踪:
     * 每条消息被 deliver 后, server 记录该 consumer 的 last-delivered-id, 下次 {@code >} 从此位置之后读取。
     * 因此非阻塞模式不会丢消息 — 不依赖 {@code $} 的客户端解析, server 保证"未 deliver 的消息一定能读到"。
     *
     * @param streamRequest 见上述说明
     */
    @SuppressWarnings("unchecked")
    private Function<ReadOffset, List<ByteRecord>> getNonBlockingReadFunction(StreamReadRequest<K> streamRequest) {

        byte[] rawKey = ((RedisSerializer<K>) template.getKeySerializer())
                .serialize(streamRequest.getStreamOffset().getKey());

        if (streamRequest instanceof ConsumerStreamReadRequest<K> consumerStreamRequest) {
            StreamReadOptions readOpts = consumerStreamRequest.isAutoAcknowledge()
                    ? this.nonBlockingReadOptions.autoAcknowledge()
                    : this.nonBlockingReadOptions;
            Consumer consumer = consumerStreamRequest.getConsumer();
            return (offset) -> template.execute((RedisCallback<List<ByteRecord>>) connection -> connection.streamCommands()
                    .xReadGroup(consumer, readOpts, StreamOffset.create(rawKey, offset)));
        }

        return (offset) -> template.execute((RedisCallback<List<ByteRecord>>) connection -> connection.streamCommands()
                .xRead(nonBlockingReadOptions, StreamOffset.create(rawKey, offset)));
    }

    // ==================== 批量 poll: ManagedRunner 单次 xReadGroup 多 stream ====================

    /**
     * 批量结果分桶 computeIfAbsent 工厂常量, 避免每次 batch lambda allocate。
     * RecycleLinkedList.of() 从 ObjectPool 拿空 list, 调用方用完调 recycle 回池, 零 GC。
     */
    @SuppressWarnings("rawtypes")
    private static final Function RECORDS_BUCKET = k -> RecycleLinkedList.of();

    /**
     * 业务作用：批量拉取 + 按 K 分桶。给 ManagedRunner 一次 xReadGroup 覆盖本 runner 单个 slot 且同 consumer 的全部就绪 task,
     * 把 N 次单 stream xReadGroup 收敛成 1 次多 stream RTT。
     * <p>
     * <b>调用前提</b>:
     * <ol>
     *   <li>所有 offsets 共享同一个 consumer 和 autoAck。runner 按 (group, consumer-name, autoAck)
     *       亲和分配 task, 并在入 batch 前再次校验。</li>
     *   <li>所有 streamKey 在同一个 Redis Cluster slot 内, 否则 server 报 CROSSLOT。Cluster 模式下
     *       ManagedRunner 已按 SlotHash 分桶, 单节点全部塞 slot=0 同 bucket, 都满足前提。</li>
     * </ol>
     * <p>
     * <b>命令形态</b>: {@code XREADGROUP GROUP g c COUNT n NOBLOCK STREAMS s1 s2 .. sN > > .. >}。
     * Lettuce 返回 flat List, 每个 ByteRecord 携带原始 stream byte[] (record.getStream()),
     * 在这里反序列化回 K 后按 K 分桶, 让 runner 直接发给对应 task。
     * <p>
     * <b>资源所有权</b>: 返回的 {@link RecycleLinkedMap} 和内层每个 {@link RecycleLinkedList}
     * 都来自 ObjectPool, 调用方负责 recycle。典型用法:
     * <ul>
     *   <li>内层 list 通过 {@code task.handleBatchResult(list, executor)} 移交给业务 lambda,
     *       lambda 在 finally 中 recycle (BatchStreamPollTask.handleBatchResult 已封装)</li>
     *   <li>外层 map 在 dispatch 循环结束后由调用方 recycle (map.recycle 不会触及内层 list,
     *       内层引用已转移给 lambda)</li>
     * </ul>
     * <b>复杂度</b>: 总记录数 R, 桶数 N → O(R) 一遍过, RecycleLinkedMap 桶查找 O(1)。
     *
     * @param offsets  见上述说明
     * @param consumer 消费者名
     * @param autoAck  见上述说明
     * @return key=task.streamKey(), value=该 task 应得的 records 子集; 空批次返回空但非 null 的 RecycleLinkedMap
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public RecycleLinkedMap<K, RecycleLinkedList<ByteRecord>> batchPollAndDemux(
            List<StreamOffset<K>> offsets, Consumer consumer, boolean autoAck) {
        // 永远返回非 null 的 RecycleLinkedMap, 让调用方无脑 recycle (含空 case)
        RecycleLinkedMap<K, RecycleLinkedList<ByteRecord>> grouped = RecycleLinkedMap.of();
        // success=false 时 finally 内回收 grouped + 已分配的内层 buckets, 防止异常路径池泄漏
        boolean success = false;
        try {
            if (offsets == null || offsets.isEmpty()) {
                success = true;
                return grouped;
            }
            Assert.notNull(consumer, "Consumer must not be null for group batch poll");

            RedisSerializer<K> keySer = (RedisSerializer<K>) template.getKeySerializer();
            // 用 iterator 而非 get(i) 遍历: offsets 形参是 List, 若调用方传 RecycleLinkedList 那种
            // 链表实现, get(i) 会退化成 O(N²), 这里用 iterator 始终 O(N) 与底层实现解耦
            StreamOffset<byte[]>[] rawOffsets = new StreamOffset[offsets.size()];
            int idx = 0;
            for (StreamOffset<K> o : offsets) {
                rawOffsets[idx++] = StreamOffset.create(keySer.serialize(o.getKey()), o.getOffset());
            }
            StreamReadOptions opts = autoAck
                    ? this.nonBlockingReadOptions.autoAcknowledge()
                    : this.nonBlockingReadOptions;

            List<ByteRecord> all = template.execute((RedisCallback<List<ByteRecord>>) connection ->
                    connection.streamCommands().xReadGroup(consumer, opts, rawOffsets));

            if (all == null || all.isEmpty()) {
                success = true;
                return grouped;
            }

            // demux: byte[] stream → K, RecycleLinkedList<ByteRecord> 桶 (从 ObjectPool 拿)
            for (ByteRecord rec : all) {
                K key = keySer.deserialize(rec.getStream());
                grouped.computeIfAbsent(key, RECORDS_BUCKET).add(rec);
            }
            success = true;
            return grouped;
        } finally {
            if (!success) {
                // 异常路径: 已 computeIfAbsent 进 grouped 的内层 RecycleLinkedList 也来自池, 必须 recycle;
                // 然后再 recycle 外层 grouped。否则池里少几个对象, 极端情况下池耗尽。
                for (RecycleLinkedList<ByteRecord> bucket : grouped.values()) {
                    bucket.recycle();
                }
                grouped.recycle();
            }
        }
    }

    /**
     * 业务作用：注册 task。group 订阅 → ManagedRunner 托管; 非 group → 独立线程 doLoop。
     *
     * @param task 见上述说明
     * @return 见上述说明。
     */
    @SuppressWarnings("unchecked")
    private Subscription doRegister(NasaStreamTask task) {

        Subscription subscription = new TaskSubscription(task);

        lifecycleMonitor.lock();
        try {
            this.subscriptions.add(subscription);
            if (this.running) {
                if (task instanceof BatchStreamPollTask<?, ?> bpt && bpt.isGroupTask()) {
                    this.assignToRunner((BatchStreamPollTask<K, V>) bpt);
                } else {
                    taskExecutor.execute(task);
                }
            }
        } finally {
            lifecycleMonitor.unlock();
        }

        return subscription;
    }

    /**
     * 业务作用：注销一个订阅并停止其拉取任务。
     *
     * @param subscription 订阅句柄
     * 返回: 无返回值。
     */
    @Override
    public void remove(Subscription subscription) {
        lifecycleMonitor.lock();
        try {
            if (subscriptions.contains(subscription)) {
                if (subscription.isActive()) {
                    subscription.cancel();
                }
                subscriptions.remove(subscription);
            }
        } finally {
            lifecycleMonitor.unlock();
        }
    }

    /**
     * 业务作用：按容器配置的序列化方式建出内部使用的操作模板。
     *
     * @param connectionFactory 连接工厂
     * @param containerOptions  容器消费选项
     * @return 操作模板。
     */
    private RedisTemplate<K, V> createRedisTemplate(RedisConnectionFactory connectionFactory,
                                                    StreamMessageListenerContainerOptions<K, V> containerOptions) {

        RedisTemplate<K, V> tpl = new RedisTemplate<>();
        tpl.setKeySerializer(containerOptions.getKeySerializer());
        tpl.setValueSerializer(containerOptions.getKeySerializer());
        tpl.setHashKeySerializer(containerOptions.getHashKeySerializer());
        tpl.setHashValueSerializer(containerOptions.getHashValueSerializer());
        tpl.setConnectionFactory(connectionFactory);
        tpl.afterPropertiesSet();

        return tpl;
    }

    // ---- inner classes ----

    /**
     * 托管 runner: 1 个线程轮询 N 个 task, 按 Redis Cluster slot 分桶批量 xReadGroup, 自适应调度 + 唤醒驱动。
     * <p>
     * <b>批量 poll 模型 (按 slot 分桶)</b>: 每周期遍历 tasks 收集所有就绪 task, 按 slot 分组各发一次
     * {@link BatchStreamMessageListenerContainer#batchPollAndDemux}, 拉到的 records 按 streamKey
     * demux 后各 task 独立 dispatch。
     * <ul>
     *   <li>Cluster 模式: 按真实 CRC16 slot 分桶, 规避多 stream xReadGroup 的 CROSSLOT 限制</li>
     *   <li>单节点模式: 全部 task 塞 slot=0 一个 bucket, 单次 RTT 拿所有 stream, batch 收益最大化</li>
     * </ul>
     * <p>
     * <b>唤醒机制 (LockSupport)</b>: 不用 Thread.sleep — runner 在 parkNanos 上停, 业务 lambda 完成时
     * 调 {@code task.onComplete.run()} → unpark(runnerThread) 立即唤醒, 避免业务跑完后 runner 还在
     * 傻等 pollTimeout 的延迟。container.stop 也通过 unpark 让 runner 退出 while。
     * <p>
     * <b>调度状态机</b>:
     * <ul>
     *   <li>managedSuccess=false → tryLock 未成功, 每 RETRY_INTERVAL_MS 重试 (跳出本轮 batch)</li>
     *   <li>!complete || !isPollReady → 业务 in-flight / 异步初始化中, 跳出本轮 batch 但 checkAlive 维持 holds 自检</li>
     *   <li>lastHadData=true → 热流, 立即加入本轮 batch</li>
     *   <li>!lastHadData &amp;&amp; now &lt; lastPollTime+pollTimeout → 冷流退避, 跳出本轮 batch</li>
     * </ul>
     * <p>
     * <b>同质性约定</b>: 同一个 runner 只接收相同 (group, consumer-name, autoAck) 的 task。
     * 同 container 可以注册不同 consumer, 但会被分配到不同 runner, 避免一次 XREADGROUP 混用多个 group。
     * <p>
     * <b>动态收缩</b>: task 退出导致 slot bucket 空 → 标 compactNeeded → 下周期顶部
     * 重建紧凑数据结构, 避免 slotTaskLists 长生命累积空 bucket。
     * <p>
     * 支持运行中动态添加 task , 新 task 放入 pending, 主循环 drainPending 按 slot 分桶取出。
     */
    static final class BatchAffinity {

        private final String group;
        private final String name;
        private final boolean autoAck;

        /**
         * 业务作用：描述一个订阅的归并特征：消费组、消费者名与是否自动确认。
         * 只有三者完全相同的订阅才可合并到同一执行器——确认策略不同的订阅合并后，
         * 其中一方的确认语义会被另一方覆盖。
         *
         * @param group    消费组名
         * @param name     消费者名
         * @param autoAck  是否自动确认
         */
        private BatchAffinity(String group, String name, boolean autoAck) {
            this.group = group;
            this.name = name;
            this.autoAck = autoAck;
        }

        /**
         * 业务作用：从一个拉取任务提取其归并特征。
         *
         * @param task 拉取任务
         * @return 归并特征。
         */
        static BatchAffinity from(BatchStreamPollTask<?, ?> task) {
            Consumer consumer = task.rawConsumer();
            Assert.notNull(consumer, "Consumer must not be null for managed BatchStream task");
            return new BatchAffinity(consumer.getGroup(), consumer.getName(), task.isAutoAcknowledge());
        }

        /**
         * 业务作用：判断一个拉取任务的归并特征是否与本特征一致，即能否合并到同一执行器。
         *
         * @param task 待判断的拉取任务
         * @return 可合并返回 true。
         */
        boolean matches(BatchStreamPollTask<?, ?> task) {
            Consumer consumer = task.rawConsumer();
            return consumer != null
                    && autoAck == task.isAutoAcknowledge()
                    && Objects.equals(group, consumer.getGroup())
                    && Objects.equals(name, consumer.getName());
        }

        /**
         * 业务作用：按消费组、消费者名与确认策略三项判等，使归并特征可作为分组键。
         *
         * @param o 待比较的对象
         * @return 三项全部相同返回 true。
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BatchAffinity that)) return false;
            return autoAck == that.autoAck
                    && Objects.equals(group, that.group)
                    && Objects.equals(name, that.name);
        }

        /**
         * 业务作用：与判等口径一致的散列，使归并特征可作为映射键。
         *
         * <p>参数说明: 无。
         *
         * @return 散列值。
         */
        @Override
        public int hashCode() {
            return Objects.hash(group, name, autoAck);
        }

        /**
         * 业务作用：产出便于排查的可读描述，日志中据此确认订阅被归并到了哪个执行器。
         *
         * <p>参数说明: 无。
         *
         * @return 归并特征的描述。
         */
        @Override
        public String toString() {
            return "group=" + group + ", consumer=" + name + ", autoAck=" + autoAck;
        }
    }

    /**
     * 业务作用：包住一个拉取任务，负责其启动、异常重启与停止，使单个订阅的故障不波及其它订阅。
     * 异常后按退避重启而非立即重试：Redis 不可用时立即重试会形成高频重连风暴。
     */
    static class ManagedRunner<K, V extends Record<K, ?>> implements Runnable {

        private final BatchStreamMessageListenerContainer<K, V> container;
        private final BatchAffinity affinity;
        private final RecycleLinkedList<BatchStreamPollTask<K, V>> pending = RecycleLinkedList.of();
        private volatile int totalTaskCount;
        /**
         * tasks 按 Redis Cluster slot 分组, 保证每次 batchPollAndDemux 的 key 在同一个 slot.
         * 用索引数组替代 Map 迭代, 热路径零 Iterator 分配:
         * - slotIndex: drainPending 时查找 slot → 数组下标 (仅 drainPending 用, 不在热路径)
         * - slotTaskLists[i]: 第 i 个 slot 的 task 链表 (cachedIterator 遍历, 零 GC)
         * - slotBatches[i]: 第 i 个 slot 的 ready 收集缓冲 (clear+复用, 零 GC)
         */
        private final java.util.concurrent.locks.ReentrantLock pendingLock = new java.util.concurrent.locks.ReentrantLock();
        private final HashMap<Integer, Integer> slotIndex = new HashMap<>();
        private final ArrayList<RecycleLinkedList<BatchStreamPollTask<K, V>>> slotTaskLists = new ArrayList<>();
        private final ArrayList<SlotBatch<K, V>> slotBatches = new ArrayList<>();
        private final Executor businessExecutor;
        private final long pollTimeoutMs;
        private final ErrorHandler errorHandler;
        private volatile boolean running = true;
        private volatile boolean gracefulStopRequested;
        private final CountDownLatch stopped = new CountDownLatch(1);
        /**
         * runner 线程引用, 给 task.onComplete 唤醒用
         */
        private volatile Thread runnerThread;
        /**
         * 缓存的唤醒回调, run() 入口创建一次, 所有 task 共享, 零 GC
         */
        private volatile Runnable wakeup;
        /**
         * Phase 1 内 task 退出时若该 slot bucket 变空, 置 true, 下一周期顶部触发 compactSlots
         * 收缩 slotTaskLists/slotBatches/slotIndex, 防止长生命 runner 累积空 bucket。
         * 仅 runner 单线程读写, 无并发。
         */
        private boolean compactNeeded = false;
        private static final long RETRY_INTERVAL_MS = 10_000;
        /**
         * drain 期间主循环回访 in-flight task 的最小间隔 ms. drain 等的是 listener 在 businessExecutor
         * 上跑完, 与冷流 1s 同量级即可, 不需要更密.
         */
        private static final long DRAIN_POLL_MS = 1_000;
        /**
         * 业务作用：建出一个执行器，承载归并特征相同的一组拉取任务。
         *
         * @param container        所属容器
         * @param affinity         本执行器接受的归并特征
         * @param businessExecutor 投递业务回调所用的线程池
         * @param pollTimeoutMs    单次阻塞读取的等待毫秒数，同时决定停机响应速度
         * @param errorHandler     拉取或投递失败时的处理器
         */
        ManagedRunner(BatchStreamMessageListenerContainer<K, V> container,
                      Executor businessExecutor, long pollTimeoutMs, ErrorHandler errorHandler,
                      BatchAffinity affinity) {
            this.container = container;
            this.businessExecutor = businessExecutor;
            this.pollTimeoutMs = pollTimeoutMs;
            this.errorHandler = errorHandler;
            this.affinity = affinity;
        }

        /**
         * per-slot 的 ready task + offset 收集缓冲, clear() 后复用
         */
        static class SlotBatch<K, V extends Record<K, ?>> {
            final ArrayList<BatchStreamPollTask<K, V>> tasks = new ArrayList<>();
            final ArrayList<StreamOffset<K>> offsets = new ArrayList<>();

            /**
             * 业务作用：清空本执行器承载的任务，用于其结束后释放引用。
             *
             * <p>参数说明: 无。
             *
             * 返回: 无返回值。
             */
            void clear() {
                tasks.clear();
                offsets.clear();
            }

            /**
             * 业务作用：判断本执行器内是否存在重复的 Stream 键。
             * <b>同一执行器内不允许重复</b>：一次读取命令中出现同一个键两次，服务端只返回一份结果，
             * 另一个任务将永远收不到消息且没有任何报错。
             *
             * <p>参数说明: 无。
             *
             * @return 存在重复返回 true。
             */
            boolean hasDuplicateStreamKey() {
                for (int i = 0, n = tasks.size(); i < n; i++) {
                    K key = tasks.get(i).streamKey();
                    for (int j = i + 1; j < n; j++) {
                        if (Objects.equals(key, tasks.get(j).streamKey())) {
                            return true;
                        }
                    }
                }
                return false;
            }
        }

        /**
         * 业务作用：判断本执行器是否接受给定归并特征的任务。
         *
         * @param affinity 归并特征
         * @return 接受返回 true。
         */
        boolean accepts(BatchAffinity affinity) {
            return this.affinity.equals(affinity);
        }

        /**
         * 业务作用：判断本执行器是否仍在接收新任务。
         * 已进入停止流程的执行器不再接收，新任务需由容器另建执行器承载。
         *
         * <p>参数说明: 无。
         *
         * @return 仍接收返回 true。
         */
        boolean isAcceptingTasks() {
            return running && stopped.getCount() > 0;
        }

        /**
         * 业务作用：把一个拉取任务加入本执行器。
         * 加入前校验 Stream 键不与已有任务重复——重复会让其中一个任务永远收不到消息。
         *
         * @param task 待加入的拉取任务
         * @return 加入成功返回 true；执行器已停止接收或键重复时返回 false。
         */
        boolean addTask(BatchStreamPollTask<K, V> task) {
            pendingLock.lock();
            try {
                if (!running || stopped.getCount() == 0 || !affinity.matches(task)) {
                    return false;
                }
                pending.add(task);
                Runnable cb = this.wakeup;
                if (cb != null) cb.run();
                return true;
            } finally {
                pendingLock.unlock();
            }
        }

        /**
         * 业务作用：读取本执行器承载的任务数，用于观测归并效果。
         *
         * <p>参数说明: 无。
         *
         * @return 任务数。
         */
        int taskCount() {
            pendingLock.lock();
            try {
                return totalTaskCount + pending.size();
            } finally {
                pendingLock.unlock();
            }
        }

        /**
         * 业务作用：向本执行器及其全部任务发出停止请求，不等待。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         */
        void requestStop() {
            gracefulStopRequested = true;
            running = false;
            Thread rt = runnerThread;
            if (rt != null) LockSupport.unpark(rt);
        }

        /**
         * 业务作用：限时等待本执行器真正停止。
         *
         * @param timeoutMs 等待毫秒数
         * @return 在时限内停止返回 true。
         * @throws InterruptedException 等待期间线程被中断
         */
        boolean awaitStop(long timeoutMs) throws InterruptedException {
            if (Thread.currentThread() == runnerThread) {
                return true;
            }
            return stopped.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        /**
         * 业务作用：从主循环中移除 task 并触发 lifecycle exitManaged.
         * 抽出 helper 替代 5 处重复的 "exitManaged + lastHadData=false + it.remove + totalTaskCount-- + compactNeeded" 样板.
         *
         * @param task  见上述说明
         * @param it    见上述说明
         * @param tasks 见上述说明
         */
        private void removeAndExit(BatchStreamPollTask<K, V> task,
                                   Iterator<BatchStreamPollTask<K, V>> it,
                                   RecycleLinkedList<BatchStreamPollTask<K, V>> tasks) {
            task.exitManaged(false);
            task.lastHadData = false;
            it.remove();
            totalTaskCount--;
            if (tasks.isEmpty()) compactNeeded = true;
        }

        /**
         * 业务作用：执行器主体：在一条独占连接上轮流为各任务拉取消息并分发。
         * <p>
         * 多个任务共用一条连接与一次读取命令，使连接数不随订阅数增长；
         * 代价是其中一个任务的业务回调卡住会拖慢同执行器上的其它任务，因此回调不应长时间阻塞。
         * <p>
         * 异常后不立即重试而是退避：服务端不可用时立即重试会形成高频重连风暴。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         */
        @Override
        public void run() {
            this.runnerThread = Thread.currentThread();
            this.wakeup = () -> LockSupport.unpark(this.runnerThread);
            try {
                while (running) {
                    this.drainPending();
                    // 上周期标记的空 bucket 在这里收缩, 周期顶部统一处理避免与 Phase 1 的迭代冲突
                    if (compactNeeded) {
                        this.compactSlots();
                        compactNeeded = false;
                    }

                    long now = System.currentTimeMillis();

                    long earliestNextWake = Long.MAX_VALUE;
                    int slotCount = slotTaskLists.size();

                    // 清空 per-slot 缓冲
                    for (int i = 0; i < slotCount; i++) {
                        slotBatches.get(i).clear();
                    }

                    // === Phase 1: 按 slot 索引遍历 tasks 分流 (零 Iterator 分配) ===
                    boolean hasReady = false;
                    for (int i = 0; i < slotCount; i++) {
                        RecycleLinkedList<BatchStreamPollTask<K, V>> tasks = slotTaskLists.get(i);
                        SlotBatch<K, V> sb = slotBatches.get(i);
                        Iterator<BatchStreamPollTask<K, V>> it = tasks.cachedIterator();
                        while (it.hasNext()) {
                            BatchStreamPollTask<K, V> task = it.next();

                            // === zombie / retry 分支: 未拿到锁, 每 RETRY_INTERVAL_MS 重试 tryLock ===
                            if (!task.managedSuccess) {
                                // markStop 后不能再 initManaged 抢锁, 否则 stopped zombie 会与真持有方一起在锁释放瞬间抢锁,
                                // 造成 rebalance 期间 thrashing. 包括首次进入 (lastRetryTime=0) 时也必须先 short-circuit.
                                if (task.isStopRequested()) {
                                    this.removeAndExit(task, it, tasks);
                                    continue;
                                }
                                // retry 期间锁丢 / cancel 等 checkAlive false 路径兜底.
                                if (task.lastRetryTime > 0 && !task.checkAlive()) {
                                    this.removeAndExit(task, it, tasks);
                                    continue;
                                }
                                long retryAt = task.lastRetryTime + RETRY_INTERVAL_MS;
                                if (now < retryAt) {
                                    if (retryAt < earliestNextWake) earliestNextWake = retryAt;
                                    continue;
                                }
                                try {
                                    if (task.initManaged()) {
                                        task.managedSuccess = true;
                                    } else {
                                        task.lastRetryTime = now;
                                        long next = now + RETRY_INTERVAL_MS;
                                        if (next < earliestNextWake) earliestNextWake = next;
                                        continue;
                                    }
                                } catch (Throwable t) {
                                    task.lastRetryTime = now;
                                    long next = now + RETRY_INTERVAL_MS;
                                    if (next < earliestNextWake) earliestNextWake = next;
                                    errorHandler.handleError(new RuntimeException("ManagedRunner initManaged failed", t));
                                    continue;
                                }
                            }

                            // === 业务 in-flight 或异步初始化中 ===
                            if (!task.complete || !task.isPollReady()) {
                                // 锁真丢失必须立即 exit，不能等待 drain；ACK fencing 会拒绝失权后的迟到确认。
                                if (task.isLockLost()) {
                                    this.removeAndExit(task, it, tasks);
                                    continue;
                                }
                                // 主动 stop 进入 drain 等待
                                if (task.isStopRequested()) {
                                    if (task.drainTimedOut(now)) {
                                        log.warn("partition drain timeout, force exit");
                                        this.removeAndExit(task, it, tasks);
                                        continue;
                                    }
                                    // drain 期间仍要 holds 自检 (不读 active/running 以免立即 exit)
                                    if (!task.checkAliveHoldsOnly()) {
                                        this.removeAndExit(task, it, tasks);
                                        continue;
                                    }
                                    long next = now + DRAIN_POLL_MS;
                                    if (next < earliestNextWake) earliestNextWake = next;
                                    continue;
                                }
                                // 非 stop 的 in-flight: 走原 checkAlive (含 active/running + holds 自检)
                                if (!task.checkAlive()) {
                                    // checkAlive 返回 false 可能因 markStop 刚触发 (race: 上面 isStopRequested 读到 false,
                                    // 之后 rebalance 调 markStop, checkAlive 内 beforePoll 读到 !active 返回 false).
                                    // 这种情况下不能立即 exit 绕过 drain — 重判 isStopRequested, 是的话切 drain 等待.
                                    // lockLost 也要重判: holds 失败可能在 checkAlive 内设置, 走 lockLost 立即 exit 路径.
                                    if (!task.isLockLost() && task.isStopRequested()) {
                                        long next = now + DRAIN_POLL_MS;
                                        if (next < earliestNextWake) earliestNextWake = next;
                                        continue;
                                    }
                                    this.removeAndExit(task, it, tasks);
                                    continue;
                                }
                                long next = now + 1000;
                                if (next < earliestNextWake) earliestNextWake = next;
                                continue;
                            }

                            // === complete=true && isPollReady=true: drain 已完成则立即 exit ===
                            if (task.isStopRequested()) {
                                this.removeAndExit(task, it, tasks);
                                continue;
                            }

                            // 冷流退避
                            if (!task.lastHadData) {
                                long pollAt = task.lastPollTime + pollTimeoutMs;
                                if (now < pollAt) {
                                    if (pollAt < earliestNextWake) earliestNextWake = pollAt;
                                    continue;
                                }
                            }

                            // beforePoll 存活自检
                            if (!task.checkAlive()) {
                                this.removeAndExit(task, it, tasks);
                                continue;
                            }

                            // 二次防御: checkAlive 内的 beforePoll 可能因 holds 限流命中直接 return true 而没刷状态,
                            // 或者本次 beforePoll 触发了 submitRecoverPending 设了 recovering=true. 重判 isLockLost / isPollReady
                            // 避免本轮加入 batch poll 与 recover dispatch 并发 (或在锁已丢的状态下读新消息).
                            if (task.isLockLost()) {
                                this.removeAndExit(task, it, tasks);
                                continue;
                            }
                            if (!task.isPollReady()) {
                                long next = now + 1000;
                                if (next < earliestNextWake) earliestNextWake = next;
                                continue;
                            }
                            if (!affinity.matches(task)) {
                                errorHandler.handleError(new IllegalStateException(
                                        "ManagedRunner affinity mismatch: expected " + affinity));
                                this.removeAndExit(task, it, tasks);
                                continue;
                            }

                            // 收集到本 slot 的 ready 缓冲
                            sb.tasks.add(task);
                            sb.offsets.add(task.currentStreamOffset());
                            hasReady = true;
                        }
                    }

                    // === Phase 2: 没就绪 task → sleep ===
                    if (!hasReady) {
                        long sleepMs = earliestNextWake == Long.MAX_VALUE
                                ? pollTimeoutMs
                                : Math.min(pollTimeoutMs, Math.max(1, earliestNextWake - System.currentTimeMillis()));
                        LockSupport.parkNanos(sleepMs * 1_000_000L);
                        if (Thread.interrupted()) break;
                        continue;
                    }

                    // === Phase 3+4: 按 slot 分别 batch poll + dispatch (索引遍历, 零 Iterator) ===
                    boolean anyHadData = false;
                    for (int i = 0; i < slotCount; i++) {
                        SlotBatch<K, V> sb = slotBatches.get(i);
                        int readySize = sb.tasks.size();
                        if (readySize == 0) continue;

                        BatchStreamPollTask<K, V> first = sb.tasks.getFirst();
                        Consumer batchConsumer = first.rawConsumer();
                        boolean batchAutoAck = first.isAutoAcknowledge();
                        if (batchConsumer == null) {
                            errorHandler.handleError(new IllegalStateException(
                                    "ManagedRunner got non-group task (consumer=null), this should not happen"));
                            continue;
                        }
                        if (sb.hasDuplicateStreamKey()) {
                            if (pollIndividuallyAndDispatch(sb, batchConsumer, batchAutoAck, now)) {
                                anyHadData = true;
                            }
                            continue;
                        }

                        RecycleLinkedMap<K, RecycleLinkedList<ByteRecord>> grouped;
                        try {
                            grouped = container.batchPollAndDemux(sb.offsets, batchConsumer, batchAutoAck);
                        } catch (RuntimeException ex) {
                            errorHandler.handleError(ex);
                            for (int j = 0; j < readySize; j++) {
                                BatchStreamPollTask<K, V> t = sb.tasks.get(j);
                                t.lastHadData = false;
                                t.lastPollTime = now;
                            }
                            continue;
                        }

                        try {
                            for (int j = 0; j < readySize; j++) {
                                BatchStreamPollTask<K, V> t = sb.tasks.get(j);
                                RecycleLinkedList<ByteRecord> myRecords = grouped.get(t.streamKey());
                                boolean taskHadData = myRecords != null && !myRecords.isEmpty();
                                t.handleBatchResult(myRecords, businessExecutor, now);
                                if (taskHadData) anyHadData = true;
                            }
                        } finally {
                            grouped.recycle();
                        }
                    }
                    // === Phase 5: 收尾 ===
                    if (anyHadData) {
                        Thread.yield();
                        continue;
                    }
                    long polledNextWake = now + pollTimeoutMs;
                    if (polledNextWake < earliestNextWake) earliestNextWake = polledNextWake;
                    long sleepMs = Math.min(pollTimeoutMs,
                            Math.max(1, earliestNextWake - System.currentTimeMillis()));
                    LockSupport.parkNanos(sleepMs * 1_000_000L);
                    if (Thread.interrupted()) break;
                }
            } catch (Throwable t) {
                errorHandler.handleError(new RuntimeException("ManagedRunner crashed", t));
            } finally {
                this.closeForNewTasks();
                // 退出收尾: 所有 slot 的 tasks 全部 drain-aware 清理
                this.drainPending();
                this.finallyDrainAndExit();
                pending.recycle();
                container.runnerTerminated(this);
                stopped.countDown();
            }
        }

        /**
         * 业务作用：停止接收新任务，进入停机流程的第一步，使停机期间不再有任务被加进来。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         */
        private void closeForNewTasks() {
            pendingLock.lock();
            try {
                running = false;
            } finally {
                pendingLock.unlock();
            }
        }

        /**
         * 业务作用：合并读取失败时回退到逐个任务单独读取。
         * <p>
         * 回退的意义在于隔离故障：合并读取只要有一个键出问题（如被删除、类型不符）整批都会失败，
         * 逐个读取能让其余键继续正常消费，只让真正有问题的那个失败。
         *
         * @param sb            本轮的任务批次
         * @param batchConsumer 批量投递的消费入口
         * @param batchAutoAck  本批是否自动确认
         * @param now           本轮的时刻，用于判定各任务的排空超时
         * @return 本轮是否有任务成功读取到消息。
         */
        private boolean pollIndividuallyAndDispatch(SlotBatch<K, V> sb, Consumer batchConsumer, boolean batchAutoAck, long now) {
            boolean anyHadData = false;
            for (int j = 0, readySize = sb.tasks.size(); j < readySize; j++) {
                BatchStreamPollTask<K, V> task = sb.tasks.get(j);
                RecycleLinkedMap<K, RecycleLinkedList<ByteRecord>> grouped = null;
                try {
                    grouped = container.batchPollAndDemux(List.of(sb.offsets.get(j)), batchConsumer, batchAutoAck);
                    RecycleLinkedList<ByteRecord> records = grouped.get(task.streamKey());
                    boolean taskHadData = records != null && !records.isEmpty();
                    task.handleBatchResult(records, businessExecutor, now);
                    if (taskHadData) {
                        anyHadData = true;
                    }
                } catch (RuntimeException ex) {
                    errorHandler.handleError(ex);
                    task.lastHadData = false;
                    task.lastPollTime = now;
                } finally {
                    if (grouped != null) {
                        grouped.recycle();
                    }
                }
            }
            return anyHadData;
        }

        /**
         * 业务作用：Finally 路径下对所有残留 task 走 drain-aware 退出:
         * <ul>
         *   <li>已 markStop ({@code isStopRequested=true}): 等待 in-flight listener / recoverPending 完成,
         *       或 {@code drainTimedOut} 后强制 exit。退出后产生的 ACK/XDEL 由 fencing 拒绝。</li>
         *   <li>未 markStop 但 in-flight: 上游忘了 markStop, 框架无法 drain. 记 warn + 直接 exit.</li>
         *   <li>空闲 ({@code complete &amp;&amp; isPollReady}): 直接 exit.</li>
         * </ul>
         * 总耗时上界 = 各 task drainTimeoutMs 之和, 但同节点多 task 实际并行 drain (各自 listener 在 businessExecutor
         * 内独立跑), 阻塞仅是 polling, 与 task 数线性但常数很小 (50ms tick).
         */
        private void finallyDrainAndExit() {
            for (int i = 0, n = slotTaskLists.size(); i < n; i++) {
                RecycleLinkedList<BatchStreamPollTask<K, V>> tasks = slotTaskLists.get(i);
                for (BatchStreamPollTask<K, V> task : tasks) {
                    try {
                        this.drainAndExitOne(task);
                    } catch (Throwable e) {
                        errorHandler.handleError(new RuntimeException("ManagedRunner exitManaged threw", e));
                    }
                }
                tasks.recycle();
            }
        }

        /**
         * 业务作用：单个 task 的 finally drain 退出. polling 间隔 50ms, 由 drainTimedOut 提供超时封顶.
         *
         * @param task 见上述说明
         */
        private void drainAndExitOne(BatchStreamPollTask<K, V> task) {
            // 已经 exit 不复议: lockLost 走立即退出
            if (task.isLockLost()) {
                task.exitManaged(false);
                return;
            }
            if (!task.isStopRequested() && gracefulStopRequested) {
                task.requestStop();
            }
            // 未 markStop 且不是 graceful stop 的 in-flight: 多半是 runner 非正常崩溃, 只能直接 exit + warn.
            // 空闲 task 也走这条路径, 不影响 (exitManaged 内部幂等性靠 PollLifecycle.afterExit 唯一调用语义).
            if (!task.isStopRequested()) {
                if (!task.complete || !task.isPollReady()) {
                    log.warn("ManagedRunner finally: task in-flight but no stop request, force exit after runner abort");
                }
                task.exitManaged(false);
                return;
            }
            // 已 markStop: 轮询等 drain 完成或超时
            while (!task.complete || !task.isPollReady()) {
                long now = System.currentTimeMillis();
                if (task.isLockLost()) break;
                if (task.drainTimedOut(now)) {
                    log.warn("ManagedRunner finally: partition drain timeout, force exit");
                    break;
                }
                // 主动 holds 自检: 锁真丢能立即 break 不等 drain timeout (拉长 shutdown 等待).
                // checkAliveHoldsOnly 内部有 holdsCheckIntervalMs 限流, 不会每 50ms 都 EVAL.
                if (!task.checkAliveHoldsOnly()) break;
                // 50ms tick: in-flight listener 多在 ms~百 ms 级, 50ms 兼顾响应性与 CPU
                LockSupport.parkNanos(50L * 1_000_000L);
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            task.exitManaged(false);
        }

        /**
         * 业务作用：将 pending 中的新 task 按 slot 分组 (不做 initManaged, 由主循环统一处理)。
         * slotIndex (HashMap) 仅在此方法中使用, 不在热路径。
         * <p>
         * Cluster 模式: 真实 CRC16 slot, 同 slot 的 task 进同一 bucket → 多 stream xReadGroup 不踩 CROSSLOT;
         * 单节点模式: 所有 task 塞 slot=0 一个 bucket → 单次 RTT 拿所有 stream, batch 收益最大化。
         */
        @SuppressWarnings("unchecked")
        private void drainPending() {
            if (pending.isEmpty()) return;
            pendingLock.lock();
            try {
                if (pending.isEmpty()) return;
                RedisSerializer<K> keySer = (RedisSerializer<K>) container.template.getKeySerializer();
                boolean cluster = container.clusterMode;
                for (BatchStreamPollTask<K, V> task : pending) {
                    task.onComplete = this.wakeup;
                    // 单节点全塞 slot=0, 不算 CRC16 节省 CPU + batch 1 RTT 不退化
                    int slot = cluster ? SlotHash.getSlot(keySer.serialize(task.streamKey())) : 0;
                    Integer idx = slotIndex.get(slot);
                    if (idx == null) {
                        idx = slotTaskLists.size();
                        slotIndex.put(slot, idx);
                        slotTaskLists.add(RecycleLinkedList.of());
                        slotBatches.add(new SlotBatch<>());
                    }
                    slotTaskLists.get(idx).add(task);
                    totalTaskCount++;
                }
                pending.clear();
            } finally {
                pendingLock.unlock();
            }
        }

        /**
         * 业务作用：收缩 slotTaskLists / slotBatches / slotIndex: 把空 bucket 删掉, 重新映射 slot → 紧凑数组下标。
         * <p>
         * 仅 runner 线程在主循环顶部调用, 不与 Phase 1 迭代并发, 不与 drainPending 并发
         * (drainPending 也只 runner 线程调)。
         * <p>
         * 触发条件: Phase 1 内 task 退出导致某 slot bucket 变空 → 设 compactNeeded=true → 下周期触发。
         * 实现策略: 一次性扫 slotIndex 重建紧凑数据结构, 空 bucket 的 RecycleLinkedList 回池。
         * 分配新 HashMap+ArrayList 在冷路径(rebalance / 节点崩溃后), 不影响热路径吞吐。
         */
        private void compactSlots() {
            if (slotIndex.isEmpty()) return;
            int kept = 0;
            // 用临时 array 保存 (slot, oldIdx) 配对, 避免边遍历边修改 slotIndex
            int[] keptSlots = new int[slotIndex.size()];
            int[] keptOldIdx = new int[slotIndex.size()];
            for (Map.Entry<Integer, Integer> e : slotIndex.entrySet()) {
                int oldIdx = e.getValue();
                RecycleLinkedList<BatchStreamPollTask<K, V>> list = slotTaskLists.get(oldIdx);
                if (list.isEmpty()) {
                    list.recycle();
                    // SlotBatch 是 ArrayList 包装, 不池化, GC 处理 (引用置 null 后会被回收)
                } else {
                    keptSlots[kept] = e.getKey();
                    keptOldIdx[kept] = oldIdx;
                    kept++;
                }
            }
            if (kept == slotIndex.size()) return;   // 没空 bucket, 实际无需 compact (理论上不会, 防御)

            // 物理重排: 保留的 list/batch 按 oldIdx 顺序紧凑到数组前部
            // 注意: keptOldIdx 不一定有序, 这里按 keptOldIdx 升序排能保证 src 总在 dst 之后, 不会覆盖未读数据。
            // 但简化起见用临时数组持有保留对象, 然后 clear+addAll 重建 — 一次小额 ArrayList 扩容是可接受代价。
            ArrayList<RecycleLinkedList<BatchStreamPollTask<K, V>>> keptLists = new ArrayList<>(kept);
            ArrayList<SlotBatch<K, V>> keptBatches = new ArrayList<>(kept);
            for (int i = 0; i < kept; i++) {
                int oldIdx = keptOldIdx[i];
                keptLists.add(slotTaskLists.get(oldIdx));
                keptBatches.add(slotBatches.get(oldIdx));
            }
            slotTaskLists.clear();
            slotTaskLists.addAll(keptLists);
            slotBatches.clear();
            slotBatches.addAll(keptBatches);
            slotIndex.clear();
            for (int i = 0; i < kept; i++) {
                slotIndex.put(keptSlots[i], i);
            }
        }
    }

    /**
     * 业务作用：一个订阅的对外句柄，暴露其运行状态并支持取消。
     * 取消是幂等的，重复调用不会产生副作用。
     */
    static class TaskSubscription implements Subscription {

        private final NasaStreamTask task;

        /**
         * 业务作用：把一个拉取任务包成对外的订阅句柄。
         *
         * @param task 拉取任务
         */
        TaskSubscription(NasaStreamTask task) {
            this.task = task;
        }

        /**
         * 业务作用：取出句柄背后的拉取任务，供容器内部管理使用。
         *
         * <p>参数说明: 无。
         *
         * @return 拉取任务。
         */
        NasaStreamTask getTask() {
            return task;
        }

        /**
         * 业务作用：判断本订阅是否仍在消费。
         *
         * <p>参数说明: 无。
         *
         * @return 仍在消费返回 true。
         */
        @Override
        public boolean isActive() {
            return task.isActive();
        }

        /**
         * 业务作用：限时等待本订阅进入运行态，用于确认订阅确实已生效。
         *
         * @param timeout 等待时长
         * @return 在时限内生效返回 true。
         * @throws InterruptedException 等待期间线程被中断
         */
        @Override
        public boolean await(Duration timeout) throws InterruptedException {
            return task.awaitStart(timeout);
        }

        /**
         * 业务作用：取消本订阅并等待其任务真正停止。幂等，重复调用无副作用。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         * @throws org.springframework.dao.DataAccessResourceFailureException 停止过程中访问失败
         */
        @Override
        public void cancel() throws DataAccessResourceFailureException {
            if (task instanceof BatchStreamPollTask<?, ?> batchTask) {
                batchTask.requestStop();
                return;
            }
            task.cancel();
        }

        /**
         * 业务作用：按背后的拉取任务判等，使同一任务的多个句柄视为相等。
         *
         * @param o 待比较的对象
         * @return 指向同一任务返回 true。
         */
        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskSubscription that = (TaskSubscription) o;
            return ObjectUtils.nullSafeEquals(task, that.task);
        }

        /**
         * 业务作用：与判等口径一致的散列。
         *
         * <p>参数说明: 无。
         *
         * @return 散列值。
         */
        @Override
        public int hashCode() {
            return ObjectUtils.nullSafeHashCode(task);
        }
    }
}
