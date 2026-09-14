package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ReadOffset;

import java.util.Base64;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 业务作用：订阅定向 Fanout Pub/Sub 通知，读取并持久确认权威 shard，再以公共状态脚本启动和完成 Worker。
 *
 * <p>Java 运行时不轮询 inbox Stream；通知只携带定位与 assignment 代次，receipt/ready 索引负责丢失后的
 * 重新发布，inbox 消息 ID 用于确认、撤销旧 assignment 和终态清理。
 */
@Slf4j
final class RedisJobFanoutDispatcher implements AutoCloseable {

    private static final String INBOX_GROUP = "redis-job-fanout";
    private final RedisProxy redisProxy;
    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final RedisJobExecutorRegistry registry;
    private final RedisJobJsonCodec jsonCodec;
    private final RedisJobPubSub pubSub;
    private final Consumer<String> receiptListener;
    private final RedisJobLeaseRenewer leaseRenewer;
    private final RedisJobMetrics metrics;
    private final RedisJobPreStartBarrier preStartBarrier;
    private final Map<String, RedisJobDefinition> definitions = new ConcurrentHashMap<>();
    private final Map<String, RedisJobHandler> handlers = new ConcurrentHashMap<>();
    private final Map<String, Boolean> executions = new ConcurrentHashMap<>();
    private final Set<String> receiptChannels = ConcurrentHashMap.newKeySet();
    private final Semaphore capacity;
    private final ExecutorService businessExecutor;
    private final ExecutorService notificationExecutor;
    private final ScheduledExecutorService controlExecutor;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch closeCompleted = new CountDownLatch(1);
    private volatile Throwable closeFailure;

    /**
     * 业务作用：建立具有独立容量配额的 Fanout 接收与执行通道，避免普通派发挤占确认能力。
     *
     * <p>返回：创建入口默认关闭、尚未开始接收通知的 Fanout 派发器。
     *
     * @param redisProxy      Redis 命令代理
     * @param properties      Job 配置
     * @param keys            键路由器
     * @param scripts         状态脚本
     * @param registry        执行器注册表
     * @param jsonCodec       Job JSON Codec
     * @param receiptListener Fanout 回执信号处理器
     * @param leaseRenewer    批量租约续期器
     * @param metrics         基础指标容器
     * @param preStartBarrier 普通与 Fanout 共享的停机前置回调屏障
     */
    RedisJobFanoutDispatcher(RedisProxy redisProxy, RedisJobProperties properties, RedisJobKeyspace keys,
                             RedisJobScriptExecutor scripts, RedisJobExecutorRegistry registry,
                             RedisJobJsonCodec jsonCodec,
                             Consumer<String> receiptListener,
                             RedisJobLeaseRenewer leaseRenewer, RedisJobMetrics metrics,
                             RedisJobPreStartBarrier preStartBarrier) {
        this.redisProxy = redisProxy;
        this.properties = properties;
        this.keys = keys;
        this.scripts = scripts;
        this.registry = registry;
        this.jsonCodec = jsonCodec;
        this.receiptListener = receiptListener;
        this.leaseRenewer = Objects.requireNonNull(leaseRenewer, "leaseRenewer must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.preStartBarrier = Objects.requireNonNull(preStartBarrier, "preStartBarrier must not be null");
        this.pubSub = new RedisJobPubSub(redisProxy, properties.getPubsubMode());
        this.capacity = new Semaphore(properties.getHandlerCapacity());
        this.businessExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("redis-job-fanout-handler-", 0).factory());
        this.notificationExecutor = new ThreadPoolExecutor(1, Math.max(2, properties.getHandlerCapacity()),
                30L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Math.max(16, properties.getHandlerCapacity() * 4)),
                Thread.ofPlatform().daemon().name("redis-job-fanout-notify-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
        this.controlExecutor = RedisJobShutdownSupport.scheduledExecutor(1,
                Thread.ofPlatform().daemon().name("redis-job-fanout-control-", 0).factory());
    }

    /**
     * 业务作用：登记能够在本节点执行的 Fanout Worker。
     *
     * @param definition Worker 定义
     * @param handler    Worker Handler
     *                   返回：无返回值。
     */
    void register(RedisJobDefinition definition, RedisJobHandler handler) {
        definitions.put(definition.workerName(), definition);
        handlers.put(definition.workerName(), handler);
    }

    /**
     * 业务作用：撤销已删除 Worker 的本地定义与 Handler，使新通知不再被本节点认领执行。
     *
     * <p>能力名可能被多个本地定义共享，仅在无其它定义仍使用时移除；已取得执行权的 shard 按
     * "允许当前 attempt 完成"语义继续，由持有线程自行收尾。
     *
     * @param definition       Worker 定义
     * @param workerStillUsed  同名能力是否仍被其它本地定义使用
     *                         返回：无返回值。
     */
    void unregister(RedisJobDefinition definition, boolean workerStillUsed) {
        if (workerStillUsed) return;
        definitions.remove(definition.workerName());
        handlers.remove(definition.workerName());
    }

    /**
     * 业务作用：在能力声明 fanoutReady 之前订阅全部固定桶的本节点定向频道。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void start() {
        if (closed.get()) throw new IllegalStateException("RedisJobFanoutDispatcher is closed");
        if (!running.compareAndSet(false, true)) return;
        List<String> channels = new ArrayList<>(keys.fanoutBucketCount() * 2);
        for (int bucket = 0; bucket < keys.fanoutBucketCount(); bucket++) {
            channels.add(keys.fanoutNotifyChannel(bucket, registry.nodeIdentity()));
            String receiptChannel = keys.fanoutReceiptChannel(bucket, registry.executorId());
            receiptChannels.add(receiptChannel);
            channels.add(receiptChannel);
        }
        try {
            pubSub.start(channels, this::enqueueSignal);
        } catch (RuntimeException error) {
            running.set(false);
            pubSub.close();
            throw error;
        }
    }

    /**
     * 业务作用：把 Lettuce I/O 线程上的轻量信封转交专用控制队列，避免同步 Redis 复验阻塞网络事件循环。
     *
     * @param channel  收到消息的频道
     * @param envelope 轻量信封
     *                 返回：无返回值；队列饱和时由持久索引后续重发。
     */
    private void enqueueSignal(String channel, String envelope) {
        try {
            if (receiptChannels.contains(channel)) {
                notificationExecutor.execute(() -> handleSignal(channel, envelope, true));
            } else {
                notificationExecutor.execute(() -> handleSignal(channel, envelope, false));
            }
        } catch (RejectedExecutionException error) {
            log.warn("RedisJob fanout notification queue is full: channel={}", channel);
        }
    }

    /**
     * 业务作用：隔离单次通知处理异常，确保后续通知仍可消费，并由持久索引继续承担补偿重试。
     *
     * @param channel  收到消息的频道
     * @param envelope 轻量信封
     * @param receipt  是否为回执信号
     *                 返回：无返回值；处理失败时记录定位信息但不终止通知线程。
     */
    private void handleSignal(String channel, String envelope, boolean receipt) {
        try {
            if (receipt) {
                receiptListener.accept(envelope);
            } else {
                onNotification(envelope);
            }
        } catch (RuntimeException error) {
            log.error("RedisJob fanout notification processing failed: channel={}", channel, error);
        }
    }

    /**
     * 业务作用：解析只含定位字段的通知，并从持久 shard 读取全部执行参数。
     *
     * @param envelope 通知信封
     *                 返回：无返回值。
     */
    private void onNotification(String envelope) {
        if (!running.get() || !accepting.get()) return;
        RedisJobPreStartBarrier.Lease preStart = preStartBarrier.enter();
        if (preStart == null) return;
        try {
            String[] fields = envelope.split("\\|", -1);
            if (fields.length != 4) {
                log.warn("RedisJob fanout notification is invalid");
                return;
            }
            String fanoutId = fields[0];
            long seq;
            long epoch;
            try {
                seq = Long.parseLong(fields[1]);
                epoch = Long.parseLong(fields[2]);
            } catch (NumberFormatException error) {
                log.warn("RedisJob fanout notification has invalid numeric fields");
                return;
            }
            String messageId = fields[3];
            FanoutShardData shard = read(fanoutId, seq);
            if (shard == null || !registry.nodeIdentity().equals(shard.targetNodeIdentity())
                    || epoch != shard.assignmentEpoch()) return;
            RedisJobDefinition definition = definitions.get(shard.workerName());
            RedisJobHandler handler = handlers.get(shard.workerName());
            if (definition == null || handler == null) return;
            createInboxGroup(keys.fanoutInbox(fanoutId, registry.nodeIdentity()));
            List<Object> accepted = scripts.list(RedisJobScript.FANOUT_ACCEPT_SHARD,
                    new String[]{keys.fanoutRoot(fanoutId), keys.fanoutShard(fanoutId, seq),
                            keys.fanoutReceipts(fanoutId), keys.fanoutReady(fanoutId),
                            keys.fanoutReceiptChannel(fanoutId, shard.originExecutorId())},
                    fanoutId, seq, registry.nodeIdentity(), registry.executorId(), epoch,
                    properties.getMinScanIntervalMs(), pubSub.publishCommand(),
                    // 持久确认前复验来源、协议代次与本地 Worker 合同：确认即承诺执行，
                    // 不可执行的 shard 必须留在 AWAITING_RECEIPT，由接收重试按失败策略换兼容节点。
                    keys.qualifier(), properties.getProtocolVersion(),
                    definition.contractRevision(), definition.schemaId(), definition.wireCodecs());
            String acceptCode = value(accepted, 0);
            if (!"OK".equals(acceptCode) && !"ADOPTED".equals(acceptCode)) {
                metrics.incrementClassified("redis_job_fanout_accept", acceptCode, "");
                return;
            }
            metrics.increment("redis_job_fanout_received_total");
            start(definition, handler, read(fanoutId, seq), messageId);
        } finally {
            preStart.close();
        }
    }

    /**
     * 业务作用：为稳定 inbox 建立消费组，使 start 的消息确认与删除具有统一协议入口。
     *
     * @param inbox inbox Stream 键
     *              返回：无返回值。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void createInboxGroup(String inbox) {
        try {
            redisProxy.getRedisTemplate().opsForStream().createGroup(inbox, ReadOffset.from("0-0"), INBOX_GROUP);
        } catch (RuntimeException error) {
            if (!contains(error, "BUSYGROUP")) throw error;
        }
    }

    /**
     * 业务作用：停止接收新的定向分片，但继续处理已开始 Worker 的续期和完成。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void drain() {
        accepting.set(false);
    }

    /**
     * 业务作用：通知订阅仍就绪时重新开放定向分片接收。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void activate() {
        accepting.set(true);
    }

    /**
     * 业务作用：以公共 start_run.lua 为 shard 分配 attempt、token 和租约后提交 Worker。
     *
     * @param definition Worker 定义
     * @param handler    Worker Handler
     * @param shard      shard 数据
     * @param messageId  inbox 消息标识
     *                   返回：无返回值。
     */
    private void start(RedisJobDefinition definition, RedisJobHandler handler,
                       FanoutShardData shard, String messageId) {
        if (shard == null) return;
        // 尚未请求 attempt 时遵守最新终态；已经进入 Redis 的回包则由下方在途交接路径负责收口。
        if (!accepting.get() || preStartBarrier.isClosed()) return;
        if (!capacity.tryAcquire()) {
            deferForCapacity(shard);
            return;
        }
        String fanoutId = shard.fanoutId();
        long seq = shard.seq();
        List<Object> response;
        try {
            response = scripts.list(RedisJobScript.START_RUN,
                    new String[]{keys.fanoutShard(fanoutId, seq), keys.fanoutLeases(fanoutId),
                            keys.fanoutReady(fanoutId), keys.fanoutRoot(fanoutId), keys.fanoutReceipts(fanoutId),
                            keys.fanoutRoot(fanoutId), keys.fanoutInbox(fanoutId, registry.nodeIdentity()),
                            keys.fanoutCompletion(fanoutId), keys.fanoutRoot(fanoutId)},
                    shard.workerName(), RedisJobIdentifiers.shardRunId(fanoutId, seq), registry.executorId(),
                    messageId, properties.getLeaseMs(), RedisJobConcurrency.PARALLEL.name(), 0,
                    properties.getMinScanIntervalMs(), INBOX_GROUP, properties.getFanoutRetentionMs(),
                    // ARGV[16] 期望 executionKey: shard 记录的身份字段被错误改写时在取得执行权前拒绝
                    "FANOUT", registry.nodeIdentity(), shard.assignmentEpoch(), fanoutId, seq,
                    keys.executionKey(fanoutId, seq), "", "",
                    // 取得执行权前复验来源、协议代次与本地 Worker 合同, 与普通 Run 同一条不变量
                    keys.qualifier(), properties.getProtocolVersion(),
                    definition.contractRevision(), definition.schemaId(), definition.wireCodecs());
        } catch (RuntimeException error) {
            capacity.release();
            throw error;
        }
        String code = value(response, 0);
        if (!"STARTED".equals(code) && !"ADOPTED".equals(code)) {
            metrics.incrementClassified("redis_job_fanout_start", code, "");
            capacity.release();
            return;
        }
        metrics.increment("redis_job_fanout_shard_started_total");
        int attempt = Integer.parseInt(value(response, 1));
        long token = Long.parseLong(value(response, 2));
        String executionId = fanoutId + ':' + seq + ':' + token;
        if (executions.putIfAbsent(executionId, Boolean.TRUE) != null) {
            capacity.release();
            return;
        }
        boolean shutdownHandoff = preStartBarrier.isClosed() || !accepting.get();
        // Redis 已授予 attempt 时先登记在途，再交给业务执行器；停机屏障不会在两份账目之间观察到零。
        registry.executionStarted();
        try {
            businessExecutor.submit(() -> execute(definition, handler, shard, attempt, token, executionId));
            if (shutdownHandoff) metrics.increment("redis_job_shutdown_handoff_total");
        } catch (Throwable submissionFailure) {
            // shard attempt 已经取得权威，开放态拒绝、关闭态拒绝或执行器 Error 都必须由当前通知线程接管，
            // 只有 execute 的 finally 才能同时释放 Registry 在途、执行去重和容量账目。
            metrics.increment("redis_job_shutdown_handoff_total");
            try {
                execute(definition, handler, shard, attempt, token, executionId);
            } catch (Throwable executionFailure) {
                submissionFailure.addSuppressed(executionFailure);
            }
            RedisJobShutdownSupport.rethrow(submissionFailure);
        }
    }

    /**
     * 业务作用：本地执行槽暂满时保留当前 assignment，并把已确认分片延后到下一轮可见性扫描。
     *
     * <p>节点已经收到定向通知且通过合同复验，短时容量不足不能作为目标失联证据；连续等待超过容量
     * 窗口后，非严格策略开放容量路由，使其它兼容节点有机会接管；严格快照保留固定目标与既有退避。
     *
     * @param shard 已确认接收的 shard
     *              返回：无返回值；assignment 已变化时不改写新代次。
     */
    private void deferForCapacity(FanoutShardData shard) {
        List<Object> response = scripts.list(RedisJobScript.FANOUT_DEFER_READY,
                new String[]{keys.fanoutShard(shard.fanoutId(), shard.seq()),
                        keys.fanoutReady(shard.fanoutId()), keys.fanoutRoot(shard.fanoutId())},
                shard.fanoutId(), shard.seq(), registry.nodeIdentity(), shard.assignmentEpoch(),
                properties.getMinScanIntervalMs(), properties.getFanoutCapacityWaitMs(),
                properties.getReadyMaxWakeups(), "CONTINUE");
        String code = value(response, 0);
        if ("CAPACITY_EXHAUSTED".equals(code)) {
            metrics.increment("redis_job_fanout_capacity_exhausted_total");
        } else if (!"DEFERRED".equals(code) && !"STALE".equals(code) && !"STALE_ASSIGNMENT".equals(code)) {
            metrics.incrementClassified("redis_job_fanout_capacity_defer", code, "");
        }
    }

    /**
     * 业务作用：运行已取得 shard 执行权的 Worker，并通过公共 finish_run.lua 聚合根终态。
     *
     * @param definition  Worker 定义
     * @param handler     Worker Handler
     * @param shard       shard 数据
     * @param attempt     当前 attempt
     * @param token       当前 token
     * @param executionId 本地执行标识
     *                    返回：无返回值。
     */
    private void execute(RedisJobDefinition definition, RedisJobHandler handler, FanoutShardData shard,
                         int attempt, long token, String executionId) {
        long executionStartedAt = System.nanoTime();
        RedisJobLeaseRenewer.LeaseHandle leaseHandle = null;
        ScheduledFuture<?> timeoutTask = null;
        AtomicBoolean timedOut = new AtomicBoolean();
        try {
            RedisJobRun run = new RedisJobRun(RedisJobIdentifiers.shardRunId(shard.fanoutId(), shard.seq()),
                    shard.workerName(), shard.workerName(), RedisJobState.RUNNING, 0L, 0L, attempt, token,
                    registry.executorId(), 0L, "", "", "", "");
            RedisJobRepository.RunData runData = new RedisJobRepository.RunData(run, shard.payload(),
                    shard.schemaId(), shard.codec(), shard.fanoutId(), shard.snapshotId(), 0, 0L);
            RedisJobFanoutContext fanoutContext = new FanoutContext(shard);
            DefaultRedisJobContext context = new DefaultRedisJobContext(keys.qualifier(),
                    keys.namespace(), runData, attempt, token,
                    properties.getLeaseMs(), properties.getRenewRttAllowanceMs() + properties.getClockDriftAllowanceMs(),
                    jsonCodec, null, fanoutContext);
            leaseHandle = leaseRenewer.registerFanout(shard.fanoutId(), shard.seq(), context);
            timeoutTask = controlExecutor.schedule(() -> {
                timedOut.set(true);
                context.requestCancellation();
            }, Math.min(definition.timeoutMs(), properties.getMaxRunDurationMs()), TimeUnit.MILLISECONDS);
            RedisJobResult result;
            try {
                result = handler.handle(context);
                if (result == null) result = RedisJobResult.success();
            } catch (RedisJobExecutionStoppedException error) {
                result = RedisJobResult.retry(error.getMessage());
            } catch (Throwable error) {
                result = RedisJobResult.retry(error.getClass().getName() + ": " + error.getMessage());
            }
            finish(definition, shard, token,
                    timedOut.get() ? RedisJobResult.timeout("handler exceeded execution timeout") : result);
        } finally {
            if (leaseHandle != null) leaseHandle.close();
            if (timeoutTask != null) timeoutTask.cancel(false);
            executions.remove(executionId);
            registry.executionFinished();
            capacity.release();
            metrics.gauge("redis_job_fanout_handler_duration_ms",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - executionStartedAt));
        }
    }

    /**
     * 业务作用：提交 shard 结果并在最后一个分片结束时原子决定 Fanout 根终态。
     *
     * @param definition Worker 定义
     * @param shard      shard 数据
     * @param token      当前 token
     * @param result     Worker 结果
     *                   返回：无返回值。
     */
    private void finish(RedisJobDefinition definition, FanoutShardData shard, long token, RedisJobResult result) {
        List<Object> response = scripts.list(RedisJobScript.FINISH_RUN,
                new String[]{keys.fanoutShard(shard.fanoutId(), shard.seq()), keys.fanoutLeases(shard.fanoutId()),
                        keys.fanoutReady(shard.fanoutId()), keys.fanoutReceipts(shard.fanoutId()),
                        keys.fanoutRoot(shard.fanoutId()), keys.fanoutRoots(shard.fanoutId()),
                        keys.fanoutCompletion(shard.fanoutId()), keys.fanoutGc(shard.fanoutId())},
                RedisJobIdentifiers.shardRunId(shard.fanoutId(), shard.seq()), registry.executorId(), token,
                shard.workerName(), result.code().name(), result.summary(), definition.maxAttempts(),
                definition.retryDelayMs(), properties.getFanoutRetentionMs(), "FANOUT",
                shard.fanoutId(), shard.seq(), shard.assignmentEpoch(),
                // 完成结果只能记到本节点实际执行的分片身份上, executionKey 由本地键路由权威重新推导
                keys.executionKey(shard.fanoutId(), shard.seq()));
        String state = value(response, 1);
        if (!state.isEmpty()) metrics.incrementClassified("redis_job_fanout_shard", state, "");
        String finishCode = value(response, 0);
        if (!"OK".equals(finishCode)) metrics.incrementClassified("redis_job_fanout_finish", finishCode, "");
        if ("STALE_ASSIGNMENT".equals(finishCode)) {
            metrics.increment("redis_job_fanout_stale_assignment_total");
        }
    }

    /**
     * 业务作用：读取 shard 的持久权威字段，通知内容只用于定位。
     *
     * @param fanoutId Fanout 标识
     * @param seq      分片序号
     * @return shard 不存在时为 null。
     */
    private FanoutShardData read(String fanoutId, long seq) {
        List<Object> values = scripts.list(RedisJobScript.READ_FANOUT_SHARD,
                new String[]{keys.fanoutShard(fanoutId, seq)});
        if (values.isEmpty()) return null;
        return new FanoutShardData(value(values, 0), value(values, 1), value(values, 2), value(values, 3),
                Long.parseLong(value(values, 4)), value(values, 5), RedisJobWireCodec.valueOf(value(values, 6)),
                Integer.parseInt(value(values, 7)), Integer.parseInt(value(values, 8)),
                Long.parseLong(value(values, 9)), value(values, 10), value(values, 11), value(values, 12),
                Long.parseLong(value(values, 13)), Integer.parseInt(value(values, 14)),
                RedisJobState.valueOf(value(values, 15)), Integer.parseInt(value(values, 16).isEmpty() ? "0" : value(values, 16)),
                value(values, 24), value(values, 21), value(values, 20).isEmpty() ? new byte[0]
                : Base64.getDecoder().decode(value(values, 20)));
    }

    /**
     * 业务作用：停止接收新通知并尽力释放全部 Fanout 订阅、控制与业务执行资源。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：全部关闭步骤均已尝试后返回；重复调用等待并复用首次关闭结果。
     */
    @Override
    public void close() {
        close(RedisJobShutdownSupport.deadlineAfterMillis(properties.getMaxRunDurationMs()));
    }

    /**
     * 业务作用：在 Scheduler 共享截止内停止 Fanout 通知，并等待通知、控制与 Handler 执行器全部终止。
     *
     * <p>返回: 无返回值；全部关闭和终止步骤均完成时返回；任一步失败时在尝试其余步骤后抛出首个异常。
     *
     * @param deadlineNanos Scheduler 停机共享的绝对单调时钟截止
     */
    void close(long deadlineNanos) {
        accepting.set(false);
        if (!closed.compareAndSet(false, true)) {
            RedisJobShutdownSupport.await(closeCompleted);
            List<Throwable> failures = new ArrayList<>();
            if (closeFailure != null) failures.add(closeFailure);
            Throwable subscriptionFailure = closeSubscriptions(null);
            if (subscriptionFailure != null) failures.add(subscriptionFailure);
            Throwable shutdownFailure = shutdownExecutors(null);
            if (shutdownFailure != null) failures.add(shutdownFailure);
            Throwable terminationFailure = null;
            try {
                awaitExecutors(deadlineNanos);
            } catch (Throwable failure) {
                terminationFailure = failure;
            }
            if (terminationFailure != null) failures.add(terminationFailure);
            RedisJobShutdownSupport.rethrow(RedisJobShutdownSupport.aggregate(
                    "RedisJob Fanout dispatcher close and executor termination failed", failures));
            return;
        }
        running.set(false);
        Throwable failure = null;
        try {
            failure = closeSubscriptions(failure);
            failure = shutdownExecutors(failure);
            failure = RedisJobShutdownSupport.attempt(failure, () -> awaitExecutors(deadlineNanos));
        } finally {
            closeFailure = failure;
            closeCompleted.countDown();
        }
        RedisJobShutdownSupport.rethrow(failure);
    }

    /**
     * 业务作用：撤销全部 Fanout 通知订阅，并只在底层确认关闭后清除频道路由清单。
     *
     * @param failure 已有关闭失败，可为 null
     * @return 保留最早失败并附带本轮退订失败；全部步骤成功时返回原 failure。
     */
    private Throwable closeSubscriptions(Throwable failure) {
        failure = RedisJobShutdownSupport.attempt(failure, pubSub::close);
        if (pubSub.isClosed()) {
            failure = RedisJobShutdownSupport.attempt(failure, receiptChannels::clear);
        }
        return failure;
    }

    /**
     * 业务作用：向 Fanout 的全部执行器重复提交幂等关闭请求，避免一次资源异常永久留下通知或 Handler 线程。
     *
     * @param failure 已有关闭失败，可为 null
     * @return 保留最早失败并附带后续关闭失败；全部请求成功时返回原 failure。
     */
    private Throwable shutdownExecutors(Throwable failure) {
        failure = RedisJobShutdownSupport.attempt(failure, notificationExecutor::shutdown);
        failure = RedisJobShutdownSupport.attempt(failure, controlExecutor::shutdown);
        failure = RedisJobShutdownSupport.attempt(failure, businessExecutor::shutdown);
        // 关闭请求本身失败不代表执行器已进入终态；立即复验并重试一次，避免随后在未 shutdown 的执行器上无限等待。
        if (!notificationExecutor.isShutdown()) {
            failure = RedisJobShutdownSupport.attempt(failure, notificationExecutor::shutdown);
        }
        if (!controlExecutor.isShutdown()) {
            failure = RedisJobShutdownSupport.attempt(failure, controlExecutor::shutdown);
        }
        if (!businessExecutor.isShutdown()) {
            failure = RedisJobShutdownSupport.attempt(failure, businessExecutor::shutdown);
        }
        return failure;
    }

    /**
     * 业务作用：在同一绝对截止内等待 Fanout 通知、控制与 Handler 执行器全部实际终止。
     *
     * <p>返回: 无返回值；三个执行器均终止时返回；任一截止超时时抛出汇总失败。
     *
     * @param deadlineNanos Scheduler 资源收口使用的绝对单调时钟截止
     */
    private void awaitExecutors(long deadlineNanos) {
        List<Throwable> failures = new ArrayList<>();
        try {
            RedisJobShutdownSupport.awaitTermination(
                    notificationExecutor, deadlineNanos, "RedisJob fanout notification executor");
        } catch (Throwable failure) {
            failures.add(failure);
        }
        try {
            RedisJobShutdownSupport.awaitTermination(
                    controlExecutor, deadlineNanos, "RedisJob fanout control executor");
        } catch (Throwable failure) {
            failures.add(failure);
        }
        try {
            RedisJobShutdownSupport.awaitTermination(
                    businessExecutor, deadlineNanos, "RedisJob fanout handler executor");
        } catch (Throwable failure) {
            failures.add(failure);
        }
        RedisJobShutdownSupport.rethrow(RedisJobShutdownSupport.aggregate(
                "RedisJob Fanout dispatcher executors did not terminate", failures));
    }

    /**
     * 业务作用：确认 Fanout 的全部回调执行器已经停止，作为 Registry 注销前的本地权威证据。
     *
     * <p>参数说明: 无。
     *
     * @return 通知、控制与 Handler 执行器均已终止时为 true。
     */
    boolean resourcesClosed() {
        return pubSub.isClosed()
                && notificationExecutor.isTerminated()
                && controlExecutor.isTerminated()
                && businessExecutor.isTerminated();
    }

    /**
     * 业务作用：沿异常因果链识别稳定 Redis 错误码。
     *
     * @param error  异常
     * @param marker 错误码
     * @return 任一层包含标识时返回 true。
     */
    private static boolean contains(Throwable error, String marker) {
        while (error != null) {
            if (error.getMessage() != null && error.getMessage().contains(marker)) return true;
            error = error.getCause();
        }
        return false;
    }

    /**
     * 业务作用：安全读取脚本复合返回字段。
     *
     * @param values 返回列表
     * @param index  下标
     * @return 字符串值。
     */
    private static String value(List<Object> values, int index) {
        return index >= values.size() || values.get(index) == null ? "" : Objects.toString(values.get(index));
    }

    /**
     * 业务作用：保存 Fanout shard 的持久字段和参数字节。
     */
    private record FanoutShardData(String fanoutId, String rootRunId, String snapshotId, String workerName,
                                   long contractRevision, String schemaId, RedisJobWireCodec codec,
                                   int shardIndex, int shardTotal, long seq, String executionKey,
                                   String targetNodeIdentity, String targetStartupId, long assignmentEpoch,
                                   int assignmentCount, RedisJobState state, int attempt,
                                   String originExecutorId, String inboxMessageId, byte[] payload) {
        /**
         * 业务作用：隔离参数数组，避免执行线程修改监视器读取到的 shard 数据。
         *
         * @param fanoutId           Fanout 标识
         * @param rootRunId          根 Run 标识
         * @param snapshotId         快照标识
         * @param workerName         Worker 名
         * @param contractRevision   契约修订号
         * @param schemaId           Schema 标识
         * @param codec              线编码
         * @param shardIndex         分片下标
         * @param shardTotal         分片总数
         * @param seq                稳定序号
         * @param executionKey       业务幂等键
         * @param targetNodeIdentity 目标节点
         * @param targetStartupId    观测启动标识
         * @param assignmentEpoch    assignment 代次
         * @param assignmentCount    assignment 次数
         * @param state              shard 状态
         * @param attempt            执行次数
         * @param originExecutorId   发起执行器标识
         * @param inboxMessageId     inbox 消息标识
         * @param payload            参数字节
         */
        private FanoutShardData {
            payload = payload.clone();
        }
    }

    /**
     * 业务作用：把持久 shard 数据投影为 Worker 可读的 Fanout 上下文。
     *
     * @param shard shard 数据
     */
    private record FanoutContext(FanoutShardData shard) implements RedisJobFanoutContext {
        /**
         * 业务作用：读取 Fanout 标识。 @return Fanout 标识。
         */
        @Override
        public String fanoutId() {
            return shard.fanoutId();
        }

        /**
         * 业务作用：读取根 Run 标识。 @return 根 Run 标识。
         */
        @Override
        public String rootRunId() {
            return shard.rootRunId();
        }

        /**
         * 业务作用：读取快照标识。 @return 快照标识。
         */
        @Override
        public String snapshotId() {
            return shard.snapshotId();
        }

        /**
         * 业务作用：读取分片下标。 @return 分片下标。
         */
        @Override
        public int shardIndex() {
            return shard.shardIndex();
        }

        /**
         * 业务作用：读取分片总数。 @return 分片总数。
         */
        @Override
        public int shardTotal() {
            return shard.shardTotal();
        }

        /**
         * 业务作用：读取稳定序号。 @return seq。
         */
        @Override
        public long seq() {
            return shard.seq();
        }

        /**
         * 业务作用：读取业务幂等键。 @return executionKey。
         */
        @Override
        public String executionKey() {
            return shard.executionKey();
        }

        /**
         * 业务作用：读取目标节点。 @return 稳定节点身份。
         */
        @Override
        public String targetNodeIdentity() {
            return shard.targetNodeIdentity();
        }

        /**
         * 业务作用：读取 assignment 代次。 @return assignmentEpoch。
         */
        @Override
        public long assignmentEpoch() {
            return shard.assignmentEpoch();
        }
    }
}
