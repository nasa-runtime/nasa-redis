package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.models.stream.ClaimedMessages;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 业务作用：消费本节点声明能力的 Dispatch Stream，并把原子 start 后的 Run 交给隔离业务线程池。
 */
@Slf4j
final class RedisJobDispatcher implements AutoCloseable {

    private final RedisProxy redisProxy;
    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobRepository repository;
    private final RedisJobJsonCodec jsonCodec;
    private final RedisJobFanoutService fanoutService;
    private final RedisJobExecutorRegistry registry;
    private final String executorId;
    private final RedisJobLeaseRenewer leaseRenewer;
    private final RedisJobMetrics metrics;
    private final Semaphore capacity;
    private final ExecutorService businessExecutor;
    private final ExecutorService streamExecutor;
    private final ScheduledExecutorService controlExecutor;
    private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private final Map<String, RedisJobDefinition> definitions = new ConcurrentHashMap<>();
    private final Map<String, RedisJobHandler> handlers = new ConcurrentHashMap<>();
    private final Map<String, StreamBinding> streams = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> handlerCapacities = new ConcurrentHashMap<>();
    private final Map<String, Boolean> executions = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    /**
     * 业务作用：建立 Job 专用字符串 Stream 容器和隔离的控制、业务执行资源。
     *
     * @param redisProxy    Redis 命令代理
     * @param properties    Job 配置
     * @param keys          键路由器
     * @param repository    普通任务仓储
     * @param jsonCodec     Job 专用 JSON Codec
     * @param fanoutService Fanout 服务
     * @param registry      执行器注册表，提供当前执行器身份并登记在执行数
     * @param leaseRenewer  批量租约续期器
     * @param metrics       基础指标容器
     */
    RedisJobDispatcher(RedisProxy redisProxy, RedisJobProperties properties, RedisJobKeyspace keys,
                       RedisJobRepository repository, RedisJobJsonCodec jsonCodec,
                       RedisJobFanoutService fanoutService, RedisJobExecutorRegistry registry,
                       RedisJobLeaseRenewer leaseRenewer, RedisJobMetrics metrics) {
        this.properties = properties;
        this.redisProxy = redisProxy;
        this.keys = keys;
        this.repository = repository;
        this.jsonCodec = jsonCodec;
        this.fanoutService = fanoutService;
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.executorId = registry.executorId();
        this.leaseRenewer = Objects.requireNonNull(leaseRenewer, "leaseRenewer must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.capacity = new Semaphore(properties.getExecutorCapacity());
        this.businessExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("redis-job-handler-", 0).factory());
        this.streamExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("redis-job-stream-", 0).factory());
        this.controlExecutor = Executors.newScheduledThreadPool(2,
                Thread.ofPlatform().daemon().name("redis-job-control-", 0).factory());
        var options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofMillis(Math.min(properties.getMaxScanIntervalMs(), 1_000L)))
                .batchSize(Math.min(properties.getScanBatchSize(), properties.getExecutorCapacity()))
                .executor(streamExecutor)
                .serializer(RedisSerializer.string())
                .errorHandler(this::handleStreamError)
                .build();
        this.container = StreamMessageListenerContainer.create(
                Objects.requireNonNull(redisProxy.getRedisTemplate().getConnectionFactory(),
                        "Redis connection factory must not be null"), options);
    }

    /**
     * 业务作用：登记本地 Handler 与其派发 Stream；同一共享 Worker Stream 只建立一个消费循环。
     *
     * @param definition 任务定义
     * @param handler    Handler
     *                   返回：无返回值。
     */
    void register(RedisJobDefinition definition, RedisJobHandler handler) {
        definitions.put(definition.name(), definition);
        handlers.put(definition.name(), handler);
        handlerCapacities.computeIfAbsent(definition.workerName(),
                ignored -> new Semaphore(properties.getHandlerCapacity()));
        if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) return;
        int shard = keys.scheduleShard(definition.name());
        String stream = keys.dispatchByWorkerKey(shard, definition.workerKey());
        StreamBinding binding = streams.computeIfAbsent(stream,
                ignored -> new StreamBinding(stream, properties.getDispatchGroup()));
        createGroup(binding);
        if (running.get()) resume(binding);
    }

    /**
     * 业务作用：撤销已删除任务的本地定义与 Handler，使新消息不再进入业务执行。
     *
     * <p>Stream 绑定保留：派发 Stream 按 workerKey 共享，其余任务可能仍在使用；已删除任务的
     * 迟到消息在 handler 缺失时延后回 visible，最终由 start/promote 的删除 fence 终态化。
     *
     * @param definition 任务定义
     *                   返回：无返回值。
     */
    void unregister(RedisJobDefinition definition) {
        definitions.remove(definition.name(), definition);
        handlers.remove(definition.name());
    }

    /**
     * 业务作用：启动已登记 Stream 的消费循环，只有框架生命周期开放后才领取新 Run。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void start() {
        if (!running.compareAndSet(false, true)) return;
        container.start();
        streams.values().forEach(this::resume);
        long claimInterval = Math.max(properties.getMinScanIntervalMs(),
                Math.min(properties.getMaxScanIntervalMs(), properties.getXautoclaimMinIdleMs() / 3L));
        controlExecutor.scheduleWithFixedDelay(this::safeClaimPending,
                claimInterval, claimInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：接管其它进程在 start 前遗留的 PEL 消息，使持久派发不依赖原消费者恢复。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void claimPending() {
        if (!running.get() || !accepting.get() || capacity.availablePermits() == 0) return;
        for (StreamBinding binding : streams.values()) {
            if (capacity.availablePermits() == 0) break;
            claim(binding);
            cleanupConsumers(binding);
        }
    }

    /**
     * 业务作用：隔离单轮 PEL 接管异常，保留监听容器和后续接管周期。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void safeClaimPending() {
        try {
            claimPending();
        } catch (RuntimeException error) {
            log.warn("RedisJob pending claim failed: executorId={}", executorId, error);
        }
    }

    /**
     * 业务作用：区分运行期读取失败与停机关闭连接产生的预期信号，避免正常关闭被报告为调度故障。
     *
     * @param error Stream 消费异常
     *              返回：无返回值。
     */
    private void handleStreamError(Throwable error) {
        if (running.get()) log.error("RedisJob dispatch stream failed", error);
    }

    /**
     * 业务作用：使用 Redis 原生 XAUTOCLAIM 取回超过安全空闲期的消息，并复用正常 start 路径。
     *
     * @param binding Stream 绑定
     *                返回：无返回值。
     */
    @SuppressWarnings("unchecked")
    private void claim(StreamBinding binding) {
        var factory = Objects.requireNonNull(redisProxy.getRedisTemplate().getConnectionFactory(),
                "Redis connection factory must not be null");
        try (RedisConnection connection = factory.getConnection()) {
            Object nativeConnection = connection.getNativeConnection();
            if (!(nativeConnection instanceof RedisClusterAsyncCommands<?, ?> nativeCommands)) {
                throw new IllegalStateException("RedisJob XAUTOCLAIM requires Lettuce commands");
            }
            RedisClusterAsyncCommands<byte[], byte[]> commands =
                    (RedisClusterAsyncCommands<byte[], byte[]>) nativeCommands;
            XAutoClaimArgs<byte[]> args = new XAutoClaimArgs<byte[]>()
                    .consumer(io.lettuce.core.Consumer.from(bytes(binding.group), bytes(executorId)))
                    .minIdleTime(properties.getXautoclaimMinIdleMs())
                    .startId(binding.claimCursor)
                    .count(Math.min(properties.getScanBatchSize(), Math.max(1, capacity.availablePermits())));
            ClaimedMessages<byte[], byte[]> claimed = commands.xautoclaim(bytes(binding.stream), args).get();
            binding.claimCursor = claimed.getId();
            for (io.lettuce.core.StreamMessage<byte[], byte[]> message : claimed.getMessages()) {
                Map<String, String> body = new LinkedHashMap<>();
                message.getBody().forEach((key, value) -> body.put(text(key), text(value)));
                MapRecord<String, String, String> record = MapRecord.create(binding.stream, body)
                        .withId(RecordId.of(message.getId()));
                onMessage(binding, record);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("RedisJob pending claim interrupted", error);
        } catch (ExecutionException error) {
            if (contains(error, "NOGROUP")) {
                createGroup(binding);
                binding.claimCursor = "0-0";
                return;
            }
            throw new IllegalStateException("RedisJob pending claim rejected", error.getCause());
        }
    }

    /**
     * 业务作用：删除长期空闲且没有 PEL 的旧消费者，限制进程重启产生的消费组元数据增长。
     *
     * @param binding Stream 绑定
     *                返回：无返回值。
     */
    private void cleanupConsumers(StreamBinding binding) {
        long now = System.nanoTime();
        if (now - binding.lastConsumerGcAtNanos < TimeUnit.MILLISECONDS.toNanos(properties.getRegistryGcGraceMs())) {
            return;
        }
        binding.lastConsumerGcAtNanos = now;
        try {
            redisProxy.getRedisTemplate().opsForStream().consumers(binding.stream, binding.group).forEach(consumer -> {
                if (!executorId.equals(consumer.consumerName()) && consumer.pendingCount() == 0
                        && consumer.idleTimeMs() >= properties.getRegistryGcGraceMs()) {
                    redisProxy.getRedisTemplate().opsForStream().deleteConsumer(binding.stream,
                            Consumer.from(binding.group, consumer.consumerName()));
                }
            });
        } catch (RuntimeException error) {
            if (!contains(error, "no such key")) {
                log.warn("RedisJob consumer cleanup failed: stream={}", binding.stream, error);
            }
        }
    }

    /**
     * 业务作用：把纯文本协议字段编码为 Redis 字节。 @param value 文本 @return UTF-8 字节。
     */
    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：把 Redis 线字段还原为纯文本。 @param value 字节 @return UTF-8 文本。
     */
    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：确保消费组在订阅前存在；已有组视为幂等成功。
     *
     * @param binding Stream 绑定
     *                返回：无返回值。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void createGroup(StreamBinding binding) {
        try {
            redisProxy.getRedisTemplate().opsForStream()
                    .createGroup(binding.stream, ReadOffset.from("0-0"), binding.group);
        } catch (RuntimeException e) {
            if (!contains(e, "BUSYGROUP")) throw e;
        }
    }

    /**
     * 业务作用：为有本地容量的 Worker 恢复 Stream 读取；重复恢复不会增加订阅者。
     *
     * @param binding Stream 绑定
     *                返回：无返回值。
     */
    private void resume(StreamBinding binding) {
        if (!running.get() || !accepting.get() || binding.subscription != null || capacity.availablePermits() == 0)
            return;
        synchronized (binding) {
            if (binding.subscription != null || !running.get() || !accepting.get()
                    || capacity.availablePermits() == 0) return;
            binding.subscription = container.receive(
                    Consumer.from(binding.group, executorId),
                    StreamOffset.create(binding.stream, ReadOffset.lastConsumed()),
                    message -> onMessage(binding, message));
        }
    }

    /**
     * 业务作用：本地容量耗尽时停止该 Stream 的新拉取，已到达回调的消息走 defer 竞态路径。
     *
     * @param binding Stream 绑定
     *                返回：无返回值。
     */
    private void pause(StreamBinding binding) {
        synchronized (binding) {
            if (binding.subscription == null) return;
            container.remove(binding.subscription);
            binding.subscription = null;
        }
    }

    /**
     * 业务作用：处理一条已解码为纯字符串的 Dispatch 消息并先取得 Redis 执行权。
     *
     * @param binding Stream 绑定
     * @param message Dispatch 消息
     *                返回：无返回值。
     */
    private void onMessage(StreamBinding binding, MapRecord<String, String, String> message) {
        String jobName = message.getValue().get("jobName");
        String runId = message.getValue().get("runId");
        // 信封自带来源声明, 消费者不读 Run 记录即可发现串源消息。这里只告警与计数:
        // 权威隔离在 start_run.lua 内完成——定义在本地时该 Run 会被终态隔离, 否则按无兼容执行器长退避。
        String envelopeSource = message.getValue().get("schedulerQualifier");
        if (envelopeSource != null && !envelopeSource.isEmpty() && !envelopeSource.equals(keys.qualifier())) {
            log.warn("RedisJob dispatch envelope declares a different source: jobName={}, runId={}, source={}, local={}",
                    jobName, runId, envelopeSource, keys.qualifier());
            metrics.incrementClassified("redis_job_dispatch_envelope", "SOURCE_MISMATCH", "");
        }
        if (!accepting.get()) {
            repository.defer(jobName, runId, message.getId().getValue(), binding.group, binding.stream,
                    properties.getMaxScanIntervalMs());
            return;
        }
        RedisJobDefinition definition = definitions.get(jobName);
        RedisJobHandler handler = handlers.get(jobName);
        if (definition == null || handler == null) {
            log.warn("RedisJob local handler is unavailable: jobName={}, runId={}", jobName, runId);
            repository.defer(jobName, runId, message.getId().getValue(), binding.group, binding.stream,
                    properties.getMaxScanIntervalMs());
            return;
        }
        String workerName = message.getValue().get("workerName");
        if (!definition.workerName().equals(workerName)) {
            log.warn("RedisJob worker route is incompatible: jobName={}, workerName={}", jobName, workerName);
            repository.defer(jobName, runId, message.getId().getValue(), binding.group, binding.stream,
                    properties.getMaxScanIntervalMs());
            return;
        }
        String messageId = message.getId().getValue();
        if (!capacity.tryAcquire()) {
            repository.defer(definition, runId, messageId, binding.group);
            pause(binding);
            controlExecutor.schedule(() -> resume(binding), properties.getMinScanIntervalMs(), TimeUnit.MILLISECONDS);
            return;
        }
        Semaphore handlerCapacity = handlerCapacities.get(definition.workerName());
        if (handlerCapacity == null || !handlerCapacity.tryAcquire()) {
            capacity.release();
            repository.defer(definition, runId, messageId, binding.group);
            return;
        }
        metrics.gauge("redis_job_running", properties.getExecutorCapacity() - capacity.availablePermits());
        RedisJobRepository.StartResult start;
        try {
            start = repository.start(definition, runId, executorId, messageId, binding.group);
        } catch (RuntimeException e) {
            handlerCapacity.release();
            capacity.release();
            throw e;
        }
        if (!"STARTED".equals(start.code()) && !"ADOPTED".equals(start.code())) {
            metrics.incrementClassified("redis_job_start", start.code(), "");
            handlerCapacity.release();
            capacity.release();
            metrics.gauge("redis_job_running", properties.getExecutorCapacity() - capacity.availablePermits());
            resumePausedStreams();
            return;
        }
        metrics.increment("redis_job_started_total");
        String executionId = runId + ':' + start.attemptToken();
        if (executions.putIfAbsent(executionId, Boolean.TRUE) != null) {
            handlerCapacity.release();
            capacity.release();
            return;
        }
        registry.executionStarted();
        try {
            businessExecutor.submit(() -> execute(binding, definition, handler, runId, start, executionId,
                    handlerCapacity));
        } catch (RejectedExecutionException error) {
            registry.executionFinished();
            executions.remove(executionId);
            handlerCapacity.release();
            capacity.release();
        }
    }

    /**
     * 业务作用：运行一个已取得权威的 Handler attempt，持续续期并在结束时提交唯一状态出口。
     *
     * @param binding         来源 Stream
     * @param definition      任务定义
     * @param handler         Handler
     * @param runId           Run 标识
     * @param start           start 权威字段
     * @param executionId     本地去重标识
     * @param handlerCapacity 当前 Worker 的并发配额
     *                        返回：无返回值。
     */
    private void execute(StreamBinding binding, RedisJobDefinition definition, RedisJobHandler handler,
                         String runId, RedisJobRepository.StartResult start, String executionId,
                         Semaphore handlerCapacity) {
        long executionStartedAt = System.nanoTime();
        AtomicBoolean timedOut = new AtomicBoolean();
        RedisJobLeaseRenewer.LeaseHandle leaseHandle = null;
        ScheduledFuture<?> timeoutTask = null;
        try {
            RedisJobRepository.RunData data = repository.read(definition.name(), runId)
                    .orElseThrow(() -> new IllegalStateException("started RedisJob run disappeared: " + runId));
            long allowance = properties.getRenewRttAllowanceMs() + properties.getClockDriftAllowanceMs();
            DefaultRedisJobContext context = new DefaultRedisJobContext(keys.qualifier(),
                    keys.namespace(), data,
                    start.attempt(), start.attemptToken(), properties.getLeaseMs(), allowance,
                    jsonCodec, fanoutService, null);
            leaseHandle = leaseRenewer.registerNormal(definition, runId, context);
            timeoutTask = controlExecutor.schedule(() -> {
                timedOut.set(true);
                context.requestCancellation();
            }, Math.min(definition.timeoutMs(), properties.getMaxRunDurationMs()), TimeUnit.MILLISECONDS);

            RedisJobResult result;
            try {
                result = handler.handle(context);
                if (result == null) result = RedisJobResult.success();
            } catch (RedisJobExecutionStoppedException e) {
                result = RedisJobResult.retry(e.getMessage());
            } catch (Throwable error) {
                result = RedisJobResult.retry(error.getClass().getName() + ": " + error.getMessage());
            }
            if (!context.fanoutPrepared()) {
                RedisJobRepository.FinishResult finish = repository.finish(definition, runId, executorId, start.attemptToken(),
                        timedOut.get() ? RedisJobResult.timeout("handler exceeded execution timeout") : result);
                metrics.incrementClassified("redis_job", finish.state(), "");
                if ("STALE_OWNER".equals(finish.code())) metrics.increment("redis_job_stale_finish_total");
            }
        } finally {
            if (leaseHandle != null) leaseHandle.close();
            if (timeoutTask != null) timeoutTask.cancel(false);
            executions.remove(executionId);
            registry.executionFinished();
            handlerCapacity.release();
            capacity.release();
            metrics.gauge("redis_job_running", properties.getExecutorCapacity() - capacity.availablePermits());
            metrics.gauge("redis_job_handler_duration_ms",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - executionStartedAt));
            resume(binding);
            resumePausedStreams();
        }
    }

    /**
     * 业务作用：业务任务释放容量后恢复此前因容量门禁暂停的 Stream。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void resumePausedStreams() {
        if (accepting.get() && capacity.availablePermits() > 0) streams.values().forEach(this::resume);
    }

    /**
     * 业务作用：停止领取新普通任务，已经开始的 attempt 继续执行和续期。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void drain() {
        accepting.set(false);
        streams.values().forEach(this::pause);
    }

    /**
     * 业务作用：重新开放普通任务领取并恢复已登记 Stream 的消费循环。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    void activate() {
        accepting.set(true);
        if (running.get()) streams.values().forEach(this::resume);
    }

    /**
     * 业务作用：沿异常因果链识别 Redis 稳定错误码。
     *
     * @param error  异常
     * @param marker 错误码片段
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
     * 业务作用：停止新消息领取并关闭控制与业务执行资源。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    @Override
    public void close() {
        accepting.set(false);
        running.set(false);
        streams.values().forEach(this::pause);
        container.stop();
        controlExecutor.shutdown();
        streamExecutor.shutdown();
        businessExecutor.shutdown();
        try {
            if (!streamExecutor.awaitTermination(
                    Math.max(1_000L, properties.getMaxScanIntervalMs() * 2L), TimeUnit.MILLISECONDS)) {
                log.warn("RedisJob stream executor did not stop before shutdown deadline");
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 业务作用：保存一个 Worker Stream 的共享消费组和当前订阅句柄。
     */
    private static final class StreamBinding {
        private final String stream;
        private final String group;
        private volatile Subscription subscription;
        private volatile String claimCursor = "0-0";
        private volatile long lastConsumerGcAtNanos;

        /**
         * 业务作用：建立 Stream 与消费组的稳定绑定。
         *
         * @param stream Stream 键
         * @param group  消费组
         */
        private StreamBinding(String stream, String group) {
            this.stream = stream;
            this.group = group;
        }
    }
}
