package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 业务作用：作为 RedisJob 门面统一管理定义、触发、查询、调度扫描、租约恢复与 Fanout 生命周期。
 *
 * <p>调度器不选举应用级主节点；多个实例可以扫描同一固定分片，Redis TIME、同 slot Lua、持久 Run、
 * Dispatch Stream、租约与 fencing token 共同裁决唯一当前执行权。普通任务是至少一次语义，外部副作用
 * 仍需使用 runId、executionKey 或目标系统 fencing 幂等。
 *
 * <p>同名任务默认使用 {@link RedisJobConcurrency#SERIAL_QUEUE} 在集群范围串行；fixed rate 可以形成
 * 有界积压但不会并行提交 Handler，fixed delay 则在当前 Run 终态后计算下一时刻。Fanout 按 Worker
 * 能力合同冻结节点快照；Java Worker 由 Pub/Sub 唤醒后读取权威 shard，receipt、ready、lease 与 root
 * 索引负责重新发布和恢复，inbox 保存可原子撤销的稳定投递身份。容量背压不会被当作节点失联。
 */
@Slf4j
public final class RedisJobScheduler implements SmartLifecycle, AutoCloseable {

    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final RedisJobRepository repository;
    private final RedisJobJsonCodec jsonCodec;
    private final RedisJobExecutorRegistry registry;
    private final RedisJobFanoutCoordinator fanoutCoordinator;
    private final RedisJobFanoutMonitor fanoutMonitor;
    private final RedisJobLeaseRenewer leaseRenewer;
    private final RedisJobPreStartBarrier preStartBarrier;
    private final RedisJobDispatcher dispatcher;
    private final RedisJobFanoutDispatcher fanoutDispatcher;
    private final RedisJobAdmissionController admission;
    private final ContinuationStarter finalCleanupStarter;
    private final ScheduledExecutorService monitors;
    private final AtomicLongArray fanoutIndexEpochs;
    private final RedisJobMetrics metrics = new RedisJobMetrics();
    private final Map<String, RedisJobDefinition> definitions = new ConcurrentHashMap<>();
    private final Map<String, RedisJobHandler> handlers = new ConcurrentHashMap<>();
    private volatile int[] localShardSnapshot = new int[0];
    private volatile long visibleScanUpperBoundMs;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicBoolean awaitingInitialRegistration = new AtomicBoolean();
    private final AtomicBoolean layoutConfirmed = new AtomicBoolean();
    private final CountDownLatch stopPrepared = new CountDownLatch(1);
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final CountDownLatch stopCompleted = new CountDownLatch(1);
    private final AtomicBoolean finalCleanupStarted = new AtomicBoolean();
    private final CountDownLatch finalCleanupCompleted = new CountDownLatch(1);
    private final AtomicLong lastHeartbeatAt = new AtomicLong();
    private final AtomicLong lastScheduleScanAt = new AtomicLong();
    private volatile Throwable stopPreparationFailure;
    private volatile Throwable stopFailure;
    private volatile Throwable finalCleanupFailure;

    /**
     * 业务作用：按指定 RedisProxy 建立完整调度运行时，但在 Spring 生命周期 start 前不领取任务。
     *
     * <p>返回：创建完成依赖装配但尚未领取任务的调度运行时。
     *
     * @param redisProxy Redis 命令代理
     * @param properties Job 配置
     */
    public RedisJobScheduler(RedisProxy redisProxy, RedisJobProperties properties) {
        this(redisProxy, properties, RedisJobScheduler::startVirtualContinuation);
    }

    /**
     * 业务作用：建立可替换 continuation 调度策略的完整运行时，使线程资源不可用时仍能由停机调用方接管所有权。
     *
     * <p>返回：创建绑定指定 continuation 策略、尚未开放准入的调度运行时。
     *
     * @param redisProxy         Redis 命令代理
     * @param properties         Job 配置
     * @param finalCleanupStarter final-cleanup continuation 启动策略
     */
    RedisJobScheduler(RedisProxy redisProxy, RedisJobProperties properties,
                      ContinuationStarter finalCleanupStarter) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null").validate();
        this.finalCleanupStarter = Objects.requireNonNull(
                finalCleanupStarter, "finalCleanupStarter must not be null");
        this.visibleScanUpperBoundMs = this.properties.getMaxScanIntervalMs();
        // source id 必须来自调用方实际选中的 RedisProxy，而不是另一份可漂移的配置字段；这样键前缀、
        // 物理连接和上下文声明天然绑定同一来源，也不存在遗漏配置后回退 primary 的路径。
        RedisProxy supplied = Objects.requireNonNull(redisProxy, "redisProxy must not be null");
        String sourceId = RedisJobSchedulers.sourceId(supplied.getQualifier());
        RedisProxy selected = requireRoutedProxy(sourceId, supplied);
        this.keys = new RedisJobKeyspace(sourceId, properties.getNamespace(), properties.getShardCount(),
                properties.getFanoutBucketCount());
        this.scripts = new RedisJobScriptExecutor(selected);
        this.repository = new RedisJobRepository(properties, keys, scripts);
        this.jsonCodec = new RedisJobJsonCodec();
        this.registry = new RedisJobExecutorRegistry(properties, keys, scripts);
        this.fanoutMonitor = new RedisJobFanoutMonitor(properties, keys, scripts, registry, metrics);
        this.fanoutCoordinator = new RedisJobFanoutCoordinator(
                properties, keys, scripts, registry, jsonCodec, fanoutMonitor::requestCancel);
        this.leaseRenewer = new RedisJobLeaseRenewer(properties, keys, scripts, registry.executorId());
        this.preStartBarrier = new RedisJobPreStartBarrier();
        this.dispatcher = new RedisJobDispatcher(selected, properties, keys, repository, jsonCodec,
                fanoutCoordinator, registry, leaseRenewer, metrics, preStartBarrier);
        this.fanoutDispatcher = new RedisJobFanoutDispatcher(selected, properties, keys, scripts, registry,
                jsonCodec, fanoutMonitor::onReceiptSignal, leaseRenewer, metrics, preStartBarrier);
        this.admission = new RedisJobAdmissionController(dispatcher, fanoutDispatcher, registry, draining);
        this.monitors = RedisJobShutdownSupport.scheduledExecutor(4,
                Thread.ofPlatform().daemon().name("redis-job-monitor-", 0).factory());
        this.fanoutIndexEpochs = new AtomicLongArray(keys.fanoutBucketCount());
        this.repository.setVisibleWakeup(this::wakeVisible);
        this.fanoutCoordinator.setVisibleWakeup(this::wakeVisible);
        this.fanoutCoordinator.setFanoutIndexWakeup(this::wakeFanoutIndex);
    }

    /**
     * 业务作用：使用虚拟线程承载迟到权威工作的最终资源收口，不占用平台线程池容量。
     *
     * <p>返回: 无返回值；线程无法建立或启动时抛出原异常，由当前停机调用方接管。
     *
     * @param threadName 线程名
     * @param task       final-cleanup 任务
     */
    private static void startVirtualContinuation(String threadName, Runnable task) {
        Thread.ofVirtual().name(threadName).start(task);
    }

    /**
     * 业务作用：登记任务定义与 Handler；相同修订号的本地冲突在写 Redis 前立即拒绝。
     *
     * <p>登记与 start、drain、activate 共用 Scheduler 生命周期锁，保证空定义判断、首次开门资格和
     * 显式关门按调用完成顺序线性化。
     *
     * <p>返回：无返回值；Scheduler 已关闭、定义冲突或 Redis 拒绝登记时抛出异常；能力登记结局不明时
     * 当前 source 永久关门并进入统一注销收口，调用方需重建运行时。
     *
     * @param definition 任务定义
     * @param handler    Handler
     */
    public synchronized void register(RedisJobDefinition definition, RedisJobHandler handler) {
        requireOpen();
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        // 布局门禁必须早于任何定义写入: 分片数或桶数不一致的节点若先写了定义,
        // 同一任务就形成两份定义与两套调度时刻, 之后只能人工对账
        confirmLayout();
        requireOpen();
        RedisJobDefinition current = definitions.get(definition.name());
        if (current != null && current.definitionRevision() == definition.definitionRevision()
                && !current.definitionDigest().equals(definition.definitionDigest())) {
            throw new IllegalStateException("conflicting local RedisJob definition: " + definition.name());
        }
        if (current != null && current.definitionRevision() > definition.definitionRevision()) return;
        long nextFireAt = initialNextFireAt(definition);
        requireOpen();
        String code = repository.register(definition, nextFireAt);
        // Redis 往返期间停机可独立发布；返回后必须先复验终态，不能再接线 Handler 或恢复本地准入。
        requireOpen();
        if (!"OK".equals(code) && !"ADOPTED".equals(code)) {
            throw new IllegalStateException("RedisJob definition rejected: " + code);
        }
        definitions.put(definition.name(), definition);
        refreshLocalShards();
        handlers.put(definition.name(), handler);
        fanoutCoordinator.register(definition);
        fanoutMonitor.register(definition);
        dispatcher.register(definition, handler);
        if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) fanoutDispatcher.register(definition, handler);
        if (running.get()) {
            boolean openAfterRegister = awaitingInitialRegistration.get();
            try {
                requireOpen();
                registry.register(definition);
                requireOpen();
                if (openAfterRegister) {
                    // 首次动态定义只有在能力登记与本地开门全部完成后才消费等待资格；异常时保留资格，
                    // 后续登记仍可重试，而 drain() 与本段共用生命周期锁，不会在返回后被旧资格重新开门。
                    admission.open();
                    requireOpen();
                    awaitingInitialRegistration.set(false);
                }
            } catch (RuntimeException | Error failure) {
                failClosedAdmission(failure);
                terminateAfterAuthorityFailure(failure);
                throw failure;
            }
        }
    }

    /**
     * 业务作用：编码 JSON 参数后以 requestId 幂等触发一个已登记普通任务。
     *
     * @param jobName   任务名
     * @param requestId 请求幂等标识
     * @param parameter 业务参数
     * @return 稳定 Run 标识。
     */
    public String triggerJson(String jobName, String requestId, Object parameter) {
        RedisJobDefinition definition = requireDefinition(jobName);
        return trigger(jobName, requestId, jsonCodec.encode(definition.schemaId(), parameter));
    }

    /**
     * 业务作用：以已编码参数和 requestId 幂等触发一个已登记普通任务。
     *
     * @param jobName   任务名
     * @param requestId 请求幂等标识
     * @param payload   已编码参数
     * @return 稳定 Run 标识。
     */
    public String trigger(String jobName, String requestId, RedisJobPayload payload) {
        return repository.manualFire(requireDefinition(jobName), requestId, payload);
    }

    /**
     * 业务作用：暂停任务的新触发与尚未 start 的积压，不撤销已经取得的 attempt 执行权。
     *
     * @param jobName 任务名
     *                返回：无返回值。
     */
    public void pause(String jobName) {
        String code = repository.pause(requireDefinition(jobName).name());
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob pause rejected: " + code);
    }

    /**
     * 业务作用：从当前 Redis 时刻之后重新计算任务下一逻辑时刻并恢复自动触发。
     *
     * @param jobName 任务名
     *                返回：无返回值。
     */
    public void resume(String jobName) {
        RedisJobDefinition definition = requireDefinition(jobName);
        String code = repository.resume(jobName, initialNextFireAt(definition));
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob resume rejected: " + code);
    }

    /**
     * 业务作用：以更高或相同的显式修订号删除定义，并保留足以覆盖滚动部署的 tombstone。
     *
     * @param jobName  任务名
     * @param revision 删除修订号
     *                 返回：无返回值；修订号落后或任务不存在时抛出异常；能力撤销未确认时永久关闭当前 source。
     */
    public synchronized void delete(String jobName, long revision) {
        RedisJobDefinition definition = requireDefinition(jobName);
        if (revision < definition.definitionRevision()) {
            throw new IllegalArgumentException("delete revision must not be smaller than current definition revision");
        }
        String code = repository.delete(jobName, revision);
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob delete rejected: " + code);
        definitions.remove(jobName, definition);
        refreshLocalShards();
        handlers.remove(jobName);
        fanoutMonitor.unregister(definition);
        // 撤销派发与 Fanout 侧的全部登记：只删门面层映射时，注册表心跳会继续续期旧能力，
        // 删除之后创建的新根仍会把该 Worker 冻结进快照并由本地 Handler 真实执行
        dispatcher.unregister(definition);
        // Fanout 能力所有权只属于 FANOUT_ONLY 定义：普通定义复用 workerName 不提供 Fanout Handler，
        // 把它计入引用会让被删 Worker 的能力与 Handler 全部保留，删除之后创建的新根仍会选中并执行它
        boolean fanoutStillUsed = definitions.values().stream()
                .anyMatch(other -> other.trigger() == RedisJobTrigger.FANOUT_ONLY
                        && other.workerName().equals(definition.workerName()));
        fanoutDispatcher.unregister(definition, fanoutStillUsed);
        fanoutCoordinator.unregister(definition, fanoutStillUsed);
        if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY && !fanoutStillUsed) {
            try {
                // Handler 已从本地路由移除后必须取得 Redis 撤权确认；结局不明时整个 source 转入停机，
                // DRAINING 与全量注销共同阻止没有 Handler 的能力继续进入新快照。
                registry.removeCapability(definition.workerName());
            } catch (RuntimeException | Error failure) {
                failClosedAdmission(failure);
                terminateAfterAuthorityFailure(failure);
                throw failure;
            }
        }
    }

    /**
     * 业务作用：在注册表写入或撤权结局不明时永久封闭 source，并复用停机序列完成全量注销补偿。
     *
     * <p>返回: 无返回值；停机失败附加到原异常，原调用方仍收到首个控制面失败。
     *
     * @param cause 原始控制面失败，用于附加停机阶段证据
     */
    private void terminateAfterAuthorityFailure(Throwable cause) {
        prepareStop();
        try {
            stop();
        } catch (RuntimeException | Error shutdownFailure) {
            cause.addSuppressed(shutdownFailure);
        }
    }

    /**
     * 业务作用：监视线程发现能力重建结局不明时提交永久关门，并把可能阻塞的资源收口移交独立线程。
     *
     * <p>返回: 无返回值；线程无法建立时由当前线程接管同一停机序列。
     *
     * @param cause 能力重建失败，用于保留停机编排证据
     */
    private void terminateAfterAuthorityFailureAsync(Throwable cause) {
        prepareStop();
        Runnable cleanup = () -> {
            try {
                stop();
            } catch (RuntimeException | Error shutdownFailure) {
                cause.addSuppressed(shutdownFailure);
                log.error("RedisJob authority withdrawal cleanup failed: source={}", keys.qualifier(), shutdownFailure);
            }
        };
        try {
            Thread.ofVirtual().name("redis-job-authority-withdrawal-" + keys.qualifier()).start(cleanup);
        } catch (RuntimeException | Error startFailure) {
            cause.addSuppressed(startFailure);
            cleanup.run();
        }
    }

    /**
     * 业务作用：显式选择本地已登记摘要解除持久定义冲突，避免集群按节点启动顺序自动裁决。
     *
     * @param jobName 任务名
     *                返回：无返回值；目标摘要不是当前持久摘要时抛出异常。
     */
    public void resolveConflict(String jobName) {
        RedisJobDefinition definition = requireDefinition(jobName);
        String code = repository.resolveConflict(definition, initialNextFireAt(definition));
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob conflict resolution rejected: " + code);
    }

    /**
     * 业务作用：最终一致地关闭全部调度分片的普通 Run 新执行权门禁，不撤销已经取得的 attempt。
     *
     * @param actor 操作来源
     *              返回：无返回值；任一分片拒绝时抛出异常。
     */
    public void pauseNamespace(String actor) {
        setNamespaceState(RedisJobDefinitionState.PAUSED, actor);
    }

    /**
     * 业务作用：最终一致地重新开放全部调度分片的普通 Run 新执行权门禁。
     *
     * @param actor 操作来源
     *              返回：无返回值；任一分片拒绝时抛出异常。
     */
    public void resumeNamespace(String actor) {
        setNamespaceState(RedisJobDefinitionState.ENABLED, actor);
    }

    /**
     * 业务作用：逐分片传播命名空间状态，Lua 触发路径仍逐次复验本分片门禁。
     *
     * @param state 目标状态
     * @param actor 操作来源
     *              返回：无返回值。
     */
    private void setNamespaceState(RedisJobDefinitionState state, String actor) {
        String selectedActor = RedisJobNames.requireName(actor, "actor");
        for (int shard = 0; shard < keys.shardCount(); shard++) {
            String code = repository.setNamespaceState(shard, state, selectedActor);
            if (!"OK".equals(code)) throw new IllegalStateException("RedisJob namespace state rejected: " + code);
        }
    }

    /**
     * 业务作用：请求取消一个 Run，运行中任务通过续期响应获得协作式取消信号。
     *
     * @param jobName 任务名
     * @param runId   Run 标识
     *                返回：无返回值。
     */
    public void cancel(String jobName, String runId) {
        RedisJobDefinition definition = requireDefinition(jobName);
        String code = repository.cancel(definition, runId);
        if (!"OK".equals(code) && !"ALREADY_COMPLETED".equals(code)) {
            throw new IllegalStateException("RedisJob cancellation rejected: " + code);
        }
        repository.read(definition.name(), runId).ifPresent(data -> {
            if (!data.fanoutId().isEmpty()) fanoutMonitor.requestCancel(data.fanoutId(), "CANCEL_REQUESTED");
        });
    }

    /**
     * 业务作用：按任务名和 Run 标识查询权威持久状态。
     *
     * @param jobName 任务名
     * @param runId   Run 标识
     * @return Run 不存在时为空。
     */
    public Optional<RedisJobRun> findRun(String jobName, String runId) {
        return repository.read(jobName, runId).map(RedisJobRepository.RunData::run);
    }

    /**
     * 业务作用：在管理查询缺少任务名时逐调度分片定位 Run；该入口不用于执行热路径。
     *
     * @param runId Run 标识
     * @return Run 不存在时为空。
     */
    public Optional<RedisJobRun> findRun(String runId) {
        for (int shard = 0; shard < keys.shardCount(); shard++) {
            Optional<RedisJobRepository.RunData> data = repository.readAtShard(shard, runId);
            if (data.isPresent()) return data.map(RedisJobRepository.RunData::run);
        }
        return Optional.empty();
    }

    /**
     * 业务作用：把本节点的不可变布局写入或比对该数据源的 layout marker，不一致时拒绝参与调度。
     *
     * <p>相同 {@code (qualifier, namespace)} 的所有 Java、Go、Rust 节点必须使用相同分片数、Fanout 桶数、
     * 协议代次和派发消费组。分片数不同会让同一任务落到不同分片、形成两份定义和两套调度时刻并产生重复 Run；
     * Fanout 桶数不同会让相同 fanoutId 的 root、receipt、lease 和清理游标分散到不同 slot；
     * 消费组不同则让同一条 Stream 被两个消费组各消费一次。这些都不会自行收敛，必须在加入前挡住。
     *
     * <p>marker 位于 registry slot，与被比对的分片布局无关；首个节点原子写入，其余节点只允许完全一致。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值；布局不一致时抛出异常阻止本节点启动。
     */
    private void confirmLayout() {
        if (layoutConfirmed.get()) return;
        // pubsubMode 是同一任务集的线协议布局而非本地调优项: SHARDED 用 SSUBSCRIBE/SPUBLISH、
        // BROADCAST 用 SUBSCRIBE/PUBLISH, 两套通道互不可见。混配节点会互相收不到 Fanout 通知,
        // 根节点只能按自己的模式重发, 健康目标最终被误判失联并触发重分配。
        String fingerprint = properties.getProtocolVersion() + "|" + keys.qualifier() + "|" + keys.namespace()
                + "|" + keys.shardCount() + "|" + keys.fanoutBucketCount() + "|" + properties.getDispatchGroup()
                + "|" + properties.getPubsubMode().name();
        List<Object> result = scripts.list(RedisJobScript.JOB_LAYOUT,
                new String[]{keys.layoutMarker()}, fingerprint);
        String code = result == null || result.isEmpty() ? "" : String.valueOf(result.get(0));
        if (!"OK".equals(code)) {
            String existing = result != null && result.size() > 1 ? String.valueOf(result.get(1)) : "";
            throw new IllegalStateException("RedisJob layout mismatch for qualifier=" + keys.qualifier()
                    + " namespace=" + keys.namespace() + "; this node=" + fingerprint
                    + "; already established=" + existing
                    + "; protocol version, shard count, fanout bucket count and dispatch group are immutable"
                    + " for an established namespace");
        }
        layoutConfirmed.set(true);
    }

    /**
     * 业务作用：确认 qualifier、传入代理与进程内登记表三者指向同一个数据源，任一不符即拒绝建立调度器。
     *
     * <p>强路由必须在每一条创建入口成立，而不只是在多数据源聚合器里。编程式构造若允许用另一台代理顶替，
     * Scheduler 会对外报告一个 source id、把定义和 Run 写进另一台物理 Redis，键前缀还伪装成该 source id，
     * 观测和对账都无法发现。
     *
     * @param sourceId   已归一的 source id
     * @param redisProxy 调用方传入的命令代理，可为 null
     * @return 登记表中与 sourceId 对应的命令代理。
     */
    private static RedisProxy requireRoutedProxy(String sourceId, RedisProxy redisProxy) {
        RedisProxy registered = RedisProxy.load(sourceId);
        if (registered == null) {
            throw new IllegalStateException("unknown RedisJob qualifier: " + sourceId
                    + "; RedisJob refuses to fall back to another RedisProxy");
        }
        if (redisProxy != null && redisProxy != registered) {
            throw new IllegalStateException("RedisJob qualifier " + sourceId
                    + " is registered to a different RedisProxy than the one supplied ("
                    + redisProxy.getQualifier() + ")");
        }
        return registered;
    }

    /**
     * 业务作用：读取本 Scheduler 永久绑定的语言无关 source id，供多数据源部署区分同名任务的归属。
     *
     * <p>参数说明: 无。
     *
     * @return source id，不携带 Spring Bean 后缀。
     */
    public String qualifier() {
        // 从构造期冻结的 keyspace 读取而不是可变的配置对象: RedisJobProperties 暴露 setter,
        // 构造后被改写会让 Redis 键仍在原数据源、而对外报告的 source id 变成另一个, 观测与幂等维度全部失真。
        return keys.qualifier();
    }

    /**
     * 业务作用：读取本 Scheduler 绑定的调度命名空间，与 qualifier 共同构成任务的本地唯一身份。
     *
     * <p>参数说明: 无。
     *
     * @return 命名空间。
     */
    public String namespace() {
        return keys.namespace();
    }

    /**
     * 业务作用：暴露当前进程的基础指标容器，便于应用桥接 Micrometer 或其它观测系统。
     *
     * <p>参数说明: 无。
     *
     * @return 指标容器。
     */
    public RedisJobMetrics metrics() {
        return metrics;
    }

    /**
     * 业务作用：根据生命周期、draining 和最近控制面成功时间给出调度健康状态。
     *
     * <p>参数说明: 无。
     *
     * @return 当前健康摘要。
     */
    public RedisJobHealth health() {
        if (!running.get()) return new RedisJobHealth("DOWN", false, draining.get(),
                lastHeartbeatAt.get(), lastScheduleScanAt.get());
        long heartbeatAge = System.currentTimeMillis() - lastHeartbeatAt.get();
        String status = draining.get() ? "DRAINING"
                : heartbeatAge > properties.getExecutorExpireMs() ? "DEGRADED" : "UP";
        return new RedisJobHealth(status, true, draining.get(),
                lastHeartbeatAt.get(), lastScheduleScanAt.get());
    }

    /**
     * 业务作用：在任何登记或生命周期副作用前拒绝已永久关闭的 Scheduler。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：Scheduler 仍可用时无返回值；已关闭时抛出稳定异常。
     */
    private void requireOpen() {
        // closed 在生命周期锁内单调发布；旧引用必须在持久写和本地接线前停止。
        if (closed.get()) throw new IllegalStateException("RedisJobScheduler is closed");
    }

    /**
     * 业务作用：确认 Scheduler 正在运行，防止无心跳和注册表维护的实例开放本地准入。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：Scheduler 可接受显式激活时无返回值；已关闭或未运行时抛出稳定异常。
     */
    private void requireRunning() {
        requireOpen();
        if (!running.get()) throw new IllegalStateException("RedisJobScheduler is not running");
    }

    /**
     * 业务作用：启动 Fanout 通知门禁、能力登记、普通派发和所有持久索引监视器。
     *
     * <p>启动期间独占 Scheduler 生命周期锁，避免动态登记落在空定义判断与等待标记发布之间。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    @Override
    public synchronized void start() {
        requireOpen();
        // 没有任何定义的节点也要确认布局，但在首个 Handler 完成能力登记前不得领取任务。
        confirmLayout();
        requireOpen();
        if (!running.compareAndSet(false, true)) return;
        try {
            // 能力登记与 ACTIVE 心跳都是启动重路，完整确认前两类本地准入必须保持关闭。
            admission.prepare();
            metrics.gauge("redis_job_executor_capacity", properties.getExecutorCapacity());
            fanoutDispatcher.start();
            requireOpen();
            for (RedisJobDefinition definition : definitions.values()) {
                requireOpen();
                registry.register(definition);
                // 单次能力登记的 Redis 回包不能覆盖并发提交的永久关闭。
                requireOpen();
            }
            dispatcher.start();
            requireOpen();
            if (definitions.isEmpty()) {
                awaitingInitialRegistration.set(true);
            } else {
                admission.open();
                requireOpen();
            }
            requireOpen();
            monitors.scheduleWithFixedDelay(this::safeHeartbeat, properties.getHeartbeatMs(),
                    properties.getHeartbeatMs(), TimeUnit.MILLISECONDS);
            monitors.schedule(this::scheduleScanCycle, 0L, TimeUnit.MILLISECONDS);
            monitors.schedule(this::indexScanCycle, 0L, TimeUnit.MILLISECONDS);
            monitors.schedule(this::fanoutRootScanCycle, 0L, TimeUnit.MILLISECONDS);
            for (int bucket = 0; bucket < keys.fanoutBucketCount(); bucket++) {
                scheduleFanoutIndexScan(bucket, 0L, fanoutIndexEpochs.get(bucket));
            }
            long registryGcInterval = Math.max(properties.getHeartbeatMs(),
                    Math.min(properties.getRegistryGcGraceMs() / 4L, properties.getMaxScanIntervalMs() * 10L));
            monitors.scheduleWithFixedDelay(this::safeRegistryGc, registryGcInterval,
                    registryGcInterval, TimeUnit.MILLISECONDS);
            // 删除任务的 waitq 存量收敛：全分片扫描（删除后任务不再出现在 localShards），
            // reaping 标记为空时每分片只付出一次 SRANDMEMBER 成本
            monitors.scheduleWithFixedDelay(this::safeReapDeleted, properties.getMaxScanIntervalMs(),
                    properties.getMaxScanIntervalMs(), TimeUnit.MILLISECONDS);
        } catch (RuntimeException | Error error) {
            // 启动中途失败必须撤销已完成的步骤并把 running 恢复为 false：
            // 保留 running=true 会让健康检查报告正在运行，而能力登记、Dispatcher 或监视循环其实只起了一部分，
            // 该节点既不领任务也不会被外部发现异常。
            running.set(false);
            awaitingInitialRegistration.set(false);
            // 并发停机已经取得唯一资源 owner 时，由该 owner 等待本方法退出后继续收口；
            // 当前线程不能在持有 Scheduler monitor 时反向等待同一个完成信号。
            if (!closed.get()) {
                try {
                    stop();
                } catch (RuntimeException | Error ignored) {
                    // 回滚期间的停机失败不能顶替真正的启动失败原因
                }
            }
            throw error;
        }
    }

    /**
     * 业务作用：扫描本节点相关调度分片并通过 Lua CAS 生成唯一自动 Run。
     *
     * <p>参数说明: 无。
     *
     * @return 下一次调度扫描的有界间隔。
     */
    private long scanSchedules() {
        long nextDelay = properties.getMaxScanIntervalMs();
        for (int shard : localShards()) {
            for (int retry = 0; retry < properties.getNeedRecomputeMaxRetries(); retry++) {
                RedisJobRepository.ScheduleDueScan scan = repository.scanScheduleDue(
                        shard, properties.getScanBatchSize());
                metrics.increment("redis_job_due_scan_total");
                if (!scan.namespaceEnabled()) {
                    // 暂停期间保留 schedule 的权威逻辑时刻，不推进也不反复提交必然被拒绝的触发；
                    // 扫描器按空闲上界复查，使其它节点的恢复动作仍能在有界时间内被观察到。
                    nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties,
                            registry.executorId(), scan.redisNow(), 0L, false,
                            RedisJobScanInterval.SCHEDULE, shard, properties.getMaxScanIntervalMs()));
                    break;
                }
                List<RedisJobRepository.FireRequest> requests = scan.members().isEmpty()
                        ? List.of() : new ArrayList<>();
                for (RedisJobRepository.ScheduleDueMember member : scan.members()) {
                    RedisJobDefinition definition = definitions.get(member.jobName());
                    if (definition == null || definition.trigger() == RedisJobTrigger.FANOUT_ONLY
                            || definition.definitionRevision() != member.definitionRevision()) continue;
                    metrics.gauge("redis_job_schedule_lag_ms",
                            Math.max(0L, scan.redisNow() - member.score()));
                    // 单任务异常必须就地隔离：时刻计算抛错（如极端参数溢出）若逃逸出本循环，
                    // 会中止整轮扫描，让同批乃至全部分片的其它任务一起停止触发
                    try {
                        requests.addAll(fireRequests(definition, member.score(), scan.redisNow()));
                    } catch (RuntimeException error) {
                        log.error("RedisJob schedule advance failed: jobName={}", member.jobName(), error);
                        metrics.incrementClassified("redis_job_fire", "ADVANCE_FAILED", "OK");
                    }
                }
                List<String> codes = fireDueBatches(shard, requests);
                for (String code : codes) {
                    metrics.incrementClassified("redis_job_fire", code, "OK");
                }
                if (codes.stream().noneMatch("NEED_RECOMPUTE"::equals)) {
                    nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                            scan.redisNow(), scan.nextScore(), !scan.members().isEmpty(),
                            RedisJobScanInterval.SCHEDULE, shard, properties.getMaxScanIntervalMs()));
                    break;
                }
            }
        }
        return nextDelay;
    }

    /**
     * 业务作用：把大批候选切成有界脚本调用，并在需要重算时停止后续链式提交。
     *
     * @param shard    调度分片
     * @param requests 触发请求
     * @return 已执行项的状态码。
     */
    private List<String> fireDueBatches(int shard, List<RedisJobRepository.FireRequest> requests) {
        if (requests.isEmpty()) return List.of();
        List<String> result = new ArrayList<>();
        for (int start = 0; start < requests.size(); start += properties.getScanBatchSize()) {
            List<String> batch = repository.fireDueBatch(shard,
                    requests.subList(start, Math.min(requests.size(), start + properties.getScanBatchSize())));
            result.addAll(batch);
            if (batch.stream().anyMatch("NEED_RECOMPUTE"::equals)) break;
        }
        return List.copyOf(result);
    }

    /**
     * 业务作用：按误触发策略把一个到期定义展开为有限链，首尾 expected/proposed 可由批量脚本连续 CAS。
     *
     * @param definition 任务定义
     * @param expected   当前权威逻辑时刻
     * @param redisNow   扫描使用的 Redis 时间
     * @return 有界触发链。
     */
    private List<RedisJobRepository.FireRequest> fireRequests(
            RedisJobDefinition definition, long expected, long redisNow) {
        boolean missed = redisNow - expected > properties.getScheduleRttAllowanceMs();
        if (definition.scheduleType() == RedisJobScheduleType.FIXED_DELAY) {
            if (missed && definition.misfire() == RedisJobMisfire.DO_NOTHING) {
                return List.of(new RedisJobRepository.FireRequest(definition, expected,
                        nextAfterNow(definition, expected, redisNow), true, true, true));
            }
            return List.of(new RedisJobRepository.FireRequest(definition, expected, 0L, false, missed, false));
        }
        if (!missed) {
            return List.of(new RedisJobRepository.FireRequest(definition, expected,
                    definition.nextFireAt(expected), false, false, true));
        }
        if (definition.misfire() == RedisJobMisfire.DO_NOTHING) {
            return List.of(new RedisJobRepository.FireRequest(definition, expected,
                    nextAfterNow(definition, expected, redisNow), true, true, true));
        }
        if (definition.misfire() == RedisJobMisfire.FIRE_ONCE_NOW) {
            return List.of(new RedisJobRepository.FireRequest(definition, expected,
                    nextAfterNow(definition, expected, redisNow), false, true, true));
        }
        return catchUpRequests(definition, expected, redisNow);
    }

    /**
     * 业务作用：只保留补偿窗口内最近的有限逻辑时刻，更早积压通过同一 CAS 链跳过。
     *
     * @param definition 任务定义
     * @param expected   当前权威逻辑时刻
     * @param redisNow   扫描使用的 Redis 时间
     * @return 先跳过旧积压、再补偿最近时刻的请求链。
     */
    private List<RedisJobRepository.FireRequest> catchUpRequests(
            RedisJobDefinition definition, long expected, long redisNow) {
        long cutoff = Math.max(0L, redisNow - properties.getMaxCatchUpWindowMs());
        long first = firstAtOrAfter(definition, expected, cutoff);
        Deque<Long> selected = new ArrayDeque<>(properties.getMaxCatchUpRuns());
        long cursor;
        if (definition.scheduleType() == RedisJobScheduleType.FIXED_RATE && first <= redisNow) {
            long total = Math.floorDiv(redisNow - first, definition.intervalMs()) + 1L;
            long retained = Math.min(total, properties.getMaxCatchUpRuns());
            long selectedFirst = Math.addExact(first,
                    Math.multiplyExact(total - retained, definition.intervalMs()));
            for (long index = 0; index < retained; index++) {
                selected.addLast(Math.addExact(selectedFirst,
                        Math.multiplyExact(index, definition.intervalMs())));
            }
            cursor = Math.addExact(first, Math.multiplyExact(total, definition.intervalMs()));
        } else {
            cursor = first;
            // 窗口内刻度总量由显式预算限制：只限制最终保留的 Run 数挡不住病态配置（超大窗口 + 高频
            // cron）在枚举阶段消耗整轮扫描。预算耗尽即放弃本轮补偿，仅推进到当前时刻之后，
            // 语义等同 DO_NOTHING 并计入 ADVANCE_FAILED 观测。
            int budget = 100_000;
            while (cursor <= redisNow) {
                if (--budget < 0) {
                    log.warn("RedisJob catch-up enumeration exceeded budget: jobName={}", definition.name());
                    metrics.incrementClassified("redis_job_fire", "ADVANCE_FAILED", "OK");
                    long advanced = definition.firstFireAtAfter(expected, redisNow);
                    return List.of(new RedisJobRepository.FireRequest(
                            definition, expected, advanced, true, true, true));
                }
                if (selected.size() == properties.getMaxCatchUpRuns()) selected.removeFirst();
                selected.addLast(cursor);
                cursor = definition.nextFireAt(cursor);
            }
        }
        if (selected.isEmpty()) {
            return List.of(new RedisJobRepository.FireRequest(definition, expected, cursor, true, true, true));
        }
        long firstSelected = selected.getFirst();
        List<RedisJobRepository.FireRequest> requests = new ArrayList<>();
        if (firstSelected != expected) {
            requests.add(new RedisJobRepository.FireRequest(
                    definition, expected, firstSelected, true, true, false));
        }
        List<Long> times = List.copyOf(selected);
        for (int index = 0; index < times.size(); index++) {
            long current = times.get(index);
            long next = index + 1 < times.size() ? times.get(index + 1) : cursor;
            requests.add(new RedisJobRepository.FireRequest(
                    definition, current, next, false, true, index + 1 == times.size()));
        }
        return List.copyOf(requests);
    }

    /**
     * 业务作用：快速定位补偿窗口首时刻，fixed rate 使用算术跳跃避免毫秒周期长循环。
     *
     * @param definition 任务定义
     * @param expected   当前逻辑时刻
     * @param cutoff     窗口下界
     * @return 不早于下界的第一个逻辑时刻。
     */
    private long firstAtOrAfter(RedisJobDefinition definition, long expected, long cutoff) {
        if (expected >= cutoff) return expected;
        if (definition.scheduleType() == RedisJobScheduleType.FIXED_RATE
                || definition.scheduleType() == RedisJobScheduleType.FIXED_DELAY) {
            // 两者的补偿窗口定位都必须保持 expected 等差序列。FIXED_DELAY 不能借用 firstFireAtAfter：
            // 那是"以当前时刻重建延迟"的恢复语义，会让窗口首时刻漂移出历史序列并少保留应补偿时刻。
            // gridFireAtAfter 自带极值防护，普通减法在可表示边界的静默环绕也一并消除。
            return definition.gridFireAtAfter(expected, cutoff - 1L);
        }
        // CRON 刻度由表达式自身决定、没有锚点漂移，直接从窗口下界求第一个不早于 cutoff 的刻度；
        // 从陈旧 expected 逐刻度追赶时，一年积压的每秒 cron 要先走完约三千万个明确不补偿的刻度，
        // 这种不抛错的长计算会卡住整轮扫描，异常隔离接不到它。
        return definition.firstFireAtAfter(expected, cutoff - 1L);
    }

    /**
     * 业务作用：推进可见性兜底并恢复租约已真实到期的普通 Run。
     *
     * <p>参数说明: 无。
     *
     * @return 下一次普通索引扫描的有界间隔。
     */
    private long scanIndexes() {
        long nextDelay = properties.getMaxScanIntervalMs();
        for (int shard : localShards()) {
            RedisJobRepository.PromotionResult visible = repository.promoteVisible(shard);
            nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                    visible.redisNow(), visible.nextScore(), visible.promoted() > 0L,
                    RedisJobScanInterval.VISIBLE, shard, visibleScanUpperBoundMs));
            RedisJobRepository.DueScan leases = repository.scanDue(keys.leases(shard), properties.getScanBatchSize());
            nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                    leases.redisNow(), leases.nextScore(), !leases.members().isEmpty(),
                    RedisJobScanInterval.LEASE, shard, properties.getLeaseMs() / 3L));
            for (RedisJobRepository.DueMember member : leases.members()) {
                Optional<RedisJobRepository.RunData> candidate = repository.readAtShard(shard, member.member());
                if (candidate.isEmpty()) continue;
                RedisJobDefinition definition = definitions.get(candidate.get().run().jobName());
                if (definition != null) repository.recoverExpired(definition, member.member());
            }
            RedisJobRepository.DueScan waiting = scanWaiting(shard);
            nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                    waiting.redisNow(), waiting.nextScore(), !waiting.members().isEmpty(),
                    RedisJobScanInterval.WAITING, shard, properties.getFanoutCreateTimeoutMs()));
        }
        return nextDelay;
    }

    /**
     * 业务作用：对账 Fanout 创建 intent，并在根等待到期时先关闭桶内新执行入口。
     *
     * @param shard 普通调度分片
     * @return 本轮到期成员与下一最小 score。
     */
    private RedisJobRepository.DueScan scanWaiting(int shard) {
        RedisJobRepository.DueScan waiting = repository.scanDue(keys.waiting(shard), properties.getScanBatchSize());
        for (RedisJobRepository.DueMember member : waiting.members()) {
            Optional<RedisJobRepository.RunData> candidate = repository.readAtShard(shard, member.member());
            if (candidate.isEmpty()) continue;
            RedisJobRepository.RunData data = candidate.get();
            RedisJobDefinition definition = definitions.get(data.run().jobName());
            if (definition == null || data.fanoutId().isEmpty()) continue;
            ListResult root = watchFanoutRoot(data.fanoutId());
            String rootState = root.state();
            if (data.run().state() == RedisJobState.FANOUT_CREATING) {
                if ("CREATING".equals(rootState)) {
                    fanoutMonitor.advanceRoot(data.fanoutId());
                    root = watchFanoutRoot(data.fanoutId());
                    rootState = root.state();
                }
                if ("COMMITTED".equals(rootState) || "WAITING_CHILDREN".equals(rootState)) {
                    fanoutCoordinator.reconcileCommitted(data.fanoutId(), data.run().runId(),
                            data.rootAttempt(), definition.name(), shard);
                } else if (root.terminal()) {
                    fanoutCoordinator.reconcileTerminal(data.fanoutId(), data.run().runId(), data.rootAttempt(),
                            definition.name(), shard, rootState, fanoutTerminalError(root));
                } else if (rootState.isEmpty()) {
                    repository.failWaitingCreation(definition, data.run().runId());
                }
            } else if (data.run().state() == RedisJobState.WAITING_CHILDREN) {
                if (root.terminal()) {
                    fanoutCoordinator.reconcileTerminal(data.fanoutId(), data.run().runId(), data.rootAttempt(),
                            definition.name(), shard, rootState, fanoutTerminalError(root));
                } else {
                    fanoutMonitor.requestCancel(data.fanoutId(), "WAIT_TIMEOUT");
                }
            }
        }
        return waiting;
    }

    /**
     * 业务作用：扫描 Fanout root 看门狗并把桶内终态幂等回填普通根 Run。
     *
     * <p>参数说明: 无。
     *
     * @return 下一次 Fanout 根扫描的有界间隔。
     */
    private long scanFanoutRoots() {
        long nextDelay = properties.getMaxScanIntervalMs();
        for (int bucket = 0; bucket < keys.fanoutBucketCount(); bucket++) {
            RedisJobRepository.DueScan scan = repository.scanDue(keys.fanoutRoots(bucket), properties.getScanBatchSize());
            nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                    scan.redisNow(), scan.nextScore(), !scan.members().isEmpty(),
                    RedisJobScanInterval.FANOUT_ROOT, bucket, properties.getFanoutCreateTimeoutMs()));
            for (RedisJobRepository.DueMember member : scan.members()) {
                String fanoutId = member.member();
                fanoutMonitor.advanceRoot(fanoutId);
                ListResult result = watchFanoutRoot(fanoutId);
                if (result.terminal()) {
                    fanoutCoordinator.reconcileTerminal(fanoutId, result.rootRunId(), result.rootAttempt(),
                            result.rootJobName(), result.rootShard(), result.state(), fanoutTerminalError(result));
                }
            }
        }
        return nextDelay;
    }

    /**
     * 业务作用：复验 Fanout root 状态并为非终态记录设置下一持久检查时刻。
     *
     * @param fanoutId Fanout 标识
     * @return 看门狗读取结果。
     */
    private ListResult watchFanoutRoot(String fanoutId) {
        var response = scripts.list(RedisJobScript.FANOUT_WATCH_ROOT,
                new String[]{keys.fanoutRoot(fanoutId), keys.fanoutRoots(fanoutId)},
                fanoutId, properties.getMaxScanIntervalMs());
        String code = value(response, 0);
        if (!"OK".equals(code)) return ListResult.EMPTY;
        String state = value(response, 1);
        boolean terminal = Set.of("SUCCEEDED", "PARTIAL_FAILED", "FAILED", "CANCELLED").contains(state);
        return new ListResult(state, value(response, 2), integer(response, 3), value(response, 4),
                integer(response, 5), terminal, value(response, 6), value(response, 7));
    }

    /**
     * 业务作用：按稳定优先级提取 Fanout 终态原因，使 waiting、roots 看门狗与删除收敛回填一致。
     *
     * @param result Fanout 根看门结果
     * @return 删除和等待取消优先返回明确原因，其余返回桶内推进异常。
     */
    private String fanoutTerminalError(ListResult result) {
        if ("WAIT_TIMEOUT".equals(result.cancelReason())) return "WAIT_TIMEOUT";
        if ("JOB_DELETED".equals(result.cancelReason())) return "JOB_DELETED";
        return result.errorType();
    }

    /**
     * 业务作用：从 Redis 当前时刻计算首次计划时刻，非自动任务返回零。
     *
     * @param definition 任务定义
     * @return 下一逻辑时刻。
     */
    private long initialNextFireAt(RedisJobDefinition definition) {
        if (definition.scheduleType() == RedisJobScheduleType.MANUAL
                || definition.scheduleType() == RedisJobScheduleType.FANOUT_ONLY) return 0L;
        int shard = keys.scheduleShard(definition.name());
        long redisNow = repository.scanDue(keys.schedule(shard), 1).redisNow();
        return definition.nextFireAt(redisNow);
    }

    /**
     * 业务作用：按误触发策略选择下一逻辑时刻，避免 FIRE_ONCE 在恢复后连续产生陈旧 Run。
     *
     * @param definition    任务定义
     * @param logicalFireAt 当前逻辑时刻
     * @param redisNow      Redis 当前时刻
     * @return 下一逻辑时刻。
     */
    private long nextAfterNow(RedisJobDefinition definition, long logicalFireAt, long redisNow) {
        long next = definition.nextFireAt(logicalFireAt);
        // CATCH_UP 的补偿数量与时间窗口由 catchUpRequests 单独裁决，不能在这里被闭式推进跳过
        if (definition.misfire() == RedisJobMisfire.CATCH_UP) return next;
        if (next > redisNow) return next;
        // 闭式一步跳到严格晚于当前时刻的网格点：逐格循环存在推进次数上限，闲置超过
        // 上限 × 间隔 的任务会永久卡死且每轮扫描抛错，必须人工重置才能恢复
        return definition.firstFireAtAfter(logicalFireAt, redisNow);
    }

    /**
     * 业务作用：列出本节点已登记普通任务涉及的调度分片，避免扫描无关分片。
     *
     * @return 不可变快照语义的有序分片数组。
     */
    private int[] localShards() {
        return localShardSnapshot;
    }

    /**
     * 业务作用：仅在本地定义集合变化时重建分片快照，使周期扫描不再创建临时集合。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void refreshLocalShards() {
        boolean[] present = new boolean[keys.shardCount()];
        long visibleUpperBound = properties.getMaxScanIntervalMs();
        int count = 0;
        for (RedisJobDefinition definition : definitions.values()) {
            if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) continue;
            int shard = keys.scheduleShard(definition.name());
            if (!present[shard]) {
                present[shard] = true;
                count++;
            }
            visibleUpperBound = Math.min(visibleUpperBound, definition.retryDelayMs());
        }
        int[] snapshot = new int[count];
        int index = 0;
        for (int shard = 0; shard < present.length; shard++) {
            if (present[shard]) snapshot[index++] = shard;
        }
        visibleScanUpperBoundMs = Math.max(properties.getMinScanIntervalMs(), visibleUpperBound);
        localShardSnapshot = snapshot;
    }

    /**
     * 业务作用：写入端产生更早可见成员时执行本分片定点推进，不等待正在进行的长退避周期结束。
     *
     * @param shard   调度分片
     * @param delayMs 服务端计算的剩余时长
     *                返回：无返回值。
     */
    private void wakeVisible(int shard, long delayMs) {
        if (!running.get()) return;
        long boundedDelay = Math.max(0L, delayMs);
        if (boundedDelay <= properties.getMinScanIntervalMs()) {
            promoteVisibleNow(shard);
            return;
        }
        try {
            monitors.schedule(() -> promoteVisibleNow(shard), boundedDelay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // 生命周期已经关闭时不再领取新任务，持久索引由其它存活节点按期限继续推进。
        }
    }

    /**
     * 业务作用：立即推进单个分片的新可见成员；异常不影响已经提交的 Run 状态与跨节点兜底。
     *
     * @param shard 调度分片
     *              返回：无返回值。
     */
    private void promoteVisibleNow(int shard) {
        if (!running.get()) return;
        try {
            repository.promoteVisible(shard);
        } catch (RuntimeException error) {
            log.warn("RedisJob targeted visible promotion failed: shard={}", shard, error);
        }
    }

    /**
     * 业务作用：读取本地已登记定义，不存在时给出明确调用错误。
     *
     * @param jobName 任务名
     * @return 任务定义。
     */
    private RedisJobDefinition requireDefinition(String jobName) {
        RedisJobDefinition definition = definitions.get(jobName);
        if (definition == null) throw new IllegalArgumentException("unknown RedisJob: " + jobName);
        return definition;
    }

    /**
     * 业务作用：执行周期心跳；权威结果不确定时立即关闭本地准入并转入 DRAINING。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值；异常由监视线程记录，任务准入保持关闭。
     */
    private void safeHeartbeat() {
        // 空定义 Scheduler 没有可登记权威，保持 DRAINING 等待首个 Handler，不把预期的 NOT_FOUND 当成运行异常。
        if (!running.get() || awaitingInitialRegistration.get()) return;
        try {
            heartbeat();
            lastHeartbeatAt.set(System.currentTimeMillis());
        } catch (Throwable failure) {
            failClosedAdmission(failure);
            log.error("RedisJob heartbeat authority became uncertain; task admission is closed", failure);
        }
    }

    /**
     * 业务作用：心跳或重登记结局不明时尽力封闭所有新任务入口。
     *
     * <p>返回：无返回值；关门失败作为 suppressed 证据附加到原异常。
     *
     * @param cause 原始权威失败
     */
    private void failClosedAdmission(Throwable cause) {
        try {
            admission.close();
        } catch (RuntimeException | Error closingFailure) {
            cause.addSuppressed(closingFailure);
        }
    }

    /**
     * 业务作用：根据最近的 Redis score 自适应安排下一次调度扫描，异常时使用最短周期恢复。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void scheduleScanCycle() {
        if (!running.get()) return;
        long delay = properties.getMinScanIntervalMs();
        try {
            delay = scanSchedules();
            lastScheduleScanAt.set(System.currentTimeMillis());
        } catch (Throwable error) {
            log.error("RedisJob schedule scan failed", error);
        }
        if (running.get()) monitors.schedule(this::scheduleScanCycle, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：按普通索引最早 score 自适应续排扫描，异常时缩短间隔恢复持久状态推进。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void indexScanCycle() {
        if (!running.get()) return;
        long delay = properties.getMinScanIntervalMs();
        try {
            delay = scanIndexes();
        } catch (Throwable error) {
            log.error("RedisJob index scan failed", error);
        }
        if (running.get()) monitors.schedule(this::indexScanCycle, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：按 Fanout 根最早 score 自适应续排跨 slot 对账，避免空桶固定频率访问 Redis。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void fanoutRootScanCycle() {
        if (!running.get()) return;
        long delay = properties.getMinScanIntervalMs();
        try {
            delay = scanFanoutRoots();
        } catch (Throwable error) {
            log.error("RedisJob fanout root scan failed", error);
        }
        if (running.get()) monitors.schedule(this::fanoutRootScanCycle, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：按 Fanout 恢复索引最早 score 自适应续排扫描，通知丢失时仍保持持久兜底。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void fanoutIndexScanCycle(int bucket, long epoch) {
        if (!running.get() || fanoutIndexEpochs.get(bucket) != epoch) return;
        long delay = properties.getMinScanIntervalMs();
        try {
            delay = fanoutMonitor.scanBucket(bucket);
        } catch (Throwable error) {
            log.error("RedisJob fanout index scan failed: bucket={}", bucket, error);
        }
        if (running.get() && fanoutIndexEpochs.get(bucket) == epoch) {
            scheduleFanoutIndexScan(bucket, delay, epoch);
        }
    }

    /**
     * 业务作用：按桶独立安排 Fanout 索引周期，使活跃桶的短期限不会拖高全部空桶的扫描频率。
     *
     * @param bucket  Fanout 桶
     * @param delayMs 下一次扫描延迟
     * @param epoch   当前桶调度代次
     *                返回：无返回值。
     */
    private void scheduleFanoutIndexScan(int bucket, long delayMs, long epoch) {
        try {
            monitors.schedule(() -> fanoutIndexScanCycle(bucket, epoch),
                    Math.max(0L, delayMs), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // 生命周期关闭后不再建立新扫描任务，持久索引由其它存活节点继续推进。
        }
    }

    /**
     * 业务作用：本进程建立 Fanout 根时中断目标桶的旧长退避，并以新期限立即重算下一扫描时刻。
     *
     * @param fanoutId         Fanout 标识
     * @param receiptTimeoutMs 根任务回执期限
     *                         返回：无返回值。
     */
    private void wakeFanoutIndex(String fanoutId, long receiptTimeoutMs) {
        int bucket = keys.fanoutBucket(fanoutId);
        fanoutMonitor.activateBucket(bucket, receiptTimeoutMs);
        if (!running.get()) return;
        long epoch = fanoutIndexEpochs.incrementAndGet(bucket);
        scheduleFanoutIndexScan(bucket, 0L, epoch);
    }

    /**
     * 业务作用：隔离注册表回收异常，避免心跳和执行恢复受到影响。 返回：无返回值。
     */
    private void safeRegistryGc() {
        safely("registry gc", this::cleanupMetadata);
    }

    /**
     * 业务作用：隔离单轮删除存量收敛异常，保留后续周期。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void safeReapDeleted() {
        safely("deleted job reap", this::reapDeletedBacklog);
    }

    /**
     * 业务作用：驱动已删除任务的四类索引存量（waitq、visible、leases、waiting）分批收敛，
     * 使删除动作保持 O(1)、收敛不依赖本地定义仍然存在。
     *
     * <p>脚本用 ZSCAN 游标跨轮接力扫描共享索引，本轮零终态化不代表收敛完成——游标可能仍在
     * 越过其它任务的成员，因此以脚本回报的 active 信号而不是终态化数量决定是否继续驱动；
     * 单分片每轮设推进上限，避免大量积压时长时间占用监视线程。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void reapDeletedBacklog() {
        long reaped = 0L;
        for (int shard = 0; shard < keys.shardCount(); shard++) {
            for (int round = 0; round < 10; round++) {
                RedisJobRepository.ReapResult result = repository.reapDeleted(shard);
                reaped += result.reaped();
                for (RedisJobRepository.FanoutReapRequest request : result.fanoutRequests()) {
                    driveDeletedFanout(shard, request);
                }
                if (!result.active()) break;
            }
        }
        if (reaped > 0L) metrics.add("redis_job_deleted_reap_total", reaped);
    }

    /**
     * 业务作用：在普通根的持久等待截止点后，跨 slot 关闭 Fanout 桶内新执行权，
     * 并在桶已终态或从未建立时幂等回填普通根。
     *
     * @param shard   普通根所在调度分片
     * @param request 跨 slot 定位证据
     * <p>返回：无返回值；未终态桶保留 waiting/reaping 由后续周期继续驱动。
     */
    private void driveDeletedFanout(int shard, RedisJobRepository.FanoutReapRequest request) {
        ListResult result = watchFanoutRoot(request.fanoutId());
        if (result.state().isEmpty()) {
            fanoutCoordinator.reconcileTerminal(request.fanoutId(), request.runId(), request.rootAttempt(),
                    request.jobName(), shard, "CANCELLED", "JOB_DELETED");
            return;
        }
        if (result.terminal()) {
            fanoutCoordinator.reconcileTerminal(request.fanoutId(), request.runId(), request.rootAttempt(),
                    request.jobName(), shard, result.state(), fanoutTerminalError(result));
            return;
        }
        // 普通根仍保留在 waiting 中，取消跨 slot 失败时下一删除收敛周期会重试，
        // 不会先撤 tombstone 再丢失桶内运行中 shard 的唯一协作式取消入口。
        fanoutMonitor.requestCancel(request.fanoutId(), "JOB_DELETED");
    }

    /**
     * 业务作用：回收注册表与到期定义墓碑，两个清理都只处理已越过各自保留门禁的记录。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void cleanupMetadata() {
        metrics.add("redis_job_registry_gc_total", registry.gc());
        long tombstones = 0L;
        for (int shard = 0; shard < keys.shardCount(); shard++) {
            tombstones += repository.cleanupTombstones(shard);
        }
        metrics.add("redis_job_tombstone_gc_total", tombstones);
    }

    /**
     * 业务作用：续期当前执行器；登记过期时重新走重路径恢复完整能力声明。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void heartbeat() {
        try {
            registry.heartbeat();
        } catch (IllegalStateException error) {
            if (error.getMessage() == null || !error.getMessage().contains("registration expired")) throw error;
            List<RedisJobDefinition> currentDefinitions = definitions.values().stream()
                    .sorted(Comparator.comparing(RedisJobDefinition::name))
                    .toList();
            if (currentDefinitions.isEmpty()) {
                throw new IllegalStateException("RedisJob executor registration expired without definitions", error);
            }
            // 按稳定顺序重建全量能力后再执行一次轻心跳，只有最终 revision 取得连续确认
            // 才能把本周期视为恢复成功；任一回包不明都会由上层立即关闭本地准入。
            try {
                currentDefinitions.forEach(registry::register);
                registry.heartbeat();
            } catch (RuntimeException | Error registrationFailure) {
                // 重建可能已写入部分 ACTIVE 能力；本地仅关门不足以撤销该外部事实，必须进入全量注销序列。
                terminateAfterAuthorityFailureAsync(registrationFailure);
                throw registrationFailure;
            }
        }
    }

    /**
     * 业务作用：让当前执行器进入 DRAINING，停止普通和 Fanout 新领取并保留已有 attempt 的完成路径。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    public synchronized void drain() {
        requireOpen();
        // 先撤销首次开门资格，再在同一生命周期临界区关门；成功返回后旧登记不能重新发布 ACTIVE。
        awaitingInitialRegistration.set(false);
        admission.close();
    }

    /**
     * 业务作用：在通知订阅与消费通道均就绪后重新把当前执行器开放为 ACTIVE。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    public synchronized void activate() {
        requireRunning();
        admission.open();
        awaitingInitialRegistration.set(false);
    }

    /**
     * 业务作用：统一记录监视器异常而不让固定周期任务被调度器取消。
     *
     * @param action 动作名
     * @param task   监视动作
     *               返回：无返回值。
     */
    private void safely(String action, Runnable task) {
        if (!running.get()) return;
        try {
            task.run();
        } catch (Throwable error) {
            log.error("RedisJob {} failed", action, error);
        }
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
     * 业务作用：读取脚本整数字段并检查 Java int 溢出。
     *
     * @param values 返回列表
     * @param index  下标
     * @return 整型值。
     */
    private static int integer(List<Object> values, int index) {
        String value = value(values, index);
        return value.isEmpty() ? 0 : Math.toIntExact(Long.parseLong(value));
    }

    /**
     * 业务作用：报告调度器是否已经开放任务领取。 @return 已启动返回 true。
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 业务作用：让 Spring 在普通组件之后启动任务调度。 @return 生命周期阶段。
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 10_000;
    }

    /**
     * 业务作用：要求 Spring 容器刷新完成后自动启动。 @return 始终为 true。
     */
    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * 业务作用：提交不可逆停机终态并关闭本 source 的全部新任务准入。
     *
     * <p>管理器会先对全部 source 调用本方法，再并行发布 DRAINING 并执行长时间资源收口。
     * 本阶段不做 Redis 往返，避免某个 source 网络超时时其它 source 仍保持本地准入。关门失败会保留到
     * 共享停机结果，不阻断其它 source 提交关门。若另一调用方已提交终态，本方法立即返回，
     * 资源收口调用会等待该关门步骤完成。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：当前调用成功提交关门，或已有调用方先行提交后返回；不在此处等待 Handler 排空。
     */
    void prepareStop() {
        if (!closed.compareAndSet(false, true)) return;
        Throwable failure = null;
        try {
            awaitingInitialRegistration.set(false);
            // 先封闭两类回调的 Redis pre-start 入口；已越过边界的回调必须交接到 Handler 在途账目后才能退出。
            preStartBarrier.closeAdmission();
            try {
                // 不可逆准入权威独立于 Scheduler monitor；即使 register/start/activate 正阻塞在 Redis，
                // 管理器也能立即关闭本 source，后到的开门动作会在落地前复验同一终态。
                admission.terminate();
            } catch (Throwable closingFailure) {
                failure = closingFailure;
            } finally {
                running.set(false);
            }
        } finally {
            // 关门结果必须先发布再通知资源收口执行者，否则会丢失准入失败证据。
            stopPreparationFailure = failure;
            stopPrepared.countDown();
        }
    }

    /**
     * 业务作用：先提交停机关门，再尽力关闭监视器、执行资源和注册成员。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：先前进入的生命周期控制调用退出且全部停机步骤均已尝试后返回；任一步失败时抛出带后续失败证据的首个异常。
     */
    @Override
    public void stop() {
        prepareStop();
        RedisJobShutdownSupport.await(stopPrepared);
        if (!shutdownStarted.compareAndSet(false, true)) {
            RedisJobShutdownSupport.await(stopCompleted);
            RedisJobShutdownSupport.rethrow(stopFailure);
            return;
        }

        Throwable failure = stopPreparationFailure;
        long shutdownDeadline = RedisJobShutdownSupport.deadlineAfterMillis(properties.getMaxRunDurationMs());
        try {
            synchronized (this) {
                // 全部 source 已由独立准入权威关门；此处才等待先前进入的 register/start/activate 退出，
                // 防止其 Redis 回包晚于 Registry 注销或执行资源释放，同时不会延后其它 source 的本地关门。
            }
            // 全部 source 的本地入口已关闭，现在才并行发布服务端 DRAINING，
            // 某个 Redis 连接超时不会延后其它 source 的关门或排空。
            failure = RedisJobShutdownSupport.attempt(failure, admission::close);
            failure = RedisJobShutdownSupport.attempt(failure, monitors::shutdown);
            // 已越过本地门禁的回调要么退出，要么先登记进 Handler 在途账目；零计数之间不能出现未记账 attempt。
            boolean preStartsDrained = preStartBarrier.awaitIdle(shutdownDeadline);
            if (!preStartsDrained) {
                failure = RedisJobShutdownSupport.attempt(failure, () -> {
                    throw new IllegalStateException(
                            "RedisJob pre-start callbacks did not drain before shutdown deadline");
                });
            }
            // Handler 排空前保留派发控制执行器与续租器；已授予 attempt 的任务仍需要它们建立超时门禁并提交终态。
            boolean handlersDrained = preStartsDrained && registry.awaitIdle(
                    RedisJobShutdownSupport.remainingMillis(shutdownDeadline));
            if (!handlersDrained) {
                failure = RedisJobShutdownSupport.attempt(failure, () -> {
                    throw new IllegalStateException("RedisJob active handlers did not drain before shutdown deadline");
                });
            }
            if (preStartsDrained && handlersDrained) {
                failure = closeOwnershipResources(failure, shutdownDeadline);
                if (ownershipResourcesClosed()) {
                    finalCleanupStarted.set(true);
                    finalCleanupFailure = failure;
                    finalCleanupCompleted.countDown();
                } else {
                    // 首次资源预算耗尽时不能注销成员或发布 final；continuation 继续等待执行器实际终止。
                    startFinalCleanupContinuation();
                }
            } else {
                // 尚有可能取得或持有 attempt 时必须保留派发控制、续租器与 DRAINING 成员，不能为追求资源关闭而制造孤儿 RUNNING。
                failure = RedisJobShutdownSupport.attempt(failure, () -> {
                    throw new IllegalStateException(
                            "RedisJob ownership resources remain active because shutdown drain is incomplete");
                });
                startFinalCleanupContinuation();
            }
        } finally {
            // 首次期限内允许执行的安全步骤均已尝试后发布稳定结果；并发 close 等待该结果，
            // Spring 回调另行等待 final cleanup，不能把首次结果误当成依赖已经可以释放。
            stopFailure = failure;
            stopCompleted.countDown();
        }
        RedisJobShutdownSupport.rethrow(failure);
    }

    /**
     * 业务作用：在首次停机期限耗尽后建立唯一后台 continuation，待权威工作退出后按安全顺序释放所有权资源。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：continuation 已存在或成功启动后返回；启动失败时由当前线程完成最终收口后返回。
     */
    private void startFinalCleanupContinuation() {
        if (!finalCleanupStarted.compareAndSet(false, true)) return;
        try {
            finalCleanupStarter.start(
                    "redis-job-final-cleanup-" + keys.qualifier(),
                    () -> finishRetainedResources(null));
        } catch (Throwable startFailure) {
            // 唯一启动权已经取得，无法交给独立线程时必须由当前停机调用方接管；回滚标志会让多个调用方争抢同一资源。
            finishRetainedResources(startFailure);
        }
    }

    /**
     * 业务作用：等待迟到 pre-start 与 Handler 全部退出，再关闭派发、监视、续租并注销 DRAINING 成员。
     *
     * @param orchestrationFailure continuation 无法启动时的编排失败，可为 null
     * <p>返回：无返回值；最终收口失败写入独立结果并记录 source，不改写已经发布的首次停机失败。
     */
    private void finishRetainedResources(Throwable orchestrationFailure) {
        Throwable failure;
        try (RedisJobShutdownSupport.InterruptDeferral ignored = RedisJobShutdownSupport.deferInterrupts()) {
            List<Throwable> failures = new ArrayList<>();
            if (orchestrationFailure != null) failures.add(orchestrationFailure);
            try {
                preStartBarrier.awaitIdle();
                registry.awaitIdle();
                // final cleanup 是 Spring 依赖可以销毁的强边界；已经进入该阶段后必须等执行器实际终止。
                Throwable cleanupFailure = null;
                while (!ownershipResourcesClosed()) {
                    Throwable current = closeOwnershipResources(null, Long.MAX_VALUE);
                    if (cleanupFailure == null && current != null) cleanupFailure = current;
                    if (!ownershipResourcesClosed()) awaitCleanupRetry();
                }
                if (cleanupFailure != null) failures.add(cleanupFailure);
            } catch (Throwable cleanupFailure) {
                failures.add(cleanupFailure);
            } finally {
                failure = RedisJobShutdownSupport.aggregate(
                        "RedisJob final ownership cleanup failed", failures);
                finalCleanupFailure = failure;
                finalCleanupCompleted.countDown();
            }
            if (failure == null) {
                log.info("RedisJob retained ownership resources were closed: source={}", keys.qualifier());
            } else {
                log.error("RedisJob retained ownership resource cleanup failed: source={}",
                        keys.qualifier(), failure);
            }
        }
    }

    /**
     * 业务作用：在 pre-start 与 Handler 均已归零后，按派发、监视、续租、Registry 的依赖顺序释放所有权资源。
     *
     * @param failure      先前停机步骤的失败，可为 null
     * @param deadlineNanos 派发执行器终止使用的绝对单调时钟截止
     * @return 最早失败及其后续 suppressed 证据；全部资源关闭时返回原 failure。
     */
    private Throwable closeOwnershipResources(Throwable failure, long deadlineNanos) {
        try (RedisJobShutdownSupport.InterruptDeferral ignored = RedisJobShutdownSupport.deferInterrupts()) {
            failure = RedisJobShutdownSupport.attempt(failure, () -> dispatcher.close(deadlineNanos));
            failure = RedisJobShutdownSupport.attempt(failure, () -> fanoutDispatcher.close(deadlineNanos));
            failure = RedisJobShutdownSupport.attempt(failure, monitors::shutdown);
            failure = RedisJobShutdownSupport.attempt(failure, () -> RedisJobShutdownSupport.awaitTermination(
                    monitors, deadlineNanos, "RedisJob monitor executor"));
            failure = RedisJobShutdownSupport.attempt(failure, () -> leaseRenewer.close(deadlineNanos));
            if (!localOwnershipResourcesClosed()) {
                return RedisJobShutdownSupport.attempt(failure, () -> {
                    throw new IllegalStateException(
                            "RedisJob local ownership resources remain active after shutdown deadline");
                });
            }
            // 所有可能访问 Redis 的本地执行器都已终止，此后才允许撤销服务端成员资格。
            return RedisJobShutdownSupport.attempt(failure, registry::unregister);
        }
    }

    /**
     * 业务作用：确认所有可能在停机后继续接收回调或访问 Redis 的本地资源均已实际关闭。
     *
     * <p>参数说明: 无。
     *
     * @return Stream 容器、Pub/Sub 与普通、Fanout、监视、续租执行器全部关闭时为 true。
     */
    private boolean localOwnershipResourcesClosed() {
        return dispatcher.resourcesClosed()
                && fanoutDispatcher.resourcesClosed()
                && monitors.isTerminated()
                && leaseRenewer.isTerminated();
    }

    /**
     * 业务作用：复验本地回调资源与服务端成员资格都已结束，作为 Spring 依赖可释放的最终边界。
     *
     * <p>参数说明: 无。
     *
     * @return 本地资源关闭且 Registry 注销得到确定确认时为 true。
     */
    private boolean ownershipResourcesClosed() {
        return localOwnershipResourcesClosed() && registry.isWithdrawn();
    }

    /**
     * 业务作用：最终收口遇到暂态外部失败时有界退避，避免 Redis 或 listener 不可用期间形成忙循环。
     *
     * <p>参数说明: 无。
     *
     * <p>返回: 无返回值；退避结束后返回；中断延迟到最外层资源边界再恢复。
     */
    private void awaitCleanupRetry() {
        long delayMs = Math.max(10L, Math.min(properties.getHeartbeatMs(), 250L));
        try {
            TimeUnit.MILLISECONDS.sleep(delayMs);
        } catch (InterruptedException interrupted) {
            RedisJobShutdownSupport.captureInterrupt();
        }
    }

    /**
     * 业务作用：等待本 source 的所有权资源完成最终收口，供实际托管生命周期的管理器建立依赖释放边界。
     *
     * <p>参数说明: 无。
     *
     * @return 最终收口成功时返回 null；派发、监视、续租或注销仍失败时返回独立失败结果。
     */
    Throwable awaitFinalCleanup() {
        RedisJobShutdownSupport.await(finalCleanupCompleted);
        return finalCleanupFailure;
    }

    /**
     * 业务作用：判断本 source 是否已经结束最终资源收口，避免生命周期回调为已完成状态建立等待线程。
     *
     * <p>参数说明: 无。
     *
     * @return 全部所有权执行器已经终止且 Registry 注销已经尝试时为 true。
     */
    boolean isFinalCleanupComplete() {
        return finalCleanupCompleted.getCount() == 0L;
    }

    /**
     * 业务作用：等待本 source 的最终所有权资源完成收口后通知 Spring 生命周期回调。
     *
     * <p>首次排空期限耗尽时，本方法异步等待迟到 Handler 退出，避免 Spring 在续租器和 Registry
     * 仍承担完成权威时先销毁 Redis 依赖。首次停机失败仍由同步 {@link #stop()} 稳定发布；异步回调路径
     * 无法把异常交还原调用栈，因此只记录失败并确保回调恰好执行一次。
     *
     * @param callback 停机完成回调
     * 返回: 最终收口已结束时同步执行回调；仍在收口时建立等待任务后返回。
     */
    @Override
    public void stop(Runnable callback) {
        Objects.requireNonNull(callback, "callback must not be null");
        Throwable failure = null;
        try {
            stop();
        } catch (Throwable stoppingFailure) {
            failure = stoppingFailure;
        }
        if (isFinalCleanupComplete()) {
            // 已完成路径保留同步调用语义；回调局部失败不得写入所有等待者共享的停机结果。
            RedisJobShutdownSupport.completeCallback(failure, callback);
            return;
        }
        Throwable stoppingFailure = failure;
        Runnable completion = () -> {
            Throwable cleanupFailure = awaitFinalCleanup();
            Throwable combined = callbackFailure(stoppingFailure, cleanupFailure);
            try {
                RedisJobShutdownSupport.completeCallback(combined, callback);
            } catch (Throwable lifecycleFailure) {
                // 异步 SmartLifecycle 调用没有异常返回通道；记录完整证据后仍保持首次停机结果不可改写。
                log.error("RedisJob asynchronous lifecycle completion failed: source={}",
                        keys.qualifier(), lifecycleFailure);
            }
        };
        try {
            // Spring 以回调作为同阶段依赖可继续释放的边界，必须晚于 Handler、派发、续租和 Registry。
            Thread.ofVirtual().name("redis-job-lifecycle-completion-" + keys.qualifier()).start(completion);
        } catch (Throwable threadFailure) {
            // 无法建立等待任务时宁可阻塞当前生命周期线程，也不能提前回调并让依赖在权威资源之前销毁。
            Throwable cleanupFailure = awaitFinalCleanup();
            Throwable combined = callbackFailure(stoppingFailure, cleanupFailure);
            combined = callbackFailure(combined, threadFailure);
            RedisJobShutdownSupport.completeCallback(combined, callback);
        }
    }

    /**
     * 业务作用：为单次生命周期回调合并首次停机与独立最终收口结果，不改写任一共享异常对象。
     *
     * @param stoppingFailure 首次停机失败，可为 null
     * @param cleanupFailure  最终所有权资源收口失败，可为 null
     * @return 两阶段均成功时返回 null；否则返回原失败或当前调用新建的汇总结果。
     */
    private static Throwable callbackFailure(Throwable stoppingFailure, Throwable cleanupFailure) {
        if (stoppingFailure == null) return cleanupFailure;
        if (cleanupFailure == null || cleanupFailure == stoppingFailure) return stoppingFailure;
        return RedisJobShutdownSupport.aggregate(
                "RedisJob shutdown and final ownership cleanup failed",
                List.of(stoppingFailure, cleanupFailure));
    }

    /**
     * 业务作用：作为最终销毁边界等待本 source 的所有权资源实际完成收口。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：final cleanup 结束后返回；首次停机或最终收口失败时抛出不改写共享结果的汇总异常。
     */
    @Override
    public void close() {
        Throwable failure = null;
        try {
            stop();
        } catch (Throwable stoppingFailure) {
            failure = stoppingFailure;
        }
        // AutoCloseable 是资源实际不可再使用的边界；不能像有界 stop 一样在迟到 Handler 仍持权时提前返回。
        Throwable cleanupFailure = awaitFinalCleanup();
        RedisJobShutdownSupport.rethrow(callbackFailure(failure, cleanupFailure));
    }

    /**
     * 业务作用：抽象 final-cleanup 的任务启动边界，使启动失败能够在唯一所有权尚未遗失时同步接管。
     */
    @FunctionalInterface
    interface ContinuationStarter {
        /**
         * 业务作用：启动一个承担最终资源收口的独立任务。
         *
         * <p>返回: 无返回值；无法启动时抛出异常，调用方必须同步接管任务。
         *
         * @param threadName 稳定线程名
         * @param task       最终资源收口任务
         */
        void start(String threadName, Runnable task);
    }

    /**
     * 业务作用：承载一次 Fanout root 看门狗读取结果。
     */
    private record ListResult(String state, String rootRunId, int rootAttempt, String rootJobName,
                              int rootShard, boolean terminal, String cancelReason, String errorType) {
        private static final ListResult EMPTY = new ListResult("", "", 0, "", 0, false, "", "");
    }
}
