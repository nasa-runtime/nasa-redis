package io.github.nasaruntime.redis.cache.redis.partition;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.nasaruntime.core.base.Partition;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.core.evt.PooledEvtData;
import io.github.nasaruntime.core.utils.MapUtils;
import io.github.nasaruntime.core.utils.ObjMprUtils;
import io.github.nasaruntime.core.utils.StringUtils;
import io.github.nasaruntime.redis.cache.redis.*;
import org.springframework.data.redis.connection.stream.MapRecord;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;

/**
 * 业务作用：承载单个 RedisProxy 的两种 Stream 来源、Partition dispatcher、硬容量、重试、确认复验与完整排干。
 */
final class StreamPartitionRuntime implements AutoCloseable {

    private final RedisProxy redisProxy;
    private final StreamSubscriptionRegistry subscriptions;
    private final OrderedKeyCoordinator orderedKeys;
    private final PartitionRecordCapacity recordCapacity;
    private final PartitionDispatchCapacity dispatchCapacity;
    private final StreamRuntimeStatus status = new StreamRuntimeStatus();
    private final ProxyRecordAckLedger proxyLedger;
    private final StreamCommitCoordinator commits;
    private final StreamRetryCoordinator retries;
    private final ExecutorService waitExecutor;
    private final AtomicBoolean admission = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final AtomicLong ticketIds = new AtomicLong();
    private final ConcurrentHashMap<Long, TaskTicket> inFlightTickets = new ConcurrentHashMap<>();
    private final java.util.Set<StreamSourceAuthority> uncertainRecoveries = new java.util.HashSet<>();
    private final ReentrantReadWriteLock submissionBarrier = new ReentrantReadWriteLock(true);
    private final String processSession = UUID.randomUUID().toString();
    private final LinkedHashMap<ProxySourceKey, ProxyBothStreamSource> proxySources = new LinkedHashMap<>();
    private final int maxProxyFieldsPerRecord;
    private final long drainTimeoutMillis;

    /**
     * 业务作用：按本代理配置建立不共享可变状态的 Stream-to-Partition 运行时。
     *
     * @param redisProxy    Redis 命令代理
     * @param subscriptions 已完成 prepare/activate 事务的计划注册表
     *                      返回: admission 开放且所有硬容量空闲的运行时。
     */
    StreamPartitionRuntime(RedisProxy redisProxy, StreamSubscriptionRegistry subscriptions) {
        this.redisProxy = Objects.requireNonNull(redisProxy, "redisProxy");
        this.subscriptions = Objects.requireNonNull(subscriptions, "subscriptions");
        RedisPartitionProperties.LocalConsumer config = redisProxy.getStream().getPartition().getLocalConsumer();
        this.orderedKeys = new OrderedKeyCoordinator(
                config.getMaxBlockedKeysPerClaim(), config.getMaxDeferredIdsPerKey());
        this.recordCapacity = new PartitionRecordCapacity(
                config.getMaxInFlightRecords(), () -> status.failReadiness("redis_count_contract"));
        this.dispatchCapacity = new PartitionDispatchCapacity(
                config.getMaxInFlightTasks(),
                config.getMaxPendingCommitAttempts(),
                config.getMaxPendingCommitRecords());
        this.maxProxyFieldsPerRecord = config.getMaxProxyFieldsPerRecord();
        this.drainTimeoutMillis = redisProxy.getStream().getPartition().getDrainTimeoutMs();
        ThreadFactory factory = Thread.ofVirtual()
                .name("redis-partition-wait-" + redisProxy.getQualifier() + "-", 0)
                .factory();
        this.waitExecutor = Executors.newThreadPerTaskExecutor(factory);
        this.proxyLedger = new ProxyRecordAckLedger(config.getMaxProxyLedgerRecords(), status);
        this.commits = new StreamCommitCoordinator(
                redisProxy.getQualifier(), orderedKeys, status, waitExecutor,
                config.getAckReconcileInitialDelayMs(), config.getAckReconcileMaxDelayMs(),
                redisProxy::streamPartitionMetrics);
        this.retries = new StreamRetryCoordinator(
                this, status,
                config.getMaxInFlightRetries(),
                config.getMaxPendingUnorderedRetries(),
                config.getMaxRouteBlockedRecords(),
                config.getRetryInitialDelayMs(),
                config.getRetryMaxDelayMs());
    }

    /**
     * 业务作用：暴露专用虚拟等待执行器，使解码、Future 等待与首次 ACK 不占用普通业务执行器。
     *
     * <p>参数说明: 无。
     *
     * @return 本运行时独占、停机最后关闭的执行器
     */
    ExecutorService waitExecutor() {
        return waitExecutor;
    }

    /**
     * 业务作用：把时间轮发出的轻量再平衡信号转交组件专用虚拟控制域，避免 Redis I/O 占住 TimingWheel 平台线程。
     *
     * @param control 单轮心跳、分区释放与抢占动作
     * @return admission 开放且动作成功进入执行域时返回 true
     */
    boolean submitRedisControl(Runnable control) {
        if (!admission.get()) return false;
        // 排队本身也承担停机责任，不能等线程开始运行后才把 Redis 控制动作计入排干表。
        StreamRuntimeStatus.DrainToken drain = status.tryEnter(StreamRuntimeStatus.Resource.REBALANCE);
        if (drain == null) return false;
        try {
            waitExecutor.execute(() -> {
                try (drain) {
                    control.run();
                }
            });
            return true;
        } catch (Throwable rejected) {
            // 执行器没有接管动作时由提交方归还责任，终态不能等待一个永远不会运行的任务。
            drain.close();
            status.failReadiness("redis_control_rejected");
            return false;
        }
    }

    /**
     * 业务作用：返回进程启动会话量，BOTH 实际 consumer name 用它隔离重启前后的迟到 ACK。
     *
     * <p>参数说明: 无。
     *
     * @return 本运行时唯一会话字符串
     */
    String processSession() {
        return processSession;
    }

    /**
     * 业务作用：取得一个 BOTH stream/group 的配置快照，未声明时使用代码默认值。
     *
     * @param stream Stream key
     * @param group  consumer group
     * @return 对应 group 配置或只读默认配置
     */
    NasaLettuceConfig.Group groupConfig(String stream, String group) {
        Map<String, NasaLettuceConfig.Group> groups = redisProxy.getStream().getGroup().get(stream);
        return groups == null ? NasaLettuceConfig.Group.DEFAULT
                : groups.getOrDefault(group, NasaLettuceConfig.Group.DEFAULT);
    }

    /**
     * 业务作用：为一个 Redis 来源创建读取前 record permit 交接状态。
     *
     * @param sourceId 来源生命周期内稳定的对象身份
     * @param wake     容量归还时缩短 runner 等待的动作
     * @return 初始不持有 record permit 的来源状态
     */
    PartitionSourceRecordState newRecordState(Object sourceId, Runnable wake) {
        return new PartitionSourceRecordState(recordCapacity, sourceId, wake);
    }

    /**
     * 业务作用：在全部 listener 计划激活后，按 stream/group 合并并开放 BOTH dedicated container。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；任一 container 初始化失败时已开放来源全部回滚关闭。
     */
    synchronized void startProxySources() {
        if (!admission.get()) throw new IllegalStateException("Stream runtime admission is closed");
        List<ProxyBothStreamSource> created = new ArrayList<>();
        try {
            for (StreamSubscriptionPlan plan : subscriptions.plans()) {
                if (plan.mode() != ConsumeMode.BOTH) continue;
                for (String topic : plan.topics()) {
                    ProxySourceKey key = new ProxySourceKey(topic, plan.group());
                    ProxyBothStreamSource source = proxySources.get(key);
                    if (source == null) {
                        source = new ProxyBothStreamSource(redisProxy, this, topic, plan.group());
                        proxySources.put(key, source);
                        created.add(source);
                    }
                }
            }
            for (ProxyBothStreamSource source : created) source.start();
        } catch (Throwable failure) {
            for (ProxyBothStreamSource source : created) {
                try {
                    source.close();
                } catch (Throwable ignored) {
                }
            }
            proxySources.values().removeAll(created);
            throw failure;
        }
    }

    /**
     * 业务作用：复验 BOTH source 的全部精确路由计划 Runner 仍处于同一健康代次。
     *
     * @param stream Stream key
     * @param group  consumer group
     * @return 所有匹配计划都 started/healthy 时返回 true
     */
    boolean sourceHealthy(String stream, String group) {
        boolean found = false;
        for (StreamSubscriptionPlan plan : subscriptions.plans()) {
            if (plan.mode() != ConsumeMode.BOTH || !Objects.equals(group, plan.group())) continue;
            for (String topic : plan.topics()) {
                if (!stream.equals(topic)) continue;
                found = true;
                if (!subscriptions.isRunnerHealthy(plan.planId())) {
                    status.failReadiness("runner_unhealthy");
                    return false;
                }
            }
        }
        if (found) clearRunnerReadinessIfAllHealthy();
        return found;
    }

    /**
     * 业务作用：在 RedisPartition Claim 读取前复验其物理分区组内全部精确计划，防止 Runner 失去健康后继续扩大 PEL。
     *
     * @param plans 当前物理分区组已发布的 topic/event 计划
     * @return 至少存在一个计划且全部 Runner 健康时返回 true
     */
    boolean partitionSourceHealthy(Iterable<StreamSubscriptionPlan> plans) {
        boolean found = false;
        LinkedHashSet<Long> checked = new LinkedHashSet<>();
        for (StreamSubscriptionPlan plan : plans) {
            if (plan == null || !checked.add(plan.planId())) continue;
            found = true;
            if (!subscriptions.isRunnerHealthy(plan.planId())) {
                status.failReadiness("runner_unhealthy");
                return false;
            }
        }
        if (found) clearRunnerReadinessIfAllHealthy();
        return found;
    }

    /**
     * 业务作用：只有全部已发布计划都恢复健康时才撤销共享 readiness 原因，单个健康来源不能遮蔽其它失效 Runner。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值。
     */
    private void clearRunnerReadinessIfAllHealthy() {
        for (StreamSubscriptionPlan plan : subscriptions.plans()) {
            if (!subscriptions.isRunnerHealthy(plan.planId())) return;
        }
        status.clearReadiness("runner_unhealthy");
    }

    /**
     * 业务作用：报告 dispatcher 全局 admission 是否仍开放。参数说明: 无。返回: 未进入停机收口时为 true。
     */
    boolean admissionOpen() {
        return admission.get();
    }

    /**
     * 业务作用：为来源级 PEL 接管建立可排干责任，运行时关闭必须等已提交的 XAUTOCLAIM 循环明确退出。
     *
     * <p>参数说明: 无。
     *
     * @return 准入开放时返回只能释放一次的恢复排干令牌，关闭后返回 null 且不允许提交恢复任务。
     */
    StreamRuntimeStatus.DrainToken beginSourceRecovery() {
        return status.tryEnter(StreamRuntimeStatus.Resource.RECOVERY);
    }

    /**
     * 业务作用：在线性化边界内关闭所有新批次与重试，取消尚未取得 Partition 执行权的 Submission，随后才允许释放 Redis owner。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；重复调用保持幂等，已经运行的 listener 不被线程级中断。
     */
    void beginShutdown() {
        if (!shutdownStarted.compareAndSet(false, true)) return;
        // 先封闭全局与容量准入，再等待已开始的 Submission 发布，停机取消才能覆盖最后一笔合法提交。
        closeAdmission();
        retries.close();
        List<TaskTicket> cancelled = new ArrayList<>();
        submissionBarrier.writeLock().lock();
        try {
            for (TaskTicket ticket : List.copyOf(inFlightTickets.values())) {
                if (ticket.cancelIfNotRunning()) cancelled.add(ticket);
            }
        } finally {
            submissionBarrier.writeLock().unlock();
        }
        recordCancellations(cancelled);
        closeProxySources();
    }

    /**
     * 业务作用：立即封闭新的批次与任务准入，不等待当前调用栈持有的提交或排干责任。
     * <p>
     * 参数说明: 无。
     * 返回: 无返回值；已有任务与确认责任保留，取消和资源排干仍由 beginShutdown 与 close 执行。
     */
    void closeAdmission() {
        // 指标活动内的停机请求必须先阻止后续准入，随后才能把需要当前活动退栈的收口交给其它线程。
        admission.set(false);
        status.closeAdmission();
        recordCapacity.closeAdmission();
        dispatchCapacity.wakeAdmissionWaiters();
    }

    /**
     * 业务作用：在单个 Claim 主动停止后取消该来源已经排队但尚未运行的任务，运行中任务继续完成并由 fencing 决定 ACK。
     *
     * @param authority 即将停止新读取的来源权威
     *                  返回: 无返回值；与同步 submit 发布使用同一线性化锁。
     */
    void cancelPendingSubmissions(StreamSourceAuthority authority) {
        List<TaskTicket> cancelled = new ArrayList<>();
        submissionBarrier.writeLock().lock();
        try {
            for (TaskTicket ticket : List.copyOf(inFlightTickets.values())) {
                if (ticket.authority() == authority && ticket.cancelIfNotRunning()) cancelled.add(ticket);
            }
        } finally {
            submissionBarrier.writeLock().unlock();
        }
        recordCancellations(cancelled);
    }

    /**
     * 业务作用：在线性化注册边界内封闭单个来源的新恢复责任，并交回尚未开始执行的 retry/route 坐标。
     *
     * @param authority 已停止新读取但仍可完成既有 ACK 的来源权威
     *                  返回: 无返回值；正在执行的恢复责任保留到真实终态以继续约束 owner 排干。
     */
    void sourceStopping(StreamSourceAuthority authority) {
        retries.stopSource(authority);
    }

    /**
     * 业务作用：在提交屏障外通知已经成功的取消结果，使同步指标扩展可以安全请求停机或复验来源排干。
     *
     * @param cancelled 在写锁内确认取消成功的 Task 所有权快照
     *                  返回: 无返回值；运行中或已完成任务不产生取消计数。
     */
    private void recordCancellations(List<TaskTicket> cancelled) {
        // 指标后端可同步进入应用代码；取消状态已在锁内发布，通知不能继续持有停机与来源排干需要的门禁。
        for (TaskTicket ticket : cancelled) {
            redisProxy.streamPartitionMetrics().submission(
                    ticket.unit().plan(), ticket.unit().route().ordered(), "cancelled");
        }
    }

    /**
     * 业务作用：停止并移除全部 BOTH dedicated 来源，确保它们不再创建新读取。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；单个来源关闭失败只影响 readiness，其它来源仍继续收口。
     */
    private void closeProxySources() {
        List<ProxyBothStreamSource> sources;
        synchronized (this) {
            sources = List.copyOf(proxySources.values());
            proxySources.clear();
        }
        for (ProxyBothStreamSource source : sources) {
            try {
                source.close();
            } catch (Throwable failure) {
                status.failReadiness("proxy_source_close_failed");
            }
        }
    }

    /**
     * 业务作用：将来源保护态发布为 readiness 失败，原始异常只进诊断日志而不作指标标签。
     *
     * @param source  已暂停来源
     * @param failure 触发保护态的原因
     *                返回: 无返回值。
     */
    void sourcePaused(PartitionSource source, Throwable failure) {
        status.failReadiness("source_paused");
        dispatchCapacity.wakeAdmissionWaiters();
    }

    /**
     * 业务作用：把整批无法分类状态发布为稳定 readiness 原因，不暴露业务 key 或异常文本。参数说明: 无。返回: 无返回值。
     */
    void sourceRouteBlocked() {
        status.failReadiness("route_blocked");
    }

    /**
     * 业务作用：汇总各来源尚未明确的接管 I/O，单一来源完成不能清除其它来源的保护原因。
     *
     * @param authority 当前来源权威
     * @param uncertain 是否仍有不确定接管责任
     *                  返回: 有任一来源未收敛时 readiness 保持关闭，全部收敛后只撤销接管 I/O 原因。
     */
    void recoveryUncertain(StreamSourceAuthority authority, boolean uncertain) {
        synchronized (uncertainRecoveries) {
            // 失权后迟到的 I/O 结果不能重新发布旧来源的临时恢复原因。
            if (uncertain && authority.isActive() && admission.get()) uncertainRecoveries.add(authority);
            else uncertainRecoveries.remove(authority);
            // 所有来源均已收口才撤销公共原因，单个来源成功不能提前开放全局 readiness。
            if (uncertainRecoveries.isEmpty()) status.clearReadiness("recovery_transport");
            else status.failReadiness("recovery_transport");
        }
    }

    /**
     * 业务作用：在来源停止或失权时撤销其 ordered gate、精确重试及接管 I/O 临时状态，后续 owner 从 PEL 重建。
     *
     * @param authority 即将失效的共享来源对象
     *                  返回: 无返回值；其它来源的状态保持不变。
     */
    void authorityLost(StreamSourceAuthority authority) {
        orderedKeys.invalidateAuthority(authority);
        retries.invalidateAuthority(authority);
        // 旧权威已经终止，临时 I/O 原因不能污染后续来源；独立的停止和暂停原因仍约束 readiness。
        recoveryUncertain(authority, false);
    }

    /**
     * 业务作用：清除已失效 BOTH consumer epoch 的多 field 成功证据。参数说明: consumer name。返回: 无返回值。
     */
    void proxyConsumerLost(String consumer) {
        proxyLedger.invalidateConsumer(consumer);
    }

    /**
     * 业务作用：合并排干、硬容量、ordered gate、重试和确认注册表的低基数运行快照。
     *
     * <p>参数说明: 无。
     *
     * @return 不携带业务 key、record id 或异常文本的固定名称数值
     */
    Map<String, Long> metrics() {
        LinkedHashMap<String, Long> metrics = new LinkedHashMap<>(status.snapshot());
        metrics.putAll(recordCapacity.snapshot());
        metrics.putAll(dispatchCapacity.snapshot());
        metrics.putAll(orderedKeys.snapshot());
        metrics.putAll(retries.snapshot());
        metrics.putAll(proxyLedger.snapshot());
        metrics.put("pending_commit_attempts", commits.pendingAttempts());
        return Map.copyOf(metrics);
    }

    /**
     * 业务作用：返回已发布订阅计划快照供指标桥接补登记。参数说明: 无。返回: 不可变计划列表。
     */
    List<StreamSubscriptionPlan> plans() {
        return subscriptions.plans();
    }

    /**
     * 业务作用：在线性化提交边界内复验指定来源的 Task、重试、路由恢复和确认责任均已收口。
     *
     * @param authority 来源共享权威对象
     * @return 本来源不再持有任何可能产生业务或确认副作用的责任时返回 true
     */
    boolean sourceDrained(StreamSourceAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        submissionBarrier.writeLock().lock();
        try {
            boolean tasksDrained = inFlightTickets.values().stream()
                    .noneMatch(ticket -> ticket.authority() == authority);
            return tasksDrained && retries.isDrained(authority) && commits.isDrained(authority);
        } finally {
            submissionBarrier.writeLock().unlock();
        }
    }

    /**
     * 业务作用：把 RedisPartition 单 field raw batch 完整解析、提交、等待并建立 holder-fenced 确认决策。
     *
     * @param source   当前 Claim 的权威与确认入口
     * @param rawBatch poll barrier 持有的原始记录
     *                 返回: 无返回值；任一未成功坐标保留在 PEL 并以聚合异常告警。
     */
    void dispatchPartition(PartitionSource source, List<MapRecord<String, Object, Object>> rawBatch) {
        dispatch(source, rawBatch, false, false, false);
    }

    /**
     * 业务作用：以 holder 恢复准入接续 XAUTOCLAIM 取得的物理分区页，并为未知路由建立整批恢复责任。
     *
     * @param source   当前 Claim 的权威与确认入口
     * @param rawBatch 接管取得且尚未登记恢复责任的原始页
     *                 返回: 无返回值；失败坐标由精确或整批恢复继续驱动，停止及失权仍拒绝投递。
     */
    void dispatchPartitionClaimed(PartitionSource source, List<MapRecord<String, Object, Object>> rawBatch) {
        dispatch(source, rawBatch, false, false, true);
    }

    /**
     * 业务作用：把 BOTH 普通 Stream 的完整多 field record 送入共享计划、ledger 和 consumer-fenced 确认链。
     *
     * @param source   dedicated consumer epoch 来源
     * @param rawBatch 新读取取得的原始批次
     *                 返回: 无返回值；单 record 的任一 field 失败都不确认该 record。
     */
    void dispatchProxy(PartitionSource source, List<MapRecord<String, Object, Object>> rawBatch) {
        dispatch(source, rawBatch, true, false, false);
    }

    /**
     * 业务作用：接续周期 XAUTOCLAIM 返回的多 field record，在取得执行权后复验其当前 PEL 归属。
     *
     * @param source   当前 dedicated consumer epoch 来源
     * @param rawBatch 接管取得、尚未复验终态的原始批次
     *                 返回: 无返回值；已有执行或确认责任的 record 保留给原驱动力，过期正文不重新执行。
     */
    void dispatchProxyClaimed(PartitionSource source, List<MapRecord<String, Object, Object>> rawBatch) {
        dispatch(source, rawBatch, true, false, true);
    }

    /**
     * 业务作用：在 PEL 精确重试时读取正文，再复验来源实际权威与本地代次，只有证据有效时才重新提交业务。
     *
     * @param source 重试坐标当前来源
     * @param ref    exact record/field 坐标
     * @return 已交给确认链、已迁移或仍需退避的结论；UNKNOWN、停止与代次变化保留原重试责任。
     */
    RetryDisposition retryExact(PartitionSource source, PartitionRecordRef ref) {
        StreamSourceAuthority.Snapshot authority = source.authority().snapshot();
        StreamCommitCoordinator.PendingDisposition pending = source.pending(ref.id());
        if (pending == StreamCommitCoordinator.PendingDisposition.ABSENT) {
            // PEL 已无此 record，先撤销它的全部本地责任，后继才可继续使用账本容量与有序 key。
            settleRemoteRecord(ref);
            return observedRecovery(source, "exact", RetryDisposition.SETTLED);
        }
        if (pending == StreamCommitCoordinator.PendingDisposition.MOVED) {
            // 新 consumer 接管后只清理旧来源的证据，不能代替新 owner 确认消息。
            settleRemoteRecord(ref);
            return observedRecovery(source, "exact", RetryDisposition.MOVED);
        }
        if (pending == StreamCommitCoordinator.PendingDisposition.UNKNOWN) {
            // 缺少所有权证据时继续保留账本、门禁与重试，不能把观察失败当成远端完成。
            return observedRecovery(source, "exact", RetryDisposition.RETAINED);
        }
        MapRecord<String, Object, Object> raw = source.exact(ref.id());
        if (raw == null) {
            status.recordPelTombstone();
            Map<String, StreamCommitCoordinator.AckDisposition> result = source.ack(List.of(ref.id()), false);
            StreamCommitCoordinator.AckDisposition disposition = result.get(ref.id());
            if (disposition == StreamCommitCoordinator.AckDisposition.CONFIRMED) {
                settleRemoteRecord(ref);
                return observedRecovery(source, "pel_tombstone", RetryDisposition.SETTLED);
            }
            if (disposition == StreamCommitCoordinator.AckDisposition.MOVED
                    || disposition == StreamCommitCoordinator.AckDisposition.LOST_AUTHORITY) {
                // 远端已明确本地不再负责该坐标，必须先收敛 gate，随后 retry 才能安全注销唯一驱动力。
                settleRemoteRecord(ref);
                return observedRecovery(source, "pel_tombstone", RetryDisposition.MOVED);
            }
            return observedRecovery(source, "pel_tombstone", RetryDisposition.RETAINED);
        }
        // 正文往返可能跨越远端移交；失败回调的重试同样不能凭读取前或定期自检的持有证据再次执行业务。
        if (!recoveryAllowedAfterRead(source, authority, source::revalidateRecoveryAuthority)) {
            return observedRecovery(source, "exact", RetryDisposition.RETAINED);
        }
        try {
            dispatch(source, List.of(raw), source.sourceKind() == StreamRecordSource.PROXY_BOTH, true, true);
            return observedRecovery(source, "exact", RetryDisposition.SETTLED);
        } catch (PartitionDispatchException retained) {
            return observedRecovery(source, "exact", RetryDisposition.RETAINED);
        }
    }

    /**
     * 业务作用：按原批次顺序复验 PEL 与正文，取得读取后的来源实际权威证据后才接续整批业务与新读取。
     *
     * @param source 当前 holder 或 consumer epoch
     * @param refs   routeBlocked 时登记的完整批次坐标
     * @return 已接续、已迁移或仍需保留整批的结论
     */
    RetryDisposition retryRouteBatch(PartitionSource source, List<PartitionRecordRef> refs) {
        return retryRouteBatch(source, refs, source::revalidateRecoveryAuthority);
    }

    /**
     * 业务作用：在整页 PEL 与正文读取完成后复验来源权威，避免把读取前的持有证据用于迟到正文的业务提交。
     *
     * @param source 当前 holder 或 consumer epoch
     * @param refs   原始顺序的完整批次坐标
     * @param fence  正文读取后的权威门禁；物理分区补扫等待真实 holder 证据，UNKNOWN 期间保留当前页
     * @return 已接续或已迁移的结论；证据未决、停止或代次失效时保留原恢复责任。
     */
    RetryDisposition retryRouteBatch(PartitionSource source, List<PartitionRecordRef> refs, BooleanSupplier fence) {
        StreamSourceAuthority.Snapshot authority = source.authority().snapshot();
        List<MapRecord<String, Object, Object>> rawBatch = new ArrayList<>(refs.size());
        boolean observedMoved = false;
        for (PartitionRecordRef ref : refs) {
            StreamCommitCoordinator.PendingDisposition pending = source.pending(ref.id());
            // 整批只有明确结束的 record 才能释放本地责任，未决成员必须继续保留路由恢复。
            if (pending == StreamCommitCoordinator.PendingDisposition.UNKNOWN) return RetryDisposition.RETAINED;
            if (pending == StreamCommitCoordinator.PendingDisposition.ABSENT) {
                settleRemoteRecord(ref);
                continue;
            }
            if (pending == StreamCommitCoordinator.PendingDisposition.MOVED) {
                settleRemoteRecord(ref);
                observedMoved = true;
                continue;
            }
            MapRecord<String, Object, Object> raw = source.exact(ref.id());
            if (raw == null) {
                status.recordPelTombstone();
                Map<String, StreamCommitCoordinator.AckDisposition> result = source.ack(List.of(ref.id()), false);
                StreamCommitCoordinator.AckDisposition disposition = result.get(ref.id());
                // 缺失或不明确的确认结果不能解除顺序与账本责任，必须保留原恢复驱动力。
                if (disposition != StreamCommitCoordinator.AckDisposition.CONFIRMED
                        && disposition != StreamCommitCoordinator.AckDisposition.MOVED
                        && disposition != StreamCommitCoordinator.AckDisposition.LOST_AUTHORITY) {
                    return RetryDisposition.RETAINED;
                }
                if (disposition == StreamCommitCoordinator.AckDisposition.MOVED
                        || disposition == StreamCommitCoordinator.AckDisposition.LOST_AUTHORITY) {
                    observedMoved = true;
                }
                settleRemoteRecord(ref);
                continue;
            }
            rawBatch.add(raw);
        }
        if (rawBatch.isEmpty()) {
            return observedRecovery(source, "route",
                    observedMoved ? RetryDisposition.MOVED : RetryDisposition.SETTLED);
        }
        // XPENDING、XRANGE 往返期间远端可能已经移交；必须在完整正文之后取得证据，不能等业务完成后的 ACK 才发现。
        // 门禁等待期间保留当前 rawBatch 与调用方的容量、恢复责任；迟到的持有响应也不能覆盖停止或代次变化。
        if (!recoveryAllowedAfterRead(source, authority, fence)) {
            return observedRecovery(source, "route", RetryDisposition.RETAINED);
        }
        try {
            dispatch(source, rawBatch, source.sourceKind() == StreamRecordSource.PROXY_BOTH, true, true);
            return observedRecovery(source, "route", RetryDisposition.SETTLED);
        } catch (PartitionDispatchException retained) {
            return observedRecovery(source, "route",
                    retained.routeBlocked() ? RetryDisposition.RETAINED : RetryDisposition.SETTLED);
        }
    }

    /**
     * 业务作用：统一约束精确重试、整批重试与补扫的正文后提交边界，远端证据不得覆盖本地停止或代次变化。
     *
     * @param source    原恢复责任所属来源
     * @param authority 正文读取前冻结的来源代次
     * @param fence     来源实际权威复验；UNKNOWN 时拒绝本次提交，由原恢复驱动力等待或退避
     * @return 远端证据和查询前后的本地准入均有效时为 true；否则不建立新的业务责任。
     */
    private boolean recoveryAllowedAfterRead(PartitionSource source, StreamSourceAuthority.Snapshot authority,
                                             BooleanSupplier fence) {
        // 先拒绝已经失效的正文，再查询实际权威；迟到的成功响应也必须受同一代次与停止状态约束。
        return admission.get() && authority.allowsExecution() && source.allowsRecovery()
                && fence.getAsBoolean()
                && admission.get() && authority.allowsExecution() && source.allowsRecovery();
    }

    /**
     * 业务作用：在远端终态明确后收敛本地 record 责任，使唯一重试驱动力退出前归还账本容量并释放有序门禁。
     *
     * @param ref 已确认不存在、迁移或本地失权的冻结 Redis 坐标
     *            返回: 无返回值；BOTH 清理整条物理 record，其它来源仅清理 exact field。
     */
    private void settleRemoteRecord(PartitionRecordRef ref) {
        if (ref.source() == StreamRecordSource.PROXY_BOTH) {
            // BOTH 的所有 field 共用一条 PEL 与 ledger；先归还该代次容量，再开放它阻挡的后继。
            proxyLedger.recordSettled(ref);
            orderedKeys.proxyRecordSettled(ref);
        } else {
            orderedKeys.recordSettled(ref);
        }
    }

    /**
     * 业务作用：发布固定低基数恢复结果并原样返回状态，避免每个分支拼接动态标签。
     *
     * @param source      当前来源
     * @param type        exact、route 或 pel_tombstone
     * @param disposition 恢复结论
     * @return 输入结论
     */
    private RetryDisposition observedRecovery(PartitionSource source,
                                              String type,
                                              RetryDisposition disposition) {
        redisProxy.streamPartitionMetrics().recovery(
                source.sourceKind(), type, disposition.name().toLowerCase(java.util.Locale.ROOT));
        return disposition;
    }

    /**
     * 业务作用：执行两种来源共用的批次所有权流程，历史 PEL 证据不确定或解析失败时保留整批恢复责任。
     *
     * @param source     Redis 来源
     * @param rawBatch   原始批次
     * @param proxy      true 表示 BOTH 多 field record
     * @param recovery   是否已有精确或整批重试责任，已有责任不能被本次尝试重复登记或提前注销
     * @param historical 正文是否来自历史 PEL，采用恢复准入，并在取得 record 执行权后重新核对 PEL
     *                   返回: 无返回值；批次资源只在 Task/ACK/PEL 决策完成后释放。
     */
    private void dispatch(PartitionSource source,
                          List<MapRecord<String, Object, Object>> rawBatch,
                          boolean proxy,
                          boolean recovery,
                          boolean historical) {
        if (rawBatch == null || rawBatch.isEmpty()) return;
        // 接管取得的页属于恢复准入，但尚无重试责任；准入与责任是否已登记必须分别判断。
        StreamSourceAuthority.Snapshot authority = source.authority().snapshot();
        boolean recoveryAdmission = recovery || historical;
        boolean sourceAllowed = recoveryAdmission ? source.allowsRecovery() : source.allowsAdmission();
        if (!admission.get() || !sourceAllowed) {
            throw new IllegalStateException("Stream Partition dispatcher admission is closed");
        }
        // 来源检查之后仍可能并发停机，批次责任必须与全局关闭在同一门禁内裁决。
        StreamRuntimeStatus.DrainToken batchDrain = status.tryEnter(StreamRuntimeStatus.Resource.BATCH);
        if (batchDrain == null) {
            throw new IllegalStateException("Stream Partition dispatcher admission is closed");
        }
        status.recordBatch();
        redisProxy.streamPartitionMetrics().batch(
                source.sourceKind(), "records", recovery, rawBatch.size());
        List<RecycleLinkedMap<String, Object>> pooledPassthrough = new ArrayList<>();
        Preparation preparation = null;
        ProxyPreparation proxyPreparation = null;
        ProxyRecordAckLedger.ExecutionBatch execution = null;
        boolean retainedAdmission = false;
        List<Throwable> failures = new ArrayList<>();
        try {
            List<PartitionRecordRef> refs = unresolvedCoordinates(source, rawBatch);
            // 所有来源先取得逐 record 执行权；句柄覆盖 Task 处理及重试或确认责任的交接，避免重复发布窗口。
            execution = proxyLedger.tryBeginExecution(refs);
            List<MapRecord<String, Object, Object>> admitted = new ArrayList<>(rawBatch.size());
            for (int index = 0; index < refs.size(); index++) {
                PartitionRecordRef ref = refs.get(index);
                if (!execution.owns(ref) || commits.ownsConfirmation(source, ref)) {
                    retainedAdmission = true;
                    continue;
                }
                if (historical) {
                    // 正文可能早于原 Task 的 ACK 取回；互斥权内再查询 PEL，已结束的 record 不得重新发布。
                    StreamCommitCoordinator.PendingDisposition pending;
                    try {
                        pending = source.pending(ref.id());
                    } catch (RuntimeException unavailable) {
                        if (!StreamPendingRecovery.isTransient(unavailable)) throw unavailable;
                        pending = StreamCommitCoordinator.PendingDisposition.UNKNOWN;
                    }
                    if (pending == StreamCommitCoordinator.PendingDisposition.UNKNOWN) {
                        // 未分类前序可能与后继同 key；保留完整页及 raw 容量，不能先发布本页其它 Task。
                        retainHistoricalBatch(source, refs, recovery);
                        throw new PartitionDispatchException("Stream batch PEL 所有权暂不可确认", List.of(), true);
                    }
                    if (pending != StreamCommitCoordinator.PendingDisposition.OWNED) {
                        settleRemoteRecord(ref);
                        continue;
                    }
                }
                admitted.add(rawBatch.get(index));
            }
            rawBatch = List.copyOf(admitted);
            if (rawBatch.isEmpty()) {
                if (retainedAdmission) throw new PartitionDispatchException(
                        "Stream record 等待已有执行或确认责任", List.of(), true);
                return;
            }
            // PEL 往返也可能跨越停止或实际 holder 移交；最终证据有效前不解析或发布任何业务。
            if (historical && !recoveryAllowedAfterRead(source, authority, source::revalidateRecoveryAuthority)) {
                retainHistoricalBatch(source, refs, recovery);
                throw new PartitionDispatchException("Stream batch 恢复权威暂不可确认", List.of(), true);
            }
            try {
                if (proxy) {
                    proxyPreparation = prepareProxyBatch(source, rawBatch, pooledPassthrough);
                    preparation = proxyPreparation.provisional();
                } else {
                    preparation = preparePartitionBatch(source, rawBatch, pooledPassthrough);
                }
            } catch (Throwable routeFailure) {
                if (routeFailure instanceof Error fatal) throw fatal;
                if (!recovery) {
                    PartitionRecordCapacity.Permit permit = source.retainActiveBatch();
                    retries.registerRouteBlocked(source, unresolvedCoordinates(source, rawBatch), permit);
                }
                throw new PartitionDispatchException("Stream batch route blocked", List.of(routeFailure), true);
            }

            int taskDemand = taskDemand(preparation.prepared());
            try (PartitionDispatchCapacity.Reservation capacity = dispatchCapacity.acquire(
                    taskDemand, rawBatch.size(),
                    () -> admission.get() && (recoveryAdmission ? source.allowsRecovery() : source.allowsAdmission()))) {
                if (proxy) {
                    // 组合容量先于全局 ledger 发布，准备失败不会占住无 Task 负责的 record 条目。
                    preparation = publishProxyLedgers(proxyPreparation);
                }
                List<StreamDispatchUnit> units = buildUnits(preparation.prepared());
                redisProxy.streamPartitionMetrics().batch(
                        source.sourceKind(), "tasks", recovery, units.size());
                for (StreamDispatchUnit unit : units) {
                    if (unit.route().ordered()) {
                        redisProxy.streamPartitionMetrics().orderedBucket(
                                unit.plan(), unit.records().size());
                    }
                }
                capacity.releaseUnusedTasks(taskDemand - units.size());
                SubmissionBatch submitted = submitUnits(source, units, failures, recoveryAdmission);
                capacity.releaseUnusedTasks(submitted.blockedUnits());
                for (DeferredUnit deferred : submitted.deferred()) {
                    for (PartitionRecordRef ref : deferred.unit().refs()) {
                        retries.register(source, ref, deferred.unit().route().ordered());
                    }
                }
                if (recovery && !submitted.deferred().isEmpty()) {
                    // 当前精确重试仍受前序 gate 阻挡时不能发布成功结论，原 RetryState 必须继续持有时间轮驱动力。
                    failures.add(new IllegalStateException("ordered key remains deferred during exact recovery"));
                }
                awaitOutcomes(submitted.tickets());
                settle(source, preparation, submitted.tickets(), capacity, failures);
            } catch (PartitionDispatchCapacity.OversizedBatchException oversized) {
                failures.add(oversized);
                status.failReadiness("dispatch_capacity_oversized");
                source.pause(oversized);
            }
            if (!failures.isEmpty()) {
                for (Throwable failure : failures) if (failure instanceof Error fatal) throw fatal;
                throw new PartitionDispatchException("Stream Partition batch 存在保留 PEL 的坐标", failures, retainedAdmission);
            }
            if (retainedAdmission) {
                // 尚未接续的 record 不能让整批路由恢复提前注销；已经完成的成员由各自确认链收口。
                throw new PartitionDispatchException("Stream record 等待已有执行或确认责任", List.of(), true);
            }
        } catch (PartitionDispatchException retained) {
            throw retained;
        } catch (Throwable infrastructureFailure) {
            if (infrastructureFailure instanceof Error fatal) throw fatal;
            // 未归类基础设施异常不能让同一来源继续越过当前 PEL；停止后由新 owner 重新建立完整责任。
            source.pause(infrastructureFailure);
            throw new PartitionDispatchException(
                    "Stream Partition batch 基础设施收口失败", List.of(infrastructureFailure), false);
        } finally {
            if (preparation != null) {
                for (PreparedRecord record : preparation.prepared()) record.route().releaseObjectKeyAfterSubmit();
            }
            for (RecycleLinkedMap<String, Object> passthrough : pooledPassthrough) passthrough.recycle();
            if (execution != null) execution.close();
            batchDrain.close();
        }
    }

    /**
     * 业务作用：把尚未建立恢复责任的历史页整体交给路由重试，证据明确前阻止后继读取越过该页。
     *
     * @param source   当前消费来源
     * @param refs     当前完整页的冻结坐标，包括仍由其它执行或确认责任持有的成员
     * @param recovery 当前调用是否已经由原重试状态持有完整责任
     *                 返回: 无返回值；已有责任继续退避，新页交接 raw 容量；停止或失权由重试协调器交回 PEL。
     */
    private void retainHistoricalBatch(PartitionSource source, List<PartitionRecordRef> refs, boolean recovery) {
        if (!recovery) {
            // 先交接完整页再释放本次执行权，UNKNOWN 不得留下已被接管却无人驱动的记录。
            PartitionRecordCapacity.Permit permit = source.retainActiveBatch();
            retries.registerRouteBlocked(source, refs, permit);
        }
    }

    /**
     * 业务作用：完整解析 RedisPartition 批次，任一 record 无法安全路由时不发布整批任何 Task。
     *
     * @param source            物理 Claim 来源
     * @param rawBatch          原始批次
     * @param pooledPassthrough 批次末统一回收的 passthrough
     * @return 全部记录已冻结计划和路由的准备结果
     */
    private Preparation preparePartitionBatch(PartitionSource source,
                                              List<MapRecord<String, Object, Object>> rawBatch,
                                              List<RecycleLinkedMap<String, Object>> pooledPassthrough) {
        List<PreparedRecord> prepared = new ArrayList<>(rawBatch.size());
        for (MapRecord<String, Object, Object> raw : rawBatch) {
            prepared.add(preparePartitionRecord(source, raw, pooledPassthrough));
        }
        return new Preparation(List.copyOf(prepared), Map.of(), Map.of());
    }

    /**
     * 业务作用：解析一条 RedisPartition record 的包装、精确计划、泛型数据和本地 Partition 路由。
     *
     * @param source            物理 Claim 来源
     * @param raw               原始 MapRecord
     * @param pooledPassthrough 批次统一回收列表
     * @return 单 field 准备记录
     */
    private PreparedRecord preparePartitionRecord(PartitionSource source,
                                                  MapRecord<String, Object, Object> raw,
                                                  List<RecycleLinkedMap<String, Object>> pooledPassthrough) {
        String id = raw.getId().getValue();
        Object wrapped = raw.getValue().get(RedisPartition.DATA_FIELD);
        DecodedEnvelope envelope = decodeEnvelope(wrapped, null, null, raw.getStream(), id, pooledPassthrough);
        StreamSubscriptionPlan plan = subscriptions.partitionPlan(envelope.topic(), envelope.event());
        if (plan == null) {
            throw new IllegalStateException("Redis Partition 没有精确 listener route topic="
                    + envelope.topic() + " event=" + envelope.event() + " id=" + id);
        }
        ensurePlanHealthy(plan);
        Object data = deserializeForPlan(plan, envelope.data());
        PartitionRecordRef ref = new PartitionRecordRef(
                raw.getStream(), source.group(), source.consumer(), id,
                envelope.topic(), envelope.event(), RedisPartition.DATA_FIELD, StreamRecordSource.REDIS_PARTITION,
                source.authority(), source.authority().snapshot().generation());
        DecodedPartitionRecord decoded = new DecodedPartitionRecord(redisProxy, ref, data, envelope.passthrough());
        return new PreparedRecord(plan, decoded, resolvePartitionKey(plan, data), null);
    }

    /**
     * 业务作用：先完整校验每条 BOTH record 的 field 数、路由、包装与 autoDelete 合同，再创建 ledger 和 Task 记录。
     *
     * @param source            dedicated consumer epoch 来源
     * @param rawBatch          原始多 field 批次
     * @param pooledPassthrough 批次统一回收列表
     * @return 未成功 field 准备记录与物理 record ledger 关联
     */
    private ProxyPreparation prepareProxyBatch(PartitionSource source,
                                               List<MapRecord<String, Object, Object>> rawBatch,
                                               List<RecycleLinkedMap<String, Object>> pooledPassthrough) {
        List<ProxyParsedRecord> parsedRecords = new ArrayList<>(rawBatch.size());
        for (MapRecord<String, Object, Object> raw : rawBatch) {
            int fields = raw.getValue().size();
            if (fields < 1 || fields > maxProxyFieldsPerRecord) {
                throw new IllegalArgumentException("BOTH record field count exceeds configured limit stream="
                        + raw.getStream() + " id=" + raw.getId().getValue() + " fields=" + fields);
            }
            List<ProxyParsedField> parsedFields = new ArrayList<>(fields);
            Boolean autoDelete = null;
            for (Map.Entry<Object, Object> entry : raw.getValue().entrySet()) {
                String field = Objects.toString(entry.getKey(), null);
                if (StringUtils.isBlank(field)) throw new IllegalArgumentException("BOTH record contains blank field");
                StreamSubscriptionPlan plan = subscriptions.proxyPlan(raw.getStream(), source.group(), field);
                if (plan == null) {
                    throw new IllegalStateException("BOTH dedicated group has no exact route stream="
                            + raw.getStream() + " group=" + source.group() + " field=" + field);
                }
                ensurePlanHealthy(plan);
                if (autoDelete != null && autoDelete != plan.autoDelete()) {
                    throw new IllegalStateException("BOTH fields in one physical record must share autoDelete policy");
                }
                autoDelete = plan.autoDelete();
                DecodedEnvelope envelope = decodeEnvelope(
                        entry.getValue(), raw.getStream(), field,
                        raw.getStream(), raw.getId().getValue(), pooledPassthrough);
                Object data = deserializeForPlan(plan, envelope.data());
                PartitionRecordRef ref = new PartitionRecordRef(
                        raw.getStream(), source.group(), source.consumer(), raw.getId().getValue(),
                        raw.getStream(), field, field, StreamRecordSource.PROXY_BOTH,
                        source.authority(), source.authority().snapshot().generation());
                parsedFields.add(new ProxyParsedField(
                        field, plan, data, envelope.passthrough(), resolvePartitionKey(plan, data), ref));
            }
            parsedRecords.add(new ProxyParsedRecord(raw, List.copyOf(parsedFields), Boolean.TRUE.equals(autoDelete)));
        }
        List<PreparedRecord> provisional = new ArrayList<>();
        for (ProxyParsedRecord record : parsedRecords) {
            for (ProxyParsedField field : record.fields()) {
                DecodedPartitionRecord decoded = new DecodedPartitionRecord(
                        redisProxy, field.ref(), field.data(), field.passthrough());
                provisional.add(new PreparedRecord(field.plan(), decoded, field.route(), null));
            }
        }
        return new ProxyPreparation(
                List.copyOf(parsedRecords),
                new Preparation(List.copyOf(provisional), Map.of(), Map.of()));
    }

    /**
     * 业务作用：在整批组合容量已经取得后一次发布全部 BOTH record ledger，并排除同 epoch 已成功的 field。
     *
     * @param preparation 完整解析但尚未发布 ledger 的批次
     * @return 与全局 ledger 建立精确关联的实际 Task 输入
     */
    private Preparation publishProxyLedgers(ProxyPreparation preparation) {
        List<ProxyRecordAckLedger.OpenRequest> requests = new ArrayList<>(preparation.records().size());
        for (ProxyParsedRecord record : preparation.records()) {
            requests.add(new ProxyRecordAckLedger.OpenRequest(
                    record.fields().getFirst().ref(),
                    record.fields().stream().map(ProxyParsedField::field).toList()));
        }

        LinkedHashMap<RecordFieldKey, PreparedRecord> provisionalByField = new LinkedHashMap<>();
        for (PreparedRecord record : preparation.provisional().prepared()) {
            provisionalByField.put(RecordFieldKey.of(record.decoded().ref()), record);
        }
        LinkedHashMap<RecordFieldKey, ProxyRecordAckLedger.Entry> ledgerByField = new LinkedHashMap<>();
        LinkedHashMap<ProxyRecordAckLedger.Entry, Boolean> ledgerPolicies = new LinkedHashMap<>();
        List<PreparedRecord> prepared = new ArrayList<>();
        try (ProxyRecordAckLedger.OpenBatch opened = proxyLedger.openBatch(requests)) {
            for (int index = 0; index < preparation.records().size(); index++) {
                ProxyParsedRecord record = preparation.records().get(index);
                ProxyRecordAckLedger.BatchEntry openedRecord = opened.entries().get(index);
                ProxyRecordAckLedger.Entry ledger = openedRecord.entry();
                Set<String> execute = Set.copyOf(openedRecord.fieldsToExecute());
                ledgerPolicies.put(ledger, record.autoDelete());
                for (ProxyParsedField field : record.fields()) {
                    RecordFieldKey key = RecordFieldKey.of(field.ref());
                    PreparedRecord provisional = provisionalByField.get(key);
                    if (provisional == null) {
                        throw new IllegalStateException("BOTH field has no provisional record");
                    }
                    ledgerByField.put(key, ledger);
                    if (execute.contains(field.field())) {
                        prepared.add(new PreparedRecord(
                                provisional.plan(), provisional.decoded(), provisional.route(), ledger));
                    } else {
                        provisional.route().releaseObjectKeyAfterSubmit();
                    }
                }
            }
            Preparation published = new Preparation(
                    List.copyOf(prepared), Map.copyOf(ledgerByField), Map.copyOf(ledgerPolicies));
            opened.commit();
            return published;
        }
    }

    /**
     * 业务作用：拆解 Partition 或 Proxy publish 包装，同时把池化 passthrough 交给批次唯一回收者。
     *
     * @param wrapped           原始 hash value
     * @param proxyTopic        BOTH 路径的 Stream key；物理分区为 null
     * @param proxyEvent        BOTH 路径的 hash field；物理分区为 null
     * @param stream            诊断 Stream key
     * @param id                诊断 record id
     * @param pooledPassthrough 批次回收列表
     * @return topic/event/data/passthrough 解码结果
     */
    @SuppressWarnings("rawtypes")
    private DecodedEnvelope decodeEnvelope(Object wrapped,
                                           String proxyTopic,
                                           String proxyEvent,
                                           String stream,
                                           String id,
                                           List<RecycleLinkedMap<String, Object>> pooledPassthrough) {
        String topic;
        String event;
        Object data;
        Map<String, Object> passthrough;
        if (wrapped instanceof PooledEvtData message) {
            topic = proxyTopic == null ? message.getTopic() : proxyTopic;
            event = proxyEvent == null ? message.getEvent() : proxyEvent;
            data = message.getData();
            passthrough = message.getPassthrough();
            message.recycle();
        } else if (wrapped instanceof Map<?, ?> message) {
            topic = proxyTopic == null ? MapUtils.getString(message, PooledEvtData.FIELD_TOPIC) : proxyTopic;
            event = proxyEvent == null ? MapUtils.getString(message, PooledEvtData.FIELD_EVENT) : proxyEvent;
            data = message.get(PooledEvtData.FIELD_DATA);
            passthrough = MapUtils.getObject(message, PooledEvtData.FIELD_PASSTHROUGH);
        } else {
            throw new IllegalArgumentException("Stream record value type is invalid stream=" + stream + " id=" + id);
        }
        if (passthrough instanceof RecycleLinkedMap pooled) pooledPassthrough.add(pooled);
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(event) || data == null) {
            throw new IllegalArgumentException("Stream poison record stream=" + stream + " id=" + id);
        }
        return new DecodedEnvelope(topic, event, data, passthrough);
    }

    /**
     * 业务作用：按计划的泛型声明对未启用类型信息的消息执行一次精确反序列化。
     *
     * @param plan 冻结的订阅计划
     * @param data 包装中的数据
     * @return listener 应接收的数据对象
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object deserializeForPlan(StreamSubscriptionPlan plan, Object data) {
        TypeReference reference = plan.listener().paramType();
        if (reference != null && !redisProxy.isActivateDefaultTyping()) {
            return ObjMprUtils.deserialize(ObjMprUtils.toString(data), reference);
        }
        return data;
    }

    /**
     * 业务作用：调用计划 partitionKey 并记录耗时、异常和粗类型，具体 key 与类名不进入指标。
     *
     * @param plan 当前订阅计划
     * @param data 已反序列化业务数据
     * @return 按真实 Partition 入口冻结的路由
     */
    private ResolvedPartitionKey resolvePartitionKey(StreamSubscriptionPlan plan, Object data) {
        long startedAt = System.nanoTime();
        try {
            ResolvedPartitionKey route = plan.resolvePartitionKey(data);
            String kind = switch (route.kind()) {
                case UNORDERED -> "null";
                case LONG -> "number";
                case STRING -> "string";
                case OBJECT -> "object";
            };
            redisProxy.streamPartitionMetrics().partitionKey(
                    plan, kind, "success", System.nanoTime() - startedAt);
            return route;
        } catch (Throwable failure) {
            redisProxy.streamPartitionMetrics().partitionKey(
                    plan, "unknown", "failure", System.nanoTime() - startedAt);
            throw failure;
        }
    }

    /**
     * 业务作用：复验计划 Runner 仍健康，不自动切换执行域。参数说明: 计划。返回: 无返回值；不健康时拒绝路由。
     */
    private void ensurePlanHealthy(StreamSubscriptionPlan plan) {
        if (!plan.runner().isStarted() || !plan.runner().isHealthy()) {
            status.failReadiness("runner_unhealthy");
            throw new IllegalStateException("PartitionRunner 不健康: " + plan.runner().getRunnerName());
        }
    }

    /**
     * 业务作用：把 ordered 的同 plan/有效 hash 记录按 Redis 遇见顺序合并，null key 保持一条一个非保序 Task。
     *
     * @param prepared 已完成整条解析的记录
     * @return 用于 gate 与 Partition submit 的执行单元
     */
    private List<StreamDispatchUnit> buildUnits(List<PreparedRecord> prepared) {
        Map<UnitKey, List<PreparedRecord>> buckets = new LinkedHashMap<>();
        long unorderedSequence = 0L;
        for (PreparedRecord record : prepared) {
            ResolvedPartitionKey route = record.route();
            UnitKey key = route.ordered()
                    ? new UnitKey(record.plan().planId(), route.effectiveHash(), 0L)
                    : new UnitKey(record.plan().planId(), 0, ++unorderedSequence);
            buckets.computeIfAbsent(key, ignored -> new ArrayList<>()).add(record);
        }
        List<StreamDispatchUnit> units = new ArrayList<>(buckets.size());
        for (List<PreparedRecord> bucket : buckets.values()) {
            PreparedRecord first = bucket.getFirst();
            List<DecodedPartitionRecord> records = bucket.stream().map(PreparedRecord::decoded).toList();
            for (int index = 1; index < bucket.size(); index++) bucket.get(index).route().releaseObjectKeyAfterSubmit();
            units.add(new StreamDispatchUnit(first.plan(), first.route(), records));
        }
        return List.copyOf(units);
    }

    /**
     * 业务作用：在不释放对象路由 key 的前提下计算整批最大 Task 需求，供 ledger 发布前取得组合容量。
     *
     * @param prepared 已完成路由解析的全部 field
     * @return ordered bucket 与逐条 unordered Task 的总数
     */
    private int taskDemand(List<PreparedRecord> prepared) {
        LinkedHashSet<UnitKey> units = new LinkedHashSet<>();
        long unorderedSequence = 0L;
        for (PreparedRecord record : prepared) {
            ResolvedPartitionKey route = record.route();
            units.add(route.ordered()
                    ? new UnitKey(record.plan().planId(), route.effectiveHash(), 0L)
                    : new UnitKey(record.plan().planId(), 0, ++unorderedSequence));
        }
        return units.size();
    }

    /**
     * 业务作用：取得 ordered gate 并在提交屏障内原子发布 Submission，屏障外通知结果；未取得 gate 的坐标保留 deferred 责任。
     *
     * @param source   当前 Redis 来源
     * @param units    待提交执行单元
     * @param failures 批次基础设施失败列表
     * @param recovery 是否按来源恢复准入规则复验本批执行单元
     * @return 已提交 ticket、blocked 数及 deferred 单元
     */
    private SubmissionBatch submitUnits(PartitionSource source,
                                        List<StreamDispatchUnit> units,
                                        List<Throwable> failures,
                                        boolean recovery) {
        List<TaskTicket> tickets = new ArrayList<>(units.size());
        List<DeferredUnit> deferred = new ArrayList<>();
        int blocked = 0;
        for (StreamDispatchUnit unit : units) {
            StreamSourceAuthority.Snapshot authority = source.authority().snapshot();
            OrderedKeyCoordinator.GateToken token = null;
            if (unit.route().ordered()) {
                try {
                    OrderedKeyCoordinator.GateReservation gate = orderedKeys.reserve(
                            unit.plan().planId(), unit.route().effectiveHash(), authority, unit.refs());
                    if (!gate.acquired()) {
                        blocked++;
                        if (gate.authorityStale()) {
                            failures.add(new IllegalStateException("Partition source authority is stale"));
                        } else {
                            deferred.add(new DeferredUnit(unit));
                        }
                        unit.route().releaseObjectKeyAfterSubmit();
                        continue;
                    }
                    token = gate.token();
                } catch (OrderedKeyCoordinator.OrderedGateCapacityException capacity) {
                    failures.add(capacity);
                    status.failReadiness("ordered_gate_capacity");
                    source.pause(capacity);
                    blocked++;
                    deferred.add(new DeferredUnit(unit));
                    unit.route().releaseObjectKeyAfterSubmit();
                    continue;
                }
            }
            CompletableFuture<ConsumeTaskOutcome> future = new CompletableFuture<>();
            PartitionStreamConsumeTask task = new PartitionStreamConsumeTask(
                    unit, authority, orderedKeys, token, future, status, redisProxy);
            Partition.Submission submission = null;
            StreamRuntimeStatus.DrainToken taskDrain = status.enter(StreamRuntimeStatus.Resource.TASK);
            long ticketId = ticketIds.incrementAndGet();
            if (ticketId <= 0L) {
                taskDrain.close();
                throw new IllegalStateException("Partition task ticket id exhausted");
            }
            TaskTicket ticket = new TaskTicket(
                    ticketId, source.authority(), unit, token, future, taskDrain);
            inFlightTickets.put(ticketId, ticket);
            try {
                submissionBarrier.readLock().lock();
                try {
                    // 准入复验与句柄发布必须共用读锁，停机写锁只能在完整提交之后取消尚未运行的任务。
                    if (!admission.get()
                            || !(recovery ? source.allowsRecovery() : source.allowsAdmission())) {
                        throw new IllegalStateException("Partition source admission closed");
                    }
                    submission = submitByKey(unit.plan().runner(), unit.route(), task);
                    ticket.publishSubmission(submission);
                } finally {
                    submissionBarrier.readLock().unlock();
                }
                // 同步指标回调可能停止当前代理；必须先释放读锁，避免回调申请停机写锁时等待自身退栈。
                redisProxy.streamPartitionMetrics().submission(
                        unit.plan(), unit.route().ordered(), "accepted");
                if (submission == null && !future.isDone()) {
                    throw new IllegalStateException("Partition submit returned null before task terminal outcome");
                }
            } catch (Throwable submitFailure) {
                failures.add(submitFailure);
                if (task.hasNeverBeenOwned()) {
                    redisProxy.streamPartitionMetrics().submission(
                            unit.plan(), unit.route().ordered(), "rejected");
                    task.definitelyNotSubmitted(submitFailure);
                } else source.pause(submitFailure);
            }
            tickets.add(ticket);
        }
        status.recordTasks(tickets.size());
        return new SubmissionBatch(List.copyOf(tickets), blocked, List.copyOf(deferred));
    }

    /**
     * 业务作用：在专用虚拟等待线程上等待全部 Task 真实终态，中断不得提前回收消息数据。
     *
     * @param tickets 已发布 Task ticket
     *                返回: 无返回值；观察到中断时在完整收口后恢复标志。
     */
    private void awaitOutcomes(List<TaskTicket> tickets) {
        CompletableFuture<?> all = CompletableFuture.allOf(
                tickets.stream().map(TaskTicket::future).toArray(CompletableFuture[]::new));
        boolean interrupted = false;
        while (!all.isDone()) {
            try {
                all.get(100L, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException ignored) {
            } catch (InterruptedException signal) {
                interrupted = true;
            } catch (java.util.concurrent.ExecutionException exceptional) {
                break;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    /**
     * 业务作用：合并 Task outcome、BOTH ledger 与 exact gate 依赖，在 Redis I/O 前发布 CommitAttempt 所有权。
     *
     * @param source      当前 Redis 来源
     * @param preparation 准备记录与 ledger 关联
     * @param tickets     已取得真实终态的 ticket
     * @param capacity    批次组合容量
     * @param failures    需要保留 PEL 的原因列表
     *                    返回: 无返回值；UNKNOWN 由 CommitAttempt 在批次外继续持有。
     */
    private void settle(PartitionSource source,
                        Preparation preparation,
                        List<TaskTicket> tickets,
                        PartitionDispatchCapacity.Reservation capacity,
                        List<Throwable> failures) {
        LinkedHashMap<String, MutableCommitRecord> candidates = new LinkedHashMap<>();
        LinkedHashSet<ProxyRecordAckLedger.Entry> touchedLedgers =
                new LinkedHashSet<>(preparation.ledgerPolicies().keySet());
        try {
            for (TaskTicket ticket : tickets) {
                ConsumeTaskOutcome outcome = ticket.future().join();
                if (!ticket.authority().isActive()) status.recordLateTaskOutcome();
                if (source.sourceKind() == StreamRecordSource.PROXY_BOTH) {
                    mergeProxyOutcome(preparation, ticket, outcome);
                } else {
                    for (PartitionRecordRef ref : outcome.successfulPrefix()) {
                        MutableCommitRecord candidate = candidates.computeIfAbsent(
                                ref.id(), ignored -> new MutableCommitRecord(ref.id(), ticket.unit().plan().autoDelete()));
                        candidate.addGate(ticket.gateToken());
                    }
                }
                if (outcome.status() != ConsumeStatus.SUCCESS) {
                    List<PartitionRecordRef> retained = new ArrayList<>();
                    if (outcome.failed() != null) retained.add(outcome.failed());
                    retained.addAll(outcome.deferredTail());
                    if (retained.isEmpty()) retained.addAll(ticket.unit().refs());
                    for (PartitionRecordRef ref : retained)
                        retries.register(source, ref, ticket.unit().route().ordered());
                    failures.add(outcome.cause() == null
                            ? new IllegalStateException("Partition Task did not succeed: " + outcome.status())
                            : outcome.cause());
                }
            }
            if (source.sourceKind() == StreamRecordSource.PROXY_BOTH) {
                for (ProxyRecordAckLedger.Entry ledger : touchedLedgers) {
                    if (!proxyLedger.ackReady(ledger)) continue;
                    List<OrderedKeyCoordinator.GateToken> gates = proxyLedger.gates(ledger);
                    String id = ledgerRecordId(preparation, ledger);
                    for (OrderedKeyCoordinator.GateToken gate : gates) orderedKeys.proxyAckReady(gate, id);
                    MutableCommitRecord candidate = new MutableCommitRecord(
                            id, preparation.ledgerPolicies().getOrDefault(ledger, false));
                    for (OrderedKeyCoordinator.GateToken gate : gates) candidate.addGate(gate);
                    candidate.confirmed(() -> proxyLedger.confirmed(ledger));
                    candidate.moved(() -> proxyLedger.moved(ledger));
                    candidates.put(id, candidate);
                }
            }
            if (candidates.isEmpty()) {
                capacity.releaseCommitCapacity();
            } else {
                capacity.shrinkCommitRecords(candidates.size());
                PartitionDispatchCapacity.CommitLease lease = capacity.transferCommitCapacity(candidates.size());
                commits.commit(source, candidates.values().stream().map(MutableCommitRecord::freeze).toList(), lease);
            }
        } finally {
            for (TaskTicket ticket : tickets) {
                try {
                    ticket.close();
                } finally {
                    inFlightTickets.remove(ticket.id(), ticket);
                }
            }
        }
    }

    /**
     * 业务作用：把 BOTH Task 的成功前缀与未成功尾部发布到同 consumer epoch ledger，不在此阶段产生 record ACK。
     *
     * @param preparation 批次 ledger 关联
     * @param ticket      当前 Task ticket
     * @param outcome     Task 真实结果
     *                    返回: 无返回值。
     */
    private void mergeProxyOutcome(Preparation preparation, TaskTicket ticket, ConsumeTaskOutcome outcome) {
        boolean gateDecisionAccepted = true;
        if (ticket.gateToken() != null) {
            List<PartitionRecordRef> failed = new ArrayList<>();
            if (outcome.failed() != null) failed.add(outcome.failed());
            failed.addAll(outcome.deferredTail());
            gateDecisionAccepted = orderedKeys.proxyTaskOutcome(
                    ticket.gateToken(), outcome.successfulPrefix(), List.copyOf(failed));
        }
        Set<PartitionRecordRef> successes = Set.copyOf(outcome.successfulPrefix());
        for (PartitionRecordRef ref : ticket.unit().refs()) {
            ProxyRecordAckLedger.Entry ledger = preparation.ledgerByField().get(RecordFieldKey.of(ref));
            if (ledger == null) throw new IllegalStateException("BOTH field has no proxy ledger entry");
            if (successes.contains(ref)) proxyLedger.succeeded(ledger, ref.field(), ticket.gateToken());
            else if (gateDecisionAccepted) proxyLedger.failed(ledger, ref.field(), ticket.gateToken());
        }
    }

    /**
     * 业务作用：从批次 field 关联中取得物理 record id。参数说明: 准备结果与 ledger。返回: 对应 record id。
     */
    private String ledgerRecordId(Preparation preparation, ProxyRecordAckLedger.Entry ledger) {
        for (Map.Entry<RecordFieldKey, ProxyRecordAckLedger.Entry> entry : preparation.ledgerByField().entrySet()) {
            if (entry.getValue() == ledger) return entry.getKey().id();
        }
        throw new IllegalStateException("proxy ledger entry has no record coordinate");
    }

    /**
     * 业务作用：按 partitionKey 的真实类型选择 Partition submit 重载，并在同步路由后释放原始对象 key。
     *
     * @param runner 计划冻结的 PartitionRunner
     * @param route  已解析的真实路由入口
     * @param task   等待真实终态的 Task
     * @return Partition 稳定 Submission 句柄
     */
    private Partition.Submission submitByKey(Partition.PartitionRunner runner,
                                             ResolvedPartitionKey route,
                                             PartitionStreamConsumeTask task) {
        try {
            if (!route.ordered()) return runner.submit((Object) null, task);
            if (route.kind() == ResolvedPartitionKey.Kind.LONG) return runner.submit(route.longKey(), task);
            route.verifyStableHashBeforeSubmit();
            return runner.submit(route.objectKeyForSubmit(), task);
        } finally {
            route.releaseObjectKeyAfterSubmit();
        }
    }

    /**
     * 业务作用：在整批解析失败时为每个已进 PEL 的物理 record 构造不含消息体的保留坐标。
     *
     * @param source   当前 Redis 来源
     * @param rawBatch 原始批次
     * @return 保持 Redis 遇见顺序的坐标列表
     */
    private List<PartitionRecordRef> unresolvedCoordinates(
            PartitionSource source, List<MapRecord<String, Object, Object>> rawBatch) {
        List<PartitionRecordRef> result = new ArrayList<>(rawBatch.size());
        for (MapRecord<String, Object, Object> raw : rawBatch) {
            String field = raw.getValue().isEmpty()
                    ? "<empty>" : Objects.toString(raw.getValue().keySet().iterator().next(), "<unknown>");
            result.add(new PartitionRecordRef(
                    raw.getStream(), source.group(), source.consumer(), raw.getId().getValue(),
                    "<unresolved>", "<unresolved>", field, source.sourceKind(),
                    source.authority(), source.authority().snapshot().generation()));
        }
        return List.copyOf(result);
    }

    /**
     * 业务作用：先关闭所有 poll/admission，再收口重试与 CommitAttempt，排干后最后关闭虚拟等待执行域。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；预算耗尽时保留在途责任并延后关闭执行器，调用方须用 shutdownComplete 复验真实终态；重复调用保持幂等。
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        beginShutdown();
        commits.closeAndDrain(drainTimeoutMillis);
        boolean drained = status.awaitDrained(drainTimeoutMillis);
        if (!drained) {
            // 预算耗尽不能中断仍持有 Task 数据或 Redis I/O 的 waiter；责任实际归零后才封闭执行器。
            status.runWhenDrained(waitExecutor::shutdown);
            return;
        }
        waitExecutor.shutdown();
        try {
            if (!waitExecutor.awaitTermination(drainTimeoutMillis, TimeUnit.MILLISECONDS)) {
                // 计数归零后任务仍可能处于退出清理，诊断只说明本轮等待结束，终态必须继续读取执行器的实际状态。
                status.failReadiness("wait_executor_drain_timeout");
            }
        } catch (InterruptedException signal) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 业务作用：区分开始关闭与真正终止，防止代理在 ACK、恢复调用或等待任务仍活动时解除停机责任。
     * <p>
     * 参数说明: 无。
     *
     * @return 已开始关闭、专用执行器实际终止且全部排干责任归零时返回 true。
     */
    boolean shutdownComplete() {
        return closed.get() && waitExecutor.isTerminated() && status.isDrained();
    }

    enum RetryDisposition {SETTLED, MOVED, RETAINED}

    private record PreparedRecord(StreamSubscriptionPlan plan, DecodedPartitionRecord decoded,
                                  ResolvedPartitionKey route, ProxyRecordAckLedger.Entry ledger) {
    }

    private record UnitKey(long planId, int effectiveHash, long unorderedSequence) {
    }

    /**
     * 业务作用：在线性化提交、停机取消、Future 收口和 Submission 回收之间保存一笔 Task 的唯一所有权。
     */
    private static final class TaskTicket implements AutoCloseable {
        private final long id;
        private final StreamSourceAuthority authority;
        private final StreamDispatchUnit unit;
        private final OrderedKeyCoordinator.GateToken gateToken;
        private final CompletableFuture<ConsumeTaskOutcome> future;
        private final StreamRuntimeStatus.DrainToken drain;
        private Partition.Submission submission;
        private boolean closed;

        /**
         * 业务作用：在调用 Partition submit 之前发布可见 ticket。参数说明: 标识、来源、单元、门禁、Future 与排干令牌。返回: 未绑定句柄的 ticket。
         */
        private TaskTicket(long id,
                           StreamSourceAuthority authority,
                           StreamDispatchUnit unit,
                           OrderedKeyCoordinator.GateToken gateToken,
                           CompletableFuture<ConsumeTaskOutcome> future,
                           StreamRuntimeStatus.DrainToken drain) {
            this.id = id;
            this.authority = authority;
            this.unit = unit;
            this.gateToken = gateToken;
            this.future = future;
            this.drain = drain;
        }

        /**
         * 业务作用：返回进程内 ticket 标识。参数说明: 无。返回: 唯一标识。
         */
        long id() {
            return id;
        }

        /**
         * 业务作用：返回提交来源权威。参数说明: 无。返回: 共享权威。
         */
        StreamSourceAuthority authority() {
            return authority;
        }

        /**
         * 业务作用：返回执行单元。参数说明: 无。返回: 批次单元。
         */
        StreamDispatchUnit unit() {
            return unit;
        }

        /**
         * 业务作用：返回 ordered gate。参数说明: 无。返回: unordered 时为 null。
         */
        OrderedKeyCoordinator.GateToken gateToken() {
            return gateToken;
        }

        /**
         * 业务作用：返回真实任务终态 Future。参数说明: 无。返回: 唯一 Future。
         */
        CompletableFuture<ConsumeTaskOutcome> future() {
            return future;
        }

        /**
         * 业务作用：在同步 submit 返回后发布稳定 Submission。参数说明: 提交句柄。返回: 无返回值。
         */
        synchronized void publishSubmission(Partition.Submission submission) {
            if (closed) throw new IllegalStateException("Task ticket already closed");
            this.submission = submission;
        }

        /**
         * 业务作用：停机时竞争取消尚未 RUNNING 的 Partition 任务。参数说明: 无。返回: 本次取消成功时为 true。
         */
        synchronized boolean cancelIfNotRunning() {
            if (closed || submission == null || future.isDone()) return false;
            try {
                return submission.cancel();
            } catch (IllegalStateException released) {
                return false;
            }
        }

        /**
         * 业务作用：Future 与确认决策完成后一次性释放 Submission 及 Task 排干令牌。参数说明: 无。返回: 无返回值。
         */
        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            try {
                if (submission != null) submission.recycle();
            } finally {
                submission = null;
                drain.close();
            }
        }
    }

    private record DeferredUnit(StreamDispatchUnit unit) {
    }

    private record SubmissionBatch(List<TaskTicket> tickets, int blockedUnits, List<DeferredUnit> deferred) {
    }

    private record Preparation(List<PreparedRecord> prepared,
                               Map<RecordFieldKey, ProxyRecordAckLedger.Entry> ledgerByField,
                               Map<ProxyRecordAckLedger.Entry, Boolean> ledgerPolicies) {
    }

    private record ProxyPreparation(List<ProxyParsedRecord> records, Preparation provisional) {
    }

    private record DecodedEnvelope(String topic, String event, Object data, Map<String, Object> passthrough) {
    }

    private record ProxyParsedField(String field, StreamSubscriptionPlan plan, Object data,
                                    Map<String, Object> passthrough, ResolvedPartitionKey route,
                                    PartitionRecordRef ref) {
    }

    private record ProxyParsedRecord(MapRecord<String, Object, Object> raw,
                                     List<ProxyParsedField> fields, boolean autoDelete) {
    }

    private record RecordFieldKey(String stream, String id, String field) {
        /**
         * 业务作用：从 exact Redis 坐标提取物理 record/field 账本键。参数说明: 消息坐标。返回: 三元键。
         */
        static RecordFieldKey of(PartitionRecordRef ref) {
            return new RecordFieldKey(ref.stream(), ref.id(), ref.field());
        }
    }

    private record ProxySourceKey(String stream, String group) {
    }

    /**
     * 业务作用：在 CommitAttempt 发布前合并同 record id 的 exact gate 与 ledger 回调。
     */
    private static final class MutableCommitRecord {
        private final String id;
        private final boolean autoDelete;
        private final LinkedHashSet<OrderedKeyCoordinator.GateToken> gates = new LinkedHashSet<>();
        private Runnable confirmed = () -> {
        };
        private Runnable moved = () -> {
        };

        /**
         * 业务作用：建立批次私有确认依赖。参数说明: id 与删除策略。返回: 可变构造器。
         */
        private MutableCommitRecord(String id, boolean autoDelete) {
            this.id = id;
            this.autoDelete = autoDelete;
        }

        /**
         * 业务作用：添加只属于本 record 的 exact gate。参数说明: gate 可为 null。返回: 无返回值。
         */
        void addGate(OrderedKeyCoordinator.GateToken gate) {
            if (gate != null) gates.add(gate);
        }

        /**
         * 业务作用：设置明确确认后的 ledger 回调。参数说明: 回调。返回: 无返回值。
         */
        void confirmed(Runnable action) {
            confirmed = action;
        }

        /**
         * 业务作用：设置明确迁移后的 ledger 回调。参数说明: 回调。返回: 无返回值。
         */
        void moved(Runnable action) {
            moved = action;
        }

        /**
         * 业务作用：冻结为可跨批次复验的 CommitRecord。参数说明: 无。返回: 不可变依赖。
         */
        StreamCommitCoordinator.CommitRecord freeze() {
            return new StreamCommitCoordinator.CommitRecord(id, List.copyOf(gates), confirmed, moved, autoDelete);
        }
    }

    /**
     * 业务作用：由 RedisPartition Claim 或 BOTH source 提供共享权威、精确 fencing ACK 与 PEL 复验入口。
     */
    interface PartitionSource extends StreamCommitCoordinator.CommitSource {
        /**
         * 业务作用：返回共享来源权威。参数说明: 无。返回: 来源权威。
         */
        @Override
        StreamSourceAuthority authority();

        /**
         * 业务作用：报告来源是否仍接受新提交。参数说明: 无。返回: admission 开放时为 true。
         */
        boolean allowsAdmission();

        /**
         * 业务作用：报告来源权威是否仍允许推进已经进入 PEL 的恢复责任。参数说明: 无。返回: 可安全恢复时为 true。
         */
        boolean allowsRecovery();

        /**
         * 业务作用：取得恢复提交所需的来源权威证据；物理分区覆盖此入口查询实际 Redis holder，BOTH 保留 consumer epoch 与逐 record PEL 复验。
         * 参数说明: 无。
         *
         * @return 明确允许恢复时为 true；UNKNOWN 返回 false 并保留权威，明确失权的来源同时撤销本地执行权威。
         */
        default boolean revalidateRecoveryAuthority() {
            return allowsRecovery();
        }

        /**
         * 业务作用：只阻断本来源的新读取并保留当前权威，使整批 route recovery 能按原顺序推进。参数说明: 失败原因。返回: 无返回值。
         */
        void blockRoute(Throwable failure);

        /**
         * 业务作用：在整批路由已经重建或交接后重新开放来源读取。参数说明: 无。返回: 无返回值。
         */
        void clearRouteBlock();

        /**
         * 业务作用：把当前 raw batch 的 record permit 转交给 route recovery。参数说明: 无。返回: 独占 Permit；没有活动批次时返回 null。
         */
        PartitionRecordCapacity.Permit retainActiveBatch();

        /**
         * 业务作用：暂停来源并关闭 readiness。参数说明: 失败原因。返回: 无返回值。
         */
        void pause(Throwable failure);

        /**
         * 业务作用：返回消息来源类型。参数说明: 无。返回: 来源类型。
         */
        StreamRecordSource sourceKind();

        /**
         * 业务作用：返回实际 Stream key。参数说明: 无。返回: Stream key。
         */
        String stream();

        /**
         * 业务作用：返回实际 consumer group。参数说明: 无。返回: group。
         */
        String group();

        /**
         * 业务作用：返回 holder 或 consumer epoch 标识。参数说明: 无。返回: 来源身份。
         */
        String consumer();

        /**
         * 业务作用：按 exact id 读取 Stream 正文供 PEL 重试。参数说明: record id。返回: 正文；缺失时为 null。
         */
        MapRecord<String, Object, Object> exact(String id);
    }

    /**
     * 业务作用：聚合同一 raw batch 内导致 PEL 保留的业务或基础设施原因。
     */
    static final class PartitionDispatchException extends RuntimeException {
        private final boolean routeBlocked;

        /**
         * 业务作用：创建批次保留结论。参数说明: message 为摘要，failures 为原因，routeBlocked 表示整批仍有成员尚未完成分类或执行责任交接。返回: 不丢弃未决成员的聚合异常。
         */
        PartitionDispatchException(String message, List<Throwable> failures, boolean routeBlocked) {
            super(message, failures.isEmpty() ? null : failures.getFirst());
            this.routeBlocked = routeBlocked;
            for (int index = 1; index < failures.size(); index++) addSuppressed(failures.get(index));
        }

        /**
         * 业务作用：判断整批恢复能否交给各 record 的后续驱动力。参数说明: 无。返回: 分类或执行责任交接尚未完成、必须保留整批恢复时为 true。
         */
        boolean routeBlocked() {
            return routeBlocked;
        }
    }
}
