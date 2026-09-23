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
 * 业务作用：承载单个 RedisProxy 的物理 Stream 来源、分域 dispatcher、硬容量、重试、确认复验与完整排干。
 */
final class StreamPartitionRuntime implements AutoCloseable {

    private final RedisProxy redisProxy;
    private volatile PartitionExecutionPlan executionPlan;
    private final StreamSubscriptionRegistry subscriptions;
    private final OrderedKeyCoordinator orderedKeys;
    private final StreamRuntimeStatus status = new StreamRuntimeStatus();
    private final PartitionRecordExecution recordExecution = new PartitionRecordExecution();
    private final ExecutorService waitExecutor;
    private final AtomicBoolean admission = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final AtomicLong ticketIds = new AtomicLong();
    private final ConcurrentHashMap<Long, TaskTicket> inFlightTickets = new ConcurrentHashMap<>();
    private final java.util.Set<StreamSourceAuthority> uncertainRecoveries = new java.util.HashSet<>();
    private final ReentrantReadWriteLock submissionBarrier = new ReentrantReadWriteLock(true);
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
        this.drainTimeoutMillis = redisProxy.getStream().getPartition().getDrainTimeoutMs();
        ThreadFactory factory = Thread.ofVirtual()
                .name("redis-partition-wait-" + redisProxy.getQualifier() + "-", 0)
                .factory();
        this.waitExecutor = Executors.newThreadPerTaskExecutor(factory);

    }

    /**
     * 业务作用：启动全部冻结域，来源开放由宿主在远端合同复验后统一完成。
     * @param plan 完整拓扑与固定份额
     * @param topicGroups 主题到逻辑组的映射
     * 返回: 无返回值；失败时来源保持关闭。
     */
    void activate(PartitionExecutionPlan plan, Map<String, String> topicGroups) {
        subscriptions.start(plan, topicGroups, this, orderedKeys, status);
        executionPlan = plan;
    }

    /**
     * 业务作用：把新 claim 永久绑定到冻结的物理来源域。
     * @param group 逻辑组
     * @param partition 物理编号
     * @return 不随持锁变化的域对象
     */
    PartitionExecutionDomain domain(String group, int partition) {
        return subscriptions.domains().get(executionPlan.domain(group, partition));
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
     * 业务作用：为一个 Redis 来源创建读取前 record permit 交接状态。
     *
     * @param sourceId 来源生命周期内稳定的对象身份
     * @param wake     容量归还时缩短 runner 等待的动作
     * @return 初始不持有 record permit 的来源状态
     */
    PartitionSourceRecordState newRecordState(PartitionSource sourceId, Runnable wake) {
        return new PartitionSourceRecordState(sourceId.authority().domain().records, sourceId, wake);
    }

    /**
     * 业务作用：在 RedisPartition Claim 读取前复验其物理分区组内全部精确计划，防止 Runner 失去健康后继续扩大 PEL。
     *
     * @param source 绑定固定执行域的物理来源
     * @param plans 当前物理分区组已发布的 topic/event 计划
     * @return 至少存在一个计划且全部 Runner 健康时返回 true
     */
    boolean partitionSourceHealthy(PartitionSource source, Iterable<StreamSubscriptionPlan> plans) {
        if (!source.authority().domain().healthy()) return false;
        boolean found = false;
        for (StreamSubscriptionPlan plan : plans) {
            plan.binding(source.authority().domain());
            found = true;
        }
        return found;
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
        // 只撤销尚未执行的恢复；在途 I/O 及原批次容量留在注册表，继续约束 Claim 的 holder 排干。
        for (var domain : subscriptions.domains()) domain.retries.close();
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
        for (var domain : subscriptions.domains()) {
            domain.records.closeAdmission();
            domain.dispatch.wakeAdmissionWaiters();
        }
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
        authority.domain().retries.stopSource(authority);
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
     * 业务作用：将来源保护态发布为 readiness 失败，原始异常只进诊断日志而不作指标标签。
     *
     * @param source  已暂停来源
     * @param failure 触发保护态的原因
     *                返回: 无返回值。
     */
    void sourcePaused(PartitionSource source, Throwable failure) {
        status.failReadiness("source_paused");
        source.authority().domain().dispatch.wakeAdmissionWaiters();
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
     * 业务作用：在来源失权时撤销其 ordered gate 和未执行恢复；在途调用保留原资源到返回，后续 owner 从 PEL 重建。
     *
     * @param authority 即将失效的共享来源对象
     *                  返回: 无返回值；其它来源的状态保持不变。
     */
    void authorityLost(StreamSourceAuthority authority) {
        orderedKeys.invalidateAuthority(authority);
        authority.domain().retries.invalidateAuthority(authority);
        // 旧权威已经终止，临时 I/O 原因不能污染后续来源；独立的停止和暂停原因仍约束 readiness。
        recoveryUncertain(authority, false);
    }

    /**
     * 业务作用：合并排干、硬容量、ordered gate、重试和确认注册表的低基数运行快照。
     *
     * <p>参数说明: 无。
     *
     * @return 不携带业务 key、record id 或异常文本的固定名称数值
     */
    Map<String, Long> metrics() {
        for (var domain : subscriptions.domains()) if (!shutdownStarted.get()) domain.healthy();
        LinkedHashMap<String, Long> metrics = new LinkedHashMap<>(status.snapshot());
        for (var domain : subscriptions.domains()) domain.usage().forEach((key, value) ->
                metrics.merge(key, value, key.contains("oldest") || key.contains("max_consecutive") ? Math::max : Long::sum));
        metrics.putAll(orderedKeys.snapshot());
        return Map.copyOf(metrics);
    }

    /**
     * 业务作用：把真实已提交 Task 与读取前预留区分，域快照不把空闲预留误报为业务执行。
     * @param domain 固定来源域
     * @return 当前仍有终态责任的 Task 数
     */
    long activeTasks(PartitionExecutionDomain domain) {
        return inFlightTickets.values().stream().filter(ticket -> ticket.authority().domain() == domain).count();
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
     * @return 本来源的 Task、恢复 I/O 与确认责任均结束且长期恢复资源已归还时返回 true
     */
    boolean sourceDrained(StreamSourceAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        submissionBarrier.writeLock().lock();
        try {
            boolean tasksDrained = inFlightTickets.values().stream()
                    .noneMatch(ticket -> ticket.authority() == authority);
            return tasksDrained && authority.domain().retries.isDrained(authority) && authority.domain().commits.isDrained(authority);
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
        dispatch(source, rawBatch, false, false);
    }

    /**
     * 业务作用：以 holder 恢复准入接续 XAUTOCLAIM 取得的物理分区页，并为未知路由建立整批恢复责任。
     *
     * @param source   当前 Claim 的权威与确认入口
     * @param rawBatch 接管取得且尚未登记恢复责任的原始页
     *                 返回: 无返回值；失败坐标由精确或整批恢复继续驱动，停止及失权仍拒绝投递。
     */
    void dispatchPartitionClaimed(PartitionSource source, List<MapRecord<String, Object, Object>> rawBatch) {
        dispatch(source, rawBatch, false, true);
    }

    /**
     * 业务作用：在 PEL 精确重试时读取正文，再复验来源实际权威与本地代次，只有证据有效时才重新提交业务。
     *
     * @param source 重试坐标当前来源
     * @param ref    exact record/field 坐标
     * @param authority 原重试责任冻结的快照
     * @return 已交给确认链、已迁移或仍需退避的结论；UNKNOWN、停止与代次变化保留原重试责任。
     */
    RetryDisposition retryExact(PartitionSource source, PartitionRecordRef ref, StreamSourceAuthority.Snapshot authority) {
        // 旧重试不能凭来源重新激活继续查询或提交，必须让合法代次重新取得正文。
        if (authority.current() != source.authority() || !authority.owns(ref)
                || !authority.allowsExecution()) return RetryDisposition.MOVED;
        // 停机只等待已经发出的 I/O；其返回后不能继续查询正文或发起缺正文 ACK。
        if (!admission.get() || !source.allowsRecovery()) return RetryDisposition.RETAINED;
        StreamCommitCoordinator.PendingDisposition pending = source.pending(ref.id());
        if (!authority.allowsExecution()) return RetryDisposition.MOVED;
        if (!admission.get() || !source.allowsRecovery()) return RetryDisposition.RETAINED;
        if (pending == StreamCommitCoordinator.PendingDisposition.ABSENT) {
            // PEL 已无此 record，先撤销它的全部本地责任，后继才可继续使用容量与有序 key。
            settleRemoteRecord(ref);
            return observedRecovery(source, "exact", RetryDisposition.SETTLED);
        }
        if (pending == StreamCommitCoordinator.PendingDisposition.MOVED) {
            // 新 consumer 接管后只清理旧来源的证据，不能代替新 owner 确认消息。
            settleRemoteRecord(ref);
            return observedRecovery(source, "exact", RetryDisposition.MOVED);
        }
        if (pending == StreamCommitCoordinator.PendingDisposition.UNKNOWN) {
            // 缺少所有权证据时继续保留门禁与重试，不能把观察失败当成远端完成。
            return observedRecovery(source, "exact", RetryDisposition.RETAINED);
        }
        MapRecord<String, Object, Object> raw = source.exact(ref.id());
        if (!authority.allowsExecution()) return RetryDisposition.MOVED;
        if (!admission.get() || !source.allowsRecovery()) return RetryDisposition.RETAINED;
        if (raw == null) {
            status.recordPelTombstone();
            Map<String, StreamCommitCoordinator.AckDisposition> result = source.ack(authority, List.of(ref.id()), false);
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
            dispatch(source, List.of(raw), true, true, null, authority);
            return observedRecovery(source, "exact", RetryDisposition.SETTLED);
        } catch (PartitionDispatchException retained) {
            return observedRecovery(source, "exact", RetryDisposition.RETAINED);
        }
    }

    /**
     * 业务作用：使用原批次组合份额复验并接续恢复，满额时不等待自身持有的 Task 许可。
     * @param source 原来源
     * @param refs 完整页坐标
     * @param fence holder 权威复验
     * @param read 原读取预留，可为空
     * @param authority 原批次或 PEL 补扫读取前冻结的权威
     * @return 保留、迁移或接续结论
     */
    RetryDisposition retryRouteBatch(PartitionSource source, List<PartitionRecordRef> refs, BooleanSupplier fence,
                                     PartitionReadReservation read, StreamSourceAuthority.Snapshot authority) {
        // 原批次身份必须覆盖全部坐标，恢复只在该代次内重读，不允许换发已有读取责任。
        if (authority == null || authority.current() != source.authority() || !authority.allowsExecution()
                || refs.stream().anyMatch(ref -> !authority.owns(ref))) return RetryDisposition.MOVED;
        List<MapRecord<String, Object, Object>> rawBatch = new ArrayList<>(refs.size());
        boolean observedMoved = false;
        for (PartitionRecordRef ref : refs) {
            // 每条记录及每次 I/O 返回后都复验停止，保留 holder 只用于排干而不授予新恢复动作。
            if (!authority.allowsExecution()) return RetryDisposition.MOVED;
            if (!admission.get() || !source.allowsRecovery()) return RetryDisposition.RETAINED;
            StreamCommitCoordinator.PendingDisposition pending = source.pending(ref.id());
            if (!authority.allowsExecution()) return RetryDisposition.MOVED;
            if (!admission.get() || !source.allowsRecovery()) return RetryDisposition.RETAINED;
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
            if (!authority.allowsExecution()) return RetryDisposition.MOVED;
            if (!admission.get() || !source.allowsRecovery()) return RetryDisposition.RETAINED;
            if (raw == null) {
                status.recordPelTombstone();
                Map<String, StreamCommitCoordinator.AckDisposition> result = source.ack(authority, List.of(ref.id()), false);
                StreamCommitCoordinator.AckDisposition disposition = result.get(ref.id());
                // 缺失或不明确的确认结果不能解除顺序责任，必须保留原恢复驱动力。
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
            dispatch(source, rawBatch, true, true, read, authority);
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
     * 业务作用：在远端终态明确或原代次失效后撤销该精确坐标的本地门禁，不改变其它代次与来源的顺序责任。
     *
     * @param ref 已确认不存在、迁移或本地失权的冻结 Redis 坐标
     *            返回: 无返回值；只清理当前来源代次的精确坐标。
     */
    void settleRemoteRecord(PartitionRecordRef ref) {
        orderedKeys.recordSettled(ref);
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
     * 业务作用：在原批次向后继步骤交接前拒绝缺失或失效权威，禁止为旧正文换发当前代次。
     * @param source 原始读取来源
     * @param authority 建立读取所有权时冻结的快照
     * 返回: 无返回值；不匹配或已失效时保留 PEL 并终止本批提交
     */
    private void requireBatchAuthority(PartitionSource source, StreamSourceAuthority.Snapshot authority) {
        // 身份证据缺失或过期时不能读取当前权威补齐，后继合法代次必须重新取得正文。
        if (authority == null || authority.current() != source.authority() || !authority.allowsExecution()) {
            throw new PartitionDispatchException("Stream batch authority is stale", List.of(), false);
        }
    }

    /**
     * 业务作用：执行物理分区的批次所有权流程，证据不确定、解析失败或恢复容量暂满时保留整批恢复责任。
     *
     * @param source     Redis 来源
     * @param rawBatch   原始批次
     * @param recovery   是否已有精确或整批重试责任，已有责任不能被本次尝试重复登记或提前注销
     * @param historical 正文是否来自历史 PEL，采用恢复准入，并在取得 record 执行权后重新核对 PEL
     *                   返回: 无返回值；批次资源只在 Task/ACK/PEL 决策完成后释放。
     */
    private void dispatch(PartitionSource source,
                          List<MapRecord<String, Object, Object>> rawBatch,
                          boolean recovery,
                          boolean historical) {
        dispatch(source, rawBatch, recovery, historical, source.readReservation(), source.readAuthority());
    }

    /**
     * 业务作用：按原域预留完成执行、确认和失败责任转移；读取后的任何拒绝都保留 PEL。
     * @param source 固定来源
     * @param rawBatch 实际正文
     * @param recovery 是否由原恢复状态驱动
     * @param historical 是否需要逐条 PEL 复验
     * @param read 原批次完整预留
     * @param authority 建立读取所有权时冻结的唯一快照
     * 返回: 无返回值；失败交回恢复责任。
     */
    private void dispatch(PartitionSource source, List<MapRecord<String, Object, Object>> rawBatch,
                          boolean recovery, boolean historical, PartitionReadReservation read,
                          StreamSourceAuthority.Snapshot authority) {
        if (rawBatch == null || rawBatch.isEmpty()) return;
        // 接管取得的页属于恢复准入，但尚无重试责任；准入与责任是否已登记必须分别判断。
        requireBatchAuthority(source, authority);
        boolean recoveryAdmission = recovery || historical;
        boolean sourceAllowed = recoveryAdmission ? source.allowsRecovery() : source.allowsAdmission();
        if (!admission.get() || !sourceAllowed || !authority.allowsExecution()) {
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
        PartitionRecordExecution.Batch execution = null;
        boolean retainedAdmission = false;
        List<Throwable> failures = new ArrayList<>();
        try {
            List<PartitionRecordRef> refs = unresolvedCoordinates(source, rawBatch, authority);
            requireBatchAuthority(source, authority);
            // 所有来源先取得逐 record 执行权；句柄覆盖 Task 处理及重试或确认责任的交接，避免重复发布窗口。
            execution = recordExecution.begin(refs);
            List<MapRecord<String, Object, Object>> admitted = new ArrayList<>(rawBatch.size());
            for (int index = 0; index < refs.size(); index++) {
                PartitionRecordRef ref = refs.get(index);
                if (!execution.owns(ref) || source.authority().domain().commits.ownsConfirmation(source, ref)) {
                    retainedAdmission = true;
                    continue;
                }
                if (historical) {
                    requireBatchAuthority(source, authority);
                    // 正文可能早于原 Task 的 ACK 取回；互斥权内再查询 PEL，已结束的 record 不得重新发布。
                    StreamCommitCoordinator.PendingDisposition pending;
                    try {
                        pending = source.pending(ref.id());
                    } catch (RuntimeException unavailable) {
                        if (!StreamPendingRecovery.isTransient(unavailable)) throw unavailable;
                        pending = StreamCommitCoordinator.PendingDisposition.UNKNOWN;
                    }
                    requireBatchAuthority(source, authority);
                    if (pending == StreamCommitCoordinator.PendingDisposition.UNKNOWN) {
                        // 未分类前序可能与后继同 key；保留完整页及 raw 容量，不能先发布本页其它 Task。
                        retainHistoricalBatch(source, refs, recovery, authority);
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
                retainHistoricalBatch(source, refs, recovery, authority);
                throw new PartitionDispatchException("Stream batch 恢复权威暂不可确认", List.of(), true);
            }
            try {
                preparation = preparePartitionBatch(source, rawBatch, pooledPassthrough, authority);
            } catch (Throwable routeFailure) {
                if (routeFailure instanceof Error fatal) throw fatal;
                if (!recovery) {
                    PartitionRecordCapacity.Permit permit = source.retainActiveBatch();
                    source.authority().domain().retries.registerRouteBlocked(source, unresolvedCoordinates(source, rawBatch, authority), permit, authority);
                }
                throw new PartitionDispatchException("Stream batch route blocked", List.of(routeFailure), true);
            }

            requireBatchAuthority(source, authority);
            int taskDemand = taskDemand(preparation.prepared());
            PartitionDispatchCapacity.Reservation prepaid = read == null ? null : read.take(taskDemand, rawBatch.size());
            if (prepaid == null && recovery) {
                // 不能占住唯一恢复执行槽等待另一保留批次的份额，否则原批次永远无法续接。
                prepaid = source.authority().domain().dispatch.tryReserve(taskDemand, rawBatch.size());
                if (prepaid == null) throw new PartitionDispatchException("recovery capacity unavailable", List.of(), true);
            }
            try (PartitionDispatchCapacity.Reservation capacity = prepaid != null ? prepaid : source.authority().domain().dispatch.acquire(
                    taskDemand, rawBatch.size(),
                    () -> admission.get() && authority.allowsExecution()
                            && (recoveryAdmission ? source.allowsRecovery() : source.allowsAdmission()))) {
                List<StreamDispatchUnit> units = buildUnits(preparation.prepared(), source.authority().domain());
                redisProxy.streamPartitionMetrics().batch(
                        source.sourceKind(), "tasks", recovery, units.size());
                for (StreamDispatchUnit unit : units) {
                    if (unit.route().ordered()) {
                        redisProxy.streamPartitionMetrics().orderedBucket(
                                unit.plan(), unit.records().size());
                    }
                }
                capacity.releaseUnusedTasks(taskDemand - units.size());
                SubmissionBatch submitted = submitUnits(source, units, failures, recoveryAdmission, authority);
                capacity.releaseUnusedTasks(submitted.blockedUnits());
                boolean capacityRetained = false;
                for (DeferredUnit deferred : submitted.deferred()) {
                    for (PartitionRecordRef ref : deferred.unit().refs()) {
                        if (!source.authority().domain().retries.register(source, ref, deferred.unit().route().ordered(), read, authority)) capacityRetained = true;
                    }
                }
                if (recovery && !submitted.deferred().isEmpty()) {
                    // 当前精确重试仍受前序 gate 阻挡时不能发布成功结论，原 RetryState 必须继续持有时间轮驱动力。
                    failures.add(new IllegalStateException("ordered key remains deferred during exact recovery"));
                }
                awaitOutcomes(submitted.tickets());
                capacityRetained |= settle(source, submitted.tickets(), capacity, failures, read);
                if (capacityRetained) {
                    // 已提交 Task 和 ACK 先完整交接，再保留整批坐标；退避不占 Task/确认配额，前序恢复才能释放容量。
                    retainHistoricalBatch(source, unresolvedCoordinates(source, rawBatch, authority), recovery, authority);
                    throw new PartitionDispatchException("Stream batch 等待恢复容量", failures, true);
                }
            } catch (PartitionDispatchCapacity.OversizedBatchException oversized) {
                failures.add(oversized);
                status.failReadiness("dispatch_capacity_oversized");
                // 旧批次的容量错误不能暂停后来取得的来源，当前代次仍有效时才关闭其读取。
                if (authority.allowsExecution()) source.pause(oversized);
            }
            if (!failures.isEmpty()) {
                for (Throwable failure : failures) if (failure instanceof Error fatal) throw fatal;
                throw new PartitionDispatchException("Stream Partition batch 存在保留 PEL 的坐标", failures, retainedAdmission);
            }
            if (retainedAdmission) {
                // 尚未接续的 record 不能让整批路由恢复提前注销；已经完成的成员由各自确认链收口。
                throw new PartitionDispatchException("Stream record 等待已有执行或确认责任", List.of(), true);
            }
        } catch (OrderedKeyCoordinator.OrderedGateCapacityException capacity) {
            // 顺序门禁在任何 Task 之前原子拒绝；保留当前 epoch 的成功证据及整批读取容量，等待前序恢复归还额度。
            retainHistoricalBatch(source, unresolvedCoordinates(source, rawBatch, authority), recovery, authority);
            throw new PartitionDispatchException("Stream batch 等待顺序门禁容量", List.of(capacity), true);
        } catch (PartitionDispatchException retained) {
            throw retained;
        } catch (Throwable infrastructureFailure) {
            if (infrastructureFailure instanceof Error fatal) throw fatal;
            // 未归类基础设施异常不能让同一来源继续越过当前 PEL；停止后由新 owner 重新建立完整责任。
            if (authority.allowsExecution()) source.pause(infrastructureFailure);
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
     * 业务作用：把尚未建立恢复责任的受阻批次整体交给重试，容量或证据恢复前阻止后继读取越过该批。
     *
     * @param source   当前消费来源
     * @param refs     当前完整页的冻结坐标，包括仍由其它执行或确认责任持有的成员
     * @param recovery 当前调用是否已经由原重试状态持有完整责任
     * @param authority 原读取的唯一权威快照
     *                 返回: 无返回值；已有责任继续退避，新批次交接 raw 容量；停止或失权由重试协调器交回 PEL。
     */
    private void retainHistoricalBatch(PartitionSource source, List<PartitionRecordRef> refs, boolean recovery,
                                       StreamSourceAuthority.Snapshot authority) {
        if (!recovery) {
            // 先交接完整批次再释放本次执行权，容量暂满或 UNKNOWN 都不得留下无人驱动的 PEL。
            PartitionRecordCapacity.Permit permit = source.retainActiveBatch();
            source.authority().domain().retries.registerRouteBlocked(source, refs, permit, authority);
        }
    }

    /**
     * 业务作用：完整解析 RedisPartition 批次，任一 record 无法安全路由时不发布整批任何 Task。
     *
     * @param source            物理 Claim 来源
     * @param rawBatch          原始批次
     * @param pooledPassthrough 批次末统一回收的 passthrough
     * @param authority 本批读取前冻结的身份与代次
     * @return 全部记录已冻结计划和路由的准备结果
     */
    private Preparation preparePartitionBatch(PartitionSource source,
                                              List<MapRecord<String, Object, Object>> rawBatch,
                                              List<RecycleLinkedMap<String, Object>> pooledPassthrough,
                                              StreamSourceAuthority.Snapshot authority) {
        List<PreparedRecord> prepared = new ArrayList<>(rawBatch.size());
        for (MapRecord<String, Object, Object> raw : rawBatch) {
            requireBatchAuthority(source, authority);
            prepared.add(preparePartitionRecord(source, raw, pooledPassthrough, authority));
        }
        return new Preparation(List.copyOf(prepared));
    }

    /**
     * 业务作用：解析一条 RedisPartition record 的包装、精确计划、泛型数据和本地 Partition 路由。
     *
     * @param source            物理 Claim 来源
     * @param raw               原始 MapRecord
     * @param pooledPassthrough 批次统一回收列表
     * @param authority 本批读取前冻结的身份与代次
     * @return 单 field 准备记录
     */
    private PreparedRecord preparePartitionRecord(PartitionSource source,
                                                  MapRecord<String, Object, Object> raw,
                                                  List<RecycleLinkedMap<String, Object>> pooledPassthrough,
                                                  StreamSourceAuthority.Snapshot authority) {
        String id = raw.getId().getValue();
        Object wrapped = raw.getValue().get(RedisPartition.DATA_FIELD);
        DecodedEnvelope envelope = decodeEnvelope(wrapped, raw.getStream(), id, pooledPassthrough);
        StreamSubscriptionPlan plan = subscriptions.partitionPlan(envelope.topic(), envelope.event());
        if (plan == null) {
            throw new IllegalStateException("Redis Partition 没有精确 listener route topic="
                    + envelope.topic() + " event=" + envelope.event() + " id=" + id);
        }
        ensurePlanHealthy(plan, source.authority().domain());
        Object data = deserializeForPlan(plan, envelope.data());
        PartitionRecordRef ref = new PartitionRecordRef(
                raw.getStream(), source.group(), source.consumer(), id,
                envelope.topic(), envelope.event(), RedisPartition.DATA_FIELD, StreamRecordSource.REDIS_PARTITION,
                authority.current(), authority.generation());
        DecodedPartitionRecord decoded = new DecodedPartitionRecord(redisProxy, ref, data, envelope.passthrough());
        return new PreparedRecord(plan, decoded, resolvePartitionKey(plan, data));
    }

    /**
     * 业务作用：拆解 Partition publish 包装，同时把池化 passthrough 交给批次唯一回收者。
     *
     * @param wrapped           原始 hash value
     * @param stream            诊断 Stream key
     * @param id                诊断 record id
     * @param pooledPassthrough 批次回收列表
     * @return topic/event/data/passthrough 解码结果
     */
    @SuppressWarnings("rawtypes")
    private DecodedEnvelope decodeEnvelope(Object wrapped,
                                           String stream,
                                           String id,
                                           List<RecycleLinkedMap<String, Object>> pooledPassthrough) {
        String topic;
        String event;
        Object data;
        Map<String, Object> passthrough;
        if (wrapped instanceof PooledEvtData message) {
            topic = message.getTopic();
            event = message.getEvent();
            data = message.getData();
            passthrough = message.getPassthrough();
            message.recycle();
        } else if (wrapped instanceof Map<?, ?> message) {
            topic = MapUtils.getString(message, PooledEvtData.FIELD_TOPIC);
            event = MapUtils.getString(message, PooledEvtData.FIELD_EVENT);
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
     * 业务作用：复验计划的原执行域仍健康，不自动切换 Runner。
     * @param plan 业务计划
     * @param domain 物理来源冻结的执行域
     * 返回: 无返回值；绑定不存在或执行域不健康时拒绝路由。
     */
    private void ensurePlanHealthy(StreamSubscriptionPlan plan, PartitionExecutionDomain domain) {
        plan.binding(domain);
        if (!domain.healthy()) throw new IllegalStateException("Partition execution domain unhealthy: " + domain.id());
    }

    /**
     * 业务作用：把 ordered 的同 plan/有效 hash 记录按 Redis 遇见顺序合并，null key 保持一条一个非保序 Task。
     *
     * @param prepared 已完成整条解析的记录
     * @param domain 物理来源冻结的执行域
     * @return 用于 gate 与 Partition submit 的执行单元
     */
    private List<StreamDispatchUnit> buildUnits(List<PreparedRecord> prepared, PartitionExecutionDomain domain) {
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
            units.add(new StreamDispatchUnit(first.plan(), first.plan().binding(domain), first.route(), records));
        }
        return List.copyOf(units);
    }

    /**
     * 业务作用：在不释放对象路由 key 的前提下计算整批最大 Task 需求，供 任务发布前取得组合容量。
     *
     * @param prepared 已完成路由解析的全部物理记录
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
     * 业务作用：先整批预留 ordered gate，再于提交屏障内发布 Submission；屏障外通知结果，受阻坐标保留 deferred 责任。
     *
     * @param source   当前 Redis 来源
     * @param units    待提交执行单元
     * @param failures 批次基础设施失败列表
     * @param recovery 是否按来源恢复准入规则复验本批执行单元
     * @param authority 本批读取前冻结且贯穿解析的唯一快照
     * @return 已提交 ticket、blocked 数及 deferred 单元
     */
    private SubmissionBatch submitUnits(PartitionSource source,
                                        List<StreamDispatchUnit> units,
                                        List<Throwable> failures,
                                        boolean recovery, StreamSourceAuthority.Snapshot authority) {
        requireBatchAuthority(source, authority);
        // 一致性必须在任一 gate 或 Task 发布前成立，不能等业务成功之后才拒绝确认。
        if (units.stream().flatMap(unit -> unit.refs().stream()).anyMatch(ref -> !authority.owns(ref))) {
            throw new PartitionDispatchException("Stream batch record authority mismatch", List.of(), false);
        }
        List<TaskTicket> tickets = new ArrayList<>(units.size());
        List<DeferredUnit> deferred = new ArrayList<>();
        int blocked = 0;
        // Task/确认配额与整批 gate 先于业务提交；单元间发生容量拒绝时不留下半批执行或半批门禁。
        var reservations = orderedKeys.reserveBatch(
                units.stream().filter(unit -> unit.route().ordered()).toList(), authority).iterator();
        for (StreamDispatchUnit unit : units) {
            OrderedKeyCoordinator.GateToken token = null;
            if (unit.route().ordered()) {
                OrderedKeyCoordinator.GateReservation gate = reservations.next();
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
                    ticketId, authority, unit, token, future, taskDrain);
            inFlightTickets.put(ticketId, ticket);
            try {
                submissionBarrier.readLock().lock();
                try {
                    // 准入复验与句柄发布必须共用读锁，停机写锁只能在完整提交之后取消尚未运行的任务。
                    if (!admission.get()
                            || !(recovery ? source.allowsRecovery() : source.allowsAdmission())
                            || !authority.allowsExecution()) {
                        throw new IllegalStateException("Partition source admission closed");
                    }
                    submission = submitByKey(unit.binding().domain().runner, unit.route(), task);
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
                } else if (authority.allowsExecution()) {
                    // 不确定提交仍由原代次承担；来源已重获时不能用旧提交异常暂停新读取。
                    source.pause(submitFailure);
                }
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
     * 业务作用：合并 Task outcome 与 exact gate 依赖，在 Redis I/O 前发布 CommitAttempt 所有权。
     *
     * @param source      当前 Redis 来源
     * @param tickets     已取得真实终态的 ticket
     * @param capacity    批次组合容量
     * @param failures    需要保留 PEL 的原因列表
     * @param read        原读取的失败承接预留；source 模式可为空
     * @return 存在未取得精确重试额度的坐标时返回 true，调用方须保留整批；UNKNOWN 由 CommitAttempt 在批次外继续持有
     */
    private boolean settle(PartitionSource source,
                        List<TaskTicket> tickets,
                        PartitionDispatchCapacity.Reservation capacity,
                        List<Throwable> failures, PartitionReadReservation read) {
        LinkedHashMap<String, MutableCommitRecord> candidates = new LinkedHashMap<>();
        boolean capacityRetained = false;
        try {
            for (TaskTicket ticket : tickets) {
                ConsumeTaskOutcome outcome = ticket.future().join();
                if (!ticket.authoritySnapshot().allowsExecution()) status.recordLateTaskOutcome();
                for (PartitionRecordRef ref : outcome.successfulPrefix()) {
                    // 成功事实只能携带业务执行时的权威，交接期间重获来源不能替旧结果取得确认权限。
                    if (ref.authority() != ticket.authority()
                            || ref.sourceGeneration() != ticket.authoritySnapshot().generation()) {
                        throw new IllegalStateException("successful record authority differs from Task authority");
                    }
                    MutableCommitRecord candidate = candidates.computeIfAbsent(
                            ref.id(), ignored -> new MutableCommitRecord(ref.id(), ticket.unit().plan().autoDelete(),
                                    ticket.authoritySnapshot()));
                    candidate.addGate(ticket.gateToken());
                }
                if (outcome.status() != ConsumeStatus.SUCCESS) {
                    List<PartitionRecordRef> retained = new ArrayList<>();
                    if (outcome.failed() != null) retained.add(outcome.failed());
                    retained.addAll(outcome.deferredTail());
                    if (retained.isEmpty()) retained.addAll(ticket.unit().refs());
                    for (PartitionRecordRef ref : retained) {
                        if (!source.authority().domain().retries.register(source, ref, ticket.unit().route().ordered(), read,
                                ticket.authoritySnapshot())) capacityRetained = true;
                    }
                    failures.add(outcome.cause() == null
                            ? new IllegalStateException("Partition Task did not succeed: " + outcome.status())
                            : outcome.cause());
                }
            }
            if (candidates.isEmpty()) {
                capacity.releaseCommitCapacity();
            } else {
                capacity.shrinkCommitRecords(candidates.size());
                PartitionDispatchCapacity.CommitLease lease = capacity.transferCommitCapacity(candidates.size());
                source.authority().domain().commits.commit(source, candidates.values().stream().map(MutableCommitRecord::freeze).toList(), lease);
            }
            return capacityRetained;
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
     * @param authority 原始读取的唯一身份与代次
     * @return 保持 Redis 遇见顺序的坐标列表
     */
    private List<PartitionRecordRef> unresolvedCoordinates(
            PartitionSource source, List<MapRecord<String, Object, Object>> rawBatch,
            StreamSourceAuthority.Snapshot authority) {
        List<PartitionRecordRef> result = new ArrayList<>(rawBatch.size());
        for (MapRecord<String, Object, Object> raw : rawBatch) {
            String field = raw.getValue().isEmpty()
                    ? "<empty>" : Objects.toString(raw.getValue().keySet().iterator().next(), "<unknown>");
            result.add(new PartitionRecordRef(
                    raw.getStream(), source.group(), source.consumer(), raw.getId().getValue(),
                    "<unresolved>", "<unresolved>", field, source.sourceKind(),
                    authority.current(), authority.generation()));
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
        for (var domain : subscriptions.domains()) domain.commits.closeAndDrain(drainTimeoutMillis);
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
                                  ResolvedPartitionKey route) {
    }

    private record UnitKey(long planId, int effectiveHash, long unorderedSequence) {
    }

    /**
     * 业务作用：在线性化提交、停机取消、Future 收口和 Submission 回收之间保存一笔 Task 的唯一所有权。
     */
    private static final class TaskTicket implements AutoCloseable {
        private final long id;
        private final StreamSourceAuthority.Snapshot authority;
        private final StreamDispatchUnit unit;
        private final OrderedKeyCoordinator.GateToken gateToken;
        private final CompletableFuture<ConsumeTaskOutcome> future;
        private final StreamRuntimeStatus.DrainToken drain;
        private Partition.Submission submission;
        private boolean closed;

        /**
         * 业务作用：在调用 Partition submit 之前发布可见 ticket。参数说明: 标识、提交时的权威快照、单元、门禁、Future 与排干令牌。返回: 未绑定句柄且固定来源代次的 ticket。
         */
        private TaskTicket(long id,
                           StreamSourceAuthority.Snapshot authority,
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
            return authority.current();
        }

        /** 业务作用：将业务执行的原权威交给确认链。参数说明: 无。返回: Task 提交时冻结的身份与代次。 */
        StreamSourceAuthority.Snapshot authoritySnapshot() {
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

    private record Preparation(List<PreparedRecord> prepared) {
    }

    private record DecodedEnvelope(String topic, String event, Object data, Map<String, Object> passthrough) {
    }

    /**
     * 业务作用：在 CommitAttempt 发布前合并同 record id 的 exact gate 依赖。
     */
    private static final class MutableCommitRecord {
        private final String id;
        private final boolean autoDelete;
        private final StreamSourceAuthority.Snapshot authority;
        private final LinkedHashSet<OrderedKeyCoordinator.GateToken> gates = new LinkedHashSet<>();

        /**
         * 业务作用：建立批次私有确认依赖。参数说明: id、删除策略与 Task 冻结权威。返回: 固定原代次的可变构造器。
         */
        private MutableCommitRecord(String id, boolean autoDelete, StreamSourceAuthority.Snapshot authority) {
            this.id = id;
            this.autoDelete = autoDelete;
            this.authority = authority;
        }

        /**
         * 业务作用：添加只属于本 record 的 exact gate。参数说明: gate 可为 null。返回: 无返回值。
         */
        void addGate(OrderedKeyCoordinator.GateToken gate) {
            if (gate != null) gates.add(gate);
        }

        /**
         * 业务作用：冻结为可跨批次复验的 CommitRecord。参数说明: 无。返回: 不可变依赖。
         */
        StreamCommitCoordinator.CommitRecord freeze() {
            return new StreamCommitCoordinator.CommitRecord(id, List.copyOf(gates), autoDelete, authority);
        }
    }

    /**
     * 业务作用：由 RedisPartition Claim 提供共享权威、精确 fencing ACK 与 PEL 复验入口。
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
         * 业务作用：取得恢复提交所需的来源权威证据；物理分区覆盖此入口查询实际 Redis holder。
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

        /** 业务作用：定位当前正文的原组合预留。参数说明: 无。返回: 当前预留；无活动读取时为 null。 */
        PartitionReadReservation readReservation();

        /**
         * 业务作用：提供实际 Redis 读取前冻结的批次身份，不允许由调用方临时重建。
         * 参数说明: 无。
         * @return 当前读取责任的原权威快照；没有读取责任时为空
         */
        StreamSourceAuthority.Snapshot readAuthority();

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
         * 业务作用：返回 holder 来源标识。参数说明: 无。返回: 来源身份。
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
