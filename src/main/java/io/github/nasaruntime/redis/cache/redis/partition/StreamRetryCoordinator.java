package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.TimingWheel;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 业务作用：以有界注册表和执行 permit 主动推进 listener 失败、未执行与 gate deferred 的精确 PEL 坐标。
 */
final class StreamRetryCoordinator implements AutoCloseable {

    private final StreamPartitionRuntime runtime;
    private final StreamRuntimeStatus status;
    private final Semaphore executionPermits;
    private final int maxInFlight;
    private final int maxPendingUnordered;
    private final int maxRouteBlocked;
    private final long initialDelayMillis;
    private final long maxDelayMillis;
    private final ConcurrentHashMap<RetryKey, RetryState> retries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RouteBatchKey, RouteBatchState> routeBatches = new ConcurrentHashMap<>();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final ReentrantLock registrationLock = new ReentrantLock();
    private int pendingUnordered;
    private int reservedUnordered;
    private int reservedRoutes;
    private int routeBlockedRecords;

    /**
     * 业务作用：建立当前执行域的精确重试协调器，本域回放共享固定 permit 上限。
     *
     * @param runtime             共享 dispatcher
     * @param status              排干与运行指标
     * @param maxInFlight         同时执行的重试上限
     * @param maxPendingUnordered null-key 待重试坐标上限
     * @param maxRouteBlocked     整批恢复坐标上限，覆盖路由、证据与容量阻断
     * @param initialDelayMillis  首次退避
     * @param maxDelayMillis      最大退避
     *                            返回: 尚无待重试坐标的协调器。
     */
    StreamRetryCoordinator(StreamPartitionRuntime runtime,
                           StreamRuntimeStatus status,
                           int maxInFlight,
                           int maxPendingUnordered,
                           int maxRouteBlocked,
                           long initialDelayMillis,
                           long maxDelayMillis) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.status = Objects.requireNonNull(status, "status");
        if (maxInFlight < 1 || maxPendingUnordered < 1 || maxRouteBlocked < 1
                || initialDelayMillis < 1L || maxDelayMillis < initialDelayMillis) {
            throw new IllegalArgumentException("retry limits are invalid");
        }
        this.executionPermits = new Semaphore(maxInFlight, true);
        this.maxInFlight = maxInFlight;
        this.maxPendingUnordered = maxPendingUnordered;
        this.maxRouteBlocked = maxRouteBlocked;
        this.initialDelayMillis = initialDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        if (!TimingWheel.isStarted()) TimingWheel.startTimingWheel();
    }

    /**
     * 业务作用：读取 exact retry 与整批 route recovery 的当前登记量，指标标签不包含坐标内容。
     *
     * <p>参数说明: 无。
     *
     * @return 固定低基数名称到当前值的不可变映射
     */
    Map<String, Long> snapshot() {
        registrationLock.lock();
        try {
            long now = System.nanoTime();
            long oldestMillis = retries.values().stream()
                    .mapToLong(state -> java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                            Math.max(0L, now - state.createdAtNanos())))
                    .max().orElse(0L);
            long consecutiveFailures = Math.max(
                    retries.values().stream().mapToLong(RetryState::attempts).max().orElse(0L),
                    routeBatches.values().stream().mapToLong(RouteBatchState::attempts).max().orElse(0L));
            return Map.ofEntries(
                    Map.entry("pending_exact_retries", (long) retries.size()),
                    Map.entry("pending_unordered_retries", (long) pendingUnordered),
                    Map.entry("retry_reserved_unordered", (long) reservedUnordered),
                    Map.entry("retry_reserved_routes", (long) reservedRoutes),
                    Map.entry("retry_oldest_age_ms", oldestMillis),
                    Map.entry("retry_max_consecutive_failures", consecutiveFailures),
                    Map.entry("route_blocked_batches", (long) routeBatches.size()),
                    Map.entry("route_blocked_records", (long) routeBlockedRecords),
                    Map.entry("retry_permits_used", (long) maxInFlight - executionPermits.availablePermits()),
                    Map.entry("retry_permit_waiters", (long) executionPermits.getQueueLength()));
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * 业务作用：在 Redis 读取前预留 null-key 与未知路由最坏承接量，失败不改变任何份额。
     * @param count 最大物理记录数
     * @return 完整预留，容量或并发登记暂不可用时为 null
     */
    SuccessorReservation reserveRead(int count) {
        if (!registrationLock.tryLock()) return null;
        try {
            if (!accepting.get() || count > maxPendingUnordered - pendingUnordered - reservedUnordered
                    || count > maxRouteBlocked - routeBlockedRecords - reservedRoutes) return null;
            reservedUnordered += count;
            reservedRoutes += count;
            return new SuccessorReservation(count);
        } finally { registrationLock.unlock(); }
    }

    /** 业务作用：保存读取已经取得的失败承接量，登记失败坐标时按原域转移而非重复申请。 */
    final class SuccessorReservation implements AutoCloseable {
        private int unordered;
        private int routes;
        /** 业务作用：接管已扣减份额。@param count 数量；返回: 未交接的预留。 */
        SuccessorReservation(int count) { unordered = count; routes = count; }
        /** 业务作用：归还未读取的后继位置。@param count 实际记录数；返回: 无返回值。 */
        void shrink(int count) {
            registrationLock.lock();
            try {
                int u = Math.min(unordered, count), r = Math.min(routes, count);
                reservedUnordered -= unordered - u;
                reservedRoutes -= routes - r;
                unordered = u; routes = r;
            } finally { registrationLock.unlock(); }
        }
        /** 业务作用：原子转交一条非保序责任；调用方持有登记锁。参数说明: 无。返回: 有原额度时为 true。 */
        boolean takeUnordered() {
            if (unordered == 0) return false;
            unordered--; reservedUnordered--; return true;
        }
        /** 业务作用：原子转交整页责任；调用方持有登记锁。@param count 页数量 @return 有完整原额度时为 true */
        boolean takeRoutes(int count) {
            if (routes < count) return false;
            routes -= count; reservedRoutes -= count; return true;
        }
        /** 业务作用：归还未交接的承接量，已有重试负责其独立终态。参数说明: 无。返回: 重复关闭幂等。 */
        @Override public void close() { shrink(0); }
    }

    /**
     * 业务作用：判断指定来源是否仍持有 exact retry 或 route recovery 责任，owner 释放前必须等待这些状态收口。
     *
     * @param authority 来源共享权威对象
     * @return 该来源恢复调用已退出且对应长期资源已归还、注册表无剩余责任时返回 true
     */
    boolean isDrained(StreamSourceAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        registrationLock.lock();
        try {
            boolean exactPending = retries.values().stream()
                    .anyMatch(state -> state.source().authority() == authority);
            if (exactPending) return false;
            return routeBatches.values().stream()
                    .noneMatch(state -> state.source().authority() == authority);
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * 业务作用：在原批次释放前尝试登记失败或未执行坐标，容量暂满时由调用方保留整批恢复责任。
     *
     * @param source  当前 Redis 来源
     * @param ref     需要重建的 record/field 坐标
     * @param ordered 是否已由 ordered gate 保护
     * @param read    原读取的失败承接预留；精确重试及 source 模式可为空
     * @param authority 原 Task 或读取批次的冻结快照
     * @return 已登记、已存在或来源已停止时返回 true；容量暂满返回 false，调用方不得丢弃尚未交接的批次
     */
    boolean register(StreamPartitionRuntime.PartitionSource source,
                  PartitionRecordRef ref,
                  boolean ordered, PartitionReadReservation read, StreamSourceAuthority.Snapshot authority) {
        RetryKey key = RetryKey.of(ref);
        RetryState created = null;
        boolean capacityRejected = false;
        registrationLock.lock();
        try {
            // 原责任只在原代次内重试；来源重获后由新读取建立责任，不能从旧坐标换发重试。
            if (authority.current() != source.authority() || !authority.owns(ref)) return true;
            if (!authority.allowsExecution()) {
                runtime.settleRemoteRecord(ref);
                return true;
            }
            if (!accepting.get() || !runtime.admissionOpen() || !source.allowsRecovery()) return true;
            if (retries.containsKey(key)) return true;
            boolean prepaid = !ordered && read != null && read.successor.takeUnordered();
            if (!ordered && !prepaid && pendingUnordered + reservedUnordered >= maxPendingUnordered) {
                capacityRejected = true;
            } else {
                created = new RetryState(source, ref, ordered, initialDelayMillis, authority);
                // admission、容量预留与 Map 发布属于同一事务，关闭扫描不会遗漏已通过检查的状态。
                RetryState previous = retries.putIfAbsent(key, created);
                if (previous != null) return true;
                if (!ordered) pendingUnordered++;
            }
        } finally {
            registrationLock.unlock();
        }
        if (capacityRejected) {
            // 暂满不是失权，既有重试必须继续推进；未登记坐标仍由调用方的 raw 批次容量保护。
            return false;
        }
        if (created != null) schedule(key, created);
        return true;
    }

    /**
     * 业务作用：按原 Redis 顺序保留路由、证据或容量受阻批次的全部 id，并封闭后继新读取直到该批接续。
     *
     * @param source  受阻断的 Redis 来源
     * @param records 该原始批次的全部坐标
     * @param permit  原始 poll batch 转交的 record 容量，可为空
     * @param authority 该批读取前冻结且不会重新解释的快照
     *                返回: 无返回值；超过硬上限时关闭来源 readiness。
     */
    void registerRouteBlocked(StreamPartitionRuntime.PartitionSource source,
                              List<PartitionRecordRef> records,
                              PartitionRecordCapacity.Permit permit, StreamSourceAuthority.Snapshot authority) {
        List<PartitionRecordRef> immutableRecords = List.copyOf(records);
        // 失效或混合代次的正文只能交回 PEL，不能建立新代次的阻断状态或重复持有原容量。
        if (immutableRecords.isEmpty() || authority.current() != source.authority()
                || !authority.allowsExecution() || immutableRecords.stream().anyMatch(ref -> !authority.owns(ref))) {
            if (permit != null) permit.release();
            return;
        }
        PartitionRecordRef first = immutableRecords.getFirst();
        RouteBatchKey key = new RouteBatchKey(
                authority.current(), authority.generation(), first.stream(), first.group(), first.consumer());
        RouteBatchState created = null;
        boolean releasePermit = false;
        boolean capacityRejected = false;
        registrationLock.lock();
        try {
            if (!accepting.get() || !runtime.admissionOpen() || !source.allowsRecovery() || !authority.allowsExecution()) {
                releasePermit = true;
            } else if (routeBatches.containsKey(key)) {
                releasePermit = true;
            } else if (!(permit != null && permit.downstream != null
                    && permit.downstream.successor.takeRoutes(immutableRecords.size()))
                    && immutableRecords.size() > maxRouteBlocked - routeBlockedRecords - reservedRoutes) {
                releasePermit = true;
                capacityRejected = true;
            } else {
                StreamRuntimeStatus.DrainToken drain = status.enter(StreamRuntimeStatus.Resource.RETRY);
                created = new RouteBatchState(
                        source, immutableRecords, permit, drain, initialDelayMillis, authority);
                try {
                    // 坐标额度和长期 DrainToken 与状态一起发布，任何失败分支都会通过 state.close() 归还。
                    RouteBatchState previous = routeBatches.putIfAbsent(key, created);
                    if (previous != null) {
                        created.close();
                        created = null;
                        return;
                    }
                    routeBlockedRecords += immutableRecords.size();
                    try {
                        // 来源保护态必须与状态发布一起先于 close 可见，关闭返回后不能再出现迟到阻断副作用。
                        source.blockRoute(new RouteBlockedException());
                        status.failReadiness("route_blocked");
                        status.recordRouteBlocked(immutableRecords.size());
                        scheduleRoute(key, created);
                    } catch (Throwable failure) {
                        finishRoute(key, created, false);
                        throw failure;
                    }
                } catch (Throwable failure) {
                    if (routeBatches.get(key) != created) created.close();
                    throw failure;
                }
            }
        } finally {
            registrationLock.unlock();
        }
        if (releasePermit && permit != null) permit.release();
        if (capacityRejected) {
            status.failReadiness("route_blocked_capacity");
            source.pause(new RetryCapacityException("route-blocked capacity exhausted"));
            return;
        }
    }

    /**
     * 业务作用：为整批 route recovery 安排一次有界退避，始终保持原 Redis 顺序和批次边界。
     *
     * @param key   来源代次键
     * @param state 原批次恢复状态
     *              返回: 无返回值。
     */
    private void scheduleRoute(RouteBatchKey key, RouteBatchState state) {
        registrationLock.lock();
        try {
            if (routeBatches.get(key) != state) return;
            // 调度与执行都复验原快照，失效责任不能借来源的新代次继续推进。
            if (!accepting.get() || !state.authority.allowsExecution() || !state.source().allowsRecovery()) {
                removeRouteLocked(key, state);
            } else {
                if (!state.schedule()) return;
                long delay = state.nextDelay(maxDelayMillis);
                try {
                    TimingWheel.exec(delay, state.timingTaskName(), () -> {
                        state.unschedule();
                        try {
                            runtime.waitExecutor().execute(() -> executeRoute(key, state));
                        } catch (Throwable rejected) {
                            finishRoute(key, state, false);
                        }
                    });
                } catch (Throwable rejected) {
                    state.unschedule();
                    removeRouteLocked(key, state);
                }
            }
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * 业务作用：为一个待重试坐标安排当前退避，定时队列拒绝时交回 PEL。
     *
     * @param key   去重坐标
     * @param state 当前退避状态
     *              返回: 无返回值。
     */
    private void schedule(RetryKey key, RetryState state) {
        registrationLock.lock();
        try {
            if (retries.get(key) != state) return;
            // 调度与执行都复验原快照，失效责任不能借来源的新代次继续推进。
            if (!accepting.get() || !state.authority.allowsExecution() || !state.source().allowsRecovery()) {
                removeExactLocked(key, state);
            } else {
                if (!state.schedule()) return;
                long delay = state.nextDelay(maxDelayMillis);
                try {
                    TimingWheel.exec(delay, state.timingTaskName(), () -> {
                        state.unschedule();
                        try {
                            runtime.waitExecutor().execute(() -> execute(key, state));
                        } catch (Throwable rejected) {
                            finishExact(key, state);
                        }
                    });
                } catch (Throwable rejected) {
                    state.unschedule();
                    removeExactLocked(key, state);
                }
            }
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * 业务作用：取得重试 permit 后通过 XPENDING + XRANGE 复验坐标，再调用同一 dispatcher。
     *
     * @param key   去重坐标
     * @param state 重试状态
     *              返回: 无返回值；仍未收敛时继续有界退避。
     */
    private void execute(RetryKey key, RetryState state) {
        if (!executionPermits.tryAcquire()) {
            schedule(key, state);
            return;
        }
        boolean execute = false;
        registrationLock.lock();
        try {
            if (retries.get(key) != state) return;
            // 调度与执行都复验原快照，失效责任不能借来源的新代次继续推进。
            if (!accepting.get() || !state.authority.allowsExecution() || !state.source().allowsRecovery()) {
                removeExactLocked(key, state);
            } else {
                execute = state.beginExecution();
            }
        } finally {
            registrationLock.unlock();
            if (!execute) executionPermits.release();
        }
        if (!execute) return;
        StreamRuntimeStatus.DrainToken drain = null;
        boolean settled = false;
        try {
            drain = status.enter(StreamRuntimeStatus.Resource.RETRY);
            status.recordRetry();
            state.executionAttempt();
            StreamPartitionRuntime.RetryDisposition disposition = runtime.retryExact(state.source(), state.ref(), state.authority);
            settled = disposition == StreamPartitionRuntime.RetryDisposition.SETTLED
                    || disposition == StreamPartitionRuntime.RetryDisposition.MOVED;
        } catch (Throwable ignored) {
        } finally {
            registrationLock.lock();
            try {
                // 恢复调用已返回；先归还执行资源，再在同一边界解除执行权并注销或调度，排空检查不能越过清理。
                if (drain != null) drain.close();
                executionPermits.release();
                state.endExecution();
                if (settled || !state.authority.allowsExecution() || !state.source().allowsRecovery()) finishExact(key, state);
                else schedule(key, state);
            } finally {
                registrationLock.unlock();
            }
        }
    }

    /**
     * 业务作用：在一个执行 permit 内按原顺序复验整批 XPENDING/XRANGE，再交给同一 dispatcher 原子重建路由。
     *
     * @param key   来源代次键
     * @param state 整批恢复状态
     *              返回: 无返回值；尚不能分类时保留批次及其 record permit。
     */
    private void executeRoute(RouteBatchKey key, RouteBatchState state) {
        if (!executionPermits.tryAcquire()) {
            scheduleRoute(key, state);
            return;
        }
        boolean execute = false;
        registrationLock.lock();
        try {
            if (routeBatches.get(key) != state) return;
            // 调度与执行都复验原快照，失效责任不能借来源的新代次继续推进。
            if (!accepting.get() || !state.authority.allowsExecution() || !state.source().allowsRecovery()) {
                removeRouteLocked(key, state);
            } else {
                execute = state.beginExecution();
            }
        } finally {
            registrationLock.unlock();
            if (!execute) executionPermits.release();
        }
        if (!execute) return;
        StreamRuntimeStatus.DrainToken execution = null;
        boolean settled = false;
        try {
            execution = status.enter(StreamRuntimeStatus.Resource.RETRY);
            status.recordRetry();
            state.executionAttempt();
            StreamPartitionRuntime.RetryDisposition disposition =
                    runtime.retryRouteBatch(state.source(), state.records(), state.source()::revalidateRecoveryAuthority,
                            state.permit == null ? null : state.permit.downstream, state.authority);
            settled = disposition == StreamPartitionRuntime.RetryDisposition.SETTLED
                    || disposition == StreamPartitionRuntime.RetryDisposition.MOVED;
        } catch (Throwable ignored) {
        } finally {
            registrationLock.lock();
            try {
                // 原批次及长期容量留到恢复调用返回；执行令牌与注册责任在同一临界区内依次收口。
                if (execution != null) execution.close();
                executionPermits.release();
                state.endExecution();
                if (settled) finishRoute(key, state, true);
                else if (!state.authority.allowsExecution() || !state.source().allowsRecovery()) finishRoute(key, state, false);
                else scheduleRoute(key, state);
            } finally {
                registrationLock.unlock();
            }
        }
    }

    /**
     * 业务作用：删除整批 route recovery，释放 retained record 容量，并仅在成功接续时重新开放来源。
     *
     * @param key    来源代次键
     * @param state  当前批次状态
     * @param reopen 是否重新开放来源读取
     *               返回: 无返回值；重复收口保持幂等。
     */
    private void finishRoute(RouteBatchKey key, RouteBatchState state, boolean reopen) {
        registrationLock.lock();
        try {
            if (!removeRouteLocked(key, state)) return;
            // 停机不能因迟到的成功结论重新开放读取；原代次和全局准入都有效时才解除保护。
            if (reopen && accepting.get() && runtime.admissionOpen()
                    && state.authority.allowsExecution() && state.source().allowsRecovery()) state.source().clearRouteBlock();
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * 业务作用：删除一个 exact retry 并一次性归还 unordered 坐标额度，所有终态路径必须通过该入口收口。
     *
     * @param key   去重坐标
     * @param state 期望删除的状态代次
     *              返回: 无返回值；状态已被其它线程收口时保持幂等。
     */
    private void finishExact(RetryKey key, RetryState state) {
        registrationLock.lock();
        try {
            removeExactLocked(key, state);
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * 业务作用：在注册锁内取消已退出执行的 exact 状态，再注销坐标及其硬容量。
     *
     * @param key   去重坐标
     * @param state 期望状态
     * @return 本调用完成注销时返回 true；执行中的状态继续保留到执行栈收口
     */
    private boolean removeExactLocked(RetryKey key, RetryState state) {
        // 停止或失权不能提前抹去执行责任；只有执行线程解除标记后才允许归还容量。
        if (state.isExecuting() || retries.get(key) != state) return false;
        state.close();
        retries.remove(key, state);
        // 失效恢复不再驱动旧坐标，清理仅匹配原代次的门禁，避免阻挡合法新读取。
        if (!state.authority.allowsExecution()) runtime.settleRemoteRecord(state.ref());
        if (!state.ordered()) pendingUnordered--;
        return true;
    }

    /**
     * 业务作用：在注册锁内先归还 route recovery 的长期资源，再注销原批次及其坐标容量。
     *
     * @param key   来源代次键
     * @param state 期望状态
     * @return 本调用完成注销时返回 true；执行中的原批次不能被关闭路径提前释放
     */
    private boolean removeRouteLocked(RouteBatchKey key, RouteBatchState state) {
        // Map 存续到 retained permit 和 DrainToken 实际释放，来源排空检查不能先于资源终态。
        if (state.isExecuting() || routeBatches.get(key) != state) return false;
        state.close();
        routeBatches.remove(key, state);
        // 整页责任退出时只撤销原坐标，后继代次及其它来源的顺序事实继续保留。
        if (!state.authority.allowsExecution()) state.records().forEach(runtime::settleRemoteRecord);
        routeBlockedRecords -= state.records().size();
        if (routeBatches.isEmpty()) status.clearReadiness("route_blocked");
        return true;
    }

    /**
     * 业务作用：来源失权后取消尚未执行的恢复，已开始的调用保留原容量直到返回，新 owner 从 PEL 重建。
     *
     * @param authority 失效的共享来源权威
     * 返回: 无返回值；执行中责任仍参与原来源排空判断。
     */
    void invalidateAuthority(StreamSourceAuthority authority) {
        discardSource(authority);
    }

    /**
     * 业务作用：来源主动停止时封闭其新恢复发布，并取消尚未开始执行的 retry/route 状态。
     *
     * @param authority 已停止读取但仍可完成既有业务与 ACK 的来源权威
     * 返回: 无返回值；正在执行的状态继续留在注册表，直到实际执行收口。
     */
    void stopSource(StreamSourceAuthority authority) {
        discardSource(authority);
    }

    /**
     * 业务作用：在注册锁内撤销一个来源尚未执行的恢复，保留仍在使用原批次的执行责任。
     *
     * @param authority 来源共享权威对象
     * 返回: 无返回值；资源归还先于注销，执行中的状态由原执行栈唯一收口。
     */
    private void discardSource(StreamSourceAuthority authority) {
        registrationLock.lock();
        try {
            for (Map.Entry<RetryKey, RetryState> entry : retries.entrySet()) {
                if (entry.getValue().source().authority() == authority) removeExactLocked(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<RouteBatchKey, RouteBatchState> entry : routeBatches.entrySet()) {
                if (entry.getValue().source().authority() == authority) removeRouteLocked(entry.getKey(), entry.getValue());
            }
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * 业务作用：永久关闭新登记并取消尚未执行的恢复，保留执行中状态、计数及原批次容量直到调用返回。
     *
     * 参数说明: 无。
     * 返回: 无返回值；不等待或中断恢复 I/O，重复关闭幂等，来源排空必须继续检查存续状态。
     */
    @Override
    public void close() {
        registrationLock.lock();
        try {
            if (!accepting.compareAndSet(true, false)) return;
            // 先在登记边界关闭准入；执行权已授予的状态继续约束 holder，不能用清空 Map 代替真实排干。
            for (Map.Entry<RetryKey, RetryState> entry : retries.entrySet()) removeExactLocked(entry.getKey(), entry.getValue());
            for (Map.Entry<RouteBatchKey, RouteBatchState> entry : routeBatches.entrySet()) removeRouteLocked(entry.getKey(), entry.getValue());
        } finally {
            registrationLock.unlock();
        }
    }

    private record RouteBatchKey(StreamSourceAuthority authority,
                                 long generation,
                                 String stream,
                                 String group,
                                 String consumer) {
    }

    private record RetryKey(StreamSourceAuthority authority,
                            long generation,
                            String stream,
                            String group,
                            String consumer,
                            String id,
                            String field) {
        /**
         * 业务作用：按来源代次与 exact Redis 坐标建立去重键。
         *
         * @param ref    原来源代次的 record/field 坐标
         * @return 不持有消息体的重试键
         */
        static RetryKey of(PartitionRecordRef ref) {
            String field = ref.field();
            return new RetryKey(
                    ref.authority(), ref.sourceGeneration(), ref.stream(), ref.group(), ref.consumer(),
                    ref.id(), field);
        }
    }

    /**
     * 业务作用：保存一个 exact PEL 坐标的退避与去重状态。
     */
    private static final class RetryState {

        private final StreamSourceAuthority.Snapshot authority;
        private final StreamPartitionRuntime.PartitionSource source;
        private final PartitionRecordRef ref;
        private final boolean ordered;
        private final String timingTaskName = "redis-stream-retry:" + UUID.randomUUID();
        private final AtomicBoolean scheduled = new AtomicBoolean();
        private final AtomicBoolean executing = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final long createdAtNanos = System.nanoTime();
        private final java.util.concurrent.atomic.AtomicLong attempts = new java.util.concurrent.atomic.AtomicLong();
        private long delayMillis;

        /**
         * 业务作用：冻结重试坐标及其初始退避。
         *
         * @param source      Redis 来源
         * @param ref         exact 坐标
         * @param ordered     是否由 ordered gate 保护
         * @param delayMillis 首次退避
         * @param authority 原读取或 Task 冻结的权威
         *                    返回: 尚未进入定时队列的状态。
         */
        private RetryState(StreamPartitionRuntime.PartitionSource source,
                           PartitionRecordRef ref,
                           boolean ordered,
                           long delayMillis, StreamSourceAuthority.Snapshot authority) {
            this.authority = authority;
            this.source = source;
            this.ref = ref;
            this.ordered = ordered;
            this.delayMillis = delayMillis;
        }

        /**
         * 业务作用：返回 Redis 来源。参数说明: 无。返回: 来源。
         */
        StreamPartitionRuntime.PartitionSource source() {
            return source;
        }

        /**
         * 业务作用：返回 exact 坐标。参数说明: 无。返回: record/field 坐标。
         */
        PartitionRecordRef ref() {
            return ref;
        }

        /**
         * 业务作用：返回是否使用 ordered gate。参数说明: 无。返回: ordered 时为 true。
         */
        boolean ordered() {
            return ordered;
        }

        /**
         * 业务作用：返回重试状态建立时刻。参数说明: 无。返回: 单调时钟纳秒值。
         */
        long createdAtNanos() {
            return createdAtNanos;
        }

        /**
         * 业务作用：累计一次实际取得 permit 的执行。参数说明: 无。返回: 最新连续尝试次数。
         */
        long executionAttempt() {
            return attempts.incrementAndGet();
        }

        /**
         * 业务作用：读取当前状态尚未收敛的连续尝试数。参数说明: 无。返回: 尝试次数。
         */
        long attempts() {
            return attempts.get();
        }

        /**
         * 业务作用：返回本坐标可取消的时间轮任务名。参数说明: 无。返回: 不含业务坐标的唯一名称。
         */
        String timingTaskName() {
            return timingTaskName;
        }

        /**
         * 业务作用：登记唯一定时任务。参数说明: 无。返回: 本调用取得登记权时为 true。
         */
        boolean schedule() {
            return !executing.get() && scheduled.compareAndSet(false, true);
        }

        /**
         * 业务作用：清除已触发定时标志。参数说明: 无。返回: 无返回值。
         */
        void unschedule() {
            scheduled.set(false);
        }

        /**
         * 业务作用：标记 exact retry 已取得执行责任。参数说明: 无。返回: 本调用取得执行权时为 true。
         */
        boolean beginExecution() {
            return executing.compareAndSet(false, true);
        }

        /**
         * 业务作用：结束 exact retry 执行责任。参数说明: 无。返回: 无返回值。
         */
        void endExecution() {
            executing.set(false);
        }

        /**
         * 业务作用：报告 exact retry 是否已进入实际恢复调用。参数说明: 无。返回: 正在执行时为 true。
         */
        boolean isExecuting() {
            return executing.get();
        }

        /**
         * 业务作用：取得当前退避并为下次翻倍到上限。参数说明: 上限。返回: 本次延迟毫秒数。
         */
        synchronized long nextDelay(long maximum) {
            long current = delayMillis;
            delayMillis = Math.min(maximum, delayMillis > maximum / 2 ? maximum : delayMillis * 2);
            return current;
        }

        /**
         * 业务作用：取消尚未触发的时间轮任务。参数说明: 无。返回: 无返回值；重复关闭保持幂等。
         */
        void close() {
            if (closed.compareAndSet(false, true)) TimingWheel.cancel(timingTaskName);
        }
    }

    /**
     * 业务作用：保存整批 routeBlocked 的有序坐标、raw record 容量和长期排干所有权，不保存消息正文。
     */
    private static final class RouteBatchState {

        private final StreamSourceAuthority.Snapshot authority;
        private final StreamPartitionRuntime.PartitionSource source;
        private final List<PartitionRecordRef> records;
        private final PartitionRecordCapacity.Permit permit;
        private final StreamRuntimeStatus.DrainToken drain;
        private final String timingTaskName = "redis-stream-route-retry:" + UUID.randomUUID();
        private final AtomicBoolean scheduled = new AtomicBoolean();
        private final AtomicBoolean executing = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicLong attempts = new java.util.concurrent.atomic.AtomicLong();
        private long delayMillis;

        /**
         * 业务作用：冻结原批次恢复责任。参数说明: 来源、坐标、容量、排干令牌、首次退避与原权威快照。返回: 固定原代次的未调度状态。
         */
        private RouteBatchState(StreamPartitionRuntime.PartitionSource source,
                                List<PartitionRecordRef> records,
                                PartitionRecordCapacity.Permit permit,
                                StreamRuntimeStatus.DrainToken drain,
                                long delayMillis, StreamSourceAuthority.Snapshot authority) {
            this.authority = authority;
            this.source = source;
            this.records = records;
            this.permit = permit;
            this.drain = drain;
            this.delayMillis = delayMillis;
        }

        /**
         * 业务作用：返回 Redis 来源。参数说明: 无。返回: 来源。
         */
        StreamPartitionRuntime.PartitionSource source() {
            return source;
        }

        /**
         * 业务作用：返回保持原 Redis 顺序的批次坐标。参数说明: 无。返回: 不可变坐标。
         */
        List<PartitionRecordRef> records() {
            return records;
        }

        /**
         * 业务作用：返回整批恢复可取消的时间轮任务名。参数说明: 无。返回: 不含业务坐标的唯一名称。
         */
        String timingTaskName() {
            return timingTaskName;
        }

        /**
         * 业务作用：登记唯一定时任务。参数说明: 无。返回: 本调用取得登记权时为 true。
         */
        boolean schedule() {
            return !executing.get() && scheduled.compareAndSet(false, true);
        }

        /**
         * 业务作用：清除已触发的调度标记。参数说明: 无。返回: 无返回值。
         */
        void unschedule() {
            scheduled.set(false);
        }

        /**
         * 业务作用：标记 route recovery 已取得执行责任。参数说明: 无。返回: 本调用取得执行权时为 true。
         */
        boolean beginExecution() {
            return executing.compareAndSet(false, true);
        }

        /**
         * 业务作用：结束 route recovery 执行责任。参数说明: 无。返回: 无返回值。
         */
        void endExecution() {
            executing.set(false);
        }

        /**
         * 业务作用：报告 route recovery 是否已进入实际恢复调用。参数说明: 无。返回: 正在执行时为 true。
         */
        boolean isExecuting() {
            return executing.get();
        }

        /**
         * 业务作用：累计一次实际取得 permit 的 route recovery。参数说明: 无。返回: 最新连续尝试次数。
         */
        long executionAttempt() {
            return attempts.incrementAndGet();
        }

        /**
         * 业务作用：读取当前 route 状态的连续尝试数。参数说明: 无。返回: 尝试次数。
         */
        long attempts() {
            return attempts.get();
        }

        /**
         * 业务作用：取得当前退避并把下次延迟翻倍到上限。参数说明: 最大延迟。返回: 本次延迟。
         */
        synchronized long nextDelay(long maximum) {
            long current = delayMillis;
            delayMillis = Math.min(maximum, delayMillis > maximum / 2 ? maximum : delayMillis * 2);
            return current;
        }

        /**
         * 业务作用：一次性释放 retained record 容量和长期排干所有权。参数说明: 无。返回: 无返回值。
         */
        void close() {
            if (!closed.compareAndSet(false, true)) return;
            TimingWheel.cancel(timingTaskName);
            if (permit != null) permit.release();
            drain.close();
        }
    }

    /**
     * 业务作用：给来源记录不携带原始异常文本的 routeBlocked 保护原因。
     */
    private static final class RouteBlockedException extends RuntimeException {
        /**
         * 业务作用：创建来源保护原因。参数说明: 无。返回: routeBlocked 异常。
         */
        private RouteBlockedException() {
            super("stream route batch is blocked");
        }
    }

    /**
     * 业务作用：表示重试状态已达硬上限，来源必须暂停而不能驱逐旧坐标。
     */
    static final class RetryCapacityException extends RuntimeException {
        /**
         * 业务作用：创建重试容量拒绝结论。参数说明: 原因。返回: 硬上限异常。
         */
        RetryCapacityException(String message) {
            super(message);
        }
    }
}
