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
    private int routeBlockedRecords;

    /**
     * 业务作用：建立一个代理级精确重试域，所有进入执行的回放共享固定 permit 上限。
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
     * 业务作用：判断指定来源是否仍持有 exact retry 或 route recovery 责任，owner 释放前必须等待这些状态收口。
     *
     * @param authority 来源共享权威对象
     * @return 该来源既无精确重试也无整批路由恢复时返回 true
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
     * @return 已登记、已存在或来源已停止时返回 true；容量暂满返回 false，调用方不得丢弃尚未交接的批次
     */
    boolean register(StreamPartitionRuntime.PartitionSource source,
                  PartitionRecordRef ref,
                  boolean ordered) {
        RetryKey key = RetryKey.of(source, ref);
        RetryState created = null;
        boolean capacityRejected = false;
        registrationLock.lock();
        try {
            if (!accepting.get() || !runtime.admissionOpen() || !source.allowsRecovery()) return true;
            if (retries.containsKey(key)) return true;
            if (!ordered && pendingUnordered >= maxPendingUnordered) {
                capacityRejected = true;
            } else {
                created = new RetryState(source, ref, ordered, initialDelayMillis);
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
     *                返回: 无返回值；超过硬上限时关闭来源 readiness。
     */
    void registerRouteBlocked(StreamPartitionRuntime.PartitionSource source,
                              List<PartitionRecordRef> records,
                              PartitionRecordCapacity.Permit permit) {
        List<PartitionRecordRef> immutableRecords = List.copyOf(records);
        StreamSourceAuthority.Snapshot snapshot = source.authority().snapshot();
        RouteBatchKey key = new RouteBatchKey(
                source.authority(), snapshot.generation(), source.stream(), source.group(), source.consumer());
        RouteBatchState created = null;
        boolean releasePermit = false;
        boolean capacityRejected = false;
        registrationLock.lock();
        try {
            if (!accepting.get() || !runtime.admissionOpen() || !source.allowsRecovery()) {
                releasePermit = true;
            } else if (routeBatches.containsKey(key)) {
                releasePermit = true;
            } else if (immutableRecords.size() > maxRouteBlocked - routeBlockedRecords) {
                releasePermit = true;
                capacityRejected = true;
            } else {
                StreamRuntimeStatus.DrainToken drain = status.enter(StreamRuntimeStatus.Resource.RETRY);
                created = new RouteBatchState(
                        source, immutableRecords, permit, drain, initialDelayMillis);
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
        boolean discard = false;
        registrationLock.lock();
        try {
            if (routeBatches.get(key) != state) return;
            if (!accepting.get() || !state.source().allowsRecovery()) {
                discard = removeRouteLocked(key, state);
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
                    discard = removeRouteLocked(key, state);
                }
            }
        } finally {
            registrationLock.unlock();
        }
        if (discard) closeRouteState(state, false);
    }

    /**
     * 业务作用：为一个待重试坐标安排当前退避，定时队列拒绝时交回 PEL。
     *
     * @param key   去重坐标
     * @param state 当前退避状态
     *              返回: 无返回值。
     */
    private void schedule(RetryKey key, RetryState state) {
        boolean discard = false;
        registrationLock.lock();
        try {
            if (retries.get(key) != state) return;
            if (!accepting.get() || !state.source().allowsRecovery()) {
                discard = removeExactLocked(key, state);
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
                    discard = removeExactLocked(key, state);
                }
            }
        } finally {
            registrationLock.unlock();
        }
        if (discard) state.close();
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
        boolean discard = false;
        registrationLock.lock();
        try {
            if (retries.get(key) != state) return;
            if (!accepting.get() || !state.source().allowsRecovery()) {
                discard = removeExactLocked(key, state);
            } else {
                execute = state.beginExecution();
            }
        } finally {
            registrationLock.unlock();
            if (!execute) {
                if (discard) state.close();
                executionPermits.release();
            }
        }
        if (!execute) return;
        StreamRuntimeStatus.DrainToken drain = null;
        boolean settled = false;
        try {
            drain = status.enter(StreamRuntimeStatus.Resource.RETRY);
            status.recordRetry();
            state.executionAttempt();
            StreamPartitionRuntime.RetryDisposition disposition = runtime.retryExact(state.source(), state.ref());
            settled = disposition == StreamPartitionRuntime.RetryDisposition.SETTLED
                    || disposition == StreamPartitionRuntime.RetryDisposition.MOVED;
        } catch (Throwable ignored) {
        } finally {
            // 先结束当前执行权，再决定注销或发布下一轮；时间轮回调不能与上一轮业务恢复重叠。
            state.endExecution();
            if (settled || !state.source().allowsRecovery()) finishExact(key, state);
            else schedule(key, state);
            if (drain != null) drain.close();
            executionPermits.release();
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
        boolean discard = false;
        registrationLock.lock();
        try {
            if (routeBatches.get(key) != state) return;
            if (!accepting.get() || !state.source().allowsRecovery()) {
                discard = removeRouteLocked(key, state);
            } else {
                execute = state.beginExecution();
            }
        } finally {
            registrationLock.unlock();
            if (!execute) {
                if (discard) closeRouteState(state, false);
                executionPermits.release();
            }
        }
        if (!execute) return;
        StreamRuntimeStatus.DrainToken execution = null;
        boolean settled = false;
        try {
            execution = status.enter(StreamRuntimeStatus.Resource.RETRY);
            status.recordRetry();
            state.executionAttempt();
            StreamPartitionRuntime.RetryDisposition disposition =
                    runtime.retryRouteBatch(state.source(), state.records());
            settled = disposition == StreamPartitionRuntime.RetryDisposition.SETTLED
                    || disposition == StreamPartitionRuntime.RetryDisposition.MOVED;
        } catch (Throwable ignored) {
        } finally {
            // route 状态同样在解除执行权后才建立未来驱动力，避免 raw record permit 留在无执行者状态。
            state.endExecution();
            if (settled) finishRoute(key, state, true);
            else if (!state.source().allowsRecovery()) finishRoute(key, state, false);
            else scheduleRoute(key, state);
            if (execution != null) execution.close();
            executionPermits.release();
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
            // 删除旧责任与重新开放来源不可分割，新 route 批次只能在开放动作完成后再建立自己的保护态。
            closeRouteState(state, reopen);
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
        boolean removed;
        registrationLock.lock();
        try {
            removed = removeExactLocked(key, state);
        } finally {
            registrationLock.unlock();
        }
        if (removed) state.close();
    }

    /**
     * 业务作用：在注册锁内删除 exact 状态并归还对应硬容量。
     *
     * @param key   去重坐标
     * @param state 期望状态
     * @return 本调用删除状态时返回 true
     */
    private boolean removeExactLocked(RetryKey key, RetryState state) {
        if (!retries.remove(key, state)) return false;
        if (!state.ordered()) pendingUnordered--;
        return true;
    }

    /**
     * 业务作用：在注册锁内删除 route recovery 并按实际坐标数归还硬容量。
     *
     * @param key   来源代次键
     * @param state 期望状态
     * @return 本调用删除状态时返回 true
     */
    private boolean removeRouteLocked(RouteBatchKey key, RouteBatchState state) {
        if (!routeBatches.remove(key, state)) return false;
        routeBlockedRecords -= state.records().size();
        return true;
    }

    /**
     * 业务作用：在 route 状态退出注册表后取消时间轮、释放长期资源，并按明确恢复结论重新开放来源。
     *
     * @param state  已删除的 route 状态
     * @param reopen 是否重新开放来源读取
     *               返回: 无返回值；状态自身保证资源只释放一次。
     */
    private void closeRouteState(RouteBatchState state, boolean reopen) {
        TimingWheel.cancel(state.timingTaskName());
        if (reopen && state.source().allowsRecovery()) state.source().clearRouteBlock();
        state.close();
        registrationLock.lock();
        try {
            if (routeBatches.isEmpty()) status.clearReadiness("route_blocked");
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * 业务作用：来源失权时删除其本地重试责任，新 owner 从 PEL 重建。
     *
     * @param authority 失效的共享来源权威
     *                  返回: 无返回值。
     */
    void invalidateAuthority(StreamSourceAuthority authority) {
        discardSource(authority, true);
    }

    /**
     * 业务作用：来源主动停止时封闭其新恢复发布，并取消尚未开始执行的 retry/route 状态。
     *
     * @param authority 已停止读取但仍可完成既有业务与 ACK 的来源权威
     *                  返回: 无返回值；正在执行的状态继续留在注册表，直到实际执行收口。
     */
    void stopSource(StreamSourceAuthority authority) {
        discardSource(authority, false);
    }

    /**
     * 业务作用：在注册锁内收口一个来源的本地恢复责任，使停止动作与并发登记形成单一先后关系。
     *
     * @param authority        来源共享权威对象
     * @param includeExecuting 是否连同已取得执行权的状态一起交回 PEL
     *                         返回: 无返回值；所有移除状态的容量、时间轮和排干资源都会恰好释放一次。
     */
    private void discardSource(StreamSourceAuthority authority, boolean includeExecuting) {
        List<RetryState> exactStates = new java.util.ArrayList<>();
        List<RouteBatchState> routeStates = new java.util.ArrayList<>();
        registrationLock.lock();
        try {
            for (Map.Entry<RetryKey, RetryState> entry : retries.entrySet()) {
                RetryState state = entry.getValue();
                if (state.source().authority() == authority
                        && (includeExecuting || !state.isExecuting())
                        && removeExactLocked(entry.getKey(), state)) {
                    exactStates.add(state);
                }
            }
            for (Map.Entry<RouteBatchKey, RouteBatchState> entry : routeBatches.entrySet()) {
                RouteBatchState state = entry.getValue();
                if (state.source().authority() == authority
                        && (includeExecuting || !state.isExecuting())
                        && removeRouteLocked(entry.getKey(), state)) {
                    routeStates.add(state);
                }
            }
        } finally {
            registrationLock.unlock();
        }
        for (RetryState state : exactStates) state.close();
        for (RouteBatchState state : routeStates) closeRouteState(state, false);
    }

    /**
     * 业务作用：停止新重试登记，取消本组件的时间轮任务，并将队列中坐标交回 PEL。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；重复关闭保持幂等。
     */
    @Override
    public void close() {
        List<RetryState> exactStates;
        List<RouteBatchState> routeStates;
        registrationLock.lock();
        try {
            if (!accepting.compareAndSet(true, false)) return;
            // 封 admission 与完整责任快照处于同一临界区，close 返回后不可能再发布新状态。
            exactStates = List.copyOf(retries.values());
            routeStates = List.copyOf(routeBatches.values());
            retries.clear();
            routeBatches.clear();
            pendingUnordered = 0;
            routeBlockedRecords = 0;
        } finally {
            registrationLock.unlock();
        }
        for (RetryState state : exactStates) state.close();
        for (RouteBatchState state : routeStates) closeRouteState(state, false);
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
         * @param source 当前来源
         * @param ref    record/field 坐标
         * @return 不持有消息体的重试键
         */
        static RetryKey of(StreamPartitionRuntime.PartitionSource source, PartitionRecordRef ref) {
            StreamSourceAuthority.Snapshot snapshot = source.authority().snapshot();
            String field = source.sourceKind() == StreamRecordSource.PROXY_BOTH ? null : ref.field();
            return new RetryKey(
                    source.authority(), snapshot.generation(), ref.stream(), ref.group(), ref.consumer(),
                    ref.id(), field);
        }
    }

    /**
     * 业务作用：保存一个 exact PEL 坐标的退避与去重状态。
     */
    private static final class RetryState {

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
         *                    返回: 尚未进入定时队列的状态。
         */
        private RetryState(StreamPartitionRuntime.PartitionSource source,
                           PartitionRecordRef ref,
                           boolean ordered,
                           long delayMillis) {
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
         * 业务作用：冻结原批次恢复责任。参数说明: 来源、坐标、容量、排干令牌与首次退避。返回: 未调度状态。
         */
        private RouteBatchState(StreamPartitionRuntime.PartitionSource source,
                                List<PartitionRecordRef> records,
                                PartitionRecordCapacity.Permit permit,
                                StreamRuntimeStatus.DrainToken drain,
                                long delayMillis) {
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
