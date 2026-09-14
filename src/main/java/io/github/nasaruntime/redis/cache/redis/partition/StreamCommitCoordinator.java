package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.TimingWheel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * 业务作用：在 Redis I/O 之前登记 CommitAttempt，并把超时或断线的 ACK 收敛为明确确认、迁移或交回 PEL。
 */
final class StreamCommitCoordinator implements AutoCloseable {

    enum AckDisposition {CONFIRMED, MOVED, UNKNOWN, LOST_AUTHORITY}

    enum PendingDisposition {ABSENT, OWNED, MOVED, UNKNOWN}

    private final OrderedKeyCoordinator orderedKeys;
    private final StreamRuntimeStatus status;
    private final Executor settlementExecutor;
    private final String timingTaskPrefix;
    private final long initialDelayMillis;
    private final long maxDelayMillis;
    private final Supplier<StreamPartitionMetrics> metrics;
    private final AtomicLong attemptIds = new AtomicLong();
    private final ConcurrentHashMap<Long, CommitAttempt> attempts = new ConcurrentHashMap<>();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final ReentrantReadWriteLock admissionBarrier = new ReentrantReadWriteLock();

    /**
     * 业务作用：建立一个代理级确认注册表，以共享时间轮触发退避，并把 Redis 复验转交专用虚拟等待执行域。
     *
     * @param qualifier          用于诊断线程名的数据源名
     * @param orderedKeys        ordered key 确认阶段协调器
     * @param status             排干与健康状态
     * @param settlementExecutor 专用虚拟等待执行域
     * @param initialDelayMillis 首次复验退避
     * @param maxDelayMillis     最大复验退避
     *                           返回: 初始无未决 attempt 的协调器。
     */
    StreamCommitCoordinator(String qualifier,
                            OrderedKeyCoordinator orderedKeys,
                            StreamRuntimeStatus status,
                            Executor settlementExecutor,
                            long initialDelayMillis,
                            long maxDelayMillis) {
        this(qualifier, orderedKeys, status, settlementExecutor,
                initialDelayMillis, maxDelayMillis, () -> StreamPartitionMetrics.NOOP);
    }

    /**
     * 业务作用：建立可动态读取指标桥接器的确认注册表，使后端晚绑定不丢失后续 fencing 结果。
     *
     * @param qualifier          数据源名
     * @param orderedKeys        ordered 门禁
     * @param status             排干与健康状态
     * @param settlementExecutor 专用等待执行域
     * @param initialDelayMillis 首次复验退避
     * @param maxDelayMillis     最大复验退避
     * @param metrics            当前可选指标后端提供器
     *                           返回: 初始没有未决 attempt 的协调器。
     */
    StreamCommitCoordinator(String qualifier,
                            OrderedKeyCoordinator orderedKeys,
                            StreamRuntimeStatus status,
                            Executor settlementExecutor,
                            long initialDelayMillis,
                            long maxDelayMillis,
                            Supplier<StreamPartitionMetrics> metrics) {
        this.orderedKeys = Objects.requireNonNull(orderedKeys, "orderedKeys");
        this.status = Objects.requireNonNull(status, "status");
        this.settlementExecutor = Objects.requireNonNull(settlementExecutor, "settlementExecutor");
        if (initialDelayMillis < 1L || maxDelayMillis < initialDelayMillis) {
            throw new IllegalArgumentException("ack reconcile delays are invalid");
        }
        this.initialDelayMillis = initialDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.timingTaskPrefix = "redis-stream-ack:" + qualifier + ":" + UUID.randomUUID() + ":";
        if (!TimingWheel.isStarted()) TimingWheel.startTimingWheel();
    }

    /**
     * 业务作用：先发布不可变确认坐标及容量所有权，再执行首次 fencing ACK。
     *
     * @param source   来源权威与 Redis 确认入口
     * @param records  按 Redis 顺序排列的确认单元
     * @param capacity 已从批次转交的 attempt/record 容量
     * @return 首次 Redis 观察后的逐 id 结论；UNKNOWN 已由注册表接管
     */
    Map<String, AckDisposition> commit(CommitSource source,
                                       List<CommitRecord> records,
                                       PartitionDispatchCapacity.CommitLease capacity) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(capacity, "capacity");
        if (records.isEmpty()) {
            capacity.close();
            return Map.of();
        }
        CommitAttempt attempt;
        admissionBarrier.readLock().lock();
        try {
            if (!accepting.get()) {
                capacity.close();
                throw new IllegalStateException("commit registry admission is closed");
            }
            long id = attemptIds.incrementAndGet();
            if (id <= 0L) {
                capacity.close();
                throw new IllegalStateException("commit attempt id exhausted");
            }
            StreamRuntimeStatus.DrainToken drain = status.enter(StreamRuntimeStatus.Resource.COMMIT);
            try {
                attempt = new CommitAttempt(id, source, records, capacity, drain, initialDelayMillis);
                // 发布与 close 封口共享线性化门禁，关闭扫描不会漏掉已经通过 admission 的责任。
                if (attempts.putIfAbsent(id, attempt) != null) {
                    throw new IllegalStateException("commit attempt id already published");
                }
            } catch (Throwable failure) {
                drain.close();
                capacity.close();
                throw failure;
            }
        } finally {
            admissionBarrier.readLock().unlock();
        }
        settle(attempt, false, false);
        return attempt.snapshot();
    }

    /**
     * 业务作用：对已发布 attempt 执行首次 ACK 或 UNKNOWN 复验，任何 Redis 异常都只影响未决 id。
     *
     * @param attempt           注册表内的未决 attempt
     * @param reconcile         true 表示先查 PEL owner 再决定是否重试 ACK
     * @param closingSettlement true 表示由 close 扫描发起并允许执行最后一次 PEL 复验
     *                          返回: 无返回值；未决状态会安排下一次复验。
     */
    private void settle(CommitAttempt attempt, boolean reconcile, boolean closingSettlement) {
        if (closingSettlement) {
            if (!attempt.beginIo()) return;
        } else {
            admissionBarrier.readLock().lock();
            try {
                if (!accepting.get()) {
                    // 普通发布者在 close 线性化后不得再发起 Redis I/O，未决 PEL 责任由新 owner 接续。
                    finishRetained(attempt);
                    return;
                }
                if (!attempt.beginIo()) return;
            } finally {
                admissionBarrier.readLock().unlock();
            }
        }
        try (StreamRuntimeStatus.DrainToken ignored = status.enter(StreamRuntimeStatus.Resource.ACK_IO)) {
            if (!attempt.source().authority().isActive()) {
                finishRetained(attempt);
                return;
            }
            List<String> unresolved = attempt.unresolvedIds();
            if (unresolved.isEmpty()) {
                finish(attempt);
                return;
            }
            if (reconcile) reconcilePending(attempt, unresolved);
            else applyRemote(attempt, ackByPolicy(attempt, unresolved));
        } catch (Throwable uncertain) {
            if (attempt.markUnknown()) {
                status.recordCommitUnknown();
                for (String id : attempt.unresolvedIds()) {
                    if (attempt.record(id).autoDelete()) recordAck(attempt, "delete", "unknown");
                }
            }
        } finally {
            attempt.endIo();
        }
        if (attempt.hasUnresolved()) schedule(attempt);
        else finish(attempt);
    }

    /**
     * 业务作用：按精确 id 复验 PEL，缺席直接收敛，迁移交给新 consumer，仅仍属当前来源的 id 重试 ACK。
     *
     * @param attempt    当前 attempt
     * @param unresolved 仍无远端结论的 id
     *                   返回: 无返回值。
     */
    private void reconcilePending(CommitAttempt attempt, List<String> unresolved) {
        List<String> retry = new ArrayList<>();
        for (String id : unresolved) {
            PendingDisposition pending;
            try {
                pending = attempt.source().pending(id);
            } catch (Throwable failure) {
                pending = PendingDisposition.UNKNOWN;
            }
            switch (pending) {
                case ABSENT -> {
                    recordAck(attempt, "pending", "absent");
                    applyOne(attempt, id, AckDisposition.CONFIRMED);
                }
                case MOVED -> {
                    recordAck(attempt, "pending", "moved");
                    applyOne(attempt, id, AckDisposition.MOVED);
                }
                case OWNED -> {
                    recordAck(attempt, "pending", "owned");
                    retry.add(id);
                }
                case UNKNOWN -> {
                    recordAck(attempt, "pending", "unknown");
                    applyOne(attempt, id, AckDisposition.UNKNOWN);
                }
            }
        }
        if (!retry.isEmpty() && attempt.source().authority().isActive()) {
            applyRemote(attempt, ackByPolicy(attempt, retry));
        }
    }

    /**
     * 业务作用：在同一 CommitAttempt 内按 record 自身 autoDelete 策略分成最多两次 fencing Lua，再合并逐 id 观察。
     *
     * @param attempt 当前 attempt
     * @param ids     待确认 id
     * @return 保持输入 id 对应关系的观察映射
     */
    private Map<String, AckDisposition> ackByPolicy(CommitAttempt attempt, List<String> ids) {
        List<String> retained = new ArrayList<>();
        List<String> deleted = new ArrayList<>();
        for (String id : ids) {
            if (attempt.record(id).autoDelete()) deleted.add(id);
            else retained.add(id);
        }
        LinkedHashMap<String, AckDisposition> result = new LinkedHashMap<>();
        if (!retained.isEmpty()) result.putAll(attempt.source().ack(retained, false));
        if (!deleted.isEmpty()) result.putAll(attempt.source().ack(deleted, true));
        return Map.copyOf(result);
    }

    /**
     * 业务作用：吸收 fencing Lua 的逐 id 结果，缺失条目按 UNKNOWN 处理而不猜测成功。
     *
     * @param attempt 当前 attempt
     * @param remote  远端逐 id 观察
     *                返回: 无返回值。
     */
    private void applyRemote(CommitAttempt attempt, Map<String, AckDisposition> remote) {
        Map<String, AckDisposition> safe = remote == null ? Map.of() : remote;
        for (String id : attempt.unresolvedIds()) {
            applyOne(attempt, id, safe.getOrDefault(id, AckDisposition.UNKNOWN));
        }
    }

    /**
     * 业务作用：将一个 id 的远端结论 exactly-once 发布到 gate 与 Proxy ledger 依赖。
     *
     * @param attempt     当前 attempt
     * @param id          record id
     * @param disposition 远端结论
     *                    返回: 无返回值；旧或重复结果保持 no-op。
     */
    private void applyOne(CommitAttempt attempt, String id, AckDisposition disposition) {
        recordAck(attempt, "fencing", disposition.name().toLowerCase(java.util.Locale.ROOT));
        if (disposition == AckDisposition.UNKNOWN) {
            if (attempt.markUnknown(id)) {
                status.recordCommitUnknown();
                if (attempt.record(id).autoDelete()) recordAck(attempt, "delete", "unknown");
                for (OrderedKeyCoordinator.GateToken gate : attempt.record(id).gates()) {
                    orderedKeys.ackUnknown(gate);
                }
            }
            return;
        }
        if (disposition == AckDisposition.LOST_AUTHORITY) {
            attempt.source().authority().loseAuthority();
            finishRetained(attempt);
            return;
        }
        if (!attempt.resolve(id, disposition)) return;
        CommitRecord record = attempt.record(id);
        if (disposition == AckDisposition.CONFIRMED) {
            if (record.autoDelete()) recordAck(attempt, "delete", "confirmed");
            for (OrderedKeyCoordinator.GateToken gate : record.gates()) orderedKeys.ackConfirmed(gate, id);
            record.confirmed().run();
        } else {
            for (OrderedKeyCoordinator.GateToken gate : record.gates()) orderedKeys.recordMoved(gate, id);
            record.moved().run();
        }
    }

    /**
     * 业务作用：按来源类型累计固定 ACK 结果，不把 record id、consumer 或异常文本带入标签。
     *
     * @param attempt 当前确认责任
     * @param stage   fencing 或 pending
     * @param result  固定结果码
     *                返回: 无返回值。
     */
    private void recordAck(CommitAttempt attempt, String stage, String result) {
        StreamRecordSource source = attempt.source() instanceof StreamPartitionRuntime.PartitionSource partitionSource
                ? partitionSource.sourceKind() : StreamRecordSource.REDIS_PARTITION;
        metrics.get().ack(source, stage, result);
    }

    /**
     * 业务作用：以有界指数退避安排 UNKNOWN 复验，同一 attempt 同时只允许一个定时任务。
     *
     * @param attempt 尚有未决 id 的 attempt
     *                返回: 无返回值。
     */
    private void schedule(CommitAttempt attempt) {
        if (!accepting.get()) {
            // 关闭扫描已经接管全部已发布 attempt，未决坐标交回 PEL，不能在封口后重建时间轮责任。
            finishRetained(attempt);
            return;
        }
        if (!attempt.schedule()) return;
        long delay = attempt.nextDelay(maxDelayMillis);
        String taskName = timingTaskName(attempt);
        try {
            TimingWheel.exec(delay, taskName, () -> {
                attempt.unschedule();
                try {
                    settlementExecutor.execute(() -> settle(attempt, true, false));
                } catch (Throwable rejected) {
                    finishRetained(attempt);
                }
            });
        } catch (Throwable rejected) {
            attempt.unschedule();
            finishRetained(attempt);
        }
    }

    /**
     * 业务作用：为一笔 attempt 生成可取消且不含 record id 的时间轮任务名。
     *
     * @param attempt 当前确认尝试
     * @return 当前协调器内唯一任务名
     */
    private String timingTaskName(CommitAttempt attempt) {
        return timingTaskPrefix + attempt.id();
    }

    /**
     * 业务作用：将失权或停机未决 id 明确交回 Redis PEL，不再调 listener 也不猜测 ACK。
     *
     * @param attempt 需要结束本地所有权的 attempt
     *                返回: 无返回值。
     */
    private void finishRetained(CommitAttempt attempt) {
        attempt.retainUnresolved();
        orderedKeys.invalidateAuthority(attempt.source().authority());
        finish(attempt);
    }

    /**
     * 业务作用：只在 attempt 已无未决 id 时删除注册并释放容量与排干令牌。
     *
     * @param attempt 已收口的 attempt
     *                返回: 无返回值；重复收口保持幂等。
     */
    private void finish(CommitAttempt attempt) {
        if (attempt.hasUnresolved() || !attempt.finish()) return;
        TimingWheel.cancel(timingTaskName(attempt));
        attempts.remove(attempt.id(), attempt);
        attempt.capacity().close();
        attempt.drain().close();
    }

    /**
     * 业务作用：判断一个 Redis 来源是否仍持有已发布的确认责任，来源 owner 只能在该集合收敛后正常释放。
     *
     * @param authority 来源共享权威对象
     * @return 没有任何 CommitAttempt 绑定该来源时返回 true
     */
    boolean isDrained(StreamSourceAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        return attempts.values().stream()
                .noneMatch(attempt -> attempt.source().authority() == authority);
    }

    /**
     * 业务作用：在 record 执行权交接后识别仍由确认链负责的记录，ACK UNKNOWN 期间不重复执行业务。
     *
     * @param source 原 Task 使用的精确来源实例，隔离不同 Stream、group 与 consumer
     * @param ref    已冻结来源代次的 record 坐标
     * @return 同一来源及代次仍有该 id 的未决确认时为 true；调用方须先取得 record 执行权。
     */
    boolean ownsConfirmation(CommitSource source, PartitionRecordRef ref) {
        return attempts.values().stream().anyMatch(attempt -> attempt.source() == source
                && attempt.sourceGeneration == ref.sourceGeneration() && attempt.markUnknown(ref.id()));
    }

    /**
     * 业务作用：读取当前确认登记量。参数说明: 无。返回: CommitAttempt 数量。
     */
    long pendingAttempts() {
        return attempts.size();
    }

    /**
     * 业务作用：停止接受新 attempt，在预算内收敛已发布确认，最后把未决 id 交回 PEL。
     *
     * @param timeoutMillis 排干预算
     *                      返回: 无返回值。
     */
    void closeAndDrain(long timeoutMillis) {
        admissionBarrier.writeLock().lock();
        try {
            // 写锁同时等待所有已通过 admission 的发布者完成注册，后续快照即覆盖完整责任集合。
            accepting.set(false);
        } finally {
            admissionBarrier.writeLock().unlock();
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1L, timeoutMillis));
        for (CommitAttempt attempt : List.copyOf(attempts.values())) {
            TimingWheel.cancel(timingTaskName(attempt));
            attempt.unschedule();
            try {
                settlementExecutor.execute(() -> settle(attempt, true, true));
            } catch (Throwable rejected) {
                finishRetained(attempt);
            }
        }
        while (!attempts.isEmpty() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(5L);
            } catch (InterruptedException signal) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        for (CommitAttempt attempt : List.copyOf(attempts.values())) finishRetained(attempt);
    }

    /**
     * 业务作用：使用默认有界预算关闭确认协调器。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值。
     */
    @Override
    public void close() {
        closeAndDrain(5_000L);
    }

    /**
     * 业务作用：由物理 Claim 或 BOTH consumer 提供精确 fencing ACK 与 PEL owner 复验。
     */
    interface CommitSource {
        /**
         * 业务作用：返回当前来源的共享权威。参数说明: 无。返回: 来源权威。
         */
        StreamSourceAuthority authority();

        /**
         * 业务作用：对精确 id 集执行原子 fencing ACK。参数说明: id 及删除策略。返回: 逐 id 远端观察。
         */
        Map<String, AckDisposition> ack(List<String> ids, boolean autoDelete);

        /**
         * 业务作用：精确查询一个 id 是否缺席、仍属当前来源或已迁移。参数说明: record id。返回: PEL 所有权结论。
         */
        PendingDisposition pending(String id);
    }

    /**
     * 业务作用：保存一个 record id 与其 exact gate/ledger 依赖，防止批次广播确认到无关 key。
     *
     * @param id         Redis record id
     * @param gates      只依赖该 id 的 gate token
     * @param confirmed  该 id 明确确认后的账本动作
     * @param moved      该 id 明确迁移后的账本动作
     * @param autoDelete 本 id 实际 XACK 成功后是否删除正文
     */
    record CommitRecord(String id,
                        List<OrderedKeyCoordinator.GateToken> gates,
                        Runnable confirmed,
                        Runnable moved,
                        boolean autoDelete) {
        /**
         * 业务作用：复制一条精确确认依赖。
         *
         * @param id         record id
         * @param gates      exact gate token
         * @param confirmed  确认回调
         * @param moved      迁移回调
         * @param autoDelete 删除正文策略
         *                   返回: 不可变 gate 列表的依赖。
         */
        CommitRecord {
            Objects.requireNonNull(id, "id");
            gates = gates == null ? List.of() : List.copyOf(gates);
            confirmed = confirmed == null ? () -> {
            } : confirmed;
            moved = moved == null ? () -> {
            } : moved;
        }
    }

    /**
     * 业务作用：持久一笔已发布确认的不可变坐标与可变逐 id 结论。
     */
    private static final class CommitAttempt {

        private final long id;
        private final CommitSource source;
        private final long sourceGeneration;
        private final LinkedHashMap<String, CommitRecord> records;
        private final LinkedHashMap<String, AckDisposition> results = new LinkedHashMap<>();
        private final PartitionDispatchCapacity.CommitLease capacity;
        private final StreamRuntimeStatus.DrainToken drain;
        private final AtomicBoolean io = new AtomicBoolean();
        private final AtomicBoolean scheduled = new AtomicBoolean();
        private final AtomicBoolean finished = new AtomicBoolean();
        private long delayMillis;

        /**
         * 业务作用：冻结 Redis I/O 前所需的全部坐标与所有权。
         *
         * @param id          attempt 标识
         * @param source      来源确认入口
         * @param records     exact record 依赖
         * @param capacity    硬容量租约
         * @param drain       排干令牌
         * @param delayMillis 首次复验退避
         *                    返回: 所有 id 初始均为 UNKNOWN 的 attempt。
         */
        private CommitAttempt(long id,
                              CommitSource source,
                              List<CommitRecord> records,
                              PartitionDispatchCapacity.CommitLease capacity,
                              StreamRuntimeStatus.DrainToken drain,
                              long delayMillis) {
            this.id = id;
            this.source = source;
            this.sourceGeneration = source.authority().snapshot().generation();
            this.records = new LinkedHashMap<>();
            for (CommitRecord record : records) {
                if (this.records.putIfAbsent(record.id(), record) != null) {
                    throw new IllegalArgumentException("duplicate commit record id: " + record.id());
                }
                this.results.put(record.id(), AckDisposition.UNKNOWN);
            }
            this.capacity = capacity;
            this.drain = drain;
            this.delayMillis = delayMillis;
        }

        /**
         * 业务作用：取得 attempt 标识。参数说明: 无。返回: 进程内唯一标识。
         */
        long id() {
            return id;
        }

        /**
         * 业务作用：返回来源确认入口。参数说明: 无。返回: 共享来源。
         */
        CommitSource source() {
            return source;
        }

        /**
         * 业务作用：返回容量租约。参数说明: 无。返回: attempt 独占租约。
         */
        PartitionDispatchCapacity.CommitLease capacity() {
            return capacity;
        }

        /**
         * 业务作用：返回排干令牌。参数说明: 无。返回: attempt 独占令牌。
         */
        StreamRuntimeStatus.DrainToken drain() {
            return drain;
        }

        /**
         * 业务作用：取得 exact record 依赖。参数说明: record id。返回: 对应依赖。
         */
        synchronized CommitRecord record(String recordId) {
            return records.get(recordId);
        }

        /**
         * 业务作用：尝试独占一次 Redis I/O。参数说明: 无。返回: 本调用取得执行权时为 true。
         */
        boolean beginIo() {
            return !finished.get() && io.compareAndSet(false, true);
        }

        /**
         * 业务作用：释放 Redis I/O 执行权。参数说明: 无。返回: 无返回值。
         */
        void endIo() {
            io.set(false);
        }

        /**
         * 业务作用：列出仍无明确结论的 id。参数说明: 无。返回: Redis 顺序的不可变列表。
         */
        synchronized List<String> unresolvedIds() {
            return results.entrySet().stream()
                    .filter(entry -> entry.getValue() == AckDisposition.UNKNOWN)
                    .map(Map.Entry::getKey).toList();
        }

        /**
         * 业务作用：判断 attempt 是否仍有未决 id。参数说明: 无。返回: 存在 UNKNOWN 时为 true。
         */
        synchronized boolean hasUnresolved() {
            return results.containsValue(AckDisposition.UNKNOWN);
        }

        /**
         * 业务作用：发布一个 id 的首个明确结论。参数说明: id 与结论。返回: 本调用改变状态时为 true。
         */
        synchronized boolean resolve(String recordId, AckDisposition disposition) {
            if (disposition == AckDisposition.UNKNOWN || results.get(recordId) != AckDisposition.UNKNOWN) return false;
            results.put(recordId, disposition);
            return true;
        }

        /**
         * 业务作用：保持指定 id 为 UNKNOWN。参数说明: record id。返回: id 存在且未决时为 true。
         */
        synchronized boolean markUnknown(String recordId) {
            return results.get(recordId) == AckDisposition.UNKNOWN;
        }

        /**
         * 业务作用：保持所有未决 id 为 UNKNOWN。参数说明: 无。返回: 存在未决 id 时为 true。
         */
        synchronized boolean markUnknown() {
            return hasUnresolved();
        }

        /**
         * 业务作用：在失权或排干超时时把 UNKNOWN 标记为已交回 PEL。参数说明: 无。返回: 无返回值。
         */
        synchronized void retainUnresolved() {
            for (Map.Entry<String, AckDisposition> entry : results.entrySet()) {
                if (entry.getValue() == AckDisposition.UNKNOWN) entry.setValue(AckDisposition.MOVED);
            }
        }

        /**
         * 业务作用：取得当前逐 id 结论快照。参数说明: 无。返回: 不可变顺序映射。
         */
        synchronized Map<String, AckDisposition> snapshot() {
            return Map.copyOf(results);
        }

        /**
         * 业务作用：为 attempt 登记唯一定时复验。参数说明: 无。返回: 本调用取得登记权时为 true。
         */
        boolean schedule() {
            return !finished.get() && scheduled.compareAndSet(false, true);
        }

        /**
         * 业务作用：清除已触发的定时标志。参数说明: 无。返回: 无返回值。
         */
        void unschedule() {
            scheduled.set(false);
        }

        /**
         * 业务作用：取得当前退避并为下一次翻倍到上限。参数说明: 最大退避。返回: 本次延迟毫秒数。
         */
        synchronized long nextDelay(long maximum) {
            long current = delayMillis;
            delayMillis = Math.min(maximum, delayMillis > maximum / 2 ? maximum : delayMillis * 2);
            return current;
        }

        /**
         * 业务作用：发布 attempt 终态。参数说明: 无。返回: 本调用完成迁移时为 true。
         */
        boolean finish() {
            return finished.compareAndSet(false, true);
        }
    }
}
