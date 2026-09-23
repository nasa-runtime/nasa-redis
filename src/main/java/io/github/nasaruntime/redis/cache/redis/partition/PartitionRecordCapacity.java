package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 业务作用：在 XREADGROUP 前按来源公平预留 raw record 容量，并把预留精确交接给活动批次或 retained 状态。
 * raw 所有权与后继组合预留分别计量，并在同一来源生命周期内转交。
 */
final class PartitionRecordCapacity {

    private final ReentrantLock lock = new ReentrantLock(true);
    private final int total;
    private final Runnable debtObserved;
    private int available;
    private final java.util.function.IntFunction<PartitionReadReservation> readReservations;
    private boolean admissionOpen = true;
    private final LinkedHashMap<Object, Waiter> waiters = new LinkedHashMap<>();

    /**
     * 业务作用：创建固定总量的来源公平读取容量池。
     *
     * @param total 同一执行域全部物理 Claim 共享的最大 raw record 数
     *              返回: 初始全部容量可用的协调器。
     */
    PartitionRecordCapacity(int total) {
        this(total, () -> {});
    }

    /**
     * 业务作用：创建固定总量的来源公平读取容量池，并在 Redis 返回数违反 COUNT 合同时发布保护信号。
     *
     * @param total        同一执行域全部物理 Claim 共享的最大 raw record 数
     * @param debtObserved 实际返回超过预留时关闭新读取 readiness 的动作
     *                     返回: 初始全部容量可用的协调器。
     */
    PartitionRecordCapacity(int total, Runnable debtObserved) {
        this(total, debtObserved, null);
    }

    /**
     * 业务作用：把域级完整预留纳入同一读取公平队列，后继不足时不能先消费 Redis。
     * @param total raw record 上限
     * @param debtObserved COUNT 失约保护动作
     * @param readReservations 完整后继容量申请；source 模式为 null
     * 返回: 固定域容量池。
     */
    PartitionRecordCapacity(int total, Runnable debtObserved,
                            java.util.function.IntFunction<PartitionReadReservation> readReservations) {
        this.readReservations = readReservations;
        if (total < 1) throw new IllegalArgumentException("record capacity must be greater than zero");
        this.total = total;
        this.available = total;
        this.debtObserved = Objects.requireNonNull(debtObserved, "debtObserved");
    }

    /**
     * 业务作用：为来源非阻塞预留一次读取的最大批量；等待队首之外的来源不能插队抢占新释放容量。
     *
     * @param sourceId 来源生命周期内稳定且按对象身份区分的标识
     * @param count    本轮有效 batch-size
     * @param wake     容量变化时唤醒该来源 runner 的动作
     * @return 已扣减容量的 Permit；暂时不可满足时返回 null
     */
    Permit tryReserve(Object sourceId, int count, Runnable wake) {
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(wake, "wake");
        if (count < 1 || count > total) {
            throw new IllegalArgumentException("record reservation must be between 1 and total capacity");
        }
        lock.lock();
        try {
            if (!admissionOpen) return null;
            Waiter waiter = waiters.get(sourceId);
            if (waiter == null && (!waiters.isEmpty() || available < count)) {
                waiters.put(sourceId, new Waiter(count, wake));
                return null;
            }
            if (waiter != null) {
                Map.Entry<Object, Waiter> first = waiters.entrySet().iterator().next();
                if (first.getKey() != sourceId || available < waiter.count()) return null;
                count = waiter.count();
            }
            PartitionReadReservation downstream = readReservations == null ? null : readReservations.apply(count);
            if (readReservations != null && downstream == null) {
                waiters.putIfAbsent(sourceId, new Waiter(count, wake));
                return null;
            }
            waiters.remove(sourceId);
            available -= count;
            return new Permit(this, count, downstream);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：来源停止时从公平等待队列移除，避免已失效来源阻挡后续有效消费者。
     *
     * @param sourceId 即将失效的来源标识
     *                 返回: 无返回值；来源未等待时保持幂等。
     */
    void removeWaiter(Object sourceId) {
        Runnable wake = null;
        lock.lock();
        try {
            if (waiters.remove(sourceId) != null) wake = firstWakeLocked();
        } finally {
            lock.unlock();
        }
        runWake(wake);
    }

    /**
     * 业务作用：关闭全部新 XREAD 预留并唤醒公平队列，使来源立即观察停机门禁而不继续等待容量。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；已经持有的 Permit 仍由原批次或 retained 状态负责归还。
     */
    void closeAdmission() {
        List<Runnable> wakes;
        lock.lock();
        try {
            admissionOpen = false;
            wakes = waiters.values().stream().map(Waiter::wake).toList();
            waiters.clear();
        } finally {
            lock.unlock();
        }
        for (Runnable wake : wakes) runWake(wake);
    }

    /**
     * 业务作用：读取 raw record 容量使用量与公平等待来源数，指标采集不得持有内部并发容器。
     *
     * <p>参数说明: 无。
     *
     * @return 固定低基数名称到当前值的不可变映射
     */
    Map<String, Long> snapshot() {
        lock.lock();
        try {
            return Map.of(
                    "record_capacity_used", (long) total - available,
                    "record_capacity_waiters", (long) waiters.size());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：归还 Permit 的精确持有量，并唤醒当前公平队首重新竞争。
     *
     * @param count 本次归还的 raw record 数
     *              返回: 无返回值；可用量超过总量表示所有权被重复释放并立即拒绝。
     */
    private void release(int count) {
        Runnable wake;
        lock.lock();
        try {
            available += count;
            if (available > total) {
                available -= count;
                throw new IllegalStateException("record capacity ownership released more than once");
            }
            wake = firstWakeLocked();
        } finally {
            lock.unlock();
        }
        runWake(wake);
    }

    /**
     * 业务作用：把异常读取超过预留的部分登记为容量债务，使新读取暂停到该批真实数量全部归还。
     *
     * @param debt 超出预留的记录数
     *             返回: 无返回值；债务允许 available 暂时为负数。
     */
    private void addDebt(int debt) {
        if (debt < 1) return;
        lock.lock();
        try {
            available -= debt;
            admissionOpen = false;
        } finally {
            lock.unlock();
        }
        debtObserved.run();
    }

    /**
     * 业务作用：读取公平队首的唤醒动作；调用方必须持有协调器锁。
     *
     * <p>参数说明: 无。
     *
     * @return 存在等待来源时返回其唤醒动作，否则返回 null
     */
    private Runnable firstWakeLocked() {
        return waiters.isEmpty() ? null : waiters.entrySet().iterator().next().getValue().wake();
    }

    /**
     * 业务作用：在协调器锁外执行唤醒，避免 runner 回调反向进入容量路径形成锁依赖。
     *
     * @param wake 待执行的唤醒动作，可为空
     *             返回: 无返回值；诊断唤醒失败不改变容量事实。
     */
    private static void runWake(Runnable wake) {
        if (wake == null) return;
        try {
            wake.run();
        } catch (Throwable ignored) {
            // 唤醒只是缩短等待，runner 的正常轮询仍会再次观察容量。
        }
    }

    private record Waiter(int count, Runnable wake) {
    }

    /**
     * 业务作用：表达一次读取预留或 retained 状态对 raw record 容量的唯一所有权。
     */
    static final class Permit {

        private final PartitionRecordCapacity owner;
        private final AtomicBoolean released = new AtomicBoolean();
        private int held;
        final PartitionReadReservation downstream;

        /**
         * 业务作用：绑定一次已经从协调器扣减的读取容量。
         *
         * @param owner 所属协调器
         * @param held  初始预留数量
         * @param downstream 同域执行和恢复预留；source 模式可为空
         *              返回: 由唯一批次所有者负责缩减和释放的 Permit。
         */
        private Permit(PartitionRecordCapacity owner, int held, PartitionReadReservation downstream) {
            this.owner = owner;
            this.downstream = downstream;
            this.held = held;
        }

        /**
         * 业务作用：把读取前的最大预留缩减为本来源真实返回数，超出时登记容量债务而不截断事实。
         *
         * @param actual 本轮实际返回记录数
         *               返回: 无返回值；零结果会释放全部预留。
         */
        synchronized void resizeToActual(int actual) {
            if (released.get()) throw new IllegalStateException("record permit already released");
            if (actual < 0) throw new IllegalArgumentException("actual record count must not be negative");
            if (actual < held) {
                int unused = held - actual;
                held = actual;
                owner.release(unused);
            } else if (actual > held) {
                owner.addDebt(actual - held);
                held = actual;
            }
            if (downstream != null) downstream.resize(actual);
            if (held == 0) { released.set(true); if (downstream != null) downstream.close(); }
        }

        /**
         * 业务作用：归还当前仍持有的全部 raw record 容量。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；重复调用保持幂等。
         */
        synchronized void release() {
            if (!released.compareAndSet(false, true)) return;
            if (downstream != null) downstream.close();
            int count = held;
            held = 0;
            if (count > 0) owner.release(count);
        }

        /**
         * 业务作用：返回当前 Permit 仍持有的真实记录数，供 retained 状态观测。
         *
         * <p>参数说明: 无。
         *
         * @return 尚未归还的记录数量
         */
        synchronized int held() {
            return held;
        }
    }
}

/**
 * 业务作用：维护单个来源在 pending poll、活动批次和 retained 状态之间的唯一 Permit 交接。
 */
final class PartitionSourceRecordState {

    private final PartitionRecordCapacity capacity;
    private final StreamPartitionRuntime.PartitionSource sourceId;
    private final Runnable wake;
    private PartitionRecordCapacity.Permit pending;
    private PartitionRecordCapacity.Permit activeBatch;
    private StreamSourceAuthority.Snapshot pendingAuthority;
    private StreamSourceAuthority.Snapshot activeAuthority;
    private boolean closed;

    /**
     * 业务作用：绑定来源身份、容量协调器和 runner 唤醒动作。
     *
     * @param capacity 共享 raw record 容量
     * @param sourceId 来源稳定身份
     * @param wake     容量变化时的 runner 唤醒动作
     *                 返回: 初始不持有任何容量的来源状态。
     */
    PartitionSourceRecordState(PartitionRecordCapacity capacity, StreamPartitionRuntime.PartitionSource sourceId, Runnable wake) {
        this.capacity = capacity;
        this.sourceId = sourceId;
        this.wake = wake;
    }

    /**
     * 业务作用：在 Redis 读取前冻结权威并取得最大批量预留，使正文与容量始终属于同一代次。
     *
     * @param batchSize 当前来源有效 batch-size
     * @return 原快照有效且已有或本次取得预留时返回 true；来源关闭、权威失效、仍有活动批次或容量暂不可满足时返回 false
     */
    synchronized boolean tryAcquire(int batchSize) {
        if (closed) return false;
        // 周期恢复可与尚未完成的原始批次重叠；保留原 Permit，等待该批真实结束后再读取。
        if (activeBatch != null) return false;
        if (pending != null) return pendingAuthority.allowsExecution();
        // Redis 读取之前冻结权威，迟到正文只能继承该快照，不能在收到结果后换成新 holder。
        StreamSourceAuthority.Snapshot authority = sourceId.authority().snapshot();
        if (!authority.allowsExecution()) return false;
        pending = capacity.tryReserve(sourceId, batchSize, wake);
        if (pending != null && !authority.allowsExecution()) {
            pending.release();
            pending = null;
            return false;
        }
        pendingAuthority = pending == null ? null : authority;
        return pending != null;
    }

    /**
     * 业务作用：把读取数量与原快照一起交接给活动批次，失权不换发身份；零结果直接清空本轮所有权。
     *
     * @param recordCount 本来源本轮真实返回数
     *                    返回: 无返回值；来源关闭后忽略迟到结果，缺少读取前预留时拒绝建立虚假批次所有权。
     */
    synchronized void onPollResult(int recordCount) {
        if (closed) return;
        PartitionRecordCapacity.Permit permit = pending;
        pending = null;
        StreamSourceAuthority.Snapshot authority = pendingAuthority;
        pendingAuthority = null;
        if (permit == null) throw new IllegalStateException("poll result has no reserved record permit");
        permit.resizeToActual(recordCount);
        if (recordCount > 0) {
            activeBatch = permit;
            activeAuthority = authority;
        }
    }

    /**
     * 业务作用：正常批次完成时释放仍归活动批次所有的 Permit；已转交 retained 状态时保持 no-op。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；重复调用保持幂等。
     */
    synchronized void afterBatchComplete() {
        PartitionRecordCapacity.Permit permit = activeBatch;
        activeBatch = null;
        activeAuthority = null;
        if (permit != null) permit.release();
    }

    /**
     * 业务作用：读取或结果交接失败时释放 pending 与活动批次两类本地所有权。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；没有容量时保持幂等。
     */
    synchronized void afterPollFailure() {
        pendingAuthority = null;
        activeAuthority = null;
        PartitionRecordCapacity.Permit p = pending;
        pending = null;
        if (p != null) p.release();
        PartitionRecordCapacity.Permit active = activeBatch;
        activeBatch = null;
        if (active != null) active.release();
    }

    /**
     * 业务作用：把活动批次的 Permit 原子转交给 retained recovery 状态，外层 batch finally 随后不得释放它。
     *
     * <p>参数说明: 无。
     *
     * @return retained 状态唯一持有的 Permit
     */
    synchronized PartitionRecordCapacity.Permit retainActiveBatch() {
        PartitionRecordCapacity.Permit permit = activeBatch;
        if (permit == null) return null;
        activeBatch = null;
        activeAuthority = null;
        return permit;
    }

    /**
     * 业务作用：把读取前冻结的权威交给正文处理或 PEL 恢复，禁止按当前来源重新解释已读数据。
     * 参数说明: 无。
     * @return 活动批次或待读取的原快照；没有读取所有权时返回 null
     */
    synchronized StreamSourceAuthority.Snapshot readAuthority() {
        return activeBatch == null ? pendingAuthority : activeAuthority;
    }

    /** 业务作用：提供当前批次原预留供执行接续，不转移 raw 所有权。参数说明: 无。返回: 可为空的预留。 */
    synchronized PartitionReadReservation readReservation() { return activeBatch == null ? null : activeBatch.downstream; }

    /**
     * 业务作用：来源退出时撤销公平等待并归还尚未交接的容量。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；终态之后拒绝重新预留，retained 状态已经取得的 Permit 由对应恢复责任方继续持有。
     */
    synchronized void close() {
        closed = true;
        pendingAuthority = null;
        activeAuthority = null;
        capacity.removeWaiter(sourceId);
        PartitionRecordCapacity.Permit p = pending;
        pending = null;
        if (p != null) p.release();
        PartitionRecordCapacity.Permit active = activeBatch;
        activeBatch = null;
        if (active != null) active.release();
    }
}
