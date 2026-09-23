package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * 业务作用：在一把公平锁下原子预留 Task、确认 attempt 与确认 record 容量，避免不同批次各持部分配额互相等待。
 */
final class PartitionDispatchCapacity {

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition changed = lock.newCondition();
    private final int totalTasks;
    private final int totalCommitAttempts;
    private final int totalCommitRecords;
    private int availableTasks;
    private int availableCommitAttempts;
    private int availableCommitRecords;

    /**
     * 业务作用：创建本 RedisProxy 独占的本地执行与确认硬容量。
     *
     * @param totalTasks          在飞 Partition Task 上限
     * @param totalCommitAttempts 在飞确认尝试上限
     * @param totalCommitRecords  确认尝试保存的 record id 上限
     *                            返回: 初始全部配额可用的容量协调器。
     */
    PartitionDispatchCapacity(int totalTasks, int totalCommitAttempts, int totalCommitRecords) {
        if (totalTasks < 1 || totalCommitAttempts < 1 || totalCommitRecords < 1) {
            throw new IllegalArgumentException("dispatch capacities must be greater than zero");
        }
        this.totalTasks = totalTasks;
        this.totalCommitAttempts = totalCommitAttempts;
        this.totalCommitRecords = totalCommitRecords;
        this.availableTasks = totalTasks;
        this.availableCommitAttempts = totalCommitAttempts;
        this.availableCommitRecords = totalCommitRecords;
    }

    /**
     * 业务作用：在来源仍有权执行时一次取得整批 Task 与最大确认需求，禁止先占 gate 再等待剩余容量。
     *
     * @param tasks      本批实际 Task 数
     * @param ackRecords 本批最大可能确认的物理 record 数
     * @param admission  来源与组件 admission 是否仍开放
     * @return 完整持有本批组合配额的 Reservation
     */
    Reservation acquire(int tasks, int ackRecords, BooleanSupplier admission) {
        if (tasks < 0 || ackRecords < 0) throw new IllegalArgumentException("capacity demand must not be negative");
        int attempts = ackRecords == 0 ? 0 : 1;
        if (tasks > totalTasks || ackRecords > totalCommitRecords || attempts > totalCommitAttempts) {
            throw new OversizedBatchException(tasks, ackRecords, totalTasks, totalCommitRecords);
        }
        boolean interrupted = false;
        lock.lock();
        try {
            while (availableTasks < tasks
                    || availableCommitAttempts < attempts
                    || availableCommitRecords < ackRecords) {
                if (!admission.getAsBoolean()) throw new AdmissionClosedException();
                try {
                    changed.awaitNanos(100_000_000L);
                } catch (InterruptedException signal) {
                    interrupted = true;
                }
            }
            if (!admission.getAsBoolean()) throw new AdmissionClosedException();
            availableTasks -= tasks;
            availableCommitAttempts -= attempts;
            availableCommitRecords -= ackRecords;
            return new Reservation(this, tasks, attempts, ackRecords, interrupted);
        } finally {
            lock.unlock();
            if (interrupted && !Thread.currentThread().isInterrupted()) Thread.currentThread().interrupt();
        }
    }

    /**
     * 业务作用：在读取前非阻塞取得最坏逐条 Task 和确认份额，失败不保留任何部分资源。
     * @param count 最大物理记录数
     * @return 完整预留，容量暂满时为 null
     */
    Reservation tryReserveRead(int count) { return tryReserve(count, count); }

    /**
     * 业务作用：恢复执行不持有重试槽等待其它恢复的预留，完整容量不可得时返回退避。
     * @param tasks 实际任务数
     * @param records 最大确认数量
     * @return 完整预留或 null
     */
    Reservation tryReserve(int tasks, int records) {
        if (tasks > totalTasks || records > totalCommitRecords)
            throw new OversizedBatchException(tasks, records, totalTasks, totalCommitRecords);
        if (!lock.tryLock()) return null;
        try {
            if (availableTasks < tasks || availableCommitAttempts < 1 || availableCommitRecords < records) return null;
            availableTasks -= tasks;
            availableCommitAttempts--;
            availableCommitRecords -= records;
            return new Reservation(this, tasks, 1, records, false);
        } finally { lock.unlock(); }
    }

    /**
     * 业务作用：归还一笔组合配额并唤醒公平等待队列重新评估完整需求。
     *
     * @param tasks      归还的 Task 数
     * @param attempts   归还的确认 attempt 数
     * @param ackRecords 归还的确认 record 数
     *                   返回: 无返回值；越过任一总容量表示重复释放并拒绝提交该事实。
     */
    private void release(int tasks, int attempts, int ackRecords) {
        lock.lock();
        try {
            int nextTasks = availableTasks + tasks;
            int nextAttempts = availableCommitAttempts + attempts;
            int nextRecords = availableCommitRecords + ackRecords;
            if (nextTasks > totalTasks || nextAttempts > totalCommitAttempts || nextRecords > totalCommitRecords) {
                throw new IllegalStateException("dispatch capacity ownership released more than once");
            }
            availableTasks = nextTasks;
            availableCommitAttempts = nextAttempts;
            availableCommitRecords = nextRecords;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在组件或来源 admission 改变时立即唤醒组合容量等待者，使其复验权威并退出等待。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值。
     */
    void wakeAdmissionWaiters() {
        lock.lock();
        try {
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：读取 Task 与确认硬容量的当前使用量，供部署侧识别饱和而不暴露 record id。
     *
     * <p>参数说明: 无。
     *
     * @return 固定低基数名称到当前值的不可变映射
     */
    Map<String, Long> snapshot() {
        lock.lock();
        try {
            return Map.of(
                    "task_capacity_used", (long) totalTasks - availableTasks,
                    "commit_attempt_capacity_used", (long) totalCommitAttempts - availableCommitAttempts,
                    "commit_record_capacity_used", (long) totalCommitRecords - availableCommitRecords);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：表达一批已经原子取得的 Task 与确认容量，并允许 gate 分流后缩减未提交 Task 数。
     */
    static final class Reservation implements AutoCloseable {

        private final PartitionDispatchCapacity owner;
        private final AtomicBoolean closed = new AtomicBoolean();
        private int tasks;
        private int attempts;
        private int records;
        private final boolean restoreInterrupt;

        /**
         * 业务作用：绑定一批已经从协调器扣减的组合容量。
         *
         * @param owner            所属协调器
         * @param tasks            Task 配额
         * @param attempts         确认 attempt 配额
         * @param records          确认 record 配额
         * @param restoreInterrupt 等待期间是否观察到中断
         *                         返回: 由批次或确认状态唯一关闭的 Reservation。
         */
        private Reservation(PartitionDispatchCapacity owner,
                            int tasks,
                            int attempts,
                            int records,
                            boolean restoreInterrupt) {
            this.owner = owner;
            this.tasks = tasks;
            this.attempts = attempts;
            this.records = records;
            this.restoreInterrupt = restoreInterrupt;
        }

        /**
         * 业务作用：在 ordered gate 拦下部分 unit 后立即归还不会提交的 Task 配额。
         *
         * @param count 本批明确不会形成 Task 的数量
         *              返回: 无返回值；超过剩余持有量时拒绝。
         */
        synchronized void releaseUnusedTasks(int count) {
            if (count < 0 || count > tasks) throw new IllegalArgumentException("invalid unused task count");
            if (count == 0) return;
            tasks -= count;
            owner.release(count, 0, 0);
        }

        /**
         * 业务作用：在本批没有确认候选时立即归还全部确认容量，Task 结果仍由批次继续收口。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；已经归还时保持幂等。
         */
        synchronized void releaseCommitCapacity() {
            int a = attempts;
            int r = records;
            attempts = 0;
            records = 0;
            if (a != 0 || r != 0) owner.release(0, a, r);
        }

        /**
         * 业务作用：把最大确认 record 预留缩减为实际候选数，保留一个 attempt 直到远端结论明确。
         *
         * @param actualRecords 实际进入 holder-fenced 确认的 id 数
         *                      返回: 无返回值；没有候选时同时释放 attempt。
         */
        synchronized void shrinkCommitRecords(int actualRecords) {
            if (actualRecords < 0 || actualRecords > records) {
                throw new IllegalArgumentException("actual commit records exceed reservation");
            }
            int unused = records - actualRecords;
            records = actualRecords;
            int unusedAttempt = actualRecords == 0 ? attempts : 0;
            if (actualRecords == 0) attempts = 0;
            if (unused != 0 || unusedAttempt != 0) owner.release(0, unusedAttempt, unused);
        }

        /**
         * 业务作用：把实际确认候选的 attempt 与 record 容量转交给持久 CommitAttempt，
         * 使原批次返回后 ACK 不确定态仍受硬上限与停机排干保护。
         *
         * @param actualRecords 转交的精确 record id 数
         * @return 由 CommitAttempt 唯一关闭的容量租约
         */
        synchronized CommitLease transferCommitCapacity(int actualRecords) {
            if (closed.get()) throw new IllegalStateException("dispatch reservation already closed");
            if (actualRecords < 1 || actualRecords > records || attempts < 1) {
                throw new IllegalArgumentException("actual commit records exceed reservation");
            }
            int unused = records - actualRecords;
            records = 0;
            attempts--;
            if (unused > 0) owner.release(0, 0, unused);
            return new CommitLease(owner, actualRecords);
        }

        /**
         * 业务作用：报告容量等待期间是否观察到线程中断，供最外层完成安全收口后恢复中断标志。
         *
         * <p>参数说明: 无。
         *
         * @return 等待期间至少收到一次中断时返回 true
         */
        boolean restoreInterrupt() {
            return restoreInterrupt;
        }

        /**
         * 业务作用：一次性归还仍由本批持有的全部组合容量。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；重复关闭保持幂等。
         */
        @Override
        public synchronized void close() {
            if (!closed.compareAndSet(false, true)) return;
            int t = tasks;
            int a = attempts;
            int r = records;
            tasks = 0;
            attempts = 0;
            records = 0;
            if (t != 0 || a != 0 || r != 0) owner.release(t, a, r);
        }
    }

    /**
     * 业务作用：在批次之外继续持有一个确认 attempt 及其精确 record 容量。
     */
    static final class CommitLease implements AutoCloseable {

        private final PartitionDispatchCapacity owner;
        private final int records;
        private final AtomicBoolean closed = new AtomicBoolean();

        /**
         * 业务作用：接管批次已预留的确认容量。
         *
         * @param owner   容量所属协调器
         * @param records 持有的 record 数
         *                返回: 尚未释放的独占租约。
         */
        private CommitLease(PartitionDispatchCapacity owner, int records) {
            this.owner = owner;
            this.records = records;
        }

        /**
         * 业务作用：在全部 id 明确确认、迁移或交回 PEL 后归还硬容量。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；重复关闭保持幂等。
         */
        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) owner.release(0, 1, records);
        }
    }

    /**
     * 业务作用：表示单批需求超过总容量，等待不会改变结果。
     */
    static final class OversizedBatchException extends RuntimeException {
        /**
         * 业务作用：记录实际与总容量需求。参数说明: Task/record 实际值及上限。返回: 容量拒绝异常。
         */
        OversizedBatchException(int tasks, int records, int totalTasks, int totalRecords) {
            super("partition batch exceeds capacity: tasks=" + tasks + "/" + totalTasks
                    + ", records=" + records + "/" + totalRecords);
        }
    }

    /**
     * 业务作用：表示来源在取得完整组合容量前已经关闭 admission。
     */
    static final class AdmissionClosedException extends RuntimeException {
        /**
         * 业务作用：创建来源 admission 已关闭结论。参数说明: 无。返回: 不再等待容量的异常。
         */
        AdmissionClosedException() {
            super("partition source admission is closed");
        }
    }
}
