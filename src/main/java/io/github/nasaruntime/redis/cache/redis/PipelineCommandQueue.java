package io.github.nasaruntime.redis.cache.redis;

import java.io.Serial;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 业务作用：为代理命令的实际入队与最终关闭提供原子边界，确保每个槽位都有唯一的排干或拒绝收口者。
 */
final class PipelineCommandQueue extends ConcurrentLinkedQueue<PipelineTask> {

    @Serial
    private static final long serialVersionUID = 601576036130857714L;

    private final ReentrantReadWriteLock admissionBarrier = new ReentrantReadWriteLock();
    private boolean closed;

    /**
     * 业务作用：创建开放准入的代理命令队列，供并发生产者移交槽位所有权。
     * <p>
     * 参数说明: 无。
     * 返回: 空队列；直到显式关闭前可以接收命令。
     */
    PipelineCommandQueue() {}

    /**
     * 业务作用：在线性化门禁内接受一个命令；终态后到达的槽位由当前调用确定失败并归还。
     *
     * @param task 已填充命令参数、可选结果 future 的池化槽位
     * @return 接纳时为 true；关闭后为 false，future 异常完成且槽位被归还，调用方不得重复回收。
     */
    @Override
    public boolean offer(PipelineTask task) {
        Objects.requireNonNull(task, "task");
        admissionBarrier.readLock().lock();
        try {
            if (!closed) return super.offer(task);
        } finally {
            admissionBarrier.readLock().unlock();
        }
        // 失败通知与池化清理不持有队列门禁，避免外部完成逻辑重入生命周期时阻塞最终关闭。
        task.reject(new RejectedExecutionException("Redis pipeline command admission is closed"));
        return false;
    }

    /**
     * 业务作用：使批量移交也逐个经过真实准入门禁，防止父类批量链表拼接绕过终态。
     *
     * @param tasks 依序移交所有权的池化命令槽位，不得为本队列自身
     * @return 至少一个命令被接纳时为 true；关闭后到达的其余命令逐个失败并归还。
     */
    @Override
    public boolean addAll(Collection<? extends PipelineTask> tasks) {
        Objects.requireNonNull(tasks, "tasks");
        if (tasks == this) throw new IllegalArgumentException("Cannot add a pipeline queue to itself");
        boolean accepted = false;
        for (PipelineTask task : tasks) accepted |= offer(task);
        return accepted;
    }

    /**
     * 业务作用：封闭命令的实际入队边界，使最终消费者面对一个不再增长的已接纳集合。
     * <p>
     * 参数说明: 无。
     * 返回: 无返回值；此前已完成入队的槽位保留待排干，后续入队只执行拒绝收口，重复调用保持幂等。
     */
    void closeAdmission() {
        admissionBarrier.writeLock().lock();
        try {
            // 写锁等待所有已开始的入队发布完毕，终态后不能再出现无人接管的迟到 future。
            closed = true;
        } finally {
            admissionBarrier.writeLock().unlock();
        }
    }
}
