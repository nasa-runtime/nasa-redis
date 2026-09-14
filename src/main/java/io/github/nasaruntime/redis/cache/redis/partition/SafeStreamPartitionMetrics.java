package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务作用：隔离可选指标后端异常，观测注册、记录或撤销失败不得改变消费、确认与停机状态机。
 */
public final class SafeStreamPartitionMetrics implements StreamPartitionMetrics {

    private static final int CLOSED_MASK = Integer.MIN_VALUE;
    private static final int ACTIVE_MASK = Integer.MAX_VALUE;
    private final StreamPartitionMetrics delegate;
    /**
     * 代理拥有的桥接器把已获准回调和真实后端撤销纳入同一代理终态；独立包装时为 null。
     */
    private final RedisProxy lifecycleOwner;
    /**
     * 高位表示关闭终态，其余位记录已获准的调用数，使回调与关闭不会交叉穿透。
     */
    private final AtomicInteger invocationState = new AtomicInteger();
    /**
     * 识别第三方回调在当前线程内同步请求关闭，此时由最外层调用完成撤销。
     */
    private final ThreadLocal<Integer> invocationDepth = ThreadLocal.withInitial(() -> 0);
    private final AtomicBoolean delegateCloseStarted = new AtomicBoolean();
    private final CountDownLatch delegateCloseComplete = new CountDownLatch(1);
    private volatile Thread delegateCloseOwner;

    /**
     * 业务作用：包装一个真实指标后端。参数说明: 被隔离的后端与可选生命周期归属。返回: 安全代理。
     */
    private SafeStreamPartitionMetrics(StreamPartitionMetrics delegate, RedisProxy lifecycleOwner) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lifecycleOwner = lifecycleOwner;
    }

    /**
     * 业务作用：为未新增代理归属的真实后端建立安全代理，使其自身关闭与观测异常保持隔离。
     * 参数说明: 指标后端。
     * 返回: NOOP、保留原归属的已有安全代理或不带 lifecycleOwner 的新安全代理。
     */
    public static StreamPartitionMetrics wrap(StreamPartitionMetrics metrics) {
        if (metrics == NOOP || metrics instanceof SafeStreamPartitionMetrics) return metrics;
        return new SafeStreamPartitionMetrics(metrics, null);
    }

    /**
     * 业务作用：为代理拥有的指标后端建立安全代理，使回调与撤销均参与所属代理的终态屏障。
     * 参数说明: 指标后端与生命周期所属代理。
     * 返回: NOOP、已有同归属安全代理或新建安全代理。
     */
    public static StreamPartitionMetrics wrap(StreamPartitionMetrics metrics, RedisProxy lifecycleOwner) {
        if (metrics == NOOP) return metrics;
        if (metrics instanceof SafeStreamPartitionMetrics safeMetrics
                && safeMetrics.lifecycleOwner == lifecycleOwner) {
            return metrics;
        }
        return new SafeStreamPartitionMetrics(metrics, Objects.requireNonNull(lifecycleOwner, "lifecycleOwner"));
    }

    /**
     * 业务作用：安全登记分区入口。参数说明: 分区入口。返回: 无返回值。
     */
    @Override
    public void partitionAvailable(RedisPartition partition) {
        invokeSafely(() -> delegate.partitionAvailable(partition));
    }

    /**
     * 业务作用：安全登记逻辑组。参数说明: 分区入口与组名。返回: 无返回值。
     */
    @Override
    public void groupAvailable(RedisPartition partition, String groupName) {
        invokeSafely(() -> delegate.groupAvailable(partition, groupName));
    }

    /**
     * 业务作用：安全登记订阅计划。参数说明: 分区入口与计划。返回: 无返回值。
     */
    @Override
    public void planAvailable(RedisPartition partition, StreamSubscriptionPlan plan) {
        invokeSafely(() -> delegate.planAvailable(partition, plan));
    }

    /**
     * 业务作用：安全记录 partitionKey 观测。参数说明: 计划、类型、结果与耗时。返回: 无返回值。
     */
    @Override
    public void partitionKey(StreamSubscriptionPlan plan, String kind, String result, long elapsedNanos) {
        invokeSafely(() -> delegate.partitionKey(plan, kind, result, elapsedNanos));
    }

    /**
     * 业务作用：安全记录批量观测。参数说明: 来源、阶段、恢复标志与数值。返回: 无返回值。
     */
    @Override
    public void batch(StreamRecordSource source, String stage, boolean recovery, long value) {
        invokeSafely(() -> delegate.batch(source, stage, recovery, value));
    }

    /**
     * 业务作用：安全记录 ordered bucket。参数说明: 计划与数量。返回: 无返回值。
     */
    @Override
    public void orderedBucket(StreamSubscriptionPlan plan, long size) {
        invokeSafely(() -> delegate.orderedBucket(plan, size));
    }

    /**
     * 业务作用：安全记录 Task 终态。参数说明: 计划、顺序属性与终态。返回: 无返回值。
     */
    @Override
    public void taskOutcome(StreamSubscriptionPlan plan, boolean ordered, ConsumeStatus outcome) {
        invokeSafely(() -> delegate.taskOutcome(plan, ordered, outcome));
    }

    /**
     * 业务作用：安全记录 Task 阶段耗时。参数说明: 计划、顺序属性、阶段与耗时。返回: 无返回值。
     */
    @Override
    public void taskLatency(StreamSubscriptionPlan plan, boolean ordered, String stage, long elapsedNanos) {
        invokeSafely(() -> delegate.taskLatency(plan, ordered, stage, elapsedNanos));
    }

    /**
     * 业务作用：安全记录 Submission 结果。参数说明: 计划、顺序属性与结果。返回: 无返回值。
     */
    @Override
    public void submission(StreamSubscriptionPlan plan, boolean ordered, String result) {
        invokeSafely(() -> delegate.submission(plan, ordered, result));
    }

    /**
     * 业务作用：安全记录恢复结果。参数说明: 来源、类型与结果。返回: 无返回值。
     */
    @Override
    public void recovery(StreamRecordSource source, String type, String result) {
        invokeSafely(() -> delegate.recovery(source, type, result));
    }

    /**
     * 业务作用：安全记录确认结果。参数说明: 来源、阶段与结果。返回: 无返回值。
     */
    @Override
    public void ack(StreamRecordSource source, String stage, String result) {
        invokeSafely(() -> delegate.ack(source, stage, result));
    }

    /**
     * 业务作用：发布指标桥接器的不可逆终态，并在已获准回调退出后撤销全部 meter。
     * 参数说明: 无。
     * 返回: 无返回值；普通调用等待撤销完成，指标回调内的同步重入由最外层回调在返回前完成撤销。
     */
    @Override
    public void close() {
        int state = invocationState.get();
        while (state >= 0 && !invocationState.compareAndSet(state, state | CLOSED_MASK)) {
            state = invocationState.get();
        }
        int closedState = state < 0 ? state : state | CLOSED_MASK;
        if ((closedState & ACTIVE_MASK) == 0) closeDelegateOnce();
        if (invocationDepth.get() > 0 || delegateCloseOwner == Thread.currentThread()) return;
        awaitDelegateClose();
    }

    /**
     * 业务作用：在关闭终态前为一次观测动作取得执行资格，并将第三方异常隔离在业务状态机之外。
     *
     * @param action 指标后端动作
     *               返回: 无返回值；关闭终态后直接忽略。
     */
    private void invokeSafely(Runnable action) {
        if (!enterInvocation()) return;
        if (lifecycleOwner != null) lifecycleOwner.beginMetricsLifecycleActivity();
        try {
            safely(action);
        } finally {
            try {
                leaveInvocation();
            } finally {
                if (lifecycleOwner != null) lifecycleOwner.completeMetricsLifecycleActivity();
            }
        }
    }

    /**
     * 业务作用：原子登记一次已获准指标调用，使关闭能等待其离开外部后端。
     * 参数说明: 无。
     * 返回: 获得资格时为 true；关闭已发布时为 false。
     */
    private boolean enterInvocation() {
        int state = invocationState.get();
        while (state >= 0) {
            if ((state & ACTIVE_MASK) == ACTIVE_MASK) return false;
            if (invocationState.compareAndSet(state, state + 1)) {
                invocationDepth.set(invocationDepth.get() + 1);
                return true;
            }
            state = invocationState.get();
        }
        return false;
    }

    /**
     * 业务作用：归还指标调用资格，并由最后离开的调用承担延后撤销责任。
     * 参数说明: 无。
     * 返回: 无返回值。
     */
    private void leaveInvocation() {
        int depth = invocationDepth.get() - 1;
        if (depth == 0) invocationDepth.remove();
        else invocationDepth.set(depth);
        if (invocationState.decrementAndGet() == CLOSED_MASK) closeDelegateOnce();
    }

    /**
     * 业务作用：唯一执行真实后端撤销，且不在任何桥接器状态锁内调用第三方实现。
     * 参数说明: 无。
     * 返回: 无返回值；其他调用者由完成信号观测同一结果。
     */
    private void closeDelegateOnce() {
        if (!delegateCloseStarted.compareAndSet(false, true)) return;
        if (lifecycleOwner != null) lifecycleOwner.beginMetricsLifecycleActivity();
        delegateCloseOwner = Thread.currentThread();
        try {
            safely(delegate::close);
        } finally {
            delegateCloseOwner = null;
            delegateCloseComplete.countDown();
            if (lifecycleOwner != null) lifecycleOwner.completeMetricsLifecycleActivity();
        }
    }

    /**
     * 业务作用：等待真实后端已完成唯一撤销，不因中断放宽生命周期返回边界。
     * 参数说明: 无。
     * 返回: 无返回值；恢复调用线程原有中断标志。
     */
    private void awaitDelegateClose() {
        boolean interrupted = false;
        while (true) {
            try {
                delegateCloseComplete.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    /**
     * 业务作用：吞掉仅观测后端的异常。参数说明: 指标动作。返回: 无返回值。
     */
    private static void safely(Runnable action) {
        try {
            action.run();
        } catch (Throwable ignored) {
            // 指标不是消费权威，后端异常不能阻断业务结果、ACK 或 owner 排干。
        }
    }
}
