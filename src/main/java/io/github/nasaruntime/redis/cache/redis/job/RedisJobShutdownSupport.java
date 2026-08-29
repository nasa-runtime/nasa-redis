package io.github.nasaruntime.redis.cache.redis.job;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 业务作用：为 RedisJob 停机步骤汇总失败并协调并发调用方观测同一完成结果。
 */
final class RedisJobShutdownSupport {

    private static final ThreadLocal<InterruptDeferral> INTERRUPT_DEFERRAL = new ThreadLocal<>();

    /**
     * 业务作用：禁止实例化停机协调工具。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：不会正常构造实例。
     */
    private RedisJobShutdownSupport() {
    }

    /**
     * 业务作用：尝试执行一项停机动作，保留最早失败并收集后续失败证据。
     *
     * @param failure 已有失败，可为 null
     * @param action  必须尝试的停机动作
     * @return 全部成功时返回 null，否则返回带 suppressed 链的最早失败。
     */
    static Throwable attempt(Throwable failure, Runnable action) {
        try {
            action.run();
        } catch (Throwable next) {
            if (failure == null) return next;
            if (failure != next) failure.addSuppressed(next);
        }
        return failure;
    }

    /**
     * 业务作用：为多个已发布的停机失败创建当前调用栈的汇总结果，不改写任一子结果。
     *
     * @param message  汇总失败说明
     * @param failures 按发生顺序排列的失败
     * @return 无失败时返回 null，单个失败时返回原对象，多个失败时返回新建的本地汇总对象。
     */
    static Throwable aggregate(String message, List<? extends Throwable> failures) {
        if (failures.isEmpty()) return null;
        if (failures.size() == 1) return failures.getFirst();
        Throwable first = failures.getFirst();
        Throwable aggregate = first instanceof Error
                ? new Error(message, first)
                : new IllegalStateException(message, first);
        for (int index = 1; index < failures.size(); index++) {
            aggregate.addSuppressed(failures.get(index));
        }
        return aggregate;
    }

    /**
     * 业务作用：在资源停机结果完成后执行当前调用方的生命周期回调，并隔离回调局部失败。
     *
     * <p>返回：资源与回调均成功时正常返回；只有一方失败时抛出原对象，两者均失败时抛出新建的当前调用汇总结果。
     *
     * @param shutdownFailure 已发布的资源停机失败，可为 null
     * @param callback        当前调用方的完成回调
     */
    static void completeCallback(Throwable shutdownFailure, Runnable callback) {
        Throwable callbackFailure = null;
        try {
            callback.run();
        } catch (Throwable failure) {
            callbackFailure = failure;
        }
        Throwable combined = aggregate("RedisJob shutdown and lifecycle callback failed",
                shutdownFailure == null
                        ? callbackFailure == null ? List.of() : List.of(callbackFailure)
                        : callbackFailure == null ? List.of(shutdownFailure)
                        : List.of(shutdownFailure, callbackFailure));
        rethrow(combined);
    }

    /**
     * 业务作用：等待唯一停机执行者完成全部资源收口，避免重复调用方提前宣布停机完成。
     *
     * <p>返回：完成信号到达后返回；等待期的中断状态会在返回前恢复。
     *
     * @param completion 停机完成信号
     */
    static void await(CountDownLatch completion) {
        try (InterruptDeferral ignored = deferInterrupts()) {
            while (completion.getCount() != 0L) {
                try {
                    completion.await();
                } catch (InterruptedException interrupted) {
                    // 停机完成是对外资源安全边界；中断只延后到最外层边界后交还，不能让回调抢跑。
                    captureInterrupt();
                }
            }
        }
    }

    /**
     * 业务作用：建立可嵌套的停机中断延迟域，使计数排空、执行器终止与 Registry 注销共享同一安全边界。
     *
     * <p>参数说明: 无。
     *
     * @return 当前线程的中断延迟域；最外层关闭时恢复期间捕获的中断状态。
     */
    static InterruptDeferral deferInterrupts() {
        InterruptDeferral deferral = INTERRUPT_DEFERRAL.get();
        if (deferral == null) {
            deferral = new InterruptDeferral();
            INTERRUPT_DEFERRAL.set(deferral);
        }
        deferral.depth++;
        if (Thread.interrupted()) deferral.interrupted = true;
        return deferral;
    }

    /**
     * 业务作用：把 interrupt 记录到当前停机延迟域，避免在资源实际终止前恢复线程中断状态。
     *
     * <p>参数说明: 无。
     *
     * @return 当前线程存在中断延迟域时为 true；调用方此时应继续等待安全边界。
     */
    static boolean captureInterrupt() {
        InterruptDeferral deferral = INTERRUPT_DEFERRAL.get();
        if (deferral == null) return false;
        deferral.interrupted = true;
        return true;
    }

    /**
     * 业务作用：从当前单调时钟建立一次 Scheduler 停机共享的绝对截止，避免各步骤重复获得完整等待预算。
     *
     * @param timeoutMs 停机允许的总等待毫秒数
     * @return 可安全比较的绝对纳秒截止；加法溢出时封顶为 {@link Long#MAX_VALUE}。
     */
    static long deadlineAfterMillis(long timeoutMs) {
        long now = System.nanoTime();
        long duration = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeoutMs));
        try {
            return Math.addExact(now, duration);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * 业务作用：计算共享停机截止的剩余毫秒，供只接受相对时限的资源账目复用同一预算。
     *
     * @param deadlineNanos 绝对单调时钟截止
     * @return 尚未耗尽时向上取整的剩余毫秒；截止已到时为零。
     */
    static long remainingMillis(long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0L) return 0L;
        long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
        return millis == 0L ? 1L : millis;
    }

    /**
     * 业务作用：建立停机时丢弃尚未到期调度项的控制执行器，避免关闭后的扫描、超时或续租任务延迟资源终止。
     *
     * <p>已经开始的任务仍按 {@link ExecutorService#shutdown()} 语义执行完毕；尚未到期的一次性任务和
     * 周期任务不再承担业务职责，关闭时必须取消，否则较长调度间隔会越过 Scheduler 的停机期限。
     *
     * @param poolSize      控制执行器的平台线程数
     * @param threadFactory 具有稳定业务命名与 daemon 属性的线程工厂
     * @return 已配置关闭策略的调度执行器。
     */
    static ScheduledThreadPoolExecutor scheduledExecutor(int poolSize, ThreadFactory threadFactory) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(poolSize, threadFactory);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /**
     * 业务作用：在共享绝对截止内等待一个已请求关闭的执行器终止，防止迟到回调越过资源注销边界。
     *
     * @param executor      已调用 shutdown 的执行器
     * @param deadlineNanos Scheduler 停机共享的绝对单调时钟截止
     * @param resourceName  稳定资源名，用于失败定位
     * @return 执行器在截止前终止时返回；超时时抛出明确停机失败，中断延迟到最外层资源边界后恢复。
     */
    static void awaitTermination(ExecutorService executor, long deadlineNanos, String resourceName) {
        try (InterruptDeferral ignored = deferInterrupts()) {
            while (!executor.isTerminated()) {
                // Long.MAX_VALUE 表示 final cleanup 的无期限安全等待；单独处理可避免 nanoTime 为负时减法溢出。
                long remaining = deadlineNanos == Long.MAX_VALUE
                        ? Long.MAX_VALUE
                        : deadlineNanos - System.nanoTime();
                if (remaining <= 0L) {
                    throw new IllegalStateException(
                            resourceName + " did not terminate before RedisJob shutdown deadline");
                }
                try {
                    if (executor.awaitTermination(remaining, TimeUnit.NANOSECONDS)) return;
                } catch (InterruptedException interrupted) {
                    // 执行器终止与成员注销属于同一边界；只记录中断，继续使用原绝对截止等待。
                    captureInterrupt();
                }
            }
            if (!executor.isTerminated()) {
                throw new IllegalStateException(resourceName + " did not terminate before RedisJob shutdown deadline");
            }
        }
    }

    /**
     * 业务作用：在最外层停机资源边界结束后统一恢复延迟的线程中断状态。
     */
    static final class InterruptDeferral implements AutoCloseable {
        private int depth;
        private boolean interrupted;

        /**
         * 业务作用：关闭一层中断延迟域，最外层结束时恢复当前线程的中断标志。
         *
         * <p>参数说明: 无。
         *
         * <p>返回：无返回值；嵌套域尚未全部关闭时继续保持中断隐藏。
         */
        @Override
        public void close() {
            if (depth <= 0) throw new IllegalStateException("RedisJob interrupt deferral is already closed");
            if (--depth != 0) return;
            if (Thread.interrupted()) interrupted = true;
            INTERRUPT_DEFERRAL.remove();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /**
     * 业务作用：把已完成收口的停机失败按原类型交还调用方。
     *
     * <p>返回：{@code failure} 为 null 时正常返回，否则抛出并不会返回。
     *
     * @param failure 停机结果，可为 null
     */
    static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        if (failure != null) throw new IllegalStateException(failure);
    }
}
