package io.github.nasaruntime.redis.cache.redis.job;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 业务作用：统计普通与 Fanout 派发在取得 Redis attempt 前的在途回调，并为停机提供无空窗的交接屏障。
 */
final class RedisJobPreStartBarrier {

    private final Object monitor = new Object();
    private boolean closed;
    private int inflight;

    /**
     * 业务作用：在派发回调进入 Redis 权威读取前登记在途身份，终态发布后拒绝新的回调进入。
     *
     * <p>参数说明: 无。
     *
     * @return 成功登记时返回必须关闭的凭据；停机终态已提交时返回 null。
     */
    Lease enter() {
        synchronized (monitor) {
            if (closed) return null;
            inflight++;
            return new Lease(this);
        }
    }

    /**
     * 业务作用：不可逆地封闭普通与 Fanout 的 pre-start 入口，使后到回调不能再请求执行权。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：终态发布后返回；已经登记的回调继续完成无空窗交接。
     */
    void closeAdmission() {
        synchronized (monitor) {
            closed = true;
        }
    }

    /**
     * 业务作用：判断停机是否已经封闭 pre-start 入口，供 Redis 回包后选择排空交接语义。
     *
     * <p>参数说明: 无。
     *
     * @return 不可逆终态已经提交时为 true。
     */
    boolean isClosed() {
        synchronized (monitor) {
            return closed;
        }
    }

    /**
     * 业务作用：等待所有已进入 Redis 边界的回调退出，或先登记为 Handler 在途后完成交接。
     *
     * @param deadlineNanos Scheduler 停机共享的绝对单调时钟截止
     * @return 截止前完成交接为 true；达到截止或线程被中断为 false。
     */
    boolean awaitIdle(long deadlineNanos) {
        synchronized (monitor) {
            while (inflight > 0) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0L) return false;
                try {
                    TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * 业务作用：在首次停机期限耗尽后继续等待全部 pre-start 回调完成退出或在途交接。
     *
     * <p>参数说明: 无。
     *
     * @return 全部回调完成交接时为 true；等待期间的中断在完成后恢复，不会提前越过资源边界。
     */
    boolean awaitIdle() {
        try (RedisJobShutdownSupport.InterruptDeferral ignored = RedisJobShutdownSupport.deferInterrupts()) {
            synchronized (monitor) {
                while (inflight > 0) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException interrupted) {
                        // final cleanup 是 Redis 依赖可销毁的安全边界；中断只能延后到最外层资源边界后交还。
                        RedisJobShutdownSupport.captureInterrupt();
                    }
                }
            }
        }
        return true;
    }

    /**
     * 业务作用：完成一个 pre-start 回调的退出或 Handler 在途交接，并唤醒停机等待者。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：无返回值；重复完成不重复扣减账目。
     */
    private void leave() {
        synchronized (monitor) {
            inflight--;
            if (inflight == 0) monitor.notifyAll();
        }
    }

    /**
     * 业务作用：代表一个必须完成的 pre-start 在途身份，保证异常分支也能释放屏障账目。
     */
    static final class Lease implements AutoCloseable {
        private final RedisJobPreStartBarrier owner;
        private final AtomicBoolean completed = new AtomicBoolean();

        /**
         * 业务作用：绑定本次回调与共享屏障。
         *
         * <p>返回：创建尚未完成、关闭时只扣减一次在途账目的凭据。
         *
         * @param owner 共享 pre-start 屏障
         */
        private Lease(RedisJobPreStartBarrier owner) {
            this.owner = owner;
        }

        /**
         * 业务作用：把当前回调从 pre-start 账目移除，表示已退出或已经登记进 Handler 在途账目。
         *
         * <p>参数说明: 无。
         *
         * <p>返回：首次调用完成扣减；重复调用不产生副作用。
         */
        @Override
        public void close() {
            if (completed.compareAndSet(false, true)) owner.leave();
        }
    }
}
