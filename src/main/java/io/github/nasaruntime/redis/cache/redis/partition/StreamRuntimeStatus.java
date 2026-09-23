package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 业务作用：集中维护 Stream Partition bridge 的低基数运行计数、readiness 原因与停机排干门禁。
 */
final class StreamRuntimeStatus {

    enum Resource {BATCH, TASK, COMMIT, ACK_IO, RETRY, RECOVERY, REBALANCE}

    private static final long[] LISTENER_LATENCY_BUCKETS_MILLIS = {
            1L, 5L, 10L, 25L, 50L, 100L, 250L, 500L,
            1_000L, 2_500L, 5_000L, 10_000L, 30_000L, Long.MAX_VALUE
    };

    private final ConcurrentHashMap<String, Boolean> readinessFailures = new ConcurrentHashMap<>();
    private final AtomicLong batches = new AtomicLong();
    private final AtomicLong tasks = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong commitUnknown = new AtomicLong();
    private final AtomicLong routeBlocked = new AtomicLong();
    private final AtomicLong pelTombstones = new AtomicLong();
    private final AtomicLong drainTimeouts = new AtomicLong();
    private final AtomicLong lateTaskOutcomes = new AtomicLong();
    private final AtomicLongArray listenerLatency = new AtomicLongArray(LISTENER_LATENCY_BUCKETS_MILLIS.length);
    private final ReentrantLock drainLock = new ReentrantLock();
    private final Condition drained = drainLock.newCondition();
    private final long[] resources = new long[Resource.values().length];
    private final List<Runnable> drainedCallbacks = new ArrayList<>();
    private boolean admissionOpen = true;

    /**
     * 业务作用：为一个运行资源建立排干所有权，停机必须等它明确释放。
     *
     * @param resource 资源类型
     * @return 只能释放一次的排干令牌
     */
    DrainToken enter(Resource resource) {
        drainLock.lock();
        try {
            resources[resource.ordinal()]++;
            return new DrainToken(this, resource);
        } finally {
            drainLock.unlock();
        }
    }

    /**
     * 业务作用：将新批次或后台动作的责任登记与关闭准入放在同一门禁内，防止终态后出现未被排干覆盖的责任。
     *
     * @param resource 新获准的资源类型
     * @return 准入开放时返回必须成对释放的令牌，关闭后返回 null 且不增加计数。
     */
    DrainToken tryEnter(Resource resource) {
        drainLock.lock();
        try {
            // 关闭后只允许已登记责任派生的收口动作，新来源与新批次不能重新扩张执行域。
            if (!admissionOpen) return null;
            resources[resource.ordinal()]++;
            return new DrainToken(this, resource);
        } finally {
            drainLock.unlock();
        }
    }

    /**
     * 业务作用：读取全部业务和 Redis I/O 责任是否已经归零，供运行时复验真实终态。
     * <p>
     * 参数说明: 无。
     *
     * @return 当前全部资源计数为零时返回 true；调用方仍须单独确认准入已关闭及执行器已终止。
     */
    boolean isDrained() {
        drainLock.lock();
        try {
            return allZero();
        } finally {
            drainLock.unlock();
        }
    }

    /**
     * 业务作用：关闭新批次、Task、重试与再平衡 admission，已登记确认 I/O 仍可完成收口。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；重复调用保持幂等。
     */
    void closeAdmission() {
        drainLock.lock();
        try {
            admissionOpen = false;
            readinessFailures.put("admission_closed", Boolean.TRUE);
            drained.signalAll();
        } finally {
            drainLock.unlock();
        }
    }

    /**
     * 业务作用：在有限停机预算内等待所有排干资源归零。
     *
     * @param timeoutMillis 最长等待毫秒数
     * @return 预算内全部归零时返回 true
     */
    boolean awaitDrained(long timeoutMillis) {
        long remaining = TimeUnit.MILLISECONDS.toNanos(Math.max(1L, timeoutMillis));
        boolean interrupted = false;
        drainLock.lock();
        try {
            while (!allZero() && remaining > 0L) {
                try {
                    remaining = drained.awaitNanos(remaining);
                } catch (InterruptedException signal) {
                    interrupted = true;
                }
            }
            if (!allZero()) {
                readinessFailures.put("drain_timeout", Boolean.TRUE);
                drainTimeouts.incrementAndGet();
            }
            return allZero();
        } finally {
            drainLock.unlock();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /**
     * 业务作用：在全部排干所有权归零后执行收尾动作，使停机超时不能提前销毁仍被迟到任务使用的执行域。
     *
     * @param callback 仅执行一次的收尾动作
     *                 返回: 无返回值；登记时已经归零则在协调器锁外立即执行。
     */
    void runWhenDrained(Runnable callback) {
        boolean runNow;
        drainLock.lock();
        try {
            runNow = allZero();
            if (!runNow) drainedCallbacks.add(callback);
        } finally {
            drainLock.unlock();
        }
        if (runNow) callback.run();
    }

    /**
     * 业务作用：发布会关闭消费 readiness 的稳定原因，不把异常消息当作高基数标签。
     *
     * @param reason 稳定原因码
     *               返回: 无返回值。
     */
    void failReadiness(String reason) {
        readinessFailures.put(reason, Boolean.TRUE);
    }

    /**
     * 业务作用：在对应条件已消失时撤销 readiness 原因。
     *
     * @param reason 稳定原因码
     *               返回: 无返回值。
     */
    void clearReadiness(String reason) {
        readinessFailures.remove(reason);
    }

    /**
     * 业务作用：返回不携带 record id、业务 key 或异常文本的运行快照。
     *
     * <p>参数说明: 无。
     *
     * @return 用于健康检查和指标桥接的低基数数值
     */
    Map<String, Long> snapshot() {
        LinkedHashMap<String, Long> result = new LinkedHashMap<>();
        result.put("ready", readinessFailures.isEmpty() && admissionOpen ? 1L : 0L);
        result.put("readiness_failures", (long) readinessFailures.size());
        result.put("batches", batches.get());
        result.put("tasks", tasks.get());
        result.put("retries", retries.get());
        result.put("commit_unknown", commitUnknown.get());
        result.put("route_blocked", routeBlocked.get());
        result.put("pel_tombstones", pelTombstones.get());
        result.put("drain_timeouts", drainTimeouts.get());
        result.put("late_task_outcomes", lateTaskOutcomes.get());
        long listenerObservations = listenerObservations();
        result.put("listener_observations", listenerObservations);
        result.put("listener_p99_ms", listenerPercentileMillis(listenerObservations, 99L));
        drainLock.lock();
        try {
            for (Resource resource : Resource.values()) {
                result.put("in_flight_" + resource.name().toLowerCase(), resources[resource.ordinal()]);
            }
        } finally {
            drainLock.unlock();
        }
        return Map.copyOf(result);
    }

    /**
     * 业务作用：累计已进入 dispatcher 的批次。参数说明: 无。返回: 无返回值。
     */
    void recordBatch() {
        batches.incrementAndGet();
    }

    /**
     * 业务作用：累计实际提交的 Partition Task。参数说明: 新增数量。返回: 无返回值。
     */
    void recordTasks(long count) {
        tasks.addAndGet(count);
    }

    /**
     * 业务作用：累计取得执行权的精确重试。参数说明: 无。返回: 无返回值。
     */
    void recordRetry() {
        retries.incrementAndGet();
    }

    /**
     * 业务作用：累计进入 ACK 不确定态的 attempt。参数说明: 无。返回: 无返回值。
     */
    void recordCommitUnknown() {
        commitUnknown.incrementAndGet();
    }

    /**
     * 业务作用：累计因整条解析无法安全路由的 record。参数说明: 新增数量。返回: 无返回值。
     */
    void recordRouteBlocked(long count) {
        routeBlocked.addAndGet(count);
    }

    /**
     * 业务作用：累计正文已缺失但 PEL 仍存在的坐标。参数说明: 无。返回: 无返回值。
     */
    void recordPelTombstone() {
        pelTombstones.incrementAndGet();
    }

    /**
     * 业务作用：累计来源失权后才被 dispatcher 观察到的 Task 终态。参数说明: 无。返回: 无返回值。
     */
    void recordLateTaskOutcome() {
        lateTaskOutcomes.incrementAndGet();
    }

    /**
     * 业务作用：把一次真实 listener 调用耗时归入固定低基数直方图，避免订阅或业务 key 扩大指标维度。
     *
     * @param elapsedNanos listener 调用经过的单调时钟纳秒数
     *                     返回: 无返回值。
     */
    void recordListenerLatency(long elapsedNanos) {
        long elapsedMillis = Math.max(0L, TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
        for (int index = 0; index < LISTENER_LATENCY_BUCKETS_MILLIS.length; index++) {
            if (elapsedMillis <= LISTENER_LATENCY_BUCKETS_MILLIS[index]) {
                listenerLatency.incrementAndGet(index);
                return;
            }
        }
    }

    /**
     * 业务作用：累计固定直方图内已经观察到的 listener 调用数。
     *
     * <p>参数说明: 无。
     *
     * @return 进程内累计观察数
     */
    private long listenerObservations() {
        long total = 0L;
        for (int index = 0; index < listenerLatency.length(); index++) total += listenerLatency.get(index);
        return total;
    }

    /**
     * 业务作用：从固定直方图计算近似延迟分位，供启动与健康日志表达接管阈值是否覆盖已观察业务时长。
     *
     * @param observations 当前总观察数
     * @param percentile   需要计算的整数分位
     * @return 对应桶上界毫秒数；尚无观察时返回 -1
     */
    private long listenerPercentileMillis(long observations, long percentile) {
        if (observations == 0L) return -1L;
        long wholeHundreds = observations / 100L;
        long remainder = observations % 100L;
        long target = Math.max(1L,
                wholeHundreds * percentile + (remainder * percentile + 99L) / 100L);
        long cumulative = 0L;
        for (int index = 0; index < listenerLatency.length(); index++) {
            cumulative += listenerLatency.get(index);
            if (cumulative >= target) return LISTENER_LATENCY_BUCKETS_MILLIS[index];
        }
        return LISTENER_LATENCY_BUCKETS_MILLIS[LISTENER_LATENCY_BUCKETS_MILLIS.length - 1];
    }

    /**
     * 业务作用：归还一个排干资源并唤醒停机等待者。
     *
     * @param resource 需要归还的资源类型
     *                 返回: 无返回值；重复释放由 DrainToken 拦截。
     */
    private void leave(Resource resource) {
        List<Runnable> callbacks = List.of();
        drainLock.lock();
        try {
            long next = --resources[resource.ordinal()];
            if (next < 0L) {
                resources[resource.ordinal()]++;
                throw new IllegalStateException("Stream runtime drain resource released more than once");
            }
            if (allZero()) {
                drained.signalAll();
                if (!drainedCallbacks.isEmpty()) {
                    callbacks = List.copyOf(drainedCallbacks);
                    drainedCallbacks.clear();
                }
            }
        } finally {
            drainLock.unlock();
        }
        for (Runnable callback : callbacks) callback.run();
    }

    /**
     * 业务作用：判断排干表内所有资源是否归零；调用方必须持有 drainLock。
     *
     * <p>参数说明: 无。
     *
     * @return 所有类型均无所有权时返回 true
     */
    private boolean allZero() {
        for (long count : resources) if (count != 0L) return false;
        return true;
    }

    /**
     * 业务作用：表达一笔 exactly-once 排干所有权。
     */
    static final class DrainToken implements AutoCloseable {

        private final StreamRuntimeStatus owner;
        private final Resource resource;
        private final AtomicBoolean closed = new AtomicBoolean();

        /**
         * 业务作用：绑定资源表中已登记的一笔所有权。
         *
         * @param owner    所属运行状态
         * @param resource 资源类型
         *                 返回: 未关闭的令牌。
         */
        private DrainToken(StreamRuntimeStatus owner, Resource resource) {
            this.owner = owner;
            this.resource = resource;
        }

        /**
         * 业务作用：一次性归还对应排干资源。
         *
         * <p>参数说明: 无。
         * 返回: 无返回值；重复关闭保持幂等。
         */
        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) owner.leave(resource);
        }
    }
}
