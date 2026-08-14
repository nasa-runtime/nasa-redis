package io.github.nasaruntime.redis.cache.redis.stream;

import io.github.nasaruntime.core.base.AnyHolder;
import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.base.RecycleLinkedList;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.ConsumerStreamReadRequest;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamReadRequest;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.util.ErrorHandler;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A poll task that supports batch delivery via {@link BatchStreamListener}
 * and ManagedRunner batch-poll integration.
 * <p>
 * Two execution modes:
 * <ul>
 *   <li><b>Standalone doLoop (non-group)</b>: 独立线程运行 {@link #run()}, 内部 {@link #doLoop()} 用 BLOCK
 *       readFunction 拉取单 stream，供非 group 订阅（XREAD with $）使用。</li>
 *   <li><b>ManagedRunner 批量托管 (group)</b>: 不调 {@link #run()}, 由 ManagedRunner 通过
 *       {@link #initManaged()} / {@link #handleBatchResult} / {@link #checkAlive()} / {@link #exitManaged}
 *       生命周期方法驱动. 多 stream 在 runner 一次 xReadGroup 拉到, 按 streamKey demux 到各 task。</li>
 * </ul>
 * 当 listener 是 {@link BatchStreamListener} 时整批一次性投递, 否则降级到逐条投递。
 *
 * @param <K> Stream key type.
 * @param <V> Stream value type.
 */
class BatchStreamPollTask<K, V extends Record<K, ?>> implements NasaStreamTask {

    private static final long CONTAINER_STOP_DRAIN_TIMEOUT_MS =
            Long.getLong("nasa.batch-stream.drain-timeout-ms", 30_000L);

    /**
     * 原始订阅请求。主要给 ManagedRunner 批量路径用 — 提取 streamKey / consumer / autoAck 元信息;
     * 非 group doLoop 路径仅在构造期通过它生成 readFunction, 之后不再直接读, 通过 readFunction / pollState 操作。
     */
    private final StreamReadRequest<K> streamRequest;
    /** 缓存的 stream key (与 streamRequest 同源), batch demux 热路径上避免每次穿透到 streamRequest */
    private final K streamKey;
    private final StreamListener<K, V> listener;
    private final ErrorHandler errorHandler;
    private final Predicate<Throwable> cancelSubscriptionOnError;
    /**
     * 当前 task 的读取函数。非 group 模式下实现同时是 {@link AutoCloseable}，
     * 持有任务级独占阻塞连接；group 模式下是无状态的非阻塞读取函数。
     */
    private final Function<ReadOffset, List<ByteRecord>> readFunction;
    private final Function<ByteRecord, V> deserializer;

    private final PollState pollState;
    private final TypeDescriptor targetType;
    /** 可选的 task 生命周期钩子（tryLock/holds/unlock 等），不传则使用 {@link PollLifecycle#NOOP}。 */
    final PollLifecycle lifecycle;
    /** 是否是 group 订阅 (ConsumerStreamReadRequest). group → ManagedRunner 托管; 非 group → 独立线程 */
    private final boolean groupTask;

    private volatile boolean isInEventLoop = false;

    // ==================== 托管模式字段 (ManagedRunner 读写) ====================

    /**
     * tryLock 是否成功。true → 已拿到锁, 走正常 poll; false → 未拿到锁, 等重试。
     */
    boolean managedSuccess = false;
    /**
     * 上次 tryLock 重试时间 (ms)。managedSuccess=false 时记录, runner 每 10s 重试一次。
     */
    long lastRetryTime = 0;
    /**
     * 业务处理完成标识。true = 上一批已处理完 (或还没派发过), 可以加入下一轮 batch poll;
     * false = 业务正在 businessExecutor 上处理中, runner 跳过本 task 不进 batch。
     */
    volatile boolean complete = true;
    /**
     * 上次 poll 是否拉到了数据。true → runner 下一轮立即再拉 (热流零延迟); false → 等 pollTimeout 再拉。
     */
    boolean lastHadData = false;
    /**
     * 上次拉取时间 (ms)。lastHadData=false 时记录, runner 用 now - lastPollTime &gt;= pollTimeout 判断是否该拉。
     */
    long lastPollTime = 0;
    /**
     * 业务处理完成后的唤醒回调。ManagedRunner 在 drainPending 时设置为 unpark(runnerThread),
     * 让 runner 从 parkNanos 中立即醒来, 不用傻等 pollTimeout。
     * task 退出时 {@link #exitManaged} 会清空本字段断引用环。
     */
    volatile Runnable onComplete;
    private volatile boolean containerStopRequested;
    private volatile long containerStopTime;

    /**
     * 业务作用：判断本任务是否以消费组身份读取。
     * 组模式下消息进入待处理列表并需要确认，独立模式下不产生待处理记录。
     *
     * <p>参数说明: 无。
     *
     * @return 以消费组身份读取返回 true。
     */
    boolean isGroupTask() {
        return groupTask;
    }

    /**
     * 业务作用：task 是否已就绪可以加入下一轮 batch poll. 透传 {@link PollLifecycle#isPollReady()},
     * 给 ManagedRunner 在 task 异步初始化 (如 RedisPartition.Claim 的异步 XAUTOCLAIM recoverPending)
     * 期间跳过 batch 收集, 只走 checkAlive 维持存活自检。
     *
     * @return 见上述说明。
     */
    boolean isPollReady() {
        return lifecycle.isPollReady();
    }

    /**
     * 业务作用：判断是否已收到停止请求，循环每轮据此决定是否继续。
     *
     * <p>参数说明: 无。
     *
     * @return 已请求停止返回 true。
     */
    boolean isStopRequested() {
        return containerStopRequested || lifecycle.isStopRequested();
    }

    /**
     * 业务作用：判断本任务对其分区的占用是否已失效。
     * <p>
     * 占用一旦失效必须<b>立即停止消费</b>：此刻另一个节点可能已接管该分区，
     * 继续读取会让同一分区被两个节点同时消费，分区内保序随即被破坏。
     *
     * <p>参数说明: 无。
     *
     * @return 占用已失效返回 true。
     */
    boolean isLockLost() {
        return lifecycle.isLockLost();
    }

    /**
     * 业务作用：判断收尾排空是否已超时。
     * 停止时要把手上已读取的消息处理完再退出，但不能无限等待——业务处理卡住时会拖住整个停机流程。
     *
     * @param now 当前时刻
     * @return 排空已超时返回 true。
     */
    boolean drainTimedOut(long now) {
        if (lifecycle.drainTimedOut(now)) {
            return true;
        }
        return containerStopRequested
                && containerStopTime > 0
                && now - containerStopTime >= CONTAINER_STOP_DRAIN_TIMEOUT_MS;
    }

    /**
     * 业务作用：只做占用有效性自检而不读取消息，用于空闲轮次。
     * <p>
     * 自检本身要访问 Redis，因此按最小间隔限流：不限流会让无消息时的空转把服务端的脚本执行打满。
     *
     * <p>参数说明: 无。
     *
     * @return 占用仍然有效返回 true。
     */
    boolean checkAliveHoldsOnly() {
        try {
            return lifecycle.checkAliveHoldsOnly();
        } catch (Throwable t) {
            errorHandler.handleError(new RuntimeException("checkAliveHoldsOnly threw", t));
            return false;
        }
    }

    /**
     * 业务作用：扩展构造: 接受 {@link PollLifecycle} 钩子。给分区消费等需要在 pollTask 关键点上插
     * 自定义初始化/自检/清理逻辑的场景用。
     *
     * @param streamRequest 见上述说明
     * @param listener      见上述说明
     * @param errorHandler  见上述说明
     * @param targetType    见上述说明
     * @param readFunction  见上述说明
     * @param deserializer  见上述说明
     * @param lifecycle     见上述说明
     */
    BatchStreamPollTask(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener, ErrorHandler errorHandler,
                        TypeDescriptor targetType, Function<ReadOffset, List<ByteRecord>> readFunction,
                        Function<ByteRecord, V> deserializer, PollLifecycle lifecycle) {

        this.streamRequest = streamRequest;
        this.streamKey = streamRequest.getStreamOffset().getKey();
        this.listener = listener;
        this.errorHandler = Optional.ofNullable(streamRequest.getErrorHandler()).orElse(errorHandler);
        this.cancelSubscriptionOnError = streamRequest.getCancelSubscriptionOnError();
        this.readFunction = readFunction;
        this.deserializer = deserializer;
        this.pollState = createPollState(streamRequest);
        this.targetType = targetType;
        this.lifecycle = lifecycle != null ? lifecycle : PollLifecycle.NOOP;
        this.groupTask = streamRequest instanceof ConsumerStreamReadRequest;
    }

    /**
     * 业务作用：按读取请求建出轮询状态：消费者身份、位点推进策略与当前位点。
     *
     * @param streamRequest 读取请求
     * @return 轮询状态。
     */
    private static PollState createPollState(StreamReadRequest<?> streamRequest) {

        StreamOffset<?> streamOffset = streamRequest.getStreamOffset();

        if (streamRequest instanceof ConsumerStreamReadRequest) {
            return PollState.consumer(((ConsumerStreamReadRequest<?>) streamRequest).getConsumer(), streamOffset.getOffset());
        }

        return PollState.standalone(streamOffset.getOffset());
    }

    /**
     * 业务作用：取消本任务并等待其真正停止。
     * 等待而非发出指令即返回：调用方随后可能要释放连接，任务若仍在读取会在连接关闭时抛异常刷屏。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     * @throws org.springframework.dao.DataAccessResourceFailureException 停止过程中访问失败
     */
    @Override
    public void cancel() throws DataAccessResourceFailureException {
        this.pollState.cancel();
        // 外部 stop/cancel 可能发生在 XREAD BLOCK 期间。先标记取消，再关闭独占连接，
        // 既阻止下一轮重建，也让当前阻塞读取尽快返回。
        closeReadFunction();
    }

    /**
     * 业务作用：置停止标志但不等待，供批量停止时先向全部任务发出信号再统一等待，避免逐个串行等待。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     * @throws org.springframework.dao.DataAccessResourceFailureException 请求过程中访问失败
     */
    void requestStop() throws DataAccessResourceFailureException {
        if (!containerStopRequested) {
            containerStopTime = System.currentTimeMillis();
            containerStopRequested = true;
        }
        cancel();
    }

    /**
     * 业务作用：读取本任务当前所处的生命周期阶段。
     *
     * <p>参数说明: 无。
     *
     * @return 生命周期阶段。
     */
    @Override
    public State getState() {
        return pollState.getState();
    }

    /**
     * 业务作用：限时等待任务真正进入运行态。
     * 容器据此确认订阅确实已生效——只发出启动指令就返回，调用方无从得知消费是否真的开始。
     *
     * @param timeout 等待时长
     * @return 在时限内进入运行态返回 true。
     * @throws InterruptedException 等待期间线程被中断
     */
    @Override
    public boolean awaitStart(Duration timeout) throws InterruptedException {
        return pollState.awaitStart(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * 业务作用：声明本任务是长驻任务，容器据此按长期占用一条线程来调度它。
     *
     * <p>参数说明: 无。
     *
     * @return 恒为 true。
     */
    @Override
    public boolean isLongLived() {
        return true;
    }

    /**
     * 业务作用：任务主体：置运行态、进入拉取循环、退出时收尾。
     * 无论循环因何种原因结束都走收尾：不收尾会让占用锁与连接一直挂着，直到超时才被别的节点接管。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void run() {
        // normal 区分两种退出: true = doLoop 完整跑完后正常退出; false = beforeStart 拒绝进入 / 异常崩溃
        // 用作 afterExit(normal) 参数, 让钩子知道退出语义, 决定是否需要 unlock 等
        boolean normal = false;
        try {
            pollState.starting();

            try {
                isInEventLoop = true;
                pollState.running();
                // === lifecycle hook 1/3: beforeStart ===
                // 在虚拟线程的"task 跑起来了, 但还没开始拉消息"这个唯一时机做线程级初始化:
                // 比如 LettuceDistributedLock.tryLock + XAUTOCLAIM 接管前任 pending。
                // 返回 false → 不进 doLoop, 直接走 finally 让 afterExit 收尾。
                if (!lifecycle.beforeStart()) {
                    pollState.cancel();
                    return;
                }
                doLoop();
                normal = true;
            } finally {
                isInEventLoop = false;
            }
        } catch (Throwable e) {
            errorHandler.handleError(new RuntimeException("BatchStreamPollTask crashed", e));
        } finally {
            // === lifecycle hook 3/3: afterExit ===
            // task 退出唯一收口处, 不论正常/异常/beforeStart 拒绝都会到这里。
            // 钩子内部异常交给 errorHandler，仍继续关闭 read function，避免清理分支中断任务退出收口。
            try {
                try {
                    lifecycle.afterExit(normal);
                } catch (Throwable e) {
                    errorHandler.handleError(new RuntimeException("PollLifecycle.afterExit threw", e));
                }
            } finally {
                closeReadFunction();
            }
        }
    }

    /**
     * 业务作用：拉取循环：每轮先自检占用有效性，再阻塞读取一批消息，解码后投递业务回调。
     * <p>
     * 自检在读取之前：占用已失效却继续读取，会让同一分区被两个节点同时消费。
     * <p>
     * 读取用阻塞方式而非轮询：无消息时阻塞等待避免空转，同时把停机响应延迟限制在一次阻塞时长内。
     * <p>
     * 业务回调抛异常时<b>不推进位点也不确认</b>，消息将被重新投递，因此业务处理必须幂等。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    private void doLoop() {

        do {
            try {
                // allow interruption
                Thread.sleep(0);

                // === lifecycle hook 2/3: beforePoll ===
                // 每个 poll 周期一次的轻量自检. 典型用法: 每 N 批做一次 holds() 检测,
                // 防长 GC 后锁丢失自己还在拉。返回 false → cancel + 当轮跳出 doLoop。
                if (!lifecycle.beforePoll()) {
                    cancel();
                    break;
                }

                List<ByteRecord> raw = readFunction.apply(pollState.getCurrentReadOffset());
                try {
                    deserializeAndEmitRecords(raw);
                } finally {
                    // 线程级上下文兜底清理 (与 handleBatchResult 路径对称)
                    AnyHolder.clear();
                }

            } catch (InterruptedException ex) {
                cancel();
                Thread.currentThread().interrupt();
            } catch (RuntimeException ex) {
                // cancel/stop 通过关闭独占连接唤醒 XREAD 时会得到连接异常，这是正常退出信号，
                // 不应打印错误，也不能重新进入下一轮读取。
                if (!pollState.isSubscriptionActive()) {
                    break;
                }

                if (cancelSubscriptionOnError.test(ex)) {
                    cancel();
                }

                errorHandler.handleError(ex);
            }
        } while (pollState.isSubscriptionActive());
    }

    /**
     * 业务作用：关闭读取函数持有的任务级资源。
     * <p>
     * 只有非 group 的 {@code PersistentBlockingStreamReadFunction} 实现 {@link AutoCloseable}；
     * group 非阻塞函数不持有连接，因此本方法对 group task 是 no-op。关闭实现必须幂等，因为
     * cancel 线程和 poll task 的 finally 都可能调用本方法。
     */
    private void closeReadFunction() {
        if (!(readFunction instanceof AutoCloseable closeable)) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception failure) {
            errorHandler.handleError(new RuntimeException("Failed to close blocking stream reader", failure));
        }
    }

    // ==================== 托管模式: 外部 ManagedRunner 调用 ====================

    /**
     * 业务作用：初始化 task: 调用 lifecycle.beforeStart (tryLock + 异步 recoverPending 等)。
     * 托管模式下由 ManagedRunner 在首次 batch poll 收集本 task 之前调用, 运行在 runner 线程内
     * (保证 lifecycle 内 tryLock 的 holder threadId 与后续 holds/unlock 一致)。
     *
     * @return true → 初始化成功 (tryLock 拿到), 可以加入 batch; false → 失败, runner 等 RETRY_INTERVAL_MS 重试
     */
    boolean initManaged() {
        pollState.starting();
        isInEventLoop = true;
        pollState.running();
        return lifecycle.beforeStart();
    }

    // ==================== 批量路径访问器 (ManagedRunner 用) ====================

    /**
     * 业务作用：当前 (stream, offset) 二元组, 给 ManagedRunner 拼 batch xReadGroup 用。
     * <p>
     * pollState.currentOffset 是 volatile, 在 deserializeAndEmitRecords 内部 updateReadOffset
     * 写入。runner 读 → batch poll → handleBatchResult 设 complete=false → businessExecutor 异步
     * 处理 + 写 offset → 完成时设 complete=true。complete=false 期间 runner 不再调本方法,
     * 所以 read/write 不会并发, volatile 保证可见性即可。
     *
     * @return 见上述说明。
     */
    StreamOffset<K> currentStreamOffset() {
        return StreamOffset.create(streamKey, pollState.getCurrentReadOffset());
    }

    /**
     * 业务作用：读取本任务负责的 Stream 键。
     *
     * <p>参数说明: 无。
     *
     * @return Stream 键。
     */
    K streamKey() {
        return streamKey;
    }

    /**
     * 业务作用：group 订阅的 consumer (group + consumer-name 二元组). 非 group 订阅返回 null。
     * ManagedRunner 按 (group, consumer-name, autoAck) 做亲和分配, 同一个 runner 内的 task
     * 必须共享相同 consumer 语义, 才能安全合并为一次 XREADGROUP。
     */
    Consumer rawConsumer() {
        return streamRequest instanceof ConsumerStreamReadRequest<K> csr ? csr.getConsumer() : null;
    }

    /**
     * 业务作用：判断是否在投递后自动确认。
     * 自动确认下消息一读出就不会重投，业务处理失败时<b>消息随之丢失</b>；
     * 不能容忍丢失的场景必须关闭它并在处理成功后手工确认。
     *
     * <p>参数说明: 无。
     *
     * @return 自动确认返回 true。
     */
    boolean isAutoAcknowledge() {
        return streamRequest instanceof ConsumerStreamReadRequest<K> csr && csr.isAutoAcknowledge();
    }

    /**
     * 业务作用：处理 batch 拉到的本 task 的 records 子集 (已经过 streamKey demux 切片)。
     * <p>
     * 空 records → 视为本周期未拉到, 标 lastHadData=false + 推进 lastPollTime;
     * 非空 → complete=false 标记 in-flight, 异步派发到 businessExecutor 做反序列化 + listener.onMessage,
     * 完成时 complete=true + onComplete unpark runner 让其立即拉下一批。
     * <p>
     * <b>串行性</b>: complete=false 期间 ManagedRunner 不会再调 currentStreamOffset 也不会再
     * handleBatchResult 本 task, 所以同一 task 的 batch 永远串行处理, 不会出现 N 批同时跑。
     * <p>
     * <b>资源所有权</b>: records 是 batchPollAndDemux 从 ObjectPool 取的 RecycleLinkedList,
     * 业务 lambda 在 finally 中 recycle (通过 {@link ObjectPool.Recycler} instanceof 检测, 兼容
     * 调用方传普通 List 的情况）。executor 拒绝路径同步走外层 finally 兜底 recycle 防泄漏。
     *
     * @param records  见上述说明
     * @param executor 见上述说明
     * @param now      见上述说明
     */
    void handleBatchResult(List<ByteRecord> records, Executor executor, long now) {
        if (records == null || records.isEmpty()) {
            lastHadData = false;
            lastPollTime = now;
            // 即使 empty, records 仍是 ObjectPool 借出的实例, 需要回收
            recycleRecords(records);
            return;
        }
        lastHadData = true;
        complete = false;
        // submitted 标记区分"已交给 executor"vs"executor 拒绝/抛异常"两种结局, 后者必须本地兜底
        // recycle records + 重置 complete=true, 否则 records 池泄漏 + task 卡死 (complete 永远 false → runner 不再 poll)
        boolean submitted = false;
        try {
            executor.execute(() -> {
                try {
                    deserializeAndEmitRecords(records);
                } catch (RuntimeException ex) {
                    errorHandler.handleError(ex);
                } finally {
                    // 线程级上下文兜底清理: listener 内部可能往 AnyHolder 写入业务上下文,
                    // 必须在 listener 结束后清除, 防止复用线程时上下文泄漏到下一批消息.
                    AnyHolder.clear();
                    // 顺序: complete=true (volatile, 一定成功) → recycle 归池 → cb.run() 唤醒 runner
                    // 先归池让 runner 醒来时池里有热实例可拿, 避免瞬时 new RecycleLinkedList
                    complete = true;
                    recycleRecords(records);
                    // 唤醒 ManagedRunner, 让它立即 poll 下一批, 不用傻等 pollTimeout
                    Runnable cb = onComplete;
                    if (cb != null) cb.run();
                }
            });
            submitted = true;
        } catch (Throwable t) {
            // executor 拒绝 (RejectedExecutionException / 关闭态适配器 / 装饰器异常) 必须就地吞掉:
            // 不能让异常冒泡到 ManagedRunner 外层 catch — 那会让整个 runner 退出, finally 清掉本 runner
            // 名下所有 partition task, 一次 reject 打死全部分区消费. 交 errorHandler 记录, 下面 finally 兜底 recycle.
            errorHandler.handleError(new RuntimeException("BatchStreamPollTask submit batch failed", t));
        } finally {
            if (!submitted) {
                // executor 拒绝 → lambda 永远不会跑,
                // 这里同步 recycle records 并恢复 complete=true 防 task 卡死。
                // 这批消息因此本节点没处理, 留在 PEL 等下一轮 / 别节点 XAUTOCLAIM 重投。
                complete = true;
                recycleRecords(records);
                Runnable cb = onComplete;
                if (cb != null) cb.run();
            }
        }
    }

    /**
     * 业务作用：回收 batch records 链表回 ObjectPool。
     * 用 instanceof 判定: batch 路径下传入的是 RecycleLinkedList 实现 {@link ObjectPool.Recycler},
     * 单测 / 兼容路径若直接传普通 List 也不会报错。
     *
     * @param records 见上述说明
     */
    private static void recycleRecords(List<ByteRecord> records) {
        if (records instanceof ObjectPool.Recycler<?> r) {
            r.recycle();
        }
    }

    // ==================== 共用 ====================

    /**
     * 业务作用：由 ManagedRunner 在两个时机调用做存活自检 + holds 检查, 不实际拉消息:
     * <ul>
     *   <li>就绪 task 进 batch 之前 — 防止用已丢锁的 consumer 发 xReadGroup</li>
     *   <li>业务 in-flight (complete=false) / 异步初始化中 (isPollReady=false) 期间 — 周期性
     *       自检防长 GC / 业务长跑后锁丢失, 而别节点已 XAUTOCLAIM 接管, 本节点继续业务最后
     *       ack 已不属于自己的 PEL</li>
     * </ul>
     * 内部调 {@link PollLifecycle#beforePoll}, 时间限流由 lifecycle 实现 (典型 5s 一次 holds EVAL)。
     *
     * @return false → task 已失效 (markStop/锁丢/cancel), runner 应 exitManaged + 移除
     */
    boolean checkAlive() {
        if (!pollState.isSubscriptionActive()) return false;
        try {
            if (!lifecycle.beforePoll()) {
                cancel();
                return false;
            }
        } catch (Throwable t) {
            errorHandler.handleError(new RuntimeException("checkAlive threw", t));
            return false;
        }
        return true;
    }

    /**
     * 业务作用：托管模式退出清理。ManagedRunner 退出时对每个 task 调用。
     * <p>
     * onComplete 设为 null 断 ManagedRunner 反向引用环 (onComplete → wakeup lambda → ManagedRunner)。
     * 即便 lambda finally 在 task 退出后才跑, 它读到 null 后跳过 cb.run(), 不会出问题
     * (此时 ManagedRunner 也大概率退出了, unpark 对 dying thread 是 no-op, 即使 onComplete 还没 null 也安全)。
     *
     * @param normal 见上述说明
     */
    void exitManaged(boolean normal) {
        isInEventLoop = false;
        onComplete = null;
        try {
            lifecycle.afterExit(normal);
        } catch (Throwable e) {
            errorHandler.handleError(new RuntimeException("PollLifecycle.afterExit threw", e));
        }
    }

    // ==================== 共用: 反序列化 + 投递 ====================

    /**
     * 业务作用：把一批原始记录解码并投递给业务回调。
     * 单条解码失败时跳过该条而非中断整批——一条格式损坏的消息不应让同批其余消息也无法处理。
     *
     * @param records 原始记录
     * 返回: 无返回值。
     */
    private void deserializeAndEmitRecords(List<ByteRecord> records) {

        if (records.isEmpty()) {
            return;
        }

        if (listener instanceof BatchStreamListener) {
            // batch delivery
            deserializeAndEmitBatch(records, (BatchStreamListener) listener);
        } else {
            // fallback: per-record delivery for plain StreamListener
            for (ByteRecord raw : records) {
                try {
                    pollState.updateReadOffset(raw.getId().getValue());
                    @SuppressWarnings("unchecked") V record = (V) convertRecord(raw);
                    listener.onMessage(record);
                } catch (RuntimeException ex) {
                    if (cancelSubscriptionOnError.test(ex)) {
                        cancel();
                        errorHandler.handleError(ex);
                        return;
                    }
                    errorHandler.handleError(ex);
                }
            }
        }
    }

    /**
     * 业务作用：使用 raw type 避免编译器插入 checkcast Record。
     * 当 activateDefaultTyping=true 时, deserializer 可能返回原始 JVM 对象 (TestOrder/Ticker 等)
     * 而非 MapRecord, 泛型 checkcast 会导致 ClassCastException。
     * raw type 跳过这个检查, 让元素原样传递到 listener lambda 中处理。
     *
     * @param records       见上述说明
     * @param batchListener 见上述说明
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void deserializeAndEmitBatch(List<ByteRecord> records, BatchStreamListener batchListener) {

        RecycleLinkedList batch = RecycleLinkedList.of();

        try {
            for (ByteRecord raw : records) {
                pollState.updateReadOffset(raw.getId().getValue());
                batch.add(convertRecord(raw));
            }

            batchListener.onMessage(batch);
        } catch (RuntimeException ex) {

            if (cancelSubscriptionOnError.test(ex)) {
                cancel();
                errorHandler.handleError(ex);
                return;
            }

            errorHandler.handleError(ex);
        } finally {
            batch.recycle();
        }
    }

    /**
     * 业务作用：把一条原始记录解码成业务对象。
     *
     * @param record 原始记录
     * @return 解码后的业务对象；无法解码时为 null。
     */
    private Object convertRecord(ByteRecord record) {

        try {
            return deserializer.apply(record);
        } catch (RuntimeException ex) {
            throw new ConversionFailedException(TypeDescriptor.forObject(record), targetType, record, ex);
        }
    }

    /**
     * 业务作用：判断本任务是否仍在消费，供容器与外部健康检查使用。
     *
     * <p>参数说明: 无。
     *
     * @return 仍在运行返回 true。
     */
    @Override
    public boolean isActive() {
        return State.RUNNING.equals(getState()) || isInEventLoop;
    }

    /**
     * Local poll state backed by public Spring Data Redis stream types.
     */
    static class PollState {

        private final OffsetStrategy readOffsetStrategy;
        private final Optional<Consumer> consumer;
        private volatile ReadOffset currentOffset;
        private volatile State state = State.CREATED;
        private volatile CountDownLatch awaitStart = new CountDownLatch(1);

        /**
         * 业务作用：承载轮询状态：消费者身份、位点推进策略与当前位点。
         *
         * @param consumer           消费者身份，独立读取时为空
         * @param readOffsetStrategy 位点推进策略
         * @param currentOffset      当前位点
         */
        private PollState(Optional<Consumer> consumer, OffsetStrategy readOffsetStrategy, ReadOffset currentOffset) {
            this.readOffsetStrategy = readOffsetStrategy;
            this.currentOffset = currentOffset;
            this.consumer = consumer;
        }

        /**
         * 业务作用：建出独立读取的轮询状态，不加入消费组，因此不产生待处理记录也无需确认。
         *
         * @param offset 起始位点
         * @return 轮询状态。
         */
        static PollState standalone(ReadOffset offset) {
            OffsetStrategy strategy = OffsetStrategy.getStrategy(offset);
            return new PollState(Optional.empty(), strategy, strategy.getFirst(offset, Optional.empty()));
        }

        /**
         * 业务作用：建出以消费组身份读取的轮询状态；读出的消息进入待处理列表，必须确认。
         *
         * @param consumer 消费者身份
         * @param offset   起始位点
         * @return 轮询状态。
         */
        static PollState consumer(Consumer consumer, ReadOffset offset) {
            OffsetStrategy strategy = OffsetStrategy.getStrategy(offset);
            Optional<Consumer> optionalConsumer = Optional.of(consumer);
            return new PollState(optionalConsumer, strategy, strategy.getFirst(offset, optionalConsumer));
        }

        /**
         * 业务作用：限时等待进入运行态。
         *
         * @param timeout 等待时长
         * @param unit    时长单位
         * @return 在时限内进入运行态返回 true。
         * @throws InterruptedException 等待期间线程被中断
         */
        boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
            return awaitStart.await(timeout, unit);
        }

        /**
         * 业务作用：读取当前生命周期阶段。
         *
         * <p>参数说明: 无。
         *
         * @return 生命周期阶段。
         */
        public State getState() {
            return state;
        }

        /**
         * 业务作用：判断订阅是否仍然有效，已取消的订阅不再消费且不会自行恢复。
         *
         * <p>参数说明: 无。
         *
         * @return 订阅仍有效返回 true。
         */
        boolean isSubscriptionActive() {
            return state == State.STARTING || state == State.RUNNING;
        }

        /**
         * 业务作用：置为启动中，使等待启动的调用方能区分「尚未开始」与「已在运行」。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         */
        void starting() {
            state = State.STARTING;
        }

        /**
         * 业务作用：置为运行中并唤醒等待启动的调用方。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         */
        void running() {
            state = State.RUNNING;
            CountDownLatch awaitStart = this.awaitStart;
            if (awaitStart.getCount() == 1) {
                awaitStart.countDown();
            }
        }

        /**
         * 业务作用：置为已取消。已取消的状态不可逆——恢复消费必须由容器重新建出任务。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         */
        void cancel() {
            awaitStart = new CountDownLatch(1);
            state = State.CANCELLED;
        }

        /**
         * 业务作用：按已处理的消息标识推进读取位点。
         * 推进发生在处理<b>之后</b>：先推进再处理会让处理失败的消息被永久跳过。
         *
         * @param messageId 已处理的消息标识
         * 返回: 无返回值。
         */
        void updateReadOffset(String messageId) {
            currentOffset = readOffsetStrategy.getNext(getCurrentReadOffset(), consumer, messageId);
        }

        /**
         * 业务作用：读取当前读取位点，下一轮拉取据此发起。
         *
         * <p>参数说明: 无。
         *
         * @return 当前读取位点。
         */
        ReadOffset getCurrentReadOffset() {
            return currentOffset;
        }
    }
}

/**
 * 业务作用：Stream 拉取任务的统一抽象，使容器无需关心任务的具体实现即可管理其生命周期。
 * 把启动等待与取消建模进接口，是为了让容器能确认任务确实已就绪或确实已停止，
 * 而不是发出指令后就假定生效。
 */
interface NasaStreamTask extends Runnable {

    /**
     * 业务作用：取消任务并等待其真正停止。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     * @throws org.springframework.dao.DataAccessResourceFailureException 停止过程中访问失败
     */
    void cancel() throws DataAccessResourceFailureException;

    /**
     * 业务作用：读取任务当前所处的生命周期阶段。
     *
     * <p>参数说明: 无。
     *
     * @return 生命周期阶段。
     */
    State getState();

    /**
     * 业务作用：限时等待任务进入运行态。
     *
     * @param timeout 等待时长
     * @return 在时限内进入运行态返回 true。
     * @throws InterruptedException 等待期间线程被中断
     */
    boolean awaitStart(Duration timeout) throws InterruptedException;

    /**
     * 业务作用：声明任务是否长驻，容器据此决定调度方式；默认为长驻。
     *
     * <p>参数说明: 无。
     *
     * @return 长驻返回 true。
     */
    default boolean isLongLived() {
        return true;
    }

    /**
     * 业务作用：判断任务是否处于运行态。
     *
     * <p>参数说明: 无。
     *
     * @return 运行中返回 true。
     */
    default boolean isActive() {
        return State.RUNNING.equals(getState());
    }
}

/**
 * 业务作用：标识拉取任务所处的生命周期阶段。
 * 容器据此判断任务是否可用、是否需要重建：只有运行中的任务才在消费，
 * 而已取消的任务不会自行恢复，必须由容器重新建出。
 */
enum State {
    CREATED, STARTING, RUNNING, CANCELLED
}

/**
 * 业务作用：决定消费组建立后从哪个位点开始读取，以及每轮读取之后位点如何推进。
 * 不同策略对「消费者重启后是否重读未确认消息」有直接影响，是消息不丢与不重之间的取舍点。
 */
enum OffsetStrategy {
    NEXT_MESSAGE {
        /**
         * 业务作用：按本策略给出首轮读取的起始位点。
         * 首轮与后续轮次的位点语义不同：首轮要区分「从头读」「只读新消息」「接续已提交位点」，
         * 之后则一律沿着已读位置推进。
         *
         * @param offset   声明的起始位点
         * @param consumer 消费者身份，独立读取时为空
         * @return 首轮实际使用的起始位点。
         */
        @Override
        ReadOffset getFirst(ReadOffset offset, Optional<Consumer> consumer) {
            return offset;
        }

        /**
         * 业务作用：按本策略给出下一轮读取的起始位点。
         *
         * @param offset    声明的起始位点
         * @param consumer  消费者身份，独立读取时为空
         * @param messageId 本轮最后一条已处理消息的标识
         * @return 下一轮使用的起始位点。
         */
        @Override
        ReadOffset getNext(ReadOffset offset, Optional<Consumer> consumer, String messageId) {
            return ReadOffset.from(messageId);
        }
    },
    LAST_CONSUMED {
        /**
         * 业务作用：按本策略给出首轮读取的起始位点。
         * 首轮与后续轮次的位点语义不同：首轮要区分「从头读」「只读新消息」「接续已提交位点」，
         * 之后则一律沿着已读位置推进。
         *
         * @param offset   声明的起始位点
         * @param consumer 消费者身份，独立读取时为空
         * @return 首轮实际使用的起始位点。
         */
        @Override
        ReadOffset getFirst(ReadOffset offset, Optional<Consumer> consumer) {
            return consumer.isPresent() ? ReadOffset.lastConsumed() : ReadOffset.from("0-0");
        }

        /**
         * 业务作用：按本策略给出下一轮读取的起始位点。
         *
         * @param offset    声明的起始位点
         * @param consumer  消费者身份，独立读取时为空
         * @param messageId 本轮最后一条已处理消息的标识
         * @return 下一轮使用的起始位点。
         */
        @Override
        ReadOffset getNext(ReadOffset offset, Optional<Consumer> consumer, String messageId) {
            return consumer.isPresent() ? ReadOffset.lastConsumed() : ReadOffset.from(messageId);
        }
    },
    LATEST {
        /**
         * 业务作用：按本策略给出首轮读取的起始位点。
         * 首轮与后续轮次的位点语义不同：首轮要区分「从头读」「只读新消息」「接续已提交位点」，
         * 之后则一律沿着已读位置推进。
         *
         * @param offset   声明的起始位点
         * @param consumer 消费者身份，独立读取时为空
         * @return 首轮实际使用的起始位点。
         */
        @Override
        ReadOffset getFirst(ReadOffset offset, Optional<Consumer> consumer) {
            return ReadOffset.latest();
        }

        /**
         * 业务作用：按本策略给出下一轮读取的起始位点。
         *
         * @param offset    声明的起始位点
         * @param consumer  消费者身份，独立读取时为空
         * @param messageId 本轮最后一条已处理消息的标识
         * @return 下一轮使用的起始位点。
         */
        @Override
        ReadOffset getNext(ReadOffset offset, Optional<Consumer> consumer, String messageId) {
            return ReadOffset.latest();
        }
    };

    /**
     * 业务作用：按声明的起始位点判定应采用哪种位点推进策略。
     *
     * @param offset 声明的起始位点
     * @return 对应的推进策略。
     */
    static OffsetStrategy getStrategy(ReadOffset offset) {
        if (ReadOffset.latest().equals(offset)) {
            return LATEST;
        }
        if (ReadOffset.lastConsumed().equals(offset)) {
            return LAST_CONSUMED;
        }
        return NEXT_MESSAGE;
    }

    /**
     * 业务作用：由各策略实现，给出首轮读取的起始位点。
     *
     * @param offset   声明的起始位点
     * @param consumer 消费者身份
     * @return 首轮起始位点。
     */
    abstract ReadOffset getFirst(ReadOffset offset, Optional<Consumer> consumer);

    /**
     * 业务作用：由各策略实现，给出下一轮读取的起始位点。
     *
     * @param offset    声明的起始位点
     * @param consumer  消费者身份
     * @param messageId 本轮最后一条已处理消息的标识
     * @return 下一轮起始位点。
     */
    abstract ReadOffset getNext(ReadOffset offset, Optional<Consumer> consumer, String messageId);
}
