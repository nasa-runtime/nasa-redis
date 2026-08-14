package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.concurrent.CompleteFuture;
import io.github.nasaruntime.core.utils.ContextUtils;
import io.lettuce.core.RedisFuture;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Nasa
 * pipeline 命令的"业务等候 future" — 把 lettuce 返回的 RedisFuture 投递给业务线程做同步等待.
 *
 * <h2>用法</h2>
 * <pre>
 *   LettuceFuture&lt;RedisFuture&lt;byte[]&gt;&gt; lf = LettuceFuture.of();
 *   pipeline().offer(PipelineTask.of(OP_GET).arg1(k).future(lf));
 *   byte[] v = lf.getFinally(valueSerializer);   // 阻塞等待, 末尾自动 recycle 归池
 * </pre>
 *
 * <h2>池化设计</h2>
 * 之前每次 sync 调用 {@code new LettuceFuture<>()} + {@code new CountDownLatch(1)} + {@code new CompletableFuture}
 * 内部 stack, 是撮合热路径上最大的 GC 来源. 改造为:
 * <ul>
 *   <li>{@link #of()} 从 {@link #POOL} 借实例 (空池时 new)</li>
 *   <li>complete/completeExceptionally 由信号线程 dispatch 阶段调一次, 唤醒 waiter</li>
 *   <li>{@link #getFinally()} 业务线程同步等待 + 递归 unwrap 嵌套 RedisFuture, 末尾自动 {@link #recycle} 归池</li>
 * </ul>
 *
 * <h2>实现选择</h2>
 * 不再 extends {@code CompletableFuture} — 那个类的内部 result/waiter chain 没法 reset 干净
 * (需要 {@code --add-opens base module/JVM.util.concurrent} + native-image reflect-config). 自维护
 * {@code result/ex/done + LockSupport.park} 单 waiter 等待, 重置零反射, 也对 native-image 友好.
 *
 * <h2>API 兼容</h2>
 * 仍 {@code implements RedisFuture<V>} 保接口签名, 但 {@link CompletionStage} 链式方法
 * (thenApply/thenCompose/...) 全部抛 {@link UnsupportedOperationException} — 现网 grep 验证无任何 caller
 * 在 LettuceFuture 实例上调链式 API.
 *
 * <h2>recycle 约束</h2>
 * 业务方调 {@link #getFinally()} 后 lf 已归池, 不能再持有 lf 引用 (会变 dangling, 下次 borrow 后状态错乱).
 * 不要手动调 {@link #recycle()}, 让 getFinally 统一管理生命周期.
 */
@SuppressWarnings("all")
public final class LettuceFuture<V> implements RedisFuture<V>, ObjectPool.Recycler<LettuceFuture<V>> {

    /**
     * 池容量默认 16384, 单实例 ~64B (4 个引用 + 1 个 boolean), 16384 个 ≈ 1MB 上限可接受.
     * 撮合 100k TPS 下每订单 5 sync 调用 = 500k 借/还/秒, 池稳态占用通常远低于 capacity.
     */
    static final ObjectPool<LettuceFuture<Object>> POOL = new ObjectPool<>(
            ContextUtils.getPropertyInt("nasa.object-pool.lettuce-future-capacity", 16384)) {
        /**
         * 业务作用：池空时新建一个实例，由对象池在借不到空闲实例时调用。
         *
         * <p>参数说明: 无。
         *
         * @return 新建的实例。
         */
        @Override
        public LettuceFuture<Object> newObject() {
            return new LettuceFuture<>();
        }
    };

    private final ObjectPool.PooledHandle<LettuceFuture<V>> handle =
            (ObjectPool.PooledHandle) new ObjectPool.PooledHandle<>(POOL);

    /**
     * complete(null) 的占位; result 用 null 表示"未完成", 用 NULL_SENTINEL 区分"完成但值为 null".
     */
    private static final Object NULL_SENTINEL = new Object();

    /**
     * 完成的 value (NULL_SENTINEL 表示 null), null 表示未完成或异常完成.
     */
    private volatile Object result;
    /**
     * 异常完成的 throwable, null 表示未完成或正常完成.
     */
    private volatile Throwable ex;
    /**
     * 已完成标记; complete/completeExceptionally CAS 设为 true 保证只生效一次.
     */
    private volatile boolean done;
    /**
     * 阻塞中的业务线程引用, 由 complete 时 {@link LockSupport#unpark} 唤醒.
     */
    private volatile Thread waiter;

    /**
     * 业务作用：私有化构造，强制经由工厂方法从池中取用，避免绕过池直接新建而使池失去意义。
     *
     * <p>参数说明: 无。
     */
    private LettuceFuture() {
    }

    /**
     * 业务作用：从池借出一个待完成的结果占位，命令入队时使用。
     * 用完必须归还——本类在等待结束后自行归还，因此调用方只需正常走等待路径即可。
     *
     * <p>参数说明: 无。
     *
     * @param <V> 结果类型
     * @return 池借的结果占位。
     */
    public static <V> LettuceFuture<V> of() {
        @SuppressWarnings("unchecked")
        LettuceFuture<V> f = (LettuceFuture<V>) POOL.get();
        return f;
    }

    /**
     * 业务作用：从池借出一个<b>已完成</b>的结果占位，用于命令无需实际下发即可确定结果的短路路径
     * （如入参为空时直接给出空结果），使调用方对短路与正常路径的处理完全一致。
     *
     * @param v   已确定的结果
     * @param <V> 结果类型
     * @return 已完成的结果占位。
     */
    public static <V> LettuceFuture<V> of(V v) {
        LettuceFuture<V> f = of();
        f.complete(v);
        return f;
    }

    /**
     * 业务作用：暴露本实例的池化句柄，供对象池完成借出与归还的状态跟踪。
     *
     * <p>参数说明: 无。
     *
     * @return 本实例的池化句柄。
     */
    @Override
    public ObjectPool.PooledHandle<LettuceFuture<V>> handle() {
        return this.handle;
    }

    /**
     * 业务作用：归还前清空结果、异常、完成标志与等待线程引用。
     * <b>必须清等待线程引用</b>：残留的引用会让实例被下一个线程借走后误唤醒上一个线程，
     * 也会让已结束的线程无法被回收。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void restore() {
        this.result = null;
        this.ex = null;
        this.done = false;
        this.waiter = null;
    }

    // ---- complete / completeExceptionally (信号线程 dispatch 调用) ----

    /**
     * 业务作用：标记完成 + 唤醒 waiter. 多次调用仅首次生效 (dispatch 单次完成保证).
     *
     * @param value 待写入的值
     * @return 见上述说明。
     */
    public boolean complete(V value) {
        if (done) return false;
        result = (value == null) ? NULL_SENTINEL : value;
        done = true;
        Thread w = waiter;
        if (w != null) LockSupport.unpark(w);
        return true;
    }

    /**
     * 业务作用：以异常标记完成并唤醒等待线程，由命令分发线程在命令失败时调用。
     * 只有首次调用生效：一条命令只会完成一次，重复完成会覆盖掉先到的结果。
     *
     * @param t 导致失败的异常
     * @return 本次调用使其完成返回 true；此前已完成返回 false。
     */
    public boolean completeExceptionally(Throwable t) {
        if (done) return false;
        ex = t;
        done = true;
        Thread w = waiter;
        if (w != null) LockSupport.unpark(w);
        return true;
    }

    /**
     * 业务作用：取失败原因的描述，供调用方记日志而不必自行拆异常。
     *
     * <p>参数说明: 无。
     *
     * @return 失败描述；未失败时为 null。
     */
    @Override
    public String getError() {
        Throwable e = ex;
        return e == null ? null : e.getMessage();
    }

    // ---- Future<V> ----

    /**
     * 业务作用：声明本结果<b>不可取消</b>，恒返回 false。
     * 命令一经提交就已发往服务端，客户端撤销不了；返回 true 会让调用方误以为命令不会执行。
     *
     * @param mayInterruptIfRunning 由接口约定保留，本实现不使用
     * @return 恒为 false。
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // pipeline 命令一经提交无法 cancel (lettuce async 已发出); 与原 CompletableFuture 行为一致返回 false
        return false;
    }

    /**
     * 业务作用：本结果不支持取消，因此恒返回 false。
     *
     * <p>参数说明: 无。
     *
     * @return 恒为 false。
     */
    @Override
    public boolean isCancelled() {
        return false;
    }

    /**
     * 业务作用：判断结果是否已就绪，供调用方在不阻塞的前提下探测。
     *
     * <p>参数说明: 无。
     *
     * @return 已完成（含失败）返回 true。
     */
    @Override
    public boolean isDone() {
        return done;
    }

    /**
     * 业务作用：读取字符串值。
     * 键不存在时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * <p>参数说明: 无。
     *
     * @return 命令的执行结果。
     */
    @Override
    public V get() throws InterruptedException, ExecutionException {
        if (!done) this.park(0L);
        if (ex != null) throw new ExecutionException(ex);
        @SuppressWarnings("unchecked")
        V v = (result == NULL_SENTINEL) ? null : (V) result;
        return v;
    }

    /**
     * 业务作用：读取字符串值。
     * 键不存在时返回空。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param timeout 超时时长
     * @param unit 时长单位
     * @return 命令的执行结果。
     */
    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!done) {
            long nanos = unit.toNanos(timeout);
            this.park(nanos);
            if (!done) throw new TimeoutException();
        }
        if (ex != null) throw new ExecutionException(ex);
        @SuppressWarnings("unchecked")
        V v = (result == NULL_SENTINEL) ? null : (V) result;
        return v;
    }

    /**
     * 业务作用：限时等待结果就绪，用于调用方不愿无限期挂起的场景。
     * 超时返回 false 而不抛异常，把「继续等还是放弃」的决定交给调用方。
     *
     * @param timeout 等待时长
     * @param unit    时长单位
     * @return 在时限内完成返回 true；超时返回 false。
     * @throws InterruptedException 等待期间线程被中断
     */
    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        if (done) return true;
        this.park(unit.toNanos(timeout));
        return done;
    }

    /**
     * 业务作用：park 直到 done 或 (nanos &gt; 0 &amp;&amp; 超时). nanos = 0 表示无限等待.
     * 单 waiter 设计 — 同一个 lf 只会有一个业务线程在 getFinally 等待 (信号线程 complete 后业务线程唤醒,
     * 不存在多 waiter 场景).
     *
     * @param nanos 见上述说明
     */
    private void park(long nanos) throws InterruptedException {
        waiter = Thread.currentThread();
        try {
            // double-check: 设 waiter 后再读一次 done, 防止 complete 在 set waiter 前已发生
            if (done) return;
            long deadline = nanos > 0 ? System.nanoTime() + nanos : 0L;
            while (!done) {
                if (Thread.interrupted()) throw new InterruptedException();
                if (nanos > 0) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0L) return;
                    LockSupport.parkNanos(this, left);
                } else {
                    LockSupport.park(this);
                }
            }
        } finally {
            waiter = null;
        }
    }

    // ---- 业务侧 sync 等待入口: 用完归池 ----

    /**
     * 业务作用：业务线程同步等待 + 递归 unwrap 嵌套 Future, 末尾 recycle 归池.
     * <p>
     * 因 lf.complete 存的是 lettuce 真正的 RedisFuture (一个 Future-of-Future), 这里走
     * {@link CompleteFuture#getFinally} 静态版本递归 get() 直到拿到最底层值.
     * <p>
     * recycle 在 finally 内执行, 即使递归过程中抛 IllegalCallerException (CompleteFuture.get 包装的 ExecutionException) 也回收.
     *
     * @return 见上述说明。
     */
    @SuppressWarnings("unchecked")
    public <T> T getFinally() {
        try {
            return (T) CompleteFuture.getFinally(this);
        } finally {
            // 仅在已完成时回池: 业务线程在 buffered 路径 park 等 drain 完成期间被 interrupt 时, done 仍为 false,
            // 此时若回池, drain 稍后会对已回池(可能被重借)的实例 complete, 造成跨请求串线. 牺牲该对象(待 GC)换安全.
            if (done) this.recycle();
        }
    }

    /**
     * 业务作用：getFinally + 反序列化器. byte[] 走 serializer.deserialize, 其他直接 cast.
     *
     * @param serializer 见上述说明
     * @return 见上述说明。
     */
    @SuppressWarnings("unchecked")
    public <T> T getFinally(RedisSerializer<?> serializer) {
        Object o;
        try {
            o = CompleteFuture.getFinally(this);
        } finally {
            // 仅在已完成时回池 (理由同无参 getFinally): 未完成被中断时不回池, 防串线
            if (done) this.recycle();
        }
        return o instanceof byte[] bs ? (T) serializer.deserialize(bs) : (T) o;
    }

    // ==================== CompletionStage 桩 — 现网无 caller, 全部抛 UOE ====================

    /**
     * 业务作用：产出统一的不支持异常，供各个链式接口的桩方法抛出。
     * 消息中写明本类的定位与替代做法，使调用方一看异常就知道该改用哪种方式，
     * 而不是面对一个无解释的不支持异常。
     *
     * <p>参数说明: 无。
     *
     * @return 带说明的不支持异常。
     */
    private static UnsupportedOperationException uoe() {
        return new UnsupportedOperationException("LettuceFuture 仅用于 pipeline 同步等待, 不支持 CompletionStage 链式 API");
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> thenApply(Function<? super V, ? extends U> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn       见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> thenAccept(Consumer<? super V> action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action   见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> thenRun(Runnable action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action   见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other 见上述说明
     * @param fn    见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U, X> CompletionStage<X> thenCombine(CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends X> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other 见上述说明
     * @param fn    见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U, X> CompletionStage<X> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends X> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other    见上述说明
     * @param fn       见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U, X> CompletionStage<X> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends X> fn, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other  见上述说明
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other  见上述说明
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other    见上述说明
     * @param action   见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other  见上述说明
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other  见上述说明
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other    见上述说明
     * @param action   见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other 见上述说明
     * @param fn    见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other 见上述说明
     * @param fn    见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other    见上述说明
     * @param fn       见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other  见上述说明
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other  见上述说明
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other    见上述说明
     * @param action   见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other  见上述说明
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other  见上述说明
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param other    见上述说明
     * @param action   见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn       见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn       见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param action   见上述说明
     * @param executor 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @param fn 见上述说明
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletionStage<V> exceptionally(Function<Throwable, ? extends V> fn) {
        throw uoe();
    }

    /**
     * 业务作用：本类只服务于批次的同步等待，<b>不支持链式组合</b>，调用即抛出不支持异常。
     * <p>
     * 保留这些方法是为了满足所接口的契约；直接抛出而不是给出一个半可用的实现，
     * 是为了让误用在第一次调用时就暴露，而不是在某个边界条件下才表现为结果丢失。
     *
     * @return 本方法不会正常返回。
     * @throws UnsupportedOperationException 恒抛出
     */
    @Override
    public CompletableFuture<V> toCompletableFuture() {
        throw uoe();
    }
}
