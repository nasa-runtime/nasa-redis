package io.github.nasaruntime.redis.cache.redis.search;

import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.utils.ContextUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

import java.util.Arrays;
import java.util.function.BiFunction;

/**
 * 可池化的 {@link RedisCallback} 包装器, 零 GC 适配
 * "<b>redisTemplate.execute(...) / redisTemplate.executePipelined(...) + 捕获外部参数</b>" 场景.
 *
 * <h2>典型用法 ({@link io.github.nasaruntime.redis.cache.redis.search.executor.SpringDataRsCommandExecutor} 单条 RediSearch dispatch)</h2>
 * <pre>{@code
 *   return redisTemplate.execute(
 *           RedisCallbackRecycler.ofRecycle(EXECUTE_CB)
 *                   .ref(0, command)
 *                   .ref(1, args));
 * }</pre>
 *
 * <h3>API</h3>
 * <ul>
 *   <li>{@link #of()} / {@link #of(BiFunction)} — 后者绑定 strategy, lambda 签名 {@code (conn, ar) -> T}</li>
 *   <li>{@link #ofRecycle(BiFunction)} — apply 末尾自动归池, 仅适用于 {@code execute*} 的"单次回调"语义</li>
 *   <li>{@link #ref(int, Object)} / {@link #refRecycle(int, Object)} — 引用槽位 0~31</li>
 *   <li>{@link #val(int, long)} (+ int/boolean/char 重载) — 原始类型槽位 0~31</li>
 * </ul>
 *
 * <h3>池配置</h3>
 * 容量: 默认 2000, 可通过 {@code nasa.object-pool.redis-callback-recycler-capacity} 调.
 *
 * <h3>常见坑</h3>
 * <ul>
 *   <li><b>refRecycle 的对象不要传出 doInRedis</b>: 标了 refRecycle 的对象在 owner recycle 时归池,
 *       回调外部读到的就是脏数据. 要传出去就别标 refRecycle.</li>
 *   <li><b>异步 RedisCallback 慎用 ofRecycle</b>: 如果 {@code execute*} 的实现是异步 (理论上不该有),
 *       回调还在跑就 recycle 会有问题. Redis data adapter 的 {@code execute} / {@code executePipelined}
 *       是同步语义, 用 ofRecycle 安全.</li>
 * </ul>
 */
@SuppressWarnings("unused")
public class RedisCallbackRecycler<T> implements RedisCallback<T>, ObjectPool.Recycler<RedisCallbackRecycler<T>> {

    /* 策略, 接收 (RedisConnection, recycler), 返回 T (可为 null, 与 RedisCallback 语义一致) */
    BiFunction<RedisConnection, RedisCallbackRecycler<T>, T> function;
    /* 是否在 doInRedis() 末尾自我回收. 默认 false, ofRecycle 工厂打开. */
    boolean recycleSelf;
    /* 引用类型槽位, 随实例池化, 零 GC */
    final Object[] refs = new Object[32];
    /* refs 回收位掩码 */
    long refsRecycle;
    /* 原始类型槽位, 统一存 long, 零装箱 */
    final long[] vals = new long[32];

    /**
     * 业务作用：私有化构造，强制经由工厂方法从池中取用。
     *
     * <p>参数说明: 无。
     */
    private RedisCallbackRecycler() {}

    // 池层面统一当 <Object>, 借出后强转回 <T>
    static final ObjectPool<RedisCallbackRecycler<Object>> POOL = new ObjectPool<>(
            ContextUtils.getPropertyInt("nasa.object-pool.redis-callback-recycler-capacity", 2000)) {
        /**
         * 业务作用：池空时新建一个实例，由对象池在借不到空闲实例时调用。
         *
         * <p>参数说明: 无。
         *
         * @return 新建的实例。
         */
        @Override
        public RedisCallbackRecycler<Object> newObject() {
            return new RedisCallbackRecycler<>();
        }
    };

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final ObjectPool.PooledHandle<RedisCallbackRecycler<T>> handle =
            (ObjectPool.PooledHandle) new ObjectPool.PooledHandle<>(POOL);

    /**
     * 业务作用：doInRedis 入口. 默认 recycleSelf=false. 用了 {@link #ofRecycle} 则在算完 T 之后归池.
     * {@link RedisCallback} 是同步单次调用语义, ofRecycle 安全.
     *
     * @param connection 见上述说明
     * @return 见上述说明。
     */
    @Override
    public T doInRedis(RedisConnection connection) throws DataAccessException {
        try {
            return this.function.apply(connection, this);
        } finally {
            if (recycleSelf) this.recycle();
        }
    }

    /**
     * 业务作用：设置回调执行完毕后是否自行归还本实例。
     * <p>
     * 只有在<b>同步语义</b>的执行路径上才可打开：回调若在异步路径上仍在运行时就归还，
     * 实例会被别的线程借走并改写其槽位，正在执行的回调随即读到别人的数据。
     *
     * @param recycleSelf true 表示回调结束时自行归还
     * @return 本实例，便于链式装配。
     */
    public RedisCallbackRecycler<T> recycleSelf(boolean recycleSelf) {
        this.recycleSelf = recycleSelf;
        return this;
    }

    /**
     * 业务作用：暴露本实例的池化句柄，供对象池完成借出与归还的状态跟踪。
     *
     * <p>参数说明: 无。
     *
     * @return 本实例的池化句柄。
     */
    @Override
    public ObjectPool.PooledHandle<RedisCallbackRecycler<T>> handle() {
        return this.handle;
    }

    /**
     * 业务作用：归还前清空策略、引用槽位与回收位掩码。
     * 按掩码标记过的引用槽位在此一并归还其中的池化对象，使嵌套的池化资源不被遗漏。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void restore() {
        this.function = null;
        this.recycleSelf = false;
        Arrays.fill(this.vals, 0);
        for (int i = 0; i < this.refs.length; i++) {
            if ((this.refsRecycle & (1L << i)) != 0 && this.refs[i] instanceof ObjectPool.Recycler<?> rec) {
                rec.recycle();
            }
            this.refs[i] = null;
        }
        this.refsRecycle = 0;
    }

    // ==================== 工厂 ====================

    /**
     * 业务作用：从池借出一个回调实例，<b>不</b>开启自行归还。
     * 适用于回调可能在方法返回后仍在运行的路径，由调用方自行决定归还时机。
     *
     * <p>参数说明: 无。
     *
     * @param <T> 回调结果类型
     * @return 池借的回调实例。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> RedisCallbackRecycler<T> of() {
        return (RedisCallbackRecycler) POOL.get();
    }

    /**
     * 业务作用：绑定策略: lambda 签名 {@code (conn, ar) -> T}, ar 即本实例, 可读 refs/vals.
     * recycleSelf=false, 调用方负责生命周期.
     *
     * @param function 见上述说明
     * @return 见上述说明。
     */
    public static <T> RedisCallbackRecycler<T> of(BiFunction<RedisConnection, RedisCallbackRecycler<T>, T> function) {
        RedisCallbackRecycler<T> r = of();
        r.function = function;
        return r;
    }

    /**
     * 业务作用：绑定策略 + recycleSelf=true: doInRedis() 末尾自动归池.
     * Redis data adapter 的 {@code execute} / {@code executePipelined} 是同步语义, 一次调用即结束, 安全.
     *
     * @param function 见上述说明
     * @return 见上述说明。
     */
    public static <T> RedisCallbackRecycler<T> ofRecycle(BiFunction<RedisConnection, RedisCallbackRecycler<T>, T> function) {
        return RedisCallbackRecycler.of(function).recycleSelf(true);
    }

    // ==================== 原始类型槽位 ====================

    /**
     * 业务作用：把一个长整型参数存进指定槽位，供回调体内取用。
     * 统一以长整型底层存放，避免装箱产生垃圾——回调在批量路径上会被高频创建。
     *
     * @param i 槽位下标
     * @param v 待存入的长整型值
     * @return 本实例，便于链式装配。
     */
    public RedisCallbackRecycler<T> val(int i, long v) {
        this.vals[i] = v;
        return this;
    }

    /**
     * 业务作用：把一个整型参数存进指定槽位，供回调体内取用。
     * 统一以长整型底层存放，避免装箱产生垃圾——回调在批量路径上会被高频创建。
     *
     * @param i 槽位下标
     * @param v 待存入的整型值
     * @return 本实例，便于链式装配。
     */
    public RedisCallbackRecycler<T> val(int i, int v) {
        this.vals[i] = v;
        return this;
    }

    /**
     * 业务作用：把一个布尔参数存进指定槽位，供回调体内取用。
     * 统一以长整型底层存放，避免装箱产生垃圾——回调在批量路径上会被高频创建。
     *
     * @param i 槽位下标
     * @param v 待存入的布尔值
     * @return 本实例，便于链式装配。
     */
    public RedisCallbackRecycler<T> val(int i, boolean v) {
        this.vals[i] = v ? 1 : 0;
        return this;
    }

    /**
     * 业务作用：把一个字符参数存进指定槽位，供回调体内取用。
     * 统一以长整型底层存放，避免装箱产生垃圾——回调在批量路径上会被高频创建。
     *
     * @param i 槽位下标
     * @param v 待存入的字符值
     * @return 本实例，便于链式装配。
     */
    public RedisCallbackRecycler<T> val(int i, char v) {
        this.vals[i] = v;
        return this;
    }

    /**
     * 业务作用：按长整型读取指定槽位的值。
     * <b>存入与读取的类型必须一致</b>：底层统一按长整型存放，类型不符不会报错而是读出一个错乱的值。
     *
     * @param i 槽位下标
     * @return 该槽位的长整型值。
     */
    public long longVal(int i) {
        return this.vals[i];
    }

    /**
     * 业务作用：按整型读取指定槽位的值。
     * <b>存入与读取的类型必须一致</b>：底层统一按长整型存放，类型不符不会报错而是读出一个错乱的值。
     *
     * @param i 槽位下标
     * @return 该槽位的整型值。
     */
    public int intVal(int i) {
        return (int) this.vals[i];
    }

    /**
     * 业务作用：按布尔读取指定槽位的值。
     * <b>存入与读取的类型必须一致</b>：底层统一按长整型存放，类型不符不会报错而是读出一个错乱的值。
     *
     * @param i 槽位下标
     * @return 该槽位的布尔值。
     */
    public boolean boolVal(int i) {
        return this.vals[i] != 0;
    }

    /**
     * 业务作用：按字符读取指定槽位的值。
     * <b>存入与读取的类型必须一致</b>：底层统一按长整型存放，类型不符不会报错而是读出一个错乱的值。
     *
     * @param i 槽位下标
     * @return 该槽位的字符值。
     */
    public char charVal(int i) {
        return (char) this.vals[i];
    }

    // ==================== 引用类型槽位 ====================

    /**
     * 业务作用：把一个引用参数存进指定槽位，归还时只清引用不做归还处理。
     * 用于存放调用方自行管理生命周期的对象。
     *
     * @param i 槽位下标
     * @param v 待存入的对象
     * @return 本实例，便于链式装配。
     */
    public RedisCallbackRecycler<T> ref(int i, Object v) {
        this.refs[i] = v;
        return this;
    }

    /**
     * 业务作用：把一个<b>池化</b>对象存进指定槽位，并标记其在本实例归还时一并归还。
     * 不标记会让嵌套的池化对象只被清引用而不归还，池逐渐借空后退化成每次新建。
     *
     * @param i 槽位下标
     * @param v 待存入的池化对象
     * @return 本实例，便于链式装配。
     */
    public RedisCallbackRecycler<T> refRecycle(int i, Object v) {
        this.refs[i] = v;
        this.refsRecycle |= (1L << i);
        return this;
    }

    /**
     * 业务作用：读取指定槽位的引用。
     *
     * @param i   槽位下标
     * @param <X> 期望的类型，由调用方保证与存入类型一致
     * @return 该槽位的对象；未存入时为 null。
     */
    @SuppressWarnings("unchecked")
    public <X> X ref(int i) {
        return (X) this.refs[i];
    }
}
