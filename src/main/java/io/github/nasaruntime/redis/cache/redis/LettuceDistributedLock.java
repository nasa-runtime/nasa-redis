package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.DistributedLock;
import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.base.TimingWheel;
import io.github.nasaruntime.core.function.Action;
import io.github.nasaruntime.core.utils.ContextUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * 基于 Lettuce + Lua 的轻量分布式锁, 无需 Redisson 依赖。
 * <p>
 * 核心设计:
 * <ul>
 *   <li>加锁/解锁/续期: 各一个 Lua 脚本, Redis 端原子执行</li>
 *   <li>可重入: Redis Hash (key=锁名, field=持有者标识, value=重入次数)</li>
 *   <li>看门狗: TimingWheel 定时续期, 持锁期间自动延长 TTL, 避免业务未完成锁已过期</li>
 *   <li>持有者标识: INSTANCE_ID(JVM级) + threadId(线程级), 跨节点 + 跨线程唯一</li>
 *   <li>锁实例池化: RedisLock 通过 ObjectPool 回收复用, 降低高并发场景 GC 压力</li>
 * </ul>
 *
 * <h2>使用方式</h2>
 * <pre>
 * // 注册为 Spring Bean
 * &#64;Bean
 * public DistributedLock distributedLock() {
 *     return new LettuceDistributedLock(RedisProxy.load());
 * }
 *
 * // 方式1: lockAndUnlock (推荐, 自动获取/释放/回收)
 * distributedLock.lockAndUnlock("order:123", () -&gt; { ... });
 *
 * // 方式2: getLock (需自行 try-finally unlock)
 * Lock lock = distributedLock.getLock("order:123");
 * lock.lock();
 * try { ... } finally { lock.unlock(); }
 * </pre>
 */
@Slf4j
public class LettuceDistributedLock implements DistributedLock {

    /* 实例级唯一标识, 同一 JVM 内所有锁共享, 区分不同节点 */
    private static final String INSTANCE_ID = UUID.randomUUID().toString();

    /**
     * 加锁 Lua: 锁不存在则创建, 已持有则重入, 否则返回剩余 TTL。
     * <pre>
     * KEYS[1] = 锁 key
     * ARGV[1] = 过期时间 (ms)
     * ARGV[2] = 持有者标识 (INSTANCE_ID:threadId)
     * 返回: nil = 加锁成功, 否则返回锁剩余 TTL (ms)
     * </pre>
     */
    private static final String LOCK_LUA = """
            if redis.call('exists', KEYS[1]) == 0 then
                redis.call('hincrby', KEYS[1], ARGV[2], 1)
                redis.call('pexpire', KEYS[1], ARGV[1])
                return nil
            end
            if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
                redis.call('hincrby', KEYS[1], ARGV[2], 1)
                redis.call('pexpire', KEYS[1], ARGV[1])
                return nil
            end
            return redis.call('pttl', KEYS[1])
            """;

    /**
     * 解锁 Lua: 校验持有者 → 重入计数减一 → 归零则删锁。
     * <pre>
     * KEYS[1] = 锁 key
     * ARGV[1] = 过期时间 (ms), 重入未完全释放时刷新 TTL
     * ARGV[2] = 持有者标识
     * 返回: nil = 非自己的锁, 0 = 重入减一(仍持有), 1 = 完全释放(锁已删)
     * </pre>
     */
    private static final String UNLOCK_LUA = """
            if redis.call('hexists', KEYS[1], ARGV[2]) == 0 then
                return nil
            end
            local count = redis.call('hincrby', KEYS[1], ARGV[2], -1)
            if count > 0 then
                redis.call('pexpire', KEYS[1], ARGV[1])
                return 0
            end
            redis.call('del', KEYS[1])
            return 1
            """;

    /**
     * 续期 Lua: 仅当自己持有锁时刷新 TTL, 看门狗定时调用。
     * <pre>
     * KEYS[1] = 锁 key
     * ARGV[1] = 过期时间 (ms)
     * ARGV[2] = 持有者标识
     * 返回: 1 = 续期成功, 0 = 锁已不在(被释放或过期)
     * </pre>
     */
    private static final String RENEW_LUA = """
            if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
                redis.call('pexpire', KEYS[1], ARGV[1])
                return 1
            end
            return 0
            """;

    /**
     * 持有检测 Lua: 只读, 不动 TTL, 不动重入计数。
     * <p>
     * 用于消费方在长 GC 后自检 "锁是否仍归我", 避免误消费。
     * 与 RENEW_LUA 不同, 此处不刷新 TTL — 续期是看门狗的职责, 检测就只是检测。
     * <pre>
     * KEYS[1] = 锁 key
     * ARGV[1] = 持有者标识
     * 返回: 1 = 仍持有, 0 = 已不在
     * </pre>
     */
    private static final String HOLDS_LUA = """
            if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
                return 1
            end
            return 0
            """;

    static final Map<RedisProxy, LettuceDistributedLock> CACHE = new ConcurrentHashMap<>();

    /**
     * 业务作用：为一个命令代理登记锁实现，使该实例上的加锁能力可用。
     *
     * @param redisProxy 承载锁命令的命令代理
     * @param props      该代理级别的锁配置
     * @return 建出的锁实现。
     */
    public static LettuceDistributedLock initialize(RedisProxy redisProxy, NasaLettuceConfig.DistributedLockProperties props) {
        LettuceDistributedLock lock = new LettuceDistributedLock(redisProxy, props);
        CACHE.put(redisProxy, lock);
        return lock;
    }

    /**
     * 业务作用：取某个命令代理对应的锁实现。
     *
     * @param redisProxy 命令代理
     * @return 该代理的锁实现；未登记时为 null。
     */
    public static LettuceDistributedLock load(RedisProxy redisProxy) {
        return CACHE.get(redisProxy);
    }

    /**
     * 业务作用：按实例名取锁实现，供多套 Redis 共存时按名选取。
     * <b>不同实例上的同名锁互不互斥</b>——互斥双方必须用同一个实例。
     *
     * @param qualifier 实例名
     * @return 该实例的锁实现；未登记时为 null。
     */
    public static LettuceDistributedLock load(String qualifier) {
        return load(RedisProxy.load(qualifier));
    }


    private final RedisProxy redisProxy;
    /* 锁 key 前缀, 避免与 stream/hash 等其他 Redis key 类型冲突 */
    private final String[] prefixes = {null};
    /* 预计算: leaseTime 的字符串形式, 传给 Lua 的 ARGV */
    private final String[] leaseTimeStrs = {null};
    /* 看门狗续期间隔 = leaseTime / 3 */
    private final long[] renewIntervals = {0};
    /* leaseTime ms 的 long 形式, 给 lock() park 超时兜底用 */
    private final long[] leaseTimes = {0};
    private final Action initAction;
    /* 等锁订阅注册中心: per-channel 引用计数, 同 JVM 多线程等同一锁共享一个真订阅 */
    final LockSubscriptionRegistry subscriptions = new LockSubscriptionRegistry();

    /**
     * 业务作用：按配置建出分布式锁实现，绑定其命令代理与看门狗参数。
     * 配置按「代理级 → 全局 → 内置默认」逐级回退，使多套 Redis 共存时各自可有不同的锁前缀与租期。
     *
     * @param redisProxy 承载锁命令的命令代理；锁的互斥范围就是该代理所连的实例
     * @param props      该代理级别的锁配置，字段为空时回退到全局配置
     */
    private LettuceDistributedLock(RedisProxy redisProxy, NasaLettuceConfig.DistributedLockProperties props) {
        this.redisProxy = redisProxy;
        this.initAction = () -> {
            // prefix: qualifier 级 → 全局 → 默认
            String p = props.getPrefix();
            prefixes[0] = (p != null && !p.isEmpty())
                    ? p
                    : ContextUtils.getPropertySafe("nasa.redis.distributed-lock.prefix", "DISTRIBUTED-LOCK:");
            // lease-time: qualifier 级 → 全局 → 默认
            Long lt = props.getLeaseTime();
            long leaseTime = (lt != null)
                    ? lt
                    : ContextUtils.getPropertyLong("nasa.redis.distributed-lock.lease-time", 30000L);
            leaseTimeStrs[0] = String.valueOf(leaseTime);
            renewIntervals[0] = leaseTime / 3;
            leaseTimes[0] = leaseTime;
        };
    }

    /**
     * 业务作用：获取一把分布式锁。锁实例从 ObjectPool 池中获取, unlock 完全释放后自动回池。
     * <p>
     * <b>注意: unlock 后锁实例被回收, 不可再次使用同一个引用调用 lock()。
     * 每次加锁应重新调用 getLock, 或使用 {@link #lockAndUnlock} 系列方法 (推荐)。</b>
     * <pre>
     * // ✓ 正确用法
     * Lock lock = distributedLock.getLock("key");
     * lock.lock();
     * try { ... } finally { lock.unlock(); }
     *
     * // ✗ 错误: unlock 后 lock 已回池, 不可复用
     * Lock lock = distributedLock.getLock("key");
     * lock.lock();
     * lock.unlock();
     * lock.lock(); // 危险: 此对象可能已被其他线程从池中取走
     * </pre>
     *
     * @param key 缓存键
     * @return 见上述说明。
     */
    @Override
    public Lock getLock(String key) {
        String prefix = prefixes[0];
        if (prefix == null) {
            this.initAction.action();
            prefix = prefixes[0];
        }
        RedisLock lock = RedisLock.POOL.get();
        lock.init(prefix + key, redisProxy, leaseTimeStrs[0], renewIntervals[0], leaseTimes[0], subscriptions);
        return lock;
    }

    /**
     * 业务作用：检测当前线程是否仍持有指定 key 的锁 (只读, 不动 TTL/重入计数)。
     * <p>
     * 典型场景: 分区消费循环跑完一批后自检 "我是否还持有这个分区的锁",
     * 防止长 GC 期间锁过期被其他节点抢走、自己醒来还在消费导致同分区双消费。
     * <p>
     * <b>必须在持锁线程上调用</b>: 持有者标识 = INSTANCE_ID + threadId, 跨线程不互通。
     * thread A 调 tryLock 后 thread B 调 holds(key) 会返回 false, 即使 Redis 端锁仍在 —
     * 因为 hexists 查的是 thread B 的 field, 而锁是 thread A 写的。
     *
     * @param key 缓存键
     */
    public boolean holds(String key) {
        if (prefixes[0] == null) initAction.action();
        String[] ks = {prefixes[0] + key};
        String h = holder();
        Long r = redisProxy.eval(HOLDS_LUA, Long.class, ks, h);
        if (r == null || r != 1) {
            log.warn("holds() failed key={} holder={} threadId={} virtual={} result={}",
                    ks[0], h, Thread.currentThread().threadId(), Thread.currentThread().isVirtual(), r);
        }
        return r != null && r == 1;
    }

    /**
     * 业务作用：按指定 holder 检测 key 是否仍被该 holder 持有 (只读, 不动 TTL / 重入计数)。
     * <p>
     * 与 {@link #holds(String)} 的区别: 不读当前线程 ID, 直接用调用方传入的 holder 字符串校验。
     * 典型场景: ACK fencing — runner 线程 tryLock 成功后记录自己的 holder,
     * 后续 businessExecutor 线程在 ACK / XDEL 前用这个 holder 校验锁是否仍属于
     * 当年 tryLock 那次会话, 防长 GC / watchdog 失败 / 锁过期后被新 owner 抢走时,
     * 旧 worker 继续发 stale XACK 把 PEL 消息删掉, 形成丢消息。
     * <p>
     * <b>调用方负责传对 holder</b>: 通常来自 {@link #currentHolder()} 在 tryLock 那条线程内快照。
     * 传错 holder (例如传了别人当前线程的 holder) 会一律返回 false, 不会误判通过。
     *
     * @param key    业务侧 key (不含 prefix), 框架自动拼 {@code prefixes[0] + key}
     * @param holder 完整 holder 标识字符串 ({@code INSTANCE_ID + ":" + threadId} 格式)
     * @return true = key 存在且 holder 字段匹配; false = key 已不在 / 已被新 owner 接管 / Redis 异常
     */
    public boolean holds(String key, String holder) {
        Long r = this.holdsStatus(key, holder);
        return r != null && r == 1;
    }

    /**
     * 业务作用：holds 的三态版本: 返回原始 EVAL 结果, 让调用方区分"真不持有"和"Redis 异常".
     *
     * <p>返回值约定：</p>
     * <ul>
     *   <li>1 = 持有 (key 存在且 holder 字段匹配)</li>
     *   <li>0 = 不持有 (key 不存在 或 已被别的 holder 接管 — 这是真锁丢)</li>
     *   <li>null = Redis 异常 (eval 返回 nil / 超时 / 网络断开 — 可能瞬时, 不一定真锁丢)</li>
     * </ul>
     * 典型用法: ACK fencing 路径只关心"是否持有",任何非 1 都拒 ACK;
     * recoverPending 路径需要区分,网络异常时不污染 lockLost,避免单次抖动让锁等 30s lease.
     *
     * @param key    缓存键
     * @param holder 见上述说明
     * @return {@code 1} 表示仍持有，{@code 0} 表示已失权，{@code null} 表示 Redis 调用异常。
     */
    public Long holdsStatus(String key, String holder) {
        if (prefixes[0] == null) initAction.action();
        String[] ks = {prefixes[0] + key};
        try {
            return redisProxy.eval(HOLDS_LUA, Long.class, ks, holder);
        } catch (Throwable t) {
            log.warn("holdsStatus eval threw key={} holder={}", ks[0], holder, t);
            return null;
        }
    }

    /**
     * 持有者标识: INSTANCE_ID + 当前线程ID。
     * <p>
     * 平台线程：线程数少且长期存活，ThreadLocal 缓存有效，同一线程反复 lock/unlock 复用同一个 String。
     * 虚拟线程：每个虚拟线程 threadId 全局递增不复用，ThreadLocal 缓存只用一次就跟着 GC，
     *          不如直接拼接，还省了 ThreadLocalMap 的开销。
     */
    private static final ThreadLocal<String> HOLDER_CACHE = ThreadLocal.withInitial(
            () -> INSTANCE_ID + ":" + Thread.currentThread().threadId());

    /**
     * 业务作用：产出本次加锁的持有者标识：进程实例标识加线程标识。
     * <p>
     * 两段都必要：只用进程标识会让同进程内不同线程互相解掉对方的锁；
     * 只用线程标识会让不同进程上编号相同的线程互相解锁。解错锁是分布式锁最严重的失效方式。
     *
     * <p>参数说明: 无。
     *
     * @return 持有者标识。
     */
    private static String holder() {
        return Thread.currentThread().isVirtual()
                ? INSTANCE_ID + ":" + Thread.currentThread().threadId()
                : HOLDER_CACHE.get();
    }

    /**
     * 业务作用：读取当前线程的持有者标识，供排查「锁被谁持有」时使用。
     *
     * <p>参数说明: 无。
     *
     * @return 当前线程的持有者标识。
     */
    public static String currentHolder() {
        return holder();
    }

    /**
     * 业务作用：返回 lockKey 加配置 prefix 后的真实 Redis key. 供外部诊断 (如查锁 TTL) 用, 避免硬编码 prefix。
     *
     * @param key 缓存键
     */
    public String redisKey(String key) {
        if (prefixes[0] == null) initAction.action();
        return prefixes[0] + key;
    }

    // ==================== LockSubscriptionRegistry ====================

    /**
     * 等锁订阅注册中心 (Redisson 思路).
     * <p>
     * 同 JVM 多线程等同一锁 → 共享一个真订阅, 引用计数管理:
     * <ul>
     *   <li>第一个等锁线程: 真 subscribe, 加入 waiter 集合</li>
     *   <li>后续等锁线程: 直接加 waiter, 不重复 subscribe</li>
     *   <li>收到 pub: handler 唤醒 waiter 集合里所有线程 (LockSupport.unpark)</li>
     *   <li>最后一个 release: waiter 集合空 → 真 unsubscribe</li>
     * </ul>
     * <p>
     * 用 {@code ConcurrentHashMap.compute} 保证 acquire/release 与 handler 的原子可见性 —
     * Subscription 创建/删除 全在 compute 内做, 不会出现"已 unsubscribe 但 handler 还在跑唤醒空集合"的脏读
     * (handler 内 {@code subs.get(channel)} 读到 null 直接 return, 安全).
     */
    final class LockSubscriptionRegistry {

        private final ConcurrentHashMap<String, Subscription> subs = new ConcurrentHashMap<>();

        /**
         * 业务作用：加入等锁队列. 第一个进入者会真 subscribe, 后续进入者只 add waiter.
         *
         * @param channel 频道名
         * @param waiter  见上述说明
         */
        void acquire(String channel, Thread waiter) {
            subs.compute(channel, (ch, sub) -> {
                if (sub == null) {
                    sub = new Subscription();
                    // 用 enableLog=false 避免每个锁都打 "Subscribed to channel" 噪音日志
                    redisProxy.subscribe(false, ch, msg -> {
                        Subscription cur = subs.get(ch);
                        if (cur != null) {
                            for (Thread t : cur.waiters) LockSupport.unpark(t);
                        }
                    });
                }
                sub.waiters.add(waiter);
                return sub;
            });
        }

        /**
         * 业务作用：退出等锁队列. waiter 空时真 unsubscribe.
         *
         * @param channel 频道名
         * @param waiter  见上述说明
         */
        void release(String channel, Thread waiter) {
            subs.compute(channel, (ch, sub) -> {
                if (sub == null) return null;
                sub.waiters.remove(waiter);
                if (sub.waiters.isEmpty()) {
                    redisProxy.unsubscribe(false, ch);
                    return null;   // 从 map 删除
                }
                return sub;
            });
        }
    }

    /**
     * 一个 channel 上所有等锁线程的集合.
     * <p>
     * 用 {@code ConcurrentHashMap.newKeySet()} 保证 handler 迭代 vs acquire/release 修改并发安全.
     */
    static final class Subscription {
        final Set<Thread> waiters = ConcurrentHashMap.newKeySet();
    }

    // ==================== RedisLock ====================

    /**
     * 具体锁实例, 实现 {@link Lock} 接口, 通过 {@link ObjectPool} 池化复用。
     * <p>
     * 池化目的: 高并发场景下避免每次 getLock 都 new 对象, 降低 GC 压力。
     * RedisLock 本身约 80 bytes, 单次分配微不足道, 但在万级 TPS 下累计的短命对象
     * 会增加 Young GC 频率。池化后稳态零分配。
     * <p>
     * 生命周期: {@link #init} 设置字段 → lock/tryLock 使用 → unlock 完全释放时
     * 自动 {@link #recycle()} 回池, 字段清空等待下次复用。
     */
    static class RedisLock implements Lock, ObjectPool.Recycler<RedisLock> {

        static final ObjectPool<RedisLock> POOL = new ObjectPool<>(
                ContextUtils.getPropertyInt("nasa.object-pool.distributed-lock-capacity", 200)) {
            /**
             * 业务作用：池空时新建一个锁实例，由对象池在借不到空闲实例时调用。
             *
             * <p>参数说明: 无。
             *
             * @return 新建的锁实例。
             */
            @Override
            public RedisLock newObject() {
                return new RedisLock();
            }
        };

        private final ObjectPool.PooledHandle<RedisLock> handle = new ObjectPool.PooledHandle<>(POOL);

        /* 预分配 keys 数组, 避免每次 eval 创建 */
        final String[] keys = {null};
        /* 锁在 Redis 中的 key */
        String key;
        RedisProxy redisProxy;
        String leaseTimeStr;
        long renewInterval;
        /* leaseTime ms (long 形), park 超时兜底, 防 pub 丢失死等 */
        long leaseTime;
        /* unlock 完全释放时 pub 的频道, 同 lock 一起 init 出来 */
        String pubChannel;
        /* 订阅注册中心引用 (来自外层 LettuceDistributedLock 实例) */
        LockSubscriptionRegistry subscriptions;
        /*
         * 标记锁是否已完全释放。
         * 解决看门狗与 recycle 竞态：unlock → stopWatchdog(cancelled=true) → recycle(清空字段)，
         * 但看门狗 lambda 可能已被 platformExecutor 取出正在执行（cancel 来不及阻止），
         * recycle 后看门狗读到 null 字段 → NPE。
         * 加 released 标记后，看门狗执行前先检查，已释放则跳过。
         */
        volatile boolean released;
        /*
         * 每次从池借出后递增的代号. 看门狗 lambda 捕获 init 时的快照, fire 时对比当前 generation,
         * 不一致直接 return.
         * <p>
         * 为什么 released 不够: 对象回池 restore() 会把 released=false (为下次使用做准备),
         * 之后的 stale watchdog lambda fire 时看 released=false → 仍读已被新 init 覆盖的字段
         * (redisProxy/keys/leaseTimeStr 已是新 holder 的值), 可能对新 holder 的锁错误 renew.
         * generation 是单调递增的, restore 不重置, 旧 lambda 永远拿不到新 init 后的值.
         */
        volatile long generation;

        /**
         * 业务作用：从池中取出后, 设置本次使用的参数
         *
         * @param key           缓存键
         * @param redisProxy    命令代理，决定连接与序列化方式
         * @param leaseTimeStr  见上述说明
         * @param renewInterval 见上述说明
         * @param leaseTime     见上述说明
         * @param subscriptions 见上述说明
         */
        void init(String key, RedisProxy redisProxy, String leaseTimeStr, long renewInterval,
                  long leaseTime, LockSubscriptionRegistry subscriptions) {
            this.key = key;
            this.keys[0] = key;
            this.redisProxy = redisProxy;
            this.leaseTimeStr = leaseTimeStr;
            this.renewInterval = renewInterval;
            this.leaseTime = leaseTime;
            this.pubChannel = key + ":pub";   // 与锁 key 同 namespace, cluster 下尽量同 slot
            this.subscriptions = subscriptions;
            this.released = false;
            // 每次借出递增 generation. 旧 watchdog lambda 捕获的快照对不上 → 被本次 fire 跳过.
            this.generation++;
        }

        /**
         * 业务作用：暴露本实例的池化句柄，供对象池完成借出与归还的状态跟踪。
         *
         * <p>参数说明: 无。
         *
         * @return 本实例的池化句柄。
         */
        @Override
        public ObjectPool.PooledHandle<RedisLock> handle() {
            return this.handle;
        }

        /**
         * 业务作用：回池前清空所有字段, 避免持有外部引用导致内存泄漏.
         * <p>
         * <b>released 不重置</b>: 让它保持 unlock 时设置的 true 状态, 直到下次 init() 重新设 false.
         * 这是为了堵 stale watchdog lambda 的 NPE 窗口 — 如果 restore 重置 released=false,
         * lambda 在 restore 之后 / init 之前 fire 时, generation 还是旧值不会 mismatch,
         * released 又被 restore 设回 false 不能 short-circuit, 接着读已清空的 redisProxy/keys → NPE.
         * 让 released=true 保持, 提供"实例已释放, 不要继续 renew"的语义信号.
         */
        @Override
        public void restore() {
            key = null;
            keys[0] = null;
            redisProxy = null;
            leaseTimeStr = null;
            renewInterval = 0;
            leaseTime = 0;
            pubChannel = null;
            subscriptions = null;
            // released 保持 true (由 unlock 路径设置), 不在这里重置. init() 内会重新设为 false 复用本实例.
        }

        // ==================== Lock 接口 ====================

        /**
         * 业务作用：阻塞获取锁 (Redisson 思路: sub-on-fail + pub-on-unlock).
         * <p>
         * 加锁失败 → 订阅 unlock 通道 → park; 收到 pub 立即重试, 不轮询。
         * 平均唤醒延迟为网络 RTT 量级，避免固定 sleep min(ttl, 100ms) 带来的额外等待。
         * <p>
         * <b>TOCTOU 防御</b>: 订阅后必须再 tryLock 一次 — 订阅前可能恰好对方 unlock+pub 错过那次唤醒,
         * 兜底也有 ttl 超时 (park 最多 min(ttl, leaseTime) 醒来重试).
         *
         * @throws RuntimeException 如果等待期间被中断
         */
        @Override
        public void lock() {
            String h = holder();
            Thread cur = Thread.currentThread();
            while (true) {
                Long ttl = redisProxy.eval(LOCK_LUA, Long.class, keys, leaseTimeStr, h);
                if (ttl == null) {
                    startWatchdog(h);
                    return;
                }
                // 失败: 订阅 + park
                subscriptions.acquire(pubChannel, cur);
                try {
                    // TOCTOU 防御: 订阅后再试一次, 避免错过订阅前的那次 unlock pub
                    ttl = redisProxy.eval(LOCK_LUA, Long.class, keys, leaseTimeStr, h);
                    if (ttl == null) {
                        startWatchdog(h);
                        return;
                    }
                    // park 等 unlock 信号或 ttl 超时. ttl 可能 > leaseTime (服务端时钟差), 取 min 兜底.
                    // 至少 1ms 防 ttl=0/极小值导致 parkNanos 立即返回 → busy spin.
                    LockSupport.parkNanos(Math.max(1L, Math.min(ttl, leaseTime)) * 1_000_000L);
                } finally {
                    subscriptions.release(pubChannel, cur);
                }
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("lock interrupted");
                }
            }
        }

        /**
         * 业务作用：可中断地阻塞获取锁。行为同 {@link #lock()}, 但等待期间响应中断。
         */
        @Override
        public void lockInterruptibly() throws InterruptedException {
            String h = holder();
            Thread cur = Thread.currentThread();
            while (true) {
                if (Thread.interrupted()) throw new InterruptedException();
                Long ttl = redisProxy.eval(LOCK_LUA, Long.class, keys, leaseTimeStr, h);
                if (ttl == null) {
                    startWatchdog(h);
                    return;
                }
                subscriptions.acquire(pubChannel, cur);
                try {
                    ttl = redisProxy.eval(LOCK_LUA, Long.class, keys, leaseTimeStr, h);
                    if (ttl == null) {
                        startWatchdog(h);
                        return;
                    }
                    LockSupport.parkNanos(Math.max(1L, Math.min(ttl, leaseTime)) * 1_000_000L);
                } finally {
                    subscriptions.release(pubChannel, cur);
                }
                if (Thread.interrupted()) throw new InterruptedException();
            }
        }

        /**
         * 业务作用：非阻塞尝试获取锁。成功返回 true 并启动看门狗, 失败立即返回 false。
         */
        @Override
        public boolean tryLock() {
            String h = holder();
            Long ttl = redisProxy.eval(LOCK_LUA, Long.class, keys, leaseTimeStr, h);
            if (ttl == null) {
                startWatchdog(h);
                return true;
            }
            return false;
        }

        /**
         * 业务作用：带超时的尝试获取锁。在 deadline 内重试, 超时返回 false。
         * <p>
         * 走 sub+park 路径 (Redisson 思路), 不再 sleep 轮询。
         *
         * @param time 见上述说明
         * @param unit 时长单位
         */
        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            String h = holder();
            Thread cur = Thread.currentThread();
            long deadline = System.currentTimeMillis() + unit.toMillis(time);
            while (true) {
                if (Thread.interrupted()) throw new InterruptedException();
                Long ttl = redisProxy.eval(LOCK_LUA, Long.class, keys, leaseTimeStr, h);
                if (ttl == null) {
                    startWatchdog(h);
                    return true;
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) return false;
                subscriptions.acquire(pubChannel, cur);
                try {
                    // TOCTOU 防御
                    ttl = redisProxy.eval(LOCK_LUA, Long.class, keys, leaseTimeStr, h);
                    if (ttl == null) {
                        startWatchdog(h);
                        return true;
                    }
                    long parkMs = Math.min(Math.min(ttl, leaseTime), remaining);
                    if (parkMs <= 0) return false;
                    // 至少 1ms 防 busy spin
                    LockSupport.parkNanos(Math.max(1L, parkMs) * 1_000_000L);
                } finally {
                    subscriptions.release(pubChannel, cur);
                }
                if (Thread.interrupted()) throw new InterruptedException();
            }
        }

        /**
         * 业务作用：释放锁。重入计数减一, 归零时完全释放并停止看门狗, 锁实例自动回池。
         *
         * @throws IllegalMonitorStateException 如果当前线程未持有此锁
         */
        @Override
        public void unlock() {
            String h = holder();
            String pubCh = this.pubChannel;   // 在 recycle 前快照, recycle 后字段会清空
            RedisProxy proxy = this.redisProxy;
            Long result;
            try {
                result = redisProxy.eval(UNLOCK_LUA, Long.class, keys, leaseTimeStr, h);
            } catch (RuntimeException e) {
                disposeLocal(h);
                throw e;
            }
            if (result == null) {
                disposeLocal(h);
                throw new IllegalMonitorStateException(
                        "attempt to unlock a lock not held by current thread: " + key);
            }
            // 完全释放: 标记 released → 停止看门狗 → 回收锁实例到池
            // released 必须在 recycle 之前设置，看门狗 lambda 检查此标记跳过操作
            if (result == 1) {
                released = true;
                stopWatchdog(h);
                recycle();   // 字段清空, 之后不能再用 this.xxx
                // pub 唤醒所有订阅者 — 用 recycle 前快照的 channel/proxy, 别用 this.* (已 null)
                try {
                    proxy.pub(pubCh, "");
                } catch (Exception ignored) {
                }
            }
        }

        /**
         * 业务作用：分布式锁不支持 Condition
         *
         * @return 见上述说明。
         */
        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("distributed lock does not support Condition");
        }

        /**
         * 业务作用：本地放弃锁实例: <b>不调 Redis</b>, 只做本地资源释放 (released=true + stopWatchdog + recycle).
         * <p>
         * 用于 unlock 路径之外的退出场景, 防止 RedisLock 池泄漏 + watchdog 持续挂:
         * <ul>
         *   <li>{@code lockLost=true} (锁被新 owner 抢走): 调用方已确认锁不属于自己, 不应再发 UNLOCK_LUA</li>
         *   <li>unlock 抛 {@link IllegalMonitorStateException} (eval 返回 null): 锁实际不在, Redis 端无需再操作</li>
         *   <li>unlock 抛任意 Redis 异常 (eval 抛超时/网络): 锁状态未知, 本地资源先释放, Redis 端靠 lease 兜底</li>
         * </ul>
         * 注意: 调用方必须自己确保锁不需要 UNLOCK_LUA (要么已 unlock, 要么没必要 unlock).
         * 重入未完全释放的 RedisLock 不应调本方法 — 那样会让 watchdog 停掉但 Redis 端锁仍存在.
         *
         * @param holder 本地 watchdog 注册时的 holder 字符串, 用于 cancel 对应任务名
         */
        void disposeLocal(String holder) {
            released = true;
            stopWatchdog(holder);
            recycle();
        }

        // ==================== 看门狗 ====================

        /**
         * 业务作用：启动看门狗: 通过 TimingWheel 定时执行续期 Lua。
         * 每 renewInterval 检查一次, 如果锁仍被自己持有则刷新 TTL。
         * 锁已不在 (被释放或过期) 时自动停止。
         * <p>
         * <b>快照不可变局部变量 + generation 双重防御</b>: lambda 闭包捕获 init 时刻的 proxy/keys/lease/key/gen
         * 局部快照, fire 时不读 this 字段 (除 generation/released 守门), 避免 generation 检查后线程切走
         * 期间被 unlock+recycle+新一次 init 改写字段, lambda 恢复后读到 null 或新 holder 的字段做误 renew/NPE.
         * <p>
         * gen 检查兜底: 新一次 init 后 generation 已递增, 旧 lambda 看到不匹配立即 return.
         * released 检查兜底: unlock 路径设的 released=true 保持到下次 init 显式重置, restore() 不重置.
         *
         * @param holder 见上述说明
         */
        private void startWatchdog(String holder) {
            if (!TimingWheel.isStarted()) TimingWheel.startTimingWheel();
            String name = watchdogName(holder);
            // 不可变快照: 这些 final 局部变量绑定 init 时的实例字段值, lambda 内只读快照.
            // 即使 unlock+recycle+restore+新 init 改写 this 字段, 旧 lambda 仍指向旧 proxy/keys/lease.
            // keys 数组单独 new 一个新数组 (this.keys 是 mutable, keys[0] 会被新 init 覆盖).
            final RedisProxy proxy = this.redisProxy;
            final String[] renewKeys = {this.key};
            final String lease = this.leaseTimeStr;
            final String logKey = this.key;
            final long gen = this.generation;
            TimingWheel.platform(renewInterval, renewInterval, name, () -> {
                // 双重门禁: gen 不匹配 (实例已被新 init 占用) 或 released (本实例 unlock 路径已 dispose) 立即 return.
                if (this.generation != gen) return;
                if (released) return;
                try {
                    Long renewed = proxy.eval(RENEW_LUA, Long.class, renewKeys, lease, holder);
                    if (renewed == null || renewed == 0) {
                        TimingWheel.cancel(name);
                    }
                } catch (Exception e) {
                    if (!released) log.error("watchdog renew failed for key={}", logKey, e);
                }
            });
        }

        /**
         * 业务作用：停止看门狗定时任务
         *
         * @param holder 见上述说明
         */
        private void stopWatchdog(String holder) {
            TimingWheel.of().remove(watchdogName(holder));
        }

        /**
         * 业务作用：看门狗任务名: dlock:{key}:{holder}, 用于 TimingWheel 注册/取消
         *
         * @param holder 见上述说明
         * @return 见上述说明。
         */
        private String watchdogName(String holder) {
            return "dlock:" + key + ":" + holder;
        }
    }
}
