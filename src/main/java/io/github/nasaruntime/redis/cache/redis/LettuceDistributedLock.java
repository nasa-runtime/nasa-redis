package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.DistributedLock;
import io.github.nasaruntime.core.base.TimingWheel;
import io.github.nasaruntime.core.function.Action;
import io.github.nasaruntime.core.utils.ContextUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于 Lettuce + Lua 的轻量分布式锁, 无需 Redisson 依赖。
 * <p>
 * 核心设计:
 * <ul>
 *   <li>加锁/解锁/续期: 各一个 Lua 脚本, Redis 端原子执行</li>
 *   <li>可重入: Redis Hash (key=锁名, field=持有者标识, value=重入次数)</li>
 *   <li>看门狗: TimingWheel 定时续期, 持锁期间自动延长 TTL, 避免业务未完成锁已过期</li>
 *   <li>持有者标识: INSTANCE_ID(JVM级) + threadId(线程级), 跨节点 + 跨线程唯一</li>
 *   <li>稳定 Lock 门面: 每个返回对象永久绑定一个 Redis 数据源与业务 key，可按标准 Lock 语义重复使用</li>
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
 * // 方式1: lockAndUnlock (推荐, 自动获取与释放)
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
        LettuceDistributedLock previous = CACHE.putIfAbsent(redisProxy, lock);
        if (previous == null) return lock;
        // 同一代理的锁入口是生命周期单例。重复装配沿用已有实例，避免配置刷新时让仍在持有的锁突然停止续租。
        lock.shutdownSubscriptions();
        return previous;
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
        RedisProxy redisProxy = RedisProxy.load(qualifier);
        return redisProxy == null ? null : load(redisProxy);
    }

    /**
     * 业务作用：解除一个命令代理的分布式锁登记，并关闭仍在等待释放通知的本地订阅。
     * 必须在代理的 pub/sub 容器关闭前执行，确保等待线程能被唤醒并停止继续排队。
     *
     * @param redisProxy 即将销毁的命令代理
     * 返回: 无返回值；没有登记时保持幂等。
     */
    static void destroy(RedisProxy redisProxy) {
        LettuceDistributedLock lock = CACHE.remove(redisProxy);
        if (lock != null) lock.shutdownSubscriptions();
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
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Set<String> watchdogTasks = ConcurrentHashMap.newKeySet();
    /* TimingWheel 是 JVM 级共享设施，任务命名必须同时隔离数据源与本次锁入口生命周期。 */
    private final String watchdogNamespace;

    /**
     * 业务作用：按配置建出分布式锁实现，绑定其命令代理与看门狗参数。
     * 配置按「代理级 → 全局 → 内置默认」逐级回退，使多套 Redis 共存时各自可有不同的锁前缀与租期。
     *
     * @param redisProxy 承载锁命令的命令代理；锁的互斥范围就是该代理所连的实例
     * @param props      该代理级别的锁配置，字段为空时回退到全局配置
     * 返回: 构造出的代理级锁入口；配置非法时拒绝构造。
     */
    private LettuceDistributedLock(RedisProxy redisProxy, NasaLettuceConfig.DistributedLockProperties props) {
        this.redisProxy = Objects.requireNonNull(redisProxy, "redisProxy must not be null");
        Objects.requireNonNull(props, "distributed lock properties must not be null");
        this.watchdogNamespace = "dlock:" + redisProxy.getQualifier() + ":" + UUID.randomUUID();
        this.initAction = () -> {
            // prefix: qualifier 级 → 全局 → 默认
            String p = props.getPrefix();
            prefixes[0] = (p != null && !p.isBlank())
                    ? p
                    : ContextUtils.getPropertySafe("nasa.redis.distributed-lock.prefix", "DISTRIBUTED-LOCK:");
            if (prefixes[0] == null || prefixes[0].isBlank()) {
                throw new IllegalArgumentException("distributed lock prefix must not be blank");
            }
            // lease-time: qualifier 级 → 全局 → 默认
            Long lt = props.getLeaseTime();
            long leaseTime = (lt != null)
                    ? lt
                    : ContextUtils.getPropertyLong("nasa.redis.distributed-lock.lease-time", 30000L);
            if (leaseTime < 3) {
                throw new IllegalArgumentException("distributed lock lease-time must be at least 3ms: " + leaseTime);
            }
            leaseTimeStrs[0] = String.valueOf(leaseTime);
            renewIntervals[0] = leaseTime / 3;
            leaseTimes[0] = leaseTime;
        };
        // 初始化阶段就解析并校验租期，避免首个业务请求才暴露无效配置。
        this.initAction.action();
    }

    /**
     * 业务作用：停止该锁实现全部本地等待订阅，使代理销毁后不再保留频道、线程或回调引用。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；重复调用保持幂等。
     */
    private void shutdownSubscriptions() {
        if (!closed.compareAndSet(false, true)) return;
        subscriptions.shutdown();
        for (String taskName : new ArrayList<>(watchdogTasks)) TimingWheel.cancel(taskName);
        watchdogTasks.clear();
    }

    /**
     * 业务作用：获取一把稳定绑定当前数据源与业务 key 的分布式锁。
     * 完整释放后可以继续复用同一个 Lock 引用，也可以由多个线程按标准 Lock 方式竞争。
     *
     * @param key 缓存键
     * @return 见上述说明。
     */
    @Override
    public Lock getLock(String key) {
        String validatedKey = requireKey(key);
        if (closed.get()) {
            throw new IllegalStateException("distributed lock is closed for RedisProxy["
                    + redisProxy.getQualifier() + "]");
        }
        String prefix = prefixes[0];
        if (prefix == null) {
            this.initAction.action();
            prefix = prefixes[0];
        }
        return new RedisLock(prefix + validatedKey, redisProxy, leaseTimeStrs[0], renewIntervals[0], leaseTimes[0],
                subscriptions, closed, watchdogTasks, watchdogNamespace);
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
     * @return 当前线程仍是 Redis 端 owner 时返回 true；不存在、失权或调用失败时返回 false。
     */
    public boolean holds(String key) {
        if (prefixes[0] == null) initAction.action();
        String[] ks = {prefixes[0] + requireKey(key)};
        String h = holder();
        Long r = redisProxy.evalDirectConnection(HOLDS_LUA, Long.class, ks, h);
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
        String[] ks = {prefixes[0] + requireKey(key)};
        if (holder == null || holder.isBlank()) {
            throw new IllegalArgumentException("distributed lock holder must not be blank");
        }
        try {
            return redisProxy.evalDirectConnection(HOLDS_LUA, Long.class, ks, holder);
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
     * @return 当前数据源配置前缀与合法业务 key 拼成的真实 Redis key。
     */
    public String redisKey(String key) {
        if (prefixes[0] == null) initAction.action();
        return prefixes[0] + requireKey(key);
    }

    /**
     * 业务作用：返回当前数据源锁 key 的真实前缀，供分区配置合同核对锁命名空间。
     *
     * <p>参数说明: 无。
     *
     * @return 当前锁入口解析后的非空前缀。
     */
    String keyPrefix() {
        return prefixes[0];
    }

    /**
     * 业务作用：统一校验公开锁入口的业务 key，避免 null 或纯空白值落成难以诊断的真实 Redis key。
     *
     * @param key 调用方提供的业务 key
     * @return 原始非空白 key；不会改写合法 key 的内容。
     */
    private static String requireKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("distributed lock key must not be blank");
        }
        return key;
    }

    // ==================== LockSubscriptionRegistry ====================

    /**
     * 等锁订阅注册中心。
     * <p>
     * 生命周期读写门禁只隔离 shutdown，不串行不同频道；每个频道用独立门禁合并首订阅、waiter 登记和最后取消。
     * 同频道只建立一次真实订阅，最后一个 waiter 离开后取消；收到通知时唤醒该频道当时登记的全部线程。
     */
    final class LockSubscriptionRegistry {

        private final ConcurrentHashMap<String, Subscription> subs = new ConcurrentHashMap<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        /** acquire/release 共享读门禁，shutdown 独占写门禁；公平模式避免持续新等待者饿死关闭流程。 */
        private final ReentrantReadWriteLock lifecycleGate = new ReentrantReadWriteLock(true);

        /**
         * 业务作用：不可中断地加入等锁队列；第一个进入频道的线程建立真实订阅，后续线程复用。
         *
         * @param channel 释放通知频道
         * @param waiter 等待锁的当前线程
         * 返回: 无返回值；锁入口关闭时拒绝登记。
         */
        void acquire(String channel, Thread waiter) {
            ReentrantReadWriteLock.ReadLock readLock = lifecycleGate.readLock();
            readLock.lock();
            Subscription selected = null;
            try {
                ensureOpen();
                while (true) {
                    Subscription sub = subscription(channel);
                    sub.lifecycleLock.lock();
                    // 最后一个 waiter 可能在当前线程等待频道门禁期间移除了旧坐标，只向仍在 map 中的实例登记。
                    if (subs.get(channel) != sub) {
                        sub.lifecycleLock.unlock();
                        continue;
                    }
                    registerWaiter(channel, waiter, sub);
                    selected = sub;
                    break;
                }
            } finally {
                readLock.unlock();
            }
            awaitSubscription(channel, waiter, selected);
        }

        /**
         * 业务作用：以可中断方式加入频道等待队列，使 `lockInterruptibly` 在生命周期或同频道门禁上等待时及时退出。
         *
         * @param channel 释放通知频道
         * @param waiter 等待锁的当前线程
         * 返回: 无返回值；登记成功后由调用方在 finally 中 release，中断或入口关闭时不留下 waiter。
         */
        void acquireInterruptibly(String channel, Thread waiter) throws InterruptedException {
            ReentrantReadWriteLock.ReadLock readLock = lifecycleGate.readLock();
            readLock.lockInterruptibly();
            Subscription selected = null;
            try {
                ensureOpen();
                while (true) {
                    Subscription sub = subscription(channel);
                    sub.lifecycleLock.lockInterruptibly();
                    if (subs.get(channel) != sub) {
                        sub.lifecycleLock.unlock();
                        continue;
                    }
                    registerWaiter(channel, waiter, sub);
                    selected = sub;
                    break;
                }
            } finally {
                readLock.unlock();
            }
            awaitSubscriptionInterruptibly(channel, waiter, selected);
        }

        /**
         * 业务作用：在调用方剩余预算内加入频道等待队列，使限时加锁不会被生命周期或同频道首订阅门禁无限阻塞。
         *
         * @param channel 释放通知频道
         * @param waiter 等待锁的当前线程
         * @param timeoutNanos 本次订阅登记可使用的剩余纳秒数；非正数只执行立即尝试
         * @return 成功登记 waiter 时返回 true；本地门禁预算耗尽时返回 false。
         */
        boolean tryAcquire(String channel, Thread waiter, long timeoutNanos) throws InterruptedException {
            long startedAt = System.nanoTime();
            ReentrantReadWriteLock.ReadLock readLock = lifecycleGate.readLock();
            if (!readLock.tryLock(Math.max(0L, timeoutNanos), TimeUnit.NANOSECONDS)) return false;
            Subscription selected = null;
            try {
                ensureOpen();
                while (true) {
                    long remainingNanos = remainingNanos(timeoutNanos, startedAt);
                    Subscription sub = subscription(channel);
                    if (!sub.lifecycleLock.tryLock(remainingNanos, TimeUnit.NANOSECONDS)) return false;
                    if (subs.get(channel) != sub) {
                        sub.lifecycleLock.unlock();
                        if (remainingNanos(timeoutNanos, startedAt) <= 0) return false;
                        continue;
                    }
                    registerWaiter(channel, waiter, sub);
                    selected = sub;
                    break;
                }
            } finally {
                readLock.unlock();
            }
            return awaitSubscription(channel, waiter, selected, timeoutNanos, startedAt);
        }

        /**
         * 业务作用：退出等锁队列；最后一个 waiter 离开时取消真实订阅并移除频道坐标。
         *
         * @param channel 释放通知频道
         * @param waiter 已登记的当前线程
         * 返回: 无返回值；取消订阅结局不确定时保留坐标供后续使用或关闭清理。
         */
        void release(String channel, Thread waiter) {
            ReentrantReadWriteLock.ReadLock readLock = lifecycleGate.readLock();
            readLock.lock();
            try {
                Subscription sub = subs.get(channel);
                if (sub == null) return;
                sub.lifecycleLock.lock();
                try {
                    if (subs.get(channel) != sub) return;
                    sub.waiters.remove(waiter);
                    if (!sub.waiters.isEmpty()) return;
                    closeUnusedSubscription(channel, sub);
                } finally {
                    sub.lifecycleLock.unlock();
                }
            } finally {
                readLock.unlock();
            }
        }

        /**
         * 业务作用：返回频道的稳定本地协调对象；网络订阅不在 map 原子函数内执行，避免阻塞其它频道的 map 操作。
         *
         * @param channel 释放通知频道
         * @return 当前频道坐标；并发首建时所有调用方得到同一实例。
         */
        private Subscription subscription(String channel) {
            Subscription existing = subs.get(channel);
            if (existing != null) return existing;
            Subscription created = new Subscription();
            Subscription raced = subs.putIfAbsent(channel, created);
            return raced == null ? created : raced;
        }

        /**
         * 业务作用：在频道门禁内登记 waiter，并为同一频道至多启动一个异步订阅初始化任务。
         * waiter 先进入集合，使订阅建立瞬间到达的 unlock 通知也能唤醒本线程；调用方按自己的等待合同观察初始化完成。
         *
         * @param channel 释放通知频道
         * @param waiter 等待锁的当前线程
         * @param sub 已取得生命周期门禁的频道坐标
         * 返回: 无返回值；任务启动失败会记录为本轮共享初始化结果，由全部 waiter 在等待阶段感知。
         */
        private void registerWaiter(String channel, Thread waiter, Subscription sub) {
            sub.waiters.add(waiter);
            if (sub.registration != null || sub.initializing || sub.initializationFailure != null) return;
            sub.initializing = true;
            try {
                Thread.ofVirtual()
                        .name("redis-lock-subscribe-" + redisProxy.getQualifier()
                                + "-" + Integer.toUnsignedString(channel.hashCode(), 16))
                        .start(() -> initializeSubscription(channel, sub));
            } catch (RuntimeException | Error startFailure) {
                sub.initializing = false;
                sub.initializationFailure = startFailure;
                sub.initialized.signalAll();
            }
        }

        /**
         * 业务作用：在后台建立锁释放通知订阅，并把成功、失败或无需保留的结果原子发布给本频道 waiter。
         * 初始化期间调用方可以超时或中断；最后已无 waiter 时只撤销本任务创建的 listener。
         *
         * @param channel 释放通知频道
         * @param sub 启动本轮初始化的频道坐标
         * 返回: 无返回值；初始化结果通过 Subscription 条件和状态发布。
         */
        private void initializeSubscription(String channel, Subscription sub) {
            sub.lifecycleLock.lock();
            try {
                if (closed.get() || sub.closed || subs.get(channel) != sub || sub.waiters.isEmpty()) {
                    sub.initializing = false;
                    subs.remove(channel, sub);
                    sub.initialized.signalAll();
                    return;
                }
            } finally {
                sub.lifecycleLock.unlock();
            }

            RedisProxy.ListenerRegistration registration = null;
            Throwable failure = null;
            try {
                // 订阅 I/O 不占用调用线程和生命周期门禁，限时与可中断入口只等待自己的剩余预算。
                registration = redisProxy.subscribeOwned(false, channel, msg -> {
                    Subscription current = subs.get(channel);
                    if (current == sub) {
                        for (Thread thread : current.waiters) LockSupport.unpark(thread);
                    }
                });
            } catch (RuntimeException | Error initializationFailure) {
                failure = initializationFailure;
            }

            sub.lifecycleLock.lock();
            try {
                sub.initializing = false;
                if (failure != null) {
                    sub.initializationFailure = failure;
                    if (sub.waiters.isEmpty()) subs.remove(channel, sub);
                } else if (closed.get() || sub.closed || subs.get(channel) != sub || sub.waiters.isEmpty()) {
                    try {
                        registration.close();
                        subs.remove(channel, sub);
                    } catch (RuntimeException cleanupFailure) {
                        // 入口仍开放且坐标仍有效时保留精确句柄，后续 waiter 可复用或在退出时再次撤销。
                        if (!closed.get() && !sub.closed && subs.get(channel) == sub) {
                            sub.registration = registration;
                        } else {
                            log.warn("distributed lock late subscription cleanup failed channel={}",
                                    channel, cleanupFailure);
                        }
                    }
                } else {
                    sub.registration = registration;
                }
                sub.initialized.signalAll();
            } finally {
                sub.lifecycleLock.unlock();
            }
        }

        /**
         * 业务作用：按 lock 的不可中断合同等待首订阅结果，同时保留线程中断标志供上层成功后恢复处理。
         *
         * @param channel 释放通知频道
         * @param waiter 已登记的等待线程
         * @param sub 当前线程已持有门禁的频道坐标
         * 返回: 无返回值；订阅成功时保持 waiter 登记，关闭或初始化失败时撤销登记并抛出异常。
         */
        private void awaitSubscription(String channel, Thread waiter, Subscription sub) {
            try {
                while (subscriptionPending(sub)) sub.initialized.awaitUninterruptibly();
                if (sub.registration != null) return;
                withdrawWaiter(channel, waiter, sub);
                propagateInitializationFailure(sub);
            } finally {
                sub.lifecycleLock.unlock();
            }
        }

        /**
         * 业务作用：按 lockInterruptibly 合同等待首订阅结果，中断发生时立即撤销当前 waiter。
         *
         * @param channel 释放通知频道
         * @param waiter 已登记的等待线程
         * @param sub 当前线程已持有门禁的频道坐标
         * 返回: 无返回值；订阅成功时保持 waiter 登记，中断、关闭或初始化失败时不遗留当前 waiter。
         */
        private void awaitSubscriptionInterruptibly(String channel, Thread waiter, Subscription sub)
                throws InterruptedException {
            try {
                try {
                    while (subscriptionPending(sub)) sub.initialized.await();
                } catch (InterruptedException interrupted) {
                    withdrawWaiter(channel, waiter, sub);
                    throw interrupted;
                }
                if (sub.registration != null) return;
                withdrawWaiter(channel, waiter, sub);
                propagateInitializationFailure(sub);
            } finally {
                sub.lifecycleLock.unlock();
            }
        }

        /**
         * 业务作用：只在调用方剩余预算内等待首订阅结果，截止后撤销当前 waiter 并返回。
         *
         * @param channel 释放通知频道
         * @param waiter 已登记的等待线程
         * @param sub 当前线程已持有门禁的频道坐标
         * @param timeoutNanos 本次订阅登记的总预算
         * @param startedAt 预算开始时的单调时钟快照
         * @return 截止前订阅成功时返回 true；预算耗尽时返回 false；关闭或初始化失败时抛出异常。
         */
        private boolean awaitSubscription(String channel, Thread waiter, Subscription sub,
                                          long timeoutNanos, long startedAt) throws InterruptedException {
            try {
                try {
                    while (subscriptionPending(sub)) {
                        long remainingNanos = remainingNanos(timeoutNanos, startedAt);
                        if (remainingNanos <= 0) {
                            withdrawWaiter(channel, waiter, sub);
                            return false;
                        }
                        sub.initialized.awaitNanos(remainingNanos);
                    }
                } catch (InterruptedException interrupted) {
                    withdrawWaiter(channel, waiter, sub);
                    throw interrupted;
                }
                if (sub.registration != null) {
                    if (remainingNanos(timeoutNanos, startedAt) > 0) return true;
                    withdrawWaiter(channel, waiter, sub);
                    return false;
                }
                withdrawWaiter(channel, waiter, sub);
                propagateInitializationFailure(sub);
                return false;
            } finally {
                sub.lifecycleLock.unlock();
            }
        }

        /**
         * 业务作用：判断本轮首订阅是否仍在进行，供三类等待入口使用同一完成状态。
         *
         * @param sub 已持有门禁的频道坐标
         * @return 尚无注册句柄、失败或关闭结论时返回 true。
         */
        private boolean subscriptionPending(Subscription sub) {
            return sub.registration == null && sub.initializationFailure == null && !sub.closed;
        }

        /**
         * 业务作用：撤销未成功进入等锁阶段的当前 waiter，并在频道无人使用时收回精确 listener。
         *
         * @param channel 释放通知频道
         * @param waiter 需要撤销的等待线程
         * @param sub 已持有门禁的频道坐标
         * 返回: 无返回值；初始化仍在进行时由后台完成路径执行最终补偿。
         */
        private void withdrawWaiter(String channel, Thread waiter, Subscription sub) {
            sub.waiters.remove(waiter);
            if (!sub.waiters.isEmpty()) return;
            closeUnusedSubscription(channel, sub);
        }

        /**
         * 业务作用：在频道已无 waiter 时关闭本频道拥有的 listener，并只在撤销结果确定后删除重试坐标。
         *
         * @param channel 释放通知频道
         * @param sub 已持有门禁的频道坐标
         * 返回: 无返回值；初始化未完成时由完成路径收口，撤销异常时保留句柄供后续重试或复用。
         */
        private void closeUnusedSubscription(String channel, Subscription sub) {
            if (sub.initializing) return;
            RedisProxy.ListenerRegistration registration = sub.registration;
            if (registration == null) {
                subs.remove(channel, sub);
                return;
            }
            try {
                registration.close();
                sub.registration = null;
                subs.remove(channel, sub);
            } catch (RuntimeException error) {
                // 撤销结果不确定时保留精确句柄，不能让清理异常改写加锁成功、超时或中断的业务结局。
                log.warn("distributed lock unsubscribe deferred channel={}", channel, error);
            }
        }

        /**
         * 业务作用：把共享订阅初始化结论转换成锁入口可感知的失败，同时保留原异常类型和因果链。
         *
         * @param sub 已持有门禁且没有有效注册句柄的频道坐标
         * 返回: 本方法始终抛出异常；入口关闭时抛 IllegalStateException，初始化失败时传播原 RuntimeException 或 Error。
         */
        private void propagateInitializationFailure(Subscription sub) {
            Throwable failure = sub.initializationFailure;
            if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
            if (failure instanceof Error error) throw error;
            if (sub.closed || closed.get()) {
                throw new IllegalStateException("distributed lock subscription registry is closed");
            }
            throw new IllegalStateException("distributed lock subscription initialization did not complete", failure);
        }

        /**
         * 业务作用：拒绝在锁入口关闭后登记新的释放通知等待者。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；入口关闭时抛出 IllegalStateException。
         */
        private void ensureOpen() {
            if (closed.get()) throw new IllegalStateException("distributed lock subscription registry is closed");
        }

        /**
         * 业务作用：使用单调时钟计算订阅登记仍可使用的等待预算。
         *
         * @param timeoutNanos 订阅登记的总预算
         * @param startedAt 开始登记时的单调时钟快照
         * @return 尚可等待的纳秒数；预算非正或已经耗尽时返回 0。
         */
        private long remainingNanos(long timeoutNanos, long startedAt) {
            if (timeoutNanos <= 0) return 0;
            long elapsedNanos = System.nanoTime() - startedAt;
            if (elapsedNanos >= timeoutNanos) return 0;
            return timeoutNanos - Math.max(0L, elapsedNanos);
        }

        /**
         * 业务作用：关闭全部等锁频道并唤醒等待线程，使代理销毁不会留下不可达订阅或永久休眠线程。
         * 写门禁先停止新登记，再等待已经进入的 acquire/release 收口，最后逐频道撤销。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；取消订阅失败时继续清理其它频道。
         */
        void shutdown() {
            ReentrantReadWriteLock.WriteLock writeLock = lifecycleGate.writeLock();
            writeLock.lock();
            try {
                if (!closed.compareAndSet(false, true)) return;
                // 写门禁只等待登记阶段，不等待外部订阅 I/O；迟到的初始化结果会根据 closed 精确撤销自己的 listener。
                for (String channel : new ArrayList<>(subs.keySet())) {
                    Subscription sub = subs.remove(channel);
                    if (sub == null) continue;
                    RedisProxy.ListenerRegistration registration;
                    sub.lifecycleLock.lock();
                    try {
                        sub.closed = true;
                        registration = sub.registration;
                        sub.registration = null;
                        for (Thread waiter : sub.waiters) LockSupport.unpark(waiter);
                        sub.initialized.signalAll();
                    } finally {
                        sub.lifecycleLock.unlock();
                    }
                    if (registration != null) {
                        try {
                            registration.close();
                        } catch (Throwable error) {
                            log.warn("distributed lock unsubscribe failed channel={}", channel, error);
                        }
                    }
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    /** 一个 channel 上的异步首订阅状态、生命周期门禁与全部等待线程。 */
    static final class Subscription {
        /** 同频道初始化状态、waiter 集合与最后取消必须串行，不同频道之间不共享该门禁。 */
        final ReentrantLock lifecycleLock = new ReentrantLock();
        final Condition initialized = lifecycleLock.newCondition();
        final Set<Thread> waiters = ConcurrentHashMap.newKeySet();
        boolean initializing;
        boolean closed;
        RedisProxy.ListenerRegistration registration;
        Throwable initializationFailure;
    }

    /**
     * 表示本地记录中的 owner 正在解锁，但 Redis 端已经不再承认该所有权。
     * 它仍属于 {@link IllegalMonitorStateException}，同时让框架层能与真正的跨线程误用分开诊断。
     */
    static final class OwnershipLostException extends IllegalMonitorStateException {

        /**
         * 业务作用：携带已经失去 Redis 锁所有权的诊断信息。
         *
         * @param message 不包含敏感值的所有权诊断
         * 返回: 构造出的异常实例。
         */
        OwnershipLostException(String message) {
            super(message);
        }
    }

    // ==================== RedisLock ====================

    /**
     * 业务作用：实现稳定绑定单个 Redis 数据源与业务 key 的公开 Lock。
     * 完整释放只结束本轮所有权，不改变对象身份和 key 绑定，因此旧引用不会别名到其它业务锁。
     */
    static class RedisLock implements Lock {

        final String[] keys;
        final String key;
        final RedisProxy redisProxy;
        final String leaseTimeStr;
        final long renewInterval;
        final long leaseTime;
        final String pubChannel;
        final LockSubscriptionRegistry subscriptions;
        final AtomicBoolean ownerClosed;
        final Set<String> watchdogTasks;
        final String watchdogNamespace;
        final String watchdogHandleId = UUID.randomUUID().toString();
        /** 内部返回哨兵：本地状态门禁未在调用方允许的等待时间内取得，未发送 Redis 命令。 */
        static final long LOCAL_STATE_BUSY = Long.MIN_VALUE;
        /** 同一个公开 Lock 的远端命令与本地 acquisition 状态必须按同一顺序交接。 */
        final ReentrantLock stateLock = new ReentrantLock();
        /** holder 在当前公开句柄上成功取得的深度；未登记的句柄没有发送 UNLOCK_LUA 的权威。 */
        final Map<String, Integer> localDepths = new HashMap<>();
        /* 当前成功取得 Redis 所有权的 holder；完整释放后清空。 */
        volatile String ownerHolder;
        /* 当前 acquisition 已结束时为 true，用于让已经入队的旧 watchdog 回调停止。 */
        volatile boolean released = true;
        /* 每次成功 acquisition 递增，旧 watchdog 只能服务创建它的那一代所有权。 */
        volatile long generation;

        /**
         * 业务作用：创建永久绑定指定数据源与 Redis key 的稳定 Lock 门面。
         *
         * @param key Redis 中的完整锁 key
         * @param redisProxy 承载锁命令的数据源代理
         * @param leaseTimeStr 传入 Lua 的毫秒租期字符串
         * @param renewInterval watchdog 续期间隔
         * @param leaseTime 锁租期，单位毫秒
         * @param subscriptions 当前数据源的等待订阅注册中心
         * @param ownerClosed 当前锁入口的关闭门禁
         * @param watchdogTasks 当前锁入口登记的 watchdog 任务集合
         * @param watchdogNamespace 包含数据源名称与入口会话的 TimingWheel 命名空间
         * 返回: 构造出的稳定 Lock；其字段不会在完整释放后改绑到其它 key。
         */
        RedisLock(String key, RedisProxy redisProxy, String leaseTimeStr, long renewInterval,
                  long leaseTime, LockSubscriptionRegistry subscriptions, AtomicBoolean ownerClosed,
                  Set<String> watchdogTasks, String watchdogNamespace) {
            this.key = key;
            this.keys = new String[]{key};
            this.redisProxy = redisProxy;
            this.leaseTimeStr = leaseTimeStr;
            this.renewInterval = renewInterval;
            this.leaseTime = leaseTime;
            this.pubChannel = key + ":pub";
            this.subscriptions = subscriptions;
            this.ownerClosed = ownerClosed;
            this.watchdogTasks = watchdogTasks;
            this.watchdogNamespace = watchdogNamespace;
        }

        // ==================== Lock 接口 ====================

        /**
         * 业务作用：阻塞获取锁；失败时订阅释放频道，解锁通知到达后立即重试。
         * <p>
         * 加锁失败 → 订阅 unlock 通道 → park; 收到 pub 立即重试, 不轮询。
         * 平均唤醒延迟为网络 RTT 量级，避免固定 sleep min(ttl, 100ms) 带来的额外等待。
         * <p>
         * <b>TOCTOU 防御</b>: 订阅后必须再 tryLock 一次 — 订阅前可能恰好对方 unlock+pub 错过那次唤醒,
         * 兜底也有 ttl 超时 (park 最多 min(ttl, leaseTime) 醒来重试).
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；取得所有权后返回。等待期间的中断会在成功后恢复中断标志，入口关闭时抛出异常。
         */
        @Override
        public void lock() {
            String h = holder();
            Thread cur = Thread.currentThread();
            boolean interrupted = Thread.interrupted();
            try {
                try {
                    while (true) {
                        Long ttl = tryAcquireOnce(h);
                        if (ttl == null) return;
                        // 失败: 订阅 + park
                        subscriptions.acquire(pubChannel, cur);
                        try {
                            // TOCTOU 防御: 订阅后再试一次, 避免错过订阅前的那次 unlock pub
                            ttl = tryAcquireOnce(h);
                            if (ttl == null) return;
                            // park 等 unlock 信号或 ttl 超时. ttl 可能 > leaseTime (服务端时钟差), 取 min 兜底.
                            // 至少 1ms 防 ttl=0/极小值导致 parkNanos 立即返回 → busy spin.
                            LockSupport.parkNanos(Math.max(1L, Math.min(ttl, leaseTime)) * 1_000_000L);
                        } finally {
                            subscriptions.release(pubChannel, cur);
                        }
                        // lock() 不把中断解释为获取失败；清除状态以免下一次 park 空转，成功或异常退出时再恢复。
                        if (Thread.interrupted()) interrupted = true;
                    }
                } catch (RuntimeException | Error failure) {
                    abandonUnacquired();
                    throw failure;
                }
            } finally {
                if (interrupted) cur.interrupt();
            }
        }

        /**
         * 业务作用：可中断地阻塞获取锁。行为同 {@link #lock()}, 但等待期间响应中断。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；取得所有权后返回，入口关闭或线程中断时抛出异常。
         */
        @Override
        public void lockInterruptibly() throws InterruptedException {
            String h = holder();
            Thread cur = Thread.currentThread();
            try {
                while (true) {
                    if (Thread.interrupted()) throw new InterruptedException();
                    Long ttl = tryAcquireOnceInterruptibly(h);
                    if (ttl == null) return;
                    subscriptions.acquireInterruptibly(pubChannel, cur);
                    try {
                        ttl = tryAcquireOnceInterruptibly(h);
                        if (ttl == null) return;
                        LockSupport.parkNanos(Math.max(1L, Math.min(ttl, leaseTime)) * 1_000_000L);
                    } finally {
                        subscriptions.release(pubChannel, cur);
                    }
                    if (Thread.interrupted()) throw new InterruptedException();
                }
            } catch (InterruptedException | RuntimeException | Error failure) {
                abandonUnacquired();
                throw failure;
            }
        }

        /**
         * 业务作用：不排队等待本地交接或远端 owner，执行至多一次同步 Redis 加锁尝试。
         * 本地状态门禁忙时立即返回 false；门禁空闲时仍包含一次 Redis 网络往返及其命令超时。
         *
         * <p>参数说明: 无。
         *
         * @return 取得所有权时返回 true，锁被占用时返回 false；入口关闭时抛出异常。
         */
        @Override
        public boolean tryLock() {
            try {
                String h = holder();
                // 非阻塞合同同时覆盖本地交接门禁与远端锁状态；门禁忙时不能等待 Redis 往返完成。
                return tryAcquireOnceImmediately(h) == null;
            } catch (RuntimeException | Error failure) {
                abandonUnacquired();
                throw failure;
            }
        }

        /**
         * 业务作用：在调用方预算内等待本地交接、释放订阅和重试，预算耗尽时返回 false。
         * <p>
         * 未取得锁时订阅释放通知并 park，减少固定间隔轮询造成的无效 Redis 请求。
         * 已经发出的 Redis 命令由连接级命令超时约束；不能在结果未知时提前返回 false，否则可能遗留远端所有权。
         *
         * @param time 允许等待本地门禁、释放通知和后续重试的总时长
         * @param unit time 的时间单位
         * @return 截止前取得所有权时返回 true，超时返回 false；入口关闭或线程中断时抛出异常。
         */
        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            String h = holder();
            Thread cur = Thread.currentThread();
            Objects.requireNonNull(unit, "time unit must not be null");
            long timeoutNanos = unit.toNanos(time);
            long startedAt = System.nanoTime();
            try {
                while (true) {
                    if (Thread.interrupted()) throw new InterruptedException();
                    long remainingNanos = remainingNanos(timeoutNanos, startedAt);
                    Long ttl = tryAcquireOnceInterruptibly(h, remainingNanos);
                    if (ttl == null) return true;
                    if (ttl == LOCAL_STATE_BUSY) return false;
                    remainingNanos = remainingNanos(timeoutNanos, startedAt);
                    if (remainingNanos <= 0) return false;
                    if (!subscriptions.tryAcquire(pubChannel, cur, remainingNanos)) return false;
                    try {
                        // TOCTOU 防御
                        remainingNanos = remainingNanos(timeoutNanos, startedAt);
                        if (remainingNanos <= 0) return false;
                        ttl = tryAcquireOnceInterruptibly(h, remainingNanos);
                        if (ttl == null) return true;
                        if (ttl == LOCAL_STATE_BUSY) return false;
                        remainingNanos = remainingNanos(timeoutNanos, startedAt);
                        if (remainingNanos <= 0) return false;
                        long ttlNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1L, Math.min(ttl, leaseTime)));
                        long parkNanos = Math.min(ttlNanos, remainingNanos);
                        if (parkNanos <= 0) return false;
                        LockSupport.parkNanos(parkNanos);
                    } finally {
                        subscriptions.release(pubChannel, cur);
                    }
                    if (Thread.interrupted()) throw new InterruptedException();
                }
            } catch (InterruptedException | RuntimeException | Error failure) {
                abandonUnacquired();
                throw failure;
            }
        }

        /**
         * 业务作用：在句柄状态锁内执行一次 Redis 加锁尝试，使远端结果与本地深度、watchdog 代次原子交接。
         *
         * @param holder 当前调用线程的 Redis holder
         * @return 取得所有权时返回 null；锁被占用时返回剩余 TTL。
         */
        private Long tryAcquireOnce(String holder) {
            stateLock.lock();
            try {
                ensureOwnerOpen();
                Long ttl = redisProxy.evalDirectConnection(LOCK_LUA, Long.class, keys, leaseTimeStr, holder);
                if (ttl == null) markAcquired(holder);
                return ttl;
            } finally {
                stateLock.unlock();
            }
        }

        /**
         * 业务作用：仅在句柄状态门禁当下空闲时执行一次 Redis 加锁尝试，保持无参 `tryLock` 的非阻塞语义。
         *
         * @param holder 当前调用线程的 Redis holder
         * @return 取得所有权时返回 null；Redis 锁被占用时返回 TTL；本地门禁忙时返回 LOCAL_STATE_BUSY。
         */
        private Long tryAcquireOnceImmediately(String holder) {
            if (!stateLock.tryLock()) return LOCAL_STATE_BUSY;
            try {
                ensureOwnerOpen();
                Long ttl = redisProxy.evalDirectConnection(LOCK_LUA, Long.class, keys, leaseTimeStr, holder);
                if (ttl == null) markAcquired(holder);
                return ttl;
            } finally {
                stateLock.unlock();
            }
        }

        /**
         * 业务作用：以可中断方式串行执行一次 Redis 加锁尝试，保持 `lockInterruptibly` 和带超时入口的中断合同。
         *
         * @param holder 当前调用线程的 Redis holder
         * @return 取得所有权时返回 null；锁被占用时返回剩余 TTL。
         */
        private Long tryAcquireOnceInterruptibly(String holder) throws InterruptedException {
            stateLock.lockInterruptibly();
            try {
                ensureOwnerOpen();
                Long ttl = redisProxy.evalDirectConnection(LOCK_LUA, Long.class, keys, leaseTimeStr, holder);
                if (ttl == null) markAcquired(holder);
                return ttl;
            } finally {
                stateLock.unlock();
            }
        }

        /**
         * 业务作用：在调用方剩余预算内取得句柄状态门禁并执行一次 Redis 加锁尝试。
         * 本地门禁等待使用单调时钟预算，超时后不发送 Redis 命令。
         *
         * @param holder 当前调用线程的 Redis holder
         * @param remainingNanos 本轮允许等待本地门禁的剩余纳秒数；非正数只做一次立即尝试
         * @return 取得所有权时返回 null；Redis 锁被占用时返回 TTL；门禁超时时返回 LOCAL_STATE_BUSY。
         */
        private Long tryAcquireOnceInterruptibly(String holder, long remainingNanos) throws InterruptedException {
            if (!stateLock.tryLock(Math.max(0L, remainingNanos), TimeUnit.NANOSECONDS)) return LOCAL_STATE_BUSY;
            try {
                ensureOwnerOpen();
                Long ttl = redisProxy.evalDirectConnection(LOCK_LUA, Long.class, keys, leaseTimeStr, holder);
                if (ttl == null) markAcquired(holder);
                return ttl;
            } finally {
                stateLock.unlock();
            }
        }

        /**
         * 业务作用：按单调时钟计算限时加锁的剩余预算，统一覆盖本地门禁、订阅登记和等待释放的耗时。
         *
         * @param timeoutNanos 调用方给出的总等待预算
         * @param startedAt 开始尝试时的单调时钟快照
         * @return 尚可等待的纳秒数；预算非正或已经耗尽时返回 0。
         */
        private static long remainingNanos(long timeoutNanos, long startedAt) {
            if (timeoutNanos <= 0) return 0;
            long elapsedNanos = System.nanoTime() - startedAt;
            if (elapsedNanos >= timeoutNanos) return 0;
            return timeoutNanos - Math.max(0L, elapsedNanos);
        }

        /**
         * 业务作用：释放锁。重入计数减一，归零时完全释放并停止看门狗；稳定 Lock 引用仍可再次使用。
         *
         * @throws IllegalMonitorStateException 如果当前线程未持有此锁
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；完整释放时停止续租并唤醒等待者，失权或跨线程调用时抛出异常。
         */
        @Override
        public void unlock() {
            Thread current = Thread.currentThread();
            boolean interrupted = Thread.interrupted();
            boolean publish = false;
            try {
                String h = holder();
                stateLock.lock();
                try {
                    Integer depth = localDepths.get(h);
                    // 本地句柄归属是 Redis 解锁之前的第一道门禁；同线程的其它句柄不能借 holder 相同越权释放。
                    if (depth == null || depth <= 0) {
                        throw new IllegalMonitorStateException(
                                "attempt to unlock a lock not acquired through this handle: " + key);
                    }
                    Long result;
                    try {
                        result = redisProxy.evalDirectConnection(UNLOCK_LUA, Long.class, keys, leaseTimeStr, h);
                    } catch (RuntimeException e) {
                        // Redis 结果未知时撤销本句柄的续租权威，让残留所有权由 lease 收敛。
                        disposeLocalLocked(h);
                        throw e;
                    }
                    if (result == null) {
                        String lostKey = key;
                        disposeLocalLocked(h);
                        throw new OwnershipLostException(
                                "lock ownership no longer exists in Redis: " + lostKey);
                    }
                    int remainingDepth = depth - 1;
                    if (remainingDepth == 0) {
                        localDepths.remove(h);
                        endLocalOwnershipLocked(h);
                    } else {
                        localDepths.put(h, remainingDepth);
                    }
                    // Redis 返回 1 表示所有句柄共享的远端重入计数归零，可以唤醒竞争者。
                    publish = result == 1;
                } finally {
                    stateLock.unlock();
                }
                if (publish) {
                    try {
                        redisProxy.pub(pubChannel, "");
                    } catch (Exception ignored) {
                    }
                }
            } finally {
                // Lettuce 会把预先存在的中断标志解释为命令中断；释放期间暂存，完成后按原语义恢复。
                if (interrupted || Thread.interrupted()) current.interrupt();
            }
        }

        /**
         * 业务作用：分布式锁不支持 Condition
         *
         * <p>参数说明: 无。
         *
         * @return 见上述说明。
         */
        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("distributed lock does not support Condition");
        }

        /**
         * 业务作用：本地结束当前 acquisition，不发送 Redis 命令，也不改变稳定 Lock 的 key 绑定。
         * <p>
         * 用于 unlock 路径之外的退出场景，确保 watchdog 不再续租：
         * <ul>
         *   <li>{@code lockLost=true} (锁被新 owner 抢走): 调用方已确认锁不属于自己, 不应再发 UNLOCK_LUA</li>
         *   <li>unlock 抛 {@link IllegalMonitorStateException} (eval 返回 null): 锁实际不在, Redis 端无需再操作</li>
         *   <li>unlock 抛任意 Redis 异常 (eval 抛超时/网络): 锁状态未知, 本地资源先释放, Redis 端靠 lease 兜底</li>
         * </ul>
         * 注意: 调用方必须自己确保锁不需要 UNLOCK_LUA (要么已 unlock, 要么没必要 unlock).
         * 重入未完全释放的 RedisLock 不应调本方法 — 那样会让 watchdog 停掉但 Redis 端锁仍存在.
         *
         * @param holder 本地 watchdog 注册时的 holder 字符串, 用于 cancel 对应任务名
         * 返回: 无返回值；本地续租权威被撤销，Redis 所有权保持原状。
         */
        void disposeLocal(String holder) {
            stateLock.lock();
            try {
                disposeLocalLocked(holder);
            } finally {
                stateLock.unlock();
            }
        }

        /**
         * 业务作用：在句柄状态锁内撤销指定 holder 的全部本地 acquisition，阻止未知远端状态继续续租。
         *
         * @param holder 需要撤销的本地持有者
         * 返回: 无返回值；调用方必须已经持有 stateLock。
         */
        private void disposeLocalLocked(String holder) {
            if (holder != null) localDepths.remove(holder);
            endLocalOwnershipLocked(holder);
        }

        /**
         * 业务作用：结束本句柄的一代所有权并取消其专属 watchdog，不影响同 key 的其它公开句柄。
         *
         * @param holder 本代 watchdog 使用的持有者
         * 返回: 无返回值；调用方必须已经持有 stateLock。
         */
        private void endLocalOwnershipLocked(String holder) {
            released = true;
            generation++;
            if (holder != null) stopWatchdog(holder);
            if (Objects.equals(ownerHolder, holder)) ownerHolder = null;
        }

        // ==================== 看门狗 ====================

        /**
         * 业务作用：记录成功加锁的线程身份并启动或刷新该身份对应的看门狗。
         * owner 记录用于阻止其它线程的错误 unlock 破坏真正持有者的本地续租状态。
         *
         * @param holder 成功写入 Redis Hash 的持有者标识
         * 返回: 无返回值。
         */
        private void markAcquired(String holder) {
            int depth = localDepths.getOrDefault(holder, 0) + 1;
            localDepths.put(holder, depth);
            if (depth == 1) {
                this.ownerHolder = holder;
                this.released = false;
                this.generation++;
                startWatchdog(holder);
            }
        }

        /**
         * 业务作用：启动看门狗: 通过 TimingWheel 定时执行续期 Lua。
         * 每 renewInterval 检查一次, 如果锁仍被自己持有则刷新 TTL。
         * 锁已不在 (被释放或过期) 时自动停止。
         * <p>
         * generation 把回调绑定到创建它的 acquisition；同一稳定 Lock 完整释放后再取得时，旧回调不能续新租约。
         *
         * @param holder 见上述说明
         * 返回: 无返回值；入口关闭时不登记或立即撤销定时任务。
         */
        private void startWatchdog(String holder) {
            if (!TimingWheel.isStarted()) TimingWheel.startTimingWheel();
            String name = watchdogName(holder);
            final AtomicBoolean closedSnapshot = this.ownerClosed;
            final Set<String> tasksSnapshot = this.watchdogTasks;
            if (closedSnapshot.get()) return;
            tasksSnapshot.add(name);
            if (closedSnapshot.get()) {
                tasksSnapshot.remove(name);
                return;
            }
            // 回调只捕获本轮 acquisition 的不可变命令参数与 generation。
            final RedisProxy proxy = this.redisProxy;
            final String[] renewKeys = {this.key};
            final String lease = this.leaseTimeStr;
            final String logKey = this.key;
            final long gen = this.generation;
            // 续租是租约控制面任务且内部执行同步 Redis Lua，使用有界平台线程避免与普通虚拟线程任务共享阻塞扩张风险。
            TimingWheel.platform(renewInterval, renewInterval, name, () -> {
                // generation 不匹配或本轮已释放时，旧回调没有续租权威。
                if (this.generation != gen) return;
                if (released) return;
                // 代理销毁即撤销续租权威；即使定时回调已进入执行队列，也不得再延长旧 owner 的租约。
                if (closedSnapshot.get()) {
                    TimingWheel.cancel(name);
                    tasksSnapshot.remove(name);
                    return;
                }
                try {
                    Long renewed = proxy.evalDirectConnection(RENEW_LUA, Long.class, renewKeys, lease, holder);
                    if (renewed == null || renewed == 0) {
                        TimingWheel.cancel(name);
                        tasksSnapshot.remove(name);
                    }
                } catch (Exception e) {
                    if (closedSnapshot.get()) {
                        TimingWheel.cancel(name);
                        tasksSnapshot.remove(name);
                    } else if (!released) {
                        log.error("watchdog renew failed for key={}", logKey, e);
                    }
                }
            });
            // 关闭可能发生在任务名登记之后、TimingWheel 调度之前。调度完再复验一次，
            // 确保 shutdownSubscriptions 没有因为当时任务尚未可见而遗留续租回调。
            if (closedSnapshot.get()) {
                TimingWheel.cancel(name);
                tasksSnapshot.remove(name);
            }
        }

        /**
         * 业务作用：在每次尝试取得锁之前复验所属代理的锁入口仍开放。
         * 代理销毁会唤醒等待线程，被唤醒者必须终止竞争，不能在关闭后再获得新所有权。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；入口已关闭时抛出 {@link IllegalStateException}。
         */
        private void ensureOwnerOpen() {
            if (ownerClosed == null || ownerClosed.get()) {
                throw new IllegalStateException("distributed lock owner is closed");
            }
        }

        /**
         * 业务作用：记录本次加锁在取得所有权前异常退出，使稳定 Lock 保持未持有状态。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；未取得所有权的重复调用保持幂等，Lock 仍可再次尝试。
         */
        private void abandonUnacquired() {
            // 远端命令只有在 tryAcquireOnce 内确认成功后才会登记本地状态，失败路径无需改变其它线程的 acquisition。
        }

        /**
         * 业务作用：停止看门狗定时任务
         *
         * @param holder 见上述说明
         * 返回: 无返回值；任务不存在时保持幂等。
         */
        private void stopWatchdog(String holder) {
            String name = watchdogName(holder);
            TimingWheel.of().remove(name);
            if (watchdogTasks != null) watchdogTasks.remove(name);
        }

        /**
         * 业务作用：生成包含数据源名称、锁入口会话、真实 key 与 holder 的 watchdog 任务名。
         *
         * @param holder 见上述说明
         * @return 见上述说明。
         */
        private String watchdogName(String holder) {
            return watchdogNamespace + ":" + key + ":" + watchdogHandleId + ":" + holder;
        }
    }
}
