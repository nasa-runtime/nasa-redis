package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.TimingWheel;
import io.github.nasaruntime.core.function.ActionRecycler;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * 虚拟线程友好的 pipeline 专用连接池。
 * <p>
 * 替代 commons-pool2 的 {@code GenericObjectPool}, 消除 {@code synchronized} 导致的虚拟线程 pin。
 * <p>
 * <b>为什么 pipeline 需要独占连接</b>:
 * {@code setAutoFlushCommands(false)} 是连接级状态, 共享连接上调用会影响其他线程。
 * pipeline 必须借一个独占连接, 攒完命令 flushCommands 后归还。
 * <p>
 * <b>并发控制</b>: 用 {@link Semaphore} 而非 {@code synchronized}。
 * {@code Semaphore.acquire()} 在虚拟线程中通过 {@code LockSupport.park()} 挂起,
 * 虚拟线程可以 unmount, 不会 pin carrier 线程。
 * <p>
 * <b>连接创建</b>: 通过 Lettuce 原生 {@code RedisClient/RedisClusterClient.connect()} 创建,
 * 绕过 Redis data adapter 的连接工厂, 不触发 commons-pool2。
 * <p>
 * <b>参数对齐 commons-pool2</b>:
 * <ul>
 *   <li>{@code maxActive} → {@link Semaphore} permits, 最大同时借出连接数 (并发限流)</li>
 *   <li>{@code maxIdle}   → release 时空闲队列超过此值直接 close, 不入队 (实际复用上限)</li>
 *   <li>{@code minIdle}   → 初始化时预创建连接数, 减少首次 borrow 的连接建立延迟</li>
 *   <li>{@code maxWait}   → borrow 超时时间 (ms), 超时抛异常而非无限等待</li>
 * </ul>
 * <b>建议 {@code maxActive = maxIdle}</b>: 若 maxActive &gt; maxIdle, 高峰时超出 maxIdle 的连接
 * 归还即销毁, 下次又要重建, 池化形同虚设。两者相等时连接全部复用, 零创建开销。
 * <p>
 * <b>重要: yml 中 {@code lettuce pool enabled property} 不要设为 true</b>。
 * 本池已替代 commons-pool2, 普通操作走 Lettuce 共享连接 (Netty 多路复用), pipeline 走本池。
 * 若 {@code pool.enabled=true}, Spring 会创建 {@code LettucePoolingClientConfiguration} 走 commons-pool2,
 * 虚拟线程 pin 问题会重新出现。pool 参数 (max-active/max-idle/min-idle/max-wait) 仍然有效,
 * 由本池读取使用, 只是 {@code enabled} 必须保持 false (默认值)。
 */
@Slf4j
public class PipelineConnectionPool {

    /**
     * 空闲连接队列。borrow 时 poll, release 时 offer (不超过 maxIdle)。
     * 无锁 CAS 操作, 不涉及 synchronized。
     */
    private final ConcurrentLinkedQueue<StatefulConnection<byte[], byte[]>> idle = new ConcurrentLinkedQueue<>();
    /**
     * Lettuce 原生客户端, 用于创建新连接。
     * 从 LettuceConnectionFactory.getNativeClient() 获取, 绕过 commons-pool2。
     */
    private final AbstractRedisClient client;
    /**
     * 并发控制信号量, permits = maxActive。
     * 控制最大同时借出连接数, acquire 时虚拟线程可 unmount, 不 pin carrier。
     */
    private final Semaphore permits;
    /**
     * 是否 Redis Cluster 模式, 决定 connect 调 RedisClusterClient 还是 RedisClient
     */
    private final boolean cluster;
    /**
     * 最大同时借出连接数 (对应 pool.max-active)
     */
    private final int maxActive;
    /**
     * 空闲队列最大保留数 (对应 pool.max-idle), 归还时超出此值的连接直接关闭。
     * 也是实际复用上限: 只有 maxIdle 个连接能被池化复用, 超出的即用即销。
     */
    @Getter
    private final int maxIdle;
    /**
     * 借连接最大等待时间 ms (对应 pool.max-wait), ≤0 表示无限等待
     */
    private final long maxWait;
    /**
     * pipeline flush 后 awaitAll 等待 sync 命令完成的最大时长 ms (对应 redis 命令超时配置).
     * ≤0 表示无限等待。超时由调用方（LettucePipeline）抛异常给业务线程。
     */
    @Getter
    private final long awaitTimeoutMs;
    /**
     * 池是否已关闭, volatile 保证跨线程可见
     */
    private volatile boolean closed = false;

    /**
     * 线程亲和: threadId → 上次该线程 release 的 conn.
     * <p>
     * 用途: 同一线程下次 borrow 时优先复用同一条 conn, 保证 Redis 端命令保序到达
     * (跨 actuator session / 跨 autoFlush batch 都依赖此机制).
     * <p>
     * <b>过期机制</b>: 不在 entry 里存 expireNs, 而是 release 时
     * {@code TimingWheel.cancel(key) + TimingWheel.exec(50ms, key, evict)}
     * 让 50ms 后自动 {@code affinity.remove(tid)}. 这样:
     * <ul>
     *   <li>borrow 路径仅需 {@code affinity.get(tid)} + {@code conn.isOpen()} 两层检查, 零 nanoTime 开销</li>
     *   <li>50ms 内同线程多次 release 会自动 cancel 上次 evict + 重新 schedule, TTL 不累加</li>
     * </ul>
     * <p>
     * <b>"同 conn 可被多 session 绑定 + 同一时刻只一个使用"</b> 的实现:
     * 多个 threadId 的 affinity 可以指向同一条 conn (共享 ownership 提示);
     * 实际"使用权"通过 {@link #idle} 的 ownership 转移 (remove 原子) 保证互斥.
     */
    private final ConcurrentHashMap<Long, StatefulConnection<byte[], byte[]>> affinity = new ConcurrentHashMap<>();

    /**
     * 当前正在 parkNanos 等亲和 conn 的线程: Thread → 它在等的 conn.
     * release 时反查此 map, 精确 unpark 等待这条 conn 的线程, 避免 thundering herd.
     */
    private final ConcurrentHashMap<Thread, StatefulConnection<byte[], byte[]>> waiters = new ConcurrentHashMap<>();

    /**
     * 亲和缓存 TTL (毫秒): 50ms 内同线程再次 borrow 优先匹配上次的 conn.
     */
    private static final long AFFINITY_TTL_MS = 50L;

    /**
     * 亲和缓存 TTL (纳秒): 给 {@link LockSupport#parkNanos} 等亲和 ownership 时用作 deadline.
     */
    private static final long AFFINITY_TTL_NS = AFFINITY_TTL_MS * 1_000_000L;

    /**
     * TimingWheel evict 任务的 unique key 前缀, 拼接 threadId 作为唯一标识.
     * 同 threadId 的 release 反复 schedule 会 cancel 上次再 exec 新任务, TTL 不累加.
     */
    private static final String AFFINITY_EVICT_KEY_PREFIX = "pipeline-pool-affinity-";

    /**
     * 亲和 evict 任务的零 GC consumer.
     * <p>
     * 槽位约定: {@code ref(0)} = PipelineConnectionPool 实例; {@code val(0)} = threadId.
     * action() 执行完毕后 ActionRecycler 自动归池, 整个调度路径无 lambda 分配 / 无 long 装箱.
     * <p>
     * 配合 {@link TimingWheel#cancel} 使用: 50ms 内同 tid 再次 release 会 cancel 旧 task,
     * cancel 路径下 TimingWheel 内部会 recycle 旧 ActionRecycler 归池 (框架在 cancel-skip 分支主动 recycle action).
     */
    private static final Consumer<ActionRecycler> EVICT_AFFINITY_ACTION = ar -> {
        PipelineConnectionPool pool = ar.ref(0);
        long tid = ar.longVal(0);
        pool.affinity.remove(tid);
    };

    /**
     * 业务作用：建出批次专用的连接池，使批量发送不与常规命令争抢同一条连接。
     * 批次会在一条连接上连续写入大量命令，与常规命令共用会让后者的延迟出现明显毛刺。
     * <p>
     * 构造时按最小空闲数预热连接：不预热则首批请求要现场建连，表现为服务刚启动时的一段高延迟。
     * 预热失败不阻断构造，连接会在实际使用时按需补建。
     * <p>
     * 以信号量按最大活跃数限流而非无界建连：无界会在流量突增时把连接数推到服务端上限，
     * 届时<b>所有</b>客户端都连不上，影响面远大于本服务排队等待。
     *
     * @param client         Lettuce 原生客户端
     * @param cluster        是否为集群模式，决定建出的连接类型
     * @param maxActive      最大活跃连接数，即并发批次数上限
     * @param maxIdle        最大空闲连接数，超出部分在归还时关闭
     * @param minIdle        最小空闲连接数，构造时按此数量预热
     * @param maxWait        取连接的最长等待毫秒数
     * @param awaitTimeoutMs 等待批次结果的超时毫秒数
     */
    public PipelineConnectionPool(AbstractRedisClient client, boolean cluster,
                                  int maxActive, int maxIdle, int minIdle, long maxWait, long awaitTimeoutMs) {
        this.client = client;
        this.cluster = cluster;
        this.maxActive = maxActive;
        this.maxIdle = maxIdle;
        this.maxWait = maxWait;
        this.awaitTimeoutMs = awaitTimeoutMs;
        this.permits = new Semaphore(maxActive);
        // 预热: 预创建 minIdle 个连接
        for (int i = 0; i < Math.min(minIdle, maxIdle); i++) {
            try {
                idle.offer(this.createConnection());
            } catch (Exception e) {
                log.warn("pipeline pool warmup failed at {}/{}", i, minIdle, e);
                break;
            }
        }
    }

    /**
     * 业务作用：借一个独占连接。
     * <p>
     * <b>亲和快速路径</b> (跨 session 同 conn 保序的核心机制):
     * <ol>
     *   <li>查 {@link #affinity}, 若同线程 50ms 内 release 过 conn → 优先匹配同一条 conn</li>
     *   <li>{@code idle.remove(affConn)} 原子拿到 ownership: 成功 → 直接返回, 跳过 idle.poll</li>
     *   <li>remove 失败 (conn 被别人 poll 走) → 用 {@link LockSupport#parkNanos} 等待
     *       (release 时反查 {@link #waiters} 精确 unpark), 直至 TTL 过期或唤醒重试成功</li>
     *   <li>TTL 过期仍未拿到 → fallback 走原 borrow (放弃顺序换不阻塞)</li>
     * </ol>
     * <p>
     * <b>permit 控制</b>: 亲和路径同样要 acquire permit, 否则会绕过 maxActive 限制.
     * remove 失败要立即 release permit, 不能持有 permit 去 park (其他线程会饿死).
     * <p>
     * <b>shutdown 检查</b>: 入口 fail-fast; 各 claim 成功路径 (affinity / idle poll / create) 再经 {@link #returnClaimedOrFailClosed}
     * 二次确认, 防 closed 在检查后置位仍把连接借出 (shutdown 的 drain 关不到已摘走的连接).
     * <p>
     * maxWait > 0 时超时抛 {@link IllegalStateException}; ≤0 时无限等待 (Semaphore, VT 友好)。
     *
     * @return 原生 Lettuce 连接 (StatefulRedisConnection 或 StatefulRedisClusterConnection)
     */
    public StatefulConnection<byte[], byte[]> borrow() {
        // 已 shutdown 直接 fail-fast, 不再 acquire permit / 创建连接
        if (closed) {
            throw new IllegalStateException("pipeline pool already shutdown");
        }
        // ============ 亲和快速路径 ============
        long tid = Thread.currentThread().threadId();
        StatefulConnection<byte[], byte[]> affConn = affinity.get(tid);
        if (affConn != null && affConn.isOpen()) {
            // 先 acquire permit, 拿到再 try-remove
            if (this.tryAcquirePermitForAffinity()) {
                if (idle.remove(affConn)) {
                    // 亲和命中, ownership 拿到; 二次确认未 shutdown (closed 可能在 remove 后才置位)
                    return this.returnClaimedOrFailClosed(affConn);
                }
                // remove 失败 (别人占用), 释放 permit 等 unpark
                permits.release();
            }
            // 亲和命中但 ownership 被别的线程占着 → park 等 release 唤醒
            // deadline 是 borrow 这一刻 + TTL, 跟 affinity map 是否还在无关 (evict 可能已 fire)
            long deadlineNs = System.nanoTime() + AFFINITY_TTL_NS;
            StatefulConnection<byte[], byte[]> got = this.parkForAffinity(affConn, deadlineNs);
            if (got != null) return got;
            // TTL 过期, fallback 走正常 borrow
        }

        // ============ 正常 borrow 路径 (原逻辑) ============
        // 获取 permit
        if (maxWait > 0) {
            try {
                if (!permits.tryAcquire(maxWait, TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException(
                            "pipeline pool exhausted, cannot borrow within " + maxWait + "ms (maxActive=" + maxActive + ")");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("pipeline pool borrow interrupted", e);
            }
        } else {
            permits.acquireUninterruptibly();
        }
        // acquire permit 后二次判断: 防 shutdown 与 borrow 竞争 (此刻池可能刚被关闭)
        if (closed) {
            permits.release();
            throw new IllegalStateException("pipeline pool already shutdown");
        }
        boolean success = false;
        try {
            // 优先从空闲队列取, 跳过已断开的连接
            StatefulConnection<byte[], byte[]> conn;
            while ((conn = idle.poll()) != null) {
                if (conn.isOpen()) {
                    // 先置 success 再二次确认 closed: returnClaimedOrFailClosed 已自行 release permit,
                    // 置 success 防 finally 再 release 一次 (双重释放). poll/create 也可能在 closed 置位后才返回.
                    success = true;
                    return this.returnClaimedOrFailClosed(conn);
                }
                try {
                    conn.close();
                } catch (Exception ignored) {
                }
            }
            // 空闲队列没有可用连接, 新建
            conn = this.createConnection();
            success = true;
            return this.returnClaimedOrFailClosed(conn);
        } finally {
            if (!success) permits.release();
        }
    }

    /**
     * 业务作用：已通过 {@code idle.remove} 拿到 conn 的 ownership 后, 二次确认池未关闭。
     * <p>
     * shutdown 可能在 {@link #borrow} 入口检查之后、{@code idle.remove(affConn)} 成功之前/之后把 {@code closed} 置 true。
     * 此时连接已被本线程从 idle 摘走, {@link #shutdown} 的 drain 关不到它, 直接返回会在关闭过程中仍借出连接。
     * 故未关闭则原样返回; 已关闭则关连接 + 释放 permit + fail-fast, 与正常 borrow 路径的二次检查一致。
     *
     * @param conn 见上述说明
     */
    private StatefulConnection<byte[], byte[]> returnClaimedOrFailClosed(StatefulConnection<byte[], byte[]> conn) {
        if (!closed) return conn;
        try {
            conn.close();
        } catch (Exception ignored) {
        } finally {
            permits.release();
        }
        throw new IllegalStateException("pipeline pool already shutdown");
    }

    /**
     * 业务作用：亲和路径专用的 permit 获取: 非阻塞 tryAcquire, 不能拿到立即返回 false 让 caller 走 park 等.
     * <p>
     * 不能阻塞等 permit: 如果阻塞等到 permit 但 ownership 还是被别人占, 还得 release permit 再 park,
     * 等于双重等待. 用 tryAcquire 立刻区分"池有容量但 ownership 被占" vs "池没容量了".
     */
    private boolean tryAcquirePermitForAffinity() {
        return permits.tryAcquire();
    }

    /**
     * 业务作用：亲和命中但 ownership 被别人占时, 循环 "先 try-claim 再 parkNanos" 等到:
     * <ul>
     *   <li>{@link #release(StatefulConnection)} 时反查 waiters 精确 unpark 当前线程</li>
     *   <li>或 TTL 过期 (parkNanos timeout 自然返回)</li>
     * </ul>
     * <b>claim 在 park 之前</b>: 覆盖 "borrow 首次 remove 失败 → waiters.put" 之间 holder release 的 missed wakeup 窗口
     * (那次 release 的 unparkWaitersOf 扫不到本线程, 否则会空等满 TTL). 拿到 ownership 返回 conn, 否则 park, 唤醒/超时后再 claim.
     *
     * @param affConn    见上述说明
     * @param deadlineNs 见上述说明
     * @return 拿到 ownership 的 conn; TTL 过期未拿到返回 null (caller 走正常 borrow)
     */
    private StatefulConnection<byte[], byte[]> parkForAffinity(
            StatefulConnection<byte[], byte[]> affConn, long deadlineNs) {
        Thread me = Thread.currentThread();
        waiters.put(me, affConn);
        try {
            while (!closed) {
                // 先 claim 再 park: 覆盖 "borrow 首次 remove 失败 → waiters.put" 之间 holder release 的 missed wakeup 窗口.
                // 否则那次 release 的 unparkWaitersOf 扫不到本线程, 会白等满 50ms TTL 才醒 (尾延迟).
                if (this.tryAcquirePermitForAffinity()) {
                    if (idle.remove(affConn)) {
                        return this.returnClaimedOrFailClosed(affConn);
                    }
                    permits.release();
                }
                // conn 被关闭了, 放弃亲和走正常 borrow
                if (!affConn.isOpen()) return null;
                long remainNs = deadlineNs - System.nanoTime();
                if (remainNs <= 0) return null;
                LockSupport.parkNanos(this, remainNs);
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return null;
        } finally {
            waiters.remove(me);
        }
    }

    /**
     * 业务作用：归还连接。空闲队列未满则回队, 否则关闭。
     * <p>
     * <b>conn=null 直接返回不 release permit</b>: 防御 caller 误把没 borrow 成功的 null 传过来,
     * 否则会让 permits 数变虚高, 池容量假象。borrow 成功的 caller 一定有非 null conn.
     * <p>
     * <b>亲和注册</b>: 归还成功 (回 idle 队列) 时, 记录 {@code affinity[当前 threadId] = conn},
     * TTL 50ms, 同线程下次 borrow 优先匹配同一条 conn (跨 session / 跨 autoFlush batch 保序).
     * <p>
     * <b>unpark waiters</b>: 反查 {@link #waiters} 找出正在等这条 conn 的线程, 精确 unpark
     * (而不是 unpark 全员避免 thundering herd). 被 unpark 的线程从 parkNanos 返回后重试拿 ownership.
     *
     * @param conn 见上述说明
     */
    public void release(StatefulConnection<byte[], byte[]> conn) {
        if (conn == null) return;
        // try 内不能用 return 跳出 (会跳过末尾的 affinity 注册), 用 boolean 标记 + if/else 分支
        boolean offered = false;
        try {
            if (!conn.isOpen() || closed) {
                try {
                    conn.close();
                } catch (Exception ignored) {
                }
            } else if (idle.size() < maxIdle) {
                offered = idle.offer(conn);
            } else {
                try {
                    conn.close();
                } catch (Exception ignored) {
                }
            }
        } finally {
            permits.release();
        }
        // 注: idle 未满 → offered=true 才注册亲和; idle 已满 / conn 已关闭 时跳过亲和无意义
        if (offered) {
            this.registerAffinity(conn);
            this.unparkWaitersOf(conn);
        }
    }

    /**
     * 业务作用：更新当前线程的亲和缓存, cancel 旧 evict 任务并 schedule 新的 50ms evict.
     * <p>
     * <b>cancel + exec 的 unique 模式</b>: TimingWheel 内部 {@code uniqueIndex} 按 unique key 索引任务,
     * cancel 是 O(1) 惰性标记 (旧 task 设 cancelled=true, tick 时跳过); exec 用同 unique key 调度新任务.
     * 这样每次 release 后 evict 时刻总是 "本次 release + 50ms", TTL 不累加.
     * <p>
     * <b>零 GC</b>: 用池化 {@link ActionRecycler} 替代 {@code () -> affinity.remove(tid)} lambda;
     * cancel 路径下 TimingWheel 框架会在 cancel-skip 分支主动 recycle action (若为 Recycler), 池化无泄漏.
     *
     * @param conn 见上述说明
     */
    private void registerAffinity(StatefulConnection<byte[], byte[]> conn) {
        long tid = Thread.currentThread().threadId();
        affinity.put(tid, conn);
        String key = AFFINITY_EVICT_KEY_PREFIX + tid;
        TimingWheel.cancel(key);
        ActionRecycler ar = ActionRecycler.ofRecycle(EVICT_AFFINITY_ACTION)
                .ref(0, this)
                .val(0, tid);
        TimingWheel.exec(AFFINITY_TTL_MS, key, ar);
    }

    /**
     * 业务作用：反查 waiters, 精确 unpark 等这条 conn 的所有线程.
     * <p>
     * waiters 通常极少 (&lt; maxActive), O(W) 扫描可接受. 不用 unpark 全员是为了避免
     * thundering herd: 多个等待者同时醒来抢同一个 idle.remove(conn), 只有一个能成功.
     *
     * @param conn 见上述说明
     */
    private void unparkWaitersOf(StatefulConnection<byte[], byte[]> conn) {
        for (Map.Entry<Thread, StatefulConnection<byte[], byte[]>> entry : waiters.entrySet()) {
            if (entry.getValue() == conn) {
                LockSupport.unpark(entry.getKey());
            }
        }
    }

    /**
     * 业务作用：使连接失效: 直接关闭而非回 idle 队列, 只释放 permit。
     * <p>
     * 用于 pipeline flush 未正常完成的场景 (flushCommands 抛异常 / setAutoFlushCommands(true) 还原失败):
     * 此时连接的 Lettuce command buffer 里可能残留 setAutoFlushCommands(false) 后写入但未 flush 的命令。
     * setAutoFlushCommands(true) 只翻 flag 不会 flush 残留, 若按 {@link #release} 回池,
     * 下一个 borrower flush 时会把残留命令一并发出, 造成跨借用方命令串扰、future 永久挂起。
     * 关闭连接让 Lettuce fail 掉所有 pending future, 业务线程的 lf.getFinally 可解阻塞。
     * <p>
     * <b>conn=null 不 release permit</b>: 与 {@link #release} 同义, 防御没 borrow 成功的 null。
     *
     * @param conn 见上述说明
     */
    public void invalidate(StatefulConnection<byte[], byte[]> conn) {
        if (conn == null) return;
        try {
            conn.close();
        } catch (Exception ignored) {
        } finally {
            permits.release();
        }
    }

    /**
     * 业务作用：pipeline awaitAll 用的超时 Duration: {@link #awaitTimeoutMs} &gt; 0 时返回对应时长,
     * ≤0 时返回 {@code Duration.ofSeconds(-1)}（LettuceFutures 约定的无限等待哨兵）。
     */
    public Duration awaitTimeout() {
        return awaitTimeoutMs > 0 ? Duration.ofMillis(awaitTimeoutMs) : Duration.ofSeconds(-1);
    }

    /**
     * 业务作用：从借来的连接获取 async commands (pipeline 路径用)。
     * 兼容 standalone 和 cluster 两种连接类型。
     *
     * @param conn 见上述说明
     * @return 见上述说明。
     */
    public static RedisClusterAsyncCommands<byte[], byte[]> async(StatefulConnection<byte[], byte[]> conn) {
        if (conn instanceof StatefulRedisClusterConnection<byte[], byte[]> c) {
            return c.async();
        }
        return ((StatefulRedisConnection<byte[], byte[]>) conn).async();
    }

    /**
     * 业务作用：关闭池: 标记已关闭 + 关闭所有空闲连接 + 清亲和缓存 + 唤醒所有 park 等待者.
     * <p>
     * waiters 中的线程被 unpark 后, 在 {@link #parkForAffinity} 内会因 {@code closed=true} 退出循环,
     * 返回 null, 让 caller 走正常 borrow 路径或 fail-fast.
     */
    public void shutdown() {
        closed = true;
        StatefulConnection<byte[], byte[]> conn;
        while ((conn = idle.poll()) != null) {
            try {
                conn.close();
            } catch (Exception ignored) {}
        }
        affinity.clear();
        for (Thread t : waiters.keySet()) {
            LockSupport.unpark(t);
        }
    }

    /**
     * 业务作用：按部署模式建出一条新连接：集群模式与单机模式的连接类型不同，在此统一分派。
     * 连接采用字节编解码而非字符串：批次路径上的键值已在入队前完成序列化，
     * 再经一次字符串编解码既多一次拷贝也会破坏二进制内容。
     *
     * <p>参数说明: 无。
     *
     * @return 新建的连接。
     */
    private StatefulConnection<byte[], byte[]> createConnection() {
        if (cluster) {
            return ((RedisClusterClient) client).connect(ByteArrayCodec.INSTANCE);
        }
        return ((RedisClient) client).connect(ByteArrayCodec.INSTANCE);
    }
}
