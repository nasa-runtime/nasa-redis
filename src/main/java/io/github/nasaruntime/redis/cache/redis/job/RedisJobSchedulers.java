package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 业务作用：按语言无关 source id 创建并托管互相隔离的 {@link RedisJobScheduler}，
 * 使多个 Redis 数据源的调度、注册表、线程池、生命周期与 Fanout 互不影响。
 *
 * <p>本类负责启停全部实际引用的数据源，框架不暴露默认 {@code RedisJobScheduler} Bean。业务通过
 * {@link #scheduler(String)} 显式选择 source；注解登记器同样只为任务声明的 qualifier 建立运行时。
 *
 * <p>静态入口由 JVM 内唯一的活动管理器承载。并行构造第二套管理器会被拒绝；关闭当前管理器时先撤销
 * 静态入口，再释放全部 Scheduler，旧 Scheduler 不能跨应用上下文复用。
 *
 * <p>停机是不可逆的：{@code RedisJobScheduler.stop()} 会关闭监视线程池与各 Dispatcher，之后 start 抛错。
 * 因此容器 stop 之后不能再 start，这与 Spring Lifecycle 可重启的一般约定不同，热重启场景应重建上下文。
 */
@Slf4j
public final class RedisJobSchedulers implements SmartLifecycle, AutoCloseable {

    private static final AtomicReference<RedisJobSchedulers> ACTIVE = new AtomicReference<>();
    private static final String ROOT_PREFIX = "nasa.redis.job";
    private static final String SOURCE_PREFIX = ROOT_PREFIX + ".sources.";
    private static final String BEAN_SUFFIX = "RedisProxy";

    private final Environment environment;
    private final RedisJobProperties root;
    private final Map<String, RedisJobScheduler> schedulers = new ConcurrentHashMap<>();
    private final List<RedisJobScheduler> managedSchedulers = new ArrayList<>();
    /* 生命周期状态锁：ConcurrentHashMap 只保证单次 map 操作安全，无法让"建立-启动-关闭"三种迁移互斥。 */
    private final Object lifecycle = new Object();
    private final AtomicBoolean terminalRequested = new AtomicBoolean();
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final CountDownLatch shutdownCompleted = new CountDownLatch(1);
    private final AtomicBoolean finalCleanupStarted = new AtomicBoolean();
    private final CountDownLatch finalCleanupCompleted = new CountDownLatch(1);
    private volatile List<RedisJobScheduler> shutdownSnapshot = List.of();
    private volatile Throwable shutdownFailure;
    private volatile Throwable finalCleanupFailure;
    private boolean running;
    private boolean stopped;
    private boolean closed;

    /**
     * 业务作用：绑定配置来源与根级默认参数，构造后不创建任何 Scheduler，等待按实际使用的数据源惰性建立。
     *
     * <p>{@code defaultProxy} 只用于建立 Bean 之间的创建顺序，使全部数据源在解析 qualifier 之前完成登记；
     * 它不会被当作任何数据源的兜底代理，解析 qualifier 一律只认 {@link RedisProxy#load(String)} 的真实结果。
     * JVM 中已经存在另一套活动管理器时构造失败，避免静态入口与容器生命周期分裂。
     *
     * @param environment  配置环境，用于读取逐源覆盖项
     * @param root         job 根级默认参数
     * @param defaultProxy 默认数据源的命令代理，仅用于排序依赖
     */
    public RedisJobSchedulers(Environment environment, RedisJobProperties root, RedisProxy defaultProxy) {
        this.environment = Objects.requireNonNull(environment, "environment must not be null");
        this.root = copyProperties(Objects.requireNonNull(root, "root properties must not be null"));
        Objects.requireNonNull(defaultProxy, "default RedisProxy must not be null");
        // 静态业务入口必须只指向一套容器生命周期。允许两套管理器同时登记会让同一 source 出现两组
        // 扫描线程与本地 Handler 表，关闭其中一个时静态入口还可能指向已经失权的另一套运行时。
        if (!ACTIVE.compareAndSet(null, this)) {
            throw new IllegalStateException("RedisJobSchedulers already initialized in this JVM");
        }
    }

    /**
     * 业务作用：显式取得指定 Redis source 的 Scheduler，并在首次引用时纳入当前容器的统一生命周期。
     *
     * <p>本方法不提供默认 source。调用方必须传入非空 qualifier；基础设施未由
     * {@link EnableRedisJob} 建立、source 不存在或管理器已经停止时直接拒绝。
     *
     * @param qualifier 业务明确选择的语言无关 source id
     * @return 该 source 在当前进程内唯一的 Scheduler。
     */
    public static RedisJobScheduler scheduler(String qualifier) {
        RedisJobSchedulers active = ACTIVE.get();
        if (active == null) {
            throw new IllegalStateException(
                    "RedisJob is not initialized; declare @EnableRedisJob and set nasa.redis.job.enabled=true");
        }
        return active.resolve(qualifier);
    }

    /**
     * 业务作用：复制一份可独立绑定的 Job 参数，并隔离绑定器会原地改写的线协议嵌套分组。
     *
     * @param source 参数来源
     * @return 与来源共享零个 {@link RedisJobProperties.Wire}、{@link RedisJobProperties.Json} 实例的副本。
     */
    private static RedisJobProperties copyProperties(RedisJobProperties source) {
        RedisJobProperties target = new RedisJobProperties();
        BeanUtils.copyProperties(source, target);
        RedisJobProperties.Wire sourceWire = source.getWire();
        if (sourceWire == null) {
            target.setWire(null);
            return target;
        }
        RedisJobProperties.Wire targetWire = new RedisJobProperties.Wire();
        RedisJobProperties.Json sourceJson = sourceWire.getJson();
        if (sourceJson == null) {
            targetWire.setJson(null);
        } else {
            RedisJobProperties.Json targetJson = new RedisJobProperties.Json();
            targetJson.setDefaultTyping(sourceJson.isDefaultTyping());
            targetWire.setJson(targetJson);
        }
        target.setWire(targetWire);
        return target;
    }

    /**
     * 业务作用：把 Java 本地别名归一为写入协议、指标和业务幂等键的语言无关 source id。
     *
     * <p>{@code primary} 与 {@code redisProxy}、{@code match} 与 {@code matchRedisProxy} 指向同一数据源，
     * 但对外只使用不带 Spring Bean 后缀的名字，否则 Go、Rust 侧无法按同一 id 对账。
     *
     * @param qualifier 注解、静态入口或 RedisProxy 声明的数据源名
     * @return 归一后的 source id；输入为空时直接拒绝。
     */
    public static String sourceId(String qualifier) {
        String value = Objects.requireNonNull(qualifier, "qualifier must not be null").trim();
        if (value.isEmpty()) throw new IllegalArgumentException("qualifier must not be blank");
        if (RedisProxy.PRIMARY.equalsIgnoreCase(value) || "redisProxy".equalsIgnoreCase(value)) {
            return RedisProxy.PRIMARY;
        }
        if (value.length() > BEAN_SUFFIX.length()
                && value.regionMatches(true, value.length() - BEAN_SUFFIX.length(),
                BEAN_SUFFIX, 0, BEAN_SUFFIX.length())) {
            value = value.substring(0, value.length() - BEAN_SUFFIX.length());
        }
        return value;
    }

    /**
     * 业务作用：取得指定数据源的 Scheduler，不存在时按逐源配置创建并纳入统一生命周期。
     *
     * <p>数据源没有对应 {@link RedisProxy} 时立即抛错终止启动：静默回退到默认数据源会把任务定义、Run、
     * Stream 和 Fanout 记录写进错误的 Redis，形成一份无人调度或被另一集群重复调度的定义。
     *
     * @param qualifier 数据源名，允许 Java 本地别名
     * @return 该数据源独占的 Scheduler。
     */
    RedisJobScheduler resolve(String qualifier) {
        String sourceId = sourceId(qualifier);
        RedisJobScheduler created;
        boolean startNow;
        // 建立与生命周期状态迁移必须线性化: 否则新建实例可能既没被本次调用启动、
        // 也没被并发 start() 的弱一致迭代看到, 最终留下一台永不领取任务的 Scheduler;
        // 与 close() 竞争时还可能在关闭之后放回一台持有线程池与连接的实例。
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("RedisJobSchedulers is closed");
            if (stopped) throw new IllegalStateException("RedisJobSchedulers is stopped");
            if (terminalRequested.get()) throw new IllegalStateException("RedisJobSchedulers is stopping");
            RedisJobScheduler existing = schedulers.get(sourceId);
            if (existing != null) return existing;
            created = create(sourceId);
            schedulers.put(sourceId, created);
            // 构造完成即纳入终态所有权清单；运行期启动失败可以退出路由表，但在最终关闭完成前不能失去追踪。
            managedSchedulers.add(created);
            startNow = running;
        }
        if (!startNow) {
            rejectTerminalCreation(created);
            return created;
        }
        try {
            created.start();
        } catch (RuntimeException | Error error) {
            // 启动失败的实例必须从登记表移除并释放: 留在表里, 下次取到的就是一台对外声称可用、
            // 内部资源只起了一半的 Scheduler, 而运行期动态创建没有 Spring 销毁流程兜底。
            synchronized (lifecycle) {
                schedulers.remove(sourceId, created);
            }
            try {
                created.close();
            } catch (RuntimeException | Error ignored) {
                // 释放失败不顶替真正的启动失败原因
            }
            throw error;
        }
        rejectTerminalCreation(created);
        return created;
    }

    /**
     * 业务作用：列出当前已建立的全部数据源 id，供健康检查与指标按数据源展开。
     *
     * <p>参数说明: 无。
     *
     * @return 已建立的 source id 集合快照。
     */
    public Collection<String> sourceIds() {
        return List.copyOf(schedulers.keySet());
    }

    /**
     * 业务作用：列出当前已建立的全部 Scheduler，供批量查询运行状态。
     *
     * <p>参数说明: 无。
     *
     * @return source id 到 Scheduler 的只读映射。
     */
    public Map<String, RedisJobScheduler> all() {
        return Map.copyOf(schedulers);
    }

    /**
     * 业务作用：按 source id 建立独占运行时，逐源覆盖项缺失时沿用 job 根级默认参数。
     *
     * @param sourceId 归一后的数据源 id
     * @return 该数据源的 Scheduler。
     */
    private RedisJobScheduler create(String sourceId) {
        // qualifier 是强路由合同: 只接受登记表里的真实结果。任何兜底代理都会让 Scheduler 对外报告一个
        // source id、实际却把定义、Run、Stream 和 Fanout 写进另一个数据源, 观测信息反过来掩盖真实路由。
        RedisProxy proxy = RedisProxy.load(sourceId);
        if (proxy == null) {
            throw new IllegalStateException("unknown RedisJob qualifier: " + sourceId
                    + "; registered=" + registered()
                    + "; RedisJob refuses to fall back to " + RedisProxy.PRIMARY);
        }
        RedisJobProperties properties = resolveProperties(sourceId);
        // 逐源显式关闭的数据源不得建立 Scheduler: 配置写了关闭却照常调度, 运维就失去了单独停用一个数据源的手段
        if (!properties.isEnabled()) {
            throw new IllegalStateException("RedisJob source is explicitly disabled: " + sourceId
                    + "; remove nasa.redis.job.sources." + sourceId
                    + ".enabled=false or stop referencing it from @RedisJob");
        }
        return new RedisJobScheduler(proxy, properties);
    }

    /**
     * 业务作用：为拒绝启动的错误信息列出已登记数据源，读取失败时降级为占位说明。
     *
     * <p>诊断信息本身不能再抛异常：一旦抛出就会顶替掉真正的 qualifier 缺失原因，把排查引向错误方向。
     *
     * <p>参数说明: 无。
     *
     * @return 已登记数据源名的可读列表。
     */
    private static String registered() {
        try {
            return String.join(",", RedisProxy.qualifiers());
        } catch (RuntimeException error) {
            return "<unavailable>";
        }
    }

    /**
     * 业务作用：生成该数据源的完整参数：先取 job 根级默认值，再叠加 {@code sources.<id>} 下真实存在的键。
     *
     * <p>只覆盖配置中显式出现的键，避免逐源配置对象里未填写的字段以类型默认值把根级设置覆盖掉。
     * {@code sources.<id>} 只是覆盖入口，不是启用该数据源的前置条件。
     *
     * @param sourceId 归一后的数据源 id
     * @return 该数据源使用的参数；source id 由实际选中的 RedisProxy 冻结，不从参数对象读取。
     */
    private RedisJobProperties resolveProperties(String sourceId) {
        Binder binder = Binder.get(environment);
        RedisJobProperties properties = copyProperties(root);
        // 根 Bean 可能经过应用自己的绑定转换或启动期加工；构造快照才是逐源默认合同，不能绕过它从环境重新绑定。
        // 每个 Scheduler 使用嵌套分组也相互隔离的副本，逐源绑定不会污染其它数据源或根 Bean。
        // 数据源名允许驼峰（userCenter），而配置属性名只接受规范形式；必须 adapt 后再定位，
        // 否则驼峰数据源在建立 Scheduler 时就会因属性名非法而中止启动。
        // adapt 产出的规范名与配置里写成 user-center、userCenter、USER_CENTER 的键都能匹配。
        binder.bind(ConfigurationPropertyName.adapt(SOURCE_PREFIX + sourceId, '.'),
                Bindable.ofInstance(properties));
        return properties;
    }

    /**
     * 业务作用：启动全部已建立的数据源；任一数据源启动失败都会中断，避免半启动状态被当成健康。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：全部已建立 source 启动成功时返回；任一 source 启动失败时回滚已启动实例并抛出原异常。
     */
    @Override
    public void start() {
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("RedisJobSchedulers is closed");
            if (stopped) throw new IllegalStateException("RedisJobSchedulers cannot restart after stop");
            if (terminalRequested.get()) throw new IllegalStateException("RedisJobSchedulers is stopping");
            List<RedisJobScheduler> started = new ArrayList<>();
            try {
                for (RedisJobScheduler scheduler : schedulers.values()) {
                    if (terminalRequested.get()) throw new IllegalStateException("RedisJobSchedulers is stopping");
                    // 先登记再启动: 启动中途抛错的那一台自身可能已经起了一部分资源, 必须进入回滚名单
                    started.add(scheduler);
                    scheduler.start();
                    // 后一个 source 卡在 Redis 时，停机终态可在管理器锁外先行发布；每次返回后必须复验。
                    if (terminalRequested.get()) throw new IllegalStateException("RedisJobSchedulers is stopping");
                }
            } catch (RuntimeException | Error error) {
                // 任一数据源启动失败就回滚已启动的实例并保持 running=false：留下"部分已启动"却对外报告运行中，
                // 会让健康检查通过，而那些未启动的数据源上的任务永远不会被领取。
                // 必须同时覆盖 Error：子 Scheduler 与惰性创建路径已经把 Error 纳入清理边界，
                // 聚合器漏掉同一失败类型会形成"对外未运行、部分数据源实际在领取任务"的状态。
                for (RedisJobScheduler scheduler : started) {
                    try {
                        scheduler.stop();
                    } catch (RuntimeException | Error ignored) {
                        // 回滚期间的停机失败不覆盖首个启动失败原因
                    }
                }
                stopped = true;
                throw error;
            }
            running = true;
        }
    }

    /**
     * 业务作用：提交管理器停机终态，先封闭全部数据源准入，再并行完成各 source 资源收口。
     *
     * <p>管理器生命周期锁不跨越 Handler 排空或 Redis 往返；单个 source 失败不影响其余 source
     * 继续释放，多 source 失败由管理器建立不改写子结果的当前调用汇总。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：首次共享收口成功时返回；任一并发或重复调用均等待同一完成信号，失败时交还同一资源结果。
     */
    @Override
    public void stop() {
        commitTerminal(false);
        shutdownOnce();
    }

    /**
     * 业务作用：等待全部已建立 source 完成最终所有权资源收口，再通知 Spring 实际托管入口。
     *
     * <p>首次排空期限耗尽时，本方法在管理器共享结果发布后继续异步等待全部 source，避免 Spring
     * 先销毁 Redis 连接而迟到 Handler 仍需续租或提交终态。异步路径的失败没有原调用栈可交还，
     * 因此记录完整证据并在最终资源边界后执行回调。
     *
     * @param callback 全部 source 已尝试收口的完成回调
     * 返回: 最终收口已结束时同步执行回调；仍在收口时建立等待任务后返回。
     */
    @Override
    public void stop(Runnable callback) {
        Objects.requireNonNull(callback, "callback must not be null");
        Throwable failure = null;
        try {
            stop();
        } catch (Throwable stoppingFailure) {
            failure = stoppingFailure;
        }
        if (finalCleanupCompleted.getCount() == 0L) {
            // 管理器才是 Spring 实际托管的 SmartLifecycle Bean；完成路径保留同步失败交还语义。
            RedisJobShutdownSupport.completeCallback(
                    lifecycleFailure(failure, finalCleanupFailure), callback);
            return;
        }
        Throwable stoppingFailure = failure;
        Runnable completion = () -> {
            RedisJobShutdownSupport.await(finalCleanupCompleted);
            try {
                RedisJobShutdownSupport.completeCallback(
                        lifecycleFailure(stoppingFailure, finalCleanupFailure), callback);
            } catch (Throwable lifecycleFailure) {
                // SmartLifecycle 异步完成没有异常返回通道；回调仍已越过全部 source 的最终资源边界。
                log.error("RedisJob manager asynchronous lifecycle completion failed", lifecycleFailure);
            }
        };
        try {
            Thread.ofVirtual().name("redis-job-manager-lifecycle-completion").start(completion);
        } catch (Throwable threadFailure) {
            // 等待任务无法建立时保守地占用当前生命周期线程，不能用提前回调换取表面停机完成。
            RedisJobShutdownSupport.await(finalCleanupCompleted);
            Throwable combined = lifecycleFailure(stoppingFailure, finalCleanupFailure);
            combined = lifecycleFailure(combined, threadFailure);
            RedisJobShutdownSupport.completeCallback(combined, callback);
        }
    }

    /**
     * 业务作用：为单次管理器生命周期回调汇总首次停机与独立最终收口结果，不改写共享异常对象。
     *
     * @param stoppingFailure 首次停机失败，可为 null
     * @param cleanupFailure  全部 source 最终收口失败，可为 null
     * @return 两阶段均成功时返回 null；否则返回原失败或当前调用新建的汇总结果。
     */
    private static Throwable lifecycleFailure(Throwable stoppingFailure, Throwable cleanupFailure) {
        if (stoppingFailure == null) return cleanupFailure;
        if (cleanupFailure == null || cleanupFailure == stoppingFailure) return stoppingFailure;
        return RedisJobShutdownSupport.aggregate(
                "RedisJob manager shutdown and final ownership cleanup failed",
                List.of(stoppingFailure, cleanupFailure));
    }

    /**
     * 业务作用：报告是否处于运行态，供 Spring 生命周期与健康检查判断。
     *
     * <p>参数说明: 无。
     *
     * @return 已启动且未关闭时为 true。
     */
    @Override
    public boolean isRunning() {
        synchronized (lifecycle) {
            return running;
        }
    }

    /**
     * 业务作用：先对全部 source 提交不可逆关门，再并行完成各自的 Handler 排空与资源释放。
     *
     * <p>两阶段顺序保证慢 source 不会延后其它 source 关门；各 source 的长时间等待并行执行，
     * 管理器不持有生命周期锁。某个虚拟线程无法建立时，当前线程仍会执行该 source 收口，
     * 并把线程建立失败与资源失败一并交还。
     *
     * @param snapshot 终态提交时已建立 Scheduler 的快照
     * @return 按快照顺序排列的 source 收口失败。
     */
    private static List<Throwable> shutdownSchedulers(List<RedisJobScheduler> snapshot) {
        // 先把所有数据面转入不可逆关门，任一 source 的慢 Handler 都不能让后续 source 继续领取。
        snapshot.forEach(RedisJobScheduler::prepareStop);
        Throwable[] failures = new Throwable[snapshot.size()];
        CountDownLatch completed = new CountDownLatch(snapshot.size());
        for (int index = 0; index < snapshot.size(); index++) {
            int sourceIndex = index;
            Runnable shutdown = () -> {
                try {
                    snapshot.get(sourceIndex).stop();
                } catch (Throwable failure) {
                    Throwable existing = failures[sourceIndex];
                    failures[sourceIndex] = existing == null ? failure : RedisJobShutdownSupport.aggregate(
                            "RedisJob source shutdown orchestration failed", List.of(existing, failure));
                } finally {
                    completed.countDown();
                }
            };
            try {
                Thread.ofVirtual().name("redis-job-source-shutdown-" + sourceIndex).start(shutdown);
            } catch (Throwable threadFailure) {
                failures[sourceIndex] = threadFailure;
                shutdown.run();
            }
        }
        RedisJobShutdownSupport.await(completed);
        List<Throwable> collected = new ArrayList<>();
        for (Throwable failure : failures) {
            if (failure != null) collected.add(failure);
        }
        return collected;
    }

    /**
     * 业务作用：在短生命周期临界区提交管理器不可逆终态，并按 close 语义撤销 JVM 静态入口。
     *
     * @param closing true 表示永久关闭管理器并撤销静态入口，false 表示提交 Spring 停机终态
     * @return 无返回值；该步骤不等待 Redis、Handler 或子资源。
     */
    private void commitTerminal(boolean closing) {
        // 终态必须先于管理器生命周期锁发布；启动中的 source 即使持锁阻塞 Redis，其它已开放 source 也能立即关门。
        terminalRequested.set(true);
        schedulers.values().forEach(RedisJobScheduler::prepareStop);
        List<RedisJobScheduler> managed;
        synchronized (lifecycle) {
            running = false;
            stopped = true;
            managed = List.copyOf(managedSchedulers);
            if (closing) {
                closed = true;
                // 先关闭静态入口，再等待共享资源结果；旧引用也会由 stopped/closed 门禁拒绝。
                ACTIVE.compareAndSet(this, null);
            }
        }
        // 先在生命周期锁内冻结全部已构造实例，再在锁外提交各 source 关门，避免慢资源阻塞状态读取。
        managed.forEach(RedisJobScheduler::prepareStop);
    }

    /**
     * 业务作用：阻止与管理器终态并发建立的 source 逃离统一停机快照并继续开放准入。
     *
     * @param scheduler 刚放入管理器路由表的 Scheduler
     * @return 管理器仍可接纳 source 时返回；终态已提交时先关闭该 source，再抛出稳定异常。
     */
    private void rejectTerminalCreation(RedisJobScheduler scheduler) {
        if (!terminalRequested.get()) return;
        // source 可能在终态发布与 ConcurrentHashMap 遍历之间完成插入，返回调用方前必须自行补交关门。
        scheduler.prepareStop();
        throw new IllegalStateException("RedisJobSchedulers is stopping");
    }

    /**
     * 业务作用：由唯一执行者收口稳定 source 快照，并向全部并发或重复调用方发布同一资源结果。
     *
     * <p>管理器快照与失败结果在完成信号之后仍保留；公开 source 表即使被 close 清空，也不能让后续
     * 调用把首次未完成的底层资源误报为成功。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：全部 source 收口成功时返回；任一 source 或编排步骤失败时，所有调用方均抛出同一稳定结果。
     */
    private void shutdownOnce() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            RedisJobShutdownSupport.await(shutdownCompleted);
            RedisJobShutdownSupport.rethrow(shutdownFailure);
            return;
        }
        Throwable failure = null;
        try {
            synchronized (lifecycle) {
                // 快照只建立一次并保留到管理器不可达，重复 close 不依赖已经清空的公开 source 表。
                shutdownSnapshot = List.copyOf(managedSchedulers);
            }
            List<Throwable> failures = shutdownSchedulers(shutdownSnapshot);
            // 管理器汇总必须新建本地结果，不能向子 Scheduler 的共享异常追加其它 source 证据。
            failure = RedisJobShutdownSupport.aggregate("RedisJob manager shutdown failed", failures);
        } catch (Throwable orchestrationFailure) {
            failure = orchestrationFailure;
        } finally {
            startFinalCleanupCompletion();
            // 先发布稳定结果再唤醒等待者，callback 和重复 close 都不能读取到默认成功值。
            shutdownFailure = failure;
            shutdownCompleted.countDown();
        }
        RedisJobShutdownSupport.rethrow(failure);
    }

    /**
     * 业务作用：建立管理器唯一的最终收口观察者，把全部 source 的独立 continuation 汇成一个 Spring 依赖边界。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：观察者已经存在、同步完成或成功启动后返回；等待线程不可建立时由当前线程保守等待。
     */
    private void startFinalCleanupCompletion() {
        if (!finalCleanupStarted.compareAndSet(false, true)) return;
        Runnable completion = () -> finishFinalCleanup(null);
        if (shutdownSnapshot.stream().allMatch(RedisJobScheduler::isFinalCleanupComplete)) {
            completion.run();
            return;
        }
        try {
            Thread.ofVirtual().name("redis-job-manager-final-cleanup").start(completion);
        } catch (Throwable threadFailure) {
            // 无观察者时 Spring 回调无法知道何时可安全拆除 Redis；当前线程必须承担同一等待职责。
            finishFinalCleanup(threadFailure);
        }
    }

    /**
     * 业务作用：等待全部 source 的最终资源阶段，并发布独立于首次管理器停机结果的共享结论。
     *
     * @param orchestrationFailure 建立异步观察者时的失败，可为 null
     * @return 无返回值；最终结果先发布，再释放生命周期等待者。
     */
    private void finishFinalCleanup(Throwable orchestrationFailure) {
        List<Throwable> failures = new ArrayList<>();
        if (orchestrationFailure != null) failures.add(orchestrationFailure);
        for (RedisJobScheduler scheduler : shutdownSnapshot) {
            Throwable failure = scheduler.awaitFinalCleanup();
            if (failure != null) failures.add(failure);
        }
        Throwable failure = RedisJobShutdownSupport.aggregate(
                "RedisJob manager final ownership cleanup failed", failures);
        finalCleanupFailure = failure;
        finalCleanupCompleted.countDown();
        if (failure == null) {
            log.info("RedisJob manager final ownership cleanup completed");
        } else {
            log.error("RedisJob manager final ownership cleanup failed", failure);
        }
    }

    /**
     * 业务作用：作为 Spring destroy method 的最终边界，等待全部数据源的所有权资源实际完成收口。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：全部 source 的 final cleanup 结束后清除公开入口；首次停机或最终收口失败时抛出当前调用汇总结果。
     */
    @Override
    public void close() {
        commitTerminal(true);
        Throwable failure = null;
        try {
            shutdownOnce();
        } catch (Throwable stoppingFailure) {
            failure = stoppingFailure;
        } finally {
            // 本 Bean 明确以 close 作为 destroy method；必须先守住 RedisProxy 依赖，再允许 Spring 销毁下层连接。
            RedisJobShutdownSupport.await(finalCleanupCompleted);
            synchronized (lifecycle) {
                // 对外路由可以释放，但 shutdownSnapshot 与 shutdownFailure 必须继续服务重复调用。
                schedulers.clear();
            }
        }
        RedisJobShutdownSupport.rethrow(lifecycleFailure(failure, finalCleanupFailure));
    }
}
