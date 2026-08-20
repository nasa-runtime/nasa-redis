package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
public final class RedisJobSchedulers implements SmartLifecycle, AutoCloseable {

    private static final AtomicReference<RedisJobSchedulers> ACTIVE = new AtomicReference<>();
    private static final String ROOT_PREFIX = "nasa.redis.job";
    private static final String SOURCE_PREFIX = ROOT_PREFIX + ".sources.";
    private static final String BEAN_SUFFIX = "RedisProxy";

    private final Environment environment;
    private final RedisJobProperties root;
    private final Map<String, RedisJobScheduler> schedulers = new ConcurrentHashMap<>();
    /* 生命周期状态锁：ConcurrentHashMap 只保证单次 map 操作安全，无法让"建立-启动-关闭"三种迁移互斥。 */
    private final Object lifecycle = new Object();
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
            RedisJobScheduler existing = schedulers.get(sourceId);
            if (existing != null) return existing;
            created = create(sourceId);
            schedulers.put(sourceId, created);
            startNow = running;
        }
        if (!startNow) return created;
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
     * 返回：无返回值。
     */
    @Override
    public void start() {
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("RedisJobSchedulers is closed");
            if (stopped) throw new IllegalStateException("RedisJobSchedulers cannot restart after stop");
            List<RedisJobScheduler> started = new ArrayList<>();
            try {
                for (RedisJobScheduler scheduler : schedulers.values()) {
                    // 先登记再启动: 启动中途抛错的那一台自身可能已经起了一部分资源, 必须进入回滚名单
                    started.add(scheduler);
                    scheduler.start();
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
     * 业务作用：停止全部数据源；单个数据源停止失败不影响其余数据源继续停止。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    @Override
    public void stop() {
        synchronized (lifecycle) {
            running = false;
            stopped = true;
            Throwable failure = null;
            for (RedisJobScheduler scheduler : new LinkedHashMap<>(schedulers).values()) {
                try {
                    scheduler.stop();
                } catch (RuntimeException | Error error) {
                    // 单个数据源停止失败不得阻断其余数据源释放, Error 同样如此
                    if (failure == null) failure = error;
                }
            }
            if (failure != null) rethrow(failure);
        }
    }

    /**
     * 业务作用：在全部数据源都已尝试释放后，把首个失败原样抛出。
     *
     * <p>返回：无返回值；总是抛出传入的失败。
     *
     * @param failure 首个失败
     */
    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        throw new IllegalStateException(failure);
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
     * 业务作用：释放全部数据源的连接、线程与订阅，关闭后拒绝再建立新的数据源。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    @Override
    public void close() {
        synchronized (lifecycle) {
            closed = true;
            stopped = true;
            running = false;
            // 先关闭静态入口，再释放子运行时；并发调用即使已经读到旧引用，也会在生命周期锁内看到 closed。
            ACTIVE.compareAndSet(this, null);
            Throwable failure = null;
            for (RedisJobScheduler scheduler : new LinkedHashMap<>(schedulers).values()) {
                try {
                    scheduler.close();
                } catch (RuntimeException | Error error) {
                    // 单个数据源释放失败不得阻断其余数据源释放, Error 同样如此
                    if (failure == null) failure = error;
                }
            }
            schedulers.clear();
            if (failure != null) rethrow(failure);
        }
    }
}
