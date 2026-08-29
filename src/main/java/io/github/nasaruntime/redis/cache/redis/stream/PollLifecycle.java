package io.github.nasaruntime.redis.cache.redis.stream;

import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

/**
 * 可选的 poll task 生命周期钩子, 给独占消费 / 分区消费等场景在 pollTask 关键点上插自定义逻辑。
 * <p>
 * <b>线程模型(分两种)</b>:
 * <ul>
 *   <li><b>Standalone doLoop (非 group / 独立线程)</b>: hook 与 {@code listener.onMessage}
 *       跑在同一线程。可以做需要"线程级 owner 校验"的本地操作。</li>
 *   <li><b>ManagedRunner batch (group / 分区消费)</b>:
 *     <ul>
 *       <li>{@link #beforeStart()} / {@link #beforePoll()} / {@link #afterExit(boolean)}
 *           以及 drain-aware 钩子({@link #isStopRequested()} / {@link #isLockLost()} /
 *           {@link #drainTimedOut(long)} / {@link #checkAliveHoldsOnly()})跑在 runner 线程。</li>
 *       <li>{@code listener.onMessage} / ACK / XDEL 跑在 businessExecutor 线程。</li>
 *       <li><b>跨线程 owner 操作</b>(例如 ACK fencing):runner 线程内 tryLock 后必须显式记录
 *           holder 字符串,businessExecutor 用 {@link io.github.nasaruntime.redis.cache.redis.LettuceDistributedLock#holds(String, String)}
 *           做校验 — 不能用线程隐式 holder 的 {@code holds(key)}。</li>
 *     </ul>
 *   </li>
 * </ul>
 * <p>
 * <b>典型用法 (分区消费)</b>:
 * <ul>
 *   <li>{@link #beforeStart()}: 在 runner 线程内 tryLock 该分区的锁 + 记录 holder + 异步 XAUTOCLAIM 接管前任 pending</li>
 *   <li>{@link #beforePoll()}: 每 N 批 holds() 自检, 防长 GC 后锁丢失自己还在消费</li>
 *   <li>{@link #afterExit(boolean)}: 释放锁 + 从 claims 中移除自己 + 清 lockHolder</li>
 *   <li>{@link #isStopRequested()} / {@link #isLockLost()}: 主循环 drain 分流读字段</li>
 *   <li>{@link #drainTimedOut(long)}: drain 超时强制 exit</li>
 *   <li>{@link #checkAliveHoldsOnly()}: drain 期间 holds 自检, 不读 active/running</li>
 * </ul>
 * <p>
 * 默认实现 {@link #NOOP} 全部钩子是 nop, 现有不需要钩子的调用方仍走原 register(req, listener)
 * 路径, 完全向上兼容。
 *
 * @see BatchStreamMessageListenerContainer#register(StreamMessageListenerContainer.StreamReadRequest, StreamListener, PollLifecycle)
 * @see BatchStreamPollTask
 */
public interface PollLifecycle {

    /** 默认空实现，不参与任何钩子逻辑，供不需要生命周期控制的订阅使用。 */
    PollLifecycle NOOP = new PollLifecycle() {};

    /**
     * 业务作用：进入消费之前的初始化 hook (tryLock / 接管 pending 等)。
     * <p>
     * 调用时机分场景:
     * <ul>
     *   <li><b>Standalone doLoop (非 group)</b>: {@code pollState.running()} 之后, {@code doLoop()} 之前调一次.
     *       返回 false → task 直接退出, doLoop 不调用, {@link #afterExit(boolean)} 仍调 (传 normal=false).</li>
     *   <li><b>ManagedRunner batch (group)</b>: 由 {@code initManaged()} 在 runner 线程调用.
     *       返回 false → task 留在 pending 队列作为 zombie, runner 每 RETRY_INTERVAL_MS 重试 initManaged → 再次调 beforeStart.
     *       <b>不会立即调用 afterExit</b>; 只有当 task 真正被 removeAndExit (markStop / 主循环判 isStopRequested 等) 时才调 afterExit.</li>
     * </ul>
     *
     * @return true → 进入正常消费 (或 ManagedRunner 模式下标记 managedSuccess=true);
     *         false → standalone 模式直接退出 + afterExit; ManagedRunner 模式作 zombie 等待 retry, 不立即 afterExit.
     */
    default boolean beforeStart() {
        return true;
    }

    /**
     * 业务作用：消费一次外部发出的立即重试信号，让托管任务在资源释放通知到达后跳过固定退避。
     * <p>
     * 该信号只改变下一次初始化尝试的时机，不代表资源已经可用；初始化仍必须执行原有的
     * 分布式锁或权威校验。默认不支持该能力，普通 Stream 订阅保持固定退避行为。
     *
     * <p>参数说明: 无。
     *
     * @return 存在尚未消费的立即重试信号时返回 true，并原子清除该信号。
     */
    default boolean consumeImmediateRetry() {
        return false;
    }

    /**
     * 业务作用：doLoop 内每次拉取消息 (readFunction.apply) 之前调用。适合做"我是否还应该继续消费"的自检。
     * <p>
     * 调用频率: 每个 poll 周期一次 (BLOCK 拉取前), 自检逻辑应该足够轻 — 重的检测请按批次计数自行降频。
     *
     * @return true → 继续 readFunction.apply 拉取; false → cancel subscription, doLoop 当轮退出
     */
    default boolean beforePoll() {
        return true;
    }

    /**
     * 业务作用：task 是否已就绪可以 pollOnce. 默认 true, 给"启动后还在做异步初始化 (如 XAUTOCLAIM 接管 pending)"
     * 的场景用 — false 期间 ManagedRunner 不实际拉消息, 但仍调 {@link BatchStreamPollTask#checkAlive()}
     * 维持 holds 自检和 markStop 响应。
     * <p>
     * 与 {@link #beforePoll()} 区别: beforePoll 返回 false 表示"不再拉, 退出"; isPollReady 返回 false
     * 表示"暂不拉, 但 task 仍存活, 下次再问"。两者语义不可互替。
     */
    default boolean isPollReady() {
        return true;
    }

    /**
     * 业务作用：task 退出时调用, 恰好一次。适合做清理 (unlock / 从注册表中移除等)。
     * <p>
     * 调用时机分场景:
     * <ul>
     *   <li><b>Standalone doLoop (非 group)</b>: 不论退出路径如何 — 正常 cancel / beforeStart 返回 false /
     *       未捕获异常 — 都会被调用 (BatchStreamPollTask.run 内 finally 兜底).</li>
     *   <li><b>ManagedRunner batch (group)</b>: 仅当 task 真正被 ManagedRunner removeAndExit (markStop /
     *       lockLost / drain 完成 / 主循环判 isStopRequested 等) 或 finally drainAndExitOne 时调用.
     *       <b>beforeStart 返回 false (zombie retry) 不会立即调 afterExit</b>; task 留在 pending,
     *       runner 每 RETRY_INTERVAL_MS 重试 initManaged → 再次调 beforeStart.</li>
     * </ul>
     * 钩子内部抛出的异常会被框架 catch 并交给 errorHandler 处理, 不会泄漏。
     * <p>
     * <b>实施警示</b>: ManagedRunner 模式下, 如果 beforeStart 已成功获取资源 (例如 tryLock 拿锁), 不能再返回 false —
     * 否则资源不会通过 afterExit 释放, 下一轮 retry 会重入覆盖资源造成泄漏. 见 {@link io.github.nasaruntime.redis.cache.redis.RedisPartition.Claim}
     * 内 submitRecoverPending 的本地 catch 设计.
     *
     * @param normal true → doLoop 完整跑过 (cancel 或 stop 触发的退出);
     *               false → standalone 模式 beforeStart 返回 false 或 run() 抛出异常; ManagedRunner 模式 removeAndExit / finally 强制 exit
     */
    default void afterExit(boolean normal) {
    }

    /**
     * 业务作用：是否被主动 stop (rebalance / shutdown 已调 markStop). 给 ManagedRunner drain-aware 分流用 —
     * stop requested 时 runner 不直接 exitManaged, 而是等待 in-flight listener / recoverPending 结束.
     * <p>
     * <b>命名注意</b>: 不要与 {@link BatchStreamPollTask#isActive()} 混淆 — 后者表示 subscription/task
     * 是否仍在 RUNNING 状态, 与"是否被主动 stop"无关.
     *
     * @return true → markStop 已触发, 进入 drain 阶段; false → 未 stop, 正常运行
     */
    default boolean isStopRequested() {
        return false;
    }

    /**
     * 业务作用：锁是否真实丢失 (watchdog 失败 / 锁过期被新 owner 抢走). 与 {@link #isStopRequested()} 互补 —
     * stop 是主动停止可 drain，lockLost 是锁丢必须立即 exit；后续 ACK 由 fencing 拒绝。
     *
     * @return true → 锁已不属于自己, 立即 exitManaged, 不等 drain
     */
    default boolean isLockLost() {
        return false;
    }

    /**
     * 业务作用：drain 是否已超过 {@code drainTimeoutMs}. 主动 stop 后等待 in-flight 完成的最大时长封顶,
     * 超过则强制 exit 并释放本节点 partition lock，迟到的 ACK/XDEL 由 fencing 拒绝。
     * <p>
     * 调用时机: 仅在 {@link #isStopRequested()} == true 的 drain 等待分支内询问.
     *
     * @param now 当前毫秒时间戳 (调用方传入, 避免在 hook 内重复 currentTimeMillis)
     * @return true → 已超 drainTimeoutMs, 强制 exit; false → 仍在 drain 窗口内, 继续等待
     */
    default boolean drainTimedOut(long now) {
        return false;
    }

    /**
     * 业务作用：drain 等待期间的 holds 自检. 与 {@link #beforePoll()} 区别: <b>不读 active / running</b>,
     * 只校验 Redis 端锁是否仍属于自己. 主动 stop 状态由 {@link #isStopRequested()} 单独驱动,
     * 不能让 active=false / running=false 在 drain 期间立即返回 false 绕过 drain.
     *
     * @return true → 锁仍属于自己, drain 继续; false → 锁已真丢, 立即 exit (与 lockLost 路径合并处理)
     */
    default boolean checkAliveHoldsOnly() {
        return true;
    }
}
