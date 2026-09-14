package io.github.nasaruntime.redis.cache.redis.job;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 业务作用：统一编排 RedisJob 本地派发门禁与服务端执行器状态，防止局部开放和权威不明状态。
 */
final class RedisJobAdmissionController {

    private final RedisJobDispatcher dispatcher;
    private final RedisJobFanoutDispatcher fanoutDispatcher;
    private final RedisJobExecutorRegistry registry;
    private final AtomicBoolean draining;
    private final Object remoteLifecycle = new Object();
    private final Object localAdmission = new Object();
    private final AtomicBoolean terminated = new AtomicBoolean();

    /**
     * 业务作用：建立一个与 Scheduler 生命周期共享 draining 观测的准入控制器。
     *
     * <p>返回：构造一个初始状态由 Scheduler 当前门禁决定的控制器。
     *
     * @param dispatcher       普通任务派发器
     * @param fanoutDispatcher Fanout 派发器
     * @param registry         执行器权威注册表
     * @param draining         Scheduler 共享的排空状态
     */
    RedisJobAdmissionController(RedisJobDispatcher dispatcher,
                                RedisJobFanoutDispatcher fanoutDispatcher,
                                RedisJobExecutorRegistry registry,
                                AtomicBoolean draining) {
        this.dispatcher = dispatcher;
        this.fanoutDispatcher = fanoutDispatcher;
        this.registry = registry;
        this.draining = draining;
    }

    /**
     * 业务作用：在启动或显式重新开放权威前关闭两类本地领取入口。
     *
     * <p>本方法只操作本地门禁，不做 Redis 往返；不可逆停机使用独立的 {@link #terminate()}。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：无返回值；本地门禁关闭异常时在尝试关闭另一入口后抛出。
     */
    void prepare() {
        synchronized (remoteLifecycle) {
            requireMutable();
            Throwable failure;
            synchronized (localAdmission) {
                draining.set(true);
                failure = closeLocal(null);
            }
            requireMutable();
            rethrow(failure);
        }
    }

    /**
     * 业务作用：不等待 Scheduler monitor 或 Redis 往返，永久封闭当前 source 的两类本地领取入口。
     *
     * <p>终态先以原子变量发布，已经进入 Redis 往返的 open/register/start 路径在最终开门前会复验；
     * 本方法返回后不会再有后到的开放动作越过该终态。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：本地入口均已尝试关闭后返回；任一入口关门失败时抛出首个异常。
     */
    void terminate() {
        terminated.set(true);
        Throwable failure;
        synchronized (localAdmission) {
            draining.set(true);
            failure = closeLocal(null);
        }
        rethrow(failure);
    }

    /**
     * 业务作用：先确认服务端 ACTIVE，再依次开放 Fanout 与普通任务本地准入。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：全部步骤成功时开放准入；任一步失败时重新关闭本地入口、
     * 尝试发布 DRAINING 并抛出原异常。
     */
    void open() {
        synchronized (remoteLifecycle) {
            Throwable preparationFailure;
            synchronized (localAdmission) {
                requireMutable();
                draining.set(true);
                preparationFailure = closeLocal(null);
            }
            if (preparationFailure != null) {
                // 本地预关门已失败时不得尝试 ACTIVE；服务端仍要收敛为 DRAINING，避免当前节点继续进入能力快照。
                rethrow(publishDraining(preparationFailure));
                return;
            }
            try {
                requireMutable();
                // Redis 权威确认与本地开门分属两个阶段；停机终态可以在 Redis 阻塞期间独立关闭本地入口。
                registry.activate();
                synchronized (localAdmission) {
                    // ACTIVE 回包只能证明服务端动作，最终开放前仍要复验不可逆终态。
                    requireMutable();
                    fanoutDispatcher.activate();
                    dispatcher.activate();
                    requireMutable();
                    draining.set(false);
                }
            } catch (RuntimeException | Error failure) {
                Throwable rollbackFailure;
                synchronized (localAdmission) {
                    draining.set(true);
                    rollbackFailure = closeLocal(null);
                }
                // ACTIVE 回包不确定或停机已经提交时必须转入 DRAINING，后续心跳不能自行重开业务路由。
                rollbackFailure = publishDraining(rollbackFailure);
                if (rollbackFailure != null) failure.addSuppressed(rollbackFailure);
                throw failure;
            }
        }
    }

    /**
     * 业务作用：立即关闭两类本地准入，再尝试把服务端成员发布为 DRAINING。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：无返回值；任一关闭步骤失败时仍尝试其余步骤，最后抛出首个异常。
     */
    void close() {
        synchronized (remoteLifecycle) {
            Throwable failure;
            synchronized (localAdmission) {
                draining.set(true);
                failure = closeLocal(null);
            }
            failure = publishDraining(failure);
            rethrow(failure);
        }
    }

    /**
     * 业务作用：拒绝在不可逆停机终态之后继续执行 ACTIVE 发布或本地开门。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：准入仍可迁移时正常返回；永久关闭后抛出稳定异常。
     */
    private void requireMutable() {
        if (terminated.get()) throw new IllegalStateException("RedisJob admission is permanently closed");
    }

    /**
     * 业务作用：尝试把服务端成员发布为 DRAINING，并把失败合并到既有的关门原因。
     *
     * @param failure 已有失败，可为 null
     * @return 合并后的失败，服务端发布成功且没有既有失败时为 null。
     */
    private Throwable publishDraining(Throwable failure) {
        try {
            // 先封闭本地入口再发布服务端状态，即使 Redis 不可达也不再领取新任务。
            registry.drain();
        } catch (RuntimeException | Error error) {
            failure = append(failure, error);
        }
        return failure;
    }

    /**
     * 业务作用：尽力关闭普通与 Fanout 本地入口，保留最早失败原因。
     *
     * @param failure 已有失败，可为 null
     * @return 合并后的失败，全部成功时为 null。
     */
    private Throwable closeLocal(Throwable failure) {
        try {
            dispatcher.drain();
        } catch (RuntimeException | Error error) {
            failure = append(failure, error);
        }
        try {
            fanoutDispatcher.drain();
        } catch (RuntimeException | Error error) {
            failure = append(failure, error);
        }
        return failure;
    }

    /**
     * 业务作用：把后续关门失败附加到首个原因，保留完整的安全收敛证据。
     *
     * @param current 已有失败，可为 null
     * @param next    后续失败
     * @return 首个失败及其 suppressed 链。
     */
    private static Throwable append(Throwable current, Throwable next) {
        if (current == null) return next;
        current.addSuppressed(next);
        return current;
    }

    /**
     * 业务作用：按原类型重新抛出准入收敛期捕获的非受检异常。
     *
     * @param failure 待抛出失败，可为 null
     *                返回：failure 为 null 时正常返回，否则抛出并不会返回。
     */
    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }
}
