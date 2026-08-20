package io.github.nasaruntime.redis.cache.redis.job;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 业务作用：声明由 RedisJob 框架登记、调度或定向执行的方法。
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RedisJob {

    /**
     * 业务作用：定义命名空间内唯一的任务名或 Worker 能力名。
     *
     * @return 任务名。
     */
    String name();

    /**
     * 业务作用：指定本任务登记到哪个 Redis 数据源，决定它的调度器、注册表、线程池与 Fanout 归属。
     *
     * <p>任务的本地唯一身份是 {@code (qualifier, namespace, name)}，不同数据源可以声明同名任务且互不影响。
     * 填写的名字不存在对应 {@code RedisProxy} 时应用启动失败，不会静默回退到默认数据源——回退会让任务
     * 写进错误的 Redis，产生一份无人调度或被重复调度的定义。
     *
     * <p>必须显式填写且不能为空。框架不提供默认 source，避免遗漏配置时把任务静默登记到 primary。
     *
     * @return 任务所属的语言无关 source id。
     */
    String qualifier();

    /**
     * 业务作用：允许多个任务共享同一个普通派发能力池；留空时等于 {@link #name()}。
     *
     * @return Worker 能力名。
     */
    String worker() default "";

    /**
     * 业务作用：定义任务的触发入口类型，定向 Worker 必须使用 {@code FANOUT_ONLY}。
     *
     * @return 触发类型。
     */
    RedisJobTrigger trigger() default RedisJobTrigger.SCHEDULED;

    /**
     * 业务作用：定义六段式 Spring Cron 表达式；非 Cron 任务留空。
     *
     * @return Cron 表达式。
     */
    String cron() default "";

    /**
     * 业务作用：定义 Cron 计算时区。
     *
     * @return IANA 时区标识。
     */
    String zone() default "UTC";

    /**
     * 业务作用：定义固定频率的逻辑触发间隔；与 Cron、fixed delay 互斥。
     *
     * @return 毫秒间隔，零表示未配置。
     */
    long fixedRateMs() default 0L;

    /**
     * 业务作用：定义以前一次终态为基准的固定延迟；与 Cron、fixed rate 互斥。
     *
     * @return 毫秒间隔，零表示未配置。
     */
    long fixedDelayMs() default 0L;

    /**
     * 业务作用：定义同一任务身份的多个 Run 之间如何竞争集群执行权，默认排队且不并行启动 Handler。
     *
     * @return 并发策略，默认 {@link RedisJobConcurrency#SERIAL_QUEUE}。
     */
    RedisJobConcurrency concurrency() default RedisJobConcurrency.SERIAL_QUEUE;

    /**
     * 业务作用：定义错过逻辑调度时刻后的补偿方式。
     *
     * @return 误触发策略。
     */
    RedisJobMisfire misfire() default RedisJobMisfire.FIRE_ONCE_NOW;

    /**
     * 业务作用：定义单次 Handler attempt 请求协作式取消的时间阈值，不强制中断业务线程。
     *
     * @return 超时毫秒数，实际阈值还受全局 max-run-duration-ms 约束。
     */
    long timeoutMs() default 120_000L;

    /**
     * 业务作用：限制一个 Run 可启动的累计 attempt 数。
     *
     * @return 最大 attempt 数。
     */
    int maxAttempts() default 3;

    /**
     * 业务作用：定义首次重试的退避时长，后续按指数增长并受全局上限约束。
     *
     * @return 首次退避毫秒数。
     */
    long retryDelayMs() default 10_000L;

    /**
     * 业务作用：定义跨语言任务契约的单调修订号。
     *
     * @return 契约修订号。
     */
    long contractRevision() default 1L;

    /**
     * 业务作用：定义跨语言参数的逻辑 Schema 标识。
     *
     * @return Schema 标识。
     */
    String schema() default "";

    /**
     * 业务作用：定义本 Worker 接受的线协议编码。
     *
     * @return 可接受的编码集合。
     */
    RedisJobWireCodec[] codecs() default {RedisJobWireCodec.JSON};

    /**
     * 业务作用：定义一次 Fanout 通知等待持久接收确认的时间。
     *
     * @return 接收超时毫秒数。
     */
    long fanoutReceiptTimeoutMs() default 2_000L;

    /**
     * 业务作用：定义首次 Fanout 通知之后允许再次发送的次数。
     *
     * @return 通知重发次数。
     */
    int fanoutReceiptMaxRetries() default 3;

    /**
     * 业务作用：定义 Fanout 分片在节点不可用时的收敛策略。
     *
     * @return Fanout 失败策略。
     */
    RedisJobFanoutFailurePolicy fanoutFailurePolicy() default RedisJobFanoutFailurePolicy.REASSIGN_ON_FAILURE;
}
