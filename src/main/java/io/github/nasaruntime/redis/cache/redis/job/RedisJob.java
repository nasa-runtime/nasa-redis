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
     * 业务作用：定义同名任务多个运行实例之间的并发关系。
     *
     * @return 并发策略。
     */
    RedisJobConcurrency concurrency() default RedisJobConcurrency.SERIAL_QUEUE;

    /**
     * 业务作用：定义错过逻辑调度时刻后的补偿方式。
     *
     * @return 误触发策略。
     */
    RedisJobMisfire misfire() default RedisJobMisfire.FIRE_ONCE_NOW;

    /**
     * 业务作用：限制单次 Handler attempt 的运行时间。
     *
     * @return 超时毫秒数。
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
