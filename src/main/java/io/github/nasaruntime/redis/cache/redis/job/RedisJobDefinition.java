package io.github.nasaruntime.redis.cache.redis.job;

import org.springframework.scheduling.support.CronExpression;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 业务作用：保存一个任务的稳定调度定义、执行策略和跨语言契约。
 */
public final class RedisJobDefinition {

    private final String name;
    private final String workerName;
    private final RedisJobTrigger trigger;
    private final RedisJobScheduleType scheduleType;
    private final String cron;
    private final ZoneId zone;
    private final long intervalMs;
    private final RedisJobConcurrency concurrency;
    private final RedisJobMisfire misfire;
    private final long timeoutMs;
    private final int maxAttempts;
    private final long retryDelayMs;
    private final long contractRevision;
    private final String schemaId;
    private final Set<RedisJobWireCodec> codecs;
    private final long fanoutReceiptTimeoutMs;
    private final int fanoutReceiptMaxRetries;
    private final RedisJobFanoutFailurePolicy fanoutFailurePolicy;
    private final long definitionRevision;
    private final String definitionDigest;
    private final String workerKey;
    private final String wireCodecs;

    /**
     * 业务作用：从构建器生成经过完整互斥与边界校验的不可变任务定义。
     *
     * @param builder 已填充的定义构建器
     */
    private RedisJobDefinition(Builder builder) {
        this.name = RedisJobNames.requireName(builder.name, "name");
        this.trigger = Objects.requireNonNull(builder.trigger, "trigger must not be null");
        this.workerName = RedisJobNames.requireName(
                this.trigger == RedisJobTrigger.FANOUT_ONLY || builder.workerName == null || builder.workerName.isBlank()
                        ? this.name : builder.workerName,
                "workerName");
        this.scheduleType = resolveScheduleType(builder);
        this.cron = builder.cron == null ? "" : builder.cron.trim();
        this.zone = Objects.requireNonNull(builder.zone, "zone must not be null");
        this.intervalMs = resolveInterval(builder, this.scheduleType);
        this.concurrency = Objects.requireNonNull(builder.concurrency, "concurrency must not be null");
        this.misfire = Objects.requireNonNull(builder.misfire, "misfire must not be null");
        this.timeoutMs = positive(builder.timeoutMs, "timeoutMs");
        this.maxAttempts = positive(builder.maxAttempts, "maxAttempts");
        this.retryDelayMs = positive(builder.retryDelayMs, "retryDelayMs");
        this.contractRevision = positive(builder.contractRevision, "contractRevision");
        this.schemaId = builder.schemaId == null || builder.schemaId.isBlank() ? this.workerName : builder.schemaId.trim();
        if (builder.codecs.isEmpty()) throw new IllegalArgumentException("codecs must not be empty");
        this.codecs = Collections.unmodifiableSet(EnumSet.copyOf(builder.codecs));
        this.fanoutReceiptTimeoutMs = positive(builder.fanoutReceiptTimeoutMs, "fanoutReceiptTimeoutMs");
        if (builder.fanoutReceiptMaxRetries < 0) {
            throw new IllegalArgumentException("fanoutReceiptMaxRetries must not be negative");
        }
        this.fanoutReceiptMaxRetries = builder.fanoutReceiptMaxRetries;
        this.fanoutFailurePolicy = Objects.requireNonNull(builder.fanoutFailurePolicy,
                "fanoutFailurePolicy must not be null");
        this.definitionRevision = positive(builder.definitionRevision, "definitionRevision");
        this.workerKey = RedisJobIdentifiers.workerKey(this.workerName);
        this.wireCodecs = this.codecs.stream().map(Enum::name).sorted().collect(Collectors.joining(","));
        this.definitionDigest = RedisJobIdentifiers.definitionDigest(this);
    }

    /**
     * 业务作用：创建空白定义构建器，供编程式注册逐项声明任务合同。
     *
     * @return 定义构建器。
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 业务作用：把注解声明转换为与编程式注册相同的任务定义。
     *
     * @param annotation 方法上的任务注解
     * @return 不可变任务定义。
     */
    public static RedisJobDefinition from(RedisJob annotation) {
        Objects.requireNonNull(annotation, "annotation must not be null");
        Builder builder = builder()
                .name(annotation.name())
                .worker(annotation.worker())
                .trigger(annotation.trigger())
                .cron(annotation.cron(), ZoneId.of(annotation.zone()))
                .concurrency(annotation.concurrency())
                .misfire(annotation.misfire())
                .timeout(Duration.ofMillis(annotation.timeoutMs()))
                .maxAttempts(annotation.maxAttempts())
                .retryDelay(Duration.ofMillis(annotation.retryDelayMs()))
                .contract(annotation.contractRevision(), annotation.schema(), annotation.codecs())
                .fanoutReceipt(Duration.ofMillis(annotation.fanoutReceiptTimeoutMs()),
                        annotation.fanoutReceiptMaxRetries())
                .fanoutFailurePolicy(annotation.fanoutFailurePolicy());
        if (annotation.fixedRateMs() > 0) builder.fixedRate(Duration.ofMillis(annotation.fixedRateMs()));
        if (annotation.fixedDelayMs() > 0) builder.fixedDelay(Duration.ofMillis(annotation.fixedDelayMs()));
        return builder.build();
    }

    /**
     * 业务作用：从一个逻辑时刻计算严格晚于它的下一调度时刻，避免延迟造成周期漂移。
     *
     * @param logicalFireAt 上一个逻辑时刻或初始基准
     * @return 下一逻辑时刻；手工及 Fanout-only 任务返回零。
     */
    public long nextFireAt(long logicalFireAt) {
        return switch (scheduleType) {
            case CRON -> {
                ZonedDateTime base = Instant.ofEpochMilli(logicalFireAt).atZone(zone);
                ZonedDateTime next = CronExpression.parse(cron).next(base);
                if (next == null) throw new IllegalStateException("cron has no next fire time: " + name);
                yield next.toInstant().toEpochMilli();
            }
            case FIXED_RATE, FIXED_DELAY -> Math.addExact(logicalFireAt, intervalMs);
            case MANUAL, FANOUT_ONLY -> 0L;
        };
    }

    /**
     * 业务作用：读取任务名。 @return 任务名。
     */
    public String name() {
        return name;
    }

    /**
     * 业务作用：读取派发能力名。 @return Worker 能力名。
     */
    public String workerName() {
        return workerName;
    }

    /**
     * 业务作用：读取触发入口类型。 @return 触发类型。
     */
    public RedisJobTrigger trigger() {
        return trigger;
    }

    /**
     * 业务作用：读取调度类型。 @return 调度类型。
     */
    public RedisJobScheduleType scheduleType() {
        return scheduleType;
    }

    /**
     * 业务作用：读取 Cron 表达式。 @return Cron 表达式或空串。
     */
    public String cron() {
        return cron;
    }

    /**
     * 业务作用：读取 Cron 时区。 @return 时区。
     */
    public ZoneId zone() {
        return zone;
    }

    /**
     * 业务作用：读取固定周期毫秒数。 @return 非固定周期任务为零。
     */
    public long intervalMs() {
        return intervalMs;
    }

    /**
     * 业务作用：读取并发策略。 @return 并发策略。
     */
    public RedisJobConcurrency concurrency() {
        return concurrency;
    }

    /**
     * 业务作用：读取误触发策略。 @return 误触发策略。
     */
    public RedisJobMisfire misfire() {
        return misfire;
    }

    /**
     * 业务作用：读取 Handler 超时。 @return 超时毫秒数。
     */
    public long timeoutMs() {
        return timeoutMs;
    }

    /**
     * 业务作用：读取最大执行次数。 @return 最大 attempt 数。
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * 业务作用：读取首次失败退避。 @return 退避毫秒数。
     */
    public long retryDelayMs() {
        return retryDelayMs;
    }

    /**
     * 业务作用：读取契约修订号。 @return 契约修订号。
     */
    public long contractRevision() {
        return contractRevision;
    }

    /**
     * 业务作用：读取参数 Schema。 @return Schema 标识。
     */
    public String schemaId() {
        return schemaId;
    }

    /**
     * 业务作用：读取允许的线编码集合。 @return 不可变编码集合。
     */
    public Set<RedisJobWireCodec> codecs() {
        return codecs;
    }

    /**
     * 业务作用：读取接收回执超时。 @return 超时毫秒数。
     */
    public long fanoutReceiptTimeoutMs() {
        return fanoutReceiptTimeoutMs;
    }

    /**
     * 业务作用：读取通知重发次数。 @return 重发次数。
     */
    public int fanoutReceiptMaxRetries() {
        return fanoutReceiptMaxRetries;
    }

    /**
     * 业务作用：读取 Fanout 失败策略。 @return 失败策略。
     */
    public RedisJobFanoutFailurePolicy fanoutFailurePolicy() {
        return fanoutFailurePolicy;
    }

    /**
     * 业务作用：读取定义修订号。 @return 单调修订号。
     */
    public long definitionRevision() {
        return definitionRevision;
    }

    /**
     * 业务作用：读取规范定义摘要。 @return SHA-256 小写十六进制摘要。
     */
    public String definitionDigest() {
        return definitionDigest;
    }

    /**
     * 业务作用：读取构建定义时生成的 Worker 路由摘要。 @return SHA-256 小写十六进制摘要。
     */
    String workerKey() {
        return workerKey;
    }

    /**
     * 业务作用：读取构建定义时规范化的跨语言编码列表。 @return 逗号分隔的编码名。
     */
    String wireCodecs() {
        return wireCodecs;
    }

    /**
     * 业务作用：从构建器输入确定唯一调度类型并拒绝互斥配置。
     *
     * @param builder 定义构建器
     * @return 调度类型。
     */
    private static RedisJobScheduleType resolveScheduleType(Builder builder) {
        if (builder.trigger == RedisJobTrigger.FANOUT_ONLY) {
            if ((builder.cron != null && !builder.cron.isBlank()) || builder.fixedRateMs > 0 || builder.fixedDelayMs > 0) {
                throw new IllegalArgumentException("FANOUT_ONLY worker cannot declare a schedule");
            }
            return RedisJobScheduleType.FANOUT_ONLY;
        }
        int configured = (builder.cron != null && !builder.cron.isBlank() ? 1 : 0)
                + (builder.fixedRateMs > 0 ? 1 : 0) + (builder.fixedDelayMs > 0 ? 1 : 0);
        if (configured > 1) throw new IllegalArgumentException("cron, fixedRate and fixedDelay are mutually exclusive");
        if (configured == 0) return RedisJobScheduleType.MANUAL;
        if (builder.cron != null && !builder.cron.isBlank()) {
            CronExpression.parse(builder.cron.trim());
            return RedisJobScheduleType.CRON;
        }
        return builder.fixedRateMs > 0 ? RedisJobScheduleType.FIXED_RATE : RedisJobScheduleType.FIXED_DELAY;
    }

    /**
     * 业务作用：提取固定周期并保证配置与调度类型一致。
     *
     * @param builder 定义构建器
     * @param type    已确定的调度类型
     * @return 固定间隔或零。
     */
    private static long resolveInterval(Builder builder, RedisJobScheduleType type) {
        return switch (type) {
            case FIXED_RATE -> builder.fixedRateMs;
            case FIXED_DELAY -> builder.fixedDelayMs;
            default -> 0L;
        };
    }

    /**
     * 业务作用：校验需要严格为正的协议数值。
     *
     * @param value 配置值
     * @param name  配置名
     * @return 原配置值。
     */
    private static long positive(long value, String name) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be greater than zero");
        return value;
    }

    /**
     * 业务作用：校验需要严格为正的整型协议数值。
     *
     * @param value 配置值
     * @param name  配置名
     * @return 原配置值。
     */
    private static int positive(int value, String name) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be greater than zero");
        return value;
    }

    /**
     * 业务作用：以流式接口收集任务定义，在 {@link #build()} 时统一校验。
     */
    public static final class Builder {
        private String name;
        private String workerName;
        private RedisJobTrigger trigger = RedisJobTrigger.SCHEDULED;
        private String cron = "";
        private ZoneId zone = ZoneId.of("UTC");
        private long fixedRateMs;
        private long fixedDelayMs;
        private RedisJobConcurrency concurrency = RedisJobConcurrency.SERIAL_QUEUE;
        private RedisJobMisfire misfire = RedisJobMisfire.FIRE_ONCE_NOW;
        private long timeoutMs = 120_000L;
        private int maxAttempts = 3;
        private long retryDelayMs = 10_000L;
        private long contractRevision = 1L;
        private String schemaId = "";
        private final EnumSet<RedisJobWireCodec> codecs = EnumSet.of(RedisJobWireCodec.JSON);
        private long fanoutReceiptTimeoutMs = 2_000L;
        private int fanoutReceiptMaxRetries = 3;
        private RedisJobFanoutFailurePolicy fanoutFailurePolicy = RedisJobFanoutFailurePolicy.REASSIGN_ON_FAILURE;
        private long definitionRevision = 1L;

        /**
         * 业务作用：设置任务名。 @param name 任务名 @return 当前构建器。
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * 业务作用：设置共享 Worker 名。 @param workerName Worker 名 @return 当前构建器。
         */
        public Builder worker(String workerName) {
            this.workerName = workerName;
            return this;
        }

        /**
         * 业务作用：设置触发入口。 @param trigger 触发类型 @return 当前构建器。
         */
        public Builder trigger(RedisJobTrigger trigger) {
            this.trigger = trigger;
            return this;
        }

        /**
         * 业务作用：设置 Cron 调度与计算时区。
         *
         * @param expression Spring 六段式 Cron
         * @param zone       时区
         * @return 当前构建器。
         */
        public Builder cron(String expression, ZoneId zone) {
            this.cron = expression;
            this.zone = zone;
            return this;
        }

        /**
         * 业务作用：设置固定频率调度。 @param interval 周期 @return 当前构建器。
         */
        public Builder fixedRate(Duration interval) {
            this.fixedRateMs = interval.toMillis();
            return this;
        }

        /**
         * 业务作用：设置固定延迟调度。 @param interval 延迟 @return 当前构建器。
         */
        public Builder fixedDelay(Duration interval) {
            this.fixedDelayMs = interval.toMillis();
            return this;
        }

        /**
         * 业务作用：设置并发策略。 @param concurrency 并发策略 @return 当前构建器。
         */
        public Builder concurrency(RedisJobConcurrency concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        /**
         * 业务作用：设置误触发策略。 @param misfire 误触发策略 @return 当前构建器。
         */
        public Builder misfire(RedisJobMisfire misfire) {
            this.misfire = misfire;
            return this;
        }

        /**
         * 业务作用：设置 Handler 超时。 @param timeout 超时 @return 当前构建器。
         */
        public Builder timeout(Duration timeout) {
            this.timeoutMs = timeout.toMillis();
            return this;
        }

        /**
         * 业务作用：设置最大 attempt 数。 @param maxAttempts 最大次数 @return 当前构建器。
         */
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        /**
         * 业务作用：设置首次重试退避。 @param delay 退避时长 @return 当前构建器。
         */
        public Builder retryDelay(Duration delay) {
            this.retryDelayMs = delay.toMillis();
            return this;
        }

        /**
         * 业务作用：设置跨语言契约和允许的线编码。
         *
         * @param revision 契约修订号
         * @param schemaId Schema 标识
         * @param codecs   允许的编码
         * @return 当前构建器。
         */
        public Builder contract(long revision, String schemaId, RedisJobWireCodec... codecs) {
            this.contractRevision = revision;
            this.schemaId = schemaId;
            this.codecs.clear();
            this.codecs.addAll(Arrays.asList(codecs));
            return this;
        }

        /**
         * 业务作用：设置 Fanout 持久接收确认的等待和重发次数。
         *
         * @param timeout    每次等待时长
         * @param maxRetries 首次通知后的重发次数
         * @return 当前构建器。
         */
        public Builder fanoutReceipt(Duration timeout, int maxRetries) {
            this.fanoutReceiptTimeoutMs = timeout.toMillis();
            this.fanoutReceiptMaxRetries = maxRetries;
            return this;
        }

        /**
         * 业务作用：设置 Fanout 失败策略。 @param policy 失败策略 @return 当前构建器。
         */
        public Builder fanoutFailurePolicy(RedisJobFanoutFailurePolicy policy) {
            this.fanoutFailurePolicy = policy;
            return this;
        }

        /**
         * 业务作用：设置任务定义单调修订号。 @param revision 修订号 @return 当前构建器。
         */
        public Builder definitionRevision(long revision) {
            this.definitionRevision = revision;
            return this;
        }

        /**
         * 业务作用：完成互斥、边界与契约校验后生成不可变任务定义。
         *
         * @return 任务定义。
         */
        public RedisJobDefinition build() {
            return new RedisJobDefinition(this);
        }
    }
}
