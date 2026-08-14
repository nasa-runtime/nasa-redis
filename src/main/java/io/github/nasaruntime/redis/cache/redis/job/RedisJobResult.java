package io.github.nasaruntime.redis.cache.redis.job;

import java.util.Objects;

/**
 * 业务作用：承载 Handler 对当前 attempt 的业务判定和有限长度结果摘要。
 *
 * @param code    结果码
 * @param summary 结果摘要
 */
public record RedisJobResult(RedisJobResultCode code, String summary) {

    /**
     * 业务作用：校验结果对象始终具有明确结果码。
     *
     * @param code    结果码
     * @param summary 结果摘要
     */
    public RedisJobResult {
        Objects.requireNonNull(code, "code must not be null");
        summary = summary == null ? "" : summary;
    }

    /**
     * 业务作用：创建无摘要的成功结果。
     *
     * @return 成功结果。
     */
    public static RedisJobResult success() {
        return new RedisJobResult(RedisJobResultCode.SUCCESS, "");
    }

    /**
     * 业务作用：创建携带有限摘要的成功结果。
     *
     * @param summary 结果摘要
     * @return 成功结果。
     */
    public static RedisJobResult success(String summary) {
        return new RedisJobResult(RedisJobResultCode.SUCCESS, summary);
    }

    /**
     * 业务作用：要求框架按任务重试策略重新执行当前 Run。
     *
     * @param summary 失败摘要
     * @return 可重试结果。
     */
    public static RedisJobResult retry(String summary) {
        return new RedisJobResult(RedisJobResultCode.RETRY, summary);
    }

    /**
     * 业务作用：声明当前失败不可重试并直接进入失败终态。
     *
     * @param summary 失败摘要
     * @return 不可重试结果。
     */
    public static RedisJobResult failure(String summary) {
        return new RedisJobResult(RedisJobResultCode.FAIL_PERMANENT, summary);
    }

    /**
     * 业务作用：确认 Handler 已响应协作式取消。
     *
     * @param summary 取消摘要
     * @return 取消结果。
     */
    public static RedisJobResult cancelled(String summary) {
        return new RedisJobResult(RedisJobResultCode.CANCELLED, summary);
    }

    /**
     * 业务作用：声明业务线程已在执行时限后退出，由当前 attempt 主动提交超时结果。
     *
     * @param summary 超时摘要
     * @return 超时结果。
     */
    public static RedisJobResult timeout(String summary) {
        return new RedisJobResult(RedisJobResultCode.TIMEOUT, summary);
    }
}
