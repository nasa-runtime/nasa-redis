package io.github.nasaruntime.redis.cache.redis.job;

import java.util.regex.Pattern;

/**
 * 业务作用：统一约束会进入 Redis 键、Stream 与跨语言信封的业务名称。
 */
final class RedisJobNames {

    private static final Pattern SAFE = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}");

    /**
     * 业务作用：阻止实例化，确保名称约束只通过统一静态门禁执行。
     *
     * <p>参数说明: 无。
     */
    private RedisJobNames() {}

    /**
     * 业务作用：拒绝空白、过长或包含控制字符的协议名称，避免键路由歧义。
     *
     * @param value 待校验名称
     * @param field 字段名
     * @return 去除首尾空白后的名称。
     */
    static String requireName(String value, String field) {
        if (value == null || !SAFE.matcher(value.trim()).matches()) {
            throw new IllegalArgumentException(field + " contains unsupported characters");
        }
        return value.trim();
    }
}
