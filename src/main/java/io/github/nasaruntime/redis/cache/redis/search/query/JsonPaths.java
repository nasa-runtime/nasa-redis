package io.github.nasaruntime.redis.cache.redis.search.query;

import io.github.nasaruntime.redis.cache.redis.search.meta.JsonValueAccessor;
import io.github.nasaruntime.redis.cache.redis.search.meta.MetaResolver;

/**
 * Nasa
 * JSONPath 字面量渲染工具 — 集中实现 RedisJSON JSONPath filter 内字符串字面量的转义.
 * <p>
 * {@code Criteria} 与 {@code JsonArraySupport} 必须复用本工具。仅转义 {@code "} 和 {@code \} 无法覆盖
 * {@code \n \t \r \b \f \0} 等控制字符，直接写入会被 RedisJSON JSONPath parser 拒绝。
 *
 * <h2>转义规则（JSON 标准与 RedisJSON path 约束）</h2>
 * <ul>
 *   <li>{@code "} → {@code \"}</li>
 *   <li>{@code \} → {@code \\}</li>
 *   <li>{@code \b \f \n \r \t} → 各自的两字符转义</li>
 *   <li>其他控制字符 (U+0000..U+001F 除上述外) → 6 字符 {@code \}{@code u00XX} 转义</li>
 *   <li>其它字符直接写入 (含 unicode, RedisJSON 接受 UTF-8 原文)</li>
 * </ul>
 */
public final class JsonPaths {

    /**
     * 业务作用：私有化构造，杜绝实例化——本类只提供 JSON 路径的拼装与转义。
     *
     * <p>参数说明: 无。
     */
    private JsonPaths() {}

    /**
     * 业务作用：渲染 JSONPath 字面量: Number/Boolean 直出, 其他 String.valueOf + 全转义 + 双引号包裹.
     * <p>
     * caller 必须自己保证 v != null (业务侧 fail-fast, 避免静默匹配 "null" 字符串).
     *
     * @param v 见上述说明
     * @return 见上述说明。
     */
    public static String literal(Object v) {
        // enum: 取 @JsonValue 原始对象 (Number/String), 让其按真实 JSON 类型渲染 (数字不加引号, 字符串加引号),
        // 与 Jackson 写进 JSON 文档的值一致; 无 @JsonValue 则用 name() 字符串。否则 enum 对象会被 String.valueOf
        // 成 name() 加引号, 与 JSON 里 @JsonValue 数字/字符串值不匹配, JSONPath filter 查不到。
        if (v instanceof Enum<?> e) {
            JsonValueAccessor acc = MetaResolver.jsonValueAccessor(e.getDeclaringClass());
            if (acc == null) {
                v = e.name();
            } else {
                Object raw = acc.get(e);
                v = raw == null ? e.name() : raw;
            }
        }
        if (v instanceof Number n) {
            // NaN/Infinity 不是合法 JSON 数字，写进 JSONPath filter 会让 RedisJSON parser 拒绝表达式。
            double d = n.doubleValue();
            if (!Double.isFinite(d)) {
                throw new IllegalArgumentException("JSONPath number must be finite, got " + v);
            }
            return n.toString();
        }
        if (v instanceof Boolean b) return b.toString();
        String s = String.valueOf(v);
        StringBuilder sb = new StringBuilder(s.length() + 4);
        sb.append('"');
        appendEscaped(sb, s);
        sb.append('"');
        return sb.toString();
    }

    /**
     * 业务作用：把 s 转义后追加到 sb. 暴露 raw 工具方法供需要自己控制引号 / 拼接的 caller 使用.
     *
     * @param sb 见上述说明
     * @param s  见上述说明
     */
    public static void appendEscaped(StringBuilder sb, String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> sb.append('\\').append('"');
                case '\\' -> sb.append('\\').append('\\');
                case '\b' -> sb.append('\\').append('b');
                case '\f' -> sb.append('\\').append('f');
                case '\n' -> sb.append('\\').append('n');
                case '\r' -> sb.append('\\').append('r');
                case '\t' -> sb.append('\\').append('t');
                default -> {
                    if (c < 0x20) {
                        // 其他 ASCII 控制字符: 6 字符 backslash-u-00XX 转义
                        sb.append('\\').append('u').append('0').append('0');
                        sb.append(hex((c >> 4) & 0xF));
                        sb.append(hex(c & 0xF));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
    }

    /**
     * 业务作用：把一个半字节转成十六进制字符，供转义序列拼装使用。
     *
     * @param n 半字节值
     * @return 对应的十六进制字符。
     */
    private static char hex(int n) {
        return (char) (n < 10 ? '0' + n : 'a' + n - 10);
    }
}
