package io.github.nasaruntime.redis.cache.redis.search.meta;

import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;

/**
 * Nasa
 * Redis key 编译表的一段 — 要么是字面 literal 文本, 要么是 entity 字段引用 (运行时反查值).
 * <p>
 * 由 {@link MetaResolver} 启动期一次性解析 {@code @RsDocument.prefix} 占位符产出 (例如
 * {@code "order:SMO:{sym}:{d}:"} 拆成 [literal("order:SMO:"), field(sym), literal(":"), field(d), literal(":")]),
 * 运行时 {@link EntityMeta#keyOf(Object)} 遍历数组拼 key, 零字符串解析.
 * <p>
 * literal / field 互斥: literal != null 时 field == null, 反之亦然.
 *
 * @param literal 字面字符串 (literal 段时非 null)
 * @param field   entity 字段引用 (placeholder 段时非 null), 走 VarHandle 零反射读
 */
public record KeySegment(String literal, FieldMeta field) {

    /**
     * 业务作用：在字符串末尾追加内容。
     *
     * @param sb 见方法语义
     * @param entity 见方法语义
     * 返回: 无返回值。
     */
    public void append(StringBuilder sb, Object entity) {
        if (literal != null) {
            sb.append(literal);
            return;
        }
        Object v = field.get(entity);
        if (v == null) {
            throw new RediSearchException("placeholder field '" + field.reflect().getName()
                    + "' is null on " + entity.getClass().getSimpleName()
                    + " — prefix occupies a key segment, value must not be null");
        }
        // enum 与全框架统一渲染 (@JsonValue 或 name); 渲染后再校验分隔符防 key 撞车
        String rendered = MetaResolver.renderValue(v);
        KeyParts.checkPart(rendered, "placeholder field '" + field.reflect().getName() + "'");
        sb.append(rendered);
    }
}
