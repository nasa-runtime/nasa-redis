package io.github.nasaruntime.redis.cache.redis.search.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Nasa
 * TAG 字段：精确匹配，适合枚举/ID/状态等离散值
 * <p>
 * 查询语法：{@code @field:{value}} 或 {@code @field:{v1|v2}}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TagField {

    /**
     * 业务作用：指定索引内字段名，使查询别名能够与对象字段或 Jackson 名称解耦。
     *
     * <p>参数说明: 无。
     *
     * @return 显式索引名；空字符串表示依次使用 Jackson 名称和对象字段名。
     */
    String name() default "";

    /**
     * 业务作用：声明标签字段是否参与 RediSearch 排序。
     *
     * <p>参数说明: 无。
     *
     * @return {@code true} 表示在索引 schema 中启用排序能力。
     */
    boolean sortable() default false;

    /**
     * 业务作用：定义多值标签在存储和查询协议中的分隔符。
     *
     * <p>参数说明: 无。
     *
     * @return 单字符标签分隔符，默认逗号。
     */
    String separator() default ",";

    /**
     * 业务作用：声明标签匹配是否保留大小写差异。
     *
     * <p>参数说明: 无。
     *
     * @return {@code true} 表示大小写不同的标签视为不同值。
     */
    boolean caseSensitive() default false;
}
