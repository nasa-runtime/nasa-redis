package io.github.nasaruntime.redis.cache.redis.search.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Nasa
 * NUMERIC 字段：数值范围查询
 * <p>
 * 查询语法：{@code @field:[min max]}，{@code -inf}/{@code +inf} 表示无穷
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface NumericField {

    /**
     * 业务作用：指定索引内字段名，使查询别名能够与对象字段或 Jackson 名称解耦。
     *
     * <p>参数说明: 无。
     *
     * @return 显式索引名；空字符串表示依次使用 Jackson 名称和对象字段名。
     */
    String name() default "";

    /**
     * 业务作用：声明数值字段是否参与 RediSearch 排序。
     *
     * <p>参数说明: 无。
     *
     * @return {@code true} 表示在索引 schema 中启用排序能力。
     */
    boolean sortable() default false;

    /**
     * 业务作用：声明字段是否仅供返回而不建立可检索索引。
     *
     * <p>参数说明: 无。
     *
     * @return {@code true} 表示写入 schema 但不为该字段建立索引。
     */
    boolean noIndex() default false;
}
