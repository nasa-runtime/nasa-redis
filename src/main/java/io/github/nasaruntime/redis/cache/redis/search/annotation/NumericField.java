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
     * 索引内字段名;默认空 → 取 Jackson {@code @JsonProperty} 值(若有), 否则 对象字段名
     */
    String name() default "";

    /**
     * 是否可排序
     */
    boolean sortable() default false;

    /**
     * 是否不建索引（仅作为返回字段）
     */
    boolean noIndex() default false;
}
