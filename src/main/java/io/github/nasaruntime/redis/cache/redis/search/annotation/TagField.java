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
     * 索引内字段名;默认空 → 取 Jackson {@code @JsonProperty} 值(若有), 否则 对象字段名
     */
    String name() default "";

    /**
     * 是否可排序
     */
    boolean sortable() default false;

    /**
     * 多值 tag 分隔符，默认逗号
     */
    String separator() default ",";

    /**
     * 是否区分大小写
     */
    boolean caseSensitive() default false;
}
