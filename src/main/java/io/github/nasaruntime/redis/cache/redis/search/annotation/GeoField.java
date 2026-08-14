package io.github.nasaruntime.redis.cache.redis.search.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Nasa
 * GEO 字段：地理位置（经度,纬度），支持半径范围查询
 * <p>
 * 查询语法：{@code @field:[lng lat radius unit]}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface GeoField {

    /**
     * 索引内字段名;默认空 → 取 Jackson {@code @JsonProperty} 值(若有), 否则 对象字段名
     */
    String name() default "";

    /**
     * 是否可排序
     */
    boolean sortable() default false;
}
