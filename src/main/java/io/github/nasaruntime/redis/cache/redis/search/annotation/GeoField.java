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
     * 业务作用：指定索引内字段名，使查询别名能够与对象字段或 Jackson 名称解耦。
     *
     * <p>参数说明: 无。
     *
     * @return 显式索引名；空字符串表示依次使用 Jackson 名称和对象字段名。
     */
    String name() default "";

    /**
     * 业务作用：声明地理字段是否参与 RediSearch 排序。
     *
     * <p>参数说明: 无。
     *
     * @return {@code true} 表示在索引 schema 中启用排序能力。
     */
    boolean sortable() default false;
}
