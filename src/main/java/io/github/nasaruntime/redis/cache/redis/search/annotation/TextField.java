package io.github.nasaruntime.redis.cache.redis.search.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Nasa
 * TEXT 字段：分词全文搜索，支持权重 / 前缀 / 模糊
 * <p>
 * 查询语法：{@code @field:word}、{@code @field:word*}（前缀）、{@code @field:%word%}（模糊）
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TextField {

    /**
     * 索引内字段名;默认空 → 取 Jackson {@code @JsonProperty} 值(若有), 否则 对象字段名
     */
    String name() default "";

    /**
     * 相关性权重
     */
    double weight() default 1.0;

    /**
     * 是否可排序
     */
    boolean sortable() default false;

    /**
     * 是否禁用词干提取
     */
    boolean noStem() default false;

    /**
     * 拼音/语音匹配（如 dm:en），空表示不启用
     */
    String phonetic() default "";
}
