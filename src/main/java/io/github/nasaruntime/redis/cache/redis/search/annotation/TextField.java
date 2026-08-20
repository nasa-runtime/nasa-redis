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
     * 业务作用：指定索引内字段名，使查询别名能够与对象字段或 Jackson 名称解耦。
     *
     * <p>参数说明: 无。
     *
     * @return 显式索引名；空字符串表示依次使用 Jackson 名称和对象字段名。
     */
    String name() default "";

    /**
     * 业务作用：设置全文检索相关性评分中的字段权重。
     *
     * <p>参数说明: 无。
     *
     * @return 传给 RediSearch schema 的正权重，默认 {@code 1.0}。
     */
    double weight() default 1.0;

    /**
     * 业务作用：声明文本字段是否参与 RediSearch 排序。
     *
     * <p>参数说明: 无。
     *
     * @return {@code true} 表示在索引 schema 中启用排序能力。
     */
    boolean sortable() default false;

    /**
     * 业务作用：控制全文索引是否关闭词干提取，供必须精确保留词形的字段使用。
     *
     * <p>参数说明: 无。
     *
     * @return {@code true} 表示关闭词干提取。
     */
    boolean noStem() default false;

    /**
     * 业务作用：选择 RediSearch 的语音匹配算法。
     *
     * <p>参数说明: 无。
     *
     * @return 算法标识（如 {@code dm:en}）；空字符串表示不启用。
     */
    String phonetic() default "";
}
