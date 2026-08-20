package io.github.nasaruntime.redis.cache.redis.search.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Nasa
 * 标记一个类为 RediSearch 文档实体
 * <p>
 * 用法：
 * <pre>
 * &#064;RsDocument(index = "idx:order", prefix = "order:")
 * public class Order {
 *     &#064;RsId String id;
 *     &#064;TagField String userId;
 *     // ...
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RsDocument {

    /**
     * 业务作用：RediSearch 索引名，如 {@code idx:order}
     *
     * @return 见上述说明。
     */
    String index();

    /**
     * 业务作用：Redis key 前缀，如 {@code order:}，与 {@link RsId} 拼接成 key
     *
     * @return 见上述说明。
     */
    String prefix();

    /**
     * 业务作用：选择实体在 Redis 中的存储布局及对应的 RediSearch 索引路径。
     *
     * <p>参数说明: 无。
     *
     * @return 文档存储类型，默认使用 HASH。
     */
    DataType type() default DataType.HASH;

    /**
     * 业务作用：为分桶 JSON 数组声明不可热变更的桶数量，控制单桶线性操作成本。
     *
     * 仅 {@link DataType#JSON_ARRAY_BUCKET} 模式生效: 桶数量, 必须 {@code > 1}.
     * <p>
     * 推荐范围 4-32 — 桶数 = 总文档数 / 期望单桶 size (~5000 是 JSON.DEL O(N) 在 1ms 内的临界点).
     * <p>
     * <b>不可热修改</b>: 一旦上线, 修改 bucketCount 会导致新旧数据按不同桶散列, 查不到老子文档.
     * 修改等同于 schema 变更, 必须停服 + 数据迁移 (扫所有老桶 → 按新规则重写).
     *
     * <p>参数说明: 无。
     *
     * @return 分桶数量；非分桶模式保持 {@code 0}。
     */
    int bucketCount() default 0;
}
