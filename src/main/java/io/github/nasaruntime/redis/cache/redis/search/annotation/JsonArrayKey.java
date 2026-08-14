package io.github.nasaruntime.redis.cache.redis.search.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Nasa
 * 标记 {@link DataType#JSON_ARRAY} / {@link DataType#JSON_ARRAY_BUCKET} 模式下决定 ARRAY key 的字段,
 * 拼接到 {@link RsDocument#prefix()} 之后.
 * <p>
 * 同类内允许多个 {@code @JsonArrayKey} 字段, 按 {@link #order()} 升序拼接 (顺序稳定与 JVM 字段反射顺序无关).
 * 跨父类继承链可叠加, 父类字段也参与排序.
 * 同 {@code order} 值出现 ≥ 2 次时 MetaResolver 启动期抛错, 强制业务消除歧义.
 * <p>
 * 字段类型限制: 必须是 {@link String} / {@link Number} / primitive / enum. enum 按 {@code @JsonValue} 渲染
 * (有则用其值, 无则 {@link Enum#name()}), 与 key 拼接 / bucket 计算一致. 值为 null 时 save 抛 IllegalArgumentException.
 * 字段值不得含 {@code :} / {@code &#123;} / {@code &#125;} (会破坏 key 拼接和 hash tag 解析).
 * <p>
 * <b>非 JSON_ARRAY/BUCKET 模式</b>下标注本注解会被忽略且 MetaResolver 记录一行 warn 日志, 不阻塞启动.
 *
 * <h2>⚠ 不可变约束</h2>
 * <b>本注解标注的字段一旦写入 Redis, 业务侧不得在 entity 生命周期内修改其值再 saveOrReplace.</b>
 * 框架的 saveOrReplace 只在 entity 当前 @JsonArrayKey 算出的目标 key 上 DEL + APPEND, 旧 @JsonArrayKey
 * 对应的 key 里旧子文档不会被清, 形成幽灵重复 (JSON_ARRAY 跨 key 跨 slot CROSSSLOT 受限,
 * JSON_ARRAY_BUCKET 跨 hash tag 同样受限). 业务侧若要更改 @JsonArrayKey, 必须先显式
 * {@code jsonArrayOps().removeSubDoc(oldParts, subId, type)} 再 save 新 entity.
 *
 * <p>单字段示例:
 * <pre>
 * &#064;RsDocument(index = "idx:order-batch", prefix = "orders:batch:", type = DataType.JSON_ARRAY)
 * public class TestOrder {
 *     &#064;RsId       Long id;          // 子文档 id (ARRAY 内唯一)
 *     &#064;JsonArrayKey String sym;     // 决定存到哪个 ARRAY key
 *     // save(mo) → 实际 key = "orders:batch:" + mo.sym
 *     //         例 "orders:batch:BTCUSDT"
 * }
 * </pre>
 *
 * <p>多字段示例 (order 显式):
 * <pre>
 * &#064;RsDocument(index = "idx:batch-mo", prefix = "batch-mo:", type = DataType.JSON_ARRAY)
 * public class BatchMo {
 *     &#064;RsId Long id;
 *     &#064;JsonArrayKey(order = 1) String sym;
 *     &#064;JsonArrayKey(order = 2) String dir;
 *     // save(mo) → key = "batch-mo:" + mo.sym + ":" + mo.dir
 *     //         例 "batch-mo:BTCUSDT:BUY"
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface JsonArrayKey {

    /**
     * 多 {@code @JsonArrayKey} 字段拼接顺序, 升序排列, 默认 0.
     * <p>
     * 单字段场景保持默认 0 即可; 同类多字段时必须显式区分 (例 {@code order = 1} / {@code order = 2}),
     * 否则 MetaResolver 启动期抛"duplicate order"错误.
     */
    int order() default 0;
}
