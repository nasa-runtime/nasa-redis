package io.github.nasaruntime.redis.cache.redis.search.annotation;

import io.github.nasaruntime.redis.cache.redis.search.convert.RsConverter;

/**
 * Nasa
 * RediSearch 索引底层数据类型.
 * <p>
 * <b>{@link #HASH}</b>: 字段以 Redis Hash 存储, 扁平结构, 默认.
 * 通过 {@link io.github.nasaruntime.redis.cache.redis.search.convert.RsConverter} 做 JVM 类型 ↔ 裸字符串转换.
 * <p>
 * <b>{@link #JSON}</b>: 文档以 RedisJSON 存储, 整体序列化为 JSON 字符串 (走 Jackson).
 * 服务端必须有 RedisJSON 模块 (Redis Stack 自带).
 * 嵌套对象天然支持; 字段路径默认 {@code $.{JVMFieldName}}.
 * <p>
 * <b>{@link #JSON_ARRAY}</b>: 多文档聚合到一个 ARRAY key, JSON.ARRAPPEND 写入,
 * RediSearch 通过 {@code $[*].field} 索引子文档. 一个 ARRAY key 由 {@link JsonArrayKey} 字段决定.
 * <ul>
 *   <li>子文档级 sort/page 不支持 → 框架直接抛 {@link UnsupportedOperationException}</li>
 *   <li>{@code findById(id)} 不支持 (没有 array key 上下文), 必须用 {@code findById(arrayKey, id)}</li>
 *   <li>定位: <b>收集</b>同一聚合维度的多个子文档, 减少 key 数, 牺牲排序分页</li>
 * </ul>
 */
public enum DataType {

    /**
     * Redis Hash 模式: 字段平铺, HSET/HGETALL
     */
    HASH,

    /**
     * RedisJSON 模式: 整体 JSON 文档, JSON.SET/JSON.GET, 支持嵌套
     */
    JSON,

    /**
     * RedisJSON 数组模式: 一个 ARRAY key 装多个子文档, JSON.ARRAPPEND/JSON.DEL filter.
     * 必须配合 {@link JsonArrayKey} 决定 ARRAY key.
     */
    JSON_ARRAY,

    /**
     * RedisJSON 分桶数组模式: 把同一组 {@link JsonArrayKey} 的子文档按 {@code @RsId.hashCode() % bucketCount}
     * 分散到多个 ARRAY key, 让单桶 size 控制在 O(N/n) 减小 JSON.DEL/JSON.SET 的 O(N) 成本.
     * <p>
     * key 格式: {@code prefix + "{" + arrayKeyParts joined + "}:bucket:" + bucketIdx}.
     * 例 {@code batch-mo:{BTCUSDT:BUY}:bucket:3}. hash tag {@code {...}} 确保同组所有桶在
     * Redis Cluster 下落同一 slot, 框架可用 LUA 跨桶聚合操作 (CROSSSLOT 限制因此规避).
     * <p>
     * 必须配合 {@link RsDocument#bucketCount()} {@code > 1}.
     * <p>
     * 设计取舍: 同组所有桶钉同节点, 失去跨节点 QPS 分散, 但换来"框架可用 LUA 一次 RTT 跨桶查/删/数".
     * Cluster 多节点的负载分散靠不同 {@link JsonArrayKey} 组合 (例 BTCUSDT vs ETHUSDT) 天然分布到不同 slot.
     */
    JSON_ARRAY_BUCKET
}
