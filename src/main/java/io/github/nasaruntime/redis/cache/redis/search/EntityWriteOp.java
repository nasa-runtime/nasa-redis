package io.github.nasaruntime.redis.cache.redis.search;

/**
 * Nasa
 * save / saveAll 使用的"单条实体写计划": 待 HSET 的字段 byte[] 视图 + 待 HDEL 的字段名 byte[] 视图.
 * <p>
 * package-internal 数据传输对象, 仅由 {@link RediSearch#buildHashOp} 构造,
 * {@link io.github.nasaruntime.redis.cache.redis.LettucePipeline.Actuator#hashSaveAsync} / {@code hashSave} 消费.
 * 业务侧不应直接持有此 record.
 * <p>
 * <b>为什么用 byte[][] hashNames + byte[][] hashValues 双数组而非 Map&lt;byte[], byte[]&gt;</b>:
 * <ol>
 *   <li>类型签名清晰: Map&lt;byte[], byte[]&gt; 走 identity hashCode/equals 是 misleading,
 *       双数组按 index 对齐 (i 位 name 对应 i 位 value) 语义直观</li>
 *   <li>性能更好: 之前每次 build 都 alloc 一个 LinkedHashMap (跨 record boundary 不能池化);
 *       现在 build 时双数组从 RecycleLinkedList 池化收集, toArray 后 list 归池, 稳态零 GC.
 *       LettucePipeline.hashSaveAsync 收到 op 后构造池化 {@code RecycleLinkedMap} 走 OP_HMSET 路径,
 *       Map 本身也复用</li>
 * </ol>
 *
 * @param key           实体 Redis key (含 prefix), 仅给上层调试 / 日志输出用
 * @param keyBytes      key 的 UTF-8 字节, pipeline HMSET / HDEL 入队直接复用
 * @param hashNames     已序列化的 HSET field 名数组 (UTF-8 byte[]), 跟 {@link #hashValues} 按 index 对齐
 * @param hashValues    已序列化的 HSET value 数组 (UTF-8 byte[]), 跟 {@link #hashNames} 按 index 对齐
 * @param toDeleteBytes 待 HDEL 的 field 名数组 (UTF-8 byte[]), 用于 OP_HDEL_MULTI extras; 没空字段时长度为 0
 */
public record EntityWriteOp(String key,
                            byte[] keyBytes,
                            byte[][] hashNames,
                            byte[][] hashValues,
                            byte[][] toDeleteBytes) {}
