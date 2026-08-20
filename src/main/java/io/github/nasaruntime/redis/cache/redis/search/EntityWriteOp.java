package io.github.nasaruntime.redis.cache.redis.search;

/**
 * save / saveAll 使用的单条实体写计划：包含待 HSET 的字段字节视图与待 HDEL 的字段名字节视图。
 * <p>
 * package-internal 数据传输对象, 仅由 {@link RediSearch#buildHashOp} 构造,
 * {@link io.github.nasaruntime.redis.cache.redis.LettucePipeline.Actuator#hashSaveAsync} / {@code hashSave} 消费.
 * 业务侧不应直接持有此 record.
 * <p>
 * <b>为什么使用 byte[][] hashNames + byte[][] hashValues 双数组而非 Map&lt;byte[], byte[]&gt;</b>：
 * <ol>
 *   <li>byte[] 使用对象身份参与 Map 比较，容易让字段匹配语义产生歧义；双数组按索引对齐，字段名和值的关系明确。</li>
 *   <li>双数组允许构建阶段复用收集容器，Pipeline 收到写计划后也能使用池化 Map，限制热路径临时对象数量。</li>
 * </ol>
 *
 * @param key           实体 Redis key（含 prefix），供诊断日志记录
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
