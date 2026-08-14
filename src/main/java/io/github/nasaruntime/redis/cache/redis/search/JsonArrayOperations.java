package io.github.nasaruntime.redis.cache.redis.search;

import io.github.nasaruntime.redis.cache.redis.search.query.Criteria;
import io.github.nasaruntime.redis.cache.redis.search.annotation.DataType;
import io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey;
import io.github.nasaruntime.redis.cache.redis.search.query.RsQuery;

import java.util.List;

/**
 * Nasa
 * {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY} 模式专属操作视图.
 * <p>
 * 跟单文档 CRUD 的语义差异:
 * <ul>
 *   <li>一个 ARRAY key 装多个子文档, 子文档通过 {@code @RsId} 字段在 array 内唯一</li>
 *   <li>ARRAY key 由 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey} 字段拼接 (业务侧调用时按声明顺序传 {@code parts})</li>
 *   <li>所有"按子文档查/删"的 API 都需要传 array key 上下文 (没有 array key 框架无法定位)</li>
 * </ul>
 * <p>
 * 限制 (第一版):
 * <ul>
 *   <li>{@link RsQuery#sortBy(org.springframework.data.domain.Sort)} 和分页一律抛 {@link UnsupportedOperationException}
 *       (JSON ARRAY 子文档级排序分页 RediSearch 不支持; 需要排序请用 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON} 单文档模式)</li>
 *   <li>{@link io.github.nasaruntime.redis.cache.redis.search.query.Criteria} 仅支持 {@code is} 等值 + 多条件 AND;
 *       其他 op (in / between / match) 暂抛 UOE, 后续按需扩展 JSONPath 翻译</li>
 * </ul>
 * <p>
 * 通过 {@link RediSearch#jsonArrayOps()} 获取实例.
 */
public interface JsonArrayOperations {

    /**
     * 业务作用：ARRAY 模式 save: 用实体里的 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey} 字段算 ARRAY key, append 到 array 末尾.
     * <p>
     * <b>语义是 append-only, 不做按 sub id 去重</b>: 同一 {@code @RsId} 值多次 {@code save} 会在 array 里出现多份重复子文档.
     * 业务侧若需要 upsert (按 sub id 去重 + 覆盖), 用 {@link #saveOrReplace(Object)}.
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    <T> T save(T entity);

    /**
     * 业务作用：ARRAY 模式 saveAll: 按 ARRAY key 分组, 每组一次 LUA append (init-or-append).
     * <p>
     * 同样是 <b>append-only 语义</b>, 不做去重. 见 {@link #save}.
     *
     * @param entities 见上述说明
     * @return 见上述说明。
     */
    <T> List<T> saveAll(Iterable<T> entities);

    /**
     * 业务作用：ARRAY 模式 upsert: 先按 {@code @RsId} 值删除已有子文档 (若存在), 再 append. 保证 array 内同 sub id 不重复.
     * <p>
     * 比 {@link #save} 多一次 JSON.DEL 开销, 但语义安全 (适合"重复触发可能性高"的写入路径, 例如启动恢复/重试).
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    <T> T saveOrReplace(T entity);

    /**
     * 业务作用：在指定 ARRAY 内按子文档条件查询. JSON.GET key '$[?(...)]'.
     * <p>
     * 参数 {@code arrayKeyParts}: 按 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey} 字段声明顺序传值. 数量必须匹配, 否则 IAE.
     *
     * @param arrayKeyParts 见上述说明
     * @param query         见上述说明
     * @param type          反序列化目标类型
     * @return 见上述说明。
     */
    <T> List<T> findInArray(Object[] arrayKeyParts, RsQuery query, Class<T> type);

    /**
     * 业务作用：按子文档标识在数组中定位一个子文档。
     * 数组存储模式下多个子文档共处一个键，因此定位要先取整个数组再筛选。
     *
     * @param arrayKeyParts 定位数组所在键的片段，顺序须与键模板一致
     * @param subId         子文档标识
     * @param type          子文档类型
     * @param <T>           子文档类型
     * @return 子文档；不存在时为 null。
     */
    <T> T findSubDoc(Object[] arrayKeyParts, Object subId, Class<T> type);

    /**
     * 业务作用：判断数组中是否存在给定标识的子文档。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param subId         子文档标识
     * @param type          子文档类型
     * @return 存在返回 true。
     */
    boolean existsSubDoc(Object[] arrayKeyParts, Object subId, Class<?> type);

    /**
     * 业务作用：按标识从数组中移除一个子文档。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param subId         子文档标识
     * @param type          子文档类型
     * @return 实际移除的条数。
     */
    long removeSubDoc(Object[] arrayKeyParts, Object subId, Class<?> type);

    /**
     * 业务作用：按查询条件从数组中批量移除子文档。
     * <b>整个数组会被重写</b>：删除是取出、过滤、写回三步，数组很大时开销显著。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param query         查询条件
     * @param type          子文档类型
     * @return 实际移除的条数。
     */
    long removeInArray(Object[] arrayKeyParts, RsQuery query, Class<?> type);

    /**
     * 业务作用：统计数组中满足条件的子文档数。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param query         查询条件
     * @param type          子文档类型
     * @return 满足条件的条数。
     */
    long countInArray(Object[] arrayKeyParts, RsQuery query, Class<?> type);

    // ============ vararg 便利重载 (单 @JsonArrayKey 字段场景常见) ============
    //
    // 把 arrayKeyParts 挪到参数列表末尾做 vararg, 避免业务侧每次 new Object[]{...}.
    // 典型用法:
    //   ops.findInArray(query, Order.class, "BTCUSDT")            // 单 key 字段
    //   ops.findInArray(query, Order.class, "BTCUSDT", "BUY")     // 多 key 字段
    // 各重载内部转发到 array-key-parts 在前的 canonical 签名, 行为完全等价.

    /**
     * 业务作用：{@link #findInArray(Object[], RsQuery, Class)} 的 vararg 重载: arrayKeyParts 放参数末尾.
     * 单/多 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey} 字段都适用, 业务侧无需手动 new Object[]{...}.
     *
     * @param query         见上述说明
     * @param type          反序列化目标类型
     * @param arrayKeyParts 见上述说明
     * @return 见上述说明。
     */
    default <T> List<T> findInArray(RsQuery query, Class<T> type, Object... arrayKeyParts) {
        return findInArray(arrayKeyParts, query, type);
    }

    /**
     * 业务作用：{@link #findSubDoc(Object[], Object, Class)} 的 vararg 重载: arrayKeyParts 放参数末尾.
     * 不存在返回 null.
     *
     * @param subId         见上述说明
     * @param type          反序列化目标类型
     * @param arrayKeyParts 见上述说明
     */
    default <T> T findSubDoc(Object subId, Class<T> type, Object... arrayKeyParts) {
        return findSubDoc(arrayKeyParts, subId, type);
    }

    /**
     * 业务作用：{@link #existsSubDoc(Object[], Object, Class)} 的 vararg 重载: arrayKeyParts 放参数末尾.
     *
     * @param subId         见上述说明
     * @param type          反序列化目标类型
     * @param arrayKeyParts 见上述说明
     * @return 见上述说明。
     */
    default boolean existsSubDoc(Object subId, Class<?> type, Object... arrayKeyParts) {
        return existsSubDoc(arrayKeyParts, subId, type);
    }

    /**
     * 业务作用：{@link #removeSubDoc(Object[], Object, Class)} 的 vararg 重载: arrayKeyParts 放参数末尾.
     * 返回被删数量 (0 或 1).
     *
     * @param subId         见上述说明
     * @param type          反序列化目标类型
     * @param arrayKeyParts 见上述说明
     */
    default long removeSubDoc(Object subId, Class<?> type, Object... arrayKeyParts) {
        return removeSubDoc(arrayKeyParts, subId, type);
    }

    /**
     * 业务作用：{@link #removeInArray(Object[], RsQuery, Class)} 的 vararg 重载: arrayKeyParts 放参数末尾.
     * 返回删除的子文档数.
     *
     * @param query         见上述说明
     * @param type          反序列化目标类型
     * @param arrayKeyParts 见上述说明
     */
    default long removeInArray(RsQuery query, Class<?> type, Object... arrayKeyParts) {
        return removeInArray(arrayKeyParts, query, type);
    }

    /**
     * 业务作用：{@link #countInArray(Object[], RsQuery, Class)} 的 vararg 重载: arrayKeyParts 放参数末尾.
     *
     * @param query         见上述说明
     * @param type          反序列化目标类型
     * @param arrayKeyParts 见上述说明
     * @return 见上述说明。
     */
    default long countInArray(RsQuery query, Class<?> type, Object... arrayKeyParts) {
        return countInArray(arrayKeyParts, query, type);
    }
}
