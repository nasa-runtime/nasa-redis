package io.github.nasaruntime.redis.cache.redis.search;

import io.github.nasaruntime.redis.cache.redis.search.query.AggregationResult;
import io.github.nasaruntime.redis.cache.redis.search.query.RsAggregation;
import io.github.nasaruntime.redis.cache.redis.search.query.RsQuery;

import java.util.List;
import java.util.Optional;

/**
 * Nasa
 * RediSearch 主操作接口，统一索引管理、文档写入、查询和聚合入口。
 * <p>
 * 完整生命周期：
 * <pre>
 *   ensureIndex(Class)  ← 启动期建索引
 *   save / saveAll      ← 写入 Hash / JSON（索引自动同步）
 *   find / count / ...  ← FT.SEARCH 查询
 *   aggregate           ← FT.AGGREGATE 聚合
 *   remove              ← FT.SEARCH + DEL
 *   dropIndex           ← FT.DROPINDEX
 * </pre>
 */
public interface RediSearchOperations {

    // ============ 索引管理 ============

    /**
     * 业务作用：确保实体对应的索引已存在，不存在则按其注解声明创建。
     * 索引只对创建之后写入的文档生效，既有文档不会被自动纳入。
     *
     * @param type 实体类型
     * 返回: 无返回值。
     */
    void ensureIndex(Class<?> type);

    /**
     * 业务作用：判断实体对应的索引是否已存在。
     *
     * @param type 实体类型
     * @return 索引已存在返回 true。
     */
    boolean indexExists(Class<?> type);

    /**
     * 业务作用：删除实体对应的索引，保留被索引的文档本身。
     *
     * @param type 实体类型
     * 返回: 无返回值。
     */
    void dropIndex(Class<?> type);

    /**
     * 业务作用：删除索引，并按需连同被索引的文档一并删除。连带删除<b>不可逆</b>。
     *
     * @param type            实体类型
     * @param deleteDocuments true 表示连同文档一并删除
     * 返回: 无返回值。
     */
    void dropIndex(Class<?> type, boolean deleteDocuments);

    // ============ 单文档 CRUD ============

    /**
     * 业务作用：保存一个实体；同主键的重复保存是覆盖而非新增。
     *
     * @param entity 待保存的实体
     * @param <T>    实体类型
     * @return 原样返回入参实体。
     */
    <T> T save(T entity);

    /**
     * 业务作用：批量保存实体，合并进同一批次一次发出；批次内各条<b>互不构成事务</b>。
     *
     * @param entities 待保存的实体集合
     * @param <T>      实体类型
     * @return 原样返回入参实体的列表。
     */
    <T> List<T> saveAll(Iterable<T> entities);

    /**
     * 业务作用：按主键查一个实体。
     *
     * @param id   主键
     * @param type 实体类型
     * @param <T>  实体类型
     * @return 实体；不存在时为 null。
     */
    <T> T findById(String id, Class<T> type);

    /**
     * 业务作用：按主键查一个实体，以可空容器返回，使调用方不必自行判空。
     *
     * @param id   主键
     * @param type 实体类型
     * @param <T>  实体类型
     * @return 实体；不存在时为空容器。
     */
    <T> Optional<T> findOptionalById(String id, Class<T> type);

    /**
     * 业务作用：按主键判断实体是否存在，不取内容，省去一次反序列化与传输。
     *
     * @param id   主键
     * @param type 实体类型
     * @return 存在返回 true。
     */
    boolean existsById(String id, Class<?> type);

    /**
     * 业务作用：按主键删除实体及其索引项。
     *
     * @param id   主键
     * @param type 实体类型
     * @return 删除成功返回 true。
     */
    boolean deleteById(String id, Class<?> type);

    // ============ 子字段原子操作 (RedisJSON) ============

    /**
     * 业务作用：按增量原子调整 JSON 文档中的数值字段。
     * 自增在服务端完成，并发调用不会丢更新。
     *
     * @param type     实体类型，据其键模板定位目标文档
     * @param id       实体主键
     * @param jsonPath 见方法语义
     * @param delta 增减量
     * 返回: 无返回值。
     */
    void jsonNumIncrBy(Class<?> type, Object id, String jsonPath, long delta);

    /**
     * 业务作用：按增量原子调整 JSON 文档中的数值字段。
     * 自增在服务端完成，并发调用不会丢更新。
     *
     * @param type     实体类型，据其键模板定位目标文档
     * @param parts 见方法语义
     * @param jsonPath 见方法语义
     * @param delta 增减量
     * 返回: 无返回值。
     */
    void jsonNumIncrBy(Class<?> type, Object[] parts, String jsonPath, long delta);

    // ============ 查询 ============

    /**
     * 业务作用：按查询条件检索实体列表；未显式分页时按默认条数截断，<b>不是返回全部命中</b>。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @param <T>   实体类型
     * @return 命中的实体列表。
     */
    <T> List<T> find(RsQuery query, Class<T> type);

    /**
     * 业务作用：按查询条件取第一个命中实体；命中多条时返回哪一条取决于排序。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @param <T>   实体类型
     * @return 第一个命中实体；无命中时为 null。
     */
    <T> T findOne(RsQuery query, Class<T> type);

    /**
     * 业务作用：按查询条件取第一个命中实体，以可空容器返回。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @param <T>   实体类型
     * @return 实体；无命中时为空容器。
     */
    <T> Optional<T> findOptionalOne(RsQuery query, Class<T> type);

    /**
     * 业务作用：统计命中条数，不取文档内容。统计走索引，与实际文档数可能有短暂偏差。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @return 命中条数。
     */
    long count(RsQuery query, Class<?> type);

    /**
     * 业务作用：判断查询条件是否至少命中一个指定类型的索引文档。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @return 至少命中一个文档时返回 true。
     */
    boolean exists(RsQuery query, Class<?> type);

    /**
     * 业务作用：按查询条件批量删除；检索与删除之间新写入的文档不会被删掉。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @return 实际删除的条数。
     */
    long remove(RsQuery query, Class<?> type);

    /**
     * 业务作用：只取命中文档的 id 列表 (FT.SEARCH NOCONTENT).
     * <p>
     * 适合需要批量按 id 做后续操作 (业务自定义 pipeline / 取部分字段 / 检查存在性) 的场景.
     * key 前缀 (@RsDocument.prefix) 已剥离, 返回的就是 @RsId 值.
     *
     * @param query 见上述说明
     * @param type  反序列化目标类型
     */
    <T> List<String> findKeys(RsQuery query, Class<T> type);

    // ============ 聚合 ============

    /**
     * 业务作用：执行聚合检索，把统计下推到服务端完成，避免把大量文档取回本地再聚合。
     *
     * @param aggregation 聚合定义
     * @param type        实体类型
     * @return 聚合结果。
     */
    AggregationResult aggregate(RsAggregation aggregation, Class<?> type);
}
