package io.github.nasaruntime.redis.cache.redis.search.convert;

import io.github.nasaruntime.redis.cache.redis.Qualifier;
import io.github.nasaruntime.redis.cache.redis.search.meta.FieldMeta;

import java.util.List;
import java.util.Map;

/**
 * Nasa
 * 字段类型转换器：JVM 对象 ↔ Redis 字符串
 * <p>
 * RediSearch Hash 字段全部以 string 形式存储，JVM 类型与字符串之间的双向转换由本接口统一管理。
 * 默认支持：String / 数值 / Boolean / BigDecimal / Enum。业务有自定义 JVM 类型需求时通过
 * {@code @Bean RsConverter} 替换默认实现 (按 {@link io.github.nasaruntime.redis.cache.redis.Qualifier#qualifiers()} 分源), 或通过自定义
 * {@code ObjectMapper} (Jackson Module) 让 JSON 路径自动支持新类型.
 * <p>
 * JSON 模式下还需要整体实体的 JSON 序列化/反序列化, 见 {@link #writeJson} / {@link #readJson}。
 */
public interface RsConverter extends Qualifier {

    /**
     * 业务作用：声明本转换器服务于哪些 Redis 实例。
     * 默认返回空表示服务全部实例，单实例部署下无需重写。
     *
     * <p>参数说明: 无。
     *
     * @return 服务的实例名数组；为空表示不限。
     */
    @Override
    default String[] qualifiers() {
        return null;
    }

    /**
     * 业务作用：JVM 值 → Redis 字符串，写入前调用 (HASH 模式字段级)
     *
     * @param value 待写入的值
     * @param meta  见上述说明
     * @return 见上述说明。
     */
    String write(Object value, FieldMeta meta);

    /**
     * 业务作用：Redis 字符串 → JVM 值，读取后调用 (HASH 模式字段级)
     *
     * @param raw        见上述说明
     * @param targetType 见上述说明
     * @param meta       见上述说明
     * @return 见上述说明。
     */
    Object read(String raw, Class<?> targetType, FieldMeta meta);

    /**
     * 业务作用：整体对象 → JSON 字符串 (JSON 模式 save 时调用)
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    String writeJson(Object entity);

    /**
     * 业务作用：JSON 字符串 → 整体对象 (JSON 模式 read 时调用)
     *
     * @param json 见上述说明
     * @param type 目标实体类型
     * @return 见上述说明。
     */
    <T> T readJson(String json, Class<T> type);

    /**
     * 业务作用：把已 typed 的字段 Map 转换为目标实体, 走 Jackson 反序列化路径以让业务的
     * {@code @JsonCreator} 工厂方法生效 (常见用法: 工厂方法内部 {@code pool.get()} 接入对象池).
     * <p>
     * HASH 模式 mapToEntity 在 {@code EntityMeta#hasJsonCreator() == true} 时调用本方法:
     * map key 是 Jackson 属性名 (= {@code FieldMeta#redisName()}), value 是已经经过
     * {@link #read} 转换成目标 JVM 类型的对象, Jackson 不会再次进行类型转换.
     *
     * @param typed 见上述说明
     * @param type  反序列化目标类型
     * @return 见上述说明。
     */
    <T> T convertMap(Map<String, Object> typed, Class<T> type);

    /**
     * 业务作用：JSON 数组字符串 → {@code List<T>} (JSON_ARRAY 模式批量读用)。复用本 converter 的 ObjectMapper, 与
     * {@link #readJson} 同款配置 (custom Module / @JsonCreator / @JsonValue 一致), 且一次解析直达实体,
     * 避免 "chunk → Map → JSON → entity" 的二次编码。
     * <p>
     * 默认抛 UnsupportedOperationException: 自定义 RsConverter 若要支持 JSON_ARRAY 模式需 override。
     *
     * @param jsonArray 见上述说明
     * @param type      反序列化目标类型
     * @return 见上述说明。
     */
    default <T> List<T> readJsonList(String jsonArray, Class<T> type) {
        throw new UnsupportedOperationException(getClass().getName()
                + " does not implement readJsonList; required for DataType.JSON_ARRAY / JSON_ARRAY_BUCKET");
    }

    /**
     * 业务作用：JSON 数组字符串的元素个数 (JSON_ARRAY count 用)。<b>不构造实体</b> — 避免对 @JsonCreator + 对象池实体
     * 仅为计数而 {@code pool.get()} 却不归还造成池泄漏。默认抛 UnsupportedOperationException, 同 {@link #readJsonList}。
     *
     * @param jsonArray 见上述说明
     * @return 见上述说明。
     */
    default int countJsonArray(String jsonArray) {
        throw new UnsupportedOperationException(getClass().getName()
                + " does not implement countJsonArray; required for DataType.JSON_ARRAY / JSON_ARRAY_BUCKET");
    }
}
