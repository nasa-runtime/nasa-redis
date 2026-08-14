package io.github.nasaruntime.redis.cache.redis.search.convert;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import io.github.nasaruntime.redis.cache.redis.search.meta.FieldMeta;
import io.github.nasaruntime.redis.cache.redis.search.meta.MetaResolver;
import io.github.nasaruntime.core.utils.ObjMprUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * Nasa
 * 默认类型转换器：覆盖 JVM 常见类型 ↔ Redis 字符串
 * <p>
 * 支持：String / 数值原生类型及包装类 / Boolean / Character / BigDecimal / BigInteger / Enum。
 * <p>
 * 业务有自定义 JVM 类型 (例 {@code Instant} / 业务 POJO) 需求时, 推荐两种扩展路径:
 * <ul>
 *   <li>通过 {@code @Bean RsConverter} 自定义实现 (按 {@link io.github.nasaruntime.redis.cache.redis.Qualifier} 分源生效)</li>
 *   <li>对 JSON 模式: 注入自定义 {@link ObjectMapper} (Jackson Module 注册自定义 Serializer / Deserializer),
 *       新类型在 {@link #writeJson} / {@link #readJson} 路径自动生效</li>
 * </ul>
 * 无参构造默认 ObjectMapper 反射可选加载 JSR-310 时间模块.
 */
public class DefaultRsConverter implements RsConverter {

    private final ObjectMapper objectMapper;

    /**
     * 业务作用：以默认的 JSON 处理配置建出转换器。
     *
     * <p>参数说明: 无。
     */
    public DefaultRsConverter() {
        this(defaultMapper());
    }

    /**
     * 业务作用：以指定的 JSON 处理配置建出转换器，供业务统一各处的序列化行为。
     *
     * @param objectMapper JSON 处理配置
     */
    public DefaultRsConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 业务作用：无参构造的兜底 ObjectMapper, 反射可选加载 Jackson JSR-310 时间模块
     * (classpath 没 jackson-datatype-jsr310 就跳过, 不强依赖).
     * <p>
     * 生产推荐直接注入业务侧 Spring Boot 的全局 ObjectMapper 以复用模块/Date 格式等配置.
     *
     * @return 见上述说明。
     */
    private static ObjectMapper defaultMapper() {
        ObjectMapper m = ObjMprUtils.OBJECT_MAPPER.copy();
        // 不序列化 null 字段: 与 HASH 模式 (null → HDEL 不写) 一致, 省 Redis 内存/网络 (撮合百万订单显著);
        // round-trip 安全 — 缺失字段反序列化即为 null, RediSearch 对 null/缺失字段也不建索引, 查询无差别。
        // 注: 仅作用于默认 mapper; 业务注入自定义 ObjectMapper 时如需同效需自行 setSerializationInclusion(NON_NULL)。
        m.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        try {
            Class<?> cls = Class.forName("com.fasterxml.jackson.datatype.jsr310.JavaTimeModule");
            m.registerModule((Module) cls.getDeclaredConstructor().newInstance());
        } catch (Throwable ignored) {
            // jsr310 模块不在 classpath, LocalDateTime 等类型反序列化会报错, 业务自行注入完整 ObjectMapper
        }
        return m;
    }

    /**
     * 业务作用：把实体字段取值转成索引中的存储形式。
     * 枚举按其序列化取值渲染而非常量名，使查询条件里写的取值与存储形式一致——
     * 两者不一致会让条件永远匹配不上且不报错。
     *
     * @param value 字段取值
     * @param meta  字段元信息
     * @return 存储形式的字符串；取值为 null 时为 null。
     */
    @Override
    public String write(Object value, FieldMeta meta) {
        if (value == null) return null;
        return switch (value) {
            case String s -> s;
            // 写 "true"/"false" 与 TAG 查询端 (Criteria.is(true) → renderValue → "true") 一致, 也跟 JSON 模式原生布尔统一。
            // parseBool 同时接受 "1"/"0"，保证已存整数布尔值仍可读取。
            case Boolean b -> b ? "true" : "false";
            case BigDecimal bd -> bd.toPlainString();
            case BigInteger bi -> bi.toString();
            case Number n -> n.toString();
            case Enum<?> e -> MetaResolver.renderEnum(e);
            case Character c -> c.toString();
            default -> value.toString();
        };
    }

    /**
     * 业务作用：把索引中的存储形式还原成实体字段取值。
     *
     * @param raw        存储形式的字符串
     * @param targetType 目标类型
     * @param meta       字段元信息
     * @return 还原后的取值；输入为 null 时为 null。
     */
    @Override
    public Object read(String raw, Class<?> targetType, FieldMeta meta) {
        if (raw == null) return null;
        if (targetType == String.class) return raw;
        if (targetType == long.class || targetType == Long.class) return Long.parseLong(raw);
        if (targetType == int.class || targetType == Integer.class) return Integer.parseInt(raw);
        if (targetType == short.class || targetType == Short.class) return Short.parseShort(raw);
        if (targetType == byte.class || targetType == Byte.class) return Byte.parseByte(raw);
        if (targetType == double.class || targetType == Double.class) return Double.parseDouble(raw);
        if (targetType == float.class || targetType == Float.class) return Float.parseFloat(raw);
        if (targetType == boolean.class || targetType == Boolean.class) return parseBool(raw);
        if (targetType == BigDecimal.class) return new BigDecimal(raw);
        if (targetType == BigInteger.class) return new BigInteger(raw);
        if (targetType == char.class || targetType == Character.class) return parseChar(raw);
        if (targetType.isEnum()) return MetaResolver.parseEnum(targetType, raw);

        throw new RediSearchException("Cannot convert '" + raw + "' to " + targetType.getName());
    }

    /**
     * 业务作用：严格校验: 空串/多字符一律抛 IAE, 防止静默丢数据.
     *
     * @param raw 见上述说明
     * @return 见上述说明。
     */
    private static char parseChar(String raw) {
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("Empty string for Character");
        }
        if (raw.length() > 1) {
            throw new IllegalArgumentException("Multi-char string for Character: " + raw);
        }
        return raw.charAt(0);
    }

    /**
     * 业务作用：识别 true/1/yes/on (true) 与 false/0/no/off (false), case-insensitive; 其它一律抛 IAE.
     *
     * @param raw 见上述说明
     * @return 见上述说明。
     */
    private static boolean parseBool(String raw) {
        if (raw.equalsIgnoreCase("true") || raw.equals("1")
                || raw.equalsIgnoreCase("yes") || raw.equalsIgnoreCase("on")) {
            return true;
        }
        if (raw.equalsIgnoreCase("false") || raw.equals("0")
                || raw.equalsIgnoreCase("no") || raw.equalsIgnoreCase("off")) {
            return false;
        }
        throw new IllegalArgumentException("Cannot parse '" + raw + "' as Boolean (expect true/1/yes/on or false/0/no/off)");
    }

    /**
     * 业务作用：把整个实体序列化成 JSON 文本，供 JSON 存储模式写入。
     *
     * @param entity 实体实例
     * @return JSON 文本。
     */
    @Override
    public String writeJson(Object entity) {
        try {
            return objectMapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
            throw new RediSearchException("Failed to serialize " + entity.getClass().getName() + " to JSON", e);
        }
    }

    /**
     * 业务作用：把 JSON 文本还原成实体。
     *
     * @param json JSON 文本
     * @param type 实体类型
     * @param <T>  实体类型
     * @return 还原出的实体。
     */
    @Override
    public <T> T readJson(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new RediSearchException("Failed to deserialize JSON to " + type.getName(), e);
        }
    }

    /**
     * 业务作用：把已解析成映射的字段集合转成实体，供解析检索响应时使用。
     *
     * @param typed 字段映射
     * @param type  实体类型
     * @param <T>   实体类型
     * @return 还原出的实体。
     */
    @Override
    public <T> T convertMap(Map<String, Object> typed, Class<T> type) {
        try {
            return objectMapper.convertValue(typed, type);
        } catch (IllegalArgumentException e) {
            throw new RediSearchException("Failed to convert map to " + type.getName()
                    + " (check @JsonCreator factory signature / @JsonProperty names)", e);
        }
    }

    /**
     * 业务作用：把 JSON 数组文本还原成实体列表，供数组存储模式读取。
     *
     * @param jsonArray JSON 数组文本
     * @param type      实体类型
     * @param <T>       实体类型
     * @return 还原出的实体列表；文本为空时为空列表。
     */
    @Override
    public <T> List<T> readJsonList(String jsonArray, Class<T> type) {
        try {
            return objectMapper.readValue(jsonArray,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, type));
        } catch (JsonProcessingException e) {
            throw new RediSearchException("Failed to deserialize JSON array to List<" + type.getName() + ">", e);
        }
    }

    /**
     * 业务作用：统计 JSON 数组文本中的元素个数，不逐个还原成实体。
     * 只需条数时省去整批反序列化，在大数组上差别显著。
     *
     * @param jsonArray JSON 数组文本
     * @return 元素个数；文本为空时为 0。
     */
    @Override
    public int countJsonArray(String jsonArray) {
        try {
            // 只解析成 JsonNode 数树, 不构造实体 (避免 @JsonCreator + 对象池仅为计数而 pool.get 泄漏)
            JsonNode node = objectMapper.readTree(jsonArray);
            if (node == null || node.isNull() || node.isMissingNode()) return 0;
            return node.isArray() ? node.size() : 1;
        } catch (JsonProcessingException e) {
            throw new RediSearchException("Failed to parse JSON array for count", e);
        }
    }
}
