package io.github.nasaruntime.redis.cache.redis.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 业务作用：使用独立且关闭 Default Typing 的 Jackson 映射器编码跨语言 JSON 任务参数。
 */
public final class RedisJobJsonCodec {

    private static final Set<String> TYPE_METADATA = Set.of("@class", "@type");
    private final ObjectMapper mapper;

    /**
     * 业务作用：建立不继承 RedisProxy 通用序列化配置的 Job 专用 JSON 映射器。
     *
     * <p>参数说明: 无。
     */
    public RedisJobJsonCodec() {
        this.mapper = JsonMapper.builder()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build()
                .deactivateDefaultTyping();
    }

    /**
     * 业务作用：把确定业务类型编码为不携带 JVM 类型元数据的 JSON 参数。
     *
     * @param schemaId Schema 标识
     * @param value    业务参数
     * @return JSON 参数。
     */
    public RedisJobPayload encode(String schemaId, Object value) {
        Objects.requireNonNull(value, "value must not be null");
        try {
            byte[] bytes = mapper.writeValueAsBytes(value);
            validate(bytes);
            return new RedisJobPayload(schemaId, RedisJobWireCodec.JSON, bytes);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot encode RedisJob JSON payload", e);
        }
    }

    /**
     * 业务作用：按 Worker 已登记的目标类型解码 JSON，并在绑定前拒绝 JVM 类型元数据。
     *
     * @param bytes JSON 字节
     * @param type  Worker 参数类型
     * @param <T>   参数类型
     * @return 解码后的参数。
     */
    public <T> T decode(byte[] bytes, Class<T> type) {
        Objects.requireNonNull(type, "type must not be null");
        validate(bytes);
        try {
            return mapper.readValue(bytes, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("INVALID_PAYLOAD", e);
        }
    }

    /**
     * 业务作用：遍历 JSON 树并拒绝 Jackson/JVM 多态元数据字段。
     *
     * @param bytes JSON 字节
     *              返回：合法时正常返回，非法时抛出参数异常。
     */
    public void validate(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        try {
            rejectTypeMetadata(mapper.readTree(bytes));
        } catch (IOException e) {
            throw new IllegalArgumentException("INVALID_PAYLOAD", e);
        }
    }

    /**
     * 业务作用：递归检查对象字段，避免嵌套参数绕过顶层类型元数据门禁。
     *
     * @param node 当前 JSON 节点
     *             返回：未发现禁止字段时正常返回。
     */
    private static void rejectTypeMetadata(JsonNode node) {
        if (node == null) return;
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if (TYPE_METADATA.contains(field.getKey())) {
                    throw new IllegalArgumentException("INVALID_PAYLOAD: JVM type metadata is forbidden");
                }
                rejectTypeMetadata(field.getValue());
            }
        } else if (node.isArray()) {
            node.forEach(RedisJobJsonCodec::rejectTypeMetadata);
        }
    }
}
