package io.github.nasaruntime.redis.cache.redis.job;

import java.util.Arrays;
import java.util.Objects;

/**
 * 业务作用：承载已经按跨语言任务契约编码的不可变参数字节。
 */
public final class RedisJobPayload {

    private final String schemaId;
    private final RedisJobWireCodec codec;
    private final byte[] bytes;

    /**
     * 业务作用：建立带 Schema 与编码标识的参数值并隔离调用方后续数组改写。
     *
     * @param schemaId Schema 标识
     * @param codec    编码方式
     * @param bytes    参数字节
     */
    public RedisJobPayload(String schemaId, RedisJobWireCodec codec, byte[] bytes) {
        this.schemaId = Objects.requireNonNull(schemaId, "schemaId must not be null");
        this.codec = Objects.requireNonNull(codec, "codec must not be null");
        this.bytes = Objects.requireNonNull(bytes, "bytes must not be null").clone();
    }

    /**
     * 业务作用：创建 Protobuf 参数，框架只透传字节而不解释消息类型。
     *
     * @param schemaId Schema 标识
     * @param bytes    Protobuf 字节
     * @return Protobuf 参数。
     */
    public static RedisJobPayload protobuf(String schemaId, byte[] bytes) {
        return new RedisJobPayload(schemaId, RedisJobWireCodec.PROTOBUF, bytes);
    }

    /**
     * 业务作用：创建不带结构解释的原始参数。
     *
     * @param schemaId Schema 标识
     * @param bytes    原始字节
     * @return 原始参数。
     */
    public static RedisJobPayload raw(String schemaId, byte[] bytes) {
        return new RedisJobPayload(schemaId, RedisJobWireCodec.RAW, bytes);
    }

    /**
     * 业务作用：读取参数 Schema 标识。
     *
     * @return Schema 标识。
     */
    public String schemaId() {
        return schemaId;
    }

    /**
     * 业务作用：读取参数编码方式。
     *
     * @return 编码方式。
     */
    public RedisJobWireCodec codec() {
        return codec;
    }

    /**
     * 业务作用：读取隔离副本，避免外部改写正在持久化或执行的参数。
     *
     * @return 参数字节副本。
     */
    public byte[] bytes() {
        return bytes.clone();
    }

    /**
     * 业务作用：比较完整契约标识与参数内容，供幂等请求复验。
     *
     * @param other 待比较对象
     * @return 内容完全相同返回 true。
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof RedisJobPayload payload
                && schemaId.equals(payload.schemaId)
                && codec == payload.codec
                && Arrays.equals(bytes, payload.bytes);
    }

    /**
     * 业务作用：生成与内容比较规则一致的摘要值。
     *
     * @return 哈希值。
     */
    @Override
    public int hashCode() {
        return 31 * Objects.hash(schemaId, codec) + Arrays.hashCode(bytes);
    }
}
