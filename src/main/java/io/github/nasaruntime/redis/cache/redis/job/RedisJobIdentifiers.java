package io.github.nasaruntime.redis.cache.redis.job;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * 业务作用：按跨语言固定字节规范生成任务、运行、能力和分片标识。
 */
public final class RedisJobIdentifiers {

    private static final HexFormat HEX = HexFormat.of();

    private RedisJobIdentifiers() {}

    /**
     * 业务作用：生成自动调度 Run 的稳定幂等标识。
     *
     * @param namespace     命名空间
     * @param jobName       任务名
     * @param logicalFireAt 逻辑触发时刻
     * @param triggerType   触发类型
     * @return 32 位小写十六进制标识。
     */
    public static String scheduledRunId(String namespace, String jobName, long logicalFireAt, String triggerType) {
        return digest128(namespace, jobName, Long.toString(logicalFireAt), triggerType);
    }

    /**
     * 业务作用：生成手工触发 Run 的稳定幂等标识，观测时刻不参与计算。
     *
     * @param namespace 命名空间
     * @param jobName   任务名
     * @param requestId 调用方幂等请求标识
     * @return 32 位小写十六进制标识。
     */
    public static String manualRunId(String namespace, String jobName, String requestId) {
        return digest128(namespace, jobName, "MANUAL", requestId);
    }

    /**
     * 业务作用：生成根 attempt 唯一对应的 Fanout 标识，使网络重试采用同一批次。
     *
     * @param rootRunId   根 Run 标识
     * @param rootAttempt 根 attempt
     * @return 32 位小写十六进制标识。
     */
    public static String fanoutId(String rootRunId, int rootAttempt) {
        return digest128(rootRunId, Integer.toString(rootAttempt));
    }

    /**
     * 业务作用：生成不随目标重分配变化的 Fanout shard 标识。
     *
     * @param fanoutId Fanout 标识
     * @param seq      稳定分片序号
     * @return 32 位小写十六进制标识。
     */
    public static String shardRunId(String fanoutId, long seq) {
        return digest128(fanoutId, Long.toString(seq));
    }

    /**
     * 业务作用：生成完整 Worker 摘要作为 Stream 路由键，避免截断碰撞共享消费通道。
     *
     * @param workerName Worker 能力名
     * @return 64 位小写十六进制摘要。
     */
    public static String workerKey(String workerName) {
        return digest256(workerName);
    }

    /**
     * 业务作用：计算任务定义的规范摘要，用于相同修订号的冲突检测。
     *
     * @param definition 任务定义
     * @return 64 位小写十六进制摘要。
     */
    public static String definitionDigest(RedisJobDefinition definition) {
        return digest256(
                definition.name(), definition.workerName(), definition.trigger().name(),
                definition.scheduleType().name(), definition.cron(), definition.zone().getId(),
                Long.toString(definition.intervalMs()), definition.concurrency().name(),
                definition.misfire().name(), Long.toString(definition.timeoutMs()),
                Integer.toString(definition.maxAttempts()), Long.toString(definition.retryDelayMs()),
                Long.toString(definition.contractRevision()), definition.schemaId(),
                definition.codecs().stream().map(Enum::name).sorted().reduce("", (a, b) -> a + "," + b),
                Long.toString(definition.fanoutReceiptTimeoutMs()),
                Integer.toString(definition.fanoutReceiptMaxRetries()),
                definition.fanoutFailurePolicy().name());
    }

    /**
     * 业务作用：生成固定 128 位协议标识。
     *
     * @param fields 按协议顺序排列的字段
     * @return 32 位小写十六进制标识。
     */
    private static String digest128(String... fields) {
        byte[] digest = digest(fields);
        return HEX.formatHex(digest, 0, 16);
    }

    /**
     * 业务作用：生成完整 SHA-256 协议摘要。
     *
     * @param fields 按协议顺序排列的字段
     * @return 64 位小写十六进制摘要。
     */
    private static String digest256(String... fields) {
        return HEX.formatHex(digest(fields));
    }

    /**
     * 业务作用：用四字节网络序长度前缀编码 UTF-8 字段，避免简单拼接产生歧义。
     *
     * @param fields 按协议顺序排列的字段
     * @return SHA-256 原始字节。
     */
    private static byte[] digest(String... fields) {
        try {
            ByteArrayOutputStream canonical = new ByteArrayOutputStream();
            for (String field : fields) {
                byte[] bytes = (field == null ? "" : field).getBytes(StandardCharsets.UTF_8);
                canonical.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                canonical.writeBytes(bytes);
            }
            return MessageDigest.getInstance("SHA-256").digest(canonical.toByteArray());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
