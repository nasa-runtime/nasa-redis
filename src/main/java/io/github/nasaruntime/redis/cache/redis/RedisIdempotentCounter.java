package io.github.nasaruntime.redis.cache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.ByteArrayCodec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.types.Expiration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * 业务作用：为 RedisProxy 计数命令建立同 slot 的短期业务凭证，并在一段 Lua 中完成判重、计数与凭证登记。
 * 该类不向业务暴露，公开合同由 RedisProxy 的 Idempotent 方法和结构化异常承载。
 */
@Slf4j
final class RedisIdempotentCounter {

    private static final byte[] EMPTY = new byte[0];
    private static final byte[] STR_INCR = ascii("STR_INCR");
    private static final byte[] STR_DECR = ascii("STR_DECR");
    private static final byte[] HASH_INCR = ascii("HASH_INCR");
    private static final byte[] HASH_DECR = ascii("HASH_DECR");
    private static final byte[] ZSET_INCR = ascii("ZSET_INCR");
    private static final byte[] FAMILY_STRING = {1};
    private static final byte[] FAMILY_HASH = {2};
    private static final byte[] FAMILY_ZSET = {3};
    private static final HexFormat HEX = HexFormat.of();
    private static final ThreadLocal<MessageDigest> SHA_256 = ThreadLocal.withInitial(RedisIdempotentCounter::newDigest);

    private final RedisProxy redisProxy;
    private final ScriptExecutor scriptExecutor;
    private final RedisIdempotentCounterMetrics metrics = new RedisIdempotentCounterMetrics();
    private volatile RedisIdempotentCounterProperties.Snapshot configured = new RedisIdempotentCounterProperties().snapshot();
    private volatile RuntimeLayout runtimeLayout;
    private Boolean hpexpireSupported;

    /**
     * 业务作用：把幂等计数内核绑定到一个 RedisProxy 及其私有字节脚本入口。
     *
     * @param redisProxy     目标 Redis 实例
     * @param scriptExecutor 私有字节脚本执行入口
     */
    RedisIdempotentCounter(RedisProxy redisProxy, ScriptExecutor scriptExecutor) {
        this.redisProxy = Objects.requireNonNull(redisProxy, "redisProxy must not be null");
        this.scriptExecutor = Objects.requireNonNull(scriptExecutor, "scriptExecutor must not be null");
    }

    /**
     * 业务作用：在首次使用前装入配置，并拒绝运行期改变已经固定的 Redis 账本布局。
     *
     * @param properties 全局幂等计数配置
     * 返回: 无返回值；布局已经启用后传入不同配置会拒绝。
     */
    synchronized void configure(RedisIdempotentCounterProperties properties) {
        RedisIdempotentCounterProperties.Snapshot next = Objects.requireNonNull(properties,
                "idempotent counter properties must not be null").snapshot();
        RuntimeLayout current = runtimeLayout;
        if (current != null && !current.config().equals(next)) {
            throw new IllegalStateException("idempotent counter layout cannot change while RedisProxy is running");
        }
        configured = next;
    }

    /**
     * 业务作用：按业务 nonce 原子自增字符串计数器；窗口内重放只返回首次结果。
     *
     * @param key   目标键
     * @param delta 增量
     * @param nonce 由稳定业务事件推导的幂等标识
     * @return 首次请求执行后的精确 int64 值。
     */
    long increment(String key, long delta, String nonce) {
        return executeLong(Operation.STRING_INCREMENT, key, EMPTY, ascii(Long.toString(delta)), nonce);
    }

    /**
     * 业务作用：按业务 nonce 原子自减字符串计数器；窗口内重放只返回首次结果。
     *
     * @param key   目标键
     * @param delta 减量
     * @param nonce 由稳定业务事件推导的幂等标识
     * @return 首次请求执行后的精确 int64 值。
     */
    long decrement(String key, long delta, String nonce) {
        return executeLong(Operation.STRING_DECREMENT, key, EMPTY, ascii(Long.toString(delta)), nonce);
    }

    /**
     * 业务作用：按业务 nonce 原子自增 HASH 字段；窗口内重放只返回首次结果。
     *
     * @param key     目标键
     * @param hashKey 目标字段
     * @param delta   增量
     * @param nonce   由稳定业务事件推导的幂等标识
     * @return 首次请求执行后的精确 int64 值。
     */
    long hashIncrement(String key, String hashKey, long delta, String nonce) {
        byte[] field = Objects.requireNonNull(redisProxy.getHashKeySerializer().serialize(
                Objects.requireNonNull(hashKey, "hashKey must not be null")), "serialized hashKey must not be null");
        return executeLong(Operation.HASH_INCREMENT, key, field, ascii(Long.toString(delta)), nonce);
    }

    /**
     * 业务作用：按业务 nonce 原子自减 HASH 字段；窗口内重放只返回首次结果。
     *
     * @param key     目标键
     * @param hashKey 目标字段
     * @param delta   减量；Long.MIN_VALUE 由服务端在首次请求路径拒绝
     * @param nonce   由稳定业务事件推导的幂等标识
     * @return 首次请求执行后的精确 int64 值。
     */
    long hashDecrement(String key, String hashKey, long delta, String nonce) {
        byte[] field = Objects.requireNonNull(redisProxy.getHashKeySerializer().serialize(
                Objects.requireNonNull(hashKey, "hashKey must not be null")), "serialized hashKey must not be null");
        return executeLong(Operation.HASH_DECREMENT, key, field, ascii(Long.toString(delta)), nonce);
    }

    /**
     * 业务作用：按业务 nonce 原子调整 ZSET 成员分值；成员沿用 RedisProxy 的 valueSerializer 字节身份。
     *
     * @param key    目标键
     * @param member 目标成员
     * @param delta  分值增量
     * @param nonce  由稳定业务事件推导的幂等标识
     * @return 首次请求执行后的分值。
     */
    double zsetIncrement(String key, Object member, double delta, String nonce) {
        byte[] memberBytes = Objects.requireNonNull(redisProxy.getValueSerializer().serialize(
                Objects.requireNonNull(member, "member must not be null")), "serialized member must not be null");
        String value = execute(Operation.ZSET_INCREMENT, key, memberBytes, ascii(Double.toString(delta)), nonce);
        return parseRedisDouble(value);
    }

    /**
     * 业务作用：取得当前 RedisProxy 的幂等计数观测容器。
     *
     * <p>参数说明: 无。
     *
     * @return 长期复用的线程安全指标容器。
     */
    RedisIdempotentCounterMetrics metrics() {
        return metrics;
    }

    /**
     * 业务作用：执行整数幂等计数并保持 int64 精度，协议值异常时拒绝静默折叠为零。
     *
     * @param operation   操作类型
     * @param key         目标键
     * @param member      字段字节；字符串计数器为空
     * @param delta       十进制增量字节
     * @param nonce       业务幂等标识
     * @return 首次请求执行后的精确 int64 值。
     */
    private long executeLong(Operation operation, String key, byte[] member, byte[] delta, String nonce) {
        String value = execute(operation, key, member, delta, nonce);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException error) {
            throw new IllegalStateException("idempotent integer script returned a non-int64 value", error);
        }
    }

    /**
     * 业务作用：派生同 slot 账本键并执行原子判重协议，成功返回首次值，拒绝则抛结构化异常。
     *
     * @param operation 操作类型
     * @param key       目标键
     * @param member    字段或成员的已序列化字节
     * @param delta     原生命令接受的十进制参数
     * @param nonce     业务幂等标识
     * @return 首次请求执行后的 Redis 字符串值。
     */
    private String execute(Operation operation, String key, byte[] member, byte[] delta, String nonce) {
        requireNonce(nonce);
        byte[] targetKey = Objects.requireNonNull(redisProxy.getKeySerializer().serialize(
                Objects.requireNonNull(key, "key must not be null")), "serialized key must not be null");
        RuntimeLayout layout = runtimeLayout();
        RedisIdempotentCounterProperties.Snapshot config = layout.config();

        int slot = SlotHash.getSlot(targetKey);
        String targetDigest = HEX.formatHex(digestRaw(targetKey));
        int shard = shard(operation.family(), member, config.ledgerShards());
        String ledgerBase = config.ledgerKeyPrefix() + ":{" + Assets.SLOT_TOKENS[slot] + "}:"
                + targetDigest + ":" + shard;
        byte[] recordField = digest(operation.family(), member, nonce.getBytes(StandardCharsets.UTF_8));

        byte[][] keys;
        String script;
        if (layout.mode() == RedisIdempotentCounterTtlMode.HASH_FIELD) {
            keys = new byte[][]{targetKey, serializeLedgerKey(ledgerBase, slot)};
            script = Assets.HFE_SCRIPT;
        } else {
            keys = new byte[config.bucketCount() + 1][];
            keys[0] = targetKey;
            for (int i = 0; i < config.bucketCount(); i++) {
                keys[i + 1] = serializeLedgerKey(ledgerBase + ":" + i, slot);
            }
            script = Assets.BUCKET_SCRIPT;
        }

        byte[][] args = new byte[][]{
                recordField,
                operation.command(),
                member,
                delta,
                ascii(Long.toString(config.nonceTtlMs())),
                ascii(Long.toString(config.bucketSpanMs()))
        };
        List<Object> result = scriptExecutor.execute(script, keys, args);
        if (result == null || result.size() != 3) {
            throw new IllegalStateException("idempotent counter script returned an invalid result");
        }
        RedisIdempotentCounterResultCode code;
        try {
            code = RedisIdempotentCounterResultCode.valueOf(String.valueOf(result.get(0)));
        } catch (RuntimeException error) {
            throw new IllegalStateException("idempotent counter script returned an unknown result code", error);
        }
        String value = result.get(1) == null ? "" : String.valueOf(result.get(1));
        String detail = result.get(2) == null ? "" : String.valueOf(result.get(2));
        return switch (code) {
            case APPLIED -> {
                metrics.recordApplied();
                yield value;
            }
            case APPLIED_TTL_MISSING -> {
                metrics.recordApplied();
                metrics.recordTtlMissing();
                log.error("[{}] idempotent counter credential TTL was not confirmed: {}", redisProxy.getQualifier(), detail);
                yield value;
            }
            case DUPLICATE -> {
                metrics.recordDuplicate();
                yield value;
            }
            case REJECTED_LEDGER_TYPE, REJECTED_OPERATION, REJECTED_COMMAND -> {
                metrics.recordRejected(code);
                throw new RedisIdempotentCounterException(code, detail);
            }
        };
    }

    /**
     * 业务作用：取得当前进程已经与共享 marker 对齐的固定账本布局；首次调用完成能力探测与竞争建标。
     *
     * <p>参数说明: 无。
     *
     * @return 当前 RedisProxy 的固定运行布局。
     */
    private RuntimeLayout runtimeLayout() {
        RuntimeLayout current = runtimeLayout;
        if (current != null) return current;
        synchronized (this) {
            current = runtimeLayout;
            if (current == null) runtimeLayout = current = resolveLayout(configured);
        }
        return current;
    }

    /**
     * 业务作用：以共享 marker 固定 TTL 布局，防止不同应用节点把同一 nonce 写入两套账本。
     * 已有 marker 优先；需要 HASH_FIELD 时必须重新确认全部 master 的 HPEXPIRE 能力。
     *
     * @param config 已校验的本地配置
     * @return 与 Redis 共享 marker 一致的运行布局。
     */
    private RuntimeLayout resolveLayout(RedisIdempotentCounterProperties.Snapshot config) {
        byte[] markerKey = Objects.requireNonNull(redisProxy.getKeySerializer().serialize(
                config.ledgerKeyPrefix() + ":layout:" + redisProxy.getQualifier()),
                "serialized layout marker key must not be null");
        String existing = readMarker(markerKey);
        if (existing != null) {
            RuntimeLayout layout = verifyMarker(existing, config);
            metrics.resolveLayout(layout.mode(), existing);
            return layout;
        }

        RedisIdempotentCounterTtlMode resolved = switch (config.ttlMode()) {
            case HASH_BUCKET -> RedisIdempotentCounterTtlMode.HASH_BUCKET;
            case HASH_FIELD -> {
                if (!allMastersSupportHpexpire()) {
                    throw new IllegalStateException("HASH_FIELD requires HPEXPIRE on every Redis master");
                }
                yield RedisIdempotentCounterTtlMode.HASH_FIELD;
            }
            case AUTO -> allMastersSupportHpexpire()
                    ? RedisIdempotentCounterTtlMode.HASH_FIELD
                    : RedisIdempotentCounterTtlMode.HASH_BUCKET;
        };
        String candidate = markerValue(resolved, config);
        byte[] candidateBytes = candidate.getBytes(StandardCharsets.UTF_8);
        redisProxy.getRedisTemplate().execute(connection -> connection.stringCommands().set(markerKey, candidateBytes,
                Expiration.persistent(), RedisStringCommands.SetOption.ifAbsent()), true);
        String actual = readMarker(markerKey);
        if (actual == null) throw new IllegalStateException("idempotent counter layout marker was not persisted");
        RuntimeLayout layout = verifyMarker(actual, config);
        metrics.resolveLayout(layout.mode(), actual);
        log.info("[{}] idempotent counter layout={} nonceTtlMs={} ledgerShards={}", redisProxy.getQualifier(),
                layout.mode(), config.nonceTtlMs(), config.ledgerShards());
        return layout;
    }

    /**
     * 业务作用：解析并复验共享布局；配置或服务端能力不一致时停止进入资金副作用路径。
     *
     * @param marker Redis 中保存的布局指纹
     * @param config 当前节点配置
     * @return 通过复验的运行布局。
     */
    private RuntimeLayout verifyMarker(String marker, RedisIdempotentCounterProperties.Snapshot config) {
        String[] parts = marker.split("\\|", -1);
        if (parts.length != 5 || !"1".equals(parts[0])) {
            throw new IllegalStateException("unsupported idempotent counter layout marker");
        }
        RedisIdempotentCounterTtlMode mode;
        long nonceTtl;
        long bucketSpan;
        int shards;
        try {
            mode = RedisIdempotentCounterTtlMode.valueOf(parts[1]);
            nonceTtl = Long.parseLong(parts[2]);
            bucketSpan = Long.parseLong(parts[3]);
            shards = Integer.parseInt(parts[4]);
        } catch (RuntimeException error) {
            throw new IllegalStateException("invalid idempotent counter layout marker", error);
        }
        if (mode == RedisIdempotentCounterTtlMode.AUTO) {
            throw new IllegalStateException("idempotent counter layout marker must contain a resolved TTL mode");
        }
        if (nonceTtl != config.nonceTtlMs() || bucketSpan != config.bucketSpanMs()
                || shards != config.ledgerShards()) {
            throw new IllegalStateException("idempotent counter configuration does not match the shared layout marker");
        }
        if (config.ttlMode() != RedisIdempotentCounterTtlMode.AUTO && config.ttlMode() != mode) {
            throw new IllegalStateException("idempotent counter ttlMode does not match the shared layout marker");
        }
        // 字段级布局一旦由其它节点固定，本节点必须先证明自己连接到的全部 master 都具备相同命令能力。
        if (mode == RedisIdempotentCounterTtlMode.HASH_FIELD && !allMastersSupportHpexpire()) {
            throw new IllegalStateException("shared HASH_FIELD layout requires HPEXPIRE on every Redis master");
        }
        return new RuntimeLayout(config, mode);
    }

    /**
     * 业务作用：读取原始字符串 marker，不经过业务 valueSerializer，保证不同 JSON 配置的节点能共享布局。
     *
     * @param markerKey marker 的已序列化键
     * @return marker 文本；不存在时为 null。
     */
    private String readMarker(byte[] markerKey) {
        byte[] value = redisProxy.getRedisTemplate().execute(
                connection -> connection.stringCommands().get(markerKey), true);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：取得本 RedisProxy 对全部 master 的固定 HPEXPIRE 探测结论，避免布局初始化阶段重复建立探测连接。
     * 结果不完整或探测失败时按不支持处理：AUTO 保守选择整桶回收，显式 HASH_FIELD 停止启用。
     *
     * <p>参数说明: 无。
     *
     * @return 每一个 master 都确认支持时返回 true。
     */
    private boolean allMastersSupportHpexpire() {
        if (hpexpireSupported != null) return hpexpireSupported;
        return hpexpireSupported = probeAllMastersHpexpire();
    }

    /**
     * 业务作用：向当前拓扑的每个 master 发出一次 HPEXPIRE 能力查询，任一结果缺失都返回不支持。
     *
     * <p>参数说明: 无。
     *
     * @return 完整拓扑全部确认支持时为 true。
     */
    private boolean probeAllMastersHpexpire() {
        if (!(redisProxy.getRedisTemplate().getConnectionFactory() instanceof LettuceConnectionFactory factory)) {
            metrics.recordProbeFailure();
            return false;
        }
        try {
            if (factory.getNativeClient() instanceof RedisClusterClient client) {
                try (StatefulRedisClusterConnection<byte[], byte[]> connection = client.connect(ByteArrayCodec.INSTANCE)) {
                    List<RedisClusterNode> masters = new ArrayList<>();
                    for (RedisClusterNode node : connection.getPartitions()) {
                        if (node.getRole() != null && node.getRole().isUpstream() && !node.hasNoSlots()) masters.add(node);
                    }
                    if (masters.isEmpty()) {
                        metrics.recordProbeFailure();
                        return false;
                    }
                    for (RedisClusterNode node : masters) {
                        RedisClusterCommands<byte[], byte[]> commands = connection.sync().getConnection(node.getNodeId());
                        if (!supportsHpexpire(commands)) return false;
                    }
                    return true;
                }
            }
            if (factory.getNativeClient() instanceof RedisClient client) {
                try (StatefulRedisConnection<byte[], byte[]> connection = client.connect(ByteArrayCodec.INSTANCE)) {
                    return supportsHpexpire(connection.sync());
                }
            }
        } catch (RuntimeException error) {
            metrics.recordProbeFailure();
            log.warn("[{}] HPEXPIRE capability probe was incomplete; HASH_BUCKET will be used when AUTO is configured",
                    redisProxy.getQualifier(), error);
            return false;
        }
        metrics.recordProbeFailure();
        return false;
    }

    /**
     * 业务作用：判断一个确定 Redis 节点的 COMMAND INFO 是否包含 HPEXPIRE 定义。
     *
     * @param commands 节点级服务端命令入口
     * @return 节点确认支持 HPEXPIRE 时返回 true。
     */
    private static boolean supportsHpexpire(RedisServerCommands<byte[], byte[]> commands) {
        List<Object> info = commands.commandInfo("HPEXPIRE");
        return info != null && !info.isEmpty() && info.getFirst() != null;
    }

    /**
     * 业务作用：生成 marker 的跨节点稳定协议文本。
     *
     * @param mode   已解析 TTL 模式
     * @param config 本地配置
     * @return marker 协议文本。
     */
    private static String markerValue(RedisIdempotentCounterTtlMode mode,
                                      RedisIdempotentCounterProperties.Snapshot config) {
        return "1|" + mode.name() + "|" + config.nonceTtlMs() + "|" + config.bucketSpanMs()
                + "|" + config.ledgerShards();
    }

    /**
     * 业务作用：序列化账本键并复验其 Redis Cluster slot，派生算法异常时在发命令前停止。
     *
     * @param key          账本文本键
     * @param expectedSlot 目标业务键的实际 slot
     * @return 已序列化且同 slot 的账本键。
     */
    private byte[] serializeLedgerKey(String key, int expectedSlot) {
        byte[] value = Objects.requireNonNull(redisProxy.getKeySerializer().serialize(key),
                "serialized ledger key must not be null");
        if (SlotHash.getSlot(value) != expectedSlot) {
            throw new IllegalStateException("derived idempotent ledger key is not in the target Redis slot");
        }
        return value;
    }

    /**
     * 业务作用：按结构类型与字段身份选择固定 ledger shard；nonce 不参与，使同一逻辑计数器的历史桶可一次查询。
     *
     * @param family 结构类型
     * @param member 字段或成员字节
     * @param shards shard 数量
     * @return 从零开始的 shard 下标。
     */
    private static int shard(byte[] family, byte[] member, int shards) {
        byte[] value = digest(family, member);
        int hash = (value[0] & 0xff) << 24 | (value[1] & 0xff) << 16
                | (value[2] & 0xff) << 8 | value[3] & 0xff;
        return hash & (shards - 1);
    }

    /**
     * 业务作用：以长度前缀组合多个二进制身份段，防止字段、成员或 nonce 内容形成拼接歧义。
     *
     * @param parts 二进制身份段
     * @return SHA-256 原始 32 字节摘要。
     */
    private static byte[] digest(byte[]... parts) {
        MessageDigest digest = SHA_256.get();
        digest.reset();
        for (byte[] part : parts) {
            updateLength(digest, part.length);
            digest.update(part);
        }
        return digest.digest();
    }

    /**
     * 业务作用：对目标键序列化字节直接计算 SHA-256，与跨语言账本键规范保持一致。
     *
     * @param value 目标键序列化字节
     * @return SHA-256 原始 32 字节摘要。
     */
    private static byte[] digestRaw(byte[] value) {
        MessageDigest digest = SHA_256.get();
        digest.reset();
        return digest.digest(value);
    }

    /**
     * 业务作用：解析 Redis 浮点协议；Redis 使用 inf/-inf，而 Java 使用 Infinity/-Infinity。
     *
     * @param value Redis 返回的浮点文本
     * @return 对应 double，包括正负无穷。
     */
    private static double parseRedisDouble(String value) {
        if ("inf".equalsIgnoreCase(value) || "+inf".equalsIgnoreCase(value)) return Double.POSITIVE_INFINITY;
        if ("-inf".equalsIgnoreCase(value)) return Double.NEGATIVE_INFINITY;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException error) {
            throw new IllegalStateException("idempotent ZSET script returned a non-numeric value", error);
        }
    }

    /**
     * 业务作用：按大端 int32 写入摘要段长度，使跨语言实现得到相同幂等身份。
     *
     * @param digest 摘要器
     * @param length 当前身份段字节数
     * 返回: 无返回值。
     */
    private static void updateLength(MessageDigest digest, int length) {
        digest.update((byte) (length >>> 24));
        digest.update((byte) (length >>> 16));
        digest.update((byte) (length >>> 8));
        digest.update((byte) length);
    }

    /**
     * 业务作用：生成覆盖全部 16384 个 Redis slot 的确定 ASCII token 表，使任意历史键都能派生同 slot sidecar。
     *
     * <p>参数说明: 无。
     *
     * @return 按 slot 下标排列的 canonical token。
     */
    private static String[] createSlotTokens() {
        String[] tokens = new String[SlotHash.SLOT_COUNT];
        int remaining = tokens.length;
        long candidate = 0;
        while (remaining > 0) {
            String token = "s" + Long.toString(candidate++, 36);
            int slot = SlotHash.getSlot(token.getBytes(StandardCharsets.US_ASCII));
            if (tokens[slot] == null) {
                tokens[slot] = token;
                remaining--;
            }
        }
        return tokens;
    }

    /**
     * 业务作用：读取随 jar 发布的 Lua 文本，首次进入幂等计数时缺失资源立即停止且不产生资金副作用。
     *
     * @param path classpath 资源路径
     * @return UTF-8 Lua 文本。
     */
    private static String loadScript(String path) {
        try (InputStream input = RedisIdempotentCounter.class.getResourceAsStream(path)) {
            if (input == null) throw new IllegalStateException("Lua resource not found: " + path);
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException error) {
            throw new IllegalStateException("cannot read Lua resource: " + path, error);
        }
    }

    /**
     * 业务作用：建立当前线程复用的 SHA-256 摘要器，减少高频结算路径的对象创建。
     *
     * <p>参数说明: 无。
     *
     * @return 新建的 SHA-256 摘要器。
     */
    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is not available", error);
        }
    }

    /**
     * 业务作用：按 Redis 协议把固定控制参数转成 ASCII 字节。
     *
     * @param value 控制参数
     * @return ASCII 字节。
     */
    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * 业务作用：拒绝会让公开 Idempotent 方法悄然退化成普通计数的空 nonce。
     *
     * @param nonce 业务幂等标识
     * 返回: 无返回值；null、空串或全空白会拒绝。
     */
    private static void requireNonce(String nonce) {
        if (nonce == null || nonce.isBlank()) throw new IllegalArgumentException("nonce must not be blank");
    }

    @FunctionalInterface
    interface ScriptExecutor {
        /**
         * 业务作用：通过 RedisProxy 私有字节入口执行幂等脚本，保留二进制字段与成员身份。
         *
         * @param script Lua 文本
         * @param keys   已序列化 Redis 键
         * @param args   已序列化脚本参数
         * @return 三元素结果数组。
         */
        List<Object> execute(String script, byte[][] keys, byte[][] args);
    }

    private enum Operation {
        STRING_INCREMENT(FAMILY_STRING, STR_INCR),
        STRING_DECREMENT(FAMILY_STRING, STR_DECR),
        HASH_INCREMENT(FAMILY_HASH, HASH_INCR),
        HASH_DECREMENT(FAMILY_HASH, HASH_DECR),
        ZSET_INCREMENT(FAMILY_ZSET, ZSET_INCR);

        private final byte[] family;
        private final byte[] command;

        /**
         * 业务作用：绑定幂等身份所属结构类型与首次请求实际执行的 Redis 操作。
         *
         * @param family  不区分方向的逻辑计数器类型
         * @param command Lua 原生命令分支标识
         */
        Operation(byte[] family, byte[] command) {
            this.family = family;
            this.command = command;
        }

        /**
         * 业务作用：取得不包含方向的结构身份，使同一 nonce 在增减方向漂移时仍命中首次结果。
         *
         * <p>参数说明: 无。
         *
         * @return 结构身份字节。
         */
        byte[] family() {
            return family;
        }

        /**
         * 业务作用：取得首次请求需要执行的 Lua 操作分支。
         *
         * <p>参数说明: 无。
         *
         * @return Lua 操作标识字节。
         */
        byte[] command() {
            return command;
        }
    }

    private record RuntimeLayout(RedisIdempotentCounterProperties.Snapshot config,
                                 RedisIdempotentCounterTtlMode mode) {
    }

    /**
     * 业务作用：延迟加载幂等脚本和 canonical slot token，未使用幂等计数的应用不承担初始化与常驻内存成本。
     */
    private static final class Assets {
        private static final String HFE_SCRIPT = loadScript("/lua/idempotent_counter_hfe.lua");
        private static final String BUCKET_SCRIPT = loadScript("/lua/idempotent_counter_bucket.lua");
        private static final String[] SLOT_TOKENS = createSlotTokens();
    }
}
