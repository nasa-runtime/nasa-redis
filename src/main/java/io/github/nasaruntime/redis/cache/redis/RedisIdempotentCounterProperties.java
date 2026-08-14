package io.github.nasaruntime.redis.cache.redis;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Objects;

/**
 * 业务作用：配置 RedisProxy nonce 幂等计数账本的命名空间、保证窗口与回收布局。
 * 这些参数共同定义 Redis 中的持久布局，同一 RedisProxy 集群运行期间必须保持一致。
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "nasa.redis-proxy.idempotent-counter")
public class RedisIdempotentCounterProperties {

    public static final long DEFAULT_NONCE_TTL_MS = 7L * 24 * 60 * 60 * 1000;
    public static final long DEFAULT_BUCKET_SPAN_MS = 24L * 60 * 60 * 1000;
    static final int MAX_LEDGER_SHARDS = 4096;
    static final int MAX_BUCKET_COUNT = 512;

    private String ledgerKeyPrefix = "rpidem";
    private long nonceTtlMs = DEFAULT_NONCE_TTL_MS;
    private RedisIdempotentCounterTtlMode ttlMode = RedisIdempotentCounterTtlMode.AUTO;
    private long bucketSpanMs = DEFAULT_BUCKET_SPAN_MS;
    private int ledgerShards = 256;

    /**
     * 业务作用：校验并冻结一份可用于账本寻址的配置快照。
     * 快照用于识别运行期布局漂移，避免同一 nonce 被不同节点写进两套账本。
     *
     * <p>参数说明: 无。
     *
     * @return 已校验的不可变配置快照。
     */
    Snapshot snapshot() {
        String prefix = Objects.requireNonNull(ledgerKeyPrefix, "ledgerKeyPrefix must not be null").trim();
        if (prefix.isEmpty()) throw new IllegalArgumentException("ledgerKeyPrefix must not be blank");
        if (prefix.length() > 128) {
            throw new IllegalArgumentException("ledgerKeyPrefix must be at most 128 characters");
        }
        for (int i = 0; i < prefix.length(); i++) {
            char value = prefix.charAt(i);
            boolean accepted = value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
                    || value >= '0' && value <= '9' || value == '.' || value == '_' || value == ':' || value == '-';
            if (!accepted) {
                throw new IllegalArgumentException(
                        "ledgerKeyPrefix may contain only ASCII letters, digits, '.', '_', ':', and '-'");
            }
        }
        if (nonceTtlMs < 1) throw new IllegalArgumentException("nonceTtlMs must be greater than zero");
        if (bucketSpanMs < 1) throw new IllegalArgumentException("bucketSpanMs must be greater than zero");
        if (ledgerShards < 1 || ledgerShards > MAX_LEDGER_SHARDS
                || (ledgerShards & (ledgerShards - 1)) != 0) {
            throw new IllegalArgumentException("ledgerShards must be a power of two between 1 and " + MAX_LEDGER_SHARDS);
        }
        long completeSpans = (nonceTtlMs - 1) / bucketSpanMs;
        if (completeSpans > MAX_BUCKET_COUNT - 2L) {
            throw new IllegalArgumentException("nonceTtlMs and bucketSpanMs require more than "
                    + MAX_BUCKET_COUNT + " ledger buckets");
        }
        int bucketCount = Math.toIntExact(completeSpans + 2);
        return new Snapshot(prefix, nonceTtlMs, Objects.requireNonNull(ttlMode, "ttlMode must not be null"),
                bucketSpanMs, ledgerShards, bucketCount);
    }

    record Snapshot(String ledgerKeyPrefix, long nonceTtlMs, RedisIdempotentCounterTtlMode ttlMode,
                    long bucketSpanMs, int ledgerShards, int bucketCount) {
    }
}
