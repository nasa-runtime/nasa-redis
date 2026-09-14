package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 业务作用：集中发布一个 Redis 消费来源的本地 admission、停止、失权和代次状态，供 Partition Task 无 I/O 复验。
 */
final class StreamSourceAuthority {

    private final AtomicLong generation = new AtomicLong();
    private final AtomicBoolean active = new AtomicBoolean();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final AtomicBoolean authorityLost = new AtomicBoolean();
    private volatile String holderOrConsumer;

    /**
     * 业务作用：在 Redis 来源取得新一代权威后发布可执行状态。
     *
     * @param identity RedisPartition holder 或 BOTH consumer epoch 身份
     * @return 本次新权威的单调 generation
     */
    long activate(String identity) {
        holderOrConsumer = Objects.requireNonNull(identity, "identity");
        authorityLost.set(false);
        stopping.set(false);
        active.set(true);
        return generation.incrementAndGet();
    }

    /**
     * 业务作用：冻结当前共享权威引用与期望代次，Task 执行时仍读取共享状态而非一次性 boolean。
     *
     * <p>参数说明: 无。
     *
     * @return 当前来源代次的执行快照
     */
    Snapshot snapshot() {
        return new Snapshot(this, generation.get(), holderOrConsumer);
    }

    /**
     * 业务作用：关闭新业务执行 admission。参数说明: 无。返回: 无返回值。
     */
    void beginStop() {
        stopping.set(true);
        active.set(false);
    }

    /**
     * 业务作用：在锁或 consumer epoch 失效时禁止迟到 Task 执行与确认。参数说明: 无。返回: 无返回值。
     */
    void loseAuthority() {
        authorityLost.set(true);
        active.set(false);
    }

    /**
     * 业务作用：报告来源当前是否允许新读取和 Task 执行。参数说明: 无。返回: 完整权威可用时为 true。
     */
    boolean isActive() {
        return active.get() && !stopping.get() && !authorityLost.get();
    }

    /**
     * 业务作用：让 Task 在执行前复验同一共享来源的 generation、身份与停止状态。
     *
     * @param current          共享来源引用
     * @param generation       提交时冻结的代次
     * @param holderOrConsumer 提交时冻结的身份
     */
    record Snapshot(StreamSourceAuthority current, long generation, String holderOrConsumer) {
        /**
         * 业务作用：复验快照仍对应当前完整权威。参数说明: 无。返回: 允许产生业务副作用时为 true。
         */
        boolean allowsExecution() {
            return current.generation.get() == generation
                    && current.isActive()
                    && Objects.equals(current.holderOrConsumer, holderOrConsumer);
        }
    }
}
