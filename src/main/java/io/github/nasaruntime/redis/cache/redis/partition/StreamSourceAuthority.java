package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 业务作用：原子发布一个 Redis 消费来源的身份、代次与准入状态，供 Partition Task 无 I/O 复验完整权威。
 */
final class StreamSourceAuthority {

    private final PartitionExecutionDomain domain;

    /** 业务作用：冻结来源的资源归属，失权和重获不改变归还路径。@param domain 原始域；返回: 未持权来源。 */
    StreamSourceAuthority(PartitionExecutionDomain domain) { this.domain = Objects.requireNonNull(domain); }

    /** 业务作用：为迟到任务和恢复定位原资源池。参数说明: 无。返回: 固定域。 */
    PartitionExecutionDomain domain() { return domain; }

    private final AtomicReference<State> state = new AtomicReference<>(new State(0L, null, false, false, false));

    /**
     * 业务作用：保存一次完整权威发布，防止快照组合来自不同代次的身份与准入标志。
     * @param generation 当前来源的单调代次
     * @param holderOrConsumer 本代次的 Redis holder 身份；尚未取得权威时为空
     * @param active 本代次是否开放本地准入
     * @param stopping 本代次是否进入停止流程
     * @param authorityLost 本代次是否已失去 Redis 权威
     */
    private record State(long generation, String holderOrConsumer, boolean active, boolean stopping, boolean authorityLost) {
        /** 业务作用：仅按同次发布的状态判断本地准入。参数说明: 无。返回: 已激活且未停止、未失权时为 true。 */
        boolean allowsExecution() { return active && !stopping && !authorityLost; }
    }

    /**
     * 业务作用：在 Redis 来源取得新一代权威后发布可执行状态。
     *
     * @param identity RedisPartition holder 身份
     * @return 本次新权威的单调 generation；身份为空或代次耗尽时拒绝发布
     */
    long activate(String identity) {
        Objects.requireNonNull(identity, "identity");
        // 新代次与开放准入必须在同一次 CAS 中可见；即使 holder 相同，旧任务也不能借重新激活恢复执行。
        return state.updateAndGet(previous -> new State(
                Math.incrementExact(previous.generation()), identity, true, false, false)).generation();
    }

    /**
     * 业务作用：冻结当前共享权威引用与期望代次，Task 执行时仍读取共享状态而非一次性 boolean。
     *
     * <p>参数说明: 无。
     *
     * @return 身份和代次来自同次原子发布的执行快照；准入仍由执行时的当前权威决定
     */
    Snapshot snapshot() {
        State observed = state.get();
        return new Snapshot(this, observed.generation(), observed.holderOrConsumer());
    }

    /**
     * 业务作用：关闭新业务执行 admission。参数说明: 无。返回: 无返回值，当前代次进入停止状态并保留已有失权结论。
     */
    void beginStop() {
        // 停止标志与关闭准入一并发布，保留本代次身份供已有责任收口，且不覆盖并发失权结论。
        state.updateAndGet(previous -> previous.stopping() ? previous : new State(
                previous.generation(), previous.holderOrConsumer(), false, true, previous.authorityLost()));
    }

    /**
     * 业务作用：在锁权威失效时禁止迟到 Task 执行与确认。参数说明: 无。返回: 无返回值，当前代次关闭准入并保留已有停止状态。
     */
    void loseAuthority() {
        // 失权立即关闭当前代次；CAS 重试保留并发停止状态，后续新代次必须由显式取得权威重新发布。
        state.updateAndGet(previous -> previous.authorityLost() ? previous : new State(
                previous.generation(), previous.holderOrConsumer(), false, previous.stopping(), true));
    }

    /**
     * 业务作用：报告来源当前是否允许新读取和 Task 执行。参数说明: 无。返回: 完整权威可用时为 true。
     */
    boolean isActive() {
        return state.get().allowsExecution();
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
         * 业务作用：核对消息坐标确属该冻结代次，阻止旧正文被新 Task 或恢复责任接收。
         * @param ref 读取时冻结的精确消息坐标
         * @return 来源对象与代次均匹配时为 true；不代表当前权威仍然有效
         */
        boolean owns(PartitionRecordRef ref) {
            return ref.authority() == current && ref.sourceGeneration() == generation;
        }

        /**
         * 业务作用：把迟到 Redis 失权响应限定在其原代次，避免撤销后继权威。
         * 参数说明: 无。
         * @return 原代次仍匹配时原子关闭准入并返回 true；来源已重获时返回 false，不改变新状态
         */
        boolean loseAuthority() {
            while (true) {
                State observed = current.state.get();
                // 旧请求的远端结论不能覆盖后来取得的 holder，即使两代 holder 字符串相同。
                if (observed.generation() != generation
                        || !Objects.equals(observed.holderOrConsumer(), holderOrConsumer)) return false;
                if (observed.authorityLost() || current.state.compareAndSet(observed, new State(
                        generation, holderOrConsumer, false, observed.stopping(), true))) return true;
            }
        }

        /**
         * 业务作用：复验快照仍对应当前完整权威。参数说明: 无。返回: 允许产生业务副作用时为 true。
         */
        boolean allowsExecution() {
            State observed = current.state.get();
            // 所有权威字段来自同一个发布值；健康检查期间发生状态迁移时，也不得交还过期执行许可。
            return observed.generation() == generation
                    && observed.allowsExecution()
                    && Objects.equals(observed.holderOrConsumer(), holderOrConsumer)
                    && current.domain.healthy()
                    && current.state.get() == observed;
        }
    }
}
