package io.github.nasaruntime.redis.cache.redis.job;

import java.util.UUID;

/**
 * 业务作用：把执行器心跳的逻辑请求载荷与 revision 证据绑定，避免重发跨越状态变化后吞掉新状态。
 */
final class RedisJobHeartbeatAuthority {

    private long confirmedRevision;
    private Request pending;

    /**
     * 业务作用：建立尚未采用服务端 revision 的心跳权威账本。
     *
     * <p>参数说明: 无。
     *
     * <p>返回：创建待由能力登记结果初始化的账本。
     */
    RedisJobHeartbeatAuthority() {
    }

    /**
     * 业务作用：记录一条不可变逻辑心跳，重发期间必须保持请求 ID、状态、在途数和预期 revision 一致。
     *
     * @param requestId        逻辑请求 ID
     * @param state            该请求发布的执行器状态
     * @param inflight         该请求发布的在途执行数
     * @param expectedRevision 请求发起前最后确认的 revision
     */
    record Request(String requestId, String state, int inflight, long expectedRevision) {

        /**
         * 业务作用：判断已确认请求是否已经发布本次调用要求的执行器状态。
         *
         * @param desiredState 当前要发布的执行器状态
         * @return 状态一致时返回 true；状态迁移尚未发布时返回 false。
         */
        boolean matchesState(String desiredState) {
            return state.equals(desiredState);
        }
    }

    /**
     * 业务作用：承载一次心跳脚本调用的状态码和服务端 revision，供权威账本封闭解释。
     *
     * @param code     心跳脚本返回的状态码
     * @param revision 心跳脚本返回的 revision
     */
    record Confirmation(String code, String revision) {
    }

    /**
     * 业务作用：把冻结的逻辑心跳交给 Redis 交换边界，并返回服务端权威确认。
     */
    @FunctionalInterface
    interface Exchange {

        /**
         * 业务作用：发送或重发一条不可变逻辑心跳，不在交换层改写其状态和 revision 证据。
         *
         * @param request 已冻结的逻辑心跳
         * @return Redis 心跳脚本的状态码和 revision；传输或协议失败时抛出异常。
         */
        Confirmation send(Request request);
    }

    /**
     * 业务作用：验证能力登记协议并采用服务端权威 revision，拒绝以失败回包初始化心跳账本。
     *
     * <p>返回：无返回值；成功时后续心跳从该 revision 建立新逻辑请求，失败状态或非法 revision
     * 抛出异常并保持原账本。
     *
     * @param code     能力登记脚本返回的状态码
     * @param revision 能力登记脚本返回的 revision
     */
    void registered(String code, String revision) {
        if (!"OK".equals(code)) {
            throw new IllegalStateException("RedisJob capability rejected: " + code);
        }
        long parsedRevision = Long.parseLong(revision);
        // 能力登记已建立新的服务端权威，旧 pending 属于此前 revision 域，不得带入后续续租。
        confirmedRevision = parsedRevision;
        pending = null;
    }

    /**
     * 业务作用：返回尚未确认的原逻辑请求，或为当前状态快照建立一条新请求。
     *
     * @param state    当前要发布的执行器状态
     * @param inflight 当前要发布的在途执行数
     * @return 未确认请求存在时原样返回；否则返回绑定当前载荷与已确认 revision 的新请求。
     */
    Request request(String state, int inflight) {
        // 结局未知时只能原样重发；替换载荷会让服务端去重返回旧结果，却让调用方误认新状态已发布。
        if (pending != null) return pending;
        pending = new Request(
                UUID.randomUUID().toString().replace("-", ""),
                state,
                inflight,
                confirmedRevision);
        return pending;
    }

    /**
     * 业务作用：解决未决心跳的唯一结局，并保证本次调用要求的状态迁移在返回前已经发布。
     *
     * <p>在途数是周期观测值：未决请求确认后即使采样已变化，也留给下一周期以新请求发布，避免一次
     * 周期因并发任务起止产生额外 Redis 往返。状态迁移是准入门禁，必须在同一次调用内追加发布。</p>
     *
     * <p>返回: 无返回值；服务端连续确认当前状态后完成，传输失败、记录过期、权威变化或协议不完整时抛出异常。
     *
     * @param desiredState    本次调用必须发布的执行器状态
     * @param desiredInflight 本次调用采样的在途执行数
     * @param exchange        Redis 心跳交换边界
     */
    void heartbeat(String desiredState, int desiredInflight, Exchange exchange) {
        Request request = request(desiredState, desiredInflight);
        Confirmation confirmation = exchange.send(request);
        confirmed(request, confirmation.code(), confirmation.revision());
        if (!request.matchesState(desiredState)) {
            // 旧请求取得唯一结局后仍未发布目标状态，必须用新 ID 完成状态迁移再开放调用方后续流程。
            Request stateChange = request(desiredState, desiredInflight);
            confirmation = exchange.send(stateChange);
            confirmed(stateChange, confirmation.code(), confirmation.revision());
        }
    }

    /**
     * 业务作用：封闭解释心跳协议回包，仅采用当前逻辑请求恰好推进一次的唯一确认。
     *
     * <p>返回：无返回值；成功时清除 pending 请求，记录缺失、权威陈旧、未知状态、非法或跳号
     * revision 均抛出异常并保留原请求证据。
     *
     * @param request  本次发送或重发的逻辑请求
     * @param code     心跳脚本返回的状态码
     * @param revision 心跳脚本返回的确认 revision
     */
    void confirmed(Request request, String code, String revision) {
        if ("NOT_FOUND".equals(code)) {
            throw new IllegalStateException("RedisJob executor registration expired");
        }
        if ("STALE_AUTHORITY".equals(code)) {
            throw new IllegalStateException("RedisJob heartbeat authority is stale");
        }
        if (!"OK".equals(code)) {
            throw new IllegalStateException("RedisJob heartbeat rejected: " + code);
        }
        if (pending != request) {
            throw new IllegalStateException("RedisJob heartbeat confirmation does not match pending request");
        }
        long nextRevision = Math.addExact(request.expectedRevision(), 1L);
        long parsedRevision = Long.parseLong(revision);
        if (parsedRevision != nextRevision) {
            throw new IllegalStateException("RedisJob heartbeat revision is not contiguous");
        }
        // 连续性成立后才能发布本地权威并释放请求 ID；失败路径保留 pending 供同载荷重发。
        this.confirmedRevision = parsedRevision;
        pending = null;
    }
}
