package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 业务作用：以 planId 与 Partition 有效 hash 为边界维护本地 ordered key 门禁，业务成功到 ACK 明确前禁止后继执行。
 */
final class OrderedKeyCoordinator {

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<OrderedKeyId, KeyGate> gates = new HashMap<>();
    private final Map<OrderedKeyId, Long> gateSinceNanos = new HashMap<>();
    private final Map<OrderedKeyId, Long> consecutiveFailures = new HashMap<>();
    private final int maxBlockedKeysPerSource;
    private final int maxDeferredIdsPerKey;
    private long sequence;

    /**
     * 业务作用：创建带来源级 key 上限和单 key deferred 坐标上限的 ordered 门禁域。
     *
     * @param maxBlockedKeysPerSource 单来源最多持有的 ordered gate 数
     * @param maxDeferredIdsPerKey    单 gate 最多登记的 deferred Redis id 数
     *                                返回: 初始没有 gate 的协调器。
     */
    OrderedKeyCoordinator(int maxBlockedKeysPerSource, int maxDeferredIdsPerKey) {
        if (maxBlockedKeysPerSource < 1 || maxDeferredIdsPerKey < 1) {
            throw new IllegalArgumentException("ordered gate capacities must be greater than zero");
        }
        this.maxBlockedKeysPerSource = maxBlockedKeysPerSource;
        this.maxDeferredIdsPerKey = maxDeferredIdsPerKey;
    }

    /**
     * 业务作用：原子取得 key 执行权或把完整 Redis 坐标登记为 deferred，避免观察与登记之间丢失后继。
     *
     * @param planId    消费计划标识
     * @param hash      Partition 真实入口使用的有效 hash
     * @param authority 来源共享引用与期望所有权代次
     * @param records   当前执行单元覆盖的 Redis 坐标
     * @return acquired 时携带唯一 GateToken，blocked 表示现有 gate 阻挡，authorityStale 表示来源已失权且未写入
     */
    GateReservation reserve(long planId,
                            int hash,
                            StreamSourceAuthority.Snapshot authority,
                            List<PartitionRecordRef> records) {
        OrderedKeyId key = new OrderedKeyId(planId, hash);
        lock.lock();
        try {
            // 权威复验与 gate 发布共用同一把锁：失权扫描要么看到本次发布，要么本次发布观察到失权并保持无副作用。
            if (!authority.allowsExecution()) return GateReservation.staleAuthority();
            KeyGate current = gates.get(key);
            if (current == null) {
                ensureSourceGateCapacity(authority);
                long nextSequence = ++sequence;
                if (nextSequence <= 0) throw new IllegalStateException("ordered gate sequence 已耗尽");
                GateToken token = new GateToken(
                        key, authority.current(), authority.generation(), nextSequence);
                gateSinceNanos.put(key, System.nanoTime());
                consecutiveFailures.put(key, 0L);
                gates.put(key, new KeyGate(
                        token,
                        BusinessPhase.EXECUTING,
                        CommitPhase.NONE,
                        GateHoldReason.NONE,
                        List.copyOf(records),
                        new ArrayList<>(),
                        Set.of(),
                        Set.of()));
                return GateReservation.acquired(token);
            }
            // Task 结果尚待物理 record 合并时不能再次取得 gate 代次，后继仍须等待当前 head 收口。
            if ((current.business() == BusinessPhase.BLOCKED
                    || current.business() == BusinessPhase.INVALIDATING)
                    && current.commit() == CommitPhase.NONE
                    && current.holdReason() != GateHoldReason.PROXY_RECORD_DECISION
                    && current.pendingCommitCoordinates().isEmpty()
                    && waitingBelongsToFailedRecords(current)
                    && sameHead(current.active(), records)) {
                List<PartitionRecordRef> remaining = remainingAfterReservation(
                        current.active(), current.deferred(), records);
                long nextSequence = ++sequence;
                GateToken token = new GateToken(
                        key, authority.current(), authority.generation(), nextSequence);
                gates.put(key, new KeyGate(
                        token,
                        BusinessPhase.EXECUTING,
                        CommitPhase.NONE,
                        GateHoldReason.NONE,
                        List.copyOf(records),
                        remaining,
                        Set.of(),
                        Set.of()));
                return GateReservation.acquired(token);
            }
            // 执行、等待其它 field 或确认中的坐标都已有人负责，不能把自己再次登记为自己的后继。
            List<PartitionRecordRef> additions = records.stream()
                    .filter(candidate -> current.active().stream()
                            .noneMatch(existing -> sameCoordinate(existing, candidate)))
                    .filter(candidate -> current.proxyWaitingCoordinates().stream()
                            .noneMatch(existing -> sameCoordinate(existing, candidate)))
                    .filter(candidate -> current.pendingCommitCoordinates().stream()
                            .noneMatch(existing -> sameCoordinate(existing, candidate)))
                    .filter(candidate -> current.deferred().stream()
                            .noneMatch(existing -> sameCoordinate(existing, candidate)))
                    .toList();
            if (!additions.isEmpty() && !containsSource(current, authority)) {
                ensureSourceGateCapacity(authority);
            }
            if (current.deferred().size() + additions.size() > maxDeferredIdsPerKey) {
                throw new OrderedGateCapacityException("ordered key deferred-id capacity exhausted");
            }
            current.deferred().addAll(additions);
            return GateReservation.blocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：复验 Task 仍持有当前 key 的唯一执行代次，旧 owner 或迟到 Task 不得产生业务副作用。
     *
     * @param token Task 注册时取得的门禁令牌
     * @return token 仍对应 EXECUTING gate 时返回 true
     */
    boolean ownsExecution(GateToken token) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            return gate != null
                    && gate.token().equals(token)
                    && gate.business() == BusinessPhase.EXECUTING
                    && gate.commit() == CommitPhase.NONE;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：聚合 ordered gate 阶段与坐标数量，观测结果不包含有效 hash、业务 key 或 record id。
     *
     * <p>参数说明: 无。
     *
     * @return 固定低基数名称到当前值的不可变映射
     */
    Map<String, Long> snapshot() {
        lock.lock();
        try {
            LinkedHashMap<String, Long> result = new LinkedHashMap<>();
            for (BusinessPhase phase : BusinessPhase.values()) {
                result.put("gate_business_" + phase.name().toLowerCase(), 0L);
            }
            for (CommitPhase phase : CommitPhase.values()) {
                result.put("gate_commit_" + phase.name().toLowerCase(), 0L);
            }
            long deferred = 0L;
            long waiting = 0L;
            long pending = 0L;
            long oldestBlockedMillis = 0L;
            long maximumConsecutiveFailures = 0L;
            long now = System.nanoTime();
            for (KeyGate gate : gates.values()) {
                String business = "gate_business_" + gate.business().name().toLowerCase();
                String commit = "gate_commit_" + gate.commit().name().toLowerCase();
                result.put(business, result.get(business) + 1L);
                result.put(commit, result.get(commit) + 1L);
                deferred += gate.deferred().size();
                waiting += gate.proxyWaitingCoordinates().size();
                pending += gate.pendingCommitCoordinates().size();
                if (gate.business() == BusinessPhase.BLOCKED
                        || gate.business() == BusinessPhase.INVALIDATING) {
                    long since = gateSinceNanos.getOrDefault(gate.token().key(), now);
                    oldestBlockedMillis = Math.max(oldestBlockedMillis,
                            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(Math.max(0L, now - since)));
                }
                maximumConsecutiveFailures = Math.max(maximumConsecutiveFailures,
                        consecutiveFailures.getOrDefault(gate.token().key(), 0L));
            }
            result.put("gate_deferred_coordinates", deferred);
            result.put("gate_proxy_waiting_coordinates", waiting);
            result.put("gate_pending_commit_coordinates", pending);
            result.put("gate_oldest_blocked_age_ms", oldestBlockedMillis);
            result.put("gate_max_consecutive_failures", maximumConsecutiveFailures);
            return Map.copyOf(result);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 Future 可见前把成功单元转换为等待确认态，使相同 key 在 ACK 明确前保持关闭。
     *
     * @param token   当前执行令牌
     * @param records 已由 listener 成功处理的坐标
     *                返回: 无返回值；迟到令牌只保留现有较新状态。
     */
    void awaitingAck(GateToken token, List<PartitionRecordRef> records) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (!matchesExecuting(gate, token)) return;
            consecutiveFailures.put(token.key(), 0L);
            gates.put(token.key(), new KeyGate(
                    token,
                    BusinessPhase.OPEN,
                    CommitPhase.PENDING,
                    GateHoldReason.NONE,
                    List.copyOf(records),
                    gate.deferred(),
                    Set.of(),
                    coordinates(records)));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 Future 可见前把 listener 失败或未执行单元转换为阻断态，后继留在 PEL 等同 key 重试。
     *
     * @param token   当前执行令牌
     * @param records 失败头及未执行尾部坐标
     *                返回: 无返回值；迟到令牌只保留现有较新状态。
     */
    void blocked(GateToken token, List<PartitionRecordRef> records) {
        listenerFailed(token, List.of(), records);
    }

    /**
     * 业务作用：在 Future 可见前同时发布成功前缀的确认保护和失败头的业务阻断。
     *
     * @param token            当前执行令牌
     * @param successfulPrefix 已完成 listener 的精确坐标
     * @param failedAndTail    失败头及未执行尾部
     *                         返回: 无返回值；迟到令牌不改写新代次。
     */
    void listenerFailed(GateToken token,
                        List<PartitionRecordRef> successfulPrefix,
                        List<PartitionRecordRef> failedAndTail) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (!matchesExecuting(gate, token)) return;
            consecutiveFailures.merge(token.key(), 1L, Long::sum);
            gates.put(token.key(), new KeyGate(
                    token,
                    BusinessPhase.BLOCKED,
                    successfulPrefix.isEmpty() ? CommitPhase.NONE : CommitPhase.PENDING,
                    GateHoldReason.LISTENER_RETRY,
                    List.copyOf(failedAndTail),
                    gate.deferred(),
                    Set.of(),
                    coordinates(successfulPrefix)));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 BOTH 单 field Task 结束时进入物理 record 决策保护态，不允许 field 单独形成 ACK PENDING。
     *
     * @param token   当前执行令牌
     * @param records 该 Task 已发布业务结果的 exact field 坐标
     *                返回: 无返回值；物理 record 账本决议前 commit 保持 NONE。
     */
    void proxyRecordDecision(GateToken token, List<PartitionRecordRef> records) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (!matchesExecuting(gate, token)) return;
            gates.put(token.key(), new KeyGate(
                    token,
                    BusinessPhase.BLOCKED,
                    CommitPhase.NONE,
                    GateHoldReason.PROXY_RECORD_DECISION,
                    List.copyOf(records),
                    gate.deferred(),
                    coordinates(records),
                    Set.of()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 BOTH Task Future 可见后发布成功 record 的 ledger 等待集合与失败头，防止单 field 先开放同 key。
     *
     * @param token            当前执行令牌
     * @param successfulPrefix 已完成 listener、仍等待整条 record 决议的 field 坐标
     * @param failedAndTail    失败头及未执行尾部
     * @return 仅当当前 gate 仍是该 Task 发布的完整 PROXY_RECORD_DECISION 时返回 true
     */
    boolean proxyTaskOutcome(GateToken token,
                             List<PartitionRecordRef> successfulPrefix,
                             List<PartitionRecordRef> failedAndTail) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            List<PartitionRecordRef> decided = new ArrayList<>(successfulPrefix.size() + failedAndTail.size());
            decided.addAll(successfulPrefix);
            decided.addAll(failedAndTail);
            if (gate == null
                    || !gate.token().equals(token)
                    || gate.business() != BusinessPhase.BLOCKED
                    || gate.commit() != CommitPhase.NONE
                    || gate.holdReason() != GateHoldReason.PROXY_RECORD_DECISION
                    || !sameCoordinates(gate.active(), decided)
                    || !gate.proxyWaitingCoordinates().equals(coordinates(decided))) {
                return false;
            }
            if (!failedAndTail.isEmpty()) consecutiveFailures.merge(token.key(), 1L, Long::sum);
            else consecutiveFailures.put(token.key(), 0L);
            gates.put(token.key(), new KeyGate(
                    token,
                    failedAndTail.isEmpty() ? BusinessPhase.OPEN : BusinessPhase.BLOCKED,
                    CommitPhase.NONE,
                    failedAndTail.isEmpty()
                            ? GateHoldReason.PROXY_RECORD_DECISION : GateHoldReason.LISTENER_RETRY,
                    List.copyOf(failedAndTail),
                    gate.deferred(),
                    coordinates(successfulPrefix),
                    Set.of()));
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 BOTH ledger 证明一条物理 record 的全部 field 成功后，把该 id 从 ledger 等待转入 ACK 等待。
     *
     * @param token 该 record field 对应的 gate 令牌
     * @param id    已满足整条 record 成功条件的物理 id
     *              返回: 无返回值；其它同 Task record 尚未决时继续保持门禁。
     */
    void proxyAckReady(GateToken token, String id) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (gate == null || !gate.token().equals(token) || id == null) return;
            LinkedHashSet<PartitionRecordRef> waiting =
                    new LinkedHashSet<>(gate.proxyWaitingCoordinates());
            LinkedHashSet<PartitionRecordRef> pending =
                    new LinkedHashSet<>(gate.pendingCommitCoordinates());
            List<PartitionRecordRef> promoted = waiting.stream()
                    .filter(ref -> id.equals(ref.id()))
                    .toList();
            if (promoted.isEmpty() && pending.stream().noneMatch(ref -> id.equals(ref.id()))) return;
            waiting.removeAll(promoted);
            pending.addAll(promoted);
            gates.put(token.key(), new KeyGate(
                    token,
                    gate.business(),
                    CommitPhase.PENDING,
                    gate.holdReason(),
                    gate.active(),
                    gate.deferred(),
                    Set.copyOf(waiting),
                    Set.copyOf(pending)));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 holder-fenced ACK 明确收敛后移除对应 record 的确认依赖，再依据剩余业务、确认与后继坐标决定门禁状态。
     *
     * <p>返回: 无返回值；旧代次或没有匹配确认依赖的结果保持状态不变。
     *
     * @param token 已确认执行单元的门禁令牌
     * @param id    本次明确确认的物理 record id
     */
    void ackConfirmed(GateToken token, String id) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (gate == null || !gate.token().equals(token)
                    || (gate.commit() != CommitPhase.PENDING && gate.commit() != CommitPhase.UNKNOWN)) return;
            LinkedHashSet<PartitionRecordRef> pending =
                    new LinkedHashSet<>(gate.pendingCommitCoordinates());
            boolean pendingChanged = pending.removeIf(ref -> id.equals(ref.id()));
            if (!pendingChanged) return;
            settleAfterRemote(token, gate, gate.active(), gate.deferred(),
                    gate.proxyWaitingCoordinates(), Set.copyOf(pending));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：供只含一个确认单元的内部场景一次收敛当前 gate 的全部 ACK 依赖。
     *
     * @param token 已确认执行单元的门禁令牌
     *              返回: 无返回值；生产确认链使用带 record id 的精确重载。
     */
    void ackConfirmed(GateToken token) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (gate == null || !gate.token().equals(token)
                    || (gate.commit() != CommitPhase.PENDING && gate.commit() != CommitPhase.UNKNOWN)) return;
            settleAfterRemote(token, gate, gate.active(), gate.deferred(),
                    gate.proxyWaitingCoordinates(), Set.of());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：标记 ACK 结果不确定，保留已成功业务事实并禁止后继 listener。
     *
     * @param token 对应 CommitAttempt 的 exact gate 令牌
     *              返回: 无返回值；无关旧令牌保持 no-op。
     */
    void ackUnknown(GateToken token) {
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (gate != null && gate.token().equals(token) && gate.commit() == CommitPhase.PENDING) {
                gates.put(token.key(), new KeyGate(
                        token,
                        gate.business(),
                        CommitPhase.UNKNOWN,
                        gate.holdReason(),
                        gate.active(),
                        gate.deferred(),
                        gate.proxyWaitingCoordinates(),
                        gate.pendingCommitCoordinates()));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 consumer-fenced ACK 证明 record 已迁移时，移除当前令牌来源代次下该 record 的门禁责任，保留其它记录的顺序约束。
     *
     * <p>返回: 无返回值；令牌失效或没有匹配坐标时保持状态不变，否则根据剩余依赖更新门禁。
     *
     * @param token 旧 consumer 的 gate 令牌
     * @param id    已迁移的 record id，与令牌的权威对象及来源代次共同限定移除范围
     */
    void recordMoved(GateToken token, String id) {
        if (token == null || id == null) return;
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (gate == null || !gate.token().equals(token)) return;
            List<PartitionRecordRef> active = gate.active().stream()
                    .filter(record -> !matchesTokenRecord(record, token, id)).toList();
            List<PartitionRecordRef> deferred = gate.deferred().stream()
                    .filter(record -> !matchesTokenRecord(record, token, id)).toList();
            LinkedHashSet<PartitionRecordRef> waiting =
                    new LinkedHashSet<>(gate.proxyWaitingCoordinates());
            LinkedHashSet<PartitionRecordRef> pending =
                    new LinkedHashSet<>(gate.pendingCommitCoordinates());
            boolean activeChanged = active.size() != gate.active().size();
            boolean deferredChanged = deferred.size() != gate.deferred().size();
            boolean waitingChanged = waiting.removeIf(record -> matchesTokenRecord(record, token, id));
            boolean pendingChanged = pending.removeIf(record -> matchesTokenRecord(record, token, id));
            boolean changed = activeChanged || deferredChanged || waitingChanged || pendingChanged;
            if (changed) {
                settleAfterRemote(token, gate, active, new ArrayList<>(deferred),
                        Set.copyOf(waiting), Set.copyOf(pending));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 XPENDING 证明 exact record 已经收敛或迁移时，移除匹配 field 与来源代次的门禁坐标，保留其它坐标的顺序约束。
     *
     * <p>返回: 无返回值；仅在存在匹配坐标时重新计算门禁状态，无关 key 和其它来源代次保持不变。
     *
     * @param ref 已收敛的 exact Redis 坐标
     */
    void recordSettled(PartitionRecordRef ref) {
        settleCoordinates(current -> sameCoordinate(current, ref));
    }

    /**
     * 业务作用：物理 BOTH record 已在远端结束时，释放该来源代次下全部 field 的有序门禁责任。
     *
     * @param ref 已取得远端终态证据的冻结 record 坐标，field 不限制清理范围
     *            返回: 无返回值；无关 record 和较新来源代次保持不变。
     */
    void proxyRecordSettled(PartitionRecordRef ref) {
        settleCoordinates(current -> sameRecord(current, ref));
    }

    /**
     * 业务作用：在同一锁内收敛匹配的执行、延后和确认坐标，保留其它 record 已建立的顺序。
     *
     * @param settled 已由远端证据界定的坐标匹配条件
     *                返回: 无返回值；仍有责任的 gate 继续阻挡新消息。
     */
    private void settleCoordinates(java.util.function.Predicate<PartitionRecordRef> settled) {
        lock.lock();
        try {
            List<OrderedKeyId> keys = new ArrayList<>(gates.keySet());
            for (OrderedKeyId key : keys) {
                KeyGate gate = gates.get(key);
                if (gate == null) continue;
                List<PartitionRecordRef> active = gate.active().stream()
                        .filter(settled.negate()).toList();
                List<PartitionRecordRef> deferred = gate.deferred().stream()
                        .filter(settled.negate()).toList();
                LinkedHashSet<PartitionRecordRef> waiting =
                        new LinkedHashSet<>(gate.proxyWaitingCoordinates());
                LinkedHashSet<PartitionRecordRef> pending =
                        new LinkedHashSet<>(gate.pendingCommitCoordinates());
                boolean activeChanged = active.size() != gate.active().size();
                boolean deferredChanged = deferred.size() != gate.deferred().size();
                boolean waitingChanged = waiting.removeIf(settled);
                boolean pendingChanged = pending.removeIf(settled);
                boolean changed = activeChanged || deferredChanged || waitingChanged || pendingChanged;
                if (!changed) continue;
                settleAfterRemote(gate.token(), gate, active, new ArrayList<>(deferred),
                        Set.copyOf(waiting), Set.copyOf(pending));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：来源失权时撤销该代次的执行权，并把其它有效来源的较早坐标原子转成接管头。
     *
     * @param token 即将失效的门禁令牌
     *              返回: 无返回值；令牌已被替换时保持 no-op。
     */
    void invalidate(GateToken token) {
        if (token == null) return;
        lock.lock();
        try {
            KeyGate gate = gates.get(token.key());
            if (gate != null && gate.token().equals(token)) {
                invalidateActiveSource(token.key(), gate,
                        ref -> ref.authority() == token.authority()
                                && ref.sourceGeneration() == token.generation());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：来源代次整体失效时只剔除该 authority 的坐标；active 失权时保留其它来源的遇见顺序并关闭新执行。
     *
     * @param authority 已停止或失权的来源对象
     *                  返回: 无返回值；其它 Claim 与 BOTH consumer 的 gate 不受影响。
     */
    void invalidateAuthority(StreamSourceAuthority authority) {
        lock.lock();
        try {
            List<OrderedKeyId> keys = new ArrayList<>(gates.keySet());
            for (OrderedKeyId key : keys) {
                KeyGate gate = gates.get(key);
                if (gate == null) continue;
                if (gate.token().authority() == authority) {
                    // 旧执行权立即失效，但其它来源已经发布的 deferred 是更早顺序事实，必须先成为接管头。
                    invalidateActiveSource(key, gate, ref -> ref.authority() == authority);
                    continue;
                }
                if (gate.business() == BusinessPhase.INVALIDATING) {
                    // 接管头可能属于非 token 来源；它再次失权时仍须按完整坐标剔除并继续保留后继。
                    invalidateActiveSource(key, gate, ref -> ref.authority() == authority);
                    continue;
                }
                List<PartitionRecordRef> deferred = gate.deferred().stream()
                        .filter(ref -> ref.authority() != authority).toList();
                if (deferred.size() != gate.deferred().size()) {
                    // 仅清理失权来源的 deferred，不能打断仍有效来源正在执行或等待 ACK 的 active/head。
                    gates.put(key, new KeyGate(
                            gate.token(), gate.business(), gate.commit(), gate.holdReason(),
                            gate.active(), new ArrayList<>(deferred),
                            gate.proxyWaitingCoordinates(), gate.pendingCommitCoordinates()));
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：把失权 active gate 原子迁移到 INVALIDATING，只允许保留下来的最早 exact 坐标接管执行权。
     * 调用方必须持有协调器锁。
     *
     * @param key       当前门禁身份
     * @param gate      失权前的门禁快照
     * @param discarded 判断坐标是否属于失权来源
     *                  返回: 无返回值；没有有效坐标时删除门禁，否则继续关闭后继。
     */
    private void invalidateActiveSource(OrderedKeyId key,
                                        KeyGate gate,
                                        java.util.function.Predicate<PartitionRecordRef> discarded) {
        List<PartitionRecordRef> retained = new ArrayList<>();
        for (PartitionRecordRef ref : gate.active()) {
            if (!discarded.test(ref)) addCoordinateIfAbsent(retained, ref);
        }
        for (PartitionRecordRef ref : gate.deferred()) {
            if (!discarded.test(ref)) addCoordinateIfAbsent(retained, ref);
        }
        LinkedHashSet<PartitionRecordRef> waiting = new LinkedHashSet<>(gate.proxyWaitingCoordinates());
        waiting.removeIf(discarded);
        LinkedHashSet<PartitionRecordRef> pending = new LinkedHashSet<>(gate.pendingCommitCoordinates());
        pending.removeIf(discarded);
        if (retained.isEmpty() && waiting.isEmpty() && pending.isEmpty()) {
            gates.remove(key);
            gateSinceNanos.remove(key);
            consecutiveFailures.remove(key);
            return;
        }
        // INVALIDATING 令牌仍属于旧代且不再具有执行权；只有 retained 头的 exact recovery 能发布新令牌。
        gates.put(key, new KeyGate(
                gate.token(), BusinessPhase.INVALIDATING,
                pending.isEmpty() ? CommitPhase.NONE : gate.commit(),
                GateHoldReason.NOT_EXECUTED_RETRY,
                List.copyOf(retained), new ArrayList<>(), Set.copyOf(waiting), Set.copyOf(pending)));
    }

    /**
     * 业务作用：接管 blocked/invalidating 头时保留本次执行单元之后的全部早期坐标，防止较晚消息越过。
     *
     * @param active   当前接管头与其连续后继
     * @param deferred 后续登记坐标
     * @param reserved 本次 exact recovery 实际取得的坐标
     * @return 去除本次坐标后仍按原遇见顺序排列的 deferred 列表
     */
    private static List<PartitionRecordRef> remainingAfterReservation(List<PartitionRecordRef> active,
                                                                      List<PartitionRecordRef> deferred,
                                                                      List<PartitionRecordRef> reserved) {
        List<PartitionRecordRef> remaining = new ArrayList<>();
        for (PartitionRecordRef ref : active) {
            if (reserved.stream().noneMatch(current -> sameCoordinate(current, ref))) {
                addCoordinateIfAbsent(remaining, ref);
            }
        }
        for (PartitionRecordRef ref : deferred) addCoordinateIfAbsent(remaining, ref);
        return remaining;
    }

    /**
     * 业务作用：按完整坐标向顺序列表追加一次，来源交接不能因重复恢复信号改变先后关系。
     *
     * @param target 目标顺序列表
     * @param ref    待追加坐标
     *               返回: 无返回值；已有相同坐标时保持原位置。
     */
    private static void addCoordinateIfAbsent(List<PartitionRecordRef> target, PartitionRecordRef ref) {
        if (target.stream().noneMatch(existing -> sameCoordinate(existing, ref))) target.add(ref);
    }

    /**
     * 业务作用：在 gate 发布临界区内执行来源级 blocked-key 硬上限，deferred 成员与 gate head 使用同一计量口径。
     *
     * @param authority 待新增 gate membership 的来源代次
     *                  返回: 无返回值；达到上限时拒绝本次发布且不改变 gate。
     */
    private void ensureSourceGateCapacity(StreamSourceAuthority.Snapshot authority) {
        long sourceGates = gates.values().stream()
                .filter(gate -> containsSource(gate, authority))
                .count();
        if (sourceGates >= maxBlockedKeysPerSource) {
            throw new OrderedGateCapacityException("source blocked-key capacity exhausted");
        }
    }

    /**
     * 业务作用：判断来源代次是否已占用一个 gate，避免同 key 多坐标重复计量并覆盖 deferred/ACK 等全部责任集合。
     *
     * @param gate      当前 gate
     * @param authority 待核对的来源代次
     * @return 任一 active、deferred、ledger waiting 或 pending-confirm 坐标属于该来源代次时返回 true；没有来源引用的坐标按 gate token 归属判断
     */
    private static boolean containsSource(KeyGate gate, StreamSourceAuthority.Snapshot authority) {
        // 未携带来源引用的坐标按 gate token 确定归属；具有完整来源信息的坐标须独立匹配来源代次。
        boolean legacyTokenMembership = gate.token().authority() == authority.current()
                && gate.token().generation() == authority.generation()
                && (gate.active().stream().anyMatch(ref -> ref.authority() == null)
                || gate.deferred().stream().anyMatch(ref -> ref.authority() == null)
                || gate.proxyWaitingCoordinates().stream().anyMatch(ref -> ref.authority() == null)
                || gate.pendingCommitCoordinates().stream().anyMatch(ref -> ref.authority() == null));
        return legacyTokenMembership
                || gate.active().stream().anyMatch(ref -> belongsToSource(ref, authority))
                || gate.deferred().stream().anyMatch(ref -> belongsToSource(ref, authority))
                || gate.proxyWaitingCoordinates().stream().anyMatch(ref -> belongsToSource(ref, authority))
                || gate.pendingCommitCoordinates().stream().anyMatch(ref -> belongsToSource(ref, authority));
    }

    /**
     * 业务作用：按共享权威对象与 generation 比较 gate 坐标的来源身份，旧代次不能占用新代次额度。
     *
     * @param ref       gate 内的精确坐标
     * @param authority 待核对的来源代次
     * @return 坐标属于该来源代次时返回 true
     */
    private static boolean belongsToSource(PartitionRecordRef ref,
                                           StreamSourceAuthority.Snapshot authority) {
        return ref.authority() == authority.current()
                && ref.sourceGeneration() == authority.generation();
    }

    /**
     * 业务作用：在一个 ACK、迁移或 PEL 缺席结论到达后按剩余 exact 依赖决定继续持门禁、进入失败重试或开放后继。
     * 调用方必须持有协调器锁。
     *
     * <p>返回: 无返回值；仍有确认依赖时保持等待，否则保留待重试的业务头、将后继转为接管头，或在没有剩余责任时移除门禁。
     *
     * @param token    当前 gate 令牌
     * @param previous 远端结论前的 gate
     * @param active   当前业务头的剩余坐标；是否仍需重试由原业务阶段决定
     * @param deferred 尚未取得执行权的后继坐标
     * @param waiting  仍等待 BOTH ledger 整条成功的 exact 坐标
     * @param pending  已产生业务成功事实、仍等待 ACK 明确的 exact 坐标
     */
    private void settleAfterRemote(GateToken token,
                                   KeyGate previous,
                                   List<PartitionRecordRef> active,
                                   List<PartitionRecordRef> deferred,
                                   Set<PartitionRecordRef> waiting,
                                   Set<PartitionRecordRef> pending) {
        // 业务成功不等于远端确认完成；尚有 ACK 责任时必须保留门禁，避免后继越过不确定提交。
        if (!pending.isEmpty()) {
            gates.put(token.key(), new KeyGate(
                    token, previous.business(), previous.commit(), previous.holdReason(),
                    List.copyOf(active), deferred, Set.copyOf(waiting), Set.copyOf(pending)));
            return;
        }
        // BOTH 的其它 field 尚未全部成功时不能按单个 field 的远端结论开放顺序执行。
        if (!waiting.isEmpty()) {
            gates.put(token.key(), new KeyGate(
                    token, previous.business(), CommitPhase.NONE, previous.holdReason(),
                    List.copyOf(active), deferred, Set.copyOf(waiting), Set.of()));
            return;
        }
        // 确认依赖已清空且业务头无需重试后，已登记的后继仍须先成为接管头，防止新消息插队。
        if (previous.business() == BusinessPhase.OPEN || active.isEmpty()) {
            if (deferred.isEmpty()) {
                gates.remove(token.key());
                gateSinceNanos.remove(token.key());
                consecutiveFailures.remove(token.key());
            } else {
                gates.put(token.key(), new KeyGate(
                        token, BusinessPhase.BLOCKED, CommitPhase.NONE,
                        GateHoldReason.NOT_EXECUTED_RETRY,
                        List.copyOf(deferred), new ArrayList<>(), Set.of(), Set.of()));
            }
            return;
        }
        // 已确认的成功前缀不再占用 ACK 阶段，但失败头仍须阻挡后继，等待自身重试。
        gates.put(token.key(), new KeyGate(
                token, BusinessPhase.BLOCKED, CommitPhase.NONE, previous.holdReason(),
                List.copyOf(active), deferred, Set.of(), Set.of()));
    }

    /**
     * 业务作用：复制一组 exact field 坐标，确认依赖据此隔离不同 Stream 中可能重复的 record id。
     *
     * @param records Task outcome 中的精确坐标
     * @return 不包含消息体、去重且不可修改的坐标集合；不保证迭代顺序
     */
    private static Set<PartitionRecordRef> coordinates(List<PartitionRecordRef> records) {
        return Set.copyOf(new LinkedHashSet<>(records));
    }

    /**
     * 业务作用：判断结果是否仍对应当前正在执行、尚未进入确认阶段的 gate，供调用方拒绝迟到结果。
     *
     * @param gate  当前门禁快照；不存在时为 null
     * @param token 待核对的执行令牌
     * @return gate 存在、令牌相同、业务阶段为 EXECUTING 且确认阶段为 NONE 时返回 true；否则返回 false，不改变门禁状态
     */
    private static boolean matchesExecuting(KeyGate gate, GateToken token) {
        return gate != null
                && gate.token().equals(token)
                && gate.business() == BusinessPhase.EXECUTING
                && gate.commit() == CommitPhase.NONE;
    }

    /**
     * 业务作用：判断本次投递是否正在重投当前 blocked head，而非一条新后继消息。
     *
     * @param active  gate 当前失败头及尾部
     * @param records 新投递坐标
     * @return 两个列表均非空，且头坐标的 stream/group/consumer/id/field、权威对象与来源代次均一致时返回 true
     */
    private static boolean sameHead(List<PartitionRecordRef> active, List<PartitionRecordRef> records) {
        if (active.isEmpty() || records.isEmpty()) return false;
        PartitionRecordRef expected = active.getFirst();
        PartitionRecordRef actual = records.getFirst();
        return sameCoordinate(expected, actual);
    }

    /**
     * 业务作用：复验 Task outcome 覆盖的完整有序坐标仍与其决策快照一致，远端已经收敛过的坐标不得被迟到结果恢复。
     *
     * @param expected gate 当前保存的决策坐标
     * @param actual   Task outcome 重新组合的成功前缀与未成功尾部
     * @return 数量、顺序与完整来源坐标均相同时返回 true
     */
    private static boolean sameCoordinates(List<PartitionRecordRef> expected,
                                           List<PartitionRecordRef> actual) {
        if (expected.size() != actual.size()) return false;
        for (int index = 0; index < expected.size(); index++) {
            if (!sameCoordinate(expected.get(index), actual.get(index))) return false;
        }
        return true;
    }

    /**
     * 业务作用：仅允许失败头重试接管与它属于同一物理 record 的 ledger 等待，禁止越过更早成功但尚未确认的 record。
     *
     * @param gate 当前 blocked gate
     * @return 没有 ledger 等待，或每个等待坐标都与失败坐标中的某条记录具有相同物理记录身份和来源代次时返回 true
     */
    private static boolean waitingBelongsToFailedRecords(KeyGate gate) {
        if (gate.proxyWaitingCoordinates().isEmpty()) return true;
        return gate.proxyWaitingCoordinates().stream().allMatch(waiting -> gate.active().stream()
                .anyMatch(active -> sameRecord(active, waiting)));
    }

    /**
     * 业务作用：限定 consumer 迁移只能剔除当前 gate 来源代次内的 record，不能按裸 id 影响其它来源的 deferred 坐标。
     *
     * @param record gate 保存的 exact 坐标
     * @param token  当前 gate 令牌
     * @param id     已迁移的物理 record id
     * @return 坐标属于该令牌来源代次且 id 相同时返回 true
     */
    private static boolean matchesTokenRecord(PartitionRecordRef record, GateToken token, String id) {
        return id.equals(record.id())
                && record.authority() == token.authority()
                && record.sourceGeneration() == token.generation();
    }

    /**
     * 业务作用：判断两个 field 是否属于同一来源代次的同一物理 record，供 ledger 等待与失败头建立关联。
     *
     * @param left  第一坐标
     * @param right 第二坐标
     * @return stream/group/consumer/id 与来源代次一致时返回 true
     */
    private static boolean sameRecord(PartitionRecordRef left, PartitionRecordRef right) {
        return Objects.equals(left.stream(), right.stream())
                && Objects.equals(left.group(), right.group())
                && Objects.equals(left.consumer(), right.consumer())
                && Objects.equals(left.id(), right.id())
                && left.authority() == right.authority()
                && left.sourceGeneration() == right.sourceGeneration();
    }

    /**
     * 业务作用：按 stream/group/consumer/id/field、权威对象与来源代次比较 exact 坐标，隔离不同来源或消费代次的同名 field。
     *
     * @param left  第一坐标
     * @param right 第二坐标
     * @return 物理 field、消费身份、权威对象与来源代次均一致时返回 true；任一项不同则返回 false
     */
    private static boolean sameCoordinate(PartitionRecordRef left, PartitionRecordRef right) {
        return Objects.equals(left.stream(), right.stream())
                && Objects.equals(left.group(), right.group())
                && Objects.equals(left.consumer(), right.consumer())
                && Objects.equals(left.id(), right.id())
                && Objects.equals(left.field(), right.field())
                && left.authority() == right.authority()
                && left.sourceGeneration() == right.sourceGeneration();
    }

    enum BusinessPhase {OPEN, EXECUTING, BLOCKED, INVALIDATING}

    enum CommitPhase {NONE, PENDING, UNKNOWN}

    enum GateHoldReason {NONE, LISTENER_RETRY, NOT_EXECUTED_RETRY, PROXY_RECORD_DECISION}

    private record OrderedKeyId(long planId, int effectiveHash) {}

    private record KeyGate(GateToken token,
                           BusinessPhase business,
                           CommitPhase commit,
                           GateHoldReason holdReason,
                           List<PartitionRecordRef> active,
                           List<PartitionRecordRef> deferred,
                           Set<PartitionRecordRef> proxyWaitingCoordinates,
                           Set<PartitionRecordRef> pendingCommitCoordinates) {
    }

    /**
     * 业务作用：标识一次 ordered key 执行代次，不保存原始业务 key 或消息体。
     *
     * @param key        planId 与有效 hash 组成的门禁身份
     * @param authority  Redis 来源共享权威对象
     * @param generation Redis 来源所有权代次
     * @param sequence   key 内执行序号
     */
    record GateToken(OrderedKeyId key, StreamSourceAuthority authority, long generation, long sequence) {}

    /**
     * 业务作用：表达 ordered gate 预留结论，blocked 记录不产生 Partition Task。
     *
     * @param acquired       是否取得执行权
     * @param authorityStale 是否因来源失权而拒绝且保持 gate 无副作用
     * @param token          取得执行权时的唯一令牌
     */
    record GateReservation(boolean acquired, boolean authorityStale, GateToken token) {
        /**
         * 业务作用：创建取得执行权的结论。参数说明: 当前令牌。返回: acquired=true 的结论。
         */
        static GateReservation acquired(GateToken token) {
            return new GateReservation(true, false, Objects.requireNonNull(token, "token"));
        }

        /**
         * 业务作用：创建已被同 key 门禁阻挡的结论。参数说明: 无。返回: acquired=false 的结论。
         */
        static GateReservation blocked() {
            return new GateReservation(false, false, null);
        }

        /**
         * 业务作用：创建来源已失权且未写入 gate 的结论。参数说明: 无。返回: acquired=false、authorityStale=true 且 token 为空的结论。
         */
        static GateReservation staleAuthority() {
            return new GateReservation(false, true, null);
        }
    }

    /**
     * 业务作用：表示 ordered gate 硬容量已满，来源必须暂停而不能丢弃精确 PEL 坐标。
     */
    static final class OrderedGateCapacityException extends RuntimeException {
        /**
         * 业务作用：创建容量拒绝结论。参数说明: 失败摘要。返回: 不可重试的当前批结论。
         */
        OrderedGateCapacityException(String message) {
            super(message);
        }
    }
}
