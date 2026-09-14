package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 业务作用：协调两类来源的逐 record 执行权，并按 BOTH consumer epoch 保存多 field 成功证据，全部 field 成功后才允许 XACK。
 */
final class ProxyRecordAckLedger {

    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedHashMap<RecordKey, Entry> entries = new LinkedHashMap<>();
    private final Map<RecordKey, Object> executing = new LinkedHashMap<>();
    private final int maximumEntries;
    private final StreamRuntimeStatus status;
    private long capacityHits;

    /**
     * 业务作用：建立不驱逐未决 record 的有界账本。
     *
     * @param maximumEntries 当前 RedisProxy 最多持有的 BOTH 未决账本 record 数
     * @param status         运行指标与 readiness 状态
     *                       返回: 初始为空的账本。
     */
    ProxyRecordAckLedger(int maximumEntries, StreamRuntimeStatus status) {
        if (maximumEntries < 1) throw new IllegalArgumentException("proxy ledger capacity must be greater than zero");
        this.maximumEntries = maximumEntries;
        this.status = Objects.requireNonNull(status, "status");
    }

    /**
     * 业务作用：在解析前按物理 record 非阻塞取得执行权，周期接管与精确重试不能同时发布同一 record 的 Task。
     *
     * @param refs 已冻结 consumer、authority 和来源代次的原始批次坐标
     * @return 仅持有本次获准坐标的句柄；执行中或已交给确认链的 record 留给现有责任继续推进
     */
    ExecutionBatch tryBeginExecution(List<PartitionRecordRef> refs) {
        lock.lock();
        try {
            Object token = new Object();
            // 执行句柄由已准入的 raw batch 或重试容量约束；物理分区不占用 BOTH 多 field 账本容量。
            Set<RecordKey> acquired = new LinkedHashSet<>();
            for (PartitionRecordRef ref : refs) {
                RecordKey key = RecordKey.of(ref);
                Entry current = entries.get(key);
                // 全 field 成功后即由确认链持有责任，ACK UNKNOWN 期间不得重新建立执行或确认分支。
                if (executing.containsKey(key) || current != null && current.fields().values().stream()
                        .allMatch(field -> field.status() == FieldStatus.SUCCESS)) continue;
                executing.put(key, token);
                acquired.add(key);
            }
            return new ExecutionBatch(this, token, Set.copyOf(acquired));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 Task 及本轮确认交接结束后归还执行权，迟到关闭不能释放其它执行者的 record。
     *
     * @param batch 当前执行者持有的精确坐标和令牌
     *              返回: 无返回值；不删除仍由 PEL 或确认链持有的账本证据。
     */
    private void endExecution(ExecutionBatch batch) {
        lock.lock();
        try {
            for (RecordKey key : batch.acquired) executing.remove(key, batch.token);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在任何 field Task 发布前原子创建或复用物理 record 账本条目。
     *
     * @param ref            完整解析时冻结的 record 坐标及来源代次
     * @param requiredFields 整条 record 完整解析得到的 field 顺序
     * @return 同 epoch 可复用成功 field 证据的条目
     */
    Entry open(PartitionRecordRef ref, List<String> requiredFields) {
        try (OpenBatch batch = openBatch(List.of(
                new OpenRequest(ref, requiredFields)))) {
            Entry entry = batch.entries().getFirst().entry();
            batch.commit();
            return entry;
        }
    }

    /**
     * 业务作用：在同一账本锁内校验并发布一个 Redis poll 批次的全部物理 record，任一合同或容量拒绝都不留下部分条目。
     *
     * @param requests 已完整解析且保持 Redis 遇见顺序的 record 声明
     * @return 可在 Task 发布前回滚新条目的批次句柄
     */
    OpenBatch openBatch(List<OpenRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            throw new IllegalArgumentException("proxy ledger batch must not be empty");
        }
        List<OpenRequest> frozen = requests.stream().map(OpenRequest::copy).toList();
        lock.lock();
        try {
            LinkedHashMap<RecordKey, OpenRequest> distinct = new LinkedHashMap<>();
            for (OpenRequest request : frozen) {
                RecordKey key = request.key();
                OpenRequest duplicate = distinct.putIfAbsent(key, request);
                if (duplicate != null && !duplicate.requiredFields().equals(request.requiredFields())) {
                    throw new IllegalStateException("BOTH record fields changed within one consumer epoch");
                }
            }

            int missing = 0;
            for (Map.Entry<RecordKey, OpenRequest> requested : distinct.entrySet()) {
                Entry current = entries.get(requested.getKey());
                if (current == null) {
                    missing++;
                } else if (!current.requiredFields().equals(requested.getValue().requiredFields())) {
                    throw new IllegalStateException("BOTH record fields changed within one consumer epoch");
                }
            }
            if (entries.size() + missing > maximumEntries) {
                capacityHits++;
                status.failReadiness("proxy_ledger_capacity");
                throw new LedgerCapacityException();
            }

            LinkedHashMap<RecordKey, Entry> created = new LinkedHashMap<>();
            for (Map.Entry<RecordKey, OpenRequest> requested : distinct.entrySet()) {
                if (!entries.containsKey(requested.getKey())) {
                    created.put(requested.getKey(), new Entry(
                            requested.getKey(), requested.getValue().requiredFields()));
                }
            }
            List<RecordKey> published = new ArrayList<>(created.size());
            try {
                for (Map.Entry<RecordKey, Entry> entry : created.entrySet()) {
                    entries.put(entry.getKey(), entry.getValue());
                    published.add(entry.getKey());
                }

                List<BatchEntry> result = new ArrayList<>(frozen.size());
                for (OpenRequest request : frozen) {
                    Entry entry = entries.get(request.key());
                    List<String> execute = entry.requiredFields().stream()
                            .filter(field -> entry.fields().get(field).status() != FieldStatus.SUCCESS)
                            .toList();
                    result.add(new BatchEntry(entry, execute));
                }
                status.proxyLedgerEntries(entries.size());
                return new OpenBatch(this, List.copyOf(result), List.copyOf(created.values()));
            } catch (Throwable failure) {
                for (RecordKey key : published) entries.remove(key, created.get(key));
                status.proxyLedgerEntries(entries.size());
                throw failure;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：读取 BOTH 账本条目、最老年龄和容量拒绝次数，不暴露 stream、consumer 或 record id。
     *
     * <p>参数说明: 无。
     *
     * @return 固定低基数账本状态
     */
    Map<String, Long> snapshot() {
        lock.lock();
        try {
            long now = System.nanoTime();
            long oldestMillis = 0L;
            for (Entry entry : entries.values()) {
                oldestMillis = Math.max(oldestMillis,
                        TimeUnit.NANOSECONDS.toMillis(Math.max(0L, now - entry.createdAtNanos())));
            }
            return Map.of(
                    "proxy_ledger_entries", (long) entries.size(),
                    "proxy_ledger_oldest_age_ms", oldestMillis,
                    "proxy_ledger_capacity_hits", capacityHits);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：撤销尚未发布任何 Task 的批次新条目，既有 epoch 成功证据不受影响。
     *
     * @param created 本批新建且尚未交给执行链的条目
     *                返回: 无返回值；并发移除或重复关闭保持幂等。
     */
    private void rollbackBatch(List<Entry> created) {
        lock.lock();
        try {
            for (Entry entry : created) entries.remove(entry.key(), entry);
            status.proxyLedgerEntries(entries.size());
            if (entries.size() < maximumEntries) status.clearReadiness("proxy_ledger_capacity");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：返回当前 epoch 仍需要执行 listener 的 field，已成功 field 不在同 epoch 重复调用。
     *
     * @param entry 当前物理 record 条目
     * @return 按原 record 顺序排列的未成功 field
     */
    List<String> fieldsToExecute(Entry entry) {
        lock.lock();
        try {
            Entry current = requireCurrent(entry);
            return current.requiredFields().stream()
                    .filter(field -> current.fields().get(field).status() != FieldStatus.SUCCESS)
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：记录一个 field 的成功证据及 exact gate token，旧成功证据不被后续未决结果降级。
     *
     * @param entry 物理 record 条目
     * @param field record hash field
     * @param gate  ordered field 对应 gate；unordered 为 null
     *              返回: 无返回值。
     */
    void succeeded(Entry entry, String field, OrderedKeyCoordinator.GateToken gate) {
        update(entry, field, FieldStatus.SUCCESS, gate);
    }

    /**
     * 业务作用：记录一个 field 本次未成功，保留它在 PEL 精确重试。
     *
     * @param entry 物理 record 条目
     * @param field record hash field
     * @param gate  ordered field 对应 gate；unordered 为 null
     *              返回: 无返回值；已成功 field 不降级。
     */
    void failed(Entry entry, String field, OrderedKeyCoordinator.GateToken gate) {
        update(entry, field, FieldStatus.FAILED, gate);
    }

    /**
     * 业务作用：判断物理 record 的所有 required field 是否都已成功。
     *
     * @param entry 物理 record 条目
     * @return 当前条目仍有效且可安全建立一次 record ACK 时返回 true；已结束条目返回 false
     */
    boolean ackReady(Entry entry) {
        lock.lock();
        try {
            Entry current = entries.get(entry.key());
            return current == entry && current.fields().values().stream()
                    .allMatch(field -> field.status() == FieldStatus.SUCCESS);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：取得只属于该 record 的 ordered gate 依赖，供 consumer-fenced ACK 精确开放。
     *
     * @param entry 物理 record 条目
     * @return 去重且保持 field 顺序的 gate token；已结束条目返回空列表
     */
    List<OrderedKeyCoordinator.GateToken> gates(Entry entry) {
        lock.lock();
        try {
            Entry current = entries.get(entry.key());
            if (current != entry) return List.of();
            LinkedHashSet<OrderedKeyCoordinator.GateToken> result = new LinkedHashSet<>();
            for (FieldState state : current.fields().values()) if (state.gate() != null) result.add(state.gate());
            return List.copyOf(result);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 record 已明确 ACK 后删除账本证据，容量随之归还。
     *
     * @param entry 已确认的条目
     *              返回: 无返回值；重复回调保持幂等。
     */
    void confirmed(Entry entry) {
        remove(entry);
    }

    /**
     * 业务作用：在 PEL 证明 record 已迁移到其它 consumer 时删除旧 epoch 证据。
     *
     * @param entry 已迁移的条目
     *              返回: 无返回值；新 consumer 会整条重放。
     */
    void moved(Entry entry) {
        remove(entry);
    }

    /**
     * 业务作用：根据远端终态删除同一来源代次的整条物理 record 证据，归还所有 field 共用的账本容量。
     *
     * @param ref 远端已确认不存在、已迁移或本地已失权的冻结坐标
     *            返回: 无返回值；其它 record、consumer、authority 和来源代次保持不变。
     */
    void recordSettled(PartitionRecordRef ref) {
        lock.lock();
        try {
            entries.remove(RecordKey.of(ref));
            status.proxyLedgerEntries(entries.size());
            if (entries.size() < maximumEntries) status.clearReadiness("proxy_ledger_capacity");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：在 container epoch 失效时整体废弃它的局部成功证据，新 epoch 从 Redis PEL 整条重建。
     *
     * @param consumer 失效的实际 consumer name
     *                 返回: 无返回值。
     */
    void invalidateConsumer(String consumer) {
        lock.lock();
        try {
            entries.entrySet().removeIf(entry -> consumer.equals(entry.getKey().consumer()));
            status.proxyLedgerEntries(entries.size());
            if (entries.size() < maximumEntries) status.clearReadiness("proxy_ledger_capacity");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：只更新当前条目的 exact field 状态，忽略已结束条目的迟到结果，拒绝未声明 field 污染 record 决策。
     *
     * @param entry  物理 record 条目
     * @param field  record hash field
     * @param target 新 field 状态
     * @param gate   exact gate token
     *               返回: 无返回值。
     */
    private void update(Entry entry,
                        String field,
                        FieldStatus target,
                        OrderedKeyCoordinator.GateToken gate) {
        lock.lock();
        try {
            Entry current = entries.get(entry.key());
            // 远端终态或失权可先结束账本责任；旧 Task 的迟到结果不能复活条目或覆盖同坐标的新证据。
            if (current != entry) return;
            FieldState previous = current.fields().get(field);
            if (previous == null) throw new IllegalArgumentException("field is not required by proxy record: " + field);
            if (previous.status() == FieldStatus.SUCCESS && target != FieldStatus.SUCCESS) return;
            current.fields().put(field, new FieldState(target, gate == null ? previous.gate() : gate));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：删除仍是当前对象的账本条目，防止迟到回调删除同 key 新对象。
     *
     * @param entry 待删除条目
     *              返回: 无返回值。
     */
    private void remove(Entry entry) {
        lock.lock();
        try {
            entries.remove(entry.key(), entry);
            status.proxyLedgerEntries(entries.size());
            if (entries.size() < maximumEntries) status.clearReadiness("proxy_ledger_capacity");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 业务作用：复验调用方持有的条目仍是账本当前代次。
     *
     * @param entry 调用方持有的条目
     * @return 账本中的同一对象
     */
    private Entry requireCurrent(Entry entry) {
        Entry current = entries.get(entry.key());
        if (current != entry) throw new IllegalStateException("proxy ledger entry is no longer current");
        return current;
    }

    /**
     * 业务作用：持有一批物理 record 从解析、执行到确认交接的唯一执行权。
     */
    static final class ExecutionBatch implements AutoCloseable {
        private final ProxyRecordAckLedger owner;
        private final Object token;
        private final Set<RecordKey> acquired;
        private final AtomicBoolean closed = new AtomicBoolean();

        /**
         * 业务作用：冻结本批实际取得的 record 与执行令牌。参数说明: owner 为所属账本，token 为唯一执行令牌，acquired 为获准坐标。返回: 尚未关闭的执行句柄。
         */
        private ExecutionBatch(ProxyRecordAckLedger owner, Object token, Set<RecordKey> acquired) {
            this.owner = owner;
            this.token = token;
            this.acquired = acquired;
        }

        /**
         * 业务作用：判断原始 record 是否由本批唯一负责执行。参数说明: ref 为冻结坐标，field 不参与身份。返回: 本批未关闭且持有该 record 时为 true。
         */
        boolean owns(PartitionRecordRef ref) {
            return !closed.get() && acquired.contains(RecordKey.of(ref));
        }

        /**
         * 业务作用：归还本批执行权，使仍未成功的 record 可由后续恢复接续。参数说明: 无。返回: 无返回值；重复关闭保持幂等。
         */
        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) owner.endExecution(this);
        }
    }

    private enum FieldStatus {PENDING, SUCCESS, FAILED}

    private record FieldState(FieldStatus status, OrderedKeyCoordinator.GateToken gate) {
    }

    private record RecordKey(String stream, String group, String consumer, String id,
                             StreamSourceAuthority authority, long sourceGeneration) {
        /**
         * 业务作用：以解析时冻结的来源权威和代次隔离同名 consumer 的 record 证据。
         *
         * @param ref 完整 record 坐标，field 不参与物理 record 身份
         * @return 不跨来源或代次复用的账本键
         */
        private static RecordKey of(PartitionRecordRef ref) {
            return new RecordKey(ref.stream(), ref.group(), ref.consumer(), ref.id(),
                    ref.authority(), ref.sourceGeneration());
        }
    }

    /**
     * 业务作用：冻结整批发布前的一条物理 record 合同。
     *
     * @param ref            完整解析时冻结的 record 坐标及来源代次
     * @param requiredFields 必须全部成功的 field 顺序
     */
    record OpenRequest(PartitionRecordRef ref, List<String> requiredFields) {
        /**
         * 业务作用：冻结已解析 record 的来源身份及完整 field 合同。参数说明: ref 为冻结坐标，requiredFields 为必需 field。返回: 非空 field 声明。
         */
        OpenRequest {
            Objects.requireNonNull(ref, "ref");
            Objects.requireNonNull(ref.group(), "group");
            Objects.requireNonNull(ref.consumer(), "consumer");
            Objects.requireNonNull(ref.authority(), "authority");
            requiredFields = List.copyOf(requiredFields);
            if (requiredFields.isEmpty()) throw new IllegalArgumentException("proxy ledger fields must not be empty");
        }

        /**
         * 业务作用：复制外部声明并冻结 field 列表。参数说明: 原声明。返回: 不共享可变列表的声明。
         */
        private static OpenRequest copy(OpenRequest request) {
            Objects.requireNonNull(request, "request");
            return new OpenRequest(request.ref(), request.requiredFields());
        }

        /**
         * 业务作用：构造账本内部 record 身份。参数说明: 无。返回: 含 authority 和来源代次的物理 record 组合键。
         */
        private RecordKey key() {
            return RecordKey.of(ref);
        }
    }

    /**
     * 业务作用：把整批发布结果中的物理条目与本轮仍需执行的 field 快照绑定。
     *
     * @param entry           当前 consumer epoch 条目
     * @param fieldsToExecute 排除既有成功证据后的 field 顺序
     */
    record BatchEntry(Entry entry, List<String> fieldsToExecute) {
    }

    /**
     * 业务作用：在组合容量取得后暂时持有整批新 ledger 发布结果，Task 建立前的异常可一次撤销全部新条目。
     */
    static final class OpenBatch implements AutoCloseable {

        private final ProxyRecordAckLedger owner;
        private final List<BatchEntry> entries;
        private final List<Entry> created;
        private final AtomicBoolean completed = new AtomicBoolean();

        /**
         * 业务作用：绑定已在同一锁内发布的批次条目及其回滚集合。
         *
         * @param owner   所属账本
         * @param entries 与请求顺序一致的条目结果
         * @param created 本批新建条目
         *                返回: 尚未提交给 Task 链的批次句柄。
         */
        private OpenBatch(ProxyRecordAckLedger owner,
                          List<BatchEntry> entries,
                          List<Entry> created) {
            this.owner = owner;
            this.entries = entries;
            this.created = created;
        }

        /**
         * 业务作用：返回与请求顺序一致的 ledger 结果。参数说明: 无。返回: 不可变结果列表。
         */
        List<BatchEntry> entries() {
            return entries;
        }

        /**
         * 业务作用：声明全部 Task 输入已经建立，后续责任由 ledger 与执行链共同持有。参数说明: 无。返回: 无返回值。
         */
        void commit() {
            completed.set(true);
        }

        /**
         * 业务作用：未提交时撤销本批全部新条目，已提交或重复关闭保持幂等。参数说明: 无。返回: 无返回值。
         */
        @Override
        public void close() {
            if (completed.compareAndSet(false, true)) owner.rollbackBatch(created);
        }
    }

    /**
     * 业务作用：表达一个 consumer epoch 内的物理 record 决策证据。
     */
    static final class Entry {

        private final RecordKey key;
        private final List<String> requiredFields;
        private final LinkedHashMap<String, FieldState> fields;
        private final long createdAtNanos = System.nanoTime();

        /**
         * 业务作用：为完整解析后的 record 建立按 field 顺序的决策容器。
         *
         * @param key            含 authority 和来源代次的物理 record 组合身份
         * @param requiredFields 必须全部成功的 field
         *                       返回: 全部 field 初始为 PENDING 的条目。
         */
        private Entry(RecordKey key, List<String> requiredFields) {
            this.key = key;
            this.requiredFields = requiredFields;
            this.fields = new LinkedHashMap<>();
            for (String field : requiredFields) this.fields.put(field, new FieldState(FieldStatus.PENDING, null));
        }

        /**
         * 业务作用：返回账本身份。参数说明: 无。返回: 组合键。
         */
        private RecordKey key() {
            return key;
        }

        /**
         * 业务作用：返回原 record 的 field 顺序。参数说明: 无。返回: 不可变 field 列表。
         */
        private List<String> requiredFields() {
            return requiredFields;
        }

        /**
         * 业务作用：返回锁保护的 field 状态表。参数说明: 无。返回: 当前可变映射。
         */
        private LinkedHashMap<String, FieldState> fields() {
            return fields;
        }

        /**
         * 业务作用：返回账本条目建立时刻。参数说明: 无。返回: 单调时钟纳秒值。
         */
        private long createdAtNanos() {
            return createdAtNanos;
        }
    }

    /**
     * 业务作用：表示 BOTH ledger 已达硬上限，来源必须暂停读取而不能驱逐旧证据。
     */
    static final class LedgerCapacityException extends RuntimeException {
        /**
         * 业务作用：创建容量拒绝结论。参数说明: 无。返回: 硬上限异常。
         */
        LedgerCapacityException() {
            super("proxy record ledger capacity exhausted");
        }
    }
}
