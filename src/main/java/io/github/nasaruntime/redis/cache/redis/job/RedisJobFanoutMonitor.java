package io.github.nasaruntime.redis.cache.redis.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 业务作用：推进 Fanout 首次投递、接收回执、启动可见性、能力恢复、重分配和有界清理。
 */
final class RedisJobFanoutMonitor {

    private static final String INBOX_GROUP = "redis-job-fanout";
    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final RedisJobExecutorRegistry registry;
    private final String publishCommand;
    private final Map<String, RedisJobDefinition> workers = new ConcurrentHashMap<>();
    private final AtomicLongArray rootReceiptScanUpperBounds;
    private volatile long workerReceiptScanUpperBoundMs;

    /**
     * 业务作用：绑定 Fanout 持久索引推进所需的全部权威组件。
     *
     * @param properties Job 配置
     * @param keys       键路由器
     * @param scripts    状态脚本
     * @param registry   执行器能力注册表
     */
    RedisJobFanoutMonitor(RedisJobProperties properties, RedisJobKeyspace keys,
                          RedisJobScriptExecutor scripts, RedisJobExecutorRegistry registry) {
        this.properties = properties;
        this.keys = keys;
        this.scripts = scripts;
        this.registry = registry;
        this.publishCommand = properties.getPubsubMode() == RedisJobPubSubMode.SHARDED ? "SPUBLISH" : "PUBLISH";
        this.rootReceiptScanUpperBounds = new AtomicLongArray(keys.fanoutBucketCount());
        for (int bucket = 0; bucket < keys.fanoutBucketCount(); bucket++) {
            rootReceiptScanUpperBounds.set(bucket, properties.getMaxScanIntervalMs());
        }
        this.workerReceiptScanUpperBoundMs = properties.getMaxScanIntervalMs();
    }

    /**
     * 业务作用：登记本地 Worker 重试边界，供 Fanout 租约恢复复用公共状态脚本。
     *
     * @param definition Worker 定义
     *                   返回：无返回值。
     */
    synchronized void register(RedisJobDefinition definition) {
        if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) {
            workers.put(definition.workerName(), definition);
            refreshWorkerReceiptBound();
        }
    }

    /**
     * 业务作用：移除已经删除的本地 Worker，并恢复由剩余 Worker 实际期限决定的空桶扫描上界。
     *
     * @param definition 已删除定义
     *                   返回：无返回值。
     */
    synchronized void unregister(RedisJobDefinition definition) {
        if (definition.trigger() != RedisJobTrigger.FANOUT_ONLY) return;
        workers.remove(definition.workerName(), definition);
        refreshWorkerReceiptBound();
    }

    /**
     * 业务作用：在 Worker 集合变化后重算全桶接收期限，避免已删除或已升级定义永久压低扫描周期。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void refreshWorkerReceiptBound() {
        long upperBound = properties.getMaxScanIntervalMs();
        for (RedisJobDefinition worker : workers.values()) {
            upperBound = Math.min(upperBound, worker.fanoutReceiptTimeoutMs());
        }
        workerReceiptScanUpperBoundMs = Math.max(properties.getMinScanIntervalMs(), upperBound);
    }

    /**
     * 业务作用：独立推进一个桶的接收、启动、租约与清理索引；没有根和本地 Worker 时恢复长退避。
     *
     * @param bucket Fanout 桶
     * @return 当前桶四类恢复索引中的最短下一扫描间隔。
     */
    long scanBucket(int bucket) {
        long observedRootBound = rootReceiptScanUpperBounds.get(bucket);
        FanoutDueScans scans = dueIndexes(bucket);
        if (!scans.hasRoots() && workerReceiptScanUpperBoundMs >= properties.getMaxScanIntervalMs()) {
            rootReceiptScanUpperBounds.compareAndSet(
                    bucket, observedRootBound, properties.getMaxScanIntervalMs());
            return RedisJobScanInterval.delay(properties, registry.executorId(), scans.receipts().redisNow(),
                    0L, false, RedisJobScanInterval.FANOUT_GC, bucket, properties.getMaxScanIntervalMs());
        }
        long nextDelay = delay(scanReceipts(bucket, scans.receipts()),
                RedisJobScanInterval.FANOUT_RECEIPT, bucket);
        nextDelay = Math.min(nextDelay, delay(scanReady(bucket, scans.ready()),
                RedisJobScanInterval.FANOUT_READY, bucket));
        nextDelay = Math.min(nextDelay, delay(scanLeases(bucket, scans.leases()),
                RedisJobScanInterval.FANOUT_LEASE, bucket));
        nextDelay = Math.min(nextDelay, delay(scanGc(bucket, scans.gc()),
                RedisJobScanInterval.FANOUT_GC, bucket));
        if (!scans.hasRoots()) {
            rootReceiptScanUpperBounds.compareAndSet(
                    bucket, observedRootBound, properties.getMaxScanIntervalMs());
        }
        return nextDelay;
    }

    /**
     * 业务作用：根任务建立时压低目标桶扫描上界，空桶恢复长退避后新 deadline 仍会立即被发现。
     *
     * @param bucket           Fanout 桶
     * @param receiptTimeoutMs 根任务回执期限
     *                         返回：无返回值。
     */
    void activateBucket(int bucket, long receiptTimeoutMs) {
        long bounded = Math.max(properties.getMinScanIntervalMs(),
                Math.min(properties.getMaxScanIntervalMs(), receiptTimeoutMs));
        rootReceiptScanUpperBounds.accumulateAndGet(bucket, bounded, Math::min);
    }

    /**
     * 业务作用：恢复服务端确认已到期的 Fanout 执行租约，并按根策略重试、重分配或聚合终态。
     *
     * @param bucket Fanout 桶
     * @param scan   本轮到期成员与下一最小 score
     * @return 原扫描结果。
     */
    private RedisJobRepository.DueScan scanLeases(int bucket, RedisJobRepository.DueScan scan) {
        for (RedisJobRepository.DueMember member : scan.members()) {
            MemberId id = parseMember(member.member());
            if (id == null) continue;
            RootData root = readRoot(id.fanoutId());
            ShardData shard = readShard(id.fanoutId(), id.seq());
            if (root == null || shard == null) continue;
            RedisJobDefinition definition = workers.get(shard.workerName());
            if (definition == null) continue;
            scripts.list(RedisJobScript.RECOVER_EXPIRED,
                    new String[]{keys.fanoutShard(id.fanoutId(), id.seq()), keys.fanoutLeases(id.fanoutId()),
                            keys.fanoutReady(id.fanoutId()), keys.fanoutRoot(id.fanoutId()),
                            keys.fanoutReceipts(id.fanoutId()), keys.fanoutRoots(id.fanoutId()),
                            keys.fanoutCompletion(id.fanoutId()), keys.fanoutGc(id.fanoutId())},
                    member.member(), shard.workerName(), definition.maxAttempts(), definition.retryDelayMs(),
                    properties.getFanoutRetentionMs(), "FANOUT", id.fanoutId(), id.seq(),
                    root.failurePolicy().name(), "");
        }
        return scan;
    }

    /**
     * 业务作用：恢复 commit 后中断的首次批量投递，并轮转检查等待新能力的分片。
     *
     * @param fanoutId Fanout 标识
     *                 返回：无返回值。
     */
    void advanceRoot(String fanoutId) {
        RootData root = readRoot(fanoutId);
        if (root == null || terminal(root.state())) return;
        if ("CREATING".equals(root.state())) {
            scripts.list(RedisJobScript.FANOUT_FAIL_CREATING,
                    new String[]{keys.fanoutRoot(fanoutId), keys.fanoutRoots(fanoutId),
                            keys.fanoutCompletion(fanoutId), keys.fanoutGc(fanoutId)},
                    fanoutId, properties.getFanoutRetentionMs());
            return;
        }
        if ("CANCELLING".equals(root.state())) {
            cancelBatch(root);
            return;
        }
        if (("COMMITTED".equals(root.state()) || "WAITING_CHILDREN".equals(root.state()))
                && root.deliveryCursor() < root.shardTotal()) {
            int count = Math.min(properties.getFanoutDeliveryBatchSize(),
                    root.shardTotal() - root.deliveryCursor());
            List<ShardData> shards = new ArrayList<>(count);
            for (long seq = root.deliveryCursor(); seq < root.deliveryCursor() + count; seq++) {
                ShardData shard = readShard(fanoutId, seq);
                if (shard != null) shards.add(shard);
            }
            deliver(root, shards, true);
        }
        recoverCapabilities(root);
    }

    /**
     * 业务作用：读取桶内根状态，供普通 waiting 索引执行跨 slot 对账。
     *
     * @param fanoutId Fanout 标识
     * @return 根不存在时为空字符串。
     */
    String rootState(String fanoutId) {
        RootData root = readRoot(fanoutId);
        return root == null ? "" : root.state();
    }

    /**
     * 业务作用：先把桶内根切入 CANCELLING，禁止新 attempt，再按有界批次撤销未运行分片。
     *
     * @param fanoutId Fanout 标识
     * @param reason   取消原因
     *                 返回：无返回值。
     */
    void requestCancel(String fanoutId, String reason) {
        RootData root = readRoot(fanoutId);
        if (root == null || terminal(root.state())) return;
        cancelBatch(new RootData(root.fanoutId(), "CANCELLING", root.rootRunId(), root.rootAttempt(),
                root.rootJobName(), root.rootShard(), root.workerName(), root.contractRevision(), root.schemaId(),
                root.codec(), root.shardTotal(), root.deliveryCursor(), root.receiptTimeoutMs(),
                root.receiptMaxRetries(), root.failurePolicy(), root.expireAt(), root.cleanupCursor(),
                root.reconciledAt(), reason, root.capabilityCursor(), root.cancelCursor(), root.createDeadlineAt()));
    }

    /**
     * 业务作用：接收发起端 Pub/Sub 回执并复验持久 shard；消息只作快速确认，不能覆盖 Redis 状态。
     *
     * @param envelope 回执信封
     *                 返回：无返回值。
     */
    void onReceiptSignal(String envelope) {
        String[] fields = envelope.split("\\|", -1);
        if (fields.length != 4 || !"RECEIVED".equals(fields[3])) return;
        try {
            long seq = Long.parseLong(fields[1]);
            long epoch = Long.parseLong(fields[2]);
            ShardData shard = readShard(fields[0], seq);
            if (shard == null || shard.assignmentEpoch() != epoch || shard.state() != RedisJobState.RECEIVED) return;
            // `fanout_accept_shard.lua` 已在发布回执前删除 receipts，持久状态本身就是确认结果。
        } catch (NumberFormatException ignored) {
            // 非法信封不触发状态写入，持久索引仍会按原截止点重试。
        }
    }

    /**
     * 业务作用：推进一次有界取消批次；运行中 shard 只写协作式取消信号并等待完成或租约恢复。
     *
     * @param root Fanout 根数据
     *             返回：无返回值。
     */
    private void cancelBatch(RootData root) {
        int start = root.cancelCursor();
        int count = Math.min(properties.getFanoutCleanupBatchSize(), Math.max(0, root.shardTotal() - start));
        String[] scriptKeys = new String[7 + count * 2];
        scriptKeys[0] = keys.fanoutRoot(root.fanoutId());
        scriptKeys[1] = keys.fanoutRoots(root.fanoutId());
        scriptKeys[2] = keys.fanoutCompletion(root.fanoutId());
        scriptKeys[3] = keys.fanoutGc(root.fanoutId());
        scriptKeys[4] = keys.fanoutReceipts(root.fanoutId());
        scriptKeys[5] = keys.fanoutReady(root.fanoutId());
        scriptKeys[6] = keys.fanoutLeases(root.fanoutId());
        for (int index = 0; index < count; index++) {
            long seq = start + index;
            ShardData shard = readShard(root.fanoutId(), seq);
            String target = shard == null ? "cancel" : shard.targetNodeIdentity();
            scriptKeys[7 + index * 2] = keys.fanoutShard(root.fanoutId(), seq);
            scriptKeys[8 + index * 2] = keys.fanoutInbox(root.fanoutId(), target);
        }
        String reason = root.cancelReason() == null || root.cancelReason().isBlank()
                ? "CANCEL_REQUESTED" : root.cancelReason();
        scripts.list(RedisJobScript.FANOUT_CANCEL_BATCH, scriptKeys,
                root.fanoutId(), reason, properties.getFanoutRetentionMs(), start, count,
                INBOX_GROUP, properties.getMinScanIntervalMs());
    }

    /**
     * 业务作用：扫描接收截止索引，并按根策略重发、保持原节点、跳过或重分配。
     *
     * @param bucket Fanout 桶
     * @param scan   本轮到期成员与下一最小 score
     * @return 原扫描结果。
     */
    private RedisJobRepository.DueScan scanReceipts(int bucket, RedisJobRepository.DueScan scan) {
        for (RedisJobRepository.DueMember member : scan.members()) {
            MemberId id = parseMember(member.member());
            if (id == null) continue;
            RootData root = readRoot(id.fanoutId());
            ShardData shard = readShard(id.fanoutId(), id.seq());
            if (root == null || shard == null) continue;
            List<Object> response = scripts.list(RedisJobScript.FANOUT_RETRY_RECEIPT,
                    new String[]{keys.fanoutShard(id.fanoutId(), id.seq()), keys.fanoutReceipts(id.fanoutId()),
                            keys.fanoutNotifyChannel(id.fanoutId(), shard.targetNodeIdentity())},
                    id.fanoutId(), id.seq(), shard.assignmentEpoch(), root.receiptMaxRetries(),
                    root.receiptTimeoutMs(), publishCommand, root.failurePolicy().name(),
                    properties.getMaxScanIntervalMs());
            if ("RETRY_EXHAUSTED".equals(value(response, 0))) handleUnavailable(root, shard, "RECEIPT_TIMEOUT");
        }
        return scan;
    }

    /**
     * 业务作用：扫描已接收但尚未 start 的分片，重新唤醒稳定 inbox 或升级根失败策略。
     *
     * @param bucket Fanout 桶
     * @param scan   本轮到期成员与下一最小 score
     * @return 原扫描结果。
     */
    private RedisJobRepository.DueScan scanReady(int bucket, RedisJobRepository.DueScan scan) {
        for (RedisJobRepository.DueMember member : scan.members()) {
            MemberId id = parseMember(member.member());
            if (id == null) continue;
            RootData root = readRoot(id.fanoutId());
            ShardData shard = readShard(id.fanoutId(), id.seq());
            if (root == null || shard == null) continue;
            List<Object> response = scripts.list(RedisJobScript.FANOUT_PROMOTE_READY,
                    new String[]{keys.fanoutShard(id.fanoutId(), id.seq()), keys.fanoutReady(id.fanoutId()),
                            keys.fanoutNotifyChannel(id.fanoutId(), shard.targetNodeIdentity())},
                    id.fanoutId(), id.seq(), shard.assignmentEpoch(), properties.getReadyMaxWakeups(),
                    properties.getMinScanIntervalMs(), publishCommand, root.failurePolicy().name(),
                    properties.getMaxScanIntervalMs());
            if ("WAKEUP_EXHAUSTED".equals(value(response, 0))) handleUnavailable(root, shard, "START_TIMEOUT");
        }
        return scan;
    }

    /**
     * 业务作用：对已经确认无法由当前 assignment 启动的分片执行根级确定性失败策略。
     *
     * @param root   Fanout 根数据
     * @param shard  当前 shard 数据
     * @param reason 失败原因
     *               返回：无返回值。
     */
    private void handleUnavailable(RootData root, ShardData shard, String reason) {
        if (root.failurePolicy() == RedisJobFanoutFailurePolicy.STRICT_SNAPSHOT) return;
        registry.recordFanoutEvidence(shard.targetNodeIdentity(), shard.targetStartupId(),
                shard.targetHeartbeatRevision(), root.fanoutId());
        if (root.failurePolicy() == RedisJobFanoutFailurePolicy.BEST_EFFORT) {
            aggregateSkipped(root, shard, reason);
            return;
        }
        reassign(root, shard, compatibleTarget(root, shard.targetNodeIdentity()));
    }

    /**
     * 业务作用：在 CAS 撤销旧 inbox 与全部索引后切换目标，并通过统一投递协议建立新 assignment。
     *
     * @param root   Fanout 根数据
     * @param shard  当前 shard 数据
     * @param target 新目标；没有兼容节点时为 null
     *               返回：无返回值。
     */
    private void reassign(RootData root, ShardData shard, RedisJobExecutorMember target) {
        String targetNode = target == null ? "" : target.nodeIdentity();
        String targetStartup = target == null ? "" : target.startupId();
        int maxAssignments = properties.getFanoutMaxAssignments();
        List<Object> response = scripts.list(RedisJobScript.FANOUT_REASSIGN_SHARD,
                new String[]{keys.fanoutShard(root.fanoutId(), shard.seq()), keys.fanoutReceipts(root.fanoutId()),
                        keys.fanoutReady(root.fanoutId()), keys.fanoutLeases(root.fanoutId()),
                        keys.fanoutInbox(root.fanoutId(), shard.targetNodeIdentity())},
                root.fanoutId(), shard.seq(), shard.targetNodeIdentity(), shard.assignmentEpoch(),
                targetNode, targetStartup, target == null ? 0L : target.heartbeatRevision(),
                INBOX_GROUP, maxAssignments);
        if (!"OK".equals(value(response, 0)) || target == null) return;
        ShardData reassigned = readShard(root.fanoutId(), shard.seq());
        if (reassigned != null) deliver(root, List.of(reassigned), false);
    }

    /**
     * 业务作用：选择仍为 ACTIVE、fanoutReady 且契约兼容的其它稳定节点。
     *
     * @param root         Fanout 根数据
     * @param excludedNode 当前不可用节点
     * @return 候选节点；当前没有兼容节点时为 null。
     */
    private RedisJobExecutorMember compatibleTarget(RootData root, String excludedNode) {
        try {
            return registry.snapshot(root.workerName(), root.contractRevision(), root.schemaId(), root.codec())
                    .members().stream().filter(member -> !member.nodeIdentity().equals(excludedNode))
                    .findFirst().orElse(null);
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    /**
     * 业务作用：轮转复查 `AWAITING_CAPABILITY` 分片，使后来加入的兼容节点能够接管。
     *
     * @param root Fanout 根数据
     *             返回：无返回值。
     */
    private void recoverCapabilities(RootData root) {
        if (root.shardTotal() == 0) return;
        int start = Math.floorMod(root.capabilityCursor(), root.shardTotal());
        int count = Math.min(properties.getFanoutDeliveryBatchSize(), root.shardTotal());
        for (int offset = 0; offset < count; offset++) {
            long seq = (start + offset) % root.shardTotal();
            ShardData shard = readShard(root.fanoutId(), seq);
            if (shard != null && shard.state() == RedisJobState.AWAITING_CAPABILITY) {
                RedisJobExecutorMember target = compatibleTarget(root, shard.targetNodeIdentity());
                if (target != null) reassign(root, shard, target);
            }
        }
        int next = (start + count) % root.shardTotal();
        scripts.list(RedisJobScript.FANOUT_ADVANCE_CAPABILITY_CURSOR,
                new String[]{keys.fanoutRoot(root.fanoutId())}, root.capabilityCursor(), next);
    }

    /**
     * 业务作用：把 BEST_EFFORT 下无法启动的分片记为 SKIPPED，并参与唯一根聚合。
     *
     * @param root   Fanout 根数据
     * @param shard  shard 数据
     * @param reason 跳过原因
     *               返回：无返回值。
     */
    private void aggregateSkipped(RootData root, ShardData shard, String reason) {
        scripts.list(RedisJobScript.FANOUT_AGGREGATE,
                new String[]{keys.fanoutRoot(root.fanoutId()), keys.fanoutShard(root.fanoutId(), shard.seq()),
                        keys.fanoutReceipts(root.fanoutId()), keys.fanoutReady(root.fanoutId()),
                        keys.fanoutLeases(root.fanoutId()), keys.fanoutCompletion(root.fanoutId()),
                        keys.fanoutGc(root.fanoutId())},
                root.fanoutId(), shard.seq(), RedisJobState.SKIPPED.name(), "SKIPPED", reason,
                properties.getFanoutRetentionMs());
    }

    /**
     * 业务作用：以统一批量脚本建立稳定 inbox、权威 deadline、索引和同 slot 通知。
     *
     * @param root          Fanout 根数据
     * @param shards        待投递 assignment
     * @param advanceCursor 是否推进首次投递游标
     *                      返回：无返回值。
     */
    private void deliver(RootData root, List<ShardData> shards, boolean advanceCursor) {
        if (shards.isEmpty()) return;
        String[] scriptKeys = new String[2 + shards.size() * 3];
        scriptKeys[0] = keys.fanoutRoot(root.fanoutId());
        scriptKeys[1] = keys.fanoutReceipts(root.fanoutId());
        List<Object> args = new ArrayList<>();
        args.add(shards.size());
        args.add(root.fanoutId());
        args.add(publishCommand);
        args.add(advanceCursor ? 1 : 0);
        for (int index = 0; index < shards.size(); index++) {
            ShardData shard = shards.get(index);
            int keyIndex = 2 + index * 3;
            scriptKeys[keyIndex] = keys.fanoutShard(root.fanoutId(), shard.seq());
            scriptKeys[keyIndex + 1] = keys.fanoutInbox(root.fanoutId(), shard.targetNodeIdentity());
            scriptKeys[keyIndex + 2] = keys.fanoutNotifyChannel(root.fanoutId(), shard.targetNodeIdentity());
            args.add(shard.seq());
            args.add(shard.assignmentEpoch());
            args.add(RedisJobIdentifiers.shardRunId(root.fanoutId(), shard.seq()));
            args.add(root.receiptTimeoutMs());
        }
        scripts.list(RedisJobScript.FANOUT_DELIVER_BATCH, scriptKeys, args.toArray());
    }

    /**
     * 业务作用：按根清理游标精确删除到期 shard 与 inbox 消息，根记录始终最后删除。
     *
     * @param bucket Fanout 桶
     * @param scan   本轮到期成员与下一最小 score
     * @return 原扫描结果。
     */
    private RedisJobRepository.DueScan scanGc(int bucket, RedisJobRepository.DueScan scan) {
        for (RedisJobRepository.DueMember member : scan.members()) {
            RootData root = readRoot(member.member());
            if (root == null || root.reconciledAt() == 0 || !terminal(root.state())) continue;
            int remaining = root.shardTotal() - root.cleanupCursor();
            int count = Math.min(properties.getFanoutCleanupBatchSize(), Math.max(0, remaining));
            String[] scriptKeys = new String[6 + count * 2];
            scriptKeys[0] = keys.fanoutRoot(root.fanoutId());
            scriptKeys[1] = keys.fanoutGc(root.fanoutId());
            scriptKeys[2] = keys.fanoutRoots(root.fanoutId());
            scriptKeys[3] = keys.fanoutReceipts(root.fanoutId());
            scriptKeys[4] = keys.fanoutReady(root.fanoutId());
            scriptKeys[5] = keys.fanoutLeases(root.fanoutId());
            for (int index = 0; index < count; index++) {
                long seq = root.cleanupCursor() + index;
                ShardData shard = readShard(root.fanoutId(), seq);
                String target = shard == null ? "cleanup" : shard.targetNodeIdentity();
                scriptKeys[6 + index * 2] = keys.fanoutShard(root.fanoutId(), seq);
                scriptKeys[7 + index * 2] = keys.fanoutInbox(root.fanoutId(), target);
            }
            scripts.list(RedisJobScript.FANOUT_CLEANUP, scriptKeys,
                    root.fanoutId(), count, INBOX_GROUP);
        }
        return scan;
    }

    /**
     * 业务作用：依据一个 Fanout 索引的 Redis score 计算带当前执行器错峰的扫描间隔。
     *
     * @param scan      本轮索引结果
     * @param indexType 索引类型稳定编号
     * @param bucket    Fanout 桶
     * @return 有界扫描间隔。
     */
    private long delay(RedisJobRepository.DueScan scan, int indexType, int bucket) {
        long upperBound = switch (indexType) {
            case RedisJobScanInterval.FANOUT_RECEIPT, RedisJobScanInterval.FANOUT_READY -> Math.min(
                    workerReceiptScanUpperBoundMs, rootReceiptScanUpperBounds.get(bucket));
            case RedisJobScanInterval.FANOUT_LEASE -> properties.getLeaseMs() / 3L;
            default -> properties.getMaxScanIntervalMs();
        };
        return RedisJobScanInterval.delay(properties, registry.executorId(), scan.redisNow(), scan.nextScore(),
                !scan.members().isEmpty(), indexType, bucket, upperBound);
    }

    /**
     * 业务作用：在一个 Fanout slot 内用一次脚本读取 receipts、ready、lease 和 gc，减少期限兜底的空载调用。
     *
     * @param bucket Fanout 桶
     * @return 四类索引使用同一 Redis 时间的扫描结果。
     */
    private FanoutDueScans dueIndexes(int bucket) {
        List<Object> values = scripts.list(RedisJobScript.FANOUT_SCAN_DUE,
                new String[]{keys.fanoutRoots(bucket), keys.fanoutReceipts(bucket), keys.fanoutReady(bucket),
                        keys.fanoutLeases(bucket), keys.fanoutGc(bucket)}, properties.getScanBatchSize());
        long redisNow = number(values, 0);
        boolean hasRoots = number(values, 1) > 0L;
        int[] cursor = {2};
        RedisJobRepository.DueScan receipts = dueIndex(values, cursor, redisNow);
        RedisJobRepository.DueScan ready = dueIndex(values, cursor, redisNow);
        RedisJobRepository.DueScan leases = dueIndex(values, cursor, redisNow);
        RedisJobRepository.DueScan gc = dueIndex(values, cursor, redisNow);
        return new FanoutDueScans(hasRoots, receipts, ready, leases, gc);
    }

    /**
     * 业务作用：从 Fanout 复合扫描协议读取一类索引，并把游标推进到下一类索引。
     *
     * @param values   脚本复合返回
     * @param cursor   当前读取位置
     * @param redisNow 四类索引共享的 Redis 时间
     * @return 单类索引结果。
     */
    private RedisJobRepository.DueScan dueIndex(List<Object> values, int[] cursor, long redisNow) {
        long nextScore = optionalNumber(values, cursor[0]++);
        int count = integer(values, cursor[0]++);
        if (count == 0) return new RedisJobRepository.DueScan(redisNow, nextScore, List.of());
        List<RedisJobRepository.DueMember> members = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            members.add(new RedisJobRepository.DueMember(
                    value(values, cursor[0]++), number(values, cursor[0]++)));
        }
        return new RedisJobRepository.DueScan(redisNow, nextScore, List.copyOf(members));
    }

    /**
     * 业务作用：读取 Fanout 根的跨监视器权威字段。
     *
     * @param fanoutId Fanout 标识
     * @return 根不存在时为 null。
     */
    private RootData readRoot(String fanoutId) {
        List<Object> values = scripts.list(RedisJobScript.READ_FANOUT_ROOT,
                new String[]{keys.fanoutRoot(fanoutId)});
        if (values.isEmpty()) return null;
        return new RootData(value(values, 0), value(values, 1), value(values, 2), integer(values, 3),
                value(values, 4), integer(values, 5), value(values, 6), number(values, 7), value(values, 8),
                codec(values, 9), integer(values, 10), integer(values, 11), number(values, 12),
                integer(values, 13), RedisJobFanoutFailurePolicy.valueOf(value(values, 14)),
                number(values, 15), integer(values, 16), number(values, 17), value(values, 18),
                integer(values, 19), integer(values, 20), number(values, 21));
    }

    /**
     * 业务作用：读取 shard 的 assignment、索引和清理所需权威字段。
     *
     * @param fanoutId Fanout 标识
     * @param seq      稳定分片序号
     * @return shard 不存在时为 null。
     */
    private ShardData readShard(String fanoutId, long seq) {
        List<Object> values = scripts.list(RedisJobScript.READ_FANOUT_SHARD,
                new String[]{keys.fanoutShard(fanoutId, seq)});
        if (values.isEmpty()) return null;
        return new ShardData(value(values, 0), number(values, 9), value(values, 3), number(values, 4),
                value(values, 5), codec(values, 6), value(values, 11), value(values, 12), number(values, 13),
                integer(values, 14), RedisJobState.valueOf(value(values, 15)), value(values, 21),
                number(values, 27));
    }

    /**
     * 业务作用：把 `<fanoutId>:<seq>` 索引成员还原为定位字段。
     *
     * @param member 索引成员
     * @return 格式无效时为 null。
     */
    private static MemberId parseMember(String member) {
        int separator = member.lastIndexOf(':');
        if (separator <= 0 || separator == member.length() - 1) return null;
        try {
            return new MemberId(member.substring(0, separator), Long.parseLong(member.substring(separator + 1)));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    /**
     * 业务作用：判断 Fanout 聚合状态是否已经关闭所有新执行入口。 @param state 根状态 @return 终态返回 true。
     */
    private static boolean terminal(String state) {
        return "SUCCEEDED".equals(state) || "PARTIAL_FAILED".equals(state)
                || "FAILED".equals(state) || "CANCELLED".equals(state);
    }

    /**
     * 业务作用：安全读取脚本字符串字段。 @param values 返回值 @param index 下标 @return 字符串值。
     */
    private static String value(List<Object> values, int index) {
        return index >= values.size() || values.get(index) == null ? "" : Objects.toString(values.get(index));
    }

    /**
     * 业务作用：读取脚本长整数字段。 @param values 返回值 @param index 下标 @return 空值为零。
     */
    private static long number(List<Object> values, int index) {
        String value = value(values, index);
        return value.isEmpty() ? 0L : Long.parseLong(value);
    }

    /**
     * 业务作用：读取脚本可选长整数字段。 @param values 返回值 @param index 下标 @return 空值为零。
     */
    private static long optionalNumber(List<Object> values, int index) {
        return number(values, index);
    }

    /**
     * 业务作用：读取脚本整数字段并检查溢出。 @param values 返回值 @param index 下标 @return 空值为零。
     */
    private static int integer(List<Object> values, int index) {
        return Math.toIntExact(number(values, index));
    }

    /**
     * 业务作用：读取线编码；旧记录缺少字段时拒绝猜测。 @param values 返回值 @param index 下标 @return 线编码。
     */
    private static RedisJobWireCodec codec(List<Object> values, int index) {
        return RedisJobWireCodec.valueOf(value(values, index));
    }

    /**
     * 业务作用：承载 Fanout 根监视所需字段。
     */
    private record RootData(String fanoutId, String state, String rootRunId, int rootAttempt,
                            String rootJobName, int rootShard, String workerName, long contractRevision,
                            String schemaId, RedisJobWireCodec codec, int shardTotal, int deliveryCursor,
                            long receiptTimeoutMs, int receiptMaxRetries,
                            RedisJobFanoutFailurePolicy failurePolicy, long expireAt, int cleanupCursor,
                            long reconciledAt, String cancelReason, int capabilityCursor,
                            int cancelCursor, long createDeadlineAt) {
    }

    /**
     * 业务作用：承载 shard assignment 与清理所需字段。
     */
    private record ShardData(String fanoutId, long seq, String workerName, long contractRevision,
                             String schemaId, RedisJobWireCodec codec, String targetNodeIdentity,
                             String targetStartupId, long assignmentEpoch, int assignmentCount,
                             RedisJobState state, String inboxMessageId, long targetHeartbeatRevision) {
    }

    /**
     * 业务作用：承载索引成员的 Fanout 定位字段。
     */
    private record MemberId(String fanoutId, long seq) {
    }

    /**
     * 业务作用：承载同一 Fanout 桶四类恢复索引的一致时间扫描结果。
     */
    private record FanoutDueScans(boolean hasRoots, RedisJobRepository.DueScan receipts,
                                  RedisJobRepository.DueScan ready,
                                  RedisJobRepository.DueScan leases, RedisJobRepository.DueScan gc) {
    }
}
