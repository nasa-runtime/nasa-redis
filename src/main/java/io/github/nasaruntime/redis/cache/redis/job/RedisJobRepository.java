package io.github.nasaruntime.redis.cache.redis.job;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 业务作用：把普通任务的注册、触发、执行权、租约、完成与索引恢复封装为同 slot Lua 调用。
 */
final class RedisJobRepository {

    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final Map<String, JobKeys> definitions = new ConcurrentHashMap<>();
    private volatile RedisJobVisibleWakeup visibleWakeup;

    /**
     * 业务作用：绑定已校验配置、键路由器和脚本执行器。
     *
     * @param properties Job 配置
     * @param keys       键路由器
     * @param scripts    脚本执行器
     */
    RedisJobRepository(RedisJobProperties properties, RedisJobKeyspace keys, RedisJobScriptExecutor scripts) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.keys = Objects.requireNonNull(keys, "keys must not be null");
        this.scripts = Objects.requireNonNull(scripts, "scripts must not be null");
    }

    /**
     * 业务作用：连接写入端与本进程可见索引扫描器，使新截止点不受旧退避周期限制。
     *
     * @param wakeup 可见索引唤醒入口
     *               返回：无返回值。
     */
    void setVisibleWakeup(RedisJobVisibleWakeup wakeup) {
        this.visibleWakeup = Objects.requireNonNull(wakeup, "wakeup must not be null");
    }

    /**
     * 业务作用：原子登记任务定义、分片枚举索引和首个调度时刻。
     *
     * @param definition 任务定义
     * @param nextFireAt 首个逻辑时刻，非自动任务为零
     * @return 登记状态码。
     */
    String register(RedisJobDefinition definition, long nextFireAt) {
        JobKeys jobKeys = deriveKeys(definition);
        int shard = jobKeys.shard();
        List<Object> result = scripts.list(RedisJobScript.JOB_REGISTER,
                new String[]{keys.jobs(shard), keys.schedule(shard), jobKeys.job()},
                definition.name(), definition.definitionRevision(), definition.definitionDigest(),
                RedisJobDefinitionState.ENABLED.name(), shard, definition.workerName(),
                definition.workerKey(), definition.trigger().name(),
                definition.scheduleType().name(), definition.cron(), definition.zone().getId(),
                definition.intervalMs(), definition.concurrency().name(), definition.misfire().name(),
                definition.timeoutMs(), definition.maxAttempts(), definition.retryDelayMs(),
                definition.contractRevision(), definition.schemaId(), definition.wireCodecs(),
                definition.fanoutReceiptTimeoutMs(), definition.fanoutReceiptMaxRetries(),
                definition.fanoutFailurePolicy().name(), nextFireAt);
        String code = text(result, 0);
        if ("OK".equals(code) || "ADOPTED".equals(code)) definitions.put(definition.name(), jobKeys);
        return code;
    }

    /**
     * 业务作用：暂停任务并从调度索引移除，未 start 积压由可见性门禁冻结，已有执行权不受影响。
     *
     * @param jobName 任务名
     * @return 状态码。
     */
    String pause(String jobName) {
        int shard = keys.scheduleShard(jobName);
        return scripts.text(RedisJobScript.JOB_PAUSE,
                new String[]{keys.schedule(shard), keys.job(shard, jobName)}, jobName);
    }

    /**
     * 业务作用：恢复任务并从调用方计算的新基准重新开放调度。
     *
     * @param jobName    任务名
     * @param nextFireAt 新的下一逻辑时刻
     * @return 状态码。
     */
    String resume(String jobName, long nextFireAt) {
        int shard = keys.scheduleShard(jobName);
        return scripts.text(RedisJobScript.JOB_RESUME,
                new String[]{keys.schedule(shard), keys.job(shard, jobName)}, jobName, nextFireAt);
    }

    /**
     * 业务作用：以单调修订号删除定义并保留 tombstone，阻止旧进程重新登记过期配置。
     *
     * @param jobName  任务名
     * @param revision 删除修订号
     * @return 状态码。
     */
    String delete(String jobName, long revision) {
        int shard = keys.scheduleShard(jobName);
        String code = text(scripts.list(RedisJobScript.JOB_DELETE,
                new String[]{keys.jobs(shard), keys.schedule(shard), keys.job(shard, jobName)},
                jobName, revision, properties.getTombstoneRetentionMs()), 0);
        if ("OK".equals(code)) definitions.remove(jobName);
        return code;
    }

    /**
     * 业务作用：有界回收 tombstone 已到期的定义枚举和 fencing 字段，保留期内绝不删除。
     *
     * @param shard 调度分片
     * @return 删除数量。
     */
    long cleanupTombstones(int shard) {
        List<Object> result = scripts.list(RedisJobScript.JOB_CLEANUP_TOMBSTONES,
                new String[]{keys.jobs(shard), keys.schedule(shard), keys.fences(shard)},
                properties.getScanBatchSize(), keys.shardKeyPrefix(shard));
        return number(result, 0);
    }

    /**
     * 业务作用：显式选择当前持久定义解除冲突，避免节点心跳按启动顺序决定调度语义。
     *
     * @param definition 目标定义
     * @param nextFireAt 恢复后的下一逻辑时刻
     * @return 状态码。
     */
    String resolveConflict(RedisJobDefinition definition, long nextFireAt) {
        int shard = keys.scheduleShard(definition.name());
        return text(scripts.list(RedisJobScript.JOB_RESOLVE_CONFLICT,
                new String[]{keys.schedule(shard), keys.job(shard, definition.name())},
                definition.name(), definition.definitionRevision(), definition.definitionDigest(),
                RedisJobDefinitionState.ENABLED.name(), nextFireAt), 0);
    }

    /**
     * 业务作用：设置一个调度分片的命名空间门禁，新触发会在权威脚本中复验该状态。
     *
     * @param shard 调度分片
     * @param state ENABLED 或 PAUSED
     * @param actor 操作来源
     * @return 状态码。
     */
    String setNamespaceState(int shard, RedisJobDefinitionState state, String actor) {
        return text(scripts.list(RedisJobScript.NAMESPACE_SET_STATE,
                new String[]{keys.control(shard)}, state.name(), actor), 0);
    }

    /**
     * 业务作用：以 requestId 原子创建手工 Run、持久参数、派发消息和可见性索引。
     *
     * @param definition 任务定义
     * @param requestId  调用方幂等标识
     * @param payload    参数
     * @return 稳定 Run 标识。
     */
    String manualFire(RedisJobDefinition definition, String requestId, RedisJobPayload payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        if (payload.bytes().length > properties.getMaxParameterBytes()) {
            throw new IllegalArgumentException("parameter exceeds maxParameterBytes");
        }
        if (!definition.codecs().contains(payload.codec()) || !definition.schemaId().equals(payload.schemaId())) {
            throw new IllegalArgumentException("payload does not match registered worker contract");
        }
        JobKeys jobKeys = jobKeys(definition);
        int shard = jobKeys.shard();
        String runId = RedisJobIdentifiers.manualRunId(keys.namespace(), definition.name(),
                RedisJobNames.requireName(requestId, "requestId"));
        List<Object> result = scripts.list(RedisJobScript.MANUAL_FIRE,
                new String[]{jobKeys.job(), keys.run(shard, runId), keys.visible(shard),
                        jobKeys.dispatch(), keys.completion(shard), keys.control(shard)},
                runId, requestId, Base64.getEncoder().encodeToString(payload.bytes()),
                payload.schemaId(), payload.codec().name(), properties.getVisibilityTimeoutMs(),
                properties.getProtocolVersion(), shard, definition.name());
        String code = text(result, 0);
        if (!"OK".equals(code) && !"ADOPTED".equals(code)) {
            throw new IllegalStateException("manual fire rejected: " + code);
        }
        return runId;
    }

    /**
     * 业务作用：读取一个索引中到期成员及下一最小 score，供自适应扫描器安排下次访问。
     *
     * @param indexKey ZSET 索引键
     * @param limit    单次最大成员数
     * @return 扫描结果。
     */
    DueScan scanDue(String indexKey, int limit) {
        List<Object> result = scripts.list(RedisJobScript.SCAN_DUE, new String[]{indexKey}, limit);
        long now = number(result, 0);
        long nextScore = optionalNumber(result, 1);
        if (result.size() <= 2) return new DueScan(now, nextScore, List.of());
        List<DueMember> members = new ArrayList<>((result.size() - 2) / 2);
        for (int index = 2; index + 1 < result.size(); index += 2) {
            members.add(new DueMember(text(result, index), number(result, index + 1)));
        }
        return new DueScan(now, nextScore, List.copyOf(members));
    }

    /**
     * 业务作用：读取到期调度项及对应定义修订号，使扫描器无需为未变化定义搬运完整配置。
     *
     * @param shard 调度分片
     * @param limit 单次最大任务数
     * @return 调度扫描结果。
     */
    ScheduleDueScan scanScheduleDue(int shard, int limit) {
        List<Object> result = scripts.list(RedisJobScript.SCAN_DUE,
                new String[]{keys.schedule(shard)}, limit, "SCHEDULE", keys.shardKeyPrefix(shard));
        if (result.size() <= 2) {
            return new ScheduleDueScan(number(result, 0), optionalNumber(result, 1), List.of());
        }
        List<ScheduleDueMember> members = new ArrayList<>((result.size() - 2) / 3);
        for (int index = 2; index + 2 < result.size(); index += 3) {
            members.add(new ScheduleDueMember(text(result, index), number(result, index + 1),
                    number(result, index + 2)));
        }
        return new ScheduleDueScan(number(result, 0), optionalNumber(result, 1), List.copyOf(members));
    }

    /**
     * 业务作用：在任务分片内原子校验到期时刻、创建自动 Run 并推进下一调度时刻。
     *
     * @param definition         任务定义
     * @param logicalFireAt      本次逻辑时刻
     * @param proposedNextFireAt 下一逻辑时刻；fixed delay 为零
     * @param skipRun            是否只推进调度时刻而不创建 Run
     * @param misfire            是否记录为 MISFIRE 触发
     * @return 创建或跳过状态码。
     */
    String fireDue(RedisJobDefinition definition, long logicalFireAt, long proposedNextFireAt,
                   boolean skipRun, boolean misfire) {
        JobKeys jobKeys = jobKeys(definition);
        int shard = jobKeys.shard();
        String triggerType = misfire ? "MISFIRE" : definition.scheduleType().name();
        String runId = RedisJobIdentifiers.scheduledRunId(keys.namespace(), definition.name(), logicalFireAt,
                triggerType);
        RedisJobWireCodec codec = definition.codecs().iterator().next();
        List<Object> result = scripts.list(RedisJobScript.FIRE_DUE,
                new String[]{keys.schedule(shard), jobKeys.job(), keys.run(shard, runId),
                        keys.visible(shard), jobKeys.dispatch(), keys.control(shard)},
                definition.definitionRevision(), logicalFireAt, proposedNextFireAt, runId,
                definition.scheduleType().name(), properties.getVisibilityTimeoutMs(),
                properties.getProtocolVersion(), definition.name(), shard, codec.name(),
                definition.scheduleType().name(), skipRun ? "SKIP" : "RUN", triggerType);
        return text(result, 0);
    }

    /**
     * 业务作用：在一个调度分片内批量提交经过链式计算的到期项，每项独立 CAS 且共享一次 Redis 时间。
     *
     * @param shard    调度分片
     * @param requests 触发请求
     * @return 每个请求对应的状态码。
     */
    List<String> fireDueBatch(int shard, List<FireRequest> requests) {
        if (requests.isEmpty()) return List.of();
        String[] scriptKeys = new String[3 + requests.size() * 3];
        scriptKeys[0] = keys.schedule(shard);
        scriptKeys[1] = keys.visible(shard);
        scriptKeys[2] = keys.control(shard);
        List<Object> args = new ArrayList<>();
        args.add(requests.size());
        for (int index = 0; index < requests.size(); index++) {
            FireRequest request = requests.get(index);
            RedisJobDefinition definition = request.definition();
            JobKeys jobKeys = jobKeys(definition);
            String triggerType = request.misfire() ? "MISFIRE" : definition.scheduleType().name();
            String runId = RedisJobIdentifiers.scheduledRunId(keys.namespace(), definition.name(),
                    request.logicalFireAt(), triggerType);
            RedisJobWireCodec codec = definition.codecs().iterator().next();
            int keyOffset = 3 + index * 3;
            scriptKeys[keyOffset] = jobKeys.job();
            scriptKeys[keyOffset + 1] = keys.run(shard, runId);
            scriptKeys[keyOffset + 2] = jobKeys.dispatch();
            args.add(definition.definitionRevision());
            args.add(request.logicalFireAt());
            args.add(request.proposedNextFireAt());
            args.add(runId);
            args.add(definition.scheduleType().name());
            args.add(properties.getVisibilityTimeoutMs());
            args.add(properties.getProtocolVersion());
            args.add(definition.name());
            args.add(shard);
            args.add(codec.name());
            args.add(request.skipRun() ? "SKIP" : "RUN");
            args.add(triggerType);
            args.add(request.mustEndInFuture() ? 1 : 0);
        }
        List<Object> result = scripts.list(RedisJobScript.FIRE_DUE_BATCH, scriptKeys, args.toArray());
        List<String> codes = new ArrayList<>();
        for (int index = 1; index + 1 < result.size(); index += 2) codes.add(text(result, index));
        return List.copyOf(codes);
    }

    /**
     * 业务作用：原子取得一个普通 Run 的 attempt 执行权并确认当前 Stream 消息。
     *
     * @param definition 任务定义
     * @param runId      Run 标识
     * @param executorId 当前执行器
     * @param messageId  Stream 消息标识
     * @param group      消费组
     * @return start 状态及新的权威字段。
     */
    StartResult start(RedisJobDefinition definition, String runId, String executorId,
                      String messageId, String group) {
        JobKeys jobKeys = jobKeys(definition);
        int shard = jobKeys.shard();
        List<Object> result = scripts.list(RedisJobScript.START_RUN,
                new String[]{keys.run(shard, runId), keys.leases(shard), keys.visible(shard),
                        keys.running(shard), jobKeys.waitq(), keys.fences(shard),
                        jobKeys.dispatch(), keys.completion(shard), jobKeys.job()},
                definition.name(), runId, executorId, messageId, properties.getLeaseMs(),
                definition.concurrency().name(), properties.getMaxSerialBacklog(),
                properties.getVisibilityTimeoutMs(), group, properties.getRunRetentionMs(),
                "NORMAL", properties.getSerialOverflowPolicy().name(), properties.getProtocolVersion(),
                definition.definitionRevision(), definition.contractRevision(), definition.schemaId(),
                definition.wireCodecs(), keys.shardKeyPrefix(shard));
        return new StartResult(text(result, 0), integer(result, 1), number(result, 2),
                number(result, 3), number(result, 4));
    }

    /**
     * 业务作用：延后拉取后失去本地容量的 Run，并原子确认当前消息。
     *
     * @param definition 任务定义
     * @param runId      Run 标识
     * @param messageId  Stream 消息标识
     * @param group      消费组
     * @return 状态码。
     */
    String defer(RedisJobDefinition definition, String runId, String messageId, String group) {
        JobKeys jobKeys = jobKeys(definition);
        return defer(definition.name(), runId, messageId, group,
                jobKeys.dispatch(), properties.getMinScanIntervalMs());
    }

    /**
     * 业务作用：本节点缺少兼容 Handler 时确认已拉取消息，并把 Run 放回持久可见性索引等待其它节点。
     *
     * @param jobName   任务名
     * @param runId     Run 标识
     * @param messageId Stream 消息标识
     * @param group     消费组
     * @param stream    当前派发 Stream
     * @param delayMs   重新可见延迟
     * @return 状态码。
     */
    String defer(String jobName, String runId, String messageId, String group, String stream, long delayMs) {
        int shard = keys.scheduleShard(jobName);
        List<Object> response = scripts.list(RedisJobScript.DEFER_RUN,
                new String[]{keys.run(shard, runId), keys.visible(shard), stream},
                runId, messageId, delayMs, group);
        String code = text(response, 0);
        signalVisible(shard, response, 1);
        return code;
    }

    /**
     * 业务作用：续期当前 attempt；Fanout 创建阶段改为延长 waiting 创建截止点。
     *
     * @param definition   任务定义
     * @param runId        Run 标识
     * @param executorId   当前 owner
     * @param attemptToken 当前 token
     * @return 续期状态和服务端截止时间。
     */
    RenewResult renew(RedisJobDefinition definition, String runId, String executorId, long attemptToken) {
        int shard = jobKeys(definition).shard();
        List<Object> result = scripts.list(RedisJobScript.RENEW_RUN,
                new String[]{keys.run(shard, runId), keys.leases(shard), keys.waiting(shard)},
                runId, executorId, attemptToken, properties.getLeaseMs());
        return new RenewResult(text(result, 0), number(result, 1), number(result, 2),
                !text(result, 3).isEmpty());
    }

    /**
     * 业务作用：提交当前 attempt 的结果，原子释放执行权并进入重试或终态。
     *
     * @param definition   任务定义
     * @param runId        Run 标识
     * @param executorId   当前 owner
     * @param attemptToken 当前 token
     * @param result       Handler 结果
     * @return 完成状态码和新 Run 状态。
     */
    FinishResult finish(RedisJobDefinition definition, String runId, String executorId,
                        long attemptToken, RedisJobResult result) {
        JobKeys jobKeys = jobKeys(definition);
        int shard = jobKeys.shard();
        String summary = limit(result.summary(), properties.getMaxResultSummaryBytes());
        List<Object> response = scripts.list(RedisJobScript.FINISH_RUN,
                new String[]{keys.run(shard, runId), keys.leases(shard), keys.visible(shard), keys.waiting(shard),
                        keys.running(shard), jobKeys.waitq(), keys.completion(shard),
                        jobKeys.dispatch(), jobKeys.job(),
                        keys.schedule(shard)},
                runId, executorId, attemptToken, definition.name(), result.code().name(), summary,
                definition.maxAttempts(), retryDelay(definition, runId), properties.getRunRetentionMs(),
                "NORMAL", properties.getVisibilityTimeoutMs(), 0L, 0L, keys.shardKeyPrefix(shard));
        long wakeDelayMs = signedNumber(response, 3, -1L);
        signalVisible(shard, response, 3);
        return new FinishResult(text(response, 0), text(response, 1), wakeDelayMs);
    }

    /**
     * 业务作用：恢复服务端租约确已到期的 Run，并撤销旧 owner 的状态权威。
     *
     * @param definition 任务定义
     * @param runId      Run 标识
     * @return 恢复状态和新 Run 状态。
     */
    FinishResult recoverExpired(RedisJobDefinition definition, String runId) {
        JobKeys jobKeys = jobKeys(definition);
        int shard = jobKeys.shard();
        List<Object> response = scripts.list(RedisJobScript.RECOVER_EXPIRED,
                new String[]{keys.run(shard, runId), keys.leases(shard), keys.visible(shard), keys.running(shard),
                        jobKeys.waitq(), keys.waiting(shard), keys.completion(shard),
                        jobKeys.job(), keys.schedule(shard), jobKeys.dispatch()},
                runId, definition.name(), definition.maxAttempts(), retryDelay(definition, runId),
                properties.getRunRetentionMs(), "NORMAL", properties.getVisibilityTimeoutMs(), 0L, 0L,
                keys.shardKeyPrefix(shard));
        long wakeDelayMs = signedNumber(response, 3, -1L);
        signalVisible(shard, response, 3);
        return new FinishResult(text(response, 0), text(response, 1), wakeDelayMs);
    }

    /**
     * 业务作用：提升到期可见性成员并重建丢失的 Dispatch 消息。
     *
     * @param shard 调度分片
     * @return 提升数量和下一最小 score。
     */
    PromotionResult promoteVisible(int shard) {
        List<Object> result = scripts.list(RedisJobScript.PROMOTE_VISIBLE,
                new String[]{keys.visible(shard)}, properties.getScanBatchSize(), properties.getVisibilityTimeoutMs(),
                properties.getMaxDispatchAttempts(), properties.getMaxScanIntervalMs(), keys.shardKeyPrefix(shard));
        return new PromotionResult(number(result, 0), optionalNumber(result, 1), number(result, 2));
    }

    /**
     * 业务作用：请求取消普通 Run；未启动 Run 立即终态，运行中 Run 由续期捎带信号。
     *
     * @param definition 任务定义
     * @param runId      Run 标识
     * @return 状态码。
     */
    String cancel(RedisJobDefinition definition, String runId) {
        JobKeys jobKeys = jobKeys(definition);
        int shard = jobKeys.shard();
        List<Object> response = scripts.list(RedisJobScript.REQUEST_CANCEL,
                new String[]{keys.run(shard, runId), keys.visible(shard), keys.leases(shard), keys.waiting(shard),
                        keys.running(shard), jobKeys.waitq(), keys.completion(shard),
                        jobKeys.job(), keys.schedule(shard)},
                runId, definition.name(), properties.getRunRetentionMs(), keys.shardKeyPrefix(shard));
        String code = text(response, 0);
        signalVisible(shard, response, 1);
        return code;
    }

    /**
     * 业务作用：在 Fanout 根从未建立或未提交时收敛已到创建截止点的普通根 Run。
     *
     * @param definition 根任务定义
     * @param runId      普通根 Run 标识
     * @return 完成状态码和新状态。
     */
    FinishResult failWaitingCreation(RedisJobDefinition definition, String runId) {
        JobKeys jobKeys = jobKeys(definition);
        int shard = jobKeys.shard();
        List<Object> response = scripts.list(RedisJobScript.FAIL_WAITING_CREATION,
                new String[]{keys.run(shard, runId), keys.waiting(shard), keys.running(shard),
                        keys.visible(shard), jobKeys.waitq(), keys.completion(shard)},
                runId, definition.name(), properties.getRunRetentionMs(), properties.getVisibilityTimeoutMs(),
                keys.shardKeyPrefix(shard));
        long wakeDelayMs = signedNumber(response, 3, -1L);
        signalVisible(shard, response, 3);
        return new FinishResult(text(response, 0), text(response, 1), wakeDelayMs);
    }

    /**
     * 业务作用：读取 Run 持久字段并转换为查询模型。
     *
     * @param jobName 任务名
     * @param runId   Run 标识
     * @return Run 不存在时为空。
     */
    Optional<RunData> read(String jobName, String runId) {
        int shard = keys.scheduleShard(jobName);
        return readAtShard(shard, runId);
    }

    /**
     * 业务作用：在已知调度分片时读取 Run，供索引监视器和管理查询避免依赖任务名。
     *
     * @param shard 调度分片
     * @param runId Run 标识
     * @return Run 不存在时为空。
     */
    Optional<RunData> readAtShard(int shard, String runId) {
        List<Object> values = scripts.list(RedisJobScript.READ_RUN, new String[]{keys.run(shard, runId)});
        if (values.isEmpty()) return Optional.empty();
        RedisJobRun run = new RedisJobRun(text(values, 0), text(values, 1), text(values, 2),
                RedisJobState.valueOf(text(values, 3)), number(values, 4), number(values, 5),
                integer(values, 6), number(values, 7), text(values, 8), number(values, 9),
                text(values, 10), text(values, 11), text(values, 12), text(values, 13));
        byte[] payload = text(values, 14).isEmpty() ? new byte[0] : Base64.getDecoder().decode(text(values, 14));
        RedisJobWireCodec codec = text(values, 16).isEmpty() ? RedisJobWireCodec.RAW
                : RedisJobWireCodec.valueOf(text(values, 16));
        return Optional.of(new RunData(run, payload, text(values, 15), codec,
                text(values, 17), text(values, 18), integer(values, 19), number(values, 20)));
    }

    /**
     * 业务作用：按 attempt 使用确定性抖动计算指数退避，避免同批失败同时重新冲击 Redis。
     *
     * @param definition 任务定义
     * @param runId      Run 标识
     * @return 退避毫秒数。
     */
    private long retryDelay(RedisJobDefinition definition, String runId) {
        long base = definition.retryDelayMs();
        long jitter = Math.floorMod(runId.hashCode(), Math.max(1L, base / 5L));
        return Math.min(base + jitter, properties.getMaxRunDurationMs());
    }

    /**
     * 业务作用：读取与定义修订号绑定的本地派生键；修订变化时整体替换，避免并发读取到混合路由。
     *
     * @param definition 任务定义
     * @return 同一修订号的派生键集合。
     */
    private JobKeys jobKeys(RedisJobDefinition definition) {
        JobKeys current = definitions.get(definition.name());
        if (current != null && current.definitionRevision() == definition.definitionRevision()) return current;
        JobKeys replacement = deriveKeys(definition);
        definitions.put(definition.name(), replacement);
        return replacement;
    }

    /**
     * 业务作用：在登记或定义修订变化时一次生成任务固定键，运行期不再重复摘要和变量段拼接。
     *
     * @param definition 任务定义
     * @return 自洽的派生键集合。
     */
    private JobKeys deriveKeys(RedisJobDefinition definition) {
        int shard = keys.scheduleShard(definition.name());
        return new JobKeys(shard, definition.definitionRevision(), keys.job(shard, definition.name()),
                keys.waitq(shard, definition.name()), keys.dispatchByWorkerKey(shard, definition.workerKey()));
    }

    /**
     * 业务作用：只在脚本确实新增或提前可见成员时通知扫描器；持久写入已经成功，唤醒异常不能回滚结果。
     *
     * @param shard  调度分片
     * @param values 脚本返回值
     * @param index  相对唤醒时长所在下标
     *               返回：无返回值。
     */
    private void signalVisible(int shard, List<Object> values, int index) {
        RedisJobVisibleWakeup wakeup = visibleWakeup;
        if (wakeup == null || index >= values.size()) return;
        long delayMs = signedNumber(values, index, -1L);
        if (delayMs < 0L) return;
        try {
            wakeup.wake(shard, delayMs);
        } catch (RuntimeException ignored) {
            // 持久索引仍由跨节点有界扫描兜底，局部唤醒不能改变已提交状态。
        }
    }

    /**
     * 业务作用：按 UTF-8 字节上限截断摘要，避免多字节字符切到半个编码单元。
     *
     * @param value    原摘要
     * @param maxBytes 最大字节数
     * @return 有界摘要。
     */
    private static String limit(String value, int maxBytes) {
        if (value == null || value.isEmpty()) return "";
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) return value;
        int length = maxBytes;
        while (length > 0 && (bytes[length] & 0xC0) == 0x80) length--;
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：安全读取脚本复合返回中的字符串字段。
     *
     * @param values 返回列表
     * @param index  字段下标
     * @return 缺失或 null 时为空串。
     */
    private static String text(List<Object> values, int index) {
        return index >= values.size() || values.get(index) == null ? "" : values.get(index).toString();
    }

    /**
     * 业务作用：安全读取脚本复合返回中的整数时间或 token。
     *
     * @param values 返回列表
     * @param index  字段下标
     * @return 缺失时为零。
     */
    private static long number(List<Object> values, int index) {
        String value = text(values, index);
        return value.isEmpty() ? 0L : Long.parseLong(value);
    }

    /**
     * 业务作用：读取允许为空的下次扫描时刻。
     *
     * @param values 返回列表
     * @param index  字段下标
     * @return 无成员时为零。
     */
    private static long optionalNumber(List<Object> values, int index) {
        return number(values, index);
    }

    /**
     * 业务作用：读取允许使用负数哨兵的脚本字段，区分立即唤醒与没有新增可见成员。
     *
     * @param values       返回列表
     * @param index        字段下标
     * @param defaultValue 字段缺失时的值
     * @return 脚本长整数或缺省值。
     */
    private static long signedNumber(List<Object> values, int index, long defaultValue) {
        String value = text(values, index);
        return value.isEmpty() ? defaultValue : Long.parseLong(value);
    }

    /**
     * 业务作用：把脚本整数转换为 Java attempt 下标并检查溢出。
     *
     * @param values 返回列表
     * @param index  字段下标
     * @return 整型值。
     */
    private static int integer(List<Object> values, int index) {
        return Math.toIntExact(number(values, index));
    }

    /**
     * 业务作用：承载一个到期索引成员及其权威 score。
     */
    record DueMember(String member, long score) {
    }

    /**
     * 业务作用：承载一个定义修订号内不变的调度分片与变量段键。
     */
    private record JobKeys(int shard, long definitionRevision, String job, String waitq, String dispatch) {
    }

    /**
     * 业务作用：承载一次自适应索引扫描结果。
     */
    record DueScan(long redisNow, long nextScore, List<DueMember> members) {
    }

    /**
     * 业务作用：承载调度项的定义修订号与权威 score。
     */
    record ScheduleDueMember(String jobName, long score, long definitionRevision) {
    }

    /**
     * 业务作用：承载调度专用扫描结果。
     */
    record ScheduleDueScan(long redisNow, long nextScore, List<ScheduleDueMember> members) {
    }

    /**
     * 业务作用：承载批量脚本中一项链式触发决策。
     */
    record FireRequest(RedisJobDefinition definition, long logicalFireAt, long proposedNextFireAt,
                       boolean skipRun, boolean misfire, boolean mustEndInFuture) {
    }

    /**
     * 业务作用：承载原子 start 返回的执行权字段。
     */
    record StartResult(String code, int attempt, long attemptToken, long redisNow, long leaseUntil) {
    }

    /**
     * 业务作用：承载续期结果和取消信号。
     */
    record RenewResult(String code, long redisNow, long deadline, boolean cancellationRequested) {
    }

    /**
     * 业务作用：承载完成或恢复后的状态。
     */
    record FinishResult(String code, String state, long wakeDelayMs) {
    }

    /**
     * 业务作用：承载可见性提升数量和下一扫描时刻。
     */
    record PromotionResult(long promoted, long nextScore, long redisNow) {
    }

    /**
     * 业务作用：承载执行与查询共同使用的 Run 数据和协议参数。
     */
    record RunData(RedisJobRun run, byte[] payload, String schemaId, RedisJobWireCodec codec,
                   String fanoutId, String snapshotId, int rootAttempt, long createDeadlineAt) {
    }
}
