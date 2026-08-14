package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 业务作用：作为 RedisJob 门面统一管理定义、触发、查询、调度扫描、租约恢复与 Fanout 生命周期。
 */
@Slf4j
public final class RedisJobScheduler implements SmartLifecycle, AutoCloseable {

    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final RedisJobRepository repository;
    private final RedisJobJsonCodec jsonCodec;
    private final RedisJobExecutorRegistry registry;
    private final RedisJobFanoutCoordinator fanoutCoordinator;
    private final RedisJobFanoutMonitor fanoutMonitor;
    private final RedisJobLeaseRenewer leaseRenewer;
    private final RedisJobDispatcher dispatcher;
    private final RedisJobFanoutDispatcher fanoutDispatcher;
    private final ScheduledExecutorService monitors;
    private final AtomicLongArray fanoutIndexEpochs;
    private final RedisJobMetrics metrics = new RedisJobMetrics();
    private final Map<String, RedisJobDefinition> definitions = new ConcurrentHashMap<>();
    private final Map<String, RedisJobHandler> handlers = new ConcurrentHashMap<>();
    private volatile int[] localShardSnapshot = new int[0];
    private volatile long visibleScanUpperBoundMs;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicLong lastHeartbeatAt = new AtomicLong();
    private final AtomicLong lastScheduleScanAt = new AtomicLong();

    /**
     * 业务作用：按指定 RedisProxy 建立完整调度运行时，但在 Spring 生命周期 start 前不领取任务。
     *
     * @param redisProxy Redis 命令代理
     * @param properties Job 配置
     */
    public RedisJobScheduler(RedisProxy redisProxy, RedisJobProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null").validate();
        this.visibleScanUpperBoundMs = this.properties.getMaxScanIntervalMs();
        RedisProxy selected = RedisProxy.load(properties.getQualifier());
        if (selected == null) selected = Objects.requireNonNull(redisProxy, "redisProxy must not be null");
        this.keys = new RedisJobKeyspace(properties.getNamespace(), properties.getShardCount(),
                properties.getFanoutBucketCount());
        this.scripts = new RedisJobScriptExecutor(selected);
        this.repository = new RedisJobRepository(properties, keys, scripts);
        this.jsonCodec = new RedisJobJsonCodec();
        this.registry = new RedisJobExecutorRegistry(properties, keys, scripts);
        this.fanoutCoordinator = new RedisJobFanoutCoordinator(properties, keys, scripts, registry, jsonCodec);
        this.fanoutMonitor = new RedisJobFanoutMonitor(properties, keys, scripts, registry);
        this.leaseRenewer = new RedisJobLeaseRenewer(properties, keys, scripts, registry.executorId());
        this.dispatcher = new RedisJobDispatcher(selected, properties, keys, repository, jsonCodec,
                fanoutCoordinator, registry, leaseRenewer, metrics);
        this.fanoutDispatcher = new RedisJobFanoutDispatcher(selected, properties, keys, scripts, registry,
                jsonCodec, fanoutMonitor::onReceiptSignal, leaseRenewer, metrics);
        this.monitors = Executors.newScheduledThreadPool(4,
                Thread.ofPlatform().daemon().name("redis-job-monitor-", 0).factory());
        this.fanoutIndexEpochs = new AtomicLongArray(keys.fanoutBucketCount());
        this.repository.setVisibleWakeup(this::wakeVisible);
        this.fanoutCoordinator.setVisibleWakeup(this::wakeVisible);
        this.fanoutCoordinator.setFanoutIndexWakeup(this::wakeFanoutIndex);
    }

    /**
     * 业务作用：登记任务定义与 Handler；相同修订号的本地冲突在写 Redis 前立即拒绝。
     *
     * @param definition 任务定义
     * @param handler    Handler
     *                   返回：无返回值；定义冲突或 Redis 拒绝登记时抛出异常。
     */
    public void register(RedisJobDefinition definition, RedisJobHandler handler) {
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        RedisJobDefinition current = definitions.get(definition.name());
        if (current != null && current.definitionRevision() == definition.definitionRevision()
                && !current.definitionDigest().equals(definition.definitionDigest())) {
            throw new IllegalStateException("conflicting local RedisJob definition: " + definition.name());
        }
        if (current != null && current.definitionRevision() > definition.definitionRevision()) return;
        long nextFireAt = initialNextFireAt(definition);
        String code = repository.register(definition, nextFireAt);
        if (!"OK".equals(code) && !"ADOPTED".equals(code)) {
            throw new IllegalStateException("RedisJob definition rejected: " + code);
        }
        definitions.put(definition.name(), definition);
        refreshLocalShards();
        handlers.put(definition.name(), handler);
        fanoutCoordinator.register(definition);
        fanoutMonitor.register(definition);
        dispatcher.register(definition, handler);
        if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) fanoutDispatcher.register(definition, handler);
        if (running.get()) registry.register(definition);
    }

    /**
     * 业务作用：编码 JSON 参数后以 requestId 幂等触发一个已登记普通任务。
     *
     * @param jobName   任务名
     * @param requestId 请求幂等标识
     * @param parameter 业务参数
     * @return 稳定 Run 标识。
     */
    public String triggerJson(String jobName, String requestId, Object parameter) {
        RedisJobDefinition definition = requireDefinition(jobName);
        return trigger(jobName, requestId, jsonCodec.encode(definition.schemaId(), parameter));
    }

    /**
     * 业务作用：以已编码参数和 requestId 幂等触发一个已登记普通任务。
     *
     * @param jobName   任务名
     * @param requestId 请求幂等标识
     * @param payload   已编码参数
     * @return 稳定 Run 标识。
     */
    public String trigger(String jobName, String requestId, RedisJobPayload payload) {
        return repository.manualFire(requireDefinition(jobName), requestId, payload);
    }

    /**
     * 业务作用：暂停任务的新触发与尚未 start 的积压，不撤销已经取得的 attempt 执行权。
     *
     * @param jobName 任务名
     *                返回：无返回值。
     */
    public void pause(String jobName) {
        String code = repository.pause(requireDefinition(jobName).name());
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob pause rejected: " + code);
    }

    /**
     * 业务作用：从当前 Redis 时刻之后重新计算任务下一逻辑时刻并恢复自动触发。
     *
     * @param jobName 任务名
     *                返回：无返回值。
     */
    public void resume(String jobName) {
        RedisJobDefinition definition = requireDefinition(jobName);
        String code = repository.resume(jobName, initialNextFireAt(definition));
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob resume rejected: " + code);
    }

    /**
     * 业务作用：以更高或相同的显式修订号删除定义，并保留足以覆盖滚动部署的 tombstone。
     *
     * @param jobName  任务名
     * @param revision 删除修订号
     *                 返回：无返回值；修订号落后或任务不存在时抛出异常。
     */
    public void delete(String jobName, long revision) {
        RedisJobDefinition definition = requireDefinition(jobName);
        if (revision < definition.definitionRevision()) {
            throw new IllegalArgumentException("delete revision must not be smaller than current definition revision");
        }
        String code = repository.delete(jobName, revision);
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob delete rejected: " + code);
        definitions.remove(jobName, definition);
        refreshLocalShards();
        handlers.remove(jobName);
        fanoutMonitor.unregister(definition);
    }

    /**
     * 业务作用：显式选择本地已登记摘要解除持久定义冲突，避免集群按节点启动顺序自动裁决。
     *
     * @param jobName 任务名
     *                返回：无返回值；目标摘要不是当前持久摘要时抛出异常。
     */
    public void resolveConflict(String jobName) {
        RedisJobDefinition definition = requireDefinition(jobName);
        String code = repository.resolveConflict(definition, initialNextFireAt(definition));
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob conflict resolution rejected: " + code);
    }

    /**
     * 业务作用：最终一致地关闭全部调度分片的新触发门禁，不撤销已经取得的执行权。
     *
     * @param actor 操作来源
     *              返回：无返回值；任一分片拒绝时抛出异常。
     */
    public void pauseNamespace(String actor) {
        setNamespaceState(RedisJobDefinitionState.PAUSED, actor);
    }

    /**
     * 业务作用：最终一致地重新开放全部调度分片的新触发门禁。
     *
     * @param actor 操作来源
     *              返回：无返回值；任一分片拒绝时抛出异常。
     */
    public void resumeNamespace(String actor) {
        setNamespaceState(RedisJobDefinitionState.ENABLED, actor);
    }

    /**
     * 业务作用：逐分片传播命名空间状态，Lua 触发路径仍逐次复验本分片门禁。
     *
     * @param state 目标状态
     * @param actor 操作来源
     *              返回：无返回值。
     */
    private void setNamespaceState(RedisJobDefinitionState state, String actor) {
        String selectedActor = RedisJobNames.requireName(actor, "actor");
        for (int shard = 0; shard < keys.shardCount(); shard++) {
            String code = repository.setNamespaceState(shard, state, selectedActor);
            if (!"OK".equals(code)) throw new IllegalStateException("RedisJob namespace state rejected: " + code);
        }
    }

    /**
     * 业务作用：请求取消一个 Run，运行中任务通过续期响应获得协作式取消信号。
     *
     * @param jobName 任务名
     * @param runId   Run 标识
     *                返回：无返回值。
     */
    public void cancel(String jobName, String runId) {
        RedisJobDefinition definition = requireDefinition(jobName);
        String code = repository.cancel(definition, runId);
        if (!"OK".equals(code) && !"ALREADY_COMPLETED".equals(code)) {
            throw new IllegalStateException("RedisJob cancellation rejected: " + code);
        }
        repository.read(definition.name(), runId).ifPresent(data -> {
            if (!data.fanoutId().isEmpty()) fanoutMonitor.requestCancel(data.fanoutId(), "CANCEL_REQUESTED");
        });
    }

    /**
     * 业务作用：按任务名和 Run 标识查询权威持久状态。
     *
     * @param jobName 任务名
     * @param runId   Run 标识
     * @return Run 不存在时为空。
     */
    public Optional<RedisJobRun> findRun(String jobName, String runId) {
        return repository.read(jobName, runId).map(RedisJobRepository.RunData::run);
    }

    /**
     * 业务作用：在管理查询缺少任务名时逐调度分片定位 Run；该入口不用于执行热路径。
     *
     * @param runId Run 标识
     * @return Run 不存在时为空。
     */
    public Optional<RedisJobRun> findRun(String runId) {
        for (int shard = 0; shard < keys.shardCount(); shard++) {
            Optional<RedisJobRepository.RunData> data = repository.readAtShard(shard, runId);
            if (data.isPresent()) return data.map(RedisJobRepository.RunData::run);
        }
        return Optional.empty();
    }

    /**
     * 业务作用：暴露当前进程的基础指标容器，便于应用桥接 Micrometer 或其它观测系统。
     *
     * <p>参数说明: 无。
     *
     * @return 指标容器。
     */
    public RedisJobMetrics metrics() {
        return metrics;
    }

    /**
     * 业务作用：根据生命周期、draining 和最近控制面成功时间给出调度健康状态。
     *
     * <p>参数说明: 无。
     *
     * @return 当前健康摘要。
     */
    public RedisJobHealth health() {
        if (!running.get()) return new RedisJobHealth("DOWN", false, draining.get(),
                lastHeartbeatAt.get(), lastScheduleScanAt.get());
        long heartbeatAge = System.currentTimeMillis() - lastHeartbeatAt.get();
        String status = draining.get() ? "DRAINING"
                : heartbeatAge > properties.getExecutorExpireMs() ? "DEGRADED" : "UP";
        return new RedisJobHealth(status, true, draining.get(),
                lastHeartbeatAt.get(), lastScheduleScanAt.get());
    }

    /**
     * 业务作用：启动 Fanout 通知门禁、能力登记、普通派发和所有持久索引监视器。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    @Override
    public void start() {
        if (closed.get()) throw new IllegalStateException("RedisJobScheduler is closed");
        if (!running.compareAndSet(false, true)) return;
        draining.set(false);
        metrics.gauge("redis_job_executor_capacity", properties.getExecutorCapacity());
        fanoutDispatcher.start();
        definitions.values().forEach(registry::register);
        dispatcher.start();
        monitors.scheduleWithFixedDelay(this::safeHeartbeat, properties.getHeartbeatMs(),
                properties.getHeartbeatMs(), TimeUnit.MILLISECONDS);
        monitors.schedule(this::scheduleScanCycle, 0L, TimeUnit.MILLISECONDS);
        monitors.schedule(this::indexScanCycle, 0L, TimeUnit.MILLISECONDS);
        monitors.schedule(this::fanoutRootScanCycle, 0L, TimeUnit.MILLISECONDS);
        for (int bucket = 0; bucket < keys.fanoutBucketCount(); bucket++) {
            scheduleFanoutIndexScan(bucket, 0L, fanoutIndexEpochs.get(bucket));
        }
        long registryGcInterval = Math.max(properties.getHeartbeatMs(),
                Math.min(properties.getRegistryGcGraceMs() / 4L, properties.getMaxScanIntervalMs() * 10L));
        monitors.scheduleWithFixedDelay(this::safeRegistryGc, registryGcInterval,
                registryGcInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：扫描本节点相关调度分片并通过 Lua CAS 生成唯一自动 Run。
     *
     * <p>参数说明: 无。
     *
     * @return 下一次调度扫描的有界间隔。
     */
    private long scanSchedules() {
        long nextDelay = properties.getMaxScanIntervalMs();
        for (int shard : localShards()) {
            for (int retry = 0; retry < properties.getNeedRecomputeMaxRetries(); retry++) {
                RedisJobRepository.ScheduleDueScan scan = repository.scanScheduleDue(
                        shard, properties.getScanBatchSize());
                metrics.increment("redis_job_due_scan_total");
                List<RedisJobRepository.FireRequest> requests = scan.members().isEmpty()
                        ? List.of() : new ArrayList<>();
                for (RedisJobRepository.ScheduleDueMember member : scan.members()) {
                    RedisJobDefinition definition = definitions.get(member.jobName());
                    if (definition == null || definition.trigger() == RedisJobTrigger.FANOUT_ONLY
                            || definition.definitionRevision() != member.definitionRevision()) continue;
                    metrics.gauge("redis_job_schedule_lag_ms",
                            Math.max(0L, scan.redisNow() - member.score()));
                    requests.addAll(fireRequests(definition, member.score(), scan.redisNow()));
                }
                List<String> codes = fireDueBatches(shard, requests);
                for (String code : codes) {
                    metrics.incrementClassified("redis_job_fire", code, "OK");
                }
                if (codes.stream().noneMatch("NEED_RECOMPUTE"::equals)) {
                    nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                            scan.redisNow(), scan.nextScore(), !scan.members().isEmpty(),
                            RedisJobScanInterval.SCHEDULE, shard, properties.getMaxScanIntervalMs()));
                    break;
                }
            }
        }
        return nextDelay;
    }

    /**
     * 业务作用：把大批候选切成有界脚本调用，并在需要重算时停止后续链式提交。
     *
     * @param shard    调度分片
     * @param requests 触发请求
     * @return 已执行项的状态码。
     */
    private List<String> fireDueBatches(int shard, List<RedisJobRepository.FireRequest> requests) {
        if (requests.isEmpty()) return List.of();
        List<String> result = new ArrayList<>();
        for (int start = 0; start < requests.size(); start += properties.getScanBatchSize()) {
            List<String> batch = repository.fireDueBatch(shard,
                    requests.subList(start, Math.min(requests.size(), start + properties.getScanBatchSize())));
            result.addAll(batch);
            if (batch.stream().anyMatch("NEED_RECOMPUTE"::equals)) break;
        }
        return List.copyOf(result);
    }

    /**
     * 业务作用：按误触发策略把一个到期定义展开为有限链，首尾 expected/proposed 可由批量脚本连续 CAS。
     *
     * @param definition 任务定义
     * @param expected   当前权威逻辑时刻
     * @param redisNow   扫描使用的 Redis 时间
     * @return 有界触发链。
     */
    private List<RedisJobRepository.FireRequest> fireRequests(
            RedisJobDefinition definition, long expected, long redisNow) {
        boolean missed = redisNow - expected > properties.getScheduleRttAllowanceMs();
        if (definition.scheduleType() == RedisJobScheduleType.FIXED_DELAY) {
            if (missed && definition.misfire() == RedisJobMisfire.DO_NOTHING) {
                return List.of(new RedisJobRepository.FireRequest(definition, expected,
                        nextAfterNow(definition, expected, redisNow), true, true, true));
            }
            return List.of(new RedisJobRepository.FireRequest(definition, expected, 0L, false, missed, false));
        }
        if (!missed) {
            return List.of(new RedisJobRepository.FireRequest(definition, expected,
                    definition.nextFireAt(expected), false, false, true));
        }
        if (definition.misfire() == RedisJobMisfire.DO_NOTHING) {
            return List.of(new RedisJobRepository.FireRequest(definition, expected,
                    nextAfterNow(definition, expected, redisNow), true, true, true));
        }
        if (definition.misfire() == RedisJobMisfire.FIRE_ONCE_NOW) {
            return List.of(new RedisJobRepository.FireRequest(definition, expected,
                    nextAfterNow(definition, expected, redisNow), false, true, true));
        }
        return catchUpRequests(definition, expected, redisNow);
    }

    /**
     * 业务作用：只保留补偿窗口内最近的有限逻辑时刻，更早积压通过同一 CAS 链跳过。
     *
     * @param definition 任务定义
     * @param expected   当前权威逻辑时刻
     * @param redisNow   扫描使用的 Redis 时间
     * @return 先跳过旧积压、再补偿最近时刻的请求链。
     */
    private List<RedisJobRepository.FireRequest> catchUpRequests(
            RedisJobDefinition definition, long expected, long redisNow) {
        long cutoff = Math.max(0L, redisNow - properties.getMaxCatchUpWindowMs());
        long first = firstAtOrAfter(definition, expected, cutoff);
        Deque<Long> selected = new ArrayDeque<>(properties.getMaxCatchUpRuns());
        long cursor;
        if (definition.scheduleType() == RedisJobScheduleType.FIXED_RATE && first <= redisNow) {
            long total = Math.floorDiv(redisNow - first, definition.intervalMs()) + 1L;
            long retained = Math.min(total, properties.getMaxCatchUpRuns());
            long selectedFirst = Math.addExact(first,
                    Math.multiplyExact(total - retained, definition.intervalMs()));
            for (long index = 0; index < retained; index++) {
                selected.addLast(Math.addExact(selectedFirst,
                        Math.multiplyExact(index, definition.intervalMs())));
            }
            cursor = Math.addExact(first, Math.multiplyExact(total, definition.intervalMs()));
        } else {
            cursor = first;
            while (cursor <= redisNow) {
                if (selected.size() == properties.getMaxCatchUpRuns()) selected.removeFirst();
                selected.addLast(cursor);
                cursor = definition.nextFireAt(cursor);
            }
        }
        if (selected.isEmpty()) {
            return List.of(new RedisJobRepository.FireRequest(definition, expected, cursor, true, true, true));
        }
        long firstSelected = selected.getFirst();
        List<RedisJobRepository.FireRequest> requests = new ArrayList<>();
        if (firstSelected != expected) {
            requests.add(new RedisJobRepository.FireRequest(
                    definition, expected, firstSelected, true, true, false));
        }
        List<Long> times = List.copyOf(selected);
        for (int index = 0; index < times.size(); index++) {
            long current = times.get(index);
            long next = index + 1 < times.size() ? times.get(index + 1) : cursor;
            requests.add(new RedisJobRepository.FireRequest(
                    definition, current, next, false, true, index + 1 == times.size()));
        }
        return List.copyOf(requests);
    }

    /**
     * 业务作用：快速定位补偿窗口首时刻，fixed rate 使用算术跳跃避免毫秒周期长循环。
     *
     * @param definition 任务定义
     * @param expected   当前逻辑时刻
     * @param cutoff     窗口下界
     * @return 不早于下界的第一个逻辑时刻。
     */
    private long firstAtOrAfter(RedisJobDefinition definition, long expected, long cutoff) {
        if (expected >= cutoff) return expected;
        if (definition.scheduleType() == RedisJobScheduleType.FIXED_RATE) {
            long distance = cutoff - expected;
            long steps = Math.floorDiv(distance + definition.intervalMs() - 1L, definition.intervalMs());
            return Math.addExact(expected, Math.multiplyExact(steps, definition.intervalMs()));
        }
        long cursor = expected;
        while (cursor < cutoff) cursor = definition.nextFireAt(cursor);
        return cursor;
    }

    /**
     * 业务作用：推进可见性兜底并恢复租约已真实到期的普通 Run。
     *
     * <p>参数说明: 无。
     *
     * @return 下一次普通索引扫描的有界间隔。
     */
    private long scanIndexes() {
        long nextDelay = properties.getMaxScanIntervalMs();
        for (int shard : localShards()) {
            RedisJobRepository.PromotionResult visible = repository.promoteVisible(shard);
            nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                    visible.redisNow(), visible.nextScore(), visible.promoted() > 0L,
                    RedisJobScanInterval.VISIBLE, shard, visibleScanUpperBoundMs));
            RedisJobRepository.DueScan leases = repository.scanDue(keys.leases(shard), properties.getScanBatchSize());
            nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                    leases.redisNow(), leases.nextScore(), !leases.members().isEmpty(),
                    RedisJobScanInterval.LEASE, shard, properties.getLeaseMs() / 3L));
            for (RedisJobRepository.DueMember member : leases.members()) {
                Optional<RedisJobRepository.RunData> candidate = repository.readAtShard(shard, member.member());
                if (candidate.isEmpty()) continue;
                RedisJobDefinition definition = definitions.get(candidate.get().run().jobName());
                if (definition != null) repository.recoverExpired(definition, member.member());
            }
            RedisJobRepository.DueScan waiting = scanWaiting(shard);
            nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                    waiting.redisNow(), waiting.nextScore(), !waiting.members().isEmpty(),
                    RedisJobScanInterval.WAITING, shard, properties.getFanoutCreateTimeoutMs()));
        }
        return nextDelay;
    }

    /**
     * 业务作用：对账 Fanout 创建 intent，并在根等待到期时先关闭桶内新执行入口。
     *
     * @param shard 普通调度分片
     * @return 本轮到期成员与下一最小 score。
     */
    private RedisJobRepository.DueScan scanWaiting(int shard) {
        RedisJobRepository.DueScan waiting = repository.scanDue(keys.waiting(shard), properties.getScanBatchSize());
        for (RedisJobRepository.DueMember member : waiting.members()) {
            Optional<RedisJobRepository.RunData> candidate = repository.readAtShard(shard, member.member());
            if (candidate.isEmpty()) continue;
            RedisJobRepository.RunData data = candidate.get();
            RedisJobDefinition definition = definitions.get(data.run().jobName());
            if (definition == null || data.fanoutId().isEmpty()) continue;
            String rootState = fanoutMonitor.rootState(data.fanoutId());
            if (data.run().state() == RedisJobState.FANOUT_CREATING) {
                if ("CREATING".equals(rootState)) {
                    fanoutMonitor.advanceRoot(data.fanoutId());
                    rootState = fanoutMonitor.rootState(data.fanoutId());
                }
                if ("COMMITTED".equals(rootState) || "WAITING_CHILDREN".equals(rootState)) {
                    fanoutCoordinator.reconcileCommitted(data.fanoutId(), data.run().runId(),
                            data.rootAttempt(), definition.name(), shard);
                } else if (Set.of("SUCCEEDED", "PARTIAL_FAILED", "FAILED", "CANCELLED").contains(rootState)) {
                    fanoutCoordinator.reconcileTerminal(data.fanoutId(), data.run().runId(), data.rootAttempt(),
                            definition.name(), shard, rootState,
                            "FAILED".equals(rootState) ? "FANOUT_CREATE_TIMEOUT" : "");
                } else if (rootState.isEmpty()) {
                    repository.failWaitingCreation(definition, data.run().runId());
                }
            } else if (data.run().state() == RedisJobState.WAITING_CHILDREN) {
                fanoutMonitor.requestCancel(data.fanoutId(), "WAIT_TIMEOUT");
            }
        }
        return waiting;
    }

    /**
     * 业务作用：扫描 Fanout root 看门狗并把桶内终态幂等回填普通根 Run。
     *
     * <p>参数说明: 无。
     *
     * @return 下一次 Fanout 根扫描的有界间隔。
     */
    private long scanFanoutRoots() {
        long nextDelay = properties.getMaxScanIntervalMs();
        for (int bucket = 0; bucket < keys.fanoutBucketCount(); bucket++) {
            RedisJobRepository.DueScan scan = repository.scanDue(keys.fanoutRoots(bucket), properties.getScanBatchSize());
            nextDelay = Math.min(nextDelay, RedisJobScanInterval.delay(properties, registry.executorId(),
                    scan.redisNow(), scan.nextScore(), !scan.members().isEmpty(),
                    RedisJobScanInterval.FANOUT_ROOT, bucket, properties.getFanoutCreateTimeoutMs()));
            for (RedisJobRepository.DueMember member : scan.members()) {
                String fanoutId = member.member();
                fanoutMonitor.advanceRoot(fanoutId);
                ListResult result = watchFanoutRoot(fanoutId);
                if (result.terminal()) {
                    fanoutCoordinator.reconcileTerminal(fanoutId, result.rootRunId(), result.rootAttempt(),
                            result.rootJobName(), result.rootShard(), result.state(),
                            "WAIT_TIMEOUT".equals(result.cancelReason()) ? "WAIT_TIMEOUT" : result.errorType());
                }
            }
        }
        return nextDelay;
    }

    /**
     * 业务作用：复验 Fanout root 状态并为非终态记录设置下一持久检查时刻。
     *
     * @param fanoutId Fanout 标识
     * @return 看门狗读取结果。
     */
    private ListResult watchFanoutRoot(String fanoutId) {
        var response = scripts.list(RedisJobScript.FANOUT_WATCH_ROOT,
                new String[]{keys.fanoutRoot(fanoutId), keys.fanoutRoots(fanoutId)},
                fanoutId, properties.getMaxScanIntervalMs());
        String code = value(response, 0);
        if (!"OK".equals(code)) return ListResult.EMPTY;
        String state = value(response, 1);
        boolean terminal = Set.of("SUCCEEDED", "PARTIAL_FAILED", "FAILED", "CANCELLED").contains(state);
        return new ListResult(state, value(response, 2), integer(response, 3), value(response, 4),
                integer(response, 5), terminal, value(response, 6), value(response, 7));
    }

    /**
     * 业务作用：从 Redis 当前时刻计算首次计划时刻，非自动任务返回零。
     *
     * @param definition 任务定义
     * @return 下一逻辑时刻。
     */
    private long initialNextFireAt(RedisJobDefinition definition) {
        if (definition.scheduleType() == RedisJobScheduleType.MANUAL
                || definition.scheduleType() == RedisJobScheduleType.FANOUT_ONLY) return 0L;
        int shard = keys.scheduleShard(definition.name());
        long redisNow = repository.scanDue(keys.schedule(shard), 1).redisNow();
        return definition.nextFireAt(redisNow);
    }

    /**
     * 业务作用：按误触发策略选择下一逻辑时刻，避免 FIRE_ONCE 在恢复后连续产生陈旧 Run。
     *
     * @param definition    任务定义
     * @param logicalFireAt 当前逻辑时刻
     * @param redisNow      Redis 当前时刻
     * @return 下一逻辑时刻。
     */
    private long nextAfterNow(RedisJobDefinition definition, long logicalFireAt, long redisNow) {
        long next = definition.nextFireAt(logicalFireAt);
        if (definition.misfire() == RedisJobMisfire.CATCH_UP) return next;
        int guard = 0;
        while (next <= redisNow && guard++ < properties.getMaxCatchUpRuns() * 10) {
            next = definition.nextFireAt(next);
        }
        if (next <= redisNow) throw new IllegalStateException("cannot advance RedisJob schedule beyond current time");
        return next;
    }

    /**
     * 业务作用：列出本节点已登记普通任务涉及的调度分片，避免扫描无关分片。
     *
     * @return 不可变快照语义的有序分片数组。
     */
    private int[] localShards() {
        return localShardSnapshot;
    }

    /**
     * 业务作用：仅在本地定义集合变化时重建分片快照，使周期扫描不再创建临时集合。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void refreshLocalShards() {
        boolean[] present = new boolean[keys.shardCount()];
        long visibleUpperBound = properties.getMaxScanIntervalMs();
        int count = 0;
        for (RedisJobDefinition definition : definitions.values()) {
            if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) continue;
            int shard = keys.scheduleShard(definition.name());
            if (!present[shard]) {
                present[shard] = true;
                count++;
            }
            visibleUpperBound = Math.min(visibleUpperBound, definition.retryDelayMs());
        }
        int[] snapshot = new int[count];
        int index = 0;
        for (int shard = 0; shard < present.length; shard++) {
            if (present[shard]) snapshot[index++] = shard;
        }
        visibleScanUpperBoundMs = Math.max(properties.getMinScanIntervalMs(), visibleUpperBound);
        localShardSnapshot = snapshot;
    }

    /**
     * 业务作用：写入端产生更早可见成员时执行本分片定点推进，不等待正在进行的长退避周期结束。
     *
     * @param shard   调度分片
     * @param delayMs 服务端计算的剩余时长
     *                返回：无返回值。
     */
    private void wakeVisible(int shard, long delayMs) {
        if (!running.get()) return;
        long boundedDelay = Math.max(0L, delayMs);
        if (boundedDelay <= properties.getMinScanIntervalMs()) {
            promoteVisibleNow(shard);
            return;
        }
        try {
            monitors.schedule(() -> promoteVisibleNow(shard), boundedDelay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // 生命周期已经关闭时不再领取新任务，持久索引由其它存活节点按期限继续推进。
        }
    }

    /**
     * 业务作用：立即推进单个分片的新可见成员；异常不影响已经提交的 Run 状态与跨节点兜底。
     *
     * @param shard 调度分片
     *              返回：无返回值。
     */
    private void promoteVisibleNow(int shard) {
        if (!running.get()) return;
        try {
            repository.promoteVisible(shard);
        } catch (RuntimeException error) {
            log.warn("RedisJob targeted visible promotion failed: shard={}", shard, error);
        }
    }

    /**
     * 业务作用：读取本地已登记定义，不存在时给出明确调用错误。
     *
     * @param jobName 任务名
     * @return 任务定义。
     */
    private RedisJobDefinition requireDefinition(String jobName) {
        RedisJobDefinition definition = definitions.get(jobName);
        if (definition == null) throw new IllegalArgumentException("unknown RedisJob: " + jobName);
        return definition;
    }

    /**
     * 业务作用：隔离心跳异常，保留后续周期恢复机会。 返回：无返回值。
     */
    private void safeHeartbeat() {
        safely("heartbeat", () -> {
            heartbeat();
            lastHeartbeatAt.set(System.currentTimeMillis());
        });
    }

    /**
     * 业务作用：根据最近的 Redis score 自适应安排下一次调度扫描，异常时使用最短周期恢复。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void scheduleScanCycle() {
        if (!running.get()) return;
        long delay = properties.getMinScanIntervalMs();
        try {
            delay = scanSchedules();
            lastScheduleScanAt.set(System.currentTimeMillis());
        } catch (Throwable error) {
            log.error("RedisJob schedule scan failed", error);
        }
        if (running.get()) monitors.schedule(this::scheduleScanCycle, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：按普通索引最早 score 自适应续排扫描，异常时缩短间隔恢复持久状态推进。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void indexScanCycle() {
        if (!running.get()) return;
        long delay = properties.getMinScanIntervalMs();
        try {
            delay = scanIndexes();
        } catch (Throwable error) {
            log.error("RedisJob index scan failed", error);
        }
        if (running.get()) monitors.schedule(this::indexScanCycle, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：按 Fanout 根最早 score 自适应续排跨 slot 对账，避免空桶固定频率访问 Redis。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void fanoutRootScanCycle() {
        if (!running.get()) return;
        long delay = properties.getMinScanIntervalMs();
        try {
            delay = scanFanoutRoots();
        } catch (Throwable error) {
            log.error("RedisJob fanout root scan failed", error);
        }
        if (running.get()) monitors.schedule(this::fanoutRootScanCycle, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：按 Fanout 恢复索引最早 score 自适应续排扫描，通知丢失时仍保持持久兜底。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void fanoutIndexScanCycle(int bucket, long epoch) {
        if (!running.get() || fanoutIndexEpochs.get(bucket) != epoch) return;
        long delay = properties.getMinScanIntervalMs();
        try {
            delay = fanoutMonitor.scanBucket(bucket);
        } catch (Throwable error) {
            log.error("RedisJob fanout index scan failed: bucket={}", bucket, error);
        }
        if (running.get() && fanoutIndexEpochs.get(bucket) == epoch) {
            scheduleFanoutIndexScan(bucket, delay, epoch);
        }
    }

    /**
     * 业务作用：按桶独立安排 Fanout 索引周期，使活跃桶的短期限不会拖高全部空桶的扫描频率。
     *
     * @param bucket  Fanout 桶
     * @param delayMs 下一次扫描延迟
     * @param epoch   当前桶调度代次
     *                返回：无返回值。
     */
    private void scheduleFanoutIndexScan(int bucket, long delayMs, long epoch) {
        try {
            monitors.schedule(() -> fanoutIndexScanCycle(bucket, epoch),
                    Math.max(0L, delayMs), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // 生命周期关闭后不再建立新扫描任务，持久索引由其它存活节点继续推进。
        }
    }

    /**
     * 业务作用：本进程建立 Fanout 根时中断目标桶的旧长退避，并以新期限立即重算下一扫描时刻。
     *
     * @param fanoutId         Fanout 标识
     * @param receiptTimeoutMs 根任务回执期限
     *                         返回：无返回值。
     */
    private void wakeFanoutIndex(String fanoutId, long receiptTimeoutMs) {
        int bucket = keys.fanoutBucket(fanoutId);
        fanoutMonitor.activateBucket(bucket, receiptTimeoutMs);
        if (!running.get()) return;
        long epoch = fanoutIndexEpochs.incrementAndGet(bucket);
        scheduleFanoutIndexScan(bucket, 0L, epoch);
    }

    /**
     * 业务作用：隔离注册表回收异常，避免心跳和执行恢复受到影响。 返回：无返回值。
     */
    private void safeRegistryGc() {
        safely("registry gc", this::cleanupMetadata);
    }

    /**
     * 业务作用：回收注册表与到期定义墓碑，两个清理都只处理已越过各自保留门禁的记录。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void cleanupMetadata() {
        metrics.add("redis_job_registry_gc_total", registry.gc());
        long tombstones = 0L;
        for (int shard = 0; shard < keys.shardCount(); shard++) {
            tombstones += repository.cleanupTombstones(shard);
        }
        metrics.add("redis_job_tombstone_gc_total", tombstones);
    }

    /**
     * 业务作用：续期当前执行器；登记过期时重新走重路径恢复完整能力声明。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void heartbeat() {
        try {
            registry.heartbeat();
        } catch (IllegalStateException error) {
            if (error.getMessage() == null || !error.getMessage().contains("registration expired")) throw error;
            definitions.values().forEach(registry::register);
        }
    }

    /**
     * 业务作用：让当前执行器进入 DRAINING，停止普通和 Fanout 新领取并保留已有 attempt 的完成路径。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    public void drain() {
        draining.set(true);
        dispatcher.drain();
        fanoutDispatcher.drain();
        registry.drain();
    }

    /**
     * 业务作用：在通知订阅与消费通道均就绪后重新把当前执行器开放为 ACTIVE。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    public void activate() {
        fanoutDispatcher.activate();
        registry.activate();
        dispatcher.activate();
        draining.set(false);
    }

    /**
     * 业务作用：统一记录监视器异常而不让固定周期任务被调度器取消。
     *
     * @param action 动作名
     * @param task   监视动作
     *               返回：无返回值。
     */
    private void safely(String action, Runnable task) {
        if (!running.get()) return;
        try {
            task.run();
        } catch (Throwable error) {
            log.error("RedisJob {} failed", action, error);
        }
    }

    /**
     * 业务作用：安全读取脚本复合返回字段。
     *
     * @param values 返回列表
     * @param index  下标
     * @return 字符串值。
     */
    private static String value(List<Object> values, int index) {
        return index >= values.size() || values.get(index) == null ? "" : Objects.toString(values.get(index));
    }

    /**
     * 业务作用：读取脚本整数字段并检查 Java int 溢出。
     *
     * @param values 返回列表
     * @param index  下标
     * @return 整型值。
     */
    private static int integer(List<Object> values, int index) {
        String value = value(values, index);
        return value.isEmpty() ? 0 : Math.toIntExact(Long.parseLong(value));
    }

    /**
     * 业务作用：报告调度器是否已经开放任务领取。 @return 已启动返回 true。
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 业务作用：让 Spring 在普通组件之后启动任务调度。 @return 生命周期阶段。
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 10_000;
    }

    /**
     * 业务作用：要求 Spring 容器刷新完成后自动启动。 @return 始终为 true。
     */
    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * 业务作用：先停止新任务与通知，再关闭监视器和执行资源。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    @Override
    public void stop() {
        if (!closed.compareAndSet(false, true)) return;
        draining.set(true);
        dispatcher.drain();
        fanoutDispatcher.drain();
        try {
            registry.drain();
        } catch (RuntimeException error) {
            log.warn("RedisJob executor could not publish draining state before shutdown", error);
        }
        running.set(false);
        monitors.shutdown();
        dispatcher.close();
        fanoutDispatcher.close();
        // 已持权 Handler 在等待窗口内继续续期并提交结果，避免正常停机制造无谓的租约恢复。
        if (!registry.awaitIdle(properties.getMaxRunDurationMs())) {
            log.warn("RedisJob shutdown deadline reached with active handlers");
        }
        leaseRenewer.close();
        try {
            registry.unregister();
        } catch (RuntimeException error) {
            log.warn("RedisJob executor could not remove registry membership during shutdown", error);
        }
    }

    /**
     * 业务作用：执行同步停机后通知 Spring 生命周期回调。
     *
     * @param callback 停机完成回调
     *                 返回：无返回值。
     */
    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    /**
     * 业务作用：兼容显式资源关闭并复用生命周期停机顺序。 返回：无返回值。
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * 业务作用：承载一次 Fanout root 看门狗读取结果。
     */
    private record ListResult(String state, String rootRunId, int rootAttempt, String rootJobName,
                              int rootShard, boolean terminal, String cancelReason, String errorType) {
        private static final ListResult EMPTY = new ListResult("", "", 0, "", 0, false, "", "");
    }
}
