package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.core.base.ME;
import io.github.nasaruntime.core.base.RecycleLinkedList;
import io.github.nasaruntime.core.utils.StringUtils;
import io.github.nasaruntime.redis.cache.redis.NasaLettuceConfig;
import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import io.github.nasaruntime.redis.cache.redis.stream.BatchStreamListener;
import io.github.nasaruntime.redis.cache.redis.stream.BatchStreamMessageListenerContainer;
import io.github.nasaruntime.redis.cache.redis.stream.PollLifecycle;
import io.lettuce.core.Consumer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.models.stream.ClaimedMessages;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 业务作用：为一个 BOTH stream/group 建立单 consumer、手工 ACK 的 dedicated container 及其 consumer epoch 权威。
 */
@Slf4j
final class ProxyBothStreamSource implements
        BatchStreamListener<String, MapRecord<String, Object, Object>>,
        PollLifecycle,
        StreamPartitionRuntime.PartitionSource {

    static final String CONSUMER_FENCED_ACK_LUA = """
            local result = {}
            for i = 4, #ARGV do
                local id = ARGV[i]
                local pending = redis.call('xpending', KEYS[1], ARGV[1], id, id, 1)
                if #pending == 0 then
                    result[#result + 1] = 0
                elseif pending[1][2] ~= ARGV[2] then
                    result[#result + 1] = -1
                else
                    local acked = redis.call('xack', KEYS[1], ARGV[1], id)
                    if acked == 1 then
                        if ARGV[3] == '1' then redis.call('xdel', KEYS[1], id) end
                        result[#result + 1] = 1
                    else
                        result[#result + 1] = 2
                    end
                end
            end
            return result
            """;

    private final RedisProxy redisProxy;
    private final StreamPartitionRuntime runtime;
    private final String stream;
    private final String group;
    private final String consumerName;
    private final int batchSize;
    private final long pendingMinIdleMillis;
    private final long drainTimeoutMillis;
    private final StreamSourceAuthority sourceAuthority = new StreamSourceAuthority();
    private final PartitionSourceRecordState recordState;
    private final StreamPendingRecovery pendingRecovery;
    private final AtomicBoolean active = new AtomicBoolean();
    /**
     * close、pause 或 task 退出后保持不可逆，防止旧 container 回调重新激活 consumer epoch。
     */
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean recovering = new AtomicBoolean();
    private final AtomicBoolean routeBlocked = new AtomicBoolean();
    private final BatchStreamMessageListenerContainer<String, MapRecord<String, Object, Object>> container;
    private volatile Subscription subscription;
    private volatile long stopNanos;
    private volatile long lastRecoveryNanos;

    /**
     * 业务作用：冻结 dedicated container 配置并生成每次容器重建都不同的实际 consumer name。
     *
     * @param redisProxy 命令、连接与 serializer 来源
     * @param runtime    共享 Partition dispatcher
     * @param stream     普通 Stream key
     * @param group      consumer group
     *                   返回: 尚未开放 poll 的 dedicated 来源。
     */
    ProxyBothStreamSource(RedisProxy redisProxy,
                          StreamPartitionRuntime runtime,
                          String stream,
                          String group) {
        this.redisProxy = Objects.requireNonNull(redisProxy, "redisProxy");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.stream = Objects.requireNonNull(stream, "stream");
        this.group = Objects.requireNonNull(group, "group");
        NasaLettuceConfig.Group config = runtime.groupConfig(stream, group);
        String prefix = StringUtils.isNotBlank(config.getConsumerName())
                ? config.getConsumerName()
                : ME.sequence();
        this.consumerName = prefix + "/" + runtime.processSession() + "/" + UUID.randomUUID();
        this.batchSize = config.getBatchSize() == null
                ? redisProxy.getStream().getBatchSize()
                : config.getBatchSize();
        int pollTimeout = config.getPollTimeout() == null
                ? redisProxy.getStream().getPollTimeout()
                : config.getPollTimeout();
        RedisPartitionProperties.LocalConsumer local = redisProxy.getStream().getPartition().getLocalConsumer();
        this.pendingMinIdleMillis = local.getProxyPendingMinIdleMs();
        this.drainTimeoutMillis = redisProxy.getStream().getPartition().getDrainTimeoutMs();
        this.container = redisProxy.createListenerContainer(pollTimeout, batchSize, runtime.waitExecutor());
        this.recordState = runtime.newRecordState(this, container::wakeManagedRunners);
        this.pendingRecovery = new StreamPendingRecovery(redisProxy, runtime, this, recordState, batchSize);
    }

    /**
     * 业务作用：先幂等建组、启动 dedicated container，再以 autoAcknowledge=false 登记唯一 consumer。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；重复开放保持幂等，终态关闭后拒绝重新启动。
     */
    synchronized void start() {
        if (closed.get() || !runtime.admissionOpen()) {
            throw new IllegalStateException("BOTH source is already closed: " + stream + "/" + group);
        }
        if (subscription != null) return;
        redisProxy.xGroupCreate(stream, group);
        container.start();
        var request = StreamMessageListenerContainer.StreamReadRequest
                .builder(StreamOffset.create(stream, ReadOffset.lastConsumed()))
                .consumer(org.springframework.data.redis.connection.stream.Consumer.from(group, consumerName))
                .autoAcknowledge(false)
                .cancelOnError(error -> false)
                .build();
        subscription = container.register(request, this, this);
        Map<String, Long> runtimeMetrics = runtime.metrics();
        log.info("[{}] BOTH 专用 Stream 来源启动: stream={} group={} batchSize={} "
                        + "proxyPendingMinIdleMs={} drainTimeoutMs={} listenerP99Ms={} listenerObservations={}",
                redisProxy.getQualifier(), stream, group, batchSize, pendingMinIdleMillis, drainTimeoutMillis,
                runtimeMetrics.getOrDefault("listener_p99_ms", -1L),
                runtimeMetrics.getOrDefault("listener_observations", 0L));
    }

    /**
     * 业务作用：激活 consumer epoch，并在开放新消息 poll 前异步接管足够空闲的历史 PEL。
     *
     * <p>参数说明: 无。
     *
     * @return 容器应继续初始化该 task 时返回 true
     */
    @Override
    public synchronized boolean beforeStart() {
        // close 与迟到启动共用本地临界区，终态之后不能重新发布 consumer epoch 或恢复责任。
        if (closed.get() || !runtime.admissionOpen()) return false;
        if (!active.compareAndSet(false, true)) return active.get();
        sourceAuthority.activate(consumerName);
        if (beginPendingRecovery()) return true;
        // 全局 admission 与本地启动并发关闭时，不得留下已经激活却没有恢复责任的 consumer epoch。
        closed.set(true);
        active.set(false);
        sourceAuthority.loseAuthority();
        return false;
    }

    /**
     * 业务作用：原子登记一轮 PEL 接管并把阻塞 Redis 操作送入专用虚拟等待执行域。
     *
     * <p>参数说明: 无。
     *
     * @return 来源仍开放且已有恢复在途或本次提交成功时返回 true
     */
    private synchronized boolean beginPendingRecovery() {
        if (closed.get() || !allowsRecovery()) return false;
        if (!recovering.compareAndSet(false, true)) return true;
        StreamRuntimeStatus.DrainToken drain = runtime.beginSourceRecovery();
        // 全局停机可能发生在本地来源检查之后，未取得排干令牌不能再提交新的接管调用。
        if (drain == null) {
            recovering.set(false);
            return false;
        }
        try {
            runtime.waitExecutor().execute(() -> recoverPending(drain));
            return true;
        } catch (Throwable failure) {
            drain.close();
            recovering.set(false);
            pause(failure);
            return false;
        }
    }

    /**
     * 业务作用：在读取前或暂停读取期间复验 consumer epoch 与计划 Runner 的存活条件，并按需安排历史 PEL 接管。
     *
     * <p>参数说明: 无。
     *
     * @return 来源仍可存活时返回 true；临时路由阻断不终结来源，实际读取由 isPollReady 决定
     */
    @Override
    public boolean beforePoll() {
        if (!routeBlocked.get() && allowsRecovery()) {
            long elapsed = System.nanoTime() - lastRecoveryNanos;
            if (lastRecoveryNanos == 0L
                    || elapsed >= TimeUnit.MILLISECONDS.toNanos(pendingMinIdleMillis)) {
                if (!beginPendingRecovery()) return false;
            }
        }
        // ManagedRunner 在 routeBlocked 期间仍执行存活检查；已有整批恢复必须保留权威，新 poll 由 isPollReady 阻挡。
        return allowsRecovery() && runtime.sourceHealthy(stream, group);
    }

    /**
     * 业务作用：在历史 PEL 接管完成前关闭新消息读取，避免同一来源内越过旧坐标。
     *
     * <p>参数说明: 无。
     *
     * @return 不在接管且来源有效时返回 true
     */
    @Override
    public boolean isPollReady() {
        return !recovering.get() && !routeBlocked.get() && allowsAdmission();
    }

    /**
     * 业务作用：停止阶段等待 XAUTOCLAIM、Task、重试、路由恢复与确认责任，全部收敛前不得释放 consumer epoch。参数说明: 无。返回: 本来源全部责任结束时为 true。
     */
    @Override
    public boolean isDrainComplete() {
        return !recovering.get() && runtime.sourceDrained(sourceAuthority);
    }

    /**
     * 业务作用：在 XREADGROUP 前预留本来源的最大 raw record 容量。参数说明: 无。返回: 容量可用时为 true。
     */
    @Override
    public boolean tryAcquirePollPermit() {
        return recordState.tryAcquire(batchSize);
    }

    /**
     * 业务作用：把读取前预留缩减为真实 raw record 所有权。参数说明: 记录数。返回: 无返回值。
     */
    @Override
    public void onPollResult(int recordCount) {
        recordState.onPollResult(recordCount);
    }

    /**
     * 业务作用：在批次完成 Task 与确认决策后归还 raw record 所有权。参数说明: 无。返回: 无返回值。
     */
    @Override
    public void afterBatchComplete() {
        recordState.afterBatchComplete();
    }

    /**
     * 业务作用：读取或结果交接失败时归还未转交 raw record 容量。参数说明: 无。返回: 无返回值。
     */
    @Override
    public void afterPollFailure() {
        recordState.afterPollFailure();
    }

    /**
     * 业务作用：将 container 交付的完整物理 record 批次送入 BOTH ledger 与共享 Partition dispatcher。
     *
     * @param messages 尚由 poll barrier 持有的原始批次
     *                 返回: 无返回值；失败 record 保留在 PEL。
     */
    @Override
    public void onMessage(RecycleLinkedList<MapRecord<String, Object, Object>> messages) {
        runtime.dispatchProxy(this, messages);
    }

    /**
     * 业务作用：兼容容器的单条 listener 接口，仍使用同一批次 dispatcher。
     *
     * @param message 单条 MapRecord
     *                返回: 无返回值。
     */
    @Override
    public void onMessage(MapRecord<String, Object, Object> message) {
        RecycleLinkedList<MapRecord<String, Object, Object>> one = RecycleLinkedList.of();
        try {
            one.add(message);
            onMessage(one);
        } finally {
            one.recycle();
        }
    }

    /**
     * 业务作用：返回该 container epoch 共享的来源权威。参数说明: 无。返回: 来源权威。
     */
    @Override
    public StreamSourceAuthority authority() {
        return sourceAuthority;
    }

    /**
     * 业务作用：复验 dedicated source 与全局 dispatcher 都仍接受新 Task。参数说明: 无。返回: 可投递时为 true。
     */
    @Override
    public boolean allowsAdmission() {
        return !closed.get() && active.get() && !routeBlocked.get()
                && sourceAuthority.isActive() && runtime.admissionOpen();
    }

    /**
     * 业务作用：允许已登记 PEL 恢复在 routeBlocked 期间继续使用当前 consumer epoch。参数说明: 无。返回: 权威与运行时仍有效时为 true。
     */
    @Override
    public boolean allowsRecovery() {
        return !closed.get() && active.get() && sourceAuthority.isActive() && runtime.admissionOpen();
    }

    /**
     * 业务作用：关闭 dedicated source 的新读取但保留 consumer epoch，避免同批后继越过未知路由。参数说明: 失败原因。返回: 无返回值。
     */
    @Override
    public void blockRoute(Throwable failure) {
        routeBlocked.set(true);
        runtime.sourceRouteBlocked();
        container.wakeManagedRunners();
    }

    /**
     * 业务作用：整批 route recovery 收敛后重新开放 dedicated source。参数说明: 无。返回: 无返回值。
     */
    @Override
    public void clearRouteBlock() {
        routeBlocked.set(false);
        container.wakeManagedRunners();
    }

    /**
     * 业务作用：把 container 当前 raw batch 容量转交给 route recovery。参数说明: 无。返回: retained Permit。
     */
    @Override
    public PartitionRecordCapacity.Permit retainActiveBatch() {
        return recordState.retainActiveBatch();
    }

    /**
     * 业务作用：将无法安全分类或超出硬容量的来源转入保护态，不再读取新消息。
     *
     * @param failure 关闭 readiness 的原因
     *                返回: 无返回值；PEL 保留为恢复真相。
     */
    @Override
    public void pause(Throwable failure) {
        closed.set(true);
        if (!active.compareAndSet(true, false)) return;
        stopNanos = System.nanoTime();
        runtime.sourceStopping(sourceAuthority);
        runtime.cancelPendingSubmissions(sourceAuthority);
        sourceAuthority.beginStop();
        runtime.sourcePaused(this, failure);
    }

    /**
     * 业务作用：在单 key Lua 中原子复验每个 id 的 PEL consumer，只确认仍属当前 epoch 的坐标。
     *
     * @param ids        record id 集
     * @param autoDelete 本次 XACK 成功后是否 XDEL
     * @return 逐 id 的明确、迁移或不确定结论
     */
    @Override
    public Map<String, StreamCommitCoordinator.AckDisposition> ack(List<String> ids, boolean autoDelete) {
        Object[] args = new Object[3 + ids.size()];
        args[0] = group;
        args[1] = consumerName;
        args[2] = autoDelete ? "1" : "0";
        for (int index = 0; index < ids.size(); index++) args[index + 3] = ids.get(index);
        List<?> result = redisProxy.evalDirectConnection(
                CONSUMER_FENCED_ACK_LUA, List.class, new String[]{stream}, args);
        if (result == null || result.size() != ids.size()) {
            throw new IllegalStateException("consumer-fenced ACK result size mismatch");
        }
        LinkedHashMap<String, StreamCommitCoordinator.AckDisposition> observations = new LinkedHashMap<>();
        for (int index = 0; index < ids.size(); index++) {
            long marker = ((Number) result.get(index)).longValue();
            StreamCommitCoordinator.AckDisposition disposition = switch ((int) marker) {
                case 0, 1 -> StreamCommitCoordinator.AckDisposition.CONFIRMED;
                case -1 -> StreamCommitCoordinator.AckDisposition.MOVED;
                case 2 -> StreamCommitCoordinator.AckDisposition.UNKNOWN;
                default -> throw new IllegalStateException("consumer-fenced ACK returned invalid marker: " + marker);
            };
            observations.put(ids.get(index), disposition);
        }
        return Map.copyOf(observations);
    }

    /**
     * 业务作用：精确复验一个未决 id 的 PEL consumer，阻止旧 epoch 迟到 ACK 影响新 consumer。
     *
     * @param id record id
     * @return 明确缺席、当前 owner 或迁移结论；连接资源故障及查询超时返回 UNKNOWN，其它内部错误继续抛出
     */
    @Override
    public StreamCommitCoordinator.PendingDisposition pending(String id) {
        org.springframework.data.redis.connection.stream.PendingMessage pending;
        try {
            pending = redisProxy.xPendingExactEntry(stream, group, id);
        } catch (DataAccessResourceFailureException | QueryTimeoutException unavailable) {
            // 读取失败不证明 PEL 已结束或 owner 已迁移，保留原责任等待下一轮证据，不能因此关闭来源。
            return StreamCommitCoordinator.PendingDisposition.UNKNOWN;
        }
        if (pending == null) return StreamCommitCoordinator.PendingDisposition.ABSENT;
        return consumerName.equals(pending.getConsumerName())
                ? StreamCommitCoordinator.PendingDisposition.OWNED
                : StreamCommitCoordinator.PendingDisposition.MOVED;
    }

    /**
     * 业务作用：标识本来源为 BOTH 普通 Stream 路径。参数说明: 无。返回: PROXY_BOTH。
     */
    @Override
    public StreamRecordSource sourceKind() {
        return StreamRecordSource.PROXY_BOTH;
    }

    /**
     * 业务作用：返回 dedicated Stream key。参数说明: 无。返回: 普通 Stream key。
     */
    @Override
    public String stream() {
        return stream;
    }

    /**
     * 业务作用：返回 dedicated consumer group。参数说明: 无。返回: 非空 group。
     */
    @Override
    public String group() {
        return group;
    }

    /**
     * 业务作用：返回包含进程会话和 container epoch 的实际 consumer name。参数说明: 无。返回: 唯一 consumer name。
     */
    @Override
    public String consumer() {
        return consumerName;
    }

    /**
     * 业务作用：以与 dedicated container 相同的 serializer 读取一条精确正文，供 listener 与 route 重试重建。
     *
     * @param id record id
     * @return 正文存在时返回 MapRecord，被裁剪或删除时返回 null
     */
    @Override
    public MapRecord<String, Object, Object> exact(String id) {
        return redisProxy.xRangeExactRecord(stream, id);
    }

    /**
     * 业务作用：声明是否应结束来源生命周期。参数说明: 无。返回: 来源已进入终态或共享 runtime 已关闭接纳时为 true。
     */
    @Override
    public boolean isStopRequested() {
        // ManagedRunner 在初始化前也检查停止状态；尚未激活仍需进入 beforeStart 取得来源权威。
        return closed.get() || !runtime.admissionOpen();
    }

    /**
     * 业务作用：判断 BOTH 来源的排干预算是否已用尽。参数说明: 当前时刻。返回: 停止超时时为 true。
     */
    @Override
    public boolean drainTimedOut(long now) {
        return stopNanos > 0L
                && System.nanoTime() - stopNanos >= TimeUnit.MILLISECONDS.toNanos(drainTimeoutMillis);
    }

    /**
     * 业务作用：在 task 退出时废弃 consumer epoch 本地证据并归还读取容量，非停机退出持续关闭 readiness。
     *
     * @param normal 独立循环是否完整结束；托管退出为 false，不能据此判断业务是否请求停机
     *               返回: 无返回值；未请求关闭且运行时仍开放的来源退出后保持不可就绪。
     */
    @Override
    public void afterExit(boolean normal) {
        boolean unexpected = !closed.getAndSet(true) && runtime.admissionOpen();
        active.set(false);
        if (unexpected) {
            // 先发布来源终止的保护原因，再注销 route retry；清除临时路由原因不能把永久停止伪装成健康。
            runtime.sourcePaused(this, new IllegalStateException("BOTH source exited before shutdown"));
        }
        // 退出后旧 consumer epoch 不再具有执行权，迟到回调只能依赖远端 fencing 完成已有确认。
        sourceAuthority.loseAuthority();
        runtime.authorityLost(sourceAuthority);
        runtime.proxyConsumerLost(consumerName);
        recordState.close();
    }

    /**
     * 业务作用：先关闭 poll 和来源权威，再等容器批次退出，不在这里关闭共享等待执行域。
     *
     * <p>参数说明: 无。
     * 返回: 无返回值；重复关闭保持幂等。
     */
    synchronized void close() {
        // 终态先于 container 与权威清理发布，所有迟到 start/beforeStart 都只能观察到关闭。
        closed.set(true);
        active.set(false);
        stopNanos = System.nanoTime();
        // 保留当前 consumer epoch 到已运行 listener 完成，使 drain 窗口内仍可执行 consumer-fenced ACK。
        runtime.sourceStopping(sourceAuthority);
        runtime.cancelPendingSubmissions(sourceAuthority);
        Subscription current = subscription;
        subscription = null;
        if (current != null) {
            try {
                current.cancel();
            } catch (Throwable ignored) {
                // container.stop 仍会统一收口任务。
            }
        }
        if (container.isRunning()) container.stop();
        sourceAuthority.loseAuthority();
        runtime.authorityLost(sourceAuthority);
        runtime.proxyConsumerLost(consumerName);
        recordState.close();
    }

    /**
     * 业务作用：用 XAUTOCLAIM 把足够空闲的旧 consumer PEL 移到当前 epoch，并与新消息共用同一 dispatcher。
     *
     * @param drain 本轮来源恢复的排干令牌
     *              返回: 无返回值；路由阻断、容量暂缺或 Redis 瞬时异常时保留本轮游标和排干责任。
     *              接管响应未知时同时复验当前 consumer PEL，历史分页和未交接页面全部收敛后才开放新 poll。
     */
    private void recoverPending(StreamRuntimeStatus.DrainToken drain) {
        String cursor = "0-0";
        try {
            while (allowsRecovery()) {
                if (pendingRecovery.isUncertain() && !pendingRecovery.reconcileOwned(this::allowsRecovery)) return;
                // 未知路由页尚未建立完整顺序责任时不能继续迁移后续页，也不能开放新读取。
                while (routeBlocked.get() || !recordState.tryAcquire(batchSize)) {
                    try {
                        Thread.sleep(10L);
                    } catch (InterruptedException signal) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (!allowsRecovery()) return;
                }
                boolean handedOff = false;
                try {
                    // 路由等待或容量等待结束后重新核对准入，停止不能因迟到唤醒而发起下一页接管。
                    if (!allowsRecovery()) return;
                    byte[] groupBytes = redisProxy.getKeySerializer().serialize(group);
                    byte[] consumerBytes = redisProxy.getKeySerializer().serialize(consumerName);
                    XAutoClaimArgs<byte[]> args = new XAutoClaimArgs<byte[]>()
                            .consumer(Consumer.from(groupBytes, consumerBytes))
                            .minIdleTime(Duration.ofMillis(pendingMinIdleMillis))
                            .startId(cursor)
                            .count(batchSize);
                    ClaimedMessages<byte[], byte[]> claimed = redisProxy.xAutoClaim(stream, args);
                    if (claimed == null) throw new QueryTimeoutException("XAUTOCLAIM returned no response");
                    List<StreamMessage<byte[], byte[]>> messages = claimed.getMessages();
                    redisProxy.streamPartitionMetrics().recovery(
                            StreamRecordSource.PROXY_BOTH, "xautoclaim",
                            messages == null || messages.isEmpty() ? "empty" : "claimed");
                    recordState.onPollResult(messages == null ? 0 : messages.size());
                    handedOff = messages != null && !messages.isEmpty();
                    if (pendingRecovery.isUncertain()) {
                        // 响应丢失可能遗漏更早的一页；按当前 consumer 的完整 PEL 顺序补回后才能交接这次游标。
                        recordState.afterBatchComplete();
                        handedOff = false;
                        if (!pendingRecovery.reconcileOwned(this::allowsRecovery)) return;
                    } else if (handedOff) {
                        try {
                            runtime.dispatchProxyClaimed(this, redisProxy.decodeClaimedMessages(messages));
                        } catch (StreamPartitionRuntime.PartitionDispatchException retained) {
                            // 当前页已交给精确或整批恢复，后续页仍由本轮负责；保留游标，避免新消息越过未接管历史。
                            redisProxy.streamPartitionMetrics().recovery(
                                    StreamRecordSource.PROXY_BOTH, "xautoclaim", "retained");
                        }
                    }
                    String next = Objects.requireNonNull(claimed.getId(), "XAUTOCLAIM cursor");
                    if ("0-0".equals(next)) {
                        pendingRecovery.completeScan();
                        return;
                    }
                    if (next.equals(cursor)) throw new IllegalStateException("BOTH recovery cursor did not advance");
                    cursor = next;
                } catch (RuntimeException unavailable) {
                    if (!StreamPendingRecovery.isTransient(unavailable)) {
                        // 未识别的内部或协议错误不具备自动重试依据，关闭来源并保留 PEL。
                        pause(unavailable);
                        return;
                    }
                    // 超时不证明接管未执行，保留当前游标、读取屏障及排干责任，并补扫当前 consumer。
                    pendingRecovery.markUncertain();
                    recordState.afterPollFailure();
                    if (!pendingRecovery.backoff()) return;
                } catch (Throwable failure) {
                    redisProxy.streamPartitionMetrics().recovery(
                            StreamRecordSource.PROXY_BOTH, "xautoclaim", "failure");
                    pause(failure);
                    return;
                } finally {
                    if (handedOff) recordState.afterBatchComplete();
                    else recordState.afterPollFailure();
                }
            }
        } catch (Throwable failure) {
            // 当前 consumer 补扫的内部错误同样不能绕过历史屏障开放新消息。
            pause(failure);
        } finally {
            lastRecoveryNanos = System.nanoTime();
            recovering.set(false);
            drain.close();
            container.wakeManagedRunners();
        }
    }
}
