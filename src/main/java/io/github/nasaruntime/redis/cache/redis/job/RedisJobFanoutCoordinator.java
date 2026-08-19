package io.github.nasaruntime.redis.cache.redis.job;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * 业务作用：冻结 Worker 能力快照并按 prepare、begin、add、commit、deliver 协议提交 Fanout。
 */
final class RedisJobFanoutCoordinator implements RedisJobFanoutService {

    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final RedisJobExecutorRegistry registry;
    private final RedisJobJsonCodec jsonCodec;
    private final String publishCommand;
    private final BiConsumer<String, String> cancellationRequester;
    private final Map<String, RedisJobDefinition> workers = new ConcurrentHashMap<>();
    private final Map<String, RedisJobDefinition> definitions = new ConcurrentHashMap<>();
    private volatile RedisJobVisibleWakeup visibleWakeup;
    private volatile RedisJobFanoutIndexWakeup fanoutIndexWakeup;

    /**
     * 业务作用：绑定 Fanout 所需的配置、键路由、状态脚本、能力注册表和安全 Codec。
     *
     * @param properties Job 配置
     * @param keys       键路由器
     * @param scripts    脚本执行器
     * @param registry   执行器注册表
     * @param jsonCodec  Job JSON Codec
     * @param cancellationRequester Fanout 桶取消入口
     * <p>返回：构造绑定同一数据源的 Fanout 协调器。
     */
    RedisJobFanoutCoordinator(RedisJobProperties properties, RedisJobKeyspace keys,
                              RedisJobScriptExecutor scripts, RedisJobExecutorRegistry registry,
                              RedisJobJsonCodec jsonCodec, BiConsumer<String, String> cancellationRequester) {
        this.properties = properties;
        this.keys = keys;
        this.scripts = scripts;
        this.registry = registry;
        this.jsonCodec = jsonCodec;
        this.cancellationRequester = Objects.requireNonNull(
                cancellationRequester, "cancellationRequester must not be null");
        this.publishCommand = properties.getPubsubMode() == RedisJobPubSubMode.SHARDED ? "SPUBLISH" : "PUBLISH";
    }

    /**
     * 业务作用：登记本地 Worker 契约，供根任务 dispatch 时确定默认 Schema 与 Codec。
     *
     * @param definition Worker 定义
     *                   返回：无返回值。
     */
    void register(RedisJobDefinition definition) {
        if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) {
            workers.put(definition.workerName(), definition);
        }
        definitions.put(definition.name(), definition);
    }

    /**
     * 业务作用：撤销已删除任务的契约登记，使新的根任务不再把该 Worker 视为可选目标。
     *
     * @param definition       任务定义
     * @param workerStillUsed  同名能力是否仍被其它本地定义使用
     *                         返回：无返回值。
     */
    void unregister(RedisJobDefinition definition, boolean workerStillUsed) {
        definitions.remove(definition.name(), definition);
        if (!workerStillUsed && definition.trigger() == RedisJobTrigger.FANOUT_ONLY) {
            workers.remove(definition.workerName(), definition);
        }
    }

    /**
     * 业务作用：连接普通根 Run 的串行队首写入与可见索引扫描器，保持 Fanout 终态出口的统一门禁。
     *
     * @param wakeup 可见索引唤醒入口
     *               返回：无返回值。
     */
    void setVisibleWakeup(RedisJobVisibleWakeup wakeup) {
        this.visibleWakeup = Objects.requireNonNull(wakeup, "wakeup must not be null");
    }

    /**
     * 业务作用：连接 Fanout 根写入端与按桶索引扫描器，使新根不受空桶长退避限制。
     *
     * @param wakeup Fanout 桶索引唤醒入口
     *               返回：无返回值。
     */
    void setFanoutIndexWakeup(RedisJobFanoutIndexWakeup wakeup) {
        this.fanoutIndexWakeup = Objects.requireNonNull(wakeup, "wakeup must not be null");
    }

    /**
     * 业务作用：把桶内 Fanout 终态以幂等方式回填普通根 Run，并释放原并发策略单活槽。
     *
     * @param fanoutId    Fanout 标识
     * @param rootRunId   普通根 Run 标识
     * @param rootAttempt 根 attempt
     * @param rootJobName 根任务名
     * @param rootShard   根调度分片
     * @param fanoutState 桶内终态
     * @param errorType   桶内终态归因
     * <p>返回：无返回值。
     */
    void reconcileTerminal(String fanoutId, String rootRunId, int rootAttempt, String rootJobName,
                           int rootShard, String fanoutState, String errorType) {
        List<Object> response = scripts.list(RedisJobScript.FINISH_FANOUT_ROOT,
                new String[]{keys.run(rootShard, rootRunId), keys.waiting(rootShard), keys.running(rootShard),
                        keys.visible(rootShard), keys.waitq(rootShard, rootJobName), keys.completion(rootShard)},
                rootRunId, fanoutId, rootAttempt, fanoutState, errorType, properties.getFanoutMaxWaitMs(),
                fanoutId, rootJobName, properties.getRunRetentionMs(), properties.getVisibilityTimeoutMs(),
                keys.shardKeyPrefix(rootShard));
        String code = value(response, 0);
        if (!"OK".equals(code) && !"ALREADY_COMPLETED".equals(code)) return;
        signalVisible(rootShard, response, 3);
        // 普通根 Run 已持久收敛后再开放桶内清理，防止跨 slot 对账尚未完成就删除唯一证据。
        scripts.list(RedisJobScript.FANOUT_MARK_RECONCILED,
                new String[]{keys.fanoutRoot(fanoutId), keys.fanoutRoots(fanoutId)}, fanoutId);
    }

    /**
     * 业务作用：对账已经 commit 的桶内根，把普通根 Run 从创建阶段推进到子任务等待阶段。
     *
     * @param fanoutId    Fanout 标识
     * @param rootRunId   普通根 Run 标识
     * @param rootAttempt 根 attempt
     * @param rootJobName 根任务名
     * @param rootShard   根调度分片
     * <p>返回：无返回值；删除 fence 命中时先关闭已提交桶，不推进普通根等待窗口。
     */
    void reconcileCommitted(String fanoutId, String rootRunId, int rootAttempt,
                            String rootJobName, int rootShard) {
        List<Object> response = scripts.list(RedisJobScript.FINISH_FANOUT_ROOT,
                new String[]{keys.run(rootShard, rootRunId), keys.waiting(rootShard), keys.running(rootShard),
                        keys.visible(rootShard), keys.waitq(rootShard, rootJobName), keys.completion(rootShard)},
                rootRunId, fanoutId, rootAttempt, "COMMITTED", "", properties.getFanoutMaxWaitMs(),
                "", rootJobName, properties.getRunRetentionMs(), properties.getVisibilityTimeoutMs(),
                keys.shardKeyPrefix(rootShard));
        if ("JOB_DELETED".equals(value(response, 0))) {
            // 对账观察者与原提交线程遵守同一删除门禁；否则提交响应丢失后，
            // 看门狗仍可能依据桶内 roots 索引取得新的分片投递权。
            cancellationRequester.accept(fanoutId, "JOB_DELETED");
        }
    }

    /**
     * 业务作用：在 Fanout 根终态释放串行槽后通知本地可见扫描器；失败不改变跨 slot 对账结果。
     *
     * @param shard  根任务调度分片
     * @param values 脚本返回值
     * @param index  相对唤醒时长所在下标
     *               返回：无返回值。
     */
    private void signalVisible(int shard, List<Object> values, int index) {
        RedisJobVisibleWakeup wakeup = visibleWakeup;
        if (wakeup == null || index >= values.size()) return;
        long delayMs = number(values, index);
        if (delayMs < 0L) return;
        try {
            wakeup.wake(shard, delayMs);
        } catch (RuntimeException ignored) {
            // 根终态已经持久提交，其它节点仍会按可见索引期限推进队首。
        }
    }

    /**
     * 业务作用：创建绑定根 attempt 的一次性 Fanout 构建器。
     *
     * @param rootContext 根执行上下文
     * @param workerName  目标 Worker
     * @return Fanout 构建器。
     */
    @Override
    public RedisJobFanoutBuilder builder(DefaultRedisJobContext rootContext, String workerName) {
        return new Builder(rootContext, workerName);
    }

    /**
     * 业务作用：实现一次性 Fanout 构建和提交，重复 dispatch 在本地立即拒绝。
     */
    private final class Builder implements RedisJobFanoutBuilder {
        private final DefaultRedisJobContext rootContext;
        private final String workerName;
        private long contractRevision;
        private String schemaId;
        private RedisJobWireCodec codec;
        private RedisJobFanoutFailurePolicy failurePolicy = RedisJobFanoutFailurePolicy.REASSIGN_ON_FAILURE;
        private Collection<?> items;
        private RedisJobPartitioner<Object> partitioner;
        private List<RedisJobPayload> payloads;
        private final AtomicBoolean dispatched = new AtomicBoolean();

        /**
         * 业务作用：绑定根上下文和目标 Worker，契约默认值延迟到 dispatch 时从 Worker 定义读取。
         *
         * @param rootContext 根执行上下文
         * @param workerName  目标 Worker
         */
        private Builder(DefaultRedisJobContext rootContext, String workerName) {
            this.rootContext = rootContext;
            this.workerName = workerName;
        }

        /**
         * 业务作用：覆盖契约修订号。 @param revision 契约修订号 @return 当前构建器。
         */
        @Override
        public RedisJobFanoutBuilder contractRevision(long revision) {
            if (revision <= 0) throw new IllegalArgumentException("contractRevision must be greater than zero");
            this.contractRevision = revision;
            return this;
        }

        /**
         * 业务作用：覆盖参数 Schema。 @param schemaId Schema 标识 @return 当前构建器。
         */
        @Override
        public RedisJobFanoutBuilder schema(String schemaId) {
            this.schemaId = RedisJobNames.requireName(schemaId, "schemaId");
            return this;
        }

        /**
         * 业务作用：选择线编码。 @param codec 线编码 @return 当前构建器。
         */
        @Override
        public RedisJobFanoutBuilder codec(RedisJobWireCodec codec) {
            this.codec = Objects.requireNonNull(codec, "codec must not be null");
            return this;
        }

        /**
         * 业务作用：保存业务输入和分片算法，待能力快照冻结后按成员数执行。
         *
         * @param items       业务输入
         * @param partitioner 分片算法
         * @param <T>         输入类型
         * @return 当前构建器。
         */
        @Override
        @SuppressWarnings("unchecked")
        public <T> RedisJobFanoutBuilder partition(Collection<T> items, RedisJobPartitioner<T> partitioner) {
            this.items = List.copyOf(items);
            this.partitioner = (RedisJobPartitioner<Object>) partitioner;
            this.payloads = null;
            return this;
        }

        /**
         * 业务作用：直接保存已编码分片。 @param payloads 分片参数 @return 当前构建器。
         */
        @Override
        public RedisJobFanoutBuilder shards(Collection<RedisJobPayload> payloads) {
            this.payloads = List.copyOf(payloads);
            this.items = null;
            this.partitioner = null;
            return this;
        }

        /**
         * 业务作用：设置目标失败收敛策略。 @param policy 失败策略 @return 当前构建器。
         */
        @Override
        public RedisJobFanoutBuilder failurePolicy(RedisJobFanoutFailurePolicy policy) {
            this.failurePolicy = Objects.requireNonNull(policy, "policy must not be null");
            return this;
        }

        /**
         * 业务作用：完成全部提交前校验并跨两个 slot 以确定性 intent 提交 Fanout。
         *
         * @return 已提交结果。
         */
        @Override
        public RedisJobResult dispatch() {
            if (!dispatched.compareAndSet(false, true)) {
                throw new IllegalStateException("one root attempt can dispatch fanout only once");
            }
            RedisJobDefinition worker = workers.get(workerName);
            if (worker == null) throw new IllegalStateException("NO_CAPABLE_EXECUTOR: unknown worker " + workerName);
            long selectedRevision = contractRevision == 0 ? worker.contractRevision() : contractRevision;
            String selectedSchema = schemaId == null ? worker.schemaId() : schemaId;
            RedisJobWireCodec selectedCodec = codec == null ? worker.codecs().iterator().next() : codec;
            if (!worker.codecs().contains(selectedCodec)) {
                throw new IllegalArgumentException("selected codec is not declared by worker");
            }
            RedisJobClusterSnapshot snapshot = registry.snapshot(
                    workerName, selectedRevision, selectedSchema, selectedCodec);
            if (snapshot.members().isEmpty()) throw new IllegalStateException("NO_CAPABLE_EXECUTOR");
            List<RedisJobPayload> shards = materialize(snapshot.members().size(), selectedSchema, selectedCodec);
            validateShards(shards, snapshot.members().size(), selectedSchema, selectedCodec);
            return submit(snapshot, shards, selectedRevision, selectedSchema, selectedCodec);
        }

        /**
         * 业务作用：在快照成员数已冻结后生成同样数量的分片参数。
         *
         * @param memberCount    快照成员数
         * @param selectedSchema Schema 标识
         * @param selectedCodec  线编码
         * @return 有序分片参数。
         */
        private List<RedisJobPayload> materialize(int memberCount, String selectedSchema,
                                                  RedisJobWireCodec selectedCodec) {
            if (payloads != null) return payloads;
            if (items == null || partitioner == null)
                throw new IllegalStateException("fanout shards are not configured");
            if (selectedCodec != RedisJobWireCodec.JSON) {
                throw new IllegalStateException("non-JSON fanout must provide pre-encoded shards");
            }
            List<Object> source = new ArrayList<>(items.size());
            source.addAll(items);
            List<List<Object>> partitions = partitioner.partition(List.copyOf(source), memberCount);
            List<RedisJobPayload> encoded = new ArrayList<>(partitions.size());
            for (List<Object> partition : partitions) encoded.add(jsonCodec.encode(selectedSchema, partition));
            return List.copyOf(encoded);
        }

        /**
         * 业务作用：在 prepare 前一次性校验分片数量、契约和整批字节总量，避免半批 Redis 副作用。
         *
         * @param shards         分片参数
         * @param memberCount    快照成员数
         * @param selectedSchema Schema 标识
         * @param selectedCodec  线编码
         *                       返回：校验通过时正常返回。
         */
        private void validateShards(List<RedisJobPayload> shards, int memberCount, String selectedSchema,
                                    RedisJobWireCodec selectedCodec) {
            if (shards.size() != memberCount)
                throw new IllegalArgumentException("shard count must equal snapshot member count");
            long totalBytes = 0;
            for (RedisJobPayload payload : shards) {
                if (!selectedSchema.equals(payload.schemaId()) || selectedCodec != payload.codec()) {
                    throw new IllegalArgumentException("fanout shard contract mismatch");
                }
                int length = payload.bytes().length;
                if (length > properties.getMaxParameterBytes()) {
                    throw new IllegalArgumentException("fanout shard exceeds maxParameterBytes");
                }
                totalBytes += length;
            }
            if (totalBytes > properties.getFanoutMaxTotalParameterBytes()) {
                throw new IllegalArgumentException("fanout payloads exceed total byte limit");
            }
        }

        /**
         * 业务作用：以根 intent 为不可逆边界提交桶记录和分片，并分批建立稳定 inbox 投递。
         *
         * @param snapshot       冻结能力快照
         * @param shards         有序分片参数
         * @param revision       契约修订号
         * @param selectedSchema Schema 标识
         * @param selectedCodec  线编码
         * @return Fanout 提交结果。
         */
        private RedisJobResult submit(RedisJobClusterSnapshot snapshot, List<RedisJobPayload> shards,
                                      long revision, String selectedSchema, RedisJobWireCodec selectedCodec) {
            String fanoutId = RedisJobIdentifiers.fanoutId(rootContext.runId(), rootContext.attempt());
            int rootShard = keys.scheduleShard(rootContext.jobName());
            List<Object> prepared = scripts.list(RedisJobScript.PREPARE_FANOUT_ROOT,
                    new String[]{keys.run(rootShard, rootContext.runId()), keys.leases(rootShard),
                            keys.waiting(rootShard), keys.job(rootShard, rootContext.jobName())},
                    rootContext.runId(), rootContext.owner(), rootContext.attemptToken(), fanoutId,
                    snapshot.snapshotId(), shards.size(), properties.getFanoutCreateTimeoutMs());
            String prepareCode = value(prepared, 0);
            if ("JOB_DELETED".equals(prepareCode)) {
                // 完成权威尚未转移，当前 attempt 仍由普通完成出口提交删除终态。
                return RedisJobResult.cancelled("job deleted before fanout prepare");
            }
            if (!"PREPARED".equals(prepareCode) && !"ADOPTED".equals(prepareCode)) {
                throw new IllegalStateException("fanout prepare rejected: " + prepareCode);
            }
            if (!rootContext.markFanoutPrepared())
                throw new IllegalStateException("fanout completion authority already transferred");

            String snapshotPayload = Base64.getEncoder().encodeToString(
                    jsonCodec.encode("redis-job-cluster-snapshot", snapshot).bytes());
            List<Object> begun = scripts.list(RedisJobScript.FANOUT_BEGIN,
                    new String[]{keys.fanoutRoot(fanoutId), keys.fanoutRoots(fanoutId)},
                    fanoutId, rootContext.runId(), rootContext.attempt(), snapshot.snapshotId(), snapshotPayload,
                    workerName, revision, selectedSchema, selectedCodec.name(), shards.size(),
                    rootReceiptTimeout(), rootReceiptRetries(), failurePolicy.name(), properties.getFanoutCreateTimeoutMs(),
                    rootContext.jobName(), rootShard,
                    // 根记录自带来源声明, 监视器凭它发现被写错前缀的外来 Fanout
                    keys.qualifier());
            require(value(begun, 0), "fanout begin");
            signalFanoutIndex(fanoutId, rootReceiptTimeout());
            addShards(fanoutId, snapshot, shards, revision, selectedSchema, selectedCodec);
            List<Object> committed = scripts.list(RedisJobScript.FANOUT_COMMIT,
                    new String[]{keys.fanoutRoot(fanoutId), keys.fanoutRoots(fanoutId), keys.fanoutCompletion(fanoutId)},
                    fanoutId);
            require(value(committed, 0), "fanout commit");

            List<Object> rootTransition = scripts.list(RedisJobScript.FINISH_FANOUT_ROOT,
                    new String[]{keys.run(rootShard, rootContext.runId()), keys.waiting(rootShard),
                            keys.running(rootShard), keys.visible(rootShard), keys.waitq(rootShard, rootContext.jobName()),
                            keys.completion(rootShard)},
                    rootContext.runId(), fanoutId, rootContext.attempt(), "COMMITTED", "",
                    properties.getFanoutMaxWaitMs(), "", rootContext.jobName(), properties.getRunRetentionMs(),
                    properties.getVisibilityTimeoutMs(), keys.shardKeyPrefix(rootShard));
            if ("JOB_DELETED".equals(value(rootTransition, 0))) {
                // 桶已提交时必须立即发布保护态；只停止本调用的 deliver 不足以阻止
                // 全局看门狗按持久 roots 索引补投。普通根等桶真实终态后再对账。
                cancellationRequester.accept(fanoutId, "JOB_DELETED");
                return RedisJobResult.cancelled("job deleted before fanout delivery");
            }
            deliver(fanoutId, snapshot, shards, rootReceiptTimeout());
            return RedisJobResult.success(fanoutId);
        }

        /**
         * 业务作用：读取根任务声明的接收回执超时，未找到本地定义时使用框架默认值。
         *
         * @return 回执超时毫秒数。
         */
        private long rootReceiptTimeout() {
            RedisJobDefinition root = definitions.get(rootContext.jobName());
            return root == null ? 2_000L : root.fanoutReceiptTimeoutMs();
        }

        /**
         * 业务作用：读取根任务声明的通知重发次数，未找到本地定义时使用框架默认值。
         *
         * @return 通知重发次数。
         */
        private int rootReceiptRetries() {
            RedisJobDefinition root = definitions.get(rootContext.jobName());
            return root == null ? 3 : root.fanoutReceiptMaxRetries();
        }

        /**
         * 业务作用：根记录已经持久建立后立即唤醒对应桶；局部通知失败不撤销已经建立的恢复入口。
         *
         * @param fanoutId         Fanout 标识
         * @param receiptTimeoutMs 根任务接收回执期限
         *                         返回：无返回值。
         */
        private void signalFanoutIndex(String fanoutId, long receiptTimeoutMs) {
            RedisJobFanoutIndexWakeup wakeup = fanoutIndexWakeup;
            if (wakeup == null) return;
            try {
                wakeup.wake(fanoutId, receiptTimeoutMs);
            } catch (RuntimeException ignored) {
                // 根索引已经持久写入，局部唤醒失败后仍由其它监视周期继续发现。
            }
        }

        /**
         * 业务作用：按有界批次幂等创建全部 shard HASH，避免一段 Lua 长时间占用 Redis 单线程。
         *
         * @param fanoutId       Fanout 标识
         * @param snapshot       能力快照
         * @param shards         分片参数
         * @param revision       契约修订号
         * @param selectedSchema Schema 标识
         * @param selectedCodec  线编码
         *                       返回：无返回值。
         */
        private void addShards(String fanoutId, RedisJobClusterSnapshot snapshot, List<RedisJobPayload> shards,
                               long revision, String selectedSchema, RedisJobWireCodec selectedCodec) {
            int batchSize = properties.getFanoutDeliveryBatchSize();
            for (int offset = 0; offset < shards.size(); offset += batchSize) {
                int end = Math.min(shards.size(), offset + batchSize);
                String[] scriptKeys = new String[1 + end - offset];
                scriptKeys[0] = keys.fanoutRoot(fanoutId);
                List<Object> args = new ArrayList<>();
                args.add(snapshot.snapshotId());
                args.add(end - offset);
                // shard 自带来源与协议代次, 使 Fanout start 能像普通 Run 一样独立复验
                args.add(keys.qualifier());
                args.add(properties.getProtocolVersion());
                for (int index = offset; index < end; index++) {
                    long seq = index;
                    scriptKeys[index - offset + 1] = keys.fanoutShard(fanoutId, seq);
                    RedisJobExecutorMember member = snapshot.members().get(index);
                    args.add(index);
                    args.add(fanoutId);
                    args.add(rootContext.runId());
                    args.add(workerName);
                    args.add(revision);
                    args.add(selectedSchema);
                    args.add(selectedCodec.name());
                    args.add(shards.size());
                    args.add(seq);
                    // executionKey 是跨语言的业务幂等键: 必须带 qualifier, 否则两个数据源的同一分片会算出同一个值
                    args.add(keys.executionKey(fanoutId, seq));
                    args.add(member.nodeIdentity());
                    args.add(member.startupId());
                    args.add(member.heartbeatRevision());
                    args.add(registry.executorId());
                    args.add(Base64.getEncoder().encodeToString(shards.get(index).bytes()));
                }
                List<Object> response = scripts.list(RedisJobScript.FANOUT_ADD_SHARDS, scriptKeys, args.toArray());
                require(value(response, 0), "fanout add shards");
            }
        }

        /**
         * 业务作用：在 commit 后按固定批次建立 inbox、receipt deadline 和低延迟通知。
         *
         * @param fanoutId Fanout 标识
         * @param snapshot 能力快照
         * @param shards   分片参数
         *                 返回：无返回值。
         */
        private void deliver(String fanoutId, RedisJobClusterSnapshot snapshot, List<RedisJobPayload> shards,
                             long receiptTimeoutMs) {
            int batchSize = properties.getFanoutDeliveryBatchSize();
            for (int offset = 0; offset < shards.size(); offset += batchSize) {
                int end = Math.min(shards.size(), offset + batchSize);
                String[] scriptKeys = new String[2 + (end - offset) * 3];
                scriptKeys[0] = keys.fanoutRoot(fanoutId);
                scriptKeys[1] = keys.fanoutReceipts(fanoutId);
                List<Object> args = new ArrayList<>();
                args.add(end - offset);
                args.add(fanoutId);
                args.add(publishCommand);
                args.add(1);
                for (int index = offset; index < end; index++) {
                    long seq = index;
                    RedisJobExecutorMember member = snapshot.members().get(index);
                    int keyIndex = 2 + (index - offset) * 3;
                    scriptKeys[keyIndex] = keys.fanoutShard(fanoutId, seq);
                    scriptKeys[keyIndex + 1] = keys.fanoutInbox(fanoutId, member.nodeIdentity());
                    scriptKeys[keyIndex + 2] = keys.fanoutNotifyChannel(fanoutId, member.nodeIdentity());
                    args.add(seq);
                    args.add(0);
                    args.add(RedisJobIdentifiers.shardRunId(fanoutId, seq));
                    args.add(receiptTimeoutMs);
                }
                List<Object> response = scripts.list(RedisJobScript.FANOUT_DELIVER_BATCH, scriptKeys, args.toArray());
                require(value(response, 0), "fanout delivery");
            }
        }

        /**
         * 业务作用：接受幂等状态码并拒绝其它脚本结果。
         *
         * @param code   状态码
         * @param action 动作名
         *               返回：允许继续时正常返回。
         */
        private void require(String code, String action) {
            if (!"OK".equals(code) && !"ADOPTED".equals(code)) {
                throw new IllegalStateException(action + " rejected: " + code);
            }
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
     * 业务作用：读取脚本返回的可选 Redis 毫秒时刻。
     *
     * @param values 返回列表
     * @param index  下标
     * @return 缺失或空值为零。
     */
    private static long number(List<Object> values, int index) {
        String selected = value(values, index);
        return selected.isEmpty() ? 0L : Long.parseLong(selected);
    }
}
