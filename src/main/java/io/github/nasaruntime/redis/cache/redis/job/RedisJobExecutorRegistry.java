package io.github.nasaruntime.redis.cache.redis.job;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 业务作用：维护当前进程的稳定节点身份、Worker 能力、轻量心跳和不可变 Fanout 快照。
 */
final class RedisJobExecutorRegistry {

    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final String nodeIdentity;
    private final String startupId = UUID.randomUUID().toString().replace("-", "");
    private final String executorId;
    private final Set<String> workers = ConcurrentHashMap.newKeySet();
    private final Set<String> knownWorkers = ConcurrentHashMap.newKeySet();
    private final AtomicInteger inflight = new AtomicInteger();
    private final Object inflightMonitor = new Object();
    private final RedisJobHeartbeatAuthority heartbeatAuthority = new RedisJobHeartbeatAuthority();
    private volatile String state = "ACTIVE";
    private volatile boolean withdrawn;

    /**
     * 业务作用：建立执行器两层身份，稳定节点身份用于定向 inbox，启动身份只用于 owner 观测。
     *
     * @param properties Job 配置
     * @param keys       键路由器
     * @param scripts    脚本执行器
     */
    RedisJobExecutorRegistry(RedisJobProperties properties, RedisJobKeyspace keys, RedisJobScriptExecutor scripts) {
        this.properties = properties;
        this.keys = keys;
        this.scripts = scripts;
        String stable = properties.getInstanceIdentity();
        if (stable == null || stable.isBlank()) stable = hostIdentity();
        this.nodeIdentity = RedisJobNames.requireName(properties.getApplicationName(), "applicationName")
                + ':' + RedisJobNames.requireName(stable, "instanceIdentity");
        this.executorId = nodeIdentity + ':' + startupId;
    }

    /**
     * 业务作用：读取稳定节点身份。 @return 节点身份。
     */
    String nodeIdentity() {
        return nodeIdentity;
    }

    /**
     * 业务作用：读取当前进程执行器身份。 @return 执行器身份。
     */
    String executorId() {
        return executorId;
    }

    /**
     * 业务作用：执行重路径能力登记并原子比对规范契约与 Worker 摘要绑定。
     *
     * <p>返回：无返回值；冲突时抛出异常并拒绝登记能力。
     *
     * @param definition 本地任务定义
     */
    synchronized void register(RedisJobDefinition definition) {
        // 注销是不可逆路由关闭；退出后的实例不得重新登记能力或恢复执行权。
        if (withdrawn) throw new IllegalStateException("RedisJob executor is withdrawn");
        String workerName = definition.workerName();
        String codecs = definition.wireCodecs();
        String implementationDigest = RedisJobIdentifiers.workerKey(
                properties.getApplicationName() + ":java:" + definition.name());
        String capabilityDigest = RedisJobIdentifiers.workerKey(workerName + ':' + definition.contractRevision()
                + ':' + definition.schemaId() + ':' + codecs);
        if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) {
            // 写出前先保留全量注销坐标；即使登记已提交但回包丢失，停机仍能撤销该能力索引。
            knownWorkers.add(workerName);
        }
        List<Object> result = scripts.list(RedisJobScript.EXECUTOR_REGISTER,
                new String[]{keys.executors(), keys.executor(executorId), keys.capability(workerName),
                        keys.capabilityMeta(workerName), keys.contract(workerName, definition.contractRevision()),
                        keys.workerKeyBinding(workerName)},
                executorId, nodeIdentity, startupId, properties.getApplicationName(), "java", state,
                workerName, definition.contractRevision(), definition.schemaId(), codecs,
                implementationDigest, properties.getExecutorCapacity(), properties.getExecutorExpireMs(),
                capabilityDigest, capabilityDigest,
                // Fanout 能力索引只由真正具备 Fanout Handler 的定义持有：普通定义复用 workerName 时
                // 若也写入能力，快照会冻结一个无 Handler 的目标，删除 Worker 时也无法按引用撤权
                definition.trigger() == RedisJobTrigger.FANOUT_ONLY ? "1" : "0");
        // 能力登记同样推进 revision，本地必须采用服务端确认值，后续心跳才能证明自己仍持有当前权威。
        heartbeatAuthority.registered(value(result, 0), value(result, 3));
        // workers 只驱动取得确认后的心跳续期；knownWorkers 独立保留写前坐标，供未知结局全量注销。
        // 普通定义不持有 Fanout 能力，不进入任一集合。
        if (definition.trigger() == RedisJobTrigger.FANOUT_ONLY) workers.add(workerName);
    }

    /**
     * 业务作用：以稳定逻辑请求和已确认 revision 续期执行器及其能力，不重复写契约元数据。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：服务端确认 revision 连续时完成；已注销、记录过期、权威变化或协议不完整时抛出异常。
     */
    synchronized void heartbeat() {
        // 注销是不可逆的路由关闭；静默返回会被上层误认为已取得新的服务端确认。
        if (withdrawn) throw new IllegalStateException("RedisJob executor is withdrawn");
        List<String> sorted = workers.stream().sorted().toList();
        String[] heartbeatKeys = new String[3 + sorted.size()];
        heartbeatKeys[0] = keys.executors();
        heartbeatKeys[1] = keys.executor(executorId);
        heartbeatKeys[2] = keys.fanoutEvidence(nodeIdentity);
        for (int index = 0; index < sorted.size(); index++) {
            heartbeatKeys[index + 3] = keys.capability(sorted.get(index));
        }
        int desiredInflight = inflight.get();
        // 未决请求先取得唯一结局；若它属于旧状态，权威账本会再以新 ID 发布当前状态后才允许返回。
        heartbeatAuthority.heartbeat(state, desiredInflight, request -> exchangeHeartbeat(heartbeatKeys, request));
    }

    /**
     * 业务作用：发送或重发一条已冻结载荷的逻辑心跳，并把服务端证据交回权威账本解释。
     *
     * @param heartbeatKeys 当前执行器及已登记能力的同 slot 键
     * @param request       已绑定请求 ID、状态、在途数与预期 revision 的逻辑请求
     * @return 心跳脚本返回的状态码和 revision；传输或协议失败时抛出异常。
     */
    private RedisJobHeartbeatAuthority.Confirmation exchangeHeartbeat(
            String[] heartbeatKeys, RedisJobHeartbeatAuthority.Request request) {
        List<Object> result = scripts.list(RedisJobScript.EXECUTOR_HEARTBEAT, heartbeatKeys,
                executorId, request.state(), properties.getExecutorExpireMs(), request.inflight(),
                "ACTIVE".equals(request.state()), request.requestId(), request.expectedRevision());
        return new RedisJobHeartbeatAuthority.Confirmation(value(result, 0), value(result, 3));
    }

    /**
     * 业务作用：在 registry slot 原子选择当前存活且契约兼容的执行器，并按稳定节点身份冻结顺序。
     *
     * @param workerName       Worker 能力名
     * @param contractRevision 契约修订号
     * @param schemaId         Schema 标识
     * @param codec            线编码
     * @return 不可变能力快照。
     */
    RedisJobClusterSnapshot snapshot(String workerName, long contractRevision, String schemaId,
                                     RedisJobWireCodec codec) {
        List<Object> result = scripts.list(RedisJobScript.FANOUT_SNAPSHOT,
                new String[]{keys.executors(), keys.capability(workerName), keys.capabilityMeta(workerName),
                        keys.contract(workerName, contractRevision)},
                workerName, contractRevision, schemaId, codec.name(), properties.getFanoutMaxMembers(),
                keys.registryKeyPrefix());
        String code = value(result, 0);
        if (!"OK".equals(code)) throw new IllegalStateException("RedisJob snapshot rejected: " + code);
        long selectedAt = Long.parseLong(value(result, 1));
        List<RedisJobExecutorMember> members = new ArrayList<>();
        for (int index = 2; index + 6 < result.size(); index += 7) {
            members.add(new RedisJobExecutorMember(value(result, index), value(result, index + 1),
                    value(result, index + 2), value(result, index + 3), value(result, index + 4),
                    value(result, index + 5), Long.parseLong(value(result, index + 6))));
        }
        members.sort(Comparator.comparing(RedisJobExecutorMember::nodeIdentity));
        // qualifier 与 namespace 必须参与快照摘要: 跨系统按 snapshotDigest 对账时,
        // 两个数据源在同一毫秒选出同名、同身份的成员会算出相同标识, 无法区分是哪一批节点。
        // 每个字段独立参与摘要而不是先拼成字符串, 否则含冒号的 namespace 或成员身份会产生歧义。
        List<String> memberFields = new ArrayList<>(members.size() * 3);
        for (RedisJobExecutorMember member : members) {
            memberFields.add(member.nodeIdentity());
            memberFields.add(member.executorId());
            memberFields.add(Long.toString(member.heartbeatRevision()));
        }
        String digest = RedisJobIdentifiers.snapshotDigest(keys.qualifier(), keys.namespace(),
                workerName, selectedAt, memberFields);
        String snapshotId = digest.substring(0, 32);
        return new RedisJobClusterSnapshot(snapshotId, workerName, selectedAt, members, digest);
    }

    /**
     * 业务作用：记录跨 Fanout 的节点失联证据；只有匹配观测启动和心跳修订号的不同根才能降级节点。
     *
     * @param nodeIdentity      目标稳定节点
     * @param startupId         快照观测的启动标识
     * @param heartbeatRevision 快照观测的心跳修订号
     * @param fanoutId          产生证据的 Fanout 标识
     * @return 证据处理状态码。
     */
    String recordFanoutEvidence(String nodeIdentity, String startupId,
                                long heartbeatRevision, String fanoutId) {
        List<Object> result = scripts.list(RedisJobScript.EXECUTOR_RECORD_FANOUT_EVIDENCE,
                new String[]{keys.executors(), keys.fanoutEvidence(nodeIdentity)},
                nodeIdentity, startupId, heartbeatRevision, fanoutId,
                properties.getNodeUnreadyEvidenceCount(), properties.getExecutorExpireMs(),
                properties.getRegistryGcGraceMs(), keys.registryKeyPrefix());
        return value(result, 0);
    }

    /**
     * 业务作用：回收超过宽限期的执行器记录及其能力反向索引，避免注册表随进程重启无限增长。
     *
     * <p>参数说明: 无。
     *
     * @return 本轮实际删除的执行器数量。
     */
    long gc() {
        List<Object> result = scripts.list(RedisJobScript.REGISTRY_GC,
                new String[]{keys.executors(), keys.registryGc()},
                properties.getRegistryGcGraceMs(), properties.getScanBatchSize(), keys.registryKeyPrefix());
        return Long.parseLong(value(result, 0));
    }

    /**
     * 业务作用：把执行器置为 DRAINING，停止进入新的能力快照并保留已有 attempt 的续期权。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    synchronized void drain() {
        // 先把服务端成员发布为 DRAINING，后续 Fanout 快照才能在本地排空前停止选择当前执行器。
        state = "DRAINING";
        try {
            heartbeat();
        } catch (IllegalStateException error) {
            // 登记已过期表示服务端路由已经封闭，停机不需要把这个等价终态升级为失败。
            if (error.getMessage() == null || !error.getMessage().contains("registration expired")) throw error;
        }
    }

    /**
     * 业务作用：重新声明执行器 ACTIVE，使通知门禁就绪后可以领取新任务。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：服务端确认 ACTIVE 时无返回值；已注销或未取得确认时抛出异常并保留原目标态。
     */
    synchronized void activate() {
        // 必须在改写目标态前拒绝已撤权实例，避免本地留下一个从未在 Redis 成立的 ACTIVE。
        if (withdrawn) throw new IllegalStateException("RedisJob executor is withdrawn");
        String previousState = state;
        state = "ACTIVE";
        try {
            // 只有新逻辑心跳把 ACTIVE 发布到服务端后调用才成功，失败时上层继续关门。
            heartbeat();
        } catch (RuntimeException | Error failure) {
            // 调用未取得 ACTIVE 确认时恢复原目标态，未决请求取得结局后也只能收敛到该状态。
            state = previousState;
            throw failure;
        }
    }

    /**
     * 业务作用：在优雅停机末尾原子移除本执行器及全部本地能力索引，避免已退出节点继续进入 Fanout 快照。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值；重复注销按幂等成功处理。
     */
    synchronized void unregister() {
        if (withdrawn) return;
        List<String> sorted = knownWorkers.stream().sorted().toList();
        String[] unregisterKeys = new String[3 + sorted.size() * 2];
        unregisterKeys[0] = keys.executors();
        unregisterKeys[1] = keys.executor(executorId);
        unregisterKeys[2] = keys.registryGc();
        for (int index = 0; index < sorted.size(); index++) {
            unregisterKeys[3 + index * 2] = keys.capability(sorted.get(index));
            unregisterKeys[4 + index * 2] = keys.capabilityMeta(sorted.get(index));
        }
        String code = value(scripts.list(RedisJobScript.EXECUTOR_UNREGISTER, unregisterKeys, executorId), 0);
        if (!"OK".equals(code) && !"NOT_FOUND".equals(code)) {
            throw new IllegalStateException("RedisJob executor unregister rejected: " + code);
        }
        // 只有服务端确认主记录与能力索引均已撤销，才能发布本地不可逆终态；未知回包保留重试资格。
        withdrawn = true;
    }

    /**
     * 业务作用：撤销本执行器在单个 Worker 能力下的登记，使删除该 Worker 后的新 Fanout 快照不再选中本节点。
     *
     * <p>能力可能被多个本地定义共享，是否可撤由调用方按引用判定。服务端撤销未确认时保留本地集合，
     * 使终态注销仍掌握完整能力键；调用方同时关闭准入并发布 DRAINING，不能继续以 ACTIVE 续期旧能力。
     *
     * @param workerName Worker 能力名
     *
     * <p>返回：无返回值。
     */
    synchronized void removeCapability(String workerName) {
        if (!workers.contains(workerName)) return;
        String code = value(scripts.list(RedisJobScript.EXECUTOR_REMOVE_CAPABILITY,
                new String[]{keys.capability(workerName), keys.capabilityMeta(workerName), keys.executor(executorId)},
                executorId, workerName), 0);
        if (!"OK".equals(code)) {
            throw new IllegalStateException("RedisJob capability removal rejected: " + code);
        }
        // Redis 已原子撤销能力后才允许后续心跳停止携带该键；失败路径保留给全量注销补偿。
        workers.remove(workerName);
        knownWorkers.remove(workerName);
    }

    /**
     * 业务作用：确认执行器主记录与全部已知能力已经由服务端撤销，供本地资源最终完成门禁复验。
     *
     * <p>参数说明: 无。
     *
     * @return 注销脚本返回 OK 或 NOT_FOUND 后为 true。
     */
    boolean isWithdrawn() {
        return withdrawn;
    }

    /**
     * 业务作用：记录本地开始执行的 Handler 数量，供心跳容量观测。 返回：无返回值。
     */
    void executionStarted() {
        inflight.incrementAndGet();
    }

    /**
     * 业务作用：记录本地结束执行的 Handler 数量，供心跳容量观测。 返回：无返回值。
     */
    void executionFinished() {
        inflight.updateAndGet(value -> Math.max(0, value - 1));
        synchronized (inflightMonitor) {
            inflightMonitor.notifyAll();
        }
    }

    /**
     * 业务作用：优雅停机期间等待已经取得执行权的 Handler 退出，使其租约与完成提交保持可用到等待期限。
     *
     * @param timeoutMs 最大等待毫秒数
     * @return 全部 Handler 已退出返回 true，达到期限仍有执行返回 false。
     */
    boolean awaitIdle(long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (inflightMonitor) {
            while (inflight.get() > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) return false;
                try {
                    TimeUnit.NANOSECONDS.timedWait(inflightMonitor, remaining);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 业务作用：在首次停机期限耗尽后继续等待全部已持权 Handler 退出，作为最终资源收口的安全门禁。
     *
     * <p>参数说明: 无。
     *
     * @return 在途数归零时为 true；等待期间的中断在完成后恢复，不会提前越过资源边界。
     */
    boolean awaitIdle() {
        try (RedisJobShutdownSupport.InterruptDeferral ignored = RedisJobShutdownSupport.deferInterrupts()) {
            synchronized (inflightMonitor) {
                while (inflight.get() > 0) {
                    try {
                        inflightMonitor.wait();
                    } catch (InterruptedException interrupted) {
                        // Registry 注销必须晚于全部已持权 Handler；中断只能延后到最外层资源边界后交还。
                        RedisJobShutdownSupport.captureInterrupt();
                    }
                }
            }
        }
        return true;
    }

    /**
     * 业务作用：取得部署环境可跨进程复用的主机身份作为未显式配置时的节点标识。
     *
     * @return 主机名。
     */
    private static String hostIdentity() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new IllegalStateException("instanceIdentity must be configured when hostname is unavailable", e);
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
}
