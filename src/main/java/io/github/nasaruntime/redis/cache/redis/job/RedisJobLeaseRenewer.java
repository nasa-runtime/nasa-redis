package io.github.nasaruntime.redis.cache.redis.job;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 业务作用：把同一 Redis slot 的普通 Run 与 Fanout shard 续期合并为有界 Lua 批次。
 */
@Slf4j
final class RedisJobLeaseRenewer implements AutoCloseable {

    private final RedisJobProperties properties;
    private final RedisJobKeyspace keys;
    private final RedisJobScriptExecutor scripts;
    private final String executorId;
    private final Map<String, Lease> leases = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor;

    /**
     * 业务作用：建立独立续期控制线程，批次周期由安全租约关系约束。
     *
     * @param properties Job 配置
     * @param keys       键路由器
     * @param scripts    状态脚本
     * @param executorId 当前执行器身份
     */
    RedisJobLeaseRenewer(RedisJobProperties properties, RedisJobKeyspace keys,
                         RedisJobScriptExecutor scripts, String executorId) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.keys = Objects.requireNonNull(keys, "keys must not be null");
        this.scripts = Objects.requireNonNull(scripts, "scripts must not be null");
        this.executorId = Objects.requireNonNull(executorId, "executorId must not be null");
        this.executor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("redis-job-renew-", 0).factory());
        this.executor.scheduleAtFixedRate(this::safeRenew, properties.getLeaseRenewMs(),
                properties.getLeaseRenewMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * 业务作用：登记普通 Run 的续期坐标；状态进入 FANOUT_CREATING 后同一记录会改续 waiting 截止点。
     *
     * @param definition 任务定义
     * @param runId      Run 标识
     * @param context    执行上下文
     * @return 用于结束登记的句柄。
     */
    LeaseHandle registerNormal(RedisJobDefinition definition, String runId, DefaultRedisJobContext context) {
        int shard = keys.scheduleShard(definition.name());
        return register(new Lease(runId + ':' + context.attemptToken(), keys.run(shard, runId),
                keys.leases(shard), keys.waiting(shard), runId, context.attemptToken(), context));
    }

    /**
     * 业务作用：登记 Fanout shard 的续期坐标，assignment epoch 已由 start 前的状态机完成复验。
     *
     * @param fanoutId Fanout 标识
     * @param seq      分片序号
     * @param context  执行上下文
     * @return 用于结束登记的句柄。
     */
    LeaseHandle registerFanout(String fanoutId, long seq, DefaultRedisJobContext context) {
        String member = fanoutId + ':' + seq;
        return register(new Lease(member + ':' + context.attemptToken(), keys.fanoutShard(fanoutId, seq),
                keys.fanoutLeases(fanoutId), keys.fanoutRoots(fanoutId), member,
                context.attemptToken(), context));
    }

    /**
     * 业务作用：把续期项加入本地活动集合，重复标识只保留同一个上下文。
     *
     * @param lease 续期项
     * @return 关闭时移除该项的句柄。
     */
    private LeaseHandle register(Lease lease) {
        leases.put(lease.id(), lease);
        return () -> leases.remove(lease.id(), lease);
    }

    /**
     * 业务作用：按 hash tag 分组并限制单次脚本项数，防止跨 slot 或无界 Lua 执行。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void renew() {
        if (leases.isEmpty()) return;
        Map<String, List<Lease>> groups = new LinkedHashMap<>();
        for (Lease lease : leases.values()) {
            groups.computeIfAbsent(hashTag(lease.recordKey()), ignored -> new ArrayList<>()).add(lease);
        }
        for (List<Lease> group : groups.values()) {
            for (int start = 0; start < group.size(); start += properties.getScanBatchSize()) {
                renewBatch(group.subList(start, Math.min(group.size(), start + properties.getScanBatchSize())));
            }
        }
    }

    /**
     * 业务作用：一次续期同 slot 的多个执行权，并把失败和取消信号同步到本地副作用门禁。
     *
     * @param batch 同 slot 续期项
     *              返回：无返回值。
     */
    private void renewBatch(List<Lease> batch) {
        String[] scriptKeys = new String[batch.size() * 3];
        List<Object> args = new ArrayList<>();
        args.add(batch.size());
        for (int index = 0; index < batch.size(); index++) {
            Lease lease = batch.get(index);
            scriptKeys[index * 3] = lease.recordKey();
            scriptKeys[index * 3 + 1] = lease.leaseKey();
            scriptKeys[index * 3 + 2] = lease.waitingKey();
            args.add(lease.member());
            args.add(executorId);
            args.add(lease.attemptToken());
            args.add(properties.getLeaseMs());
        }
        List<Object> result = scripts.list(RedisJobScript.RENEW_BATCH, scriptKeys, args.toArray());
        for (int index = 0; index < batch.size(); index++) {
            Lease lease = batch.get(index);
            String code = value(result, 1 + index * 3);
            if (!"OK".equals(code)) {
                lease.context().requestCancellation();
                leases.remove(lease.id(), lease);
                continue;
            }
            if (!value(result, 3 + index * 3).isEmpty()) lease.context().requestCancellation();
            lease.context().renewed(properties.getLeaseMs(),
                    properties.getRenewRttAllowanceMs() + properties.getClockDriftAllowanceMs());
        }
    }

    /**
     * 业务作用：隔离 Redis 续期异常；不推进本地截止点，使不可确认的 attempt 保守失权。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    private void safeRenew() {
        try {
            renew();
        } catch (RuntimeException error) {
            log.warn("RedisJob lease batch renewal failed: executorId={}", executorId, error);
        }
    }

    /**
     * 业务作用：提取 Cluster hash tag 作为批量同 slot 分组键。 @param key Redis 键 @return hash tag。
     */
    private static String hashTag(String key) {
        int start = key.indexOf('{');
        int end = key.indexOf('}', start + 1);
        if (start < 0 || end < 0) throw new IllegalArgumentException("RedisJob key has no hash tag: " + key);
        return key.substring(start + 1, end);
    }

    /**
     * 业务作用：安全读取脚本复合返回字段。 @param values 返回列表 @param index 下标 @return 文本值。
     */
    private static String value(List<Object> values, int index) {
        return index >= values.size() || values.get(index) == null ? "" : Objects.toString(values.get(index));
    }

    /**
     * 业务作用：停止后续批量续期；本地上下文将按已有截止点自然失权。 返回：无返回值。
     */
    @Override
    public void close() {
        executor.shutdown();
        leases.clear();
    }

    /**
     * 业务作用：承载一个活动 attempt 的同 slot 续期坐标。
     */
    private record Lease(String id, String recordKey, String leaseKey, String waitingKey,
                         String member, long attemptToken, DefaultRedisJobContext context) {
    }

    /**
     * 业务作用：允许执行线程在唯一完成出口撤销本地续期登记。
     */
    @FunctionalInterface
    interface LeaseHandle extends AutoCloseable {
        /**
         * 业务作用：撤销当前 attempt 的续期登记。 参数说明: 无。 返回：无返回值。
         */
        @Override
        void close();
    }
}
