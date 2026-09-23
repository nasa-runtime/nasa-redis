package io.github.nasaruntime.redis.cache.redis.partition;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.PendingMessages;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * 业务作用：在接管响应不确定时保留扫描屏障，并分页接续当前 consumer 尚未交接的 PEL。
 */
final class StreamPendingRecovery {

    private final RedisProxy proxy;
    private final StreamPartitionRuntime runtime;
    private final StreamPartitionRuntime.PartitionSource source;
    private final PartitionSourceRecordState records;
    private final int batchSize;
    private boolean uncertain;
    private long delayMillis = 100L;

    /**
     * 业务作用：识别 Redis 连接与超时失败，解开同步 Future 的固定包装而不把任意内部异常降级为重试。
     *
     * @param failure Redis 调用抛出的异常
     * @return 明确的传输或超时类别为 true；协议错误、业务错误、中断及无法识别的异常为 false。
     */
    static boolean isTransient(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; depth < 8 && current != null; depth++) {
            if (current instanceof DataAccessResourceFailureException || current instanceof QueryTimeoutException
                    || current instanceof io.lettuce.core.RedisCommandTimeoutException
                    || current instanceof io.lettuce.core.RedisConnectionException
                    || current instanceof java.net.SocketException
                    || current instanceof java.net.SocketTimeoutException
                    || current instanceof java.net.UnknownHostException
                    || current instanceof java.nio.channels.ClosedChannelException
                    || current instanceof java.util.concurrent.TimeoutException) return true;
            // LettuceFuture 同步展开可能保留底层连接原因；只有已知 Future 包装允许继续查看 cause。
            if (!(current instanceof IllegalCallerException
                    || current instanceof java.util.concurrent.ExecutionException
                    || current instanceof java.util.concurrent.CompletionException)) return false;
            current = current.getCause();
        }
        return false;
    }

    /**
     * 业务作用：绑定单一来源的接管恢复上下文，复用其原始记录容量与业务恢复入口。
     *
     * @param proxy     Redis 命令来源
     * @param runtime   共享执行和确认运行时
     * @param source    当前 holder
     * @param records   来源原始记录容量
     * @param batchSize 单页记录上限
     *                  返回: 不占用记录容量、尚无不确定接管的恢复上下文。
     */
    StreamPendingRecovery(RedisProxy proxy, StreamPartitionRuntime runtime,
                          StreamPartitionRuntime.PartitionSource source,
                          PartitionSourceRecordState records, int batchSize) {
        this.proxy = proxy;
        this.runtime = runtime;
        this.source = source;
        this.records = records;
        this.batchSize = batchSize;
    }

    /**
     * 业务作用：标记本轮可能存在服务端已迁移但客户端未交接的 PEL。参数说明: 无。返回: 保持保护原因直到完整扫描结束。
     */
    void markUncertain() {
        // 客户端失败不能证明服务端没有迁移 PEL，未知页面尚未建立顺序责任时保持保护。
        uncertain = true;
        runtime.recoveryUncertain(source.authority(), true);
    }

    /**
     * 业务作用：判断后续接管页是否仍需包含当前 consumer 的补扫。参数说明: 无。返回: 本轮发生过不确定响应时为 true。
     */
    boolean isUncertain() {
        return uncertain;
    }

    /**
     * 业务作用：仅在游标完整结束且当前 consumer 已复验后解除本轮不确定状态。参数说明: 无。返回: 归零退避并撤销本来源的临时保护原因。
     */
    void completeScan() {
        uncertain = false;
        delayMillis = 100L;
        // 调用方已经走到游标终点并完成当前 consumer 复验，才能解除该来源的临时保护。
        runtime.recoveryUncertain(source.authority(), false);
    }

    /**
     * 业务作用：按最高一秒的指数退避重试 Redis 证据，等待期间持续响应停止。参数说明: 无。返回: 等待后仍允许恢复时为 true。
     */
    boolean backoff() {
        long wait = delayMillis;
        delayMillis = Math.min(1_000L, delayMillis * 2L);
        return await(wait);
    }

    /**
     * 业务作用：短段等待恢复条件，停止或异常中断不能使未决历史越过读取屏障。
     *
     * @param millis 本次最长等待毫秒数
     * @return 来源仍允许恢复时为 true；异常中断先停止来源并保留 PEL。
     */
    boolean await(long millis) {
        long remaining = millis;
        while (remaining > 0 && source.allowsRecovery() && runtime.admissionOpen()) {
            long step = Math.min(20L, remaining);
            try {
                Thread.sleep(step);
            } catch (InterruptedException signal) {
                Thread.currentThread().interrupt();
                // 中断不证明本轮历史已经接续，终止来源后由后续 owner 重建责任。
                source.pause(signal);
                return false;
            }
            remaining -= step;
        }
        return source.allowsRecovery() && runtime.admissionOpen();
    }

    /**
     * 业务作用：不受 minIdle 限制地分页复验当前 consumer PEL，并在原扫描屏障内补全未交接记录。
     *
     * @param fence 补扫前及完整正文读取后的权威复验；必须读取真实 holder
     * @return 当前 consumer 已无未决 PEL 时为 true；停止或失权时保留远端 PEL 并返回 false。
     */
    boolean reconcileOwned(BooleanSupplier fence) {
        while (source.allowsRecovery() && runtime.admissionOpen()) {
            // 旧 Task、精确重试和 ACK 必须先收口；随后剩余的当前 consumer PEL 才可重建，避免重复执行业务。
            if (!runtime.sourceDrained(source.authority()) || !records.tryAcquire(batchSize)) {
                if (!await(20L)) return false;
                continue;
            }
            try {
                StreamSourceAuthority.Snapshot authority = records.readAuthority();
                // 补扫与正文重读共用取得容量时的代次，迟到页不能由当前 holder 重新授权。
                if (authority == null || !authority.allowsExecution()
                        || !fence.getAsBoolean() || !authority.allowsExecution()) return false;
                PendingMessages pending = Objects.requireNonNull(proxy.xPending(
                                source.stream(), Consumer.from(source.group(), source.consumer()), batchSize),
                        "current consumer pending response");
                records.onPollResult(pending.size());
                if (!authority.allowsExecution()) return false;
                // 只有明确的空结果才证明当前 consumer 不再有未交接页面，不能用超时替代这项证据。
                if (pending.isEmpty()) return true;
                List<PartitionRecordRef> refs = new ArrayList<>(pending.size());
                pending.forEach(record -> refs.add(new PartitionRecordRef(
                        source.stream(), source.group(), source.consumer(), record.getIdAsString(),
                        "<unresolved>", "<unresolved>", "<unresolved>", source.sourceKind(),
                        authority.current(), authority.generation())));
                while (source.allowsRecovery() && runtime.admissionOpen() && authority.allowsExecution()) {
                    // 先确认允许读取；完整正文返回后由恢复入口再次取得权威证据，读取前的证据不能跨越 Redis 往返。
                    if (!fence.getAsBoolean() || !authority.allowsExecution()) return false;
                    if (runtime.retryRouteBatch(source, refs, fence, records.readReservation(), authority)
                            != StreamPartitionRuntime.RetryDisposition.RETAINED)
                        break;
                    if (!backoff()) return false;
                }
                if (!authority.allowsExecution()) return false;
            } catch (RuntimeException unavailable) {
                if (!isTransient(unavailable)) throw unavailable;
                // 当前 consumer 的空缺只能由明确响应证明，读取或确认超时不能解除扫描责任。
                if (!backoff()) return false;
            } finally {
                records.afterPollFailure();
            }
        }
        return false;
    }
}
