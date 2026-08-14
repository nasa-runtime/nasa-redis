package io.github.nasaruntime.redis.cache.redis.stream;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/**
 * 非 consumer-group {@code XREAD BLOCK} 的任务级独占读取器。
 * <p>
 * 一个实例只归属于一个长期运行的 Stream poll task。第一次读取时通过
 * {@link RedisConnectionFactory#getConnection()} 懒加载一个连接包装器，后续所有阻塞读取
 * 都复用该包装器。对 Lettuce 而言，包装器内部为阻塞命令创建的 dedicated native connection
 * 会随包装器一起保留，不会像 {@code RedisTemplate.execute(...)} 那样每轮读取后立即释放并关闭。
 * <p>
 * 本类不是通用连接池，也绝不能跨订阅任务共享连接。阻塞命令执行期间连接不可承载其他命令；
 * 每个非 group 订阅任务独占一个实例，避免相互阻塞，同时不影响普通命令的共享连接和
 * pipeline 的专用连接池。
 * <p>
 * 连接故障时丢弃当前包装器，下一轮读取再懒加载新连接。取消订阅或容器停机时
 * {@link #close()} 会关闭连接；若此时线程正阻塞在 XREAD，关闭动作也会让阻塞尽快退出。
 */
final class PersistentBlockingStreamReadFunction
        implements Function<ReadOffset, List<ByteRecord>>, AutoCloseable {

    /**
     * 连接故障后的最小重连退避，防止 Redis/Narix 不可用时 poll task 紧循环建连。
     */
    private static final long RECONNECT_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    /**
     * 本订阅所属 Redis 数据源的连接工厂。
     */
    private final RedisConnectionFactory connectionFactory;

    /**
     * 已包含 BLOCK/COUNT 参数的 XREAD 选项。
     */
    private final StreamReadOptions readOptions;

    /**
     * 序列化后的 Stream key。构造时缓存，避免每轮读取重复序列化。
     */
    private final byte[] rawKey;

    /**
     * 当前任务独占的连接包装器。AtomicReference 用于协调 poll 线程与外部 cancel/stop 线程。
     */
    private final AtomicReference<RedisConnection> connection = new AtomicReference<>();

    /**
     * 读取器是否已永久关闭。关闭后禁止重新创建连接。
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 业务作用：为一个订阅建出<b>独占</b>的阻塞读取器。
     * <p>
     * 必须独占而不能共用连接：阻塞读取会长时间占住连接，共用会让同一连接上的其它订阅与命令被一起阻塞住。
     *
     * @param connectionFactory 本订阅所属数据源的连接工厂
     * @param readOptions       已含阻塞时长与批量条数的读取选项
     * @param rawKey            序列化后的 Stream 键；调用方不得在构造后修改其内容
     */
    PersistentBlockingStreamReadFunction(RedisConnectionFactory connectionFactory,
                                         StreamReadOptions readOptions,
                                         byte[] rawKey) {
        Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null");
        Assert.notNull(readOptions, "StreamReadOptions must not be null");
        Assert.notNull(rawKey, "Stream key must not be null");
        this.connectionFactory = connectionFactory;
        this.readOptions = readOptions;
        this.rawKey = rawKey;
    }

    /**
     * 业务作用：在本任务独占连接上执行一次阻塞 XREAD。
     * <p>
     * 正常返回和 Redis 的 BLOCK 超时都不会归还连接。只有连接资源故障才会废弃连接；
     * WRONGTYPE 等业务/命令错误保留连接，避免错误持续期间产生新的连接风暴。
     *
     * @param offset 本订阅下一次要读取的 Stream offset
     * @return Redis 返回的原始 Stream records；无消息时遵循驱动原始返回值
     * @throws IllegalStateException          读取器已关闭时抛出
     * @throws RuntimeException               Redis 驱动或命令执行异常
     */
    @Override
    public List<ByteRecord> apply(ReadOffset offset) {
        Assert.notNull(offset, "ReadOffset must not be null");
        RedisConnection current = getOrOpenConnection();
        try {
            return current.streamCommands().xRead(readOptions, StreamOffset.create(rawKey, offset));
        } catch (RuntimeException failure) {
            if (isConnectionFailure(current, failure)) {
                invalidate(current);
                if (!closed.get()) {
                    LockSupport.parkNanos(RECONNECT_BACKOFF_NANOS);
                }
            }
            throw failure;
        }
    }

    /**
     * 业务作用：返回当前连接；尚未建立时懒加载一个。
     * <p>
     * 正常情况下只有 poll task 线程进入本方法。CAS 仍然是必须的，因为 cancel/stop 线程可能
     * 在新连接创建期间并发调用 {@link #close()}；此时新建连接必须立即关闭，不能在停机后泄漏。
     *
     * @return 当前任务独占且尚未关闭的 Redis 连接包装器
     * @throws IllegalStateException 读取器已经关闭，或刚创建连接时与关闭动作发生竞争
     */
    private RedisConnection getOrOpenConnection() {
        RedisConnection current = connection.get();
        if (current != null && !current.isClosed()) {
            return current;
        }
        if (closed.get()) {
            throw new IllegalStateException("blocking stream reader already closed");
        }

        RedisConnection opened = connectionFactory.getConnection();
        if (closed.get()) {
            closeQuietly(opened);
            throw new IllegalStateException("blocking stream reader already closed");
        }
        if (connection.compareAndSet(current, opened)) {
            // close 可能恰好发生在上一次 closed 检查与 CAS 之间。CAS 成功后必须再检查一次，
            // 防止停机线程刚清空连接，poll 线程又把新连接挂回引用中。
            if (closed.get()) {
                if (connection.compareAndSet(opened, null)) {
                    closeQuietly(opened);
                }
                throw new IllegalStateException("blocking stream reader closed while opening connection");
            }
            if (current != null) {
                closeQuietly(current);
            }
            return opened;
        }

        closeQuietly(opened);
        RedisConnection winner = connection.get();
        if (winner == null || winner.isClosed()) {
            throw new IllegalStateException("blocking stream reader closed while opening connection");
        }
        return winner;
    }

    /**
     * 业务作用：判断异常是否意味着当前连接不能继续使用。
     *
     * @param current 当前执行 XREAD 的连接
     * @param failure XREAD 抛出的运行时异常
     * @return true 表示必须关闭并在下一轮重建连接；false 表示属于命令/数据错误
     */
    private static boolean isConnectionFailure(RedisConnection current, RuntimeException failure) {
        if (current.isClosed()) {
            return true;
        }
        Throwable cause = failure;
        while (cause != null) {
            if (cause instanceof DataAccessResourceFailureException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * 业务作用：原子废弃发生资源故障的连接。
     *
     * @param failed 执行失败的连接；只有它仍是当前连接时才负责关闭
     */
    private void invalidate(RedisConnection failed) {
        if (connection.compareAndSet(failed, null)) {
            closeQuietly(failed);
        }
    }

    /**
     * 业务作用：永久关闭读取器及当前独占连接。方法幂等，可由 cancel 线程和 poll task finally 重复调用。
     */
    @Override
    public void close() {
        closed.set(true);
        closeQuietly(connection.getAndSet(null));
    }

    /**
     * 业务作用：关闭连接但不覆盖原始业务异常或停机流程。
     *
     * @param target 待关闭连接；null 表示当前没有连接
     */
    private static void closeQuietly(RedisConnection target) {
        if (target == null) {
            return;
        }
        try {
            target.close();
        } catch (RuntimeException ignored) {
            // 连接已损坏或工厂正在停止时，关闭失败没有可恢复动作。
        }
    }
}
