package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * 业务作用：为 Fanout 通知建立 Redis 7 Sharded Pub/Sub 通道，并隔离普通 Pub/Sub 降级语义。
 */
final class RedisJobPubSub implements AutoCloseable {

    private final RedisProxy redisProxy;
    private final RedisJobPubSubMode mode;
    private StatefulRedisPubSubConnection<String, String> shardedConnection;
    private Collection<String> channels = List.of();

    /**
     * 业务作用：绑定 Redis 连接和通知模式，实际连接延迟到调度生命周期启动。
     *
     * @param redisProxy Redis 命令代理
     * @param mode       Pub/Sub 模式
     */
    RedisJobPubSub(RedisProxy redisProxy, RedisJobPubSubMode mode) {
        this.redisProxy = Objects.requireNonNull(redisProxy, "redisProxy must not be null");
        this.mode = Objects.requireNonNull(mode, "mode must not be null");
    }

    /**
     * 业务作用：订阅本节点全部固定桶频道；全部订阅成功后节点才可声明 fanoutReady。
     *
     * @param channels 定向通知频道
     * @param listener 通知处理器，依次接收频道和信封
     *                 返回：无返回值；任一 Sharded 订阅失败时拒绝启动。
     */
    void start(Collection<String> channels, BiConsumer<String, String> listener) {
        this.channels = List.copyOf(channels);
        if (mode == RedisJobPubSubMode.BROADCAST) {
            this.channels.forEach(channel -> redisProxy.subscribe(channel,
                    message -> listener.accept(channel, Objects.toString(message, ""))));
            return;
        }
        RedisConnectionFactory connectionFactory = redisProxy.getRedisTemplate().getConnectionFactory();
        if (!(connectionFactory instanceof LettuceConnectionFactory lettuce)) {
            throw new IllegalStateException("RedisJob SHARDED Pub/Sub requires LettuceConnectionFactory");
        }
        AbstractRedisClient client = lettuce.getNativeClient();
        if (client instanceof RedisClusterClient clusterClient) {
            shardedConnection = clusterClient.connectPubSub(StringCodec.UTF8);
        } else if (client instanceof RedisClient redisClient) {
            shardedConnection = redisClient.connectPubSub(StringCodec.UTF8);
        } else {
            throw new IllegalStateException("RedisJob SHARDED Pub/Sub requires a Lettuce Redis client");
        }
        shardedConnection.addListener(new RedisPubSubAdapter<>() {
            /**
             * 业务作用：把 Sharded Pub/Sub 信封交给 Fanout 状态处理器。
             *
             * @param channel 收到消息的频道
             * @param message 轻量通知信封
             * 返回：无返回值。
             */
            @Override
            public void message(String channel, String message) {
                listener.accept(channel, message);
            }

            /**
             * 业务作用：接收 Redis 7 Sharded Pub/Sub 信封；该回调与普通 Pub/Sub 的 message 相互独立。
             *
             * @param shardChannel 分片频道
             * @param message 轻量通知信封
             * 返回：无返回值。
             */
            @Override
            public void smessage(String shardChannel, String message) {
                listener.accept(shardChannel, message);
            }
        });
        // Redis Cluster 的一次 SSUBSCRIBE 只能携带同 slot 频道；逐频道路由可覆盖全部固定桶。
        this.channels.forEach(channel -> shardedConnection.sync().ssubscribe(channel));
    }

    /**
     * 业务作用：返回状态脚本应使用的原子发布命令，使 deadline、索引和通知在同一 slot 一起提交。
     *
     * @return `SPUBLISH` 或显式降级的 `PUBLISH`。
     */
    String publishCommand() {
        return mode == RedisJobPubSubMode.SHARDED ? "SPUBLISH" : "PUBLISH";
    }

    /**
     * 业务作用：取消全部通知订阅并关闭 Job 独占的 Sharded Pub/Sub 连接。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    @Override
    public void close() {
        if (mode == RedisJobPubSubMode.BROADCAST) {
            channels.forEach(redisProxy::unsubscribe);
        } else if (shardedConnection != null) {
            shardedConnection.close();
            shardedConnection = null;
        }
        channels = List.of();
    }
}
