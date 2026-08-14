package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.evt.EventListener;

/**
 * Nasa
 * redis stream 订阅根接口。继承 {@link EventListener} 通用事件抽象,
 * 叠加 redis stream 专属的 qualifier / mode / group / autoDelete 语义。
 * <p>
 * <b>继承关系</b>:
 * <pre>
 *   EventListener&lt;T, TS&gt;           (io.github.nasaruntime.core.evt — 通用事件抽象)
 *     ↑
 *   StreamSubscribe&lt;T, TS&gt;         (本接口 — redis stream 专属扩展)
 *     ├── RedisEventBatchListener&lt;T&gt;  (批量消费: TS = List&lt;T&gt;)
 *     └── RedisEventSingleListener&lt;T&gt; (单条消费: TS = T)
 * </pre>
 * <p>
 * <b>泛型说明</b>:
 * <ul>
 *   <li>{@code T}  — 单条消息体类型</li>
 *   <li>{@code TS} — onEvent 实际接收到的类型: 单条消费 = T, 批量消费 = List&lt;T&gt;</li>
 * </ul>
 * <p>
 * <b>使用方式</b>: 业务方一般直接实现 {@link RedisEventBatchListener} / {@link RedisEventSingleListener},
 * 极少需要直接实现本接口。
 *
 * @param <T>  消息体类型
 * @param <TS> 消费回调接收类型 (T 或 List&lt;T&gt;)
 */
interface StreamSubscribe<T, TS> extends EventListener<T, TS>, Qualifier {

    /**
     * 业务作用：服务于哪些 redis server (qualifier 名数组). null = 服务所有 RedisProxy。
     *
     * @return 见上述说明。
     */
    default String[] qualifiers() {
        return null;
    }

    /**
     * 业务作用：消费模式. 决定走 RedisProxy.subscribe (普通流共享 group) 还是 RedisPartition (per-partition 串行)。
     *
     * @return 见上述说明。
     */
    default ConsumeMode mode() {
        return ConsumeMode.PROXY;
    }

    /**
     * 业务作用：stream consumer group 名. PARTITION 模式下忽略 (group 由 streamPrefix 决定)。
     * 不填 PROXY 模式下框架自动用 ME.sequence() 当 consumer name。
     *
     * @return 见上述说明。
     */
    default String group() {
        return null;
    }

    /**
     * 业务作用：是否在 onEvent 成功后自动 XDEL 消息. PARTITION 模式下忽略 (用 ACK 而非 XDEL)。
     *
     * @return 见上述说明。
     */
    default boolean autoDelete() {
        return false;
    }
}
