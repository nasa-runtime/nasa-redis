package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.evt.EventListener;
import io.github.nasaruntime.core.base.Partition;
import io.github.nasaruntime.core.evt.PartitionedEventListener;

/**
 * 业务作用：声明 Redis Stream 消费的业务计划、来源、顺序与确认策略。继承 {@link EventListener} 通用事件抽象，
 * 叠加 redis stream 专属的 qualifier / mode / group / autoDelete 语义。
 * <p>
 * <b>继承关系</b>:
 * <pre>
 *   PartitionedEventListener&lt;T, TS&gt;           (io.github.nasaruntime.core.evt — 通用事件抽象)
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
public interface StreamSubscribe<T, TS> extends PartitionedEventListener<T, TS>, Qualifier {

    /**
     * 业务作用：服务于哪些 redis server (qualifier 名数组). null = 服务所有 RedisProxy。
     *
     * 参数说明: 无。
     * @return 目标 RedisProxy 的 qualifier 数组；null 表示服务全部数据源。
     */
    default String[] qualifiers() {
        return null;
    }

    /**
     * 业务作用：选择普通 Stream、物理分区或两种来源；PARTITION 与 BOTH 使用逐条业务和本地按键 Task。
     *
     * 参数说明: 无。
     * @return 默认 PROXY；PARTITION 与 BOTH 拒绝 Batch listener，BOTH 普通来源仅提供 JVM 内顺序。
     */
    default ConsumeMode mode() {
        return ConsumeMode.PROXY;
    }

    /**
     * 业务作用：stream consumer group 名。PARTITION 模式下由物理分区组决定；PROXY 沿用既有消费组配置；
     * BOTH 必须显式声明非空 group，并由内部 dedicated 手工确认容器使用。
     *
     * 参数说明: 无。
     * @return 默认 null；BOTH 必须返回非空普通 Stream group，PARTITION 使用物理分区组命名空间。
     */
    default String group() {
        return null;
    }

    /**
     * 业务作用：是否在业务成功并通过权威确认后删除 Stream 正文。PROXY 沿用既有语义；PARTITION
     * 在 holder-fenced ACK 内执行；BOTH 在 consumer-fenced ACK 内执行。XDEL 对所有 group 生效，
     * 只有确认该 Stream 正文不再服务其它消费组时才能开启。
     *
     * 参数说明: 无。
     * @return 默认 false 保留正文；true 允许按对应来源的成功确认语义删除正文，影响同 Stream 的全部 group。
     */
    default boolean autoDelete() {
        return false;
    }
}
