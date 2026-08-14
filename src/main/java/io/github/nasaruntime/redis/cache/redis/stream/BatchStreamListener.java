package io.github.nasaruntime.redis.cache.redis.stream;

import io.github.nasaruntime.core.base.RecycleLinkedList;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.stream.StreamListener;

/**
 * Batch-aware variant of {@link StreamListener}.
 * Receives a full batch of records from a single poll collected into a {@link RecycleLinkedList}.
 *
 * @param <K> Stream key type.
 * @param <V> Stream value type.
 */
@FunctionalInterface
public interface BatchStreamListener<K, V extends Record<K, ?>> extends StreamListener<K, V> {

    /**
     * 业务作用：Callback for processing a batch of messages obtained from a stream.
     * <p>
     * <b>Important:</b> the {@link RecycleLinkedList} is recycled by this component after this method returns.
     * Do not hold a reference to it beyond this call.
     *
     * @param messages never {@literal null}.
     */
    void onMessage(RecycleLinkedList<V> messages);

    /**
     * 业务作用：Default single-message callback that delegates to batch callback with a singleton list.
     *
     * @param message 消息体
     */
    @Override
    default void onMessage(V message) {
        RecycleLinkedList<V> list = RecycleLinkedList.of();
        list.add(message);
        try {
            onMessage(list);
        } finally {
            list.recycle();
        }
    }
}
