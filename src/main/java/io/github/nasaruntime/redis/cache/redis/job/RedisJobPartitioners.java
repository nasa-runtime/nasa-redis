package io.github.nasaruntime.redis.cache.redis.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 业务作用：提供跨节点顺序稳定且显式保留空分片的通用分片算法。
 */
public final class RedisJobPartitioners {

    private RedisJobPartitioners() {}

    /**
     * 业务作用：按连续区间尽量均衡地切分输入，保持原始业务顺序。
     *
     * @param <T> 数据类型
     * @return 均衡分片算法。
     */
    public static <T> RedisJobPartitioner<T> balanced() {
        return (items, memberCount) -> {
            if (memberCount <= 0) throw new IllegalArgumentException("memberCount must be greater than zero");
            List<List<T>> result = new ArrayList<>(memberCount);
            int quotient = items.size() / memberCount;
            int remainder = items.size() % memberCount;
            int cursor = 0;
            for (int index = 0; index < memberCount; index++) {
                int size = quotient + (index < remainder ? 1 : 0);
                result.add(Collections.unmodifiableList(new ArrayList<>(items.subList(cursor, cursor + size))));
                cursor += size;
            }
            return Collections.unmodifiableList(result);
        };
    }
}
