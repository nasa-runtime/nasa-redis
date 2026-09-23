package io.github.nasaruntime.redis.cache.redis.partition;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 业务作用：在读取、PEL 接管与精确重试之间互斥同一物理记录的业务执行，句柄覆盖确认和恢复责任交接。
 */
final class PartitionRecordExecution {
    private final Map<Coordinate, Object> executing = new HashMap<>();

    /**
     * 业务作用：为已取得读取或恢复容量的批次非阻塞获取精确记录执行权。
     * @param records 已冻结来源权威及代次的坐标
     * @return 仅包含本次获准坐标的句柄；其它坐标仍由原执行者负责
     */
    synchronized Batch begin(List<PartitionRecordRef> records) {
        Object token = new Object();
        Set<Coordinate> acquired = new LinkedHashSet<>();
        for (PartitionRecordRef ref : records) {
            Coordinate coordinate = Coordinate.of(ref);
            if (executing.putIfAbsent(coordinate, token) == null) acquired.add(coordinate);
        }
        return new Batch(token, Set.copyOf(acquired));
    }

    /**
     * 业务作用：按物理位置互斥记录，避免租约重获时旧业务尚未结束却开始新一次业务。
     * @param stream 物理 Stream
     * @param group 消费组
     * @param id 记录 ID
     */
    private record Coordinate(String stream, String group, String id) {
        /**
         * 业务作用：提取不随来源代次变化的物理记录位置。
         * @param ref 冻结坐标
         * @return 当前源内唯一的物理位置
         */
        static Coordinate of(PartitionRecordRef ref) {
            return new Coordinate(ref.stream(), ref.group(), ref.id());
        }
    }

    /** 业务作用：唯一持有一批已获准记录直到业务和后继责任交接完成。 */
    final class Batch implements AutoCloseable {
        private final Object token;
        private final Set<Coordinate> acquired;
        private final AtomicBoolean closed = new AtomicBoolean();

        /**
         * 业务作用：冻结获准集合，迟到关闭只能释放自己的执行权。
         * @param token 唯一执行者身份
         * @param acquired 获准物理坐标
         * 返回: 尚未释放的句柄。
         */
        private Batch(Object token, Set<Coordinate> acquired) {
            this.token = token;
            this.acquired = acquired;
        }

        /**
         * 业务作用：判断该记录是否由当前批次负责执行。
         * @param ref 冻结坐标
         * @return 句柄未关闭且记录在获准集合中时为 true
         */
        boolean owns(PartitionRecordRef ref) {
            return !closed.get() && acquired.contains(Coordinate.of(ref));
        }

        /**
         * 业务作用：在业务与后继责任交接后归还执行权，重复关闭不影响其它执行者。
         * 参数说明: 无。
         * 返回: 无返回值；获准坐标可以由后续恢复重新竞争。
         */
        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) return;
            synchronized (PartitionRecordExecution.this) {
                for (Coordinate coordinate : acquired) executing.remove(coordinate, token);
            }
        }
    }
}
