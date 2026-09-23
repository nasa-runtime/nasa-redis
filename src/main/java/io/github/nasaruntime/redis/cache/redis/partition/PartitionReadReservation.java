package io.github.nasaruntime.redis.cache.redis.partition;

/** 业务作用：随原始读取责任保存组合预留，未知路由恢复直接续接原份额。 */
final class PartitionReadReservation implements AutoCloseable {
    private PartitionDispatchCapacity.Reservation dispatch;
    final StreamRetryCoordinator.SuccessorReservation successor;
    private int tasks;

    /**
     * 业务作用：接管读取前全部可失败资源预留。
     * @param dispatch 执行和确认容量
     * @param successor 失败坐标承接位置
     * @param count 最大物理记录数
     * 返回: 由 raw permit 唯一管理的预留。
     */
    PartitionReadReservation(PartitionDispatchCapacity.Reservation dispatch,
                             StreamRetryCoordinator.SuccessorReservation successor, int count) {
        this.dispatch = dispatch;
        this.successor = successor;
        this.tasks = count;
    }

    /**
     * 业务作用：缩减空余读取位置，保留真实记录的执行与后继责任。
     * @param actual 真实数量
     * 返回: 无返回值；数量超出预留由外层 COUNT 保护门禁处理。
     */
    synchronized void resize(int actual) {
        if (actual > tasks || dispatch == null) return;
        dispatch.releaseUnusedTasks(tasks - actual);
        dispatch.shrinkCommitRecords(actual);
        tasks = actual;
        successor.shrink(actual);
    }

    /**
     * 业务作用：把原预留转交当前 dispatch，恢复不得重复申请仍由自身持有的额度。
     * @param demand 已解析的真实任务数
     * @param records 本次确认最大记录数
     * @return 原预留；此前已经交接时为 null
     */
    synchronized PartitionDispatchCapacity.Reservation take(int demand, int records) {
        if (dispatch == null) return null;
        dispatch.releaseUnusedTasks(tasks - demand);
        dispatch.shrinkCommitRecords(records);
        var value = dispatch;
        dispatch = null;
        return value;
    }

    /** 业务作用：归还尚未转交的执行与后继份额。参数说明: 无。返回: 重复关闭幂等。 */
    @Override public synchronized void close() {
        if (dispatch != null) { dispatch.close(); dispatch = null; }
        successor.close();
    }
}
