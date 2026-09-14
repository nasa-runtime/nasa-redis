package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.core.utils.ContextUtils;

/**
 * 池化的 pipeline 命令槽位.
 * <p>
 * 业务线程通过 {@link #of(byte)} 借实例, 链式填充 op / arg1~3 / longArg1~2 / extras / future, 投入 {@link RedisProxy#pipeline()} cqueue;
 * TimingWheel 1ms tick 单消费者把 cqueue 里的 task 槽位批量搬入 {@code LettucePipeline.CmdBuffer}, 之后 {@link #recycle()} 归池.
 * <p>
 * 设计上是 {@code LettucePipeline.CmdBuffer} 单槽位的最小描述, 字段顺序/语义与 CmdBuffer 同列对应,
 * 方便 drainTasks 内一行映射.
 * <p>
 * <b>生命周期</b>
 * <ul>
 *   <li>{@code arg1/arg2/arg3/extras/lf} 都是引用直传 — drain 时拷入 CmdBuffer 槽位, task.recycle 仅清引用</li>
 *   <li>extras 若是 {@code RecycleLinkedMap}, cascade recycle 由 CmdBuffer.restore 负责 (避免 double-recycle)</li>
 *   <li>lf 的生命周期由业务线程持有, task 不管</li>
 *   <li>队列拒绝未接纳命令时就地异常完成 lf，并归还尚未移交给 CmdBuffer 的池化 extras 与槽位</li>
 * </ul>
 */
public final class PipelineTask implements ObjectPool.Recycler<PipelineTask> {

    /**
     * pool 容量默认 16384. 单个 PipelineTask 仅几十字节 (5 个引用 + 2 个 long), 16384 个 ≈ 1MB 上限可接受.
     * <p>
     * 16384 用于覆盖 1ms tick 内数千至上万次 burst offer，避免超出池容量后回退到 {@code new PipelineTask()}。
     * 单消费者（TimingWheel）在 tick 内 recycle 还池，稳态下池占用通常远低于 capacity。
     */
    static final ObjectPool<PipelineTask> POOL = new ObjectPool<>(
            ContextUtils.getPropertyInt("nasa.object-pool.pipeline-task-capacity", 16384)) {
        /**
         * 业务作用：池空时新建一个实例，由对象池在借不到空闲实例时调用。
         *
         * <p>参数说明: 无。
         *
         * @return 新建的实例。
         */
        @Override
        public PipelineTask newObject() {
            return new PipelineTask();
        }
    };

    private final ObjectPool.PooledHandle<PipelineTask> handle = new ObjectPool.PooledHandle<>(POOL);

    short op;
    byte[] arg1;
    Object arg2;
    Object arg3;
    long longArg;
    long longArg2;
    Object extras;
    LettuceFuture<?> lf;

    /**
     * 业务作用：私有化构造，强制经由工厂方法从池中取用。
     *
     * <p>参数说明: 无。
     */
    private PipelineTask() {}

    /**
     * 业务作用：从池借空白实例并设置 op. 业务方继续链式填参数.
     * <p>
     * op 用 short 容纳 byte 类型 OP 常量 (1-127) 与 short 类型扩展 OP 常量 (128+) 两种, byte 字面量自动 widening.
     *
     * @param op 见上述说明
     * @return 见上述说明。
     */
    public static PipelineTask of(short op) {
        PipelineTask t = POOL.get();
        t.op = op;
        return t;
    }

    /**
     * 业务作用：设置第一个命令参数，恒为序列化后的键。
     *
     * @param v 第一个命令参数，恒为序列化后的键
     * @return 本实例，便于链式装配。
     */
    public PipelineTask arg1(byte[] v) {
        this.arg1 = v;
        return this;
    }

    /**
     * 业务作用：设置第二个命令参数，按操作码决定其含义。
     *
     * @param v 第二个命令参数，按操作码决定其含义
     * @return 本实例，便于链式装配。
     */
    public PipelineTask arg2(Object v) {
        this.arg2 = v;
        return this;
    }

    /**
     * 业务作用：设置第三个命令参数，按操作码决定其含义。
     *
     * @param v 第三个命令参数，按操作码决定其含义
     * @return 本实例，便于链式装配。
     */
    public PipelineTask arg3(Object v) {
        this.arg3 = v;
        return this;
    }

    /**
     * 业务作用：设置第一个数值参数，按操作码决定其含义。
     *
     * @param v 第一个数值参数，按操作码决定其含义
     * @return 本实例，便于链式装配。
     */
    public PipelineTask longArg(long v) {
        this.longArg = v;
        return this;
    }

    /**
     * 业务作用：设置第二个数值参数，按操作码决定其含义。
     *
     * @param v 第二个数值参数，按操作码决定其含义
     * @return 本实例，便于链式装配。
     */
    public PipelineTask longArg2(long v) {
        this.longArg2 = v;
        return this;
    }

    /**
     * 业务作用：设置额外参数槽，承载三个固定槽位放不下的复杂参数。
     *
     * @param v 额外参数槽，承载三个固定槽位放不下的复杂参数
     * @return 本实例，便于链式装配。
     */
    public PipelineTask extras(Object v) {
        this.extras = v;
        return this;
    }

    /**
     * 业务作用：设置承接命令结果的占位。
     *
     * @param v 承接命令结果的占位
     * @return 本实例，便于链式装配。
     */
    public PipelineTask future(LettuceFuture<?> v) {
        this.lf = v;
        return this;
    }

    /**
     * 业务作用：收口未被命令队列接纳的槽位，异常完成等待方并归还尚未移交给 CmdBuffer 的池化参数。
     *
     * @param failure 队列拒绝接纳的确定原因
     *                返回: 无返回值；不会发送 Redis 命令，槽位及自有 extras 被归还，future 的回收仍由调用方负责。
     */
    void reject(Throwable failure) {
        try {
            // 未入队的 extras 没有 CmdBuffer 接管者，必须在当前拒绝分支归还以免永久占用池化映射。
            if (extras instanceof RecycleLinkedMap<?, ?> map) map.recycle();
        } finally {
            try {
                if (lf != null) lf.completeExceptionally(failure);
            } finally {
                recycle();
            }
        }
    }

    /**
     * 业务作用：暴露本实例的池化句柄，供对象池完成借出与归还的状态跟踪。
     *
     * <p>参数说明: 无。
     *
     * @return 本实例的池化句柄。
     */
    @Override
    public ObjectPool.PooledHandle<PipelineTask> handle() {
        return this.handle;
    }

    /**
     * 业务作用：归还前清空各参数槽与结果占位的引用。
     * <b>必须清引用</b>：参数槽可能持有大字节数组或映射，不清会让已归还的实例仍把它们钉在内存里，
     * 表现为一个随批次量增长而不释放的内存占用。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回: 无返回值。
     */
    @Override
    public void restore() {
        this.op = 0;
        this.arg1 = null;
        this.arg2 = null;
        this.arg3 = null;
        this.longArg = 0L;
        this.longArg2 = 0L;
        // extras 引用所有权已转给 CmdBuffer, 由 CmdBuffer.restore 负责 cascade recycle, 此处仅清引用
        this.extras = null;
        // lf 生命周期归业务线程, task 不参与回收
        this.lf = null;
    }
}
