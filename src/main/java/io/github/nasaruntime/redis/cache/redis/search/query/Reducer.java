package io.github.nasaruntime.redis.cache.redis.search.query;

import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta;
import io.github.nasaruntime.core.function.FunctionUtils;
import io.github.nasaruntime.core.function.SerFunction;
import io.github.nasaruntime.core.utils.ContextUtils;

import java.util.List;

/**
 * Nasa
 * FT.AGGREGATE 的 REDUCE 函数封装
 * <p>
 * 池化语义：从 {@link #POOL} 取实例，所有权交给 {@link RsAggregation} 后由其连锁回收。
 */
@SuppressWarnings("unused")
public final class Reducer implements ObjectPool.Recycler<Reducer> {

    static final ObjectPool<Reducer> POOL = new ObjectPool<>(
            ContextUtils.getPropertyInt("nasa.object-pool.rs-reducer-capacity", 200)) {
        /**
         * 业务作用：池空时新建一个实例，由对象池在借不到空闲实例时调用。
         *
         * <p>参数说明: 无。
         *
         * @return 新建的实例。
         */
        @Override
        public Reducer newObject() {
            return new Reducer();
        }
    };

    private final ObjectPool.PooledHandle<Reducer> handle = new ObjectPool.PooledHandle<>(POOL);

    private String function;
    // null 表示无字段 (COUNT)
    private String javaFieldName;
    // QUANTILE 的 q 值
    private String extraArg;
    private String alias;

    /**
     * 业务作用：私有化构造，强制经由工厂方法从池中取用，避免绕过池直接新建而使池失去意义。
     *
     * <p>参数说明: 无。
     */
    private Reducer() {}

    // ============ 工厂 ============

    /**
     * 业务作用：统计分组内的文档数。
     *
     * @param as 输出字段名，结果行中按此名取值
     * @return 归约算子。
     */
    public static Reducer count(String as) {
        Reducer r = POOL.get();
        r.function = "COUNT";
        r.alias = as;
        return r;
    }

    /**
     * 业务作用：统计分组内某字段的去重取值数。
     * <p>
     * 去重统计要在服务端维护取值集合，<b>基数很大时内存开销显著</b>，
     * 对超大基数应考虑改用近似统计。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer countDistinct(String javaFieldName, String as) {
        return singleArg("COUNT_DISTINCT", javaFieldName, as);
    }

    /**
     * 业务作用：Lambda getter 重载: {@code countDistinct(Order::getUid, "uniqueUsers")}
     *
     * @param getter 见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer countDistinct(SerFunction<T, R> getter, String as) {
        return countDistinct(FunctionUtils.fieldNameCached(getter), as);
    }

    /**
     * 业务作用：对分组内某数值字段求和。
     * 非数值取值会被检索实现按 0 处理而<b>不报错</b>，字段类型需由调用方确认。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer sum(String javaFieldName, String as) {
        return singleArg("SUM", javaFieldName, as);
    }

    /**
     * 业务作用：Lambda getter 重载: {@code sum(Order::getPrice, "total")}
     *
     * @param getter 见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer sum(SerFunction<T, R> getter, String as) {
        return sum(FunctionUtils.fieldNameCached(getter), as);
    }

    /**
     * 业务作用：对分组内某数值字段求平均。
     * 非数值取值会被检索实现按 0 处理而<b>不报错</b>，字段类型需由调用方确认。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer avg(String javaFieldName, String as) {
        return singleArg("AVG", javaFieldName, as);
    }

    /**
     * 业务作用：Lambda getter 重载: {@code avg(Order::getPrice, "avgPrice")}
     *
     * @param getter 见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer avg(SerFunction<T, R> getter, String as) {
        return avg(FunctionUtils.fieldNameCached(getter), as);
    }

    /**
     * 业务作用：对分组内某数值字段取最小值。
     * 非数值取值会被检索实现按 0 处理而<b>不报错</b>，字段类型需由调用方确认。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer min(String javaFieldName, String as) {
        return singleArg("MIN", javaFieldName, as);
    }

    /**
     * 业务作用：Lambda getter 重载: {@code min(Order::getPrice, "minPrice")}
     *
     * @param getter 见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer min(SerFunction<T, R> getter, String as) {
        return min(FunctionUtils.fieldNameCached(getter), as);
    }

    /**
     * 业务作用：对分组内某数值字段取最大值。
     * 非数值取值会被检索实现按 0 处理而<b>不报错</b>，字段类型需由调用方确认。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer max(String javaFieldName, String as) {
        return singleArg("MAX", javaFieldName, as);
    }

    /**
     * 业务作用：Lambda getter 重载: {@code max(Order::getPrice, "maxPrice")}
     *
     * @param getter 见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer max(SerFunction<T, R> getter, String as) {
        return max(FunctionUtils.fieldNameCached(getter), as);
    }

    /**
     * 业务作用：对分组内某数值字段求标准差。
     * 非数值取值会被检索实现按 0 处理而<b>不报错</b>，字段类型需由调用方确认。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer stddev(String javaFieldName, String as) {
        return singleArg("STDDEV", javaFieldName, as);
    }

    /**
     * 业务作用：Lambda getter 重载: {@code stddev(Order::getPrice, "priceStdDev")}
     *
     * @param getter 见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer stddev(SerFunction<T, R> getter, String as) {
        return stddev(FunctionUtils.fieldNameCached(getter), as);
    }

    /**
     * 业务作用：求分组内某数值字段的分位数，用于延迟、金额一类的分布观测。
     * 结果是<b>近似</b>值：精确分位数需要保留全部取值，服务端采用的是有误差的估算算法。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param q             分位点，取值范围 0 到 1
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer quantile(String javaFieldName, double q, String as) {
        // QUANTILE 的 q 必须是 [0,1] 的有限分位数，否则 RediSearch 会拒绝或返回无意义结果。
        if (!Double.isFinite(q) || q < 0.0 || q > 1.0) {
            throw new IllegalArgumentException("QUANTILE q must be a finite value in [0,1], got " + q);
        }
        Reducer r = POOL.get();
        r.function = "QUANTILE";
        r.javaFieldName = javaFieldName;
        r.extraArg = String.valueOf(q);
        r.alias = as;
        return r;
    }

    /**
     * 业务作用：Lambda getter 重载: {@code quantile(Order::getPrice, 0.95, "p95Price")}
     *
     * @param getter 见上述说明
     * @param q      见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer quantile(SerFunction<T, R> getter, double q, String as) {
        return quantile(FunctionUtils.fieldNameCached(getter), q, as);
    }

    /**
     * 业务作用：把分组内某字段的全部取值收集成列表。
     * <b>不做数量限制</b>：分组内文档很多时会产出巨大的结果行，应确保分组粒度足够细。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer toList(String javaFieldName, String as) {
        return singleArg("TOLIST", javaFieldName, as);
    }

    /**
     * 业务作用：Lambda getter 重载: {@code toList(Order::getOrderId, "orderIds")}
     *
     * @param getter 见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer toList(SerFunction<T, R> getter, String as) {
        return toList(FunctionUtils.fieldNameCached(getter), as);
    }

    /**
     * 业务作用：取分组内某字段的第一个取值。
     * 「第一个」由服务端的遍历顺序决定，<b>未指定排序时结果不稳定</b>，
     * 需要确定性结果应先在聚合中指定排序。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    public static Reducer firstValue(String javaFieldName, String as) {
        return singleArg("FIRST_VALUE", javaFieldName, as);
    }

    /**
     * 业务作用：Lambda getter 重载: {@code firstValue(Order::getCreateTime, "earliestTime")}
     *
     * @param getter 见上述说明
     * @param as     见上述说明
     * @return 见上述说明。
     */
    public static <T, R> Reducer firstValue(SerFunction<T, R> getter, String as) {
        return firstValue(FunctionUtils.fieldNameCached(getter), as);
    }

    /**
     * 业务作用：构造单字段参数的归约算子，是各单字段算子的共同实现。
     *
     * @param fn            归约函数名
     * @param javaFieldName 实体的 Java 字段名
     * @param as            输出字段名
     * @return 归约算子。
     */
    private static Reducer singleArg(String fn, String javaFieldName, String as) {
        Reducer r = POOL.get();
        r.function = fn;
        r.javaFieldName = javaFieldName;
        r.alias = as;
        return r;
    }

    // ============ getter ============

    /**
     * 业务作用：读取归约函数名，渲染命令时使用。
     *
     * <p>参数说明: 无。
     *
     * @return 归约函数名。
     */
    public String function() {
        return function;
    }

    /**
     * 业务作用：读取输出字段名，结果行中按此名取值。
     *
     * <p>参数说明: 无。
     *
     * @return 输出字段名。
     */
    public String as() {
        return alias;
    }

    /**
     * 业务作用：读取归约函数的参数列表。
     *
     * <p>参数说明: 无。
     *
     * @return 参数列表。
     */
    public List<String> arguments() {
        throw new RediSearchException("Use appendTo for rendering");
    }

    // ============ 渲染 ============

    /**
     * 业务作用：把本归约算子渲染进命令参数列表。
     * 字段名在此由 Java 字段名映射成索引字段名——映射发生在渲染时而非构造时，
     * 使同一个算子可复用于结构不同的实体。
     *
     * @param args 命令参数列表
     * @param meta 实体结构元信息
     * 返回: 无返回值。
     */
    void appendTo(List<String> args, EntityMeta meta) {
        args.add("REDUCE");
        args.add(function);

        if (javaFieldName == null) {
            // COUNT 无字段
            args.add(extraArg == null ? "0" : "1");
            if (extraArg != null) args.add(extraArg);
        } else {
            int nargs = (extraArg == null) ? 1 : 2;
            args.add(String.valueOf(nargs));
            args.add("@" + meta.fieldByJavaName(javaFieldName).redisName());
            if (extraArg != null) args.add(extraArg);
        }

        if (alias != null) {
            args.add("AS");
            args.add(alias);
        }
    }

    // ============ ObjectPool ============

    /**
     * 业务作用：暴露本实例的池化句柄，供对象池完成借出与归还的状态跟踪。
     *
     * <p>参数说明: 无。
     *
     * @return 本实例的池化句柄。
     */
    @Override
    public ObjectPool.PooledHandle<Reducer> handle() {
        return this.handle;
    }

    /**
     * 业务作用：归还前清空函数名、别名与参数列表。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void restore() {
        this.function = null;
        this.javaFieldName = null;
        this.extraArg = null;
        this.alias = null;
    }
}
