package io.github.nasaruntime.redis.cache.redis.job;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 业务作用：提供不绑定特定指标库的 RedisJob 基础计数和当前值快照，应用可桥接到自身观测系统。
 */
public final class RedisJobMetrics {

    private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CounterFamily> counterFamilies = new ConcurrentHashMap<>();

    /**
     * 业务作用：读取一个累计计数。
     *
     * @param name 指标名
     * @return 尚未发生时为零。
     */
    public long counter(String name) {
        LongAdder value = counters.get(name);
        return value == null ? 0L : value.sum();
    }

    /**
     * 业务作用：读取一个最新值指标。
     *
     * @param name 指标名
     * @return 尚未记录时为零。
     */
    public long gauge(String name) {
        AtomicLong value = gauges.get(name);
        return value == null ? 0L : value.get();
    }

    /**
     * 业务作用：生成一致的只读指标快照，避免调用方持有内部并发容器。
     *
     * <p>参数说明: 无。
     *
     * @return 指标名到当前值的不可变映射。
     */
    public Map<String, Long> snapshot() {
        Map<String, Long> result = new LinkedHashMap<>();
        counters.keySet().stream().sorted().forEach(name -> result.put(name, counter(name)));
        gauges.keySet().stream().sorted().forEach(name -> result.put(name, gauge(name)));
        return Map.copyOf(result);
    }

    /**
     * 业务作用：累计一次框架事件。 @param name 指标名 返回：无返回值。
     */
    void increment(String name) {
        counters.computeIfAbsent(name, ignored -> new LongAdder()).increment();
    }

    /**
     * 业务作用：累计一组框架事件。 @param name 指标名 @param amount 增量 返回：无返回值。
     */
    void add(String name, long amount) {
        counters.computeIfAbsent(name, ignored -> new LongAdder()).add(amount);
    }

    /**
     * 业务作用：发布一个最新观测值。 @param name 指标名 @param value 当前值 返回：无返回值。
     */
    void gauge(String name, long value) {
        gauges.computeIfAbsent(name, ignored -> new AtomicLong()).set(value);
    }

    /**
     * 业务作用：按脚本固定状态码缓存分类计数器，首次出现后不再拼接指标名或转换大小写。
     *
     * @param baseName  指标族基础名
     * @param code      脚本状态码或持久状态
     * @param plainCode 使用基础名本身计数的状态码；没有时传空串
     *                  返回：无返回值。
     */
    void incrementClassified(String baseName, String code, String plainCode) {
        Objects.requireNonNull(baseName, "baseName must not be null");
        Objects.requireNonNull(code, "code must not be null");
        CounterFamily family = counterFamilies.get(baseName);
        if (family == null) {
            CounterFamily candidate = new CounterFamily(baseName, plainCode == null ? "" : plainCode);
            CounterFamily existing = counterFamilies.putIfAbsent(baseName, candidate);
            family = existing == null ? candidate : existing;
        }
        family.increment(code, counters);
    }

    /**
     * 业务作用：保存一个分类指标族已经解析的状态码到计数器映射。
     */
    private static final class CounterFamily {
        private final String baseName;
        private final String plainCode;
        private final ConcurrentHashMap<String, LongAdder> values = new ConcurrentHashMap<>();

        /**
         * 业务作用：建立不可变指标族命名规则。
         *
         * @param baseName  指标族基础名
         * @param plainCode 不增加状态后缀的状态码
         */
        private CounterFamily(String baseName, String plainCode) {
            this.baseName = baseName;
            this.plainCode = plainCode;
        }

        /**
         * 业务作用：复用状态码对应计数器；新状态码只在首次出现时生成规范指标名。
         *
         * @param code        状态码
         * @param allCounters 全局计数器索引
         *                    返回：无返回值。
         */
        private void increment(String code, ConcurrentHashMap<String, LongAdder> allCounters) {
            LongAdder counter = values.get(code);
            if (counter == null) {
                String name = plainCode.equals(code) ? baseName + "_total"
                        : baseName + '_' + code.toLowerCase(Locale.ROOT) + "_total";
                LongAdder candidate = new LongAdder();
                LongAdder registered = allCounters.putIfAbsent(name, candidate);
                LongAdder shared = registered == null ? candidate : registered;
                LongAdder existing = values.putIfAbsent(code, shared);
                counter = existing == null ? shared : existing;
            }
            counter.increment();
        }
    }
}
