package io.github.nasaruntime.redis.cache.redis.search.query;

import java.util.List;
import java.util.Map;

/**
 * Nasa
 * FT.AGGREGATE 返回结果：总行数 + 行数据
 * <p>
 * 不池化：业务侧持有，生命周期由业务掌控。
 */
public final class AggregationResult {

    private final long totalResults;
    private final List<Map<String, Object>> rows;

    /**
     * 业务作用：承载一次聚合检索的结果：命中总数与各分组行。
     *
     * @param totalResults 命中总数
     * @param rows         各分组的结果行
     */
    public AggregationResult(long totalResults, List<Map<String, Object>> rows) {
        this.totalResults = totalResults;
        this.rows = rows;
    }

    /**
     * 业务作用：读取命中总数。
     * 该值是<b>过滤后的命中数</b>而非返回的行数——分组之后行数通常远小于它。
     *
     * <p>参数说明: 无。
     *
     * @return 命中总数。
     */
    public long totalResults() {
        return totalResults;
    }

    /**
     * 业务作用：读取各分组的结果行，键为输出字段名。
     *
     * <p>参数说明: 无。
     *
     * @return 结果行列表。
     */
    public List<Map<String, Object>> rows() {
        return rows;
    }

    /**
     * 业务作用：判断本次聚合是否没有任何结果行，供调用方跳过后续处理。
     *
     * <p>参数说明: 无。
     *
     * @return 无结果行返回 true。
     */
    public boolean isEmpty() {
        return rows == null || rows.isEmpty();
    }
}
