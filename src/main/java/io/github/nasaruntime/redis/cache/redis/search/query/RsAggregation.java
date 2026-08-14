package io.github.nasaruntime.redis.cache.redis.search.query;

import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.base.RecycleLinkedList;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta;
import io.github.nasaruntime.redis.cache.redis.search.meta.FieldMeta;
import io.github.nasaruntime.core.function.FunctionUtils;
import io.github.nasaruntime.core.function.SerFunction;
import io.github.nasaruntime.core.utils.ContextUtils;
import io.github.nasaruntime.core.utils.StringUtils;
import org.springframework.data.domain.Sort;

/**
 * Nasa
 * FT.AGGREGATE 聚合查询 DSL
 * <p>
 * pipeline 顺序固定：LOAD → MATCH(criteria) → GROUPBY+REDUCE → APPLY → FILTER → SORTBY → LIMIT，
 * 与调用各构建方法的先后无关
 * <p>
 * 池化语义：from {@link #POOL}，提交给 {@code rs.aggregate(...)} 后框架接管 {@code recycle()},
 * 连锁回收 match Criteria 和 Reducer。与 {@link RsQuery} 相同 — <b>不实现 AutoCloseable</b>,
 * 故意不开 try-with-resources 入口, 防止业务以为"close 才归池"从而双重 recycle 污染池。
 */
@SuppressWarnings("unused")
public final class RsAggregation implements ObjectPool.Recycler<RsAggregation> {

    static final ObjectPool<RsAggregation> POOL = new ObjectPool<>(
            ContextUtils.getPropertyInt("nasa.object-pool.rs-aggregation-capacity", 100)) {
        /**
         * 业务作用：池空时新建一个实例，由对象池在借不到空闲实例时调用。
         *
         * <p>参数说明: 无。
         *
         * @return 新建的实例。
         */
        @Override
        public RsAggregation newObject() {
            return new RsAggregation();
        }
    };

    private final ObjectPool.PooledHandle<RsAggregation> handle = new ObjectPool.PooledHandle<>(POOL);

    private static final int MAX_MATCH = 8;
    private static final int MAX_REDUCE = 16;
    private static final int MAX_APPLY = 8;

    private final Criteria[] matchCriterias = new Criteria[MAX_MATCH];
    private int matchCount;

    private String[] loadFields;

    private String[] groupByFields;
    private final Reducer[] reducers = new Reducer[MAX_REDUCE];
    private int reducerCount;

    private final String[] applyExprs = new String[MAX_APPLY];
    private final String[] applyAliases = new String[MAX_APPLY];
    private int applyCount;

    private String filterExpr;

    private Sort sort;
    private int offset = -1;
    private int limit = -1;

    private int dialectVersion = 2;

    /**
     * 业务作用：私有化构造，强制经由工厂方法从池中取用，避免绕过池直接新建而使池失去意义。
     *
     * <p>参数说明: 无。
     */
    private RsAggregation() {}

    // ============ 工厂 ============

    /**
     * 业务作用：开始构造一个不带过滤的聚合，对全部文档做统计。
     *
     * <p>参数说明: 无。
     *
     * @return 新建的聚合定义。
     */
    public static RsAggregation aggregate() {
        return POOL.get();
    }

    /**
     * 业务作用：以给定过滤条件开始构造聚合，只统计命中的文档。
     *
     * @param filter 过滤条件
     * @return 新建的聚合定义。
     */
    public static RsAggregation aggregate(Criteria filter) {
        return aggregate().match(filter);
    }

    // ============ pipeline ============

    /**
     * 业务作用：设置过滤条件，覆盖此前已设的条件。
     *
     * @param c 过滤条件
     * @return 本聚合定义，便于链式组合。
     */
    public RsAggregation match(Criteria c) {
        if (matchCount >= MAX_MATCH) {
            c.recycle();
            throw new RediSearchException("Too many match criterias (max " + MAX_MATCH + ")");
        }
        matchCriterias[matchCount++] = c;
        return this;
    }

    /**
     * 业务作用：声明要从文档中载入哪些字段供后续算子使用。
     * <p>
     * <b>未载入的字段在归约算子中取不到值</b>，表现为统计结果恒为零或空，
     * 且不会报错——这是聚合中最常见的配置遗漏。
     *
     * @param javaFieldNames 实体的 Java 字段名
     * @return 本聚合定义，便于链式组合。
     */
    public RsAggregation load(String... javaFieldNames) {
        this.loadFields = javaFieldNames;
        return this;
    }

    /**
     * 业务作用：Lambda getter 重载: {@code load(Order::getUid, Order::getPrice)}.
     * 与 {@link Criteria#where(SerFunction)} / {@link #sortByDesc(SerFunction[])} 风格一致.
     * <p>
     * 字段名解析走 {@link FunctionUtils#fieldNameCached} (CHM 缓存). 0 长度等价不发 LOAD 子句 (与 {@link #load(String...)} 一致).
     *
     * @param getters 见上述说明
     * @return 见上述说明。
     */
    @SafeVarargs
    public final <T> RsAggregation load(SerFunction<T, ?>... getters) {
        return this.load(FunctionUtils.fieldNamesCached(getters));
    }

    /**
     * 业务作用：声明分组维度，结果按这些字段的取值组合成行。
     * 不声明分组时全部文档归为一组，产出单行汇总。
     *
     * @param javaFieldNames 实体的 Java 字段名
     * @return 本聚合定义，便于链式组合。
     */
    public RsAggregation groupBy(String... javaFieldNames) {
        this.groupByFields = javaFieldNames;
        return this;
    }

    /**
     * 业务作用：Lambda getter 重载: {@code groupBy(Order::getUid, Order::getStatus)}.
     * <p>
     * 字段名解析走 {@link FunctionUtils#fieldNameCached} (CHM 缓存). 至少需要 1 个字段 — 0 长度延迟到 {@link #toCommandArgs}
     * 抛"GROUPBY requires at least one field" (与 {@link #groupBy(String...)} 一致).
     *
     * @param getters 见上述说明
     * @return 见上述说明。
     */
    @SafeVarargs
    public final <T> RsAggregation groupBy(SerFunction<T, ?>... getters) {
        return this.groupBy(FunctionUtils.fieldNamesCached(getters));
    }

    /**
     * 业务作用：为当前分组追加一个归约算子。
     * 多个算子可叠加，各自产出一个输出字段；<b>输出字段名重复时后者覆盖前者</b>。
     *
     * @param reducer 归约算子
     * @return 本聚合定义，便于链式组合。
     */
    public RsAggregation reduce(Reducer reducer) {
        if (reducerCount >= MAX_REDUCE) {
            reducer.recycle();
            throw new RediSearchException("Too many reducers (max " + MAX_REDUCE + ")");
        }
        reducers[reducerCount++] = reducer;
        return this;
    }

    /**
     * 业务作用：APPLY 表达式。<b>trusted-only</b>：{@code expression} 原样下发给 RediSearch，禁止拼入终端用户输入
     * (聚合表达式注入)。需面向不可信输入时请用参数化/白名单封装, 不要手拼字符串。
     * <p>
     * fail-fast: expression / as 都必须非空白. null / 空字符串会让 toCommandArgs 渲染出 {@code APPLY null AS ""}
     * 让 Redis 报无意义语法错, 入口提前拒绝.
     *
     * @param expression 见上述说明
     * @param as         见上述说明
     * @return 见上述说明。
     */
    public RsAggregation apply(String expression, String as) {
        if (StringUtils.isBlank(expression)) {
            throw new IllegalArgumentException("apply(expression, as) requires non-blank expression");
        }
        if (StringUtils.isBlank(as)) {
            throw new IllegalArgumentException("apply(expression, as) requires non-blank alias");
        }
        if (applyCount >= MAX_APPLY) throw new RediSearchException("Too many apply expressions");
        applyExprs[applyCount] = expression;
        applyAliases[applyCount] = as;
        applyCount++;
        return this;
    }

    /**
     * 业务作用：FILTER 表达式。<b>trusted-only</b>：{@code expression} 原样下发，同 {@link #apply} 禁止拼入不可信输入。
     * <p>
     * fail-fast: expression 必须非空白. 不支持"传 null 清空 filter" 语义 — 链式 DSL 每次新建实例, 没有"清空"场景.
     *
     * @param expression 见上述说明
     * @return 见上述说明。
     */
    public RsAggregation filter(String expression) {
        if (StringUtils.isBlank(expression)) {
            throw new IllegalArgumentException("filter(expression) requires non-blank expression");
        }
        this.filterExpr = expression;
        return this;
    }

    /**
     * 业务作用：追加 FT.AGGREGATE 的 SORTBY. 与 {@link RsQuery#sortBy(Sort)} 命名对齐, 但<b>语义不同</b>:
     * <p>
     * <b>RsAggregation: 累加</b> — 多次调用合并所有 Sort.Order 到一个总排序里 (FT.AGGREGATE SORTBY 支持多字段 + 每字段独立方向).
     * 这样 {@code sortByDesc(Order::getUid).sortByAsc(Order::getId)} 链式累加成 {@code [uid DESC, id ASC]},
     * 等价 {@code sortBy(Sort.by(Sort.Order.desc("uid"), Sort.Order.asc("id")))}.
     * <p>
     * <b>RsQuery: 覆盖 + fail-fast</b> — FT.SEARCH 单字段限制, 重复设 sort 直接抛, 引导用 RsAggregation.
     * <p>
     * null / 空 Sort 视为 no-op, 不影响已累积的 sort. 用 {@link Sort#and(Sort)} 拼接, Spring 内部新建组合 Sort,
     * 不 mutate 已有实例.
     *
     * @param sort 见上述说明
     * @return 见上述说明。
     */
    public RsAggregation sortBy(Sort sort) {
        if (sort == null || !sort.isSorted()) return this;
        this.sort = (this.sort == null || !this.sort.isSorted()) ? sort : this.sort.and(sort);
        return this;
    }

    /**
     * 业务作用：便捷重载: 按 对象字段名追加一段降序排序. 转发 {@link #sortBy(Sort)}, <b>累加语义</b>:
     * 链式调用与前面已设的 sort 合并, 不覆盖.
     * <p>
     * 例:
     * <pre>
     * agg.sortByDesc("uid").sortByAsc("id")
     * // 累加成 [uid DESC, id ASC] → SORTBY 4 @uid DESC @id ASC
     *
     * agg.sortByDesc("uid", "createTime")
     * // 一次传多字段同方向 → SORTBY 4 @uid DESC @createTime DESC
     * </pre>
     * <p>
     * 字段名可以是: 实体 对象字段名 (会翻译为 RediSearch alias), 或 reducer/apply 阶段产生的 alias (原样下发).
     * <p>
     * FT.AGGREGATE SORTBY 支持多字段, 与 {@link RsQuery#sortByDesc(String)} 的单字段限制不同.
     *
     * @param javaFieldNames 见上述说明
     * @return 见上述说明。
     */
    public RsAggregation sortByDesc(String... javaFieldNames) {
        return this.sortBy(Sort.by(Sort.Direction.DESC, javaFieldNames));
    }

    /**
     * 业务作用：便捷重载: 按 对象字段名追加一段升序排序. 同 {@link #sortByDesc(String...)} 累加语义,
     * 支持多字段 + reducer/apply alias.
     *
     * @param javaFieldNames 见上述说明
     * @return 见上述说明。
     */
    public RsAggregation sortByAsc(String... javaFieldNames) {
        return this.sortBy(Sort.by(Sort.Direction.ASC, javaFieldNames));
    }

    /**
     * 业务作用：Lambda getter 重载: {@code sortByDesc(Order::getUid, Order::getCreateTime)}.
     * 与 {@link Criteria#where(SerFunction)} / {@link RsQuery#sortByDesc(SerFunction)} 风格一致.
     * 累加语义同 {@link #sortByDesc(String...)} — 链式调用与已设 sort 合并.
     * <p>
     * 解析走 {@link FunctionUtils#fieldNameCached} (CHM 缓存). 仅支持实体 getter, reducer/apply alias 仍要用
     * {@link #sortByDesc(String...)} 字符串重载传 (alias 不是 getter, 无法用 method reference).
     *
     * @param getters 见上述说明
     * @return 见上述说明。
     */
    @SafeVarargs
    public final <T> RsAggregation sortByDesc(SerFunction<T, ?>... getters) {
        return this.sortByDesc(FunctionUtils.fieldNamesCached(getters));
    }

    /**
     * 业务作用：Lambda getter 重载: 升序. 同 {@link #sortByDesc(SerFunction[])} 累加语义.
     *
     * @param getters 见上述说明
     * @return 见上述说明。
     */
    @SafeVarargs
    public final <T> RsAggregation sortByAsc(SerFunction<T, ?>... getters) {
        return this.sortByAsc(FunctionUtils.fieldNamesCached(getters));
    }

    /**
     * 业务作用：便捷重载: 按 page / size 自动算 offset+limit. <b>page 从 1 开始</b> (MySQL PageHelper / UI 自然语言"第几页"习惯,
     * 传 0 会被拒绝）。等价 {@code limit((page - 1) * size, size)}。
     * <p>
     * 示例 (size=20):
     * <ul>
     *   <li>{@code page(1, 20)} → 第 1 页 → LIMIT 0 20</li>
     *   <li>{@code page(2, 20)} → 第 2 页 → LIMIT 20 20</li>
     *   <li>{@code page(3, 20)} → 第 3 页 → LIMIT 40 20</li>
     * </ul>
     * <p>
     * 越界 fail-fast: page 必须 >= 1, size 必须 > 0; offset 超 {@code Integer.MAX_VALUE} 直接抛.
     *
     * @param page 见上述说明
     * @param size 见上述说明
     * @return 见上述说明。
     */
    public RsAggregation page(int page, int size) {
        if (page < 1) {
            throw new IllegalArgumentException("page(page, size) requires page >= 1 (1-based), got " + page);
        }
        if (size <= 0) {
            throw new IllegalArgumentException("page(page, size) requires size > 0, got " + size);
        }
        long off = (long) (page - 1) * size;
        if (off > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("page offset " + off + " exceeds Integer.MAX_VALUE; "
                    + "用更小的 page 或显式 limit(offset, limit)");
        }
        return this.limit((int) off, size);
    }

    /**
     * 业务作用：设置 LIMIT offset count.
     * <p>
     * 校验: offset &gt;= 0, limit &gt;= 1. <b>禁止 limit=0</b> — 业务侧通过 aggregate 路径传 0 返回 0 条 row 没有意义,
     * 没有业务意义。不限制返回行数时不要调用本方法（默认 offset=-1/limit=-1 时 renderArgs 不发送 LIMIT 子句）。
     *
     * @param offset 偏移量
     * @param limit  数量上限
     */
    public RsAggregation limit(int offset, int limit) {
        // RediSearch LIMIT 不接受负 offset，且 limit=0 没有业务意义。
        if (offset < 0) {
            throw new IllegalArgumentException("aggregate limit requires offset >= 0, got offset=" + offset);
        }
        if (limit < 1) {
            throw new IllegalArgumentException("aggregate limit requires limit >= 1, got limit=" + limit
                    + " — 不限行数请不调本方法");
        }
        this.offset = offset;
        this.limit = limit;
        return this;
    }

    /**
     * 业务作用：指定查询语法版本，含义与查询构造器上的同名设置一致。
     *
     * @param dialect 语法版本号
     * @return 本聚合定义，便于链式组合。
     */
    public RsAggregation dialect(int dialect) {
        this.dialectVersion = dialect;
        return this;
    }

    // ============ 渲染 ============

    /**
     * 业务作用：把整个聚合定义渲染成命令参数数组。
     * 渲染顺序即命令的语法顺序，<b>不可调整</b>——过滤、载入、分组、归约、分页各段的先后
     * 由检索实现的语法规定，顺序错了会被解析成完全不同的语义。
     *
     * @param meta 实体结构元信息
     * @return 命令参数数组。
     */
    public String[] toCommandArgs(EntityMeta meta) {
        // 池化主 args 链表 + 临时 sortArgs (拼接前需要先算 size, 必须先全部收集再写入主 args).
        // toArray 后立刻 recycle, 节点回池零稳态 GC.
        RecycleLinkedList<String> args = RecycleLinkedList.of();
        RecycleLinkedList<String> sortArgs = null;
        try {
            args.add(meta.index());
            args.add(buildQueryString(meta));

            if (loadFields != null && loadFields.length > 0) {
                args.add("LOAD");
                args.add(String.valueOf(loadFields.length));
                for (String jn : loadFields) {
                    args.add("@" + meta.fieldByJavaName(jn).redisName());
                }
            }

            if (groupByFields != null) {
                // GROUPBY 0 没有任何分组键且语义不明确，提前拒绝无分组键的调用。
                if (groupByFields.length == 0) {
                    throw new RediSearchException("RsAggregation.groupBy(...) requires at least one field");
                }
                args.add("GROUPBY");
                args.add(String.valueOf(groupByFields.length));
                for (String jn : groupByFields) {
                    args.add("@" + meta.fieldByJavaName(jn).redisName());
                }
                for (int i = 0; i < reducerCount; i++) {
                    reducers[i].appendTo(args, meta);
                }
            } else if (reducerCount > 0) {
                // 全局聚合没有分组键时必须发送 GROUPBY 0 REDUCE，才能对整个查询结果执行 COUNT/SUM/AVG。
                args.add("GROUPBY");
                args.add("0");
                for (int i = 0; i < reducerCount; i++) {
                    reducers[i].appendTo(args, meta);
                }
            }

            for (int i = 0; i < applyCount; i++) {
                args.add("APPLY");
                args.add(applyExprs[i]);
                args.add("AS");
                args.add(applyAliases[i]);
            }

            if (filterExpr != null) {
                args.add("FILTER");
                args.add(filterExpr);
            }

            if (sort != null && sort.isSorted()) {
                // FT.AGGREGATE SORTBY 支持多字段: SORTBY N @f1 ASC @f2 DESC ...
                // 若 property 是实体 对象字段名 → 翻译成 RediSearch 实际字段名(保持与 RsQuery 一致);
                // 若 property 是 reducer/apply alias(实体里没有) → 原样下发, RediSearch 自行识别.
                sortArgs = RecycleLinkedList.of();
                for (Sort.Order o : sort) {
                    String prop = o.getProperty();
                    String redisName = resolveSortProperty(meta, prop);
                    sortArgs.add("@" + redisName);
                    sortArgs.add(o.isDescending() ? "DESC" : "ASC");
                }
                args.add("SORTBY");
                args.add(String.valueOf(sortArgs.size()));
                args.addAll(sortArgs);
            }

            if (offset >= 0 && limit >= 0) {
                args.add("LIMIT");
                args.add(String.valueOf(offset));
                args.add(String.valueOf(limit));
            }

            if (dialectVersion > 0) {
                args.add("DIALECT");
                args.add(String.valueOf(dialectVersion));
            }

            return args.toArray(new String[0]);
        } finally {
            args.recycle();
            if (sortArgs != null) sortArgs.recycle();
        }
    }

    /**
     * 业务作用：解析 SORTBY property: 优先匹配实体字段并翻译为 RediSearch alias,
     * 命中失败时认为是 reducer/apply alias, 原样返回(实体侧不存在所以 fieldByJVMName 会抛, 用 try-catch 兜底).
     *
     * @param meta 见上述说明
     * @param prop 见上述说明
     */
    private static String resolveSortProperty(EntityMeta meta, String prop) {
        try {
            FieldMeta fm = meta.fieldByJavaName(prop);
            return fm.redisName();
        } catch (RediSearchException ignored) {
            // 不是实体字段 → 必然是聚合阶段产生的别名(reducer AS / apply AS), 直接透传
            return prop;
        }
    }

    /**
     * 业务作用：把过滤条件渲染成查询串；无条件时渲染成匹配全部文档的形式。
     *
     * @param meta 实体结构元信息
     * @return 查询串。
     */
    private String buildQueryString(EntityMeta meta) {
        if (matchCount == 0) return "*";
        StringBuilder sb = new StringBuilder(64);
        for (int i = 0; i < matchCount; i++) {
            if (i > 0) sb.append(' ');
            matchCriterias[i].appendTo(sb, meta);
        }
        return sb.toString();
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
    public ObjectPool.PooledHandle<RsAggregation> handle() {
        return this.handle;
    }

    /**
     * 业务作用：归还前清空过滤条件、载入字段、分组与归约算子。
     * 过滤条件与归约算子本身也是池化对象，在此一并归还。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void restore() {
        for (int i = 0; i < matchCount; i++) {
            matchCriterias[i].recycle();
            matchCriterias[i] = null;
        }
        matchCount = 0;

        loadFields = null;
        groupByFields = null;

        for (int i = 0; i < reducerCount; i++) {
            reducers[i].recycle();
            reducers[i] = null;
        }
        reducerCount = 0;

        for (int i = 0; i < applyCount; i++) {
            applyExprs[i] = null;
            applyAliases[i] = null;
        }
        applyCount = 0;

        filterExpr = null;
        sort = null;
        offset = -1;
        limit = -1;
        dialectVersion = 2;
    }
}
