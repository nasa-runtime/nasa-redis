package io.github.nasaruntime.redis.cache.redis.search.query;

import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.base.RecycleLinkedList;
import io.github.nasaruntime.redis.cache.redis.search.annotation.DataType;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta;
import io.github.nasaruntime.redis.cache.redis.search.meta.FieldMeta;
import io.github.nasaruntime.core.function.FunctionUtils;
import io.github.nasaruntime.core.function.SerFunction;
import io.github.nasaruntime.core.utils.ContextUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

/**
 * Nasa
 * 查询容器：Criteria（AND）+ Sort + 分页 + 投影 + 修饰符
 *
 * <h2>⚠ 池化语义 — 强一次性, 必读</h2>
 * <ul>
 *   <li><b>框架接管 recycle</b>: 业务调 {@code rs.find / findOne / findKeys / findOptionalOne /
 *       count / exists / remove / aggregate} 或 ARRAY 模式 {@code jsonArrayOps().findInArray /
 *       removeInArray / countInArray} 任意方法后, query 立即归池, 内部状态 (criterias /
 *       sort / 分页 / 投影) 全部 reset.</li>
 *   <li><b>实例严格一次性</b>: 拿到的 RsQuery <b>只能提交一次</b>给框架. 提交后业务不能再持有或访问 —
 *       拿同一变量再次调用 rs.xxx() 是<b>严重误用</b>：第二次进入时 query 已 restore 或被别的线程借走，
 *       命中错误数据 / NPE / 并发状态污染.</li>
 *   <li><b>跑相同查询多次必须每次重新取</b>: 每次都 {@code RsQuery.query(...)} 从 {@link #POOL} 新借,
 *       <b>禁止</b>把同一局部变量 q 传两次给 rs.find.</li>
 *   <li><b>Criteria 所有权级联</b>: query 持有的所有 {@link Criteria} 在 query.recycle() 时一并连锁归池,
 *       业务侧不要再持有/手动 recycle 已经交给 RsQuery 的 Criteria.</li>
 *   <li><b>不实现 AutoCloseable</b>: 故意不开 try-with-resources 入口, 防止业务以为"close 才归池"
 *       从而双重 recycle 污染池.</li>
 * </ul>
 *
 * <h2>✓ 正确用法</h2>
 * <pre>
 * // 1. 一行一查 (撮合最常见, 不持局部变量)
 * List&lt;Order&gt; orders = rs.find(
 *         RsQuery.query(Criteria.where("uid").is("u_001")),
 *         Order.class);
 *
 * // 2. 链式组装后立即提交, 提交后 q 不能再用
 * RsQuery q = RsQuery.query()
 *         .where(Criteria.where("userId").is("u_001"))
 *         .and(Criteria.where("price").between(60000, 70000))
 *         .sortByDesc(Order::getCreateTime)
 *         .page(PageRequest.of(0, 20));
 * List&lt;Order&gt; r = rs.find(q, Order.class);  // 提交即归池, 局部变量 q 之后绝不可访问
 *
 * // 3. 跑两次相同查询 — 必须每次重新取
 * List&lt;Order&gt; r1 = rs.find(RsQuery.query(Criteria.where("uid").is("u1")), Order.class);
 * List&lt;Order&gt; r2 = rs.find(RsQuery.query(Criteria.where("uid").is("u2")), Order.class);
 * </pre>
 *
 * <h2>✗ 反模式</h2>
 * <pre>
 * // 同一 query 提交两次: 第二次 q 已 restore, 内部 criterias=null, 命中"空查询" 或 NPE
 * RsQuery q = RsQuery.query(Criteria.where("uid").is("u1"));
 * rs.find(q, Order.class);
 * rs.find(q, Order.class);  // ✗ 已归池，不可再次提交
 *
 * // 提交后还访问 query 字段
 * RsQuery q = RsQuery.query(...);
 * List&lt;Order&gt; r = rs.find(q, Order.class);
 * int lim = q.limit();      // ✗ q 已归池, limit 已 reset 到默认 10
 * </pre>
 */
@SuppressWarnings("unused")
public final class RsQuery implements ObjectPool.Recycler<RsQuery> {

    static final ObjectPool<RsQuery> POOL = new ObjectPool<>(
            ContextUtils.getPropertyInt("nasa.object-pool.rs-query-capacity", 200)) {
        /**
         * 业务作用：池空时新建一个实例，由对象池在借不到空闲实例时调用。
         *
         * <p>参数说明: 无。
         *
         * @return 新建的实例。
         */
        @Override
        public RsQuery newObject() {
            return new RsQuery();
        }
    };

    private final ObjectPool.PooledHandle<RsQuery> handle = new ObjectPool.PooledHandle<>(POOL);

    /**
     * 池化 Criteria 列表. RsQuery 实例池化, criterias 字段跟 RsQuery 同生命周期 (一次 of() 借, 永不 recycle, restore 时 clear).
     * Node 走 RecycleLinkedList 节点池, 单次 add/clear 都是 O(1) 池往返, 零稳态 GC.
     * 无 size 上限: 业务真要 100 个 AND 自负责任, RediSearch 协议层会做查询长度兜底.
     */
    private final RecycleLinkedList<Criteria> criterias = RecycleLinkedList.of();
    private Sort sort;
    private int offset;
    private int limit = 10;
    /**
     * true 表示 caller 显式调用了 with(Pageable)/limit(...) 设过分页; false 时下游 (findKeys 等) 可按业务语义覆盖默认值.
     */
    private boolean limitExplicit;
    private String[] returnFields;
    private int dialectVersion = 2;

    /**
     * 业务作用：私有化构造，强制经由工厂方法从池中取用，避免绕过池直接新建而使池失去意义。
     *
     * <p>参数说明: 无。
     */
    private RsQuery() {}

    // ============ 工厂 ============

    /**
     * 业务作用：开始构造一个无条件的查询，随后按需补充条件、排序与分页。
     *
     * <p>参数说明: 无。
     *
     * @return 新建的查询。
     */
    public static RsQuery query() {
        return POOL.get();
    }

    /**
     * 业务作用：以给定条件开始构造查询。
     *
     * @param criteria 初始条件
     * @return 新建的查询。
     */
    public static RsQuery query(Criteria criteria) {
        return query().where(criteria);
    }

    /**
     * 业务作用：构造一个匹配全部文档的查询。
     * <b>会命中索引中的所有文档</b>，仅在配合分页或用于统计总量时使用。
     *
     * <p>参数说明: 无。
     *
     * @return 匹配全部文档的查询。
     */
    public static RsQuery empty() {
        return query().where(Criteria.all());
    }

    // ============ 条件 ============

    /**
     * 业务作用：设置查询条件，覆盖此前已设的条件。
     *
     * @param c 查询条件
     * @return 本查询，便于链式组合。
     */
    public RsQuery where(Criteria c) {
        criterias.add(c);
        return this;
    }

    /**
     * 业务作用：以「与」追加一个条件。
     * 与覆盖式设置的区别是保留已有条件；多次追加按调用顺序依次与合。
     *
     * @param c 要追加的条件
     * @return 本查询，便于链式组合。
     */
    public RsQuery and(Criteria c) {
        return where(c);
    }

    // ============ 排序 / 分页 ============

    /**
     * 业务作用：设置 FT.SEARCH 的 SORTBY. <b>FT.SEARCH 只支持单字段排序</b>,
     * 本方法做两层 fail-fast 校验, stack trace 直指业务侧调用点 (比 renderArgs 兜底报错更易排查):
     * <ol>
     *   <li>{@code this.sort != null}: 已经设过 sort 不允许第二次 — 业务侧链式调用
     *       {@code sortByDesc(a).sortByAsc(b)} 这种"想表达多字段"的写法第二次会抛, 引导改用 RsAggregation</li>
     *   <li>传入 Sort 本身含多字段: {@code Sort.by(DESC, "a", "b")} 或 {@code Sort.by(Order.desc("a"), Order.asc("b"))}
     *       都直接抛, 不让多字段 Sort 进入字段后才在 renderArgs 抛</li>
     * </ol>
     * 多字段 / 混合方向请用 {@link RsAggregation#sortBy(Sort)}, FT.AGGREGATE SORTBY 支持多字段独立方向.
     *
     * @param sort 见上述说明
     * @return 见上述说明。
     */
    public RsQuery sortBy(Sort sort) {
        if (this.sort != null) {
            throw new RediSearchException("RsQuery.sortBy already set — FT.SEARCH SORTBY only supports a single field. "
                    + "多字段排序请用 RsAggregation.sortBy / sortByDesc / sortByAsc.");
        }
        if (sort != null && sort.isSorted()) {
            int n = 0;
            for (Sort.Order ignored : sort) n++;
            if (n > 1) {
                throw new RediSearchException("FT.SEARCH SORTBY only supports a single field, got Sort with " + n
                        + " fields. 多字段排序请用 RsAggregation.sortBy / sortByDesc / sortByAsc.");
            }
        }
        this.sort = sort;
        return this;
    }

    /**
     * 业务作用：便捷重载: 按 对象字段名降序排序. 等价 {@code sortBy(Sort.by(Direction.DESC, field))}.
     * <p>
     * <b>故意是单参数而非 vararg</b>: FT.SEARCH SORTBY 只支持单字段, vararg 形式会让多字段调用编译通过但
     * 运行时抛 {@link io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException}, 调试不友好. 单参数让 IDE 在编译期就阻止多字段误用.
     * <p>
     * 多字段排序请用 {@link RsAggregation#sortByDesc(String...)} (FT.AGGREGATE 支持多字段 + 每字段独立方向).
     *
     * @param javaFieldName 见上述说明
     * @return 见上述说明。
     */
    public RsQuery sortByDesc(String javaFieldName) {
        return this.sortBy(Sort.by(Sort.Direction.DESC, javaFieldName));
    }

    /**
     * 业务作用：便捷重载: 按 对象字段名升序排序. 同 {@link #sortByDesc(String)} 单字段限制.
     *
     * @param javaFieldName 见上述说明
     * @return 见上述说明。
     */
    public RsQuery sortByAsc(String javaFieldName) {
        return this.sortBy(Sort.by(Sort.Direction.ASC, javaFieldName));
    }

    /**
     * 业务作用：Lambda getter 重载: {@code sortByDesc(Order::getCreateTime)}. 与 {@link Criteria#where(SerFunction)} 风格一致.
     * <p>
     * 解析走 {@link FunctionUtils#fieldNameCached} (CHM 缓存 writeReplace 解析), 热路径反复用同一 getter 零额外开销.
     * 单参数限制同 {@link #sortByDesc(String)} — FT.SEARCH SORTBY 只支持单字段.
     *
     * @param getter 见上述说明
     * @return 见上述说明。
     */
    public <T, R> RsQuery sortByDesc(SerFunction<T, R> getter) {
        return this.sortByDesc(FunctionUtils.fieldNameCached(getter));
    }

    /**
     * 业务作用：Lambda getter 重载: {@code sortByAsc(Order::getCreateTime)}. 同 {@link #sortByDesc(SerFunction)} 单字段限制.
     *
     * @param getter 见上述说明
     * @return 见上述说明。
     */
    public <T, R> RsQuery sortByAsc(SerFunction<T, R> getter) {
        return this.sortByAsc(FunctionUtils.fieldNameCached(getter));
    }

    /**
     * 业务作用：按分页对象设置偏移与条数。
     * <p>
     * 检索实现对<b>深分页</b>的代价随偏移线性上升——服务端仍要遍历被跳过的部分。
     * 翻到很深的页时应改用按上一页末尾位置续查。
     *
     * @param pageable 分页参数
     * @return 本查询，便于链式组合。
     */
    public RsQuery page(Pageable pageable) {
        // Pageable 内部已经算好 offset+limit, 直接转发 limit(int, int) — 不要绕道 page(int, int) 重载:
        // 后者第一个参数是 1-based 页号 (业务用), 不是 offset, 语义不同, 传 offset 进去会让页码错位.
        // getOffset() 是 long, 极端越界 (offset > Integer.MAX_VALUE) 时 (int) 强转回绕成负值,
        // 由 limit(int, int) 内 offset>=0 校验兜底抛 IAE, 此处不重复校验.
        this.limit((int) pageable.getOffset(), pageable.getPageSize());
        // Pageable.getSort() 永不为 null, 无序时返 Sort.unsorted()
        if (pageable.getSort().isSorted()) {
            this.sortBy(pageable.getSort());
        }
        return this;
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
     * <b>⚠ 与 {@link #page(Pageable)} 的 page 起算点不同</b>: {@link Pageable} (含 Spring 的 {@code PageRequest})
     * 内部 page 从 0 开始, 业务侧切勿直接 {@code page(pageable.getPageNumber(), pageable.getPageSize())} —
     * 传入的 0 会抛 IAE. 从 Pageable 转过来请用 {@link #page(Pageable)} 重载.
     * <p>
     * 越界 fail-fast: page 必须 >= 1, size 必须 > 0; offset 超 {@code Integer.MAX_VALUE} 直接抛
     * (RediSearch LIMIT offset 是 int).
     *
     * @param page 见上述说明
     * @param size 见上述说明
     * @return 见上述说明。
     */
    public RsQuery page(int page, int size) {
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
     * 校验: offset &gt;= 0, limit &gt;= 1. <b>禁止 limit=0</b> — 业务侧通过 find/findOne 路径传 0 返回 0 条 doc 没有意义,
     * 没有业务意义。想拿 total 用 {@code rs.count(query)}（框架内部 forceCountOnly 走 LIMIT 0 0 是协议层用法，
     * 不经本 setter).
     *
     * @param offset 偏移量
     * @param limit  数量上限
     */
    public RsQuery limit(int offset, int limit) {
        // RediSearch LIMIT 不接受负 offset，且 limit=0 没有业务意义。
        if (offset < 0) {
            throw new IllegalArgumentException("limit(offset, limit) requires offset >= 0, got offset=" + offset);
        }
        if (limit < 1) {
            throw new IllegalArgumentException("limit(offset, limit) requires limit >= 1, got limit=" + limit
                    + " — 想拿 total 用 rs.count(query), 不要 limit=0");
        }
        this.offset = offset;
        this.limit = limit;
        this.limitExplicit = true;
        return this;
    }

    // ============ 投影 / 修饰符 ============

    /**
     * 业务作用：设置 RETURN 投影字段 (对象字段名).
     * <p>
     * <b>注意</b>: 此重载不做 fail-fast 校验, 字段名错误会延迟到 {@link #renderArgs} (即 template.find) 时
     * 由 {@link io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta} 抛 {@code RediSearchException}, 调试不友好.
     * <p>
     * 调试 / 开发期请优先使用 {@link #fields(EntityMeta, String...)} 重载, 在 set 时即校验字段名.
     *
     * @param javaFieldNames 见上述说明
     * @return 见上述说明。
     */
    public RsQuery fields(String... javaFieldNames) {
        this.returnFields = javaFieldNames;
        return this;
    }

    /**
     * 业务作用：设置 RETURN 投影字段, 同时立即校验每个字段名是否存在于 meta, 错误立即抛 {@link IllegalArgumentException},
     * 不延迟到查询时, 调试友好.
     *
     * @param meta           见上述说明
     * @param javaFieldNames 见上述说明
     * @return 见上述说明。
     */
    public RsQuery fields(EntityMeta meta, String... javaFieldNames) {
        // meta 非空契约, 调用本重载等同于"我要 fail-fast 校验" — null 直接抛, 而不是 NPE
        if (meta == null) throw new IllegalArgumentException("meta required for fail-fast field check");
        if (javaFieldNames != null) {
            for (String jn : javaFieldNames) {
                if (meta.findFieldByJavaName(jn) == null) {
                    throw new IllegalArgumentException(
                            "Unknown field: " + jn + " on " + meta.type().getSimpleName());
                }
            }
        }
        this.returnFields = javaFieldNames;
        return this;
    }

    /**
     * 业务作用：Lambda getter 重载: {@code fields(Order::getUid, Order::getPrice)}.
     * 与 {@link Criteria#where(SerFunction)} / {@link #sortByDesc(SerFunction)} 风格一致.
     * <p>
     * 字段名解析走 {@link FunctionUtils#fieldNamesCached} (CHM 缓存 writeReplace, 热路径反复用同一 getter 零额外开销).
     * <p>
     * <b>注意</b>: 此重载不做 fail-fast 校验 — getter 解析出的字段名理论上一定有效 (JDK 编译器已检查方法存在),
     * 但 {@code getXxx()} 命名对应的 对象字段 {@code xxx} 不一定标了 {@code @NumericField/TagField/TextField} 等索引注解.
     * 字段不在 meta 时延迟到 {@link #renderArgs} 抛, 调试 / 开发期请用 {@link #fields(EntityMeta, SerFunction[])} 重载.
     *
     * @param getters 见上述说明
     * @return 见上述说明。
     */
    @SafeVarargs
    public final <T> RsQuery fields(SerFunction<T, ?>... getters) {
        return this.fields(FunctionUtils.fieldNamesCached(getters));
    }

    /**
     * 业务作用：Lambda getter + meta 校验重载: 字段不存在 / 未索引 立即抛 {@link IllegalArgumentException}, 不延迟到查询时.
     *
     * @param meta    见上述说明
     * @param getters 见上述说明
     * @return 见上述说明。
     */
    @SafeVarargs
    public final <T> RsQuery fields(EntityMeta meta, SerFunction<T, ?>... getters) {
        return this.fields(meta, FunctionUtils.fieldNamesCached(getters));
    }

    /**
     * 业务作用：指定查询语法版本。
     * 不同版本对同一段查询串的解析可能不同，<b>改动它会影响既有查询的行为</b>，
     * 仅在需要用到新版本特性时设置。
     *
     * @param dialect 语法版本号
     * @return 本查询，便于链式组合。
     */
    public RsQuery dialect(int dialect) {
        this.dialectVersion = dialect;
        return this;
    }

    // ============ getter ============

    /**
     * 业务作用：读取排序设置，渲染命令时使用。
     *
     * <p>参数说明: 无。
     *
     * @return 排序设置；未设置时为 null。
     */
    public Sort sort() {
        return sort;
    }

    /**
     * 业务作用：读取分页偏移。
     *
     * <p>参数说明: 无。
     *
     * @return 分页偏移。
     */
    public int offset() {
        return offset;
    }

    /**
     * 业务作用：读取分页条数。
     *
     * <p>参数说明: 无。
     *
     * @return 分页条数。
     */
    public int limit() {
        return limit;
    }

    /**
     * 业务作用：判断分页条数是否由调用方显式设置。
     * <p>
     * 未显式设置时上层会施加一个默认上限——把「没设」与「设成了默认值」区分开，
     * 才能让调用方显式要求更大的条数而不被默认值悄悄截断。
     *
     * <p>参数说明: 无。
     *
     * @return 条数由调用方显式设置返回 true。
     */
    public boolean limitExplicit() {
        return limitExplicit;
    }

    /**
     * 业务作用：读取要返回的字段名。
     * 限定返回字段可显著减少传输量，未限定时返回文档全部字段。
     *
     * <p>参数说明: 无。
     *
     * @return 要返回的字段名；未限定时为 null。
     */
    public String[] returnFields() {
        return returnFields;
    }

    /**
     * 业务作用：读取查询语法版本号。
     *
     * <p>参数说明: 无。
     *
     * @return 语法版本号。
     */
    public int dialectVersion() {
        return dialectVersion;
    }

    // ============ 渲染 ============

    /**
     * 业务作用：渲染为完整 FT.SEARCH 参数（含索引名，不含命令名）.
     * <p>
     * 渲染期间不修改本对象的任何字段, 同一个 RsQuery 可以多次渲染.
     *
     * @param meta 见上述说明
     * @return 见上述说明。
     */
    public String[] toCommandArgs(EntityMeta meta) {
        return renderArgs(meta, this.offset, this.limit, false, false);
    }

    /**
     * 业务作用：内部渲染入口, 允许 RediSearch 在不 mutate 业务方 RsQuery 的前提下临时覆盖 offset/limit/noContent
     * (例如 findOne / findKeys / count / remove 这些方法).
     * <p>
     * forceCountOnly = true 时强制 NOCONTENT + LIMIT 0 0, 仅取 total.
     * <p>
     * <b>内部 API, 业务方不要直接调用</b>: RsQuery 与 RediSearch 不在同一包, 只能 public, 但语义上是 package-private.
     *
     * @param meta           见上述说明
     * @param useOffset      见上述说明
     * @param useLimit       见上述说明
     * @param useNoContent   见上述说明
     * @param forceCountOnly 见上述说明
     * @return 见上述说明。
     */
    public String[] renderArgs(EntityMeta meta, int useOffset, int useLimit, boolean useNoContent, boolean forceCountOnly) {
        // 撮合热路径每次 find/count 都重建一份 args, 用 RecycleLinkedList 节点池化, toArray 后立刻 recycle
        RecycleLinkedList<String> args = RecycleLinkedList.of();
        try {
            args.add(meta.index());
            args.add(buildQueryString(meta));

            if (forceCountOnly) {
                args.add("NOCONTENT");
                args.add("LIMIT");
                args.add("0");
                args.add("0");
                if (dialectVersion > 0) {
                    args.add("DIALECT");
                    args.add(String.valueOf(dialectVersion));
                }
                return args.toArray(new String[0]);
            }

            if (useNoContent) args.add("NOCONTENT");

            // #5: NOCONTENT (findKeys/remove 只要 key 不要字段) 与 RETURN 互斥 — 同时下发 RediSearch 会语法错/返回形态异常。
            // findKeys/remove 复用了带 fields() 的 RsQuery 时, NOCONTENT 语义优先, 跳过 RETURN 投影 (SORTBY/LIMIT 仍保留)。
            if (!useNoContent && returnFields != null && returnFields.length > 0) {
                if (meta.dataType() == DataType.JSON) {
                    // JSON 模式投影会让 RediSearch 返回 alias-value 而非 ["$", json], 后续 readJson 会失败
                    throw new RediSearchException(
                            "RsQuery.fields() not supported in JSON mode (entity=" + meta.type().getSimpleName()
                                    + "). 使用默认完整返回 $ JSON, 业务侧再做字段裁剪.");
                }
                args.add("RETURN");
                args.add(String.valueOf(returnFields.length));
                for (String jn : returnFields) {
                    FieldMeta fm = meta.fieldByJavaName(jn);
                    args.add(fm.redisName());
                }
            }

            if (sort != null && sort.isSorted()) {
                // RediSearch FT.SEARCH 的 SORTBY 只支持单字段, 多字段直接拒绝, 避免静默丢弃后续字段
                int sortFields = 0;
                for (Sort.Order ignored : sort) sortFields++;
                if (sortFields > 1) {
                    throw new RediSearchException("FT.SEARCH SORTBY only supports a single field, got " + sortFields
                            + " (RediSearch 限制; 多字段排序请改用 FT.AGGREGATE)");
                }
                Sort.Order order = sort.iterator().next();
                FieldMeta fm = meta.fieldByJavaName(order.getProperty());
                args.add("SORTBY");
                args.add(fm.redisName());
                args.add(order.isDescending() ? "DESC" : "ASC");
            }

            args.add("LIMIT");
            args.add(String.valueOf(useOffset));
            args.add(String.valueOf(useLimit));

            if (dialectVersion > 0) {
                args.add("DIALECT");
                args.add(String.valueOf(dialectVersion));
            }

            return args.toArray(new String[0]);
        } finally {
            args.recycle();
        }
    }

    /**
     * 业务作用：把条件渲染成完整的查询串。
     * 无条件时渲染成匹配全部文档的形式，使调用方对「有条件」与「无条件」的处理路径一致。
     *
     * @param meta 实体结构元信息
     * @return 查询串。
     */
    private String buildQueryString(EntityMeta meta) {
        if (criterias.isEmpty()) return "*";
        StringBuilder sb = new StringBuilder(64);
        boolean first = true;
        for (Criteria c : criterias) {
            if (!first) sb.append(' ');
            c.appendTo(sb, meta);
            first = false;
        }
        return sb.toString();
    }

    /**
     * 业务作用：渲染为 JSONPath filter 表达式 (含外围 {@code $[?(...)]}), 供 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY} 模式
     * JSON.GET / JSON.DEL 用. 多 criteria 之间 AND 拼接.
     * <p>
     * 0 个 criteria → 返回 {@code $[*]} (匹配所有子文档).
     * <p>
     * 若 query 设置了 sort 或 limit (limitExplicit), 直接抛 {@link UnsupportedOperationException}:
     * JSON_ARRAY 子文档级排序分页 RediSearch 不支持.
     *
     * @param meta 见上述说明
     */
    public String toJsonPathFilter(EntityMeta meta) {
        if (sort != null && sort.isSorted()) {
            throw new UnsupportedOperationException(
                    "JSON_ARRAY mode does not support sort (子文档级 sort RediSearch 不支持; "
                            + "需要排序请用 DataType.JSON 单文档模式)");
        }
        if (limitExplicit) {
            throw new UnsupportedOperationException(
                    "JSON_ARRAY mode does not support pagination (子文档级 limit 不支持; "
                            + "需要分页请用 DataType.JSON 单文档模式或业务层补救)");
        }
        if (criterias.isEmpty()) return "$[*]";
        // 全是 ALL (RsQuery.empty() 内含 Criteria.all()) 等价于 $[*]
        boolean allMatchAll = true;
        for (Criteria c : criterias) {
            if (!c.isAll()) {
                allMatchAll = false;
                break;
            }
        }
        if (allMatchAll) return "$[*]";
        StringBuilder sb = new StringBuilder(64);
        sb.append("$[?(");
        boolean first = true;
        for (Criteria c : criterias) {
            // 跳过 ALL: 跟其他 criteria AND 时不应该影响 (例 where(all).and(where("x").is("y")) 等价于纯 x==y)
            if (c.isAll()) continue;
            if (!first) sb.append(" && ");
            c.appendJsonPath(sb, meta);
            first = false;
        }
        sb.append(")]");
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
    public ObjectPool.PooledHandle<RsQuery> handle() {
        return this.handle;
    }

    /**
     * 业务作用：归还前清空条件、排序、分页与返回字段设置。
     * 条件本身是池化对象，在此一并归还。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void restore() {
        // 级联回收所有 Criteria, 然后 clear list (Node 归节点池, list 实例跟 RsQuery 同生命周期不归 list 池)
        for (Criteria c : criterias) c.recycle();
        criterias.clear();
        sort = null;
        offset = 0;
        limit = 10;
        limitExplicit = false;
        returnFields = null;
        dialectVersion = 2;
    }
}
