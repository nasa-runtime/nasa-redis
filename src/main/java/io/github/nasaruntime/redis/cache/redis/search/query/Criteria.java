package io.github.nasaruntime.redis.cache.redis.search.query;

import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta;
import io.github.nasaruntime.redis.cache.redis.search.meta.FieldMeta;
import io.github.nasaruntime.redis.cache.redis.search.meta.FieldType;
import io.github.nasaruntime.redis.cache.redis.search.meta.JsonValueAccessor;
import io.github.nasaruntime.redis.cache.redis.search.meta.MetaResolver;
import io.github.nasaruntime.core.function.FunctionUtils;
import io.github.nasaruntime.core.function.SerFunction;
import io.github.nasaruntime.core.utils.ContextUtils;


/**
 * Nasa
 * 单字段查询条件 DSL，对标 Spring Data 的 Criteria
 * <p>
 * 用法（在 {@link RsQuery} 内组装为 AND）：
 * <pre>
 * RsQuery.query()
 *        .where(Criteria.where("userId").is("u_001"))
 *        .and(Criteria.where("price").between(60000, 70000))
 *        .and(Criteria.where("status").in("OPEN", "FILLED"));
 * </pre>
 *
 * <h2>⚠ 池化语义 — 跟 RsQuery 一致, 必读</h2>
 * <ul>
 *   <li><b>从 POOL 取</b>: {@link #where} 等工厂方法从 {@link #POOL} 借实例.</li>
 *   <li><b>所有权一次性转移给 RsQuery</b>: Criteria 一旦传给 {@code RsQuery.where/and}, 业务侧立即失去
 *       所有权, <b>不能</b>再持有 Criteria 局部变量 / 不能再修改 / 不能手动 recycle.</li>
 *   <li><b>框架级联回收</b>: 业务调 rs.find / aggregate 等提交 query 后, RsQuery.recycle() 触发,
 *       内部所有 Criteria 自动连锁归池. 业务无需也<b>不应</b>手工干预 Criteria 的生命周期.</li>
 *   <li><b>禁止跨 query 复用</b>: 同一 Criteria 实例只能放进一个 RsQuery; 想跑相似查询请每次
 *       {@code Criteria.where(...).is(...)} 重新构造.</li>
 * </ul>
 */
@SuppressWarnings("unused")
public final class Criteria implements ObjectPool.Recycler<Criteria> {

    static final ObjectPool<Criteria> POOL = new ObjectPool<>(
            ContextUtils.getPropertyInt("nasa.object-pool.rs-criteria-capacity", 500)) {
        /**
         * 业务作用：池空时新建一个实例，由对象池在借不到空闲实例时调用。
         *
         * <p>参数说明: 无。
         *
         * @return 新建的实例。
         */
        @Override
        public Criteria newObject() {
            return new Criteria();
        }
    };

    private final ObjectPool.PooledHandle<Criteria> handle = new ObjectPool.PooledHandle<>(POOL);

    /**
     * 目标字段的 对象字段名 (例 {@code "userId"}). 渲染时通过 {@link io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta#fieldByJVMName} 反查
     * RediSearch alias ({@code redisName}) 与 JSONPath. 全局 op ({@link OpType#ALL} / {@link OpType#TEXT_GLOBAL})
     * 不针对单一字段, 此字段保持 null.
     */
    private String fieldName;

    /**
     * 操作类型. null 表示业务调了 {@link #where(String)} 后忘了接 {@code .is / .in / .gt / .matches} 等 op,
     * 渲染时 fail-fast 抛错而不是静默生成空查询.
     */
    private OpType opType;

    /**
     * 通用值槽位 (类型擦除式存值, 由 {@link #opType} 解释含义). 各 op 的槽位约定:
     * <ul>
     *   <li>{@code EQ / GT / GTE / LT / LTE}: {@code a = value}</li>
     *   <li>{@code BETWEEN}: {@code a = from, b = to}</li>
     *   <li>{@code MATCH / PHRASE / PREFIX / SUFFIX / FUZZY}: {@code a = text} (单字段文本)</li>
     *   <li>{@code TEXT_GLOBAL}: {@code a = text} (全索引文本, 不绑定字段)</li>
     *   <li>{@code GEO} ({@link #geoWithin}): {@code a = lng, b = lat, c = radius, d = }{@link GeoUnit}</li>
     *   <li>{@code IN}: 不占 a/b/c/d, 值走独立的 {@link #inValues} 数组 (NIN 复用 IN 路径 + {@link #negated})</li>
     *   <li>{@code ALL}: 不占任何槽位 (match-all 占位)</li>
     * </ul>
     * 为什么用 4 个 Object 通用槽位而非按 op 拆 specific field: Criteria 实例池化复用, 4 个引用槽 (32 bytes)
     * 比给每种 op 拆字段 (BigDecimal / String / Number / GeoUnit 等并存) 更省内存; 槽位含义靠 opType 解释,
     * {@link #restore} 时统一 4 个槽 set null 即可, 不必关心当前是哪种 op.
     */
    private Object a, b, c, d;

    /**
     * {@link OpType#IN} 操作专用值数组. 其它 op 用 {@link #a a / b / c / d} 槽位.
     * <p>
     * NIN 复用 IN 路径 + {@link #negated} 取反, 不引入独立 OpType.
     * <p>
     * 不做防御性 clone: 业务调 {@code Criteria.where("x").in(values)} 后若 mutate {@code values} 数组
     * 会污染本 Criteria 的渲染输出 (违反 Criteria 一次性语义).
     */
    private Object[] inValues;

    /**
     * 取反标志. true 时:
     * <ul>
     *   <li>{@link #appendTo} 输出 FT.SEARCH 片段前置 {@code -} (例 {@code -@status:{OPEN}})</li>
     *   <li>{@link #appendJsonPath} 输出 JSONPath 条件外包 {@code !(...)}</li>
     * </ul>
     * 业务通过 {@link #negate()} 或 {@code .not(value)} / {@code .nin(values)} 设置.
     */
    private boolean negated;

    /**
     * 业务作用：私有化构造，强制经由工厂方法从池中取用，避免绕过池直接新建而使池失去意义。
     *
     * <p>参数说明: 无。
     */
    private Criteria() {}

    // ============ 工厂 ============

    /**
     * 业务作用：从一个实体字段开始构造查询条件，是构造条件的唯一入口。
     * <p>
     * 传入的是 <b>Java 字段名</b>而非索引中的字段名：两者的映射由实体元信息负责，
     * 调用方因此不必关心索引里的实际命名，重命名索引字段也不影响业务代码。
     *
     * @param javaFieldName 实体的 Java 字段名
     * @return 新建的条件，待补充比较算子。
     */
    public static Criteria where(String javaFieldName) {
        Criteria c = POOL.get();
        c.fieldName = javaFieldName;
        return c;
    }

    /**
     * 业务作用：lambda 字段引用版 {@code where}, 类似 MyBatis-Plus: {@code Criteria.where(User::getName).is("aa")}。
     * <p>
     * 只解析出 对象字段名 (经 {@link FunctionUtils#fieldNameCached} 缓存 writeReplace 解析), 后续映射 RediSearch
     * alias / JSONPath、@JsonProperty rename、schema-only、JSON_ARRAY filter 等行为与字符串版 {@link #where(String)} 完全一致。
     * <p>
     * <b>仅支持标准 getter / is 方法引用</b> ({@code User::getName} → name, {@code User::isActive} → active);
     * 普通 lambda ({@code u -> u.getName()})、复杂表达式、{@code User::toString} 等不支持 ——
     * 其 implMethodName 非 get/is 前缀, 在 {@code ReflectUtils.fieldName} 处 fail-fast。
     *
     * @param getter 实体的 get/is 方法引用
     * @return 见上述说明。
     */
    public static <T, R> Criteria where(SerFunction<T, R> getter) {
        if (getter == null) {
            throw new IllegalArgumentException("Criteria.where getter cannot be null");
        }
        return where(FunctionUtils.fieldNameCached(getter));
    }

    /**
     * 业务作用：全索引全文匹配（不限定字段）
     *
     * @param text 见上述说明
     * @return 见上述说明。
     */
    public static Criteria match(String text) {
        // 与实例 TEXT API（requireText）一致，null 会被渲染为字面值 "null" 并产生静默误匹配，因此必须拒绝。
        if (text == null) {
            throw new IllegalArgumentException("Criteria.match text cannot be null");
        }
        Criteria c = POOL.get();
        c.opType = OpType.TEXT_GLOBAL;
        c.a = text;
        return c;
    }

    /**
     * 业务作用：匹配所有 {@code *}
     *
     * @return 见上述说明。
     */
    public static Criteria all() {
        Criteria c = POOL.get();
        c.opType = OpType.ALL;
        return c;
    }

    // ============ 通用 ============

    /**
     * 业务作用：要求字段等于给定值。
     * 按字段的索引类型自动选择匹配方式——标签型走精确匹配，数值型走等值区间，
     * 因此调用方无需为不同类型改写条件。
     *
     * @param value 目标值
     * @return 本实例，便于链式组合。
     */
    public Criteria is(Object value) {
        // TAG/TEXT 不接受 null，否则会渲染成字面字符串 "null" 并静默误匹配；查询该字符串时必须显式传 "null"。
        if (value == null) {
            throw new IllegalArgumentException("Criteria value cannot be null (field=" + fieldName
                    + "); to match the literal string \"null\" pass it explicitly");
        }
        this.opType = OpType.EQ;
        this.a = value;
        return this;
    }

    /**
     * 业务作用：要求字段不等于给定值。
     *
     * @param value 要排除的值
     * @return 本实例，便于链式组合。
     */
    public Criteria not(Object value) {
        this.negated = !this.negated;
        return is(value);
    }

    /**
     * 业务作用：要求字段取值落在给定集合内。
     * 展开成多个取值的或组合；<b>集合过大时会拼出很长的查询串</b>，检索实现对查询串长度有上限，
     * 大集合应改为分批查询。
     *
     * @param values 候选取值
     * @return 本实例，便于链式组合。
     */
    public Criteria in(Object... values) {
        // in 要求集合非 null、非空且元素非 null，避免生成 @f:{}、空条件组或字面值 "null"。
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("Criteria.in requires at least one value (field=" + fieldName + ")");
        }
        for (Object v : values) {
            if (v == null) {
                throw new IllegalArgumentException("Criteria.in values must not contain null (field=" + fieldName + ")");
            }
        }
        this.opType = OpType.IN;
        this.inValues = values;
        return this;
    }

    /**
     * 业务作用：要求字段取值不在给定集合内。
     *
     * @param values 要排除的取值
     * @return 本实例，便于链式组合。
     */
    public Criteria nin(Object... values) {
        this.negated = !this.negated;
        return in(values);
    }

    // ============ NUMERIC ============

    /**
     * 业务作用：要求数值字段大于给定值（不含边界）。
     *
     * @param n 下界
     * @return 本实例，便于链式组合。
     */
    public Criteria gt(Number n) {
        requireNumber(n);
        this.opType = OpType.GT;
        this.a = n;
        return this;
    }

    /**
     * 业务作用：要求数值字段大于等于给定值（含边界）。
     *
     * @param n 下界
     * @return 本实例，便于链式组合。
     */
    public Criteria gte(Number n) {
        requireNumber(n);
        this.opType = OpType.GTE;
        this.a = n;
        return this;
    }

    /**
     * 业务作用：要求数值字段小于给定值（不含边界）。
     *
     * @param n 上界
     * @return 本实例，便于链式组合。
     */
    public Criteria lt(Number n) {
        requireNumber(n);
        this.opType = OpType.LT;
        this.a = n;
        return this;
    }

    /**
     * 业务作用：要求数值字段小于等于给定值（含边界）。
     *
     * @param n 上界
     * @return 本实例，便于链式组合。
     */
    public Criteria lte(Number n) {
        requireNumber(n);
        this.opType = OpType.LTE;
        this.a = n;
        return this;
    }

    // 数值条件不接受 null，NaN/Infinity 由 numStr 在渲染时拒绝。

    /**
     * 业务作用：拦住数值算子上的空值。
     * 空值在数值区间里没有对应表达，放行会拼出一个语法非法的查询串，
     * 届时报错发生在服务端且消息晦涩；在此拒绝可直接指出是哪个调用点的问题。
     *
     * @param n 待校验的数值
     * 返回: 无返回值；为 null 时抛出异常。
     * @throws IllegalArgumentException 数值为 null
     */
    private void requireNumber(Number n) {
        if (n == null) {
            throw new IllegalArgumentException("Numeric criteria value cannot be null (field=" + fieldName + ")");
        }
    }

    /**
     * 业务作用：要求数值字段落在闭区间内。
     *
     * @param from 区间下界（含）
     * @param to   区间上界（含）
     * @return 本实例，便于链式组合。
     */
    public Criteria between(Number from, Number to) {
        requireNumber(from);
        requireNumber(to);
        this.opType = OpType.BETWEEN;
        this.a = from;
        this.b = to;
        return this;
    }

    // ============ TEXT ============

    /**
     * 业务作用：要求文本字段命中给定内容的<b>分词匹配</b>，而非整串相等。
     * 分词由索引建立时的规则决定，因此同一段文本在不同索引配置下的命中结果可能不同。
     *
     * @param text 待匹配的文本
     * @return 本实例，便于链式组合。
     */
    public Criteria matches(String text) {
        requireText(text);
        this.opType = OpType.MATCH;
        this.a = text;
        return this;
    }

    /**
     * 业务作用：短语匹配, 渲染为 "..." 形式, 内部分词按顺序连续命中
     *
     * @param text 见上述说明
     * @return 见上述说明。
     */
    public Criteria phrase(String text) {
        requireText(text);
        this.opType = OpType.PHRASE;
        this.a = text;
        return this;
    }

    /**
     * 业务作用：要求文本字段以给定内容开头。
     * 前缀匹配可利用索引，代价随前缀变短而上升——<b>极短的前缀会命中大量词项</b>，应避免。
     *
     * @param text 前缀文本
     * @return 本实例，便于链式组合。
     */
    public Criteria prefix(String text) {
        requireText(text);
        this.opType = OpType.PREFIX;
        this.a = text;
        return this;
    }

    /**
     * 业务作用：要求文本字段以给定内容结尾。
     * 后缀匹配无法利用前缀索引，代价明显高于前缀匹配，不宜用在高频查询上。
     *
     * @param text 后缀文本
     * @return 本实例，便于链式组合。
     */
    public Criteria suffix(String text) {
        requireText(text);
        this.opType = OpType.SUFFIX;
        this.a = text;
        return this;
    }

    /**
     * 业务作用：要求文本字段模糊命中给定内容，容忍少量字符差异，用于拼写容错。
     * 模糊匹配会显著扩大候选词项，代价远高于精确与前缀匹配。
     *
     * @param text 待匹配的文本
     * @return 本实例，便于链式组合。
     */
    public Criteria fuzzy(String text) {
        requireText(text);
        this.opType = OpType.FUZZY;
        this.a = text;
        return this;
    }

    // TEXT 条件不接受 null，否则 appendText 会渲染成字面值 "null" 并静默误匹配。

    /**
     * 业务作用：拦住文本算子上的空白内容。
     * 空白文本会拼出一个匹配全部文档的条件，使本意为过滤的查询变成全表扫描——
     * 这类错误在数据量小时毫无征兆，上量后才表现为查询突然变慢。
     *
     * @param text 待校验的文本
     * 返回: 无返回值；为空白时抛出异常。
     * @throws IllegalArgumentException 文本为空或纯空白
     */
    private void requireText(String text) {
        if (text == null) {
            throw new IllegalArgumentException("Text criteria value cannot be null (field=" + fieldName + ")");
        }
    }

    // ============ GEO ============

    /**
     * 业务作用：要求地理字段落在以给定坐标为圆心的半径范围内。
     *
     * @param lng    圆心经度
     * @param lat    圆心纬度
     * @param radius 半径
     * @param unit   半径单位
     * @return 本实例，便于链式组合。
     */
    public Criteria geoWithin(double lng, double lat, double radius, GeoUnit unit) {
        // RediSearch GEO 不接受 NaN/Infinity，必须在渲染前拒绝，避免生成非法 query。
        if (!Double.isFinite(lng) || !Double.isFinite(lat) || !Double.isFinite(radius)) {
            throw new IllegalArgumentException("GEO lng/lat/radius must be finite (field=" + fieldName
                    + "), got lng=" + lng + " lat=" + lat + " radius=" + radius);
        }
        if (unit == null) {
            throw new IllegalArgumentException("GEO unit cannot be null (field=" + fieldName + ")");
        }
        this.opType = OpType.GEO;
        this.a = lng;
        this.b = lat;
        this.c = radius;
        this.d = unit;
        return this;
    }

    // ============ 逻辑组合 ============

    /**
     * 业务作用：对本条件整体取反。
     * 作用于<b>整个条件</b>而非其中某一项，因此对组合条件取反得到的是「不同时满足」而非「逐项都不满足」。
     *
     * <p>参数说明: 无。
     *
     * @return 本实例，便于链式组合。
     */
    public Criteria negate() {
        this.negated = !this.negated;
        return this;
    }

    /**
     * 业务作用：本类不承担条件间的「与」组合，调用即拒绝，把组合位置固定在查询对象上。
     * 条件之间的与合由 {@link RsQuery#and} 完成；两处都能组合会让同一个条件对象出现在两棵组合树里，
     * 而条件实例来自对象池，重复挂载会在归还后被另一处继续引用。
     *
     * @param other 另一个条件；调用方通常刚从池里借出，本方法在抛出前先替其归还，避免池实例泄漏
     * @return 不返回；本方法总是抛出。
     * @throws RediSearchException 总是抛出，提示改用 {@link RsQuery#and}
     */
    public Criteria and(Criteria other) {
        // other 已从 POOL 借出, 若直接抛会泄漏池实例; 先回收再抛
        if (other != null) {
            try {
                other.recycle();
            } catch (RuntimeException ignored) { /* swallow recycle 异常, 优先暴露原因 */ }
        }
        throw new RediSearchException("Criteria.and 不提供条件组合, 请改用 RsQuery.and(Criteria)");
    }

    /**
     * 业务作用：本类不提供条件间的「或」组合，调用即拒绝。
     * <p>
     * 查询侧只支持条件间的与合（{@link RsQuery#and}）。单字段的多值取或用 {@link #in(Object...)}；
     * 跨字段的或需要自行拼 FT.SEARCH 查询串，本 DSL 不生成。
     *
     * @param other 另一个条件；调用方通常刚从池里借出，本方法在抛出前先替其归还，避免池实例泄漏
     * @return 不返回；本方法总是抛出。
     * @throws RediSearchException 总是抛出，提示单字段用 in、跨字段需自行拼查询串
     */
    public Criteria or(Criteria other) {
        if (other != null) {
            try {
                other.recycle();
            } catch (RuntimeException ignored) { /* swallow recycle 异常, 优先暴露原因 */ }
        }
        throw new RediSearchException("Criteria.or 不提供条件组合: 单字段多值取或用 Criteria.in(...), 跨字段的或需自行拼 FT.SEARCH 查询串");
    }

    // ============ 渲染 ============

    /**
     * 业务作用：把本条件渲染成查询串片段，供调试与拼接使用。
     *
     * @param meta 实体结构元信息，用于把 Java 字段名映射成索引字段名
     * @return 查询串片段。
     */
    public String toFragment(EntityMeta meta) {
        StringBuilder sb = new StringBuilder(32);
        appendTo(sb, meta);
        return sb.toString();
    }

    /**
     * 业务作用：把本条件追加进查询串缓冲，按字段的索引类型分派到对应的渲染方式。
     * 直接写入缓冲而非返回字符串，避免组合条件时产生大量中间字符串。
     *
     * @param sb   查询串缓冲
     * @param meta 实体结构元信息
     * 返回: 无返回值。
     */
    void appendTo(StringBuilder sb, EntityMeta meta) {
        if (opType == null) {
            throw new RediSearchException("Criteria on field '" + fieldName
                    + "' has no operator (调用 where(...) 后必须接 .is/.in/.gt/.matches 等)");
        }
        if (negated) sb.append('-');
        if (opType == OpType.ALL) {
            sb.append('*');
            return;
        }
        if (opType == OpType.TEXT_GLOBAL) {
            sb.append(escapeText(String.valueOf(a)));
            return;
        }
        FieldMeta fm = meta.fieldByJavaName(fieldName);
        // NUMERIC IN/NIN 特例: RediSearch NUMERIC 不支持 {a|b}, 必须分组 OR, 每分支独立带 @field 前缀 →
        // (@f:[1 1]|@f:[2 2]); NIN 由上面已 append 的 '-' 整组取反 → -(...). 故在写 @field: 前缀前特判。
        // 高频场景: @TagField enum + 数字型 @JsonValue 被自动转 NUMERIC schema 后, 业务仍 .in(enum...) 查询。
        if (fm.type() == FieldType.NUMERIC && opType == OpType.IN) {
            appendNumericIn(sb, fm);
            return;
        }
        sb.append('@').append(fm.redisName()).append(':');
        switch (fm.type()) {
            case TAG -> appendTag(sb);
            case NUMERIC -> appendNumeric(sb);
            case TEXT -> appendText(sb);
            case GEO -> appendGeo(sb);
            case ID -> throw new RediSearchException("Cannot query on ID field");
        }
    }

    /**
     * 业务作用：NUMERIC 多值 IN/NIN 渲染: {@code (@f:[v v]|@f:[v v]...)}。NIN 复用 IN + 外层已 append 的 {@code '-'} 取反整组。
     * 每个 OR 分支必须带 {@code @field:} 前缀 (NUMERIC range 不能像 TAG 那样 {@code {a|b}} 共享前缀)。
     *
     * @param sb 见上述说明
     * @param fm 见上述说明
     */
    private void appendNumericIn(StringBuilder sb, FieldMeta fm) {
        sb.append('(');
        for (int i = 0; i < inValues.length; i++) {
            if (i > 0) sb.append('|');
            String v = numStr(inValues[i]);
            sb.append('@').append(fm.redisName()).append(":[").append(v).append(' ').append(v).append(']');
        }
        sb.append(')');
    }

    /**
     * 业务作用：按标签型字段的语法渲染条件。
     * 标签型走精确匹配，取值中的特殊字符必须转义，否则会被当作语法记号而改变条件含义。
     *
     * @param sb 查询串缓冲
     * 返回: 无返回值。
     */
    private void appendTag(StringBuilder sb) {
        sb.append('{');
        if (opType == OpType.EQ) {
            // enum 与持久化/key 统一渲染 (@JsonValue 或 name), 不再用 String.valueOf 以免 JSON 模式 @JsonValue 查不到
            sb.append(escapeTag(MetaResolver.renderValue(a)));
        } else if (opType == OpType.IN) {
            for (int i = 0; i < inValues.length; i++) {
                if (i > 0) sb.append('|');
                sb.append(escapeTag(MetaResolver.renderValue(inValues[i])));
            }
        } else {
            throw new RediSearchException("TAG supports is/in only, got " + opType);
        }
        sb.append('}');
    }

    /**
     * 业务作用：按数值型字段的语法渲染区间条件，含开闭区间的边界表示。
     *
     * @param sb 查询串缓冲
     * 返回: 无返回值。
     */
    private void appendNumeric(StringBuilder sb) {
        switch (opType) {
            case EQ -> {
                String va = numStr(a);
                sb.append('[').append(va).append(' ').append(va).append(']');
            }
            case GT -> sb.append("[(").append(numStr(a)).append(" +inf]");
            case GTE -> sb.append('[').append(numStr(a)).append(" +inf]");
            case LT -> sb.append("[-inf (").append(numStr(a)).append(']');
            case LTE -> sb.append("[-inf ").append(numStr(a)).append(']');
            case BETWEEN -> sb.append('[').append(numStr(a)).append(' ').append(numStr(b)).append(']');
            default -> throw new RediSearchException("NUMERIC unsupported op: " + opType);
        }
    }

    /**
     * 业务作用：NUMERIC 区间值校验 + enum @JsonValue 序列化兜底:
     * <ul>
     *   <li>{@link Number}: 拒绝 NaN/Infinity (RediSearch 不接受), 否则透传 {@code toString}</li>
     *   <li>{@link String}: 仅放行 {@code +inf}/{@code -inf} 哨兵, 其它字符串拒绝</li>
     *   <li>{@link Enum}: 调 {@link io.github.nasaruntime.redis.cache.redis.search.meta.MetaResolver#numericJsonValueAccessor} 取 enum 上数字型 {@code @JsonValue}
     *       (方法或字段, 典型 {@code SerialEnum.serial()} 返数字) 的值。<b>必须有数字型 @JsonValue</b> ——
     *       没有是配置错误 (parseFieldAnnotation 处理 @NumericField/@TagField-auto 时已启动期 fail-fast),
     *       此处 acc==null 直接抛 {@code RediSearchException}；禁止回退到 {@link Enum#ordinal()}，否则查询值会与持久值分裂。
     *       对应 schema 推导端把 "enum + 数字型 @JsonValue + @TagField" 自动转 NUMERIC: 索引存数字, 查询也按数字 range 写,
     *       业务侧仍能用 {@code .is(Direction.BUY)} 自然语法, 框架内部按 @JsonValue 取数字。</li>
     * </ul>
     *
     * @param v 见上述说明
     * @return 见上述说明。
     */
    private static String numStr(Object v) {
        if (v instanceof Enum<?> e) {
            // 走 getDeclaringClass 而非 v.getClass(): anonymous enum constant (例 BUY { @Override... }) 时
            // v.getClass() 是匿名子类 isEnum() == false → 拿不到 @JsonValue, 跟 prefix 占位符 / Jackson 序列化脱节
            JsonValueAccessor acc = MetaResolver.numericJsonValueAccessor(e.getDeclaringClass());
            // NUMERIC enum 字段启动期已要求有数字型 @JsonValue (parseFieldAnnotation fail-fast), 此处不该为 null。
            // 缺少数字型 @JsonValue 表示配置合同不成立；回退到 ordinal 会查询错误持久值，必须直接拒绝。
            if (acc == null) {
                throw new RediSearchException("NUMERIC query on enum " + e.getDeclaringClass().getName()
                        + " requires a numeric @JsonValue (启动期应已 fail-fast)");
            }
            Object r = acc.get(e);
            if (r instanceof Number n) return n.toString();
            throw new RediSearchException("numeric @JsonValue on " + e.getDeclaringClass().getName()
                    + " returned non-number: " + r);
        }
        if (v instanceof Number n) {
            double d = n.doubleValue();
            if (!Double.isFinite(d)) {
                throw new IllegalArgumentException("NUMERIC range expects finite number, got " + v);
            }
            return n.toString();
        }
        if (v instanceof String s) {
            if ("+inf".equals(s) || "-inf".equals(s)) return s;
            throw new IllegalArgumentException("NUMERIC range expects finite number or +inf/-inf, got '" + s + "'");
        }
        throw new IllegalArgumentException("NUMERIC range expects Number, got " + (v == null ? "null" : v.getClass().getName()));
    }

    /**
     * 业务作用：按文本型字段的语法渲染分词、前缀、后缀与模糊匹配。
     *
     * @param sb 查询串缓冲
     * 返回: 无返回值。
     */
    private void appendText(StringBuilder sb) {
        switch (opType) {
            case MATCH -> sb.append(escapeText(String.valueOf(a)));
            case PHRASE -> sb.append('"').append(escapePhrase(String.valueOf(a))).append('"');
            case PREFIX -> sb.append(escapeText(String.valueOf(a))).append('*');
            case SUFFIX -> sb.append('*').append(escapeText(String.valueOf(a)));
            case FUZZY -> sb.append('%').append(escapeText(String.valueOf(a))).append('%');
            default -> throw new RediSearchException("TEXT unsupported op: " + opType);
        }
    }

    /**
     * 业务作用：按地理型字段的语法渲染半径范围条件。
     *
     * @param sb 查询串缓冲
     * 返回: 无返回值。
     */
    private void appendGeo(StringBuilder sb) {
        if (opType != OpType.GEO) throw new RediSearchException("GEO supports geoWithin only");
        sb.append('[').append(a).append(' ').append(b).append(' ').append(c).append(' ')
                .append(((GeoUnit) d).name().toLowerCase()).append(']');
    }

    /**
     * 业务作用：白名单转义: 非 [a-zA-Z0-9_] 与非中日韩统一表意文字 (U+4E00..U+9FA5) 的字符一律前置 backslash.
     * <p>
     * 用白名单而非黑名单, 防止漏掉 RediSearch 保留字符 (例如 {@code < > [ ] ( ) ! @ # $ % ^ &amp; * + = ~ / ? { } | : ; - . , " \ space}).
     * TAG / TEXT / 全文匹配统一用此方法, 杜绝注入风险.
     *
     * @param s 见上述说明
     * @return 见上述说明。
     */
    private static String escapeQueryChars(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '_' ||
                    (c >= 0x4e00 && c <= 0x9fa5)) {
                sb.append(c);
            } else {
                sb.append('\\').append(c);
            }
        }
        return sb.toString();
    }

    /**
     * 业务作用：PHRASE 渲染时已被双引号包裹, 只需 escape 反斜杠和引号本身; 其它字符在 phrase 语义内不会被 RediSearch 当成操作符.
     *
     * @param s 见上述说明
     * @return 见上述说明。
     */
    private static String escapePhrase(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '"') sb.append('\\');
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * 业务作用：转义标签取值中的语法记号。
     * 不转义会让取值里的连字符、空格等被解析成语法结构，<b>条件含义随之改变且不报错</b>，
     * 表现为查询结果莫名其妙地多或少。
     *
     * @param s 原始取值
     * @return 转义后的取值。
     */
    private static String escapeTag(String s) {
        return escapeQueryChars(s);
    }

    /**
     * 业务作用：转义文本取值中的语法记号，理由同标签取值的转义。
     *
     * @param s 原始取值
     * @return 转义后的取值。
     */
    private static String escapeText(String s) {
        return escapeQueryChars(s);
    }

    /**
     * 业务作用：是否是 {@link OpType#ALL} (match-all 占位); RsQuery 在 ARRAY 模式渲染时遇到全 ALL 直接返回 {@code $[*]}.
     */
    public boolean isAll() {
        return opType == OpType.ALL;
    }

    // ============ JSONPath 渲染 (JSON_ARRAY 模式用) ============

    /**
     * 业务作用：渲染为 JSONPath filter 内的条件片段, 不含外围 {@code [?(...)]}.
     * 例: 字段 uid eq 123 → {@code @.uid==123}; price between 60000 70000 → {@code @.price>=60000 &amp;&amp; @.price<=70000}.
     * <p>
     * <b>支持的 op</b>: EQ / IN / GT / GTE / LT / LTE / BETWEEN.
     * <b>暂不支持</b>: TEXT 类 (MATCH/PHRASE/PREFIX/SUFFIX/FUZZY) / GEO / TEXT_GLOBAL / ALL,
     * 调用抛 {@link UnsupportedOperationException} 提示需要时再扩展.
     * <p>
     * negated 通过外层 JSONPath 表达式取反 ({@code !(...)}) 实现, 这里只渲染正向片段.
     *
     * @param sb   见上述说明
     * @param meta 见上述说明
     */
    public void appendJsonPath(StringBuilder sb, EntityMeta meta) {
        if (opType == null) {
            throw new RediSearchException("Criteria on field '" + fieldName + "' has no operator");
        }
        // ALL / TEXT_GLOBAL 没有字段维度, 不能翻译成 JSONPath
        if (opType == OpType.ALL || opType == OpType.TEXT_GLOBAL) {
            throw new UnsupportedOperationException("JSON_ARRAY filter does not support op " + opType
                    + " (ALL/TEXT_GLOBAL 没有字段维度, 无法翻译 JSONPath)");
        }
        FieldMeta fm = meta.fieldByJavaName(fieldName);
        // JSONPath 用 @.fieldName 引用当前元素的字段, 这里用 redisName (= alias) 保持和 FT.SEARCH 一致
        String fname = "@." + fm.redisName();
        boolean group = negated || opType == OpType.BETWEEN || opType == OpType.IN;
        if (negated) sb.append("!(");
        else if (group) sb.append('(');
        switch (opType) {
            case EQ -> sb.append(fname).append("==").append(jsonPathVal(a));
            case GT -> sb.append(fname).append('>').append(jsonPathVal(a));
            case GTE -> sb.append(fname).append(">=").append(jsonPathVal(a));
            case LT -> sb.append(fname).append('<').append(jsonPathVal(a));
            case LTE -> sb.append(fname).append("<=").append(jsonPathVal(a));
            case BETWEEN -> sb.append(fname).append(">=").append(jsonPathVal(a))
                    .append(" && ").append(fname).append("<=").append(jsonPathVal(b));
            case IN -> {
                for (int i = 0; i < inValues.length; i++) {
                    if (i > 0) sb.append(" || ");
                    sb.append(fname).append("==").append(jsonPathVal(inValues[i]));
                }
            }
            default -> throw new UnsupportedOperationException(
                    "JSON_ARRAY filter does not support op " + opType + " on field " + fieldName);
        }
        if (negated || group) sb.append(')');
    }

    /**
     * 业务作用：JSONPath 字面量渲染: 数字直出, 字符串走 {@link JsonPaths#literal} (含完整控制字符转义).
     * 不接受 null (业务逻辑应该用单独的字段存在性判断, 而不是 == null).
     *
     * @param v 见上述说明
     * @return 见上述说明。
     */
    private static String jsonPathVal(Object v) {
        if (v == null) {
            throw new IllegalArgumentException("JSONPath filter value cannot be null");
        }
        return JsonPaths.literal(v);
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
    public ObjectPool.PooledHandle<Criteria> handle() {
        return this.handle;
    }

    /**
     * 业务作用：归还前清空字段名、取值与子条件引用。
     * 子条件本身也是池化对象，在此一并归还，漏还会让池随查询次数逐渐借空。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void restore() {
        this.fieldName = null;
        this.opType = null;
        this.a = null;
        this.b = null;
        this.c = null;
        this.d = null;
        this.inValues = null;
        this.negated = false;
    }

}
