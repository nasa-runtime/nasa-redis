package io.github.nasaruntime.redis.cache.redis.search.meta;

import io.github.nasaruntime.redis.cache.redis.search.annotation.DataType;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Nasa
 * 单个字段的元数据：反射句柄 + RediSearch 索引描述
 * <p>
 * <b>redisName</b>: 该字段在 RediSearch 中的引用名 (alias). HASH 模式同时是 hash field 名,
 * JSON 模式仅作为 FT.SEARCH 查询时 {@code @alias} 的别名.
 * <p>
 * <b>jsonPath</b>: JSON 模式专用 JSONPath. 顶层字段为 {@code $.{redisName}},
 * 嵌套字段由 MetaResolver 根据注解显式指定的 path 决定.
 */
@SuppressWarnings("unused")
public final class FieldMeta {

    /**
     * 反射 Field 句柄，只在启动期解析 annotation、字段类型与字段名；运行期读写统一使用 {@link #handle}。
     */
    private final Field reflect;

    /**
     * 字段读写 VarHandle, 启动期通过 {@link MethodHandles#privateLookupIn} + {@link MethodHandles.Lookup#unreflectVarHandle}
     * 构造一次. JIT 内联后 get/set 性能等同直接字段访问 (~1ns), 比反射快 5-15x.
     * <p>
     * 跟 reflect 区别: reflect 是元数据 (annotation/type/name 反射 API), handle 是热路径访问器 (取值/赋值).
     */
    private final VarHandle handle;

    private final FieldType type;
    private final String redisName;
    private final String jsonPath;
    private final boolean sortable;
    private final double weight;
    private final boolean noStem;
    private final boolean noIndex;
    private final boolean caseSensitive;
    private final String separator;
    private final String phonetic;
    /**
     * 是否持久化字段: true = 进 HASH 写入并在反序列化时赋值 (普通索引字段 / @RsId);
     * false = schema-only (仅进 FT.CREATE schema + 查询 meta, 不写 HASH 不反序列化赋值) —— 用于
     * {@code @JsonIgnore + 索引注解 + 显式 JSONPath} 声明指向嵌套数据的索引 (其值由别的非 ignore 字段写入 JSON)。
     */
    private final boolean stored;

    /**
     * 业务作用：承载一个索引字段的全部属性：反射入口、索引类型、索引字段名、取值路径与各项索引选项。
     * 构造时即定型，此后只读。
     *
     * @param reflect  该字段的反射入口
     * @param type     索引类型
     * @param redisName     索引中的字段名
     * @param jsonPath      取值路径，JSON 存储模式下使用
     * @param sortable      是否可排序；不开启则该字段用于排序会被服务端拒绝
     * @param weight        全文相关性打分中的权重
     * @param noStem        是否关闭词干还原，编码类内容应关闭以保证整词匹配
     * @param noIndex       是否只存储不建索引，只存储的字段无法作为查询条件
     * @param caseSensitive 标签匹配是否区分大小写
     * @param separator     多值标签的分隔符
     * @param phonetic      语音匹配算法，为空表示不启用
     * @param stored        是否随文档一并存储；不存储则可检索但取不回取值
     */
    public FieldMeta(Field reflect, FieldType type, String redisName, String jsonPath,
                     boolean sortable, double weight, boolean noStem, boolean noIndex,
                     boolean caseSensitive, String separator, String phonetic, boolean stored) {
        // 不调 reflect.setAccessible(true): reflect 字段只用于读 annotation / getType / getName 元数据,
        // 字段值访问由 handle VarHandle 接管 (private lookup 已具备访问权限, 不依赖 setAccessible).
        this.reflect = reflect;
        this.handle = createVarHandle(reflect);
        this.type = type;
        this.redisName = redisName;
        this.jsonPath = jsonPath;
        this.sortable = sortable;
        this.weight = weight;
        this.noStem = noStem;
        this.noIndex = noIndex;
        this.caseSensitive = caseSensitive;
        this.separator = separator;
        this.phonetic = phonetic;
        this.stored = stored;
    }

    /**
     * 业务作用：读取该字段是否随文档一并存储。
     * 未存储的字段可被检索但取不回取值——只声明索引而不存储可省空间，代价是查回的实体该字段为空。
     *
     * <p>参数说明: 无。
     *
     * @return 随文档存储返回 true。
     */
    public boolean stored() {
        return stored;
    }

    /**
     * 业务作用：构造 VarHandle. 业务 entity 在严格 module 下 (没 --add-opens) 会抛 IAE, 启动期 fail-fast 比运行时崩好.
     * <p>
     * package-private 给同包的 {@code EntityMeta.arrayKeyHandles} 派生路径共用, 避免 DRY 漂移.
     *
     * @param f 见上述说明
     * @return 见上述说明。
     */
    static VarHandle createVarHandle(Field f) {
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(
                    f.getDeclaringClass(), MethodHandles.lookup());
            return lookup.unreflectVarHandle(f);
        } catch (IllegalAccessException e) {
            throw new RediSearchException("Cannot create VarHandle for "
                    + f.getDeclaringClass().getName() + "#" + f.getName()
                    + " (业务 entity 在严格 module 下需要 --add-opens 该 module/包)", e);
        }
    }

    /**
     * 业务作用：读取该字段的反射入口，读写实体取值时使用。
     *
     * <p>参数说明: 无。
     *
     * @return 反射入口。
     */
    public Field reflect() {
        return reflect;
    }

    /**
     * 业务作用：读取字段对应的 RediSearch 索引类型。
     *
     * <p>参数说明: 无。
     *
     * @return TAG、NUMERIC、TEXT、GEO 或 ID 类型。
     */
    public FieldType type() {
        return type;
    }

    /**
     * 业务作用：读取该字段在索引中的名称，渲染查询串时使用。
     *
     * <p>参数说明: 无。
     *
     * @return 索引中的字段名。
     */
    public String redisName() {
        return redisName;
    }

    /**
     * 业务作用：读取该字段的取值路径，JSON 存储模式下定位嵌套取值。
     *
     * <p>参数说明: 无。
     *
     * @return 取值路径。
     */
    public String jsonPath() {
        return jsonPath;
    }

    /**
     * 业务作用：读取该字段是否可排序。
     * 不可排序的字段用于排序会被服务端拒绝，需在索引声明时开启。
     *
     * <p>参数说明: 无。
     *
     * @return 可排序返回 true。
     */
    public boolean sortable() {
        return sortable;
    }

    /**
     * 业务作用：读取该字段在全文相关性打分中的权重。
     *
     * <p>参数说明: 无。
     *
     * @return 权重。
     */
    public double weight() {
        return weight;
    }

    /**
     * 业务作用：读取该字段是否关闭词干还原。
     * 关闭后必须整词匹配，适用于编码、标识一类不应被词干化的内容。
     *
     * <p>参数说明: 无。
     *
     * @return 关闭词干还原返回 true。
     */
    public boolean noStem() {
        return noStem;
    }

    /**
     * 业务作用：读取该字段是否只存储不建索引。
     * 只存储的字段取得回但<b>无法作为查询条件</b>。
     *
     * <p>参数说明: 无。
     *
     * @return 只存储不建索引返回 true。
     */
    public boolean noIndex() {
        return noIndex;
    }

    /**
     * 业务作用：读取该字段的标签匹配是否区分大小写。
     *
     * <p>参数说明: 无。
     *
     * @return 区分大小写返回 true。
     */
    public boolean caseSensitive() {
        return caseSensitive;
    }

    /**
     * 业务作用：读取多值标签字段的分隔符。
     * 取值中若含有该分隔符会被拆成多个标签，需由写入方确保不含。
     *
     * <p>参数说明: 无。
     *
     * @return 分隔符。
     */
    public String separator() {
        return separator;
    }

    /**
     * 业务作用：读取该字段的语音匹配算法，为空表示不启用。
     *
     * <p>参数说明: 无。
     *
     * @return 语音匹配算法。
     */
    public String phonetic() {
        return phonetic;
    }

    /**
     * 业务作用：通过启动期固化的 VarHandle 读取实体字段值。
     *
     * @param entity 字段所属的业务实体
     * @return 实体中的当前字段值；字段值本身允许为 null。
     */
    public Object get(Object entity) {
        return handle.get(entity);
    }

    /**
     * 业务作用：写入字符串值。
     *
     * @param entity 见方法语义
     * @param value 待写入的值
     * 返回: 无返回值。
     */
    public void set(Object entity, Object value) {
        handle.set(entity, value);
    }

    /**
     * 业务作用：生成 FT.CREATE SCHEMA 子片段, 根据 DataType 输出 HASH / JSON 两种格式.
     * <p>
     * HASH 模式: {@code [userId, TAG, SEPARATOR, ',', SORTABLE]} <br>
     * JSON 模式: {@code [$.user.id, AS, userId, TAG, SEPARATOR, ',', SORTABLE]}
     *
     * @param dataType 见上述说明
     * @return 见上述说明。
     */
    public List<String> toSchemaArgs(DataType dataType) {
        List<String> args = new ArrayList<>(10);
        if (dataType == DataType.JSON) {
            args.add(jsonPath);
            args.add("AS");
            args.add(redisName);
        } else if (dataType == DataType.JSON_ARRAY || dataType == DataType.JSON_ARRAY_BUCKET) {
            // ARRAY / BUCKET 模式索引子文档字段: $.field → $[*].field (RediSearch 把 array 展开后逐元素索引).
            // BUCKET 模式下索引依然覆盖所有桶 key (FT.CREATE PREFIX 匹配), 单桶内子文档展开规则不变.
            // 假设 jsonPath 是 "$.xxx" 形式 (默认/简单注解), 简单替换 "$." → "$[*]."
            // 若业务给的是更复杂的 JSONPath (如 "$.payload.ip"), 不当 ARRAY field 处理, 直接保留 (索引到根 array 上)
            String p = jsonPath.startsWith("$.") ? "$[*]." + jsonPath.substring(2) : jsonPath;
            args.add(p);
            args.add("AS");
            args.add(redisName);
        } else {
            args.add(redisName);
        }
        switch (type) {
            case TAG -> {
                args.add("TAG");
                if (separator != null && !",".equals(separator)) {
                    args.add("SEPARATOR");
                    args.add(separator);
                }
                if (caseSensitive) args.add("CASESENSITIVE");
            }
            case NUMERIC -> {
                args.add("NUMERIC");
                if (noIndex) args.add("NOINDEX");
            }
            case TEXT -> {
                args.add("TEXT");
                if (weight != 1.0) {
                    args.add("WEIGHT");
                    args.add(String.valueOf(weight));
                }
                if (noStem) args.add("NOSTEM");
                if (phonetic != null && !phonetic.isEmpty()) {
                    args.add("PHONETIC");
                    args.add(phonetic);
                }
            }
            case GEO -> args.add("GEO");
            case ID -> throw new RediSearchException("ID field should not appear in schema");
        }
        if (sortable) args.add("SORTABLE");
        return args;
    }
}
