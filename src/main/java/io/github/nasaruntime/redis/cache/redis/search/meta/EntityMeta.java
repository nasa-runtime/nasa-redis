package io.github.nasaruntime.redis.cache.redis.search.meta;

import io.github.nasaruntime.redis.cache.redis.search.RediSearch;
import io.github.nasaruntime.redis.cache.redis.search.annotation.DataType;
import io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import lombok.extern.slf4j.Slf4j;

import java.lang.invoke.VarHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Nasa
 * 实体类的元数据：索引名、前缀、数据类型、字段集合
 * <p>
 * 启动期一次性解析后缓存到 {@link MetaResolver}，运行时只读不写。
 */
@SuppressWarnings("unused")
@Slf4j
public final class EntityMeta {

    private final Class<?> type;
    private final String index;
    private final String prefix;
    private final DataType dataType;
    private final FieldMeta idField;
    private final Map<String, FieldMeta> fieldsByJavaName;
    private final Map<String, FieldMeta> fieldsByRedisName;
    /**
     * 持久化字段集 = idField + 索引字段中 {@code stored=true} 的 + placeholder 字段 (排除 schema-only)。
     * HASH 写入/读回用, 保证三类字段一起 round-trip:
     * <ul>
     *   <li>@RsId 作为普通 hash field 一起 round-trip，使 HASH+placeholder 查询可以恢复 id；</li>
     *   <li>placeholder 字段 (prefix 占位符里的字段) 也写入 HASH, 否则它们的值只存在 key 里, find(query) 读回为 null —
     *       因为 key 不可逆推 (字段值含分隔符就挂). JSON 模式占位符字段本就在 JSON 对象里, storedFields 不参与 JSON 写, 不受影响。</li>
     * </ul>
     * 已是索引字段 (placeholder 又叠加 @TagField 等) 的去重, 避免重复写。FT.CREATE schema 仍用 {@link #fields()}。
     */
    private final List<FieldMeta> storedFields;
    /**
     * {@link #storedFields} 的 redisName→FieldMeta 索引, mapToEntity 读回时用.
     * 含 idField + stored 索引字段 + placeholder 字段, 故 HASH 里的 id / placeholder field 都能被解析回填。
     */
    private final Map<String, FieldMeta> storedFieldsByRedisName;
    /**
     * key 编译表. {@code null} 表示 prefix 无占位符 ({@code keyOf} 走 fast path: {@code prefix + id}).
     * 非 null 时为 (literal / placeholder field) 交替段, 运行时遍历拼 key + 末尾 append @RsId.
     */
    private final KeySegment[] keySegments;
    /**
     * 占位符字段数 ({@link #keySegments} 里 field 段数). 0 表示无占位符. 用作 {@link #keyOf(Object...)} vararg 入参校验.
     */
    private final int placeholderCount;
    /**
     * StringBuilder 初始容量提示 (literal 总长 + 占位符数 × 8 平均估算). 占位符模式 keyOf 一次性 alloc 避免扩容.
     */
    private final int keyHintLen;
    /**
     * 索引 PREFIX 参数用的 literal head. 无占位符时等于 {@link #prefix}; 占位符时 = prefix 第一个 {@code {}} 之前的字面字符串.
     * <p>
     * 例 prefix={@code "order:SMO:{sym}:{d}:"} → literalPrefix={@code "order:SMO:"}.
     * FT.CREATE PREFIX 走 startsWith 匹配, 给 literal head 即覆盖该 entity 全部 key (含所有占位符值的变体).
     */
    private final String literalPrefix;
    /**
     * 仅 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY} / {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY_BUCKET} 模式下生效:
     * 标注 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey} 的字段, 按声明顺序. 其他模式为空 list.
     */
    private final List<Field> arrayKeyFields;
    /**
     * 跟 {@link #arrayKeyFields} 同序的 VarHandle 数组, 热路径 (arrayKey / collectParts / arrayKeyPartsOf) 用,
     * 走 JIT 内联零反射. 数组而非 List: 1-2 个 @JsonArrayKey 字段是常态, 数组直接索引最快.
     */
    private final VarHandle[] arrayKeyHandles;
    /**
     * 仅 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY_BUCKET} 模式生效: 桶数. 其他模式为 0.
     */
    private final int bucketCount;
    /**
     * 类上是否存在 {@code @JsonCreator} 工厂方法/构造. 用于 HASH 模式 mapToEntity 二选一路径:
     * 有则走 Jackson convertValue (接入对象池), 无则走 newInstance + 字段 set.
     */
    private final boolean hasJsonCreator;
    /**
     * 缓存的无参构造句柄, 已 setAccessible(true).
     * 用于 mapToEntity 直接 newInstance(), 避免每次 read 走 getDeclaredConstructor 反射.
     */
    private final Constructor<?> noArgConstructor;

    /**
     * 业务作用：承载一个实体的检索结构：索引名、键前缀、存储模式、主键字段与各索引字段。
     * 构造时即定型，此后只读——元信息在运行期被高频读取，可变会引入不必要的同步。
     *
     * @param type     实体类型
     * @param index    索引名
     * @param prefix   键前缀模板
     * @param dataType 存储模式
     * @param idField          主键字段
     * @param fieldsByJavaName 按 Java 字段名索引的字段表，供查询条件把业务字段名映射到索引字段名
     * @param fieldsByRedisName 按索引字段名索引的字段表，供解析检索响应时把字段还原到实体
     * @param arrayKeyFields   参与数组键拼装的字段
     * @param bucketCount      分桶数，为 0 表示未分桶
     * @param hasJsonCreator   该实体是否声明了反序列化工厂，声明后由其接管实例创建
     * @param keySegments      键模板切分出的各段
     * @param placeholderCount 键模板中的占位段个数，拼装时据此校验片段个数
     * @param keyHintLen       键的预估长度，用于预分配拼装缓冲，减少扩容
     * @param literalPrefix    键模板中第一个占位段之前的固定前缀，用于按前缀扫描
     */
    public EntityMeta(Class<?> type, String index, String prefix, DataType dataType,
                      FieldMeta idField, Map<String, FieldMeta> fieldsByJavaName,
                      Map<String, FieldMeta> fieldsByRedisName,
                      List<Field> arrayKeyFields, int bucketCount, boolean hasJsonCreator,
                      KeySegment[] keySegments, int placeholderCount, int keyHintLen,
                      String literalPrefix) {
        this.type = type;
        this.index = index;
        this.prefix = prefix;
        this.dataType = dataType;
        this.idField = idField;
        this.fieldsByJavaName = fieldsByJavaName;
        this.fieldsByRedisName = fieldsByRedisName;
        // 持久化字段集: stored 索引字段 + idField (idField 放末尾, redisName 撞名时 id 优先, 保证 id 能回填)
        List<FieldMeta> sf = new ArrayList<>(fieldsByJavaName.size() + 1);
        Map<String, FieldMeta> sbr = new HashMap<>((fieldsByRedisName.size() + 1) * 2);
        for (FieldMeta fm : fieldsByJavaName.values()) {
            if (fm.stored()) {
                sf.add(fm);
                sbr.put(fm.redisName(), fm);
            }
        }
        sf.add(idField);
        sbr.put(idField.redisName(), idField);
        // placeholder 字段也纳入持久化集 (HASH 模式): 值只在 key 里且不可逆推, 不写 HASH 则 find(query) 回来为 null。
        // JSON 模式 placeholder 字段本就在 JSON 对象里, storedFields 不参与 JSON 写, 不受影响。
        // 已是索引字段 (placeholder 又叠加 @TagField 等) 的跳过, 避免重复写。
        if (keySegments != null) {
            for (KeySegment seg : keySegments) {
                FieldMeta pf = seg.field();
                if (pf != null && !sbr.containsKey(pf.redisName())) {
                    sf.add(pf);
                    sbr.put(pf.redisName(), pf);
                }
            }
        }
        this.storedFields = sf;
        this.storedFieldsByRedisName = sbr;
        this.arrayKeyFields = arrayKeyFields;
        this.arrayKeyHandles = resolveArrayKeyHandles(arrayKeyFields);
        this.bucketCount = bucketCount;
        this.hasJsonCreator = hasJsonCreator;
        this.noArgConstructor = resolveNoArgConstructor(type);
        this.keySegments = keySegments;
        this.placeholderCount = placeholderCount;
        this.keyHintLen = keyHintLen;
        this.literalPrefix = literalPrefix;
    }

    /**
     * 业务作用：索引 PREFIX 参数: 无占位符 = prefix 全部; 占位符模式 = prefix 第一个 {@code {}} 之前的 literal head.
     * 给 {@link io.github.nasaruntime.redis.cache.redis.search.RediSearch} FT.CREATE PREFIX 参数用 — RediSearch 按 startsWith 匹配 key.
     *
     * @return 见上述说明。
     */
    public String literalPrefix() {
        return this.literalPrefix;
    }

    /**
     * 业务作用：一次性把 @JsonArrayKey Field 列表派生成 VarHandle 数组, 顺序对齐.
     * 复用 {@link FieldMeta#createVarHandle} 同款逻辑, 避免双份漂移.
     *
     * @param fields 见上述说明
     * @return 见上述说明。
     */
    private static VarHandle[] resolveArrayKeyHandles(List<Field> fields) {
        VarHandle[] handles = new VarHandle[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            handles[i] = FieldMeta.createVarHandle(fields.get(i));
        }
        return handles;
    }

    /**
     * 业务作用：解析无参构造并执行一次 setAccessible；找不到时仅告警，由 mapToEntity 在实际需要构造实体时拒绝。
     *
     * @param type 反序列化目标类型
     * @return 见上述说明。
     */
    private static Constructor<?> resolveNoArgConstructor(Class<?> type) {
        try {
            Constructor<?> c = type.getDeclaredConstructor();
            c.setAccessible(true);
            return c;
        } catch (NoSuchMethodException e) {
            log.warn("EntityMeta: {} has no no-arg constructor; mapToEntity will fail at runtime", type.getName());
            return null;
        }
    }

    /**
     * 业务作用：查询键的数据类型。
     *
     * <p>参数说明: 无。
     *
     * @return 命令的执行结果。
     */
    public Class<?> type() {
        return type;
    }

    /**
     * 业务作用：读取索引名，创建索引与检索时使用。
     *
     * <p>参数说明: 无。
     *
     * @return 索引名。
     */
    public String index() {
        return index;
    }

    /**
     * 业务作用：读取键前缀模板，其中的占位段在写入时由实体字段填充。
     *
     * <p>参数说明: 无。
     *
     * @return 键前缀模板。
     */
    public String prefix() {
        return prefix;
    }

    /**
     * 业务作用：读取存储模式，决定文档以哈希、JSON 还是 JSON 数组形式落地。
     *
     * <p>参数说明: 无。
     *
     * @return 存储模式。
     */
    public DataType dataType() {
        return dataType;
    }

    /**
     * 业务作用：读取主键字段的元信息，键的拼装与按主键定位都依赖它。
     *
     * <p>参数说明: 无。
     *
     * @return 主键字段元信息。
     */
    public FieldMeta idField() {
        return idField;
    }

    /**
     * 业务作用：读取全部索引字段的元信息。
     *
     * <p>参数说明: 无。
     *
     * @return 索引字段元信息集合。
     */
    public Collection<FieldMeta> fields() {
        return fieldsByJavaName.values();
    }

    /**
     * 业务作用：持久化字段集 (idField + stored 索引字段 + placeholder 字段). HASH 写入/读回遍历此集合,
     * schema-only 字段不在内。详见 {@link #storedFields} 字段注释。
     *
     * @return 见上述说明。
     */
    public List<FieldMeta> storedFields() {
        return storedFields;
    }

    /**
     * 业务作用：按 redisName 查持久化字段 (含 idField + stored 索引字段 + placeholder 字段). HASH mapToEntity 读回用,
     * 返回 null 表示该 hash field 非持久化字段 (跳过)。
     *
     * @param redisName 见上述说明
     */
    public FieldMeta storedFieldByRedisName(String redisName) {
        return storedFieldsByRedisName.get(redisName);
    }

    /**
     * 业务作用：缓存的无参构造句柄, 可能为 null (实体没有无参构造).
     *
     * @return 见上述说明。
     */
    public Constructor<?> noArgConstructor() {
        return noArgConstructor;
    }

    /**
     * 业务作用：按 Java 字段名取字段元信息，供查询条件把业务侧的字段名映射到索引字段名。
     *
     * @param javaName Java 字段名
     * @return 字段元信息；无该字段时为 null。
     */
    public FieldMeta fieldByJavaName(String javaName) {
        FieldMeta fm = fieldsByJavaName.get(javaName);
        if (fm == null) {
            throw new RediSearchException("Unknown field '" + javaName + "' on " + type.getSimpleName());
        }
        return fm;
    }

    /**
     * 业务作用：仅检查存在性, 不存在返回 null (用于 fail-fast 校验时手动判断).
     *
     * @param javaName 见上述说明
     */
    public FieldMeta findFieldByJavaName(String javaName) {
        return fieldsByJavaName.get(javaName);
    }

    /**
     * 业务作用：按索引字段名取字段元信息，供解析检索响应时把字段还原到实体。
     *
     * @param redisName 索引字段名
     * @return 字段元信息；无该字段时为 null。
     */
    public FieldMeta fieldByRedisName(String redisName) {
        return fieldsByRedisName.get(redisName);
    }

    /**
     * 业务作用：单 id 拼 key, 仅无占位符模式可用. 占位符模式下抛错引导 caller 用 {@link #keyOf(Object)} / {@link #keyOf(Object...)}.
     *
     * @param id 条目标识
     * @return 见上述说明。
     */
    public String key(String id) {
        if (this.keySegments != null) {
            throw new RediSearchException(type.getSimpleName() + " has placeholder prefix '"
                    + prefix + "', must use keyOf(entity) or keyOf(parts...) instead of key(String)");
        }
        return prefix + id;
    }

    /**
     * 业务作用：是否含 {@code @RsDocument.prefix} 占位符 (例 {@code "order:SMO:{sym}:{d}:"}).
     * 业务侧可据此决定走单 id API 还是 entity/vararg API.
     *
     * @return 见上述说明。
     */
    public boolean hasPlaceholder() {
        return this.keySegments != null;
    }

    /**
     * 业务作用：占位符字段数. 0 表示无占位符. 撑 {@link #keyOf(Object...)} vararg 入参校验.
     *
     * @return 见上述说明。
     */
    public int placeholderCount() {
        return this.placeholderCount;
    }

    /**
     * 业务作用：entity-based 算 key (HASH + JSON 共用入口). 无占位符 fast path = {@code prefix + id}, 有占位符遍历 segments 拼接.
     * 撮合主路径首选, 业务侧直接传 entity 实例.
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    public String keyOf(Object entity) {
        Object id = this.idField.get(entity);
        if (id == null) throw new RediSearchException("@RsId field is null");
        // id 与全框架统一渲染 (enum 走 @JsonValue/name); 非占位模式 prefix+id 可逆, 无需分隔符校验
        String idStr = MetaResolver.renderValue(id);
        if (this.keySegments == null) {
            return this.prefix + idStr;
        }
        StringBuilder sb = new StringBuilder(this.keyHintLen);
        for (KeySegment seg : this.keySegments) {
            seg.append(sb, entity);
        }
        // 占位模式 id 拼末尾, 与占位段同样校验分隔符防撞 key
        KeyParts.checkPart(idStr, "@RsId field");
        sb.append(idStr);
        return sb.toString();
    }

    /**
     * 业务作用：vararg 算 key — 业务侧没 entity 实例但知道 placeholder 值 + @RsId 值时用 (例如撤单只有 sym/dir/orderId 三个值).
     * <p>
     * parts 顺序约定: <b>placeholder 字段值按 prefix 占位符出现顺序排列, @RsId 值放末尾</b>.
     * 例 {@code prefix = "order:SMO:{sym}:{d}:"} + {@code @RsId Long id} →
     * {@code keyOf("BTCUSDT", "BUY", 12345L)} 出 {@code "order:SMO:BTCUSDT:BUY:12345"}.
     * <p>
     * 无占位符模式 parts 长度必须 = 1 (只传 @RsId 值); 有占位符模式 parts 长度必须 = placeholderCount + 1.
     *
     * @param parts 见上述说明
     * @return 见上述说明。
     */
    public String keyOf(Object... parts) {
        if (this.keySegments == null) {
            if (parts.length != 1) {
                throw new RediSearchException(type.getSimpleName() + " has no placeholder, "
                        + "expected 1 part (@RsId value), got " + parts.length);
            }
            if (parts[0] == null) throw new RediSearchException("@RsId part is null");
            return this.prefix + MetaResolver.renderValue(parts[0]);
        }
        int expected = this.placeholderCount + 1;
        if (parts.length != expected) {
            throw new RediSearchException(type.getSimpleName() + " has " + this.placeholderCount
                    + " placeholder(s), expected " + expected + " parts (placeholders + @RsId), got " + parts.length);
        }
        StringBuilder sb = new StringBuilder(this.keyHintLen);
        int partIdx = 0;
        for (KeySegment seg : this.keySegments) {
            if (seg.literal() != null) {
                sb.append(seg.literal());
            } else {
                Object v = parts[partIdx++];
                if (v == null) {
                    throw new RediSearchException("placeholder part #" + (partIdx - 1) + " is null");
                }
                // 与 keyOf(entity)/KeySegment 同款: enum 统一渲染 + 分隔符校验, 保证两条算 key 路径一致且不撞 key
                String rendered = MetaResolver.renderValue(v);
                KeyParts.checkPart(rendered, "placeholder part #" + (partIdx - 1));
                sb.append(rendered);
            }
        }
        Object rsId = parts[partIdx];
        if (rsId == null) throw new RediSearchException("@RsId part is null");
        String renderedId = MetaResolver.renderValue(rsId);
        KeyParts.checkPart(renderedId, "@RsId part");
        sb.append(renderedId);
        return sb.toString();
    }

    /**
     * 业务作用：从 Redis key 反推 @RsId 值. 仅无占位符模式可用 — 占位符模式下 key 含动态片段, split 不可逆 (字段值含分隔符就挂),
     * 业务应改从 FT.SEARCH 返回的 entity 直接拿 id 字段 (反序列化已经填好).
     *
     * @param key 缓存键
     */
    public String idFromKey(String key) {
        if (this.keySegments != null) {
            throw new RediSearchException(type.getSimpleName() + " has placeholder prefix '"
                    + prefix + "', idFromKey unsupported — read id field from deserialized entity instead");
        }
        if (key == null || !key.startsWith(this.prefix)) {
            throw new RediSearchException("Key does not match prefix: " + key);
        }
        return key.substring(this.prefix.length());
    }

    /**
     * 业务作用：仅 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY} / {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY_BUCKET} 模式有效: 标注 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey} 的字段列表.
     * 顺序为声明顺序 (子类字段先, 父类字段后). 其他模式返回空 list.
     */
    public List<Field> arrayKeyFields() {
        return arrayKeyFields;
    }

    /**
     * 业务作用：仅 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY_BUCKET} 模式生效: 桶数. 其他模式返回 0.
     */
    public int bucketCount() {
        return bucketCount;
    }

    /**
     * 业务作用：类上是否存在 {@code @JsonCreator} 工厂方法或构造. HASH 模式 mapToEntity 路径决策用:
     * 有则走 Jackson {@code convertValue} 让业务的工厂方法接入对象池, 没有则保留 newInstance + 字段 set.
     *
     * @return 见上述说明。
     */
    public boolean hasJsonCreator() {
        return hasJsonCreator;
    }

    /**
     * 业务作用：计算实体在 ARRAY 模式下的 Redis key.
     * <ul>
     *   <li>{@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY}: {@code prefix + part1[:part2...]}</li>
     *   <li>{@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY_BUCKET}: {@code prefix + "{" + part1[:part2...] + "}:bucket:" + (subId.hashCode() %% bucketCount)}</li>
     * </ul>
     * 字段值不能含分隔符 {@code :}, 否则抛 IAE.
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    public String arrayKey(Object entity) {
        if (arrayKeyFields.isEmpty()) {
            throw new RediSearchException(type.getSimpleName()
                    + " is not JSON_ARRAY/JSON_ARRAY_BUCKET mode or has no @JsonArrayKey field");
        }
        String body = collectParts(entity);
        if (dataType == DataType.JSON_ARRAY_BUCKET) {
            Object subId = idField.get(entity);
            if (subId == null) {
                throw new IllegalArgumentException("@RsId field is null on " + type.getSimpleName());
            }
            return prefix + "{" + body + "}:bucket:" + bucketOf(subId);
        }
        return prefix + body;
    }

    /**
     * 业务作用：直接给定 array key 部件 (按字段声明顺序) 拼接为 array key (不含 bucket idx 部分).
     * 在 BUCKET 模式下用于"取 hash tag 前缀", 进而生成所有桶 key.
     *
     * @param parts 见上述说明
     * @return 见上述说明。
     */
    public String arrayKeyOf(Object... parts) {
        if (arrayKeyFields.isEmpty()) {
            throw new RediSearchException(type.getSimpleName()
                    + " is not JSON_ARRAY/JSON_ARRAY_BUCKET mode or has no @JsonArrayKey field");
        }
        if (parts.length != arrayKeyFields.size()) {
            throw new IllegalArgumentException("arrayKeyOf expects " + arrayKeyFields.size()
                    + " parts, got " + parts.length);
        }
        String body = collectPartsArray(parts);
        // ARRAY 模式: 整 key 就是 prefix+body; BUCKET 模式: 不含 bucket idx 的"前缀" (调用方再追加 :bucket:N)
        return prefix + body;
    }

    /**
     * 业务作用：反射读出实体的 @JsonArrayKey 字段值列表, 返回顺序与 {@link #arrayKeyFields()} 一致 (子类先父类后).
     * <p>
     * 给 {@link #allBucketKeys} / {@link #bucketKey} 等需要 parts 数组的 caller 用 — 把反射逻辑收敛在 meta 层,
     * 数据访问层 (例 JsonArraySupport) 不直接调 {@link Field#get}.
     *
     * @param entity 见上述说明
     */
    public Object[] arrayKeyPartsOf(Object entity) {
        if (arrayKeyHandles.length == 0) {
            throw new RediSearchException(type.getSimpleName()
                    + " is not JSON_ARRAY/JSON_ARRAY_BUCKET mode or has no @JsonArrayKey field");
        }
        Object[] parts = new Object[arrayKeyHandles.length];
        for (int i = 0; i < arrayKeyHandles.length; i++) {
            parts[i] = arrayKeyHandles[i].get(entity);
        }
        return parts;
    }

    /**
     * 业务作用：BUCKET 模式: 算 subId 应该所在的桶 idx (0-based, 范围 [0, bucketCount)).
     * 与 {@link #bucketKey} 内部用的是同一个 floorMod 计算, 暴露这个方法是为了让 caller (例 saveOrReplace
     * 跨桶 LUA) 不必通过字符串比对回找 target idx.
     *
     * @param subId 见上述说明
     * @return 见上述说明。
     */
    public int bucketIndexOf(Object subId) {
        requireBucket("bucketIndexOf");
        if (subId == null) throw new IllegalArgumentException("subId cannot be null");
        return bucketOf(subId);
    }

    /**
     * 业务作用：BUCKET 模式: 给定 array key parts + sub id, 算出该子文档应该所在的桶 key.
     *
     * @param parts 见上述说明
     * @param subId 见上述说明
     * @return 见上述说明。
     */
    public String bucketKey(Object[] parts, Object subId) {
        requireBucket("bucketKey");
        if (subId == null) throw new IllegalArgumentException("subId cannot be null");
        if (parts.length != arrayKeyFields.size()) {
            throw new IllegalArgumentException("bucketKey expects " + arrayKeyFields.size()
                    + " parts, got " + parts.length);
        }
        String body = collectPartsArray(parts);
        return prefix + "{" + body + "}:bucket:" + bucketOf(subId);
    }

    /**
     * 业务作用：BUCKET 模式: 给定 array key parts, 返回所有 N 个桶 key (按 bucket idx 0..N-1 顺序).
     * <p>
     * 所有桶 key 共享同一个 hash tag {@code {body}}, 在 Redis Cluster 下落同一 slot, 可被 LUA 一次性跨桶操作.
     *
     * @param parts 见上述说明
     */
    public String[] allBucketKeys(Object... parts) {
        requireBucket("allBucketKeys");
        if (parts.length != arrayKeyFields.size()) {
            throw new IllegalArgumentException("allBucketKeys expects " + arrayKeyFields.size()
                    + " parts, got " + parts.length);
        }
        String body = collectPartsArray(parts);
        String tagPrefix = prefix + "{" + body + "}:bucket:";
        String[] out = new String[bucketCount];
        for (int i = 0; i < bucketCount; i++) out[i] = tagPrefix + i;
        return out;
    }

    /**
     * 业务作用：从实体上取出参与键拼装的各字段取值，并逐个校验其合法性。
     * 校验在拼装前完成：含分隔符的片段会拼出歧义键，写入后无法分辨谁覆盖了谁。
     *
     * @param entity 实体实例
     * @return 拼装完毕的键片段串。
     */
    private String collectParts(Object entity) {
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < arrayKeyHandles.length; i++) {
            Object v = arrayKeyHandles[i].get(entity);
            if (v == null) {
                Field f = arrayKeyFields.get(i);
                throw new IllegalArgumentException("@JsonArrayKey field '" + f.getName()
                        + "' is null on " + type.getSimpleName());
            }
            // 渲染(enum 统一)+校验后拼接; 抛错诊断用 arrayKeyFields 同 index 拿字段名
            String rendered = renderCheckedPart(v, "@JsonArrayKey field '" + arrayKeyFields.get(i).getName() + "'");
            if (i > 0) sb.append(':');
            sb.append(rendered);
        }
        return sb.toString();
    }

    /**
     * 业务作用：按调用方直接给出的片段拼装键，并校验其合法性与个数。
     * 个数必须与键模板的占位段一致——多一个或少一个都会拼出指向别处的键，且不会报错。
     *
     * @param parts 按键模板顺序排列的键片段
     * @return 拼装完毕的键片段串。
     */
    private String collectPartsArray(Object[] parts) {
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < parts.length; i++) {
            if (parts[i] == null) {
                throw new IllegalArgumentException("arrayKey part #" + i + " is null");
            }
            String rendered = renderCheckedPart(parts[i], "arrayKey part #" + i);
            if (i > 0) sb.append(':');
            sb.append(rendered);
        }
        return sb.toString();
    }

    /**
     * 业务作用：{@code Math.floorMod} 比 {@code Math.abs(...) %} 安全: 后者在 Integer.MIN_VALUE 时溢出.
     *
     * @param subId 见上述说明
     * @return 见上述说明。
     */
    private int bucketOf(Object subId) {
        // 先把 subId 规范成持久化值再算 hash: 否则 save 传 enum 对象、find/remove 传 @JsonValue 原始值时,
        // enum 的身份 hashCode 与原始值 hashCode 不同 → 落不同桶查不到 (且 enum 身份 hash 跨重启不稳定)。
        return Math.floorMod(MetaResolver.renderValue(subId).hashCode(), bucketCount);
    }

    /**
     * 业务作用：确认本实体声明了分桶，未声明时拒绝分桶相关的操作。
     * 未分桶的实体走分桶路径会算出错误的键，提前拒绝比事后排查脏数据代价小得多。
     *
     * @param op 操作名，出现在错误消息中便于定位
     * 返回: 无返回值；未声明分桶时抛出异常。
     */
    private void requireBucket(String op) {
        if (dataType != DataType.JSON_ARRAY_BUCKET) {
            throw new RediSearchException(op + " requires DataType.JSON_ARRAY_BUCKET, got " + dataType);
        }
        if (bucketCount <= 1) {
            throw new RediSearchException(op + " requires bucketCount > 1, got " + bucketCount);
        }
    }

    /**
     * 业务作用：<p>@JsonArrayKey 段渲染 + 分隔符校验: enum 走全框架统一渲染 ({@link MetaResolver#renderValue}), 再经
     * {@link KeyParts#checkPart} 禁 {@code :} / {@code &#123;} / {@code &#125;}, 返回渲染后字符串供拼接。
     *
     * @param v         见上述说明
     * @param fieldDesc 见上述说明
     */
    private static String renderCheckedPart(Object v, String fieldDesc) {
        String s = MetaResolver.renderValue(v);
        KeyParts.checkPart(s, fieldDesc);
        return s;
    }
}
