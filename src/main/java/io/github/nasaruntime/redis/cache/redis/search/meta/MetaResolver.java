package io.github.nasaruntime.redis.cache.redis.search.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.nasaruntime.redis.cache.redis.search.annotation.DataType;
import io.github.nasaruntime.redis.cache.redis.search.annotation.GeoField;
import io.github.nasaruntime.redis.cache.redis.search.annotation.JsonArrayKey;
import io.github.nasaruntime.redis.cache.redis.search.annotation.NumericField;
import io.github.nasaruntime.redis.cache.redis.search.annotation.RsDocument;
import io.github.nasaruntime.redis.cache.redis.search.annotation.RsId;
import io.github.nasaruntime.redis.cache.redis.search.annotation.TagField;
import io.github.nasaruntime.redis.cache.redis.search.annotation.TextField;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Nasa
 * 实体元数据解析器：扫描 {@link RsDocument @RsDocument} 类，反射所有字段注解，
 * 构造 {@link EntityMeta} 并缓存。
 * <p>
 * CACHE 用 Caffeine weakKeys + maximumSize, key 是 {@code Class<?>}。
 * <p>
 * <b>注意</b>: value ({@code EntityMeta}) 强引用 {@code Class / Field / Constructor / VarHandle}, 形成 cache→value→Class(key)
 * 强引用链, 故 weakKeys <b>不足以</b>在 classloader 卸载时立即回收 entry。实际回收靠 maximumSize 上限淘汰 +
 * 热重载或卸载时显式调用 {@link #clear()}（由 {@code RediSearch.destroyAll} 触发）。DevTools 反复加载由 maximumSize 限制缓存上限。
 */
@Slf4j
public final class MetaResolver {

    private static final Cache<Class<?>, EntityMeta> CACHE = Caffeine.newBuilder()
            .weakKeys()
            .maximumSize(4096)
            .build();

    /**
     * 业务作用：私有化构造，杜绝实例化——本类只提供元信息的解析与缓存。
     *
     * <p>参数说明: 无。
     */
    private MetaResolver() {}

    /**
     * 业务作用：取实体的结构元信息，首次解析后缓存。
     * 解析要走反射与注解读取，开销远高于检索本身；不缓存会让每次查询都重复付出这份代价。
     *
     * @param type 实体类型
     * @return 实体结构元信息。
     */
    public static EntityMeta resolve(Class<?> type) {
        return CACHE.get(type, MetaResolver::doResolve);
    }

    /**
     * 业务作用：清空元信息缓存，用于类被重新加载后强制重新解析。
     * 常规运行期无需调用——实体结构在进程生命周期内不会变化。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    public static void clear() {
        CACHE.invalidateAll();
        ENUM_JSONVALUE_CACHE.invalidateAll();
    }

    /**
     * 业务作用：实际解析一个实体的结构：索引名、键前缀、存储模式、主键与各索引字段。
     * 在此完成各项一致性校验（如主键必须可往返序列化），使配置错误在首次使用时就暴露，
     * 而不是等到写入或查询时才以难解释的形式出现。
     *
     * @param type 实体类型
     * @return 解析出的结构元信息。
     */
    private static EntityMeta doResolve(Class<?> type) {
        RsDocument doc = type.getAnnotation(RsDocument.class);
        if (doc == null) {
            throw new RediSearchException(type.getName() + " is missing @RsDocument");
        }

        Map<String, FieldMeta> byJava = new LinkedHashMap<>();
        Map<String, FieldMeta> byRedis = new LinkedHashMap<>();
        FieldMeta idField = null;
        List<Field> arrayKeyFields = new ArrayList<>(2);

        // 收集本类 + 父类链全部字段, 子类字段优先(已在 byJVM 时跳过 super 同名),
        // 这样 @TagField/@RsId 写在父类抽象基类里也能被正确识别.
        for (Field f : collectAllFields(type)) {
            if (Modifier.isStatic(f.getModifiers())) continue;

            // @JsonIgnore: 框架完全跳过该字段的索引注册 + 不进 byJVM/byRedis.
            // 但 @RsId / @JsonArrayKey 仍识别(语义: "id/key 字段不进 JSON, 框架从 Redis key 反推/拼接"),
            // 业务自负责标 @JsonCreator 工厂方法回填这些字段; Jackson 序列化天然跳过 @JsonIgnore 字段, 无需框架配合.
            boolean ignored = f.isAnnotationPresent(JsonIgnore.class);

            // @JsonArrayKey 可以叠加在其他索引注解上 (例如同一字段既是 ARRAY key 又被 TagField 索引),
            // 因此单独收集, 不和下面的 @RsId / index 注解互斥.
            // 不调 f.setAccessible(true): 字段值访问由 EntityMeta.arrayKeyHandles VarHandle 接管 (private lookup 自带权限).
            if (f.isAnnotationPresent(JsonArrayKey.class)) {
                arrayKeyFields.add(f);
            }

            if (f.isAnnotationPresent(RsId.class)) {
                if (idField != null) {
                    // 父类与子类都标注 @RsId, 或同一类两个字段都标注 → 多 ID, 直接报错避免歧义
                    throw new RediSearchException("Multiple @RsId on " + type.getSimpleName());
                }
                // @RsId 字段必须进 JSON/HASH (key 跟字段值要一致): 标 @JsonIgnore 后 JSON 不存 id, 反序列化拿不到 id, fail-fast
                if (ignored) {
                    throw new RediSearchException("@RsId field '" + f.getName() + "' on " + type.getName()
                            + " cannot be @JsonIgnore — id must be persisted to JSON/HASH and read back at deserialization");
                }
                // 适配 Jackson @JsonProperty rename: 框架内部 idFilter "$[?(@.<idName>==X)]" 用的就是
                // JSON 文档实际字段名, 业务挂 @JsonProperty("docId") 时, JSON 输出 "docId" → idName 也用 "docId",
                // findSubDoc / saveOrReplace 的 DEL filter 才能命中；无注解时使用对象字段名。
                String idName = resolveJacksonName(f);
                // idName 会进入 idFilter "$[?(@.<idName>==X)]"，必须先校验为安全 alias。
                validateAlias(idName, f);
                // enum @RsId: 解析阶段触发 @JsonValue accessor 校验, 提前暴露非法 @JsonValue (否则首次 save/key 才炸)
                validateEnumJsonValue(f.getType());
                idField = new FieldMeta(f, FieldType.ID, idName, "$." + idName,
                        false, 1.0, false, false, false, ",", "", true);
                continue;
            }

            // @JsonIgnore + 索引注解 → schema-only 字段 (stored=false): 进 schema/查询 meta, 不写 HASH 不反序列化赋值.
            // 典型: @JsonIgnore @NumericField(name="$.extra.vip") 声明指向嵌套数据的索引, 其值由别的非 ignore 字段写进 JSON.
            // 普通 @JsonIgnore (无索引注解) → parseFieldAnnotation 返 null → 跳过, 框架完全不碰.
            FieldMeta fm = parseFieldAnnotation(f, !ignored, doc.type());
            if (fm == null) continue;

            // 子类先扫到则保留子类版本, 父类同名字段静默跳过(模拟 对象字段隐藏语义)
            if (byJava.containsKey(f.getName())) continue;
            if (byRedis.containsKey(fm.redisName())) {
                throw new RediSearchException("Duplicate field redis name: " + fm.redisName());
            }
            byJava.put(f.getName(), fm);
            byRedis.put(fm.redisName(), fm);
        }

        if (idField == null) {
            throw new RediSearchException(type.getName() + " missing @RsId field");
        }
        // idField 不在 byRedis (上面 continue 了), 故 byRedis 的重名检查漏了"普通字段 alias 撞 @RsId alias"的情况.
        // 撞名会让 HASH 同一 field 被 storedFields 写两次 / 读回歧义, 单独 fail-fast。
        if (byRedis.containsKey(idField.redisName())) {
            throw new RediSearchException("Field redis name '" + idField.redisName()
                    + "' collides with @RsId alias on " + type.getName());
        }
        // 框架只读取字段级 @JsonProperty / @JsonIgnore；方法级注解会让 Jackson 与索引元数据分裂，必须在启动期拒绝。
        validateNoMethodLevelJacksonAnnotations(type);
        // 拆出 array key / bucket 两块校验, 保持 doResolve 主线只剩"收集 → 校验 → 构造"三步
        List<Field> effectiveArrayKeys = validateArrayKeyFields(type, doc, arrayKeyFields);
        int bucketCount = validateBucketConfig(type, doc);
        boolean hasJsonCreator = detectJsonCreator(type);
        // 解析 @RsDocument.prefix 占位符 (例 "order:SMO:{sym}:{d}:") → KeySegment 编译表 + placeholder 数 + keyHintLen
        PrefixParseResult ppr = parsePrefixPlaceholders(type, doc, idField, byJava, byRedis);

        return new EntityMeta(type, doc.index(), doc.prefix(), doc.type(), idField, byJava, byRedis,
                effectiveArrayKeys, bucketCount, hasJsonCreator,
                ppr.segments, ppr.placeholderCount, ppr.keyHintLen, ppr.literalPrefix);
    }

    /**
     * {@link #parsePrefixPlaceholders} 返回值. segments=null 表示 prefix 无占位符 (走 EntityMeta fast path).
     * literalPrefix: 无占位符时 = 完整 prefix; 占位符时 = 第一个 {@code {} 之前的 literal head (给 FT.CREATE PREFIX 用).
     */
    private record PrefixParseResult(KeySegment[] segments, int placeholderCount, int keyHintLen,
                                     String literalPrefix) {
    }

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{([^{}]+)\\}");

    /**
     * 业务作用：解析 @RsDocument.prefix 里的 {fieldName} 占位符, 拼成 (literal / field reference) 编译表.
     * <p>
     * 字段查找优先级: byJVM (对象字段名) → byRedis (@JsonProperty rename 后的名) →
     * fallback 全字段扫描 (含 @JsonIgnore 标的字段, 因 @JsonIgnore 字段不进 byJVM/byRedis 索引,
     * 但语义上本组件仍识别它做 key 拼接, 跟 @RsId 同款"特殊字段").
     * <p>
     * fail-fast 校验:
     * <ul>
     *   <li>占位符名不命中任何字段 → 抛错</li>
     *   <li>同名同时匹配 对象名 + 另一字段 redisName (ambiguous) → 抛错强制业务 disambiguate</li>
     *   <li>占位符引用 @RsId 字段 → 抛错 (@RsId 必拼末尾, 占位符重复矛盾)</li>
     *   <li>占位符字段类型必须 String/Number/primitive (跟 @JsonArrayKey 同款限制)</li>
     * </ul>
     * 无占位符返回 segments=null (EntityMeta 走 fast path: {@code prefix + id}).
     *
     * @param type    反序列化目标类型
     * @param doc     见上述说明
     * @param idField 见上述说明
     * @param byJava  见上述说明
     * @param byRedis 见上述说明
     */
    private static PrefixParseResult parsePrefixPlaceholders(Class<?> type, RsDocument doc, FieldMeta idField,
                                                             Map<String, FieldMeta> byJava,
                                                             Map<String, FieldMeta> byRedis) {
        String prefix = doc.prefix();
        Matcher m = PLACEHOLDER_PATTERN.matcher(prefix);
        if (!m.find()) {
            // 无占位符 fast path: literalPrefix = 完整 prefix
            return new PrefixParseResult(null, 0, 0, prefix);
        }
        // 占位符模式: literalPrefix = prefix 第一个 { 之前的 literal head (给 FT.CREATE PREFIX startsWith 用)
        int firstBrace = prefix.indexOf('{');
        String literalPrefix = firstBrace > 0 ? prefix.substring(0, firstBrace) : "";
        // 有占位符: 切分 literal / placeholder segments
        List<KeySegment> segs = new ArrayList<>();
        int placeholderCount = 0;
        int literalLen = 0;
        m.reset();
        int last = 0;
        while (m.find()) {
            if (m.start() > last) {
                String literal = prefix.substring(last, m.start());
                segs.add(new KeySegment(literal, null));
                literalLen += literal.length();
            }
            String name = m.group(1);
            FieldMeta fm = resolvePlaceholderField(name, type, idField, byJava, byRedis);
            // 占位符字段类型校验: String/Number/primitive/Enum (Enum 经 renderValue 拼 key: 有 @JsonValue 用其值否则 name(), 跟 Jackson 一致)
            Class<?> ft = fm.reflect().getType();
            if (!(CharSequence.class.isAssignableFrom(ft) || Number.class.isAssignableFrom(ft)
                    || ft.isPrimitive() || ft.isEnum())) {
                throw new RediSearchException("@RsDocument.prefix placeholder '{" + name + "}' field type must be "
                        + "String/Number/primitive/Enum, got " + ft.getName() + " on " + type.getName());
            }
            segs.add(new KeySegment(null, fm));
            placeholderCount++;
            last = m.end();
        }
        if (last < prefix.length()) {
            String literal = prefix.substring(last);
            segs.add(new KeySegment(literal, null));
            literalLen += literal.length();
        }
        // keyHintLen = literal 总长 + placeholder 数 × 8 (字段值平均长度估算) + RsId 8 (Long 估算)
        int keyHintLen = literalLen + placeholderCount * 8 + 8;
        return new PrefixParseResult(segs.toArray(new KeySegment[0]), placeholderCount, keyHintLen, literalPrefix);
    }

    /**
     * 业务作用：查找占位符引用的字段 (含 @JsonIgnore 标的字段, fallback 走原始反射). 详见 {@link #parsePrefixPlaceholders}.
     *
     * @param name    见上述说明
     * @param type    反序列化目标类型
     * @param idField 见上述说明
     * @param byJava  见上述说明
     * @param byRedis 见上述说明
     * @return 见上述说明。
     */
    private static FieldMeta resolvePlaceholderField(String name, Class<?> type, FieldMeta idField,
                                                     Map<String, FieldMeta> byJava,
                                                     Map<String, FieldMeta> byRedis) {
        // 占位符不能引用 @RsId 字段 (RsId 必拼末尾, 占位符重复矛盾)
        if (name.equals(idField.reflect().getName()) || name.equals(idField.redisName())) {
            throw new RediSearchException("@RsDocument.prefix placeholder '{" + name + "}' cannot reference @RsId field on "
                    + type.getName() + " — @RsId is appended at the end of the key");
        }
        FieldMeta j = byJava.get(name);
        FieldMeta r = byRedis.get(name);
        // ambiguous: 对象名 + 另一字段 redisName 同名
        if (j != null && r != null && j != r) {
            throw new RediSearchException("@RsDocument.prefix placeholder '{" + name + "}' is ambiguous on "
                    + type.getName() + " — matches Java field '" + name + "' (" + j.reflect().getName()
                    + ") and another field's @JsonProperty name (" + r.reflect().getName()
                    + "); rename one to disambiguate");
        }
        if (j != null) return j;
        if (r != null) return r;
        // fallback: 字段在原始反射可见但不在 byJVM/byRedis (可能没标 @TagField 等索引注解, 也可能被 @JsonIgnore 跳过)
        // - 没标索引注解但不 @JsonIgnore: Jackson 默认序列化, JSON 有字段值, 反序列化能拿到 → placeholder OK, 构造简化 FieldMeta
        // - 标了 @JsonIgnore: JSON 不存字段值, 反序列化拿到 null, placeholder 算 key 会出错 → fail-fast 抛错
        for (Field f : collectAllFields(type)) {
            if (Modifier.isStatic(f.getModifiers())) continue;
            String jName = f.getName();
            String jacksonName = resolveJacksonName(f);
            if (jName.equals(name) || jacksonName.equals(name)) {
                if (f.isAnnotationPresent(JsonIgnore.class)) {
                    throw new RediSearchException("@RsDocument.prefix placeholder '{" + name + "}' references field '"
                            + jName + "' which is @JsonIgnore on " + type.getName()
                            + " — placeholder field must be persisted to JSON, remove @JsonIgnore");
                }
                // placeholder 字段 alias 也会进入查询与 JSONPath，必须使用相同的安全校验。
                validateAlias(jacksonName, f);
                // enum placeholder 字段: 解析阶段触发 @JsonValue 校验 (KeySegment.append 走 renderValue)
                validateEnumJsonValue(f.getType());
                // 简化 FieldMeta: FieldType.ID 表示"框架使用但不进 RediSearch schema", 跟 @RsId 同款
                return new FieldMeta(f, FieldType.ID, jacksonName, "$." + jacksonName,
                        false, 1.0, false, false, false, ",", "", true);
            }
        }
        throw new RediSearchException("@RsDocument.prefix placeholder '{" + name + "}' not match any field on "
                + type.getName() + " (existing fields: " + byJava.keySet() + ")");
    }

    /**
     * 业务作用：校验 @JsonArrayKey 字段集合, 返回按 {@code order} 升序排好的最终列表:
     * <ul>
     *   <li>ARRAY/BUCKET 模式: 必须 ≥ 1 个 @JsonArrayKey, 字段类型必须 String/Number/primitive,
     *       同一 {@code order} 值出现 ≥ 2 次时抛错 (强制业务消除歧义)</li>
     *   <li>非 ARRAY 模式: 标注本注解会被忽略 + warn 日志, 返回空 list</li>
     * </ul>
     * 同类多字段时必须用 {@code @JsonArrayKey(order = N)} 显式编号 (单字段保持默认 order = 0 即可).
     * 之所以走 order 而非反射顺序: {@link Class#getDeclaredFields()} 顺序 JLS 不保证 (HotSpot 按源码序,
     * GraalVM native-image / 其他 JVM 不保证), 跨 JVM 不可移植.
     *
     * @param type           反序列化目标类型
     * @param doc            见上述说明
     * @param arrayKeyFields 见上述说明
     */
    private static List<Field> validateArrayKeyFields(Class<?> type, RsDocument doc, List<Field> arrayKeyFields) {
        boolean isArrayMode = doc.type() == DataType.JSON_ARRAY || doc.type() == DataType.JSON_ARRAY_BUCKET;
        if (!isArrayMode) {
            if (!arrayKeyFields.isEmpty()) {
                log.warn("{}: @JsonArrayKey present but DataType is {}, annotation will be ignored",
                        type.getName(), doc.type());
            }
            return Collections.emptyList();
        }
        if (arrayKeyFields.isEmpty()) {
            throw new RediSearchException(type.getName()
                    + " is " + doc.type() + " but has no @JsonArrayKey field");
        }
        // 同 order 重复校验 + 字段类型校验
        Map<Integer, Field> seenOrder = new LinkedHashMap<>();
        for (Field f : arrayKeyFields) {
            int order = f.getAnnotation(JsonArrayKey.class).order();
            Field prev = seenOrder.putIfAbsent(order, f);
            if (prev != null) {
                throw new RediSearchException("Duplicate @JsonArrayKey(order=" + order + ") on "
                        + type.getName() + ": '" + prev.getName() + "' and '" + f.getName()
                        + "' — use distinct order values to disambiguate");
            }
            Class<?> ft = f.getType();
            // enum 放行: 渲染链 (renderCheckedPart → renderValue) 已统一支持 enum @JsonValue, 与 placeholder 校验一致
            if (!(CharSequence.class.isAssignableFrom(ft) || Number.class.isAssignableFrom(ft)
                    || ft.isPrimitive() || ft.isEnum())) {
                throw new RediSearchException("@JsonArrayKey field '" + f.getName()
                        + "' must be String/Number/primitive/enum, got " + ft.getName());
            }
            // enum @JsonArrayKey: 解析阶段触发 @JsonValue 校验 (key 渲染走 renderValue)
            validateEnumJsonValue(ft);
        }
        // 按 order 升序稳定排序: 业务无需关注反射顺序
        List<Field> sorted = new ArrayList<>(arrayKeyFields);
        sorted.sort(Comparator.comparingInt(a -> a.getAnnotation(JsonArrayKey.class).order()));
        return sorted;
    }

    /**
     * 业务作用：探测类上是否存在 {@code @JsonCreator} 工厂方法或构造器, 含父类链.
     * <p>
     * HASH 模式 mapToEntity 据此选择路径:
     * <ul>
     *   <li>有 @JsonCreator → Jackson {@code convertValue} 走业务工厂方法 (业务自行接入对象池)</li>
     *   <li>无 @JsonCreator → 使用 newInstance + 字段 set，支持普通实体</li>
     * </ul>
     *
     * @param type 待检测的实体类型
     * @return 见上述说明。
     */
    private static boolean detectJsonCreator(Class<?> type) {
        Class<?> c = type;
        while (c != null && c != Object.class) {
            for (Constructor<?> ctor : c.getDeclaredConstructors()) {
                if (ctor.isAnnotationPresent(JsonCreator.class)) return true;
            }
            for (Method m : c.getDeclaredMethods()) {
                if (Modifier.isStatic(m.getModifiers()) && m.isAnnotationPresent(JsonCreator.class)) {
                    return true;
                }
            }
            c = c.getSuperclass();
        }
        return false;
    }

    /**
     * 业务作用：校验 bucket 相关配置, 返回最终生效的 bucketCount:
     * <ul>
     *   <li>JSON_ARRAY_BUCKET 模式: bucketCount 必须 &gt; 1; prefix 不能含 '{' 或 '}' (会破坏框架自己拼的 hash tag)</li>
     *   <li>其他模式: bucketCount 必须 0 (= 未设置), 非 0 时仅 warn 并强制归零</li>
     * </ul>
     * prefix 含 '{' 的危害: Redis Cluster 解析 hash tag 取第一个 {...}, 业务 prefix 内嵌 hash tag 会让所有桶
     * 的 effective tag 退化到 prefix 内容, 不同 @JsonArrayKey 的桶都落同一 slot, 失去分片或形成隐性耦合.
     *
     * @param type 反序列化目标类型
     * @param doc  见上述说明
     */
    private static int validateBucketConfig(Class<?> type, RsDocument doc) {
        int bucketCount = doc.bucketCount();
        if (doc.type() != DataType.JSON_ARRAY_BUCKET) {
            if (bucketCount != 0) {
                log.warn("{}: bucketCount={} set but DataType is {}, it will be ignored",
                        type.getName(), bucketCount, doc.type());
            }
            return 0;
        }
        if (bucketCount <= 1) {
            throw new RediSearchException(type.getName()
                    + " is DataType.JSON_ARRAY_BUCKET, requires bucketCount > 1, got " + bucketCount);
        }
        String pfx = doc.prefix();
        if (pfx.indexOf('{') >= 0 || pfx.indexOf('}') >= 0) {
            throw new RediSearchException(type.getName() + " is JSON_ARRAY_BUCKET, prefix '" + pfx
                    + "' must not contain '{' or '}' — this component appends its own hash tag '{arrayKey}:bucket:N' "
                    + "for cluster co-location; user-supplied braces would shift the effective hash tag "
                    + "and break per-key slot distribution");
        }
        return bucketCount;
    }

    /**
     * 业务作用：从子类向父类(到 Object 为止)收集所有 declared field, 顺序为子类在前父类在后.
     *
     * @param type 反序列化目标类型
     * @return 见上述说明。
     */
    private static List<Field> collectAllFields(Class<?> type) {
        List<Field> all = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Class<?> c = type;
        while (c != null && c != Object.class) {
            for (Field f : c.getDeclaredFields()) {
                // 同名字段子类已记录则跳过, 不重复加入
                if (seen.add(f.getName())) all.add(f);
            }
            c = c.getSuperclass();
        }
        return all;
    }

    /**
     * 业务作用：校验 @JsonProperty / @JsonIgnore 只标在字段上。框架只读 Field（resolveJacksonName / 字段扫描），
     * 方法级 (getter/setter) 注解框架不感知, 会与 Jackson 实际行为分裂:
     * <ul>
     *   <li>method 级 @JsonProperty: JSON 字段名 rename, 但框架 alias/JSONPath 仍按字段名 → schema/查询与 JSON 不一致</li>
     *   <li>method 级 @JsonIgnore: Jackson JSON 跳过该属性, 但框架扫字段仍入 schema/byRedis (JSON 空索引);
     *       HASH 模式更糟 — 框架按字段反射 HSET, getter @JsonIgnore 完全不生效, 字段照写进 Redis</li>
     * </ul>
     * <b>不拦 @JsonCreator 工厂方法/构造器</b> (参数级 @JsonProperty 是 Jackson 映射构造参数的标准必需用法,
     * 参数级注解不被 {@code method.isAnnotationPresent} 命中; 排除 @JsonCreator 方法本身)。
     *
     * @param type 反序列化目标类型
     */
    private static void validateNoMethodLevelJacksonAnnotations(Class<?> type) {
        Class<?> c = type;
        while (c != null && c != Object.class) {
            for (Method m : c.getDeclaredMethods()) {
                if (m.isAnnotationPresent(JsonCreator.class)) continue;
                if (m.isAnnotationPresent(JsonProperty.class)) {
                    throw new RediSearchException("@JsonProperty on method '" + c.getSimpleName() + "#" + m.getName()
                            + "' is not supported — this component only reads field-level @JsonProperty. "
                            + "Move it to the field, otherwise the index alias / JSONPath would diverge from the actual JSON field name.");
                }
                if (m.isAnnotationPresent(JsonIgnore.class)) {
                    throw new RediSearchException("@JsonIgnore on method '" + c.getSimpleName() + "#" + m.getName()
                            + "' is not supported — this component only honors field-level @JsonIgnore. "
                            + "Move it to the field, otherwise the field would still be indexed (empty in JSON mode) / written to HASH.");
                }
            }
            c = c.getSuperclass();
        }
    }

    /**
     * 业务作用：要求 schema-only 字段（@JsonIgnore + 索引注解，stored=false）显式声明 JSONPath（注解 name 以 "$." 开头）。
     * 否则默认 path 是 $.&lt;字段名&gt;, 但该字段被 @JsonIgnore 不进 JSON → 索引指向不存在的路径, 启动成功却永远查不到。
     * schema-only 的正当语义是"为另一个真实 JSON 路径建索引", 故强制显式 path, 不允许默认指向被 ignore 的字段本身。
     *
     * @param annoName 见上述说明
     * @param f        见上述说明
     * @param dataType 见上述说明
     */
    private static void requireSchemaOnlyExplicitPath(String annoName, Field f, DataType dataType) {
        // HASH 模式: schema 只用 redisName 不用 jsonPath, 且 stored=false 不写 HASH field → 索引必空, 无意义. 直接拒。
        if (dataType == DataType.HASH) {
            throw new RediSearchException("@JsonIgnore + 索引注解 (schema-only) field '"
                    + f.getDeclaringClass().getSimpleName() + "#" + f.getName()
                    + "' is not supported in HASH mode — HASH schema 用 hash field 名不用 JSONPath, 且该字段不写 HASH, 索引必为空。"
                    + "schema-only 仅 JSON / JSON_ARRAY / JSON_ARRAY_BUCKET 模式有效。");
        }
        // JSON 模式: 必须显式 JSONPath (name 以 "$." 开头) 指向真实 JSON 数据; 默认 $.<字段名> 因字段被 ignore 不进 JSON → 空索引。
        if (annoName == null || !annoName.startsWith("$.")) {
            throw new RediSearchException("@JsonIgnore + 索引注解 (schema-only) field '"
                    + f.getDeclaringClass().getSimpleName() + "#" + f.getName()
                    + "' must declare an explicit JSONPath via name=\"$.x.y\" pointing at real JSON data — "
                    + "否则该字段被 @JsonIgnore 不进 JSON, 默认 $." + f.getName() + " 索引为空, 查不到。");
        }
    }

    /**
     * 业务作用：#P2: 元数据解析阶段对框架识别的 enum 字段触发一次 @JsonValue accessor 解析, 提前暴露非法 @JsonValue 形态
     * (带参/void/static 方法、多个 @JsonValue 等) —— 否则这些 enum 用法 (@RsId / @JsonArrayKey / placeholder / 非 TagField 索引)
     * 启动建索引都成功, 首次 save/find/key 计算走 renderValue 才炸。jsonValueAccessor 结果缓存, 重复调用极廉价。
     *
     * @param ft 见上述说明
     */
    private static void validateEnumJsonValue(Class<?> ft) {
        if (ft != null && ft.isEnum()) {
            jsonValueAccessor(ft);
        }
    }

    /**
     * 业务作用：把一个字段上的索引注解解析成字段元信息。
     *
     * @param f        待解析的字段
     * @param stored   该字段是否随文档存储
     * @param dataType 所属实体的存储模式
     * @return 字段元信息。
     */
    private static FieldMeta parseFieldAnnotation(Field f, boolean stored, DataType dataType) {
        // 任意被索引注解识别的 enum 字段 (不只 @TagField), 解析阶段先触发 @JsonValue 校验
        validateEnumJsonValue(f.getType());
        if (f.isAnnotationPresent(TagField.class)) {
            TagField a = f.getAnnotation(TagField.class);
            Names n = names(a.name(), f);
            if (!stored) requireSchemaOnlyExplicitPath(a.name(), f, dataType);
            // enum 字段 + @JsonValue 返回数字 (如 SerialEnum 走 ordinal): JSON 里实际是 integer,
            // RediSearch TAG schema 期望 string, 类型不匹配会导致整索引 indexing failure.
            // 自动转 NUMERIC schema; Criteria query 端联动走 numStr 内 enum 分支取 @JsonValue 值.
            if (numericJsonValueAccessor(f.getType()) != null) {
                return new FieldMeta(f, FieldType.NUMERIC, n.alias(), n.path(),
                        a.sortable(), 1.0, false, false, false, ",", "", stored);
            }
            return new FieldMeta(f, FieldType.TAG, n.alias(), n.path(),
                    a.sortable(), 1.0, false, false, a.caseSensitive(), a.separator(), "", stored);
        }
        if (f.isAnnotationPresent(NumericField.class)) {
            NumericField a = f.getAnnotation(NumericField.class);
            Names n = names(a.name(), f);
            if (!stored) requireSchemaOnlyExplicitPath(a.name(), f, dataType);
            // 手写 @NumericField + enum: NUMERIC 索引建在数字上, 但 enum 写入是 name()/字符串型 @JsonValue (HASH renderEnum / JSON Jackson),
            // schema 与写入值类型不符 → 写得进查不到, 且 numStr 查询端没数字 @JsonValue 时无从取数。必须有数字型 @JsonValue, 否则 fail-fast。
            if (f.getType().isEnum() && numericJsonValueAccessor(f.getType()) == null) {
                throw new RediSearchException("@NumericField on enum '" + f.getDeclaringClass().getSimpleName() + "#" + f.getName()
                        + "' requires a numeric @JsonValue (返回数字的方法或字段). 否则 enum 写入是字符串、schema 是 NUMERIC, 写得进查不到。"
                        + "改用 @TagField (无数字 @JsonValue 时), 或给 enum 加数字型 @JsonValue。");
            }
            return new FieldMeta(f, FieldType.NUMERIC, n.alias(), n.path(),
                    a.sortable(), 1.0, false, a.noIndex(), false, ",", "", stored);
        }
        if (f.isAnnotationPresent(TextField.class)) {
            TextField a = f.getAnnotation(TextField.class);
            Names n = names(a.name(), f);
            if (!stored) requireSchemaOnlyExplicitPath(a.name(), f, dataType);
            return new FieldMeta(f, FieldType.TEXT, n.alias(), n.path(),
                    a.sortable(), a.weight(), a.noStem(), false, false, ",", a.phonetic(), stored);
        }
        if (f.isAnnotationPresent(GeoField.class)) {
            GeoField a = f.getAnnotation(GeoField.class);
            Names n = names(a.name(), f);
            if (!stored) requireSchemaOnlyExplicitPath(a.name(), f, dataType);
            return new FieldMeta(f, FieldType.GEO, n.alias(), n.path(),
                    a.sortable(), 1.0, false, false, false, ",", "", stored);
        }
        return null;
    }

    /**
     * 缓存 enum class → 标了 {@link JsonValue} 的访问器 ({@link JsonValueAccessor}, 封装方法型或字段型, 用于 prefix 占位符渲染 /
     * schema 推导 / Criteria query 序列化). 没找到时缓存 {@link #ABSENT} 哨兵, 避免反复反射. 扫 declared 方法 + 字段 + 接口 default 方法.
     */
    private static final Object ABSENT = new Object();
    /**
     * 与主 {@link #CACHE} 一致使用 Caffeine weakKeys + maximumSize，并由 {@link #clear()} 一并清理。
     * 同主 CACHE: value ({@code JsonValueAccessor} 持 Method/Field) 强引用其 declaring class, weakKeys 不足以即时回收, 实际靠 maximumSize + clear()。
     * size 上限与显式 clear() 共同避免缓存条目无界堆积。
     */
    private static final Cache<Class<?>, Object> ENUM_JSONVALUE_CACHE = Caffeine.newBuilder()
            .weakKeys()
            .maximumSize(4096)
            .build();

    /**
     * 业务作用：取 enum 类型上标了 {@code @JsonValue} 的访问器 (方法型或字段型, 任意值类型); 非 enum 或没找到返 null.
     * <p>
     * 用于让 prefix 占位符 / Criteria query / schema 推导跟 Jackson 实际序列化路径对齐 —
     * Jackson 序列化 enum 时若有 @JsonValue 会优先用它 (方法返回值 / 字段值), 否则走 enum.name() (默认配置).
     * 框架对 enum 的字符串化必须遵循同一路径, 否则跟 JSON 内字段值 / RediSearch 索引值不一致.
     * <p>
     * 扫描顺序: enum class 自身 declared 方法 + 字段先看 (业务覆盖优先, 同类多 @JsonValue fail-fast) → 实现的 interface default 方法
     * (覆盖 SerialEnum 这类公共接口 @JsonValue). 不扫 superclass (enum 无 superclass 概念).
     *
     * @param enumType 候选 enum 类型 (非 enum 直接返 null)
     * @return @JsonValue 访问器 (方法型或字段型); 没有返 null
     */
    public static JsonValueAccessor jsonValueAccessor(Class<?> enumType) {
        if (enumType == null || !enumType.isEnum()) return null;
        Object cached = ENUM_JSONVALUE_CACHE.get(enumType, k -> {
            JsonValueAccessor acc = findJsonValue(k);
            return acc == null ? ABSENT : acc;
        });
        return cached == ABSENT ? null : (JsonValueAccessor) cached;
    }

    /**
     * 业务作用：取 enum 上数字型 @JsonValue 的访问器（返回类型或字段类型是数字），用于处理
     * "enum 走 @JsonValue ordinal 但 RediSearch @TagField schema 期望 string" 的类型不匹配：
     * 检测到数字 @JsonValue 时, parseFieldAnnotation 把 @TagField 自动转 NUMERIC schema,
     * Criteria.numStr 端也调本方法把 enum 入参转成数字值进 NUMERIC range 表达式.
     *
     * @param enumType 见上述说明
     */
    public static JsonValueAccessor numericJsonValueAccessor(Class<?> enumType) {
        JsonValueAccessor acc = jsonValueAccessor(enumType);
        return (acc != null && acc.returnsNumber()) ? acc : null;
    }

    /**
     * 业务作用：enum 的全框架统一渲染: 有 {@code @JsonValue} (方法或字段) 取真实值 (与 Jackson JSON 序列化一致), 否则 {@link Enum#name()}。
     * <p>
     * HASH 写入、TAG 查询、key 占位段、JSON_ARRAY id filter、bucket 计算全部复用本方法, 保证同一 enum 在
     * 索引值 / 查询值 / key 值 / JSON 值之间一致, 避免分裂导致写得进查不到。
     * 走 {@link Enum#getDeclaringClass()} 而非 {@code getClass()}: 匿名 enum 常量子类 isEnum()==false 会拿不到 @JsonValue。
     *
     * @param e 见上述说明
     * @return 见上述说明。
     */
    public static String renderEnum(Enum<?> e) {
        JsonValueAccessor acc = jsonValueAccessor(e.getDeclaringClass());
        if (acc == null) return e.name();
        Object raw = acc.get(e);
        return raw == null ? e.name() : String.valueOf(raw);
    }

    /**
     * 业务作用：通用值→持久化字符串: enum 走 {@link #renderEnum}, 其它走 {@code String.valueOf}。
     * 用于 key 段 / TAG 查询等"把任意字段值渲染成与持久化一致字符串"的场景。
     *
     * @param v 见上述说明
     * @return 见上述说明。
     */
    public static String renderValue(Object v) {
        return v instanceof Enum<?> e ? renderEnum(e) : String.valueOf(v);
    }

    /**
     * 业务作用：enum 反序列化, 与 {@link #renderEnum} 对称: 有 {@code @JsonValue} 时按其渲染值匹配, 否则按 {@link Enum#name()}。
     *
     * @param enumType 见上述说明
     * @param raw      见上述说明
     * @return 见上述说明。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Enum<?> parseEnum(Class<?> enumType, String raw) {
        JsonValueAccessor acc = jsonValueAccessor(enumType);
        if (acc != null) {
            for (Object c : enumType.getEnumConstants()) {
                Enum<?> e = (Enum<?>) c;
                Object val = acc.get(e);
                if (val != null && String.valueOf(val).equals(raw)) return e;
            }
        }
        return Enum.valueOf((Class<Enum>) enumType, raw);
    }

    /**
     * 业务作用：找 enum 的 @JsonValue 访问器, 支持方法型 + 字段型 (Jackson 两种合法写法), 取值前 setAccessible 支持非 public。
     * <p>
     * 扫描: enum class 自身 declared 方法 + 字段 (业务覆盖优先) → 实现的 interface default 方法 (覆盖 SerialEnum 公共接口)。
     * 同一 class 上出现多个 @JsonValue (方法/字段) 启动期 fail-fast, 避免框架与 Jackson 选择规则不一致。
     *
     * @param type 待扫描的实体类型
     * @return 见上述说明。
     */
    private static JsonValueAccessor findJsonValue(Class<?> type) {
        Method foundMethod = null;
        Field foundField = null;
        int count = 0;
        for (Method m : type.getDeclaredMethods()) {
            if (m.isAnnotationPresent(JsonValue.class)) {
                foundMethod = m;
                count++;
            }
        }
        for (Field f : type.getDeclaredFields()) {
            if (f.isAnnotationPresent(JsonValue.class)) {
                foundField = f;
                count++;
            }
        }
        if (count > 1) {
            throw new RediSearchException("Multiple @JsonValue on enum " + type.getName()
                    + " — Jackson 只允许一个, 框架同样要求唯一, 请删到只剩一个");
        }
        if (foundMethod != null) return JsonValueAccessor.of(foundMethod);
        if (foundField != null) return JsonValueAccessor.of(foundField);
        return findInInterfaces(type.getInterfaces());
    }

    /**
     * 业务作用：递归扫接口及其 superinterface 链 (BFS): 业务可能 {@code MyEnum implements ChildSerial},
     * {@code ChildSerial extends SerialEnum}, @JsonValue 在 superinterface 上 — 不递归会漏.
     * 用迭代而非递归避免极端深继承栈深; visited set 防接口菱形继承重复扫.
     *
     * @param ifaces 见上述说明
     * @return 见上述说明。
     */
    private static JsonValueAccessor findInInterfaces(Class<?>[] ifaces) {
        if (ifaces.length == 0) return null;
        Deque<Class<?>> queue = new ArrayDeque<>(ifaces.length);
        Set<Class<?>> visited = new HashSet<>();
        for (Class<?> i : ifaces) {
            if (visited.add(i)) queue.offer(i);
        }
        // 收集所有接口上的 @JsonValue 方法, 多于 1 个 fail-fast (与 class 自身的唯一性校验一致, 避免框架与 Jackson 选择规则分歧)
        Method found = null;
        while (!queue.isEmpty()) {
            Class<?> i = queue.poll();
            for (Method m : i.getDeclaredMethods()) {
                if (m.isAnnotationPresent(JsonValue.class)) {
                    if (found != null) {
                        throw new RediSearchException("Multiple @JsonValue on interfaces of enum hierarchy ("
                                + found + " 与 " + m + ") — 只允许一个");
                    }
                    found = m;
                }
            }
            for (Class<?> sup : i.getInterfaces()) {
                if (visited.add(sup)) queue.offer(sup);
            }
        }
        return found == null ? null : JsonValueAccessor.of(found);
    }

    /**
     * 业务作用：解析字段在 JSON 文档中的实际字段名: 优先取 {@link JsonProperty#value()}, fallback 对象字段名.
     * <p>
     * 用于让框架的 JSONPath filter / RediSearch alias 与 Jackson 序列化输出的 JSON 字段名对齐 — 业务侧挂
     * {@code @JsonProperty("docId")} 时, JSON 实际字段 "docId", 本组件也用 "docId" 做 filter,
     * 避免静默 "找不到字段". {@code @JsonAlias} 只影响反序列化输入, 不参与解析.
     *
     * @param f 见上述说明
     * @return 见上述说明。
     */
    private static String resolveJacksonName(Field f) {
        JsonProperty jp = f.getAnnotation(JsonProperty.class);
        if (jp != null && !jp.value().isEmpty()) return jp.value();
        return f.getName();
    }

    /**
     * 业务作用：解析注解的 name 属性, 计算 alias 和 jsonPath:
     * <pre>
     *   注解 name = ""              → alias = Jackson名,   path = $.{Jackson名}
     *   注解 name = "foo"           → alias = "foo",       path = $.foo
     *   注解 name = "$.extra.bar"   → alias = Jackson名,   path = $.extra.bar
     * </pre>
     * Jackson 名 = 字段上 {@link JsonProperty#value()} 非空时取该值, 否则 对象字段名. 这样保证
     * 业务挂 @JsonProperty("docId") 时 本组件的 FT.SEARCH alias / JSONPath filter 与 JSON 输出对齐.
     * JSON 嵌套字段务必用第三种形式: name 显式写 JSONPath, alias 自动用 Jackson 名(不含点).
     *
     * @param annoName 见上述说明
     * @param f        见上述说明
     * @return 见上述说明。
     */
    private static Names names(String annoName, Field f) {
        Names n;
        if (annoName.isEmpty()) {
            // 适配 Jackson @JsonProperty rename: alias 与 jsonPath 都对齐 JSON 实际字段名,
            // 无 @JsonProperty 时使用对象字段名。
            String jName = resolveJacksonName(f);
            n = new Names(jName, "$." + jName);
        } else if (annoName.startsWith("$.")) {
            // 显式 JSONPath: alias 仍取 Jackson 名 (避免点号 alias 被 FT.SEARCH 解析失败,
            // 同时跟空 name 默认分支保持一致 — 业务只想覆盖 path 时 alias 不被静默"退化"成 对象字段名)
            n = new Names(resolveJacksonName(f), annoName);
        } else {
            // 注解 name 显式非 JSONPath: 业务明确意图, 完全 override Jackson, alias 和 path 都用注解 name
            n = new Names(annoName, "$." + annoName);
        }
        // alias 会直接进入 FT 查询、RETURN、SORTBY 和 JSONPath filter，必须在启动期限制字符以防破坏语法或形成注入。
        validateAlias(n.alias(), f);
        return n;
    }

    /**
     * 业务作用：校验 RediSearch alias / hash field 名，只允许 {@code [A-Za-z_][A-Za-z0-9_]*}。
     * 显式 JSONPath ({@code name="$.x.y"}) 走 path 不受此限, 但其 alias (Jackson 名/字段名) 仍须合法。
     *
     * @param alias 见上述说明
     * @param f     见上述说明
     */
    private static void validateAlias(String alias, Field f) {
        boolean ok = !alias.isEmpty();
        if (ok) {
            char c0 = alias.charAt(0);
            ok = (c0 == '_' || (c0 >= 'A' && c0 <= 'Z') || (c0 >= 'a' && c0 <= 'z'));
            for (int i = 1; ok && i < alias.length(); i++) {
                char c = alias.charAt(i);
                ok = (c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'));
            }
        }
        if (!ok) {
            throw new RediSearchException("Illegal RediSearch alias '" + alias + "' for field '"
                    + f.getDeclaringClass().getSimpleName() + "#" + f.getName()
                    + "' — alias must match [A-Za-z_][A-Za-z0-9_]* (它直接拼进 FT 查询/RETURN/SORTBY, 特殊字符会破坏语法). "
                    + "用 @JsonProperty / 注解 name 指定合法 alias, 或显式 JSONPath name=\"$.x\" 单独覆盖 path。");
        }
    }
}
