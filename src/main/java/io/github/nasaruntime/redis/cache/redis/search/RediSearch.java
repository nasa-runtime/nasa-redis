package io.github.nasaruntime.redis.cache.redis.search;

import io.github.nasaruntime.core.base.RecycleLinkedList;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.redis.cache.redis.LettucePipeline;
import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import io.github.nasaruntime.redis.cache.redis.search.annotation.DataType;
import io.github.nasaruntime.redis.cache.redis.search.convert.DefaultRsConverter;
import io.github.nasaruntime.redis.cache.redis.search.convert.RsConverter;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import io.github.nasaruntime.redis.cache.redis.search.executor.RsCommandExecutor;
import io.github.nasaruntime.redis.cache.redis.search.executor.SpringDataRsCommandExecutor;
import io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta;
import io.github.nasaruntime.redis.cache.redis.search.meta.FieldMeta;
import io.github.nasaruntime.redis.cache.redis.search.meta.MetaResolver;
import io.github.nasaruntime.redis.cache.redis.search.query.AggregationResult;
import io.github.nasaruntime.redis.cache.redis.search.query.JsonPaths;
import io.github.nasaruntime.redis.cache.redis.search.query.RsAggregation;
import io.github.nasaruntime.redis.cache.redis.search.query.RsQuery;
import io.github.nasaruntime.core.utils.ColUtils;
import io.github.nasaruntime.core.utils.ContextUtils;
import io.github.nasaruntime.core.utils.MapUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nasa
 * {@link RediSearchOperations} 默认实现。
 *
 * <h2>支持的 Jackson 注解 (仅以下 4 个生效, 其它一律忽略)</h2>
 * <ul>
 *   <li>{@code @JsonProperty("name")}: 字段 rename. 同时影响序列化输出 (JSON / HASH field) 与反序列化输入,
 *       框架内部 RediSearch alias / JSONPath filter 也一并对齐到 rename 后的名字。<b>仅识别字段级 @JsonProperty</b>:
 *       标在 getter/setter/@JsonCreator 参数上的 @JsonProperty 框架不感知, 启动期会 fail-fast (见 MetaResolver)。
 *       <b>但不支持全局命名策略</b> (如 ObjectMapper 配 SNAKE_CASE): 框架按 对象字段名 / 字段级 @JsonProperty 推 schema,
 *       拿不到 ObjectMapper 的 naming strategy, <b>不会 fail-fast</b>; 若业务配了全局策略, JSON 写出的字段名会与 schema/query 分裂
 *       (写得进查不到)。需要 rename 时一律用字段级 @JsonProperty, 不要依赖全局命名策略</li>
 *   <li>{@code @JsonIgnore}: 默认字段不进 RediSearch 索引、不进 byJVM/byRedis、不参与 HSET/JSON 序列化.
 *       仍保留 {@code @RsId} / {@code @JsonArrayKey} 识别 (id/key 字段可不进 JSON, 业务自己从 Redis key 拼回)。
 *       <b>例外</b>: {@code @JsonIgnore} 叠加索引注解 (@TagField/@NumericField/@TextField/@GeoField) + 显式 JSONPath name 时,
 *       视为 <b>schema-only 字段</b> — 进 FT.CREATE schema + 查询 meta (可 {@code Criteria.where} 查), 但不写 HASH 不反序列化赋值;
 *       典型: {@code @JsonIgnore @NumericField(name="$.extra.vip") int vip} 声明指向嵌套数据的索引, 其值由别的非 ignore 字段写进 JSON。
 *       {@code @RsId + @JsonIgnore} 仍 fail-fast (id 必须 round-trip)</li>
 *   <li>{@code @JsonCreator}: 反序列化走业务静态工厂方法 (典型用法: 工厂内 {@code pool.get()} 接入对象池).
 *       HASH 模式 mapToEntity 启动期探测一次, 有则走 Jackson convertValue, 无则走 newInstance + 字段 set;
 *       JSON / JSON_ARRAY 模式天然走 Jackson readValue, 业务标了自动生效</li>
 *   <li>enum {@code @JsonValue}: enum 在 JSON 值 / HASH 值 / 查询值 / key 动态段 / JSON_ARRAY filter / bucket 计算里
 *       全部按 {@code @JsonValue} 渲染 (无则 {@code name()}), 经 {@code MetaResolver.renderEnum} 统一, 保证各路径一致</li>
 * </ul>
 * 其它 Jackson 注解 (例 {@code @JsonAlias} / {@code @JsonIgnoreProperties} / {@code @JsonInclude} 等) 不被框架特殊处理,
 * 业务自行评估是否还能通过 Jackson 默认行为间接生效, 不要依赖框架提供任何兼容承诺.
 */
@SuppressWarnings({"unused", "ConstantConditions"})
@Slf4j
public class RediSearch implements RediSearchOperations {

    /**
     * saveAll pipeline 内 JSON.SET 的 root path "$" 预序列化 byte[].
     */
    public static final byte[] DOLLAR_PATH = "$".getBytes(StandardCharsets.UTF_8);

    static final Map<RedisProxy, RediSearch> CACHE = new ConcurrentHashMap<>();

    /**
     * 业务作用：为一个命令代理登记检索入口，使该实例上的检索能力可用。
     *
     * @param redisProxy 承载检索命令的命令代理
     * 返回: 无返回值。
     */
    public static void initialize(RedisProxy redisProxy) {
        CACHE.put(redisProxy, new RediSearch(redisProxy));
    }

    /**
     * 业务作用：取某个命令代理对应的检索入口。
     *
     * @param redisProxy 命令代理
     * @return 该代理的检索入口；未登记时为 null。
     */
    public static RediSearch load(RedisProxy redisProxy) {
        return CACHE.get(redisProxy);
    }

    /**
     * 业务作用：按实例名取检索入口，供多套 Redis 共存时按名选取。
     *
     * @param qualifier 实例名
     * @return 该实例的检索入口；未登记时为 null。
     */
    public static RediSearch load(String qualifier) {
        return load(RedisProxy.load(qualifier));
    }

    /**
     * 业务作用：从 CACHE 移除并释放某 {@link io.github.nasaruntime.redis.cache.redis.RedisProxy} 对应的 RediSearch 实例，返回被移除的实例（无则 null）。
     * <p>
     * CACHE 是 static 强引用 map：RedisProxy 在热加载或多租户动态配置中反复创建销毁时，不移除会让
     * 旧 RediSearch + RedisProxy 一直被强引用无法 GC (内存泄漏)。RedisProxy 生命周期结束时应调本方法。
     *
     * @param redisProxy 命令代理，决定连接与序列化方式
     */
    public static RediSearch destroy(RedisProxy redisProxy) {
        return CACHE.remove(redisProxy);
    }

    /**
     * 业务作用：清空整个 CACHE，供应用关停或全量重载时释放所有缓存实例的强引用。
     */
    public static void destroyAll() {
        CACHE.clear();
        // 同时清 MetaResolver 的 EntityMeta / enum @JsonValue 两级缓存 (它们强引用 Class/Field/Method, 见 MetaResolver 注释):
        // 热重载或卸载时只清 CACHE 不够，否则已卸载的 Class 仍会被 metadata 缓存强引用。
        MetaResolver.clear();
    }

    private final RedisProxy redisProxy;
    private final StringRedisTemplate redisTemplate;
    private volatile RsConverter converter;
    private volatile RsCommandExecutor executor;
    /**
     * JSON_ARRAY 模式专属操作视图. 懒加载: 单文档模式业务不会触发, 不创建多余对象.
     */
    private volatile JsonArraySupport jsonArrayOps;
    /**
     * Class → Actuator 视图缓存. 业务侧通过 {@link #actuator(Class)} 拿到针对单一 entity 类型绑定的 Actuator
     */
    private final ConcurrentHashMap<Class<?>, Actuator<?>> actuators = new ConcurrentHashMap<>();

    /**
     * 业务作用：绑定检索入口与其命令代理，检索命令、序列化方式与连接都由该代理决定。
     *
     * @param redisProxy 承载检索命令的命令代理
     */
    private RediSearch(RedisProxy redisProxy) {
        this.redisProxy = redisProxy;
        this.redisTemplate = new StringRedisTemplate(this.redisProxy.getRedisTemplate().getConnectionFactory());
    }

    /**
     * 业务作用：取本入口使用的命令执行器，惰性解析并缓存。
     * 执行器可被业务替换，用于对接不同的检索命令实现。
     *
     * <p>参数说明: 无。
     *
     * @return 命令执行器。
     */
    RsCommandExecutor executor() {
        RsCommandExecutor local = this.executor;
        if (local != null) return local;
        synchronized (this) {
            local = this.executor;
            if (local != null) return local;
            Map<String, RsCommandExecutor> map = ContextUtils.getBeansOfType(RsCommandExecutor.class);
            if (MapUtils.isEmpty(map)) {
                return this.executor = new SpringDataRsCommandExecutor(this.redisTemplate);
            }
            // 精确
            for (RsCommandExecutor rce : map.values()) {
                String[] qualifiers = rce.qualifiers();
                if (ColUtils.isEmpty(qualifiers)) continue;
                for (String qualifier : qualifiers) {
                    if (RedisProxy.load(qualifier) == this.redisProxy) return this.executor = rce;
                }
            }
            // 第一个空
            for (RsCommandExecutor rce : map.values()) {
                String[] qualifiers = rce.qualifiers();
                if (ColUtils.isEmpty(qualifiers)) return this.executor = rce;
            }
            // 兜底
            return this.executor = new SpringDataRsCommandExecutor(this.redisTemplate);
        }
    }

    /**
     * 业务作用：取本入口使用的取值转换器，惰性解析并缓存。
     * 转换器负责实体字段与检索文档字段之间的互转，业务可替换以支持自定义类型。
     *
     * <p>参数说明: 无。
     *
     * @return 取值转换器。
     */
    RsConverter converter() {
        RsConverter local = this.converter;
        if (local != null) return local;
        synchronized (this) {
            local = this.converter;
            if (local != null) return local;
            Map<String, RsConverter> map = ContextUtils.getBeansOfType(RsConverter.class);
            if (MapUtils.isEmpty(map)) {
                return this.converter = new DefaultRsConverter();
            }
            // 精确
            for (RsConverter rc : map.values()) {
                String[] qualifiers = rc.qualifiers();
                if (ColUtils.isEmpty(qualifiers)) continue;
                for (String qualifier : qualifiers) {
                    if (RedisProxy.load(qualifier) == this.redisProxy) return this.converter = rc;
                }
            }
            // 第一个空
            for (RsConverter rc : map.values()) {
                String[] qualifiers = rc.qualifiers();
                if (ColUtils.isEmpty(qualifiers)) return this.converter = rc;
            }
            // 兜底
            return this.converter = new DefaultRsConverter();
        }
    }

    /**
     * 业务作用：取 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY} 模式专属操作视图. 双重检查懒加载, 业务从不用 ARRAY 模式时不会创建实例.
     *
     * @return 见上述说明。
     */
    public JsonArrayOperations jsonArrayOps() {
        JsonArraySupport local = this.jsonArrayOps;
        if (local != null) return local;
        synchronized (this) {
            local = this.jsonArrayOps;
            if (local != null) return local;
            return this.jsonArrayOps = new JsonArraySupport(this.redisProxy, this.executor(), this.converter());
        }
    }

    // ============ entity → byte[] helper (给 LettucePipeline.Actuator entity 重载用) ============

    /**
     * 业务作用：算 entity 的 Redis key byte[]: 走 {@link io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta#keyOf(Object)} 自动处理 prefix 占位符 (HASH+JSON 共用入口).
     * 给 {@link io.github.nasaruntime.redis.cache.redis.LettucePipeline.Actuator#jsonSetAsync(Object)} 等 entity 重载用.
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    public byte[] keyBytes(Object entity) {
        EntityMeta meta = MetaResolver.resolve(entity.getClass());
        return meta.keyOf(entity).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：算 (type, id) 对应的 Redis key byte[]; 给"知道 type + id 但没 entity 实例"的撤单/部分成交路径用.
     * <p>
     * 仅无占位符模式可用; 占位符模式下抛错引导 caller 用 {@link #keyBytes(Class, Object...)} 传 (placeholder values + @RsId).
     *
     * @param type 反序列化目标类型
     * @param id   条目标识
     * @return 见上述说明。
     */
    public byte[] keyBytes(Class<?> type, Object id) {
        EntityMeta meta = MetaResolver.resolve(type);
        if (id == null) throw new RediSearchException("id is null");
        // enum @RsId 走 renderValue (@JsonValue/name) 与 save 的 keyOf 一致, 不能 String.valueOf 否则 enum 打到另一个 key
        return meta.key(MetaResolver.renderValue(id)).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：vararg 算 key byte[] — 占位符模式撤单 / 局部更新路径用 (业务知道 placeholder 字段值 + @RsId 值但没 entity 实例).
     * <p>
     * parts 顺序: placeholder 字段值按 prefix 占位符出现顺序 + @RsId 值放末尾. 详见 {@link EntityMeta#keyOf(Object...)}.
     *
     * @param type  反序列化目标类型
     * @param parts 见上述说明
     * @return 见上述说明。
     */
    public byte[] keyBytes(Class<?> type, Object... parts) {
        EntityMeta meta = MetaResolver.resolve(type);
        return meta.keyOf(parts).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：把 entity 序列化为 JSON byte[]: {@code converter.writeJson(entity)} → UTF-8 字节.
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    public byte[] jsonBytes(Object entity) {
        return this.converter().writeJson(entity).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：构造 HASH 模式实体的写计划: 已 UTF-8 编码的 keyBytes + hashBytes + toDeleteBytes 视图,
     * 给 {@link io.github.nasaruntime.redis.cache.redis.LettucePipeline.Actuator#hashSaveAsync(Object)} 等 entity 重载用.
     * <p>
     * 仅适用 {@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#HASH}, 其他 mode 调用抛 {@link io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException}.
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    public EntityWriteOp buildHashOp(Object entity) {
        EntityMeta meta = MetaResolver.resolve(entity.getClass());
        if (meta.dataType() != DataType.HASH) {
            throw new RediSearchException("buildHashOp requires DataType.HASH, got " + meta.dataType()
                    + " on " + meta.type().getSimpleName());
        }
        return this.buildWriteOp(entity, meta);
    }

    /**
     * 业务作用：取 entity 在 ARRAY/BUCKET 模式下的 Redis key byte[].
     * 给 {@link io.github.nasaruntime.redis.cache.redis.LettucePipeline.Actuator#jsonArraySaveAsync} 等用.
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    public byte[] arrayKeyBytes(Object entity) {
        EntityMeta meta = MetaResolver.resolve(entity.getClass());
        if (meta.dataType() != DataType.JSON_ARRAY && meta.dataType() != DataType.JSON_ARRAY_BUCKET) {
            throw new RediSearchException("arrayKeyBytes requires JSON_ARRAY / JSON_ARRAY_BUCKET, got "
                    + meta.dataType() + " on " + meta.type().getSimpleName());
        }
        return meta.arrayKey(entity).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：构造 entity 的 @RsId DEL filter: {@code $[?(@.<idName>==<literal>)]}, 给 ARRAY 模式 saveOrReplace 的 LUA 用.
     * <p>
     * idName 取 entity {@link io.github.nasaruntime.redis.cache.redis.search.annotation.RsId @RsId} 字段的 JSON 名 (受 @JsonProperty rename 影响,
     * 与实际 JSON 文档对齐). literal 走 {@link io.github.nasaruntime.redis.cache.redis.search.query.JsonPaths#literal(Object)} 按 id 真实类型渲染 — Number/Boolean 直出,
     * String 加双引号转义 — 避免 {@code Long → "12345"} 字符串字面量跟 JSON 文档里 number 字段比较失败.
     *
     * @param entity 见上述说明
     * @return 见上述说明。
     */
    public String idDelFilter(Object entity) {
        EntityMeta meta = MetaResolver.resolve(entity.getClass());
        Object id = meta.idField().get(entity);
        if (id == null) throw new RediSearchException("@RsId field is null");
        return "$[?(@." + meta.idField().redisName() + "==" + JsonPaths.literal(id) + ")]";
    }

    // ============ 索引管理 ============

    /**
     * 业务作用：确保实体对应的索引已存在，不存在则按其注解声明创建。
     * <p>
     * 索引<b>只对创建之后写入的文档生效</b>：既有文档不会被自动纳入，
     * 因此索引结构变更后需要重建索引并回填数据，否则检索会漏掉历史数据。
     *
     * @param type 实体类型
     * 返回: 无返回值。
     */
    @Override
    public void ensureIndex(Class<?> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        if (indexExists(type)) return;

        RecycleLinkedList<String> args = RecycleLinkedList.of();
        try {
            args.add(meta.index());
            // RediSearch FT.CREATE ON 只接受 HASH 或 JSON, 不认识 JSON_ARRAY; ARRAY 模式底层仍是 JSON 文档
            String onType = meta.dataType() == DataType.HASH ? "HASH" : "JSON";
            args.add("ON");
            args.add(onType);
            args.add("PREFIX");
            args.add("1");
            // 占位符模式 prefix 含 {xxx} 字面, 直接传给 FT.CREATE 永不匹配实际 key, 必须用 literalPrefix 头部
            args.add(meta.literalPrefix());
            args.add("SCHEMA");
            for (FieldMeta fm : meta.fields()) {
                args.addAll(fm.toSchemaArgs(meta.dataType()));
            }
            try {
                this.executor().execute("FT.CREATE", args.toArray(new String[0]));
            } catch (RediSearchException e) {
                // 多实例并发启动 race: indexExists 检查后另一实例先 FT.CREATE 成功, 本实例 FT.CREATE 抛 "Index already exists".
                // 静默忽略 (跟 dropIndex 内 catch "Unknown Index name" 同对偶模式), 其它错误向上抛.
                if (isIndexAlreadyExists(e)) return;
                throw e;
            }
        } finally {
            args.recycle();
        }
    }

    /**
     * 业务作用：是否 RediSearch "索引已存在" 错误信号 (并发 FT.CREATE 的 race 标志).
     *
     * @param t 见上述说明
     * @return 见上述说明。
     */
    private static boolean isIndexAlreadyExists(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("Index already exists") || msg.contains("already in use"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * 业务作用：判断实体对应的索引是否已存在。
     *
     * @param type 实体类型
     * @return 索引已存在返回 true。
     */
    @Override
    public boolean indexExists(Class<?> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        try {
            this.executor().execute("FT.INFO", meta.index());
            return true;
        } catch (RediSearchException e) {
            // 仅 "Unknown Index name" 等 RediSearch 明确"索引不存在"信号才视为不存在,
            // 其它错误(网络/权限/Redis 挂了)向上抛, 避免被吞导致 ensureIndex 紧接着 FT.CREATE 报误诊错误.
            if (isUnknownIndex(e)) return false;
            throw e;
        }
    }

    /**
     * 业务作用：判定一个异常是否为「索引不存在」。
     * 检索实现以异常而非返回值表达这一情形，需按消息特征识别；
     * 识别不出会把一个可恢复的「尚未建索引」当成真正的故障抛给调用方。
     *
     * @param t 待判定的异常
     * @return 属于索引不存在返回 true。
     */
    private static boolean isUnknownIndex(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            // RedisStack: "Unknown Index name" / "no such index";
            // Dragonfly 措辞不同——FT.INFO / FT.DROPINDEX 索引不存在时报 "Index with name '<idx>' not found",
            // 必须一并识别, 否则 indexExists 把它当真错误重抛, ensureIndex 在预检就崩、永远建不了索引(Dragonfly 兼容)。
            if (msg != null && (msg.contains("Unknown Index name") || msg.contains("no such index")
                    || (msg.contains("Index with name") && msg.contains("not found")))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * 业务作用：删除实体对应的索引，保留被索引的文档本身。
     *
     * @param type 实体类型
     * 返回: 无返回值。
     */
    @Override
    public void dropIndex(Class<?> type) {
        dropIndex(type, false);
    }

    /**
     * 业务作用：删除索引，并按需连同被索引的文档一并删除。
     * <p>
     * 连带删除文档是<b>不可逆</b>的：这些文档是业务数据本体，删除后无法从索引恢复。
     * <p>
     * 索引本来就不存在时视为已达成目标而非失败，使重复调用是安全的；
     * 其余错误向上抛出，避免把真正的故障当成幂等命中而静默跳过。
     *
     * @param type            实体类型
     * @param deleteDocuments true 表示连同文档一并删除
     * 返回: 无返回值。
     */
    @Override
    public void dropIndex(Class<?> type, boolean deleteDocuments) {
        EntityMeta meta = MetaResolver.resolve(type);
        try {
            if (deleteDocuments) {
                this.executor().execute("FT.DROPINDEX", meta.index(), "DD");
            } else {
                this.executor().execute("FT.DROPINDEX", meta.index());
            }
        } catch (RediSearchException e) {
            // 跟 ensureIndex 对偶: 索引本来就不存在视为成功 (幂等), 其它错误向上抛
            if (isUnknownIndex(e)) return;
            throw e;
        }
    }

    // ============ CRUD ============

    /**
     * 业务作用：保存一个实体，写入其全部被索引字段。
     * 键由实体的主键与键模板拼出，因此<b>同主键的重复保存是覆盖而非新增</b>。
     *
     * @param entity 待保存的实体
     * @param <T>    实体类型
     * @return 原样返回入参实体，便于串接调用。
     */
    @Override
    public <T> T save(T entity) {
        EntityMeta meta = MetaResolver.resolve(entity.getClass());
        if (isArrayMode(meta)) {
            return jsonArrayOps().save(entity);
        }
        // HASH / JSON 都走 LettucePipeline 单点入口, 跟 saveAll / jsonSetAsync 嵌套语义统一:
        // 外层有 pipeline 时合并到 outer flush, 单独调用时 open+pipeline 内 1 RTT 完成 HMSET+HDEL (或 JSON.SET).
        LettucePipeline.Actuator ac = LettucePipeline.open(this.redisProxy, this);
        try {
            if (meta.dataType() == DataType.JSON) {
                ac.jsonSet(entity);
            } else {
                ac.hashSave(entity);
            }
        } finally {
            ac.pipeline(this);
        }
        return entity;
    }

    /**
     * 业务作用：批量保存实体，全部写入合并进同一个批次一次发出，避免逐条往返。
     * <p>
     * 批次内各条<b>互不构成事务</b>：中途失败时先前各条已经写入且不会回滚。
     *
     * @param entities 待保存的实体集合
     * @param <T>      实体类型
     * @return 原样返回入参实体的列表。
     */
    @Override
    public <T> List<T> saveAll(Iterable<T> entities) {
        // 先判断 batch 是否含 ARRAY/BUCKET: 整批同为 ARRAY 模式走 jsonArrayOps; 混用 ARRAY+非 ARRAY 抛 IAE
        var iter = entities.iterator();
        if (!iter.hasNext()) return new ArrayList<>();
        T first = iter.next();
        if (first == null) throw new RediSearchException("saveAll: entity cannot be null");
        EntityMeta firstMeta = MetaResolver.resolve(first.getClass());
        boolean firstIsArray = isArrayMode(firstMeta);

        RecycleLinkedList<T> all = RecycleLinkedList.of();
        try {
            all.add(first);
            while (iter.hasNext()) {
                T next = iter.next();
                if (next == null) throw new RediSearchException("saveAll: entity cannot be null");
                assertSameMode(firstMeta, first, MetaResolver.resolve(next.getClass()), next, firstIsArray);
                all.add(next);
            }
            if (firstIsArray) return jsonArrayOps().saveAll(all);

            // 非 ARRAY 路径: 走 LettucePipeline 统一管理 (跟 ARRAY saveAll / jsonSetAsync 嵌套语义一致).
            // 业务在外层 LettucePipeline.open() 块内调本方法 → 合并到外层一次 flush; 否则本方法内 open+pipeline 立即 flush.
            List<T> saved = new ArrayList<>();
            LettucePipeline.Actuator ac = LettucePipeline.open(this.redisProxy, this);
            try {
                for (T e : all) {
                    EntityMeta meta = MetaResolver.resolve(e.getClass());
                    if (meta.dataType() == DataType.JSON) {
                        ac.jsonSet(e);
                    } else {
                        ac.hashSave(e);
                    }
                    saved.add(e);
                }
            } finally {
                ac.pipeline(this);
            }
            return saved;
        } finally {
            all.recycle();
        }
    }

    /**
     * 业务作用：把一个实体拆成写入操作：目标键、各字段取值与其存储形态。
     * 在此完成键片段的合法性校验与字段取值的转换，使非法数据在发出命令之前就被拒绝。
     *
     * @param entity 待写入的实体
     * @param meta   该实体的结构元信息
     * @return 组装完毕的写入操作。
     */
    private EntityWriteOp buildWriteOp(Object entity, EntityMeta meta) {
        // meta.keyOf 内部已校验 @RsId 非空 + 处理 prefix 占位符 (HASH+JSON 共用)
        String key = meta.keyOf(entity);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        // 三个 RecycleLinkedList 都是方法局部, 收集完 toArray 后立即 recycle (Node 归节点池, byte[] 引用进数组归 caller).
        // EntityWriteOp 出方法后只持 byte[][] 引用, 跟池化 list 完全脱钩, 安全.
        RecycleLinkedList<byte[]> namesBuf = RecycleLinkedList.of();
        RecycleLinkedList<byte[]> valuesBuf = RecycleLinkedList.of();
        RecycleLinkedList<byte[]> toDelBuf = RecycleLinkedList.of();
        try {
            // storedFields = idField + stored 索引字段: 把 @RsId 当普通 hash field 一起写, 否则 placeholder 模式
            // placeholder key 无法从查询结果反推出 id，因此 id 必须作为 stored field 写入；schema-only 字段 stored=false 不在内。
            for (FieldMeta fm : meta.storedFields()) {
                Object v = fm.get(entity);
                String s = this.converter().write(v, fm);
                byte[] nameBytes = fm.redisName().getBytes(StandardCharsets.UTF_8);
                if (s != null) {
                    namesBuf.add(nameBytes);
                    valuesBuf.add(s.getBytes(StandardCharsets.UTF_8));
                } else {
                    toDelBuf.add(nameBytes);
                }
            }
            return new EntityWriteOp(key, keyBytes,
                    namesBuf.toArray(new byte[0][]),
                    valuesBuf.toArray(new byte[0][]),
                    toDelBuf.toArray(new byte[0][]));
        } finally {
            namesBuf.recycle();
            valuesBuf.recycle();
            toDelBuf.recycle();
        }
    }

    /**
     * 业务作用：按 ID 查询单个实体. 不存在返回 null.
     * <p>
     * HASH 模式语义说明: HGETALL 对"不存在的 key"与"存在但所有 field 都被 HDEL 清空的 key" (此时
     * key 本身也会被 Redis 自动删除) 都返回空 Map, 因此本方法两种情况都返回 null. 业务方若需精确区分
     * 是"从未写过"还是"曾写过但已清空", 请改用 EXISTS + 业务字段判断, 而非依赖此方法的返回值,
     * 避免多 1 次 RTT 的成本.
     *
     * @param id   条目标识
     * @param type 反序列化目标类型
     */
    @Override
    public <T> T findById(String id, Class<T> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "findById");
        // 占位符模式下单 id 信息不足以算出完整 key, 抛错引导 caller 用 findByEntity / findByParts
        rejectPlaceholder(meta, "findById", "findByEntity(template) / findByParts(parts...)");
        if (meta.dataType() == DataType.JSON) {
            return findByIdJson(id, meta, type);
        }
        Map<Object, Object> raw = redisTemplate.opsForHash().entries(meta.key(id));
        if (MapUtils.isEmpty(raw)) return null;
        RecycleLinkedMap<String, String> fields = toStringMap(raw);
        try {
            return mapToEntity(id, fields, meta, type);
        } finally {
            fields.recycle();
        }
    }

    /**
     * 业务作用：findById 的 entity 模板版 — 占位符模式 / 通用版 (entity 已含 placeholder 字段值 + @RsId 值).
     * 业务侧 new 一个空 entity 填上 placeholder 字段 + id, 框架自动算 key 后查询.
     *
     * @param template 见上述说明
     * @param type     反序列化目标类型
     * @return 见上述说明。
     */
    public <T> T findByEntity(T template, Class<T> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "findByEntity");
        String key = meta.keyOf(template);
        // template.id 已业务填好, 作 fallback 回填 id 传给 mapToEntity. enum 走 renderValue (@JsonValue/name)
        // 与 key 渲染一致, 不能 String.valueOf 否则 enum (尤其重写 toString) 兜底回填会转不回去。
        Object id = meta.idField().get(template);
        return findByCookedKey(key, id == null ? null : MetaResolver.renderValue(id), meta, type);
    }

    /**
     * 业务作用：findById 的 vararg 版 — 占位符模式下业务没 entity 实例只有 (placeholder 值 + id) 时用.
     * parts 顺序: placeholder 值按 prefix 占位符顺序 + @RsId 末尾.
     *
     * @param type  反序列化目标类型
     * @param parts 见上述说明
     * @return 见上述说明。
     */
    public <T> T findByParts(Class<T> type, Object... parts) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "findByParts");
        String key = meta.keyOf(parts);
        // parts 末尾是 @RsId 值, 作 fallback 回填. enum 走 renderValue 与 key 一致 (同 findByEntity)
        Object id = parts.length > 0 ? parts[parts.length - 1] : null;
        return findByCookedKey(key, id == null ? null : MetaResolver.renderValue(id), meta, type);
    }

    /**
     * 业务作用：内部 helper: 已经算好完整 key 字符串后走 HASH/JSON 读路径. id 字符串由 caller 从 template/parts 提取传入,
     * 作 fallback 回填 @RsId 字段. 注: HASH 现已把 id 当普通 field 存 (storedFields), mapToEntity 会从 fields 读回 id,
     * 此处 id 仅作 fields 缺 id / JSON id 为空时的兜底; 经 setIdFromString → converter.read 按字段类型转 (enum 走 parseEnum)。
     *
     * @param key  缓存键
     * @param id   条目标识
     * @param meta 见上述说明
     * @param type 反序列化目标类型
     * @return 见上述说明。
     */
    private <T> T findByCookedKey(String key, String id, EntityMeta meta, Class<T> type) {
        if (meta.dataType() == DataType.JSON) {
            List<Object> raw = this.executor().execute("JSON.GET", key);
            if (raw == null || raw.isEmpty()) return null;
            Object first = raw.getFirst();
            if (first == null) return null;
            String json = asString(first);
            if (json == null || json.isEmpty() || "null".equals(json)) return null;
            T entity = this.converter().readJson(json, type);
            if (id != null && meta.idField().get(entity) == null) {
                this.setIdFromString(entity, id, meta);
            }
            return entity;
        }
        Map<Object, Object> raw = redisTemplate.opsForHash().entries(key);
        if (MapUtils.isEmpty(raw)) return null;
        RecycleLinkedMap<String, String> fields = toStringMap(raw);
        try {
            return mapToEntity(id, fields, meta, type);
        } finally {
            fields.recycle();
        }
    }

    /**
     * 业务作用：在 byId / byParts / byEntity 等单 id 路径调用前, 校验 entity 不是占位符模式. 是的话引导 caller 用 entity/vararg API.
     *
     * @param meta        见上述说明
     * @param op          见上述说明
     * @param alternative 见上述说明
     */
    private static void rejectPlaceholder(EntityMeta meta, String op, String alternative) {
        if (meta.hasPlaceholder()) {
            throw new RediSearchException(op + " requires non-placeholder prefix on "
                    + meta.type().getSimpleName() + " — use " + alternative);
        }
    }

    /**
     * 业务作用：校验 batch 内实体的 ARRAY/非ARRAY 模式一致, 不一致直接抛 IAE.
     * 通过单一入口避免两条分支重复 reject 逻辑.
     *
     * @param firstMeta    见上述说明
     * @param first        见上述说明
     * @param nextMeta     见上述说明
     * @param next         见上述说明
     * @param firstIsArray 见上述说明
     */
    private static <T> void assertSameMode(EntityMeta firstMeta, T first,
                                           EntityMeta nextMeta, T next, boolean firstIsArray) {
        if (firstIsArray == isArrayMode(nextMeta)) return;
        throw new RediSearchException("saveAll cannot mix " + firstMeta.dataType()
                + " (" + first.getClass().getSimpleName() + ") and "
                + nextMeta.dataType() + " (" + next.getClass().getSimpleName()
                + ") in the same batch — split into two saveAll calls");
    }

    /**
     * 业务作用：ARRAY / BUCKET 模式下单文档 API 抛 IAE, 引导业务用 {@link #jsonArrayOps()}.
     *
     * @param meta 见上述说明
     * @param op   见上述说明
     */
    private static void rejectArray(EntityMeta meta, String op) {
        if (isArrayMode(meta)) {
            throw new RediSearchException(op + " is not supported for " + meta.dataType() + " on "
                    + meta.type().getSimpleName() + " — use template.jsonArrayOps()." + arrayCounterpart(op));
        }
    }

    /**
     * 业务作用：是否 ARRAY/BUCKET 任一模式. 单文档 API 在这两种模式下都不适用.
     *
     * @param meta 见上述说明
     * @return 见上述说明。
     */
    private static boolean isArrayMode(EntityMeta meta) {
        DataType dt = meta.dataType();
        return dt == DataType.JSON_ARRAY || dt == DataType.JSON_ARRAY_BUCKET;
    }

    /**
     * 业务作用：把一个针对单文档的操作名换成其数组形态的对应操作名。
     * 数组存储模式下同一逻辑操作要走不同的底层命令，在此集中映射，避免在各调用点重复判断。
     *
     * @param op 单文档形态的操作名
     * @return 数组形态的对应操作名。
     */
    private static String arrayCounterpart(String op) {
        return switch (op) {
            case "findById" -> "findSubDoc(arrayKeyParts, subId, type)";
            case "existsById" -> "existsSubDoc(arrayKeyParts, subId, type)";
            case "deleteById" -> "removeSubDoc(arrayKeyParts, subId, type)";
            case "find" -> "findInArray(arrayKeyParts, query, type)";
            case "findOne" -> "findInArray(...).getFirst()";
            case "count" -> "countInArray(arrayKeyParts, query, type)";
            case "remove" -> "removeInArray(arrayKeyParts, query, type)";
            case "findKeys" -> "findInArray(...) 取 id 字段";
            default -> "对应 JsonArrayOperations API";
        };
    }

    /**
     * 业务作用：JSON.GET key 返回单个 JSON 字符串, 不存在返回 null
     *
     * @param id   条目标识
     * @param meta 见上述说明
     * @param type     实体类型，据其键模板定位目标文档
     */
    private <T> T findByIdJson(String id, EntityMeta meta, Class<T> type) {
        List<Object> raw = this.executor().execute("JSON.GET", meta.key(id));
        if (raw == null || raw.isEmpty()) return null;
        Object first = raw.getFirst();
        if (first == null) return null;
        String json = asString(first);
        if (json == null || json.isEmpty() || "null".equals(json)) return null;
        T entity = this.converter().readJson(json, type);
        // 兜底确保 @RsId 字段值正确(JSON 里可能没存 id 字段)
        if (meta.idField().get(entity) == null) {
            this.setIdFromString(entity, id, meta);
        }
        return entity;
    }

    /**
     * 业务作用：按主键查一个实体，以可空容器返回，使调用方不必自行判空。
     *
     * @param id   主键
     * @param type 实体类型
     * @param <T>  实体类型
     * @return 查到的实体；不存在时为空容器。
     */
    @Override
    public <T> Optional<T> findOptionalById(String id, Class<T> type) {
        return Optional.ofNullable(findById(id, type));
    }

    /**
     * 业务作用：按主键判断实体是否存在。
     * 只判存在不取内容，因此比取回后判空省去一次反序列化与网络传输。
     *
     * @param id   主键
     * @param type 实体类型
     * @return 存在返回 true。
     */
    @Override
    public boolean existsById(String id, Class<?> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "existsById");
        rejectPlaceholder(meta, "existsById", "existsByEntity(template) / existsByParts(parts...)");
        // 同步直接调用 EXISTS 协议返 0/1, 必非 null. spring-data-redis 标 @Nullable 是为 pipeline/transaction 模式 fallthrough, 这里不会触达.
        return redisTemplate.hasKey(meta.key(id));
    }

    /**
     * 业务作用：existsById 的 entity 模板版 — 占位符模式 / 通用版.
     *
     * @param template 见上述说明
     * @param type     反序列化目标类型
     * @return 见上述说明。
     */
    public boolean existsByEntity(Object template, Class<?> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "existsByEntity");
        return redisTemplate.hasKey(meta.keyOf(template));
    }

    /**
     * 业务作用：existsById 的 vararg 版 — 占位符模式下传 (placeholder 值 + @RsId 末尾).
     *
     * @param type  反序列化目标类型
     * @param parts 见上述说明
     * @return 见上述说明。
     */
    public boolean existsByParts(Class<?> type, Object... parts) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "existsByParts");
        return redisTemplate.hasKey(meta.keyOf(parts));
    }

    /**
     * 业务作用：按主键删除实体及其索引项。
     *
     * @param id   主键
     * @param type 实体类型
     * @return 删除成功返回 true；实体不存在时返回 false。
     */
    @Override
    public boolean deleteById(String id, Class<?> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "deleteById");
        rejectPlaceholder(meta, "deleteById", "deleteByEntity(template) / deleteByParts(parts...)");
        // 同 existsById: 同步直接调用必非 null.
        return redisTemplate.delete(meta.key(id));
    }

    /**
     * 业务作用：deleteById 的 entity 模板版 — 占位符模式 / 通用版.
     *
     * @param template 见上述说明
     * @param type     反序列化目标类型
     * @return 见上述说明。
     */
    public boolean deleteByEntity(Object template, Class<?> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "deleteByEntity");
        return redisTemplate.delete(meta.keyOf(template));
    }

    /**
     * 业务作用：deleteById 的 vararg 版 — 占位符模式下传 (placeholder 值 + @RsId 末尾).
     *
     * @param type  反序列化目标类型
     * @param parts 见上述说明
     * @return 见上述说明。
     */
    public boolean deleteByParts(Class<?> type, Object... parts) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "deleteByParts");
        return redisTemplate.delete(meta.keyOf(parts));
    }

    // ============ 子字段原子操作 ============

    /**
     * 业务作用：按增量原子调整 JSON 文档中的数值字段。
     * 自增在服务端完成，并发调用不会丢更新。
     *
     * @param type 反序列化目标类型
     * @param id 条目标识
     * @param jsonPath 见方法语义
     * @param delta 增减量
     * 返回: 无返回值。
     */
    @Override
    public void jsonNumIncrBy(Class<?> type, Object id, String jsonPath, long delta) {
        if (id == null) throw new RediSearchException("id is null");
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "jsonNumIncrBy");
        // 占位符模式下单 id 不足以算完整 key, 抛错引导 caller 用 vararg 版 (跟 findById 一致)
        rejectPlaceholder(meta, "jsonNumIncrBy", "jsonNumIncrBy(type, parts, jsonPath, delta) vararg 版");
        // 走 RediSearch 自己的命令执行通道 (跟 FT.CREATE / FT.INFO 等一致), 不依赖 LettucePipeline 上下文.
        // 业务想攒批多笔 NUMINCRBY 时, 用 LettucePipeline.Actuator.jsonNumIncrByAsync 直接走 pipeline 路径.
        this.executor().execute("JSON.NUMINCRBY", meta.key(MetaResolver.renderValue(id)), jsonPath, Long.toString(delta));
    }

    /**
     * 业务作用：按增量原子调整 JSON 文档中的数值字段。
     * 自增在服务端完成，并发调用不会丢更新。
     *
     * @param type     实体类型，据其键模板定位目标文档
     * @param parts    按键模板顺序排列的键片段
     * @param jsonPath JSON 路径
     * @param delta 增减量
     * 返回: 无返回值。
     */
    @Override
    public void jsonNumIncrBy(Class<?> type, Object[] parts, String jsonPath, long delta) {
        EntityMeta meta = MetaResolver.resolve(type);
        rejectArray(meta, "jsonNumIncrBy");
        this.executor().execute("JSON.NUMINCRBY", meta.keyOf(parts), jsonPath, Long.toString(delta));
    }

    // ============ 查询 ============

    /**
     * 业务作用：按查询条件检索实体列表。
     * <p>
     * 未显式设置分页时按检索实现的默认条数截断，<b>不是返回全部命中</b>——
     * 需要全量应显式指定分页并自行翻页。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @param <T>   实体类型
     * @return 命中的实体列表；无命中时为空列表。
     */
    @Override
    public <T> List<T> find(RsQuery query, Class<T> type) {
        // 池化语义: query 一旦提交给框架, 由框架接管 recycle, caller 不能再持有/复用 query.
        // 若业务想跑相同查询多次, 应每次新构造 RsQuery, 不要把同一 query 多次传入.
        try {
            EntityMeta meta = MetaResolver.resolve(type);
            rejectArray(meta, "find");
            List<Object> raw = this.executor().execute("FT.SEARCH", query.toCommandArgs(meta));
            return parseSearch(raw, meta, type);
        } finally {
            query.recycle();
        }
    }

    /**
     * 业务作用：按查询条件检索单个实体，取命中结果的第一条。
     * <b>命中多条时不报错</b>，返回哪一条取决于排序；未指定排序时结果不稳定。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @param <T>   实体类型
     * @return 命中的第一个实体；无命中时为 null。
     */
    @Override
    public <T> T findOne(RsQuery query, Class<T> type) {
        try {
            // 临时覆盖 offset=0/limit=1, 不污染 caller 的 query 状态
            EntityMeta meta = MetaResolver.resolve(type);
            rejectArray(meta, "findOne");
            String[] args = query.renderArgs(meta, 0, 1, false, false);
            List<Object> raw = this.executor().execute("FT.SEARCH", args);
            List<T> list = parseSearch(raw, meta, type);
            return list.isEmpty() ? null : list.getFirst();
        } finally {
            query.recycle();
        }
    }

    /**
     * RediSearch 服务端 MAXSEARCHRESULTS 默认上限 (FT.CONFIG GET MAXSEARCHRESULTS), 超过会 revert 报错
     */
    private static final int FT_MAX_SEARCH_RESULTS = 1_000_000;

    /**
     * 业务作用：按查询条件只取命中文档的键，不取其内容。
     * 用于只需要键的场景（如批量删除、存在性统计），省去反序列化开销。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @param <T>   实体类型
     * @return 命中文档的键列表。
     */
    @Override
    public <T> List<String> findKeys(RsQuery query, Class<T> type) {
        try {
            // NOCONTENT 模式: RediSearch 返回 [total, key1, key2, ...], 不返回字段内容, 单次往返代价最低.
            EntityMeta meta = MetaResolver.resolve(type);
            rejectArray(meta, "findKeys");
            // 占位符模式下 key 含多字段, idFromKey split 不可靠 — 业务应改用 find(query) 拿 entity 后取 id 字段
            rejectPlaceholder(meta, "findKeys", "find(query) 后取 entity.id 字段");
            // findKeys 语义是"批量按 id 后续操作", caller 不显式设分页时应取全量, 避免默认 limit=10 静默丢数据
            // 但不能超过 RediSearch 服务端 MAXSEARCHRESULTS (默认 1M), 否则报 "LIMIT exceeds maximum"
            int useOffset = query.limitExplicit() ? query.offset() : 0;
            int useLimit = query.limitExplicit() ? query.limit() : FT_MAX_SEARCH_RESULTS;
            String[] args = query.renderArgs(meta, useOffset, useLimit, true, false);
            List<Object> raw = this.executor().execute("FT.SEARCH", args);
            if (raw == null || raw.size() < 2) return Collections.emptyList();
            List<String> ids = new ArrayList<>(raw.size() - 1);
            for (int i = 1; i < raw.size(); i++) {
                ids.add(meta.idFromKey(asString(raw.get(i))));
            }
            return ids;
        } finally {
            query.recycle();
        }
    }

    /**
     * 业务作用：按查询条件检索单个实体，以可空容器返回。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @param <T>   实体类型
     * @return 命中的第一个实体；无命中时为空容器。
     */
    @Override
    public <T> Optional<T> findOptionalOne(RsQuery query, Class<T> type) {
        // findOne 内部已经 recycle query, 这里不能重复 recycle
        return Optional.ofNullable(findOne(query, type));
    }

    /**
     * 业务作用：统计命中条数，不取文档内容。
     * <p>
     * 统计走的是索引，<b>与实际文档数可能有短暂偏差</b>——索引更新相对写入是异步的。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @return 命中条数。
     */
    @Override
    public long count(RsQuery query, Class<?> type) {
        try {
            EntityMeta meta = MetaResolver.resolve(type);
            rejectArray(meta, "count");
            // forceCountOnly: 强制 NOCONTENT + LIMIT 0 0, 仅取 total
            String[] args = query.renderArgs(meta, 0, 0, true, true);
            List<Object> raw = this.executor().execute("FT.SEARCH", args);
            return raw == null || raw.isEmpty() ? 0L : asLong(raw.getFirst());
        } finally {
            query.recycle();
        }
    }

    /**
     * 业务作用：判断键是否存在。
     *
     * @param query 见方法语义
     * @param type 反序列化目标类型
     * @return 命令的执行结果。
     */
    @Override
    public boolean exists(RsQuery query, Class<?> type) {
        // count 内部已经 recycle query, 这里不能重复 recycle
        return count(query, type) > 0;
    }

    /**
     * remove() 分批 DEL 单批 key 数, 100w key 一次 DEL 会让 Redis 单线程阻塞 + 客户端 RAM 暴涨, 1000 是常见安全值.
     */
    private static final int REMOVE_DEL_BATCH = 1000;

    /**
     * 业务作用：按查询条件批量删除命中的实体。
     * <p>
     * 先检索出命中键再逐一删除，因此在检索与删除之间新写入的文档<b>不会</b>被删掉；
     * 需要严格一致的清理应改用带版本或时间戳的条件。
     *
     * @param query 查询条件
     * @param type  实体类型
     * @return 实际删除的条数。
     */
    @Override
    public long remove(RsQuery query, Class<?> type) {
        try {
            EntityMeta meta = MetaResolver.resolve(type);
            rejectArray(meta, "remove");
            // 循环 search+del 直到一轮命中数 < FT_MAX_SEARCH_RESULTS, 避免 > 1M 命中时静默丢一部分.
            // 每轮拿前 FT_MAX_SEARCH_RESULTS 条 NOCONTENT, 分 REMOVE_DEL_BATCH 批 DEL; DEL 后这些 key
            // 在下一轮 FT.SEARCH 不会再命中 (索引自动同步), 直到结果集空.
            long deleted = 0L;
            RecycleLinkedList<String> batch = RecycleLinkedList.of();
            try {
                while (true) {
                    String[] args = query.renderArgs(meta, 0, FT_MAX_SEARCH_RESULTS, true, false);
                    List<Object> raw = this.executor().execute("FT.SEARCH", args);
                    if (raw == null || raw.size() < 2) return deleted;

                    int roundHits = raw.size() - 1;
                    long beforeRound = deleted;
                    for (int i = 1; i < raw.size(); i++) {
                        batch.add(asString(raw.get(i)));
                        if (batch.size() >= REMOVE_DEL_BATCH) {
                            deleted += redisTemplate.delete(batch);
                            batch.clear();
                        }
                    }
                    if (!batch.isEmpty()) {
                        deleted += redisTemplate.delete(batch);
                        batch.clear();
                    }
                    // 防死循环: 本轮命中但实际 DEL 0 条 (RediSearch 索引刷新滞后 / 业务方持续高速写入新数据
                    // 导致每轮 search 都拿到满 1M 但 DEL 不掉旧的). 不退出会无限循环空跑 Redis CPU.
                    // 退出后 deleted 反映已删数, 业务可重试.
                    if (deleted == beforeRound) {
                        log.warn("remove({}): roundHits={} 但本轮 DEL 0 条 (索引滞后或业务并发写入), 提前退出避免死循环",
                                meta.type().getSimpleName(), roundHits);
                        return deleted;
                    }
                    // 这轮没拿满 LIMIT 说明已经查到末尾, 不会再有命中, 跳出
                    if (roundHits < FT_MAX_SEARCH_RESULTS) return deleted;
                }
            } finally {
                batch.recycle();
            }
        } finally {
            query.recycle();
        }
    }

    // ============ 聚合 ============

    /**
     * 业务作用：执行聚合检索，按分组与归约算子产出统计结果。
     * 用于把统计下推到服务端完成，避免把大量文档取回本地再聚合。
     *
     * @param aggregation 聚合定义
     * @param type        实体类型
     * @return 聚合结果。
     */
    @Override
    public AggregationResult aggregate(RsAggregation aggregation, Class<?> type) {
        // 池化语义同 find: aggregation 提交后由框架接管 recycle
        try {
            EntityMeta meta = MetaResolver.resolve(type);
            List<Object> raw = this.executor().execute("FT.AGGREGATE", aggregation.toCommandArgs(meta));
            return parseAggregation(raw);
        } finally {
            aggregation.recycle();
        }
    }

    // ============ 内部工具 ============

    /**
     * 业务作用：把检索命令的原始响应解析成实体列表。
     * 响应是扁平的数组结构，需按约定的位置切分成「键 + 字段映射」再逐个还原成实体。
     *
     * @param raw  原始响应
     * @param meta 实体结构元信息
     * @param type 实体类型
     * @param <T>  实体类型
     * @return 解析出的实体列表。
     */
    @SuppressWarnings("unchecked")
    private <T> List<T> parseSearch(List<Object> raw, EntityMeta meta, Class<T> type) {
        if (raw == null || raw.size() < 2) return Collections.emptyList();
        long total = asLong(raw.getFirst());
        if (total == 0) return Collections.emptyList();

        boolean json = meta.dataType() == DataType.JSON;

        // HASH:  [total, key1, [field1, val1, ...],   key2, [...], ...]
        // JSON:  [total, key1, ["$", "<jsonString>"], key2, [...], ...]
        // total 是服务端命中数, 超过 Integer.MAX_VALUE 时直接 (int) 强转会回绕成负值传给 ArrayList ctor, 抛 IAE.
        // raw 本批最多 LIMIT 条, 用 long 算 min 后再 cast int 才安全.
        int capacity = (int) Math.min(total, (long) raw.size() / 2);
        List<T> result = new ArrayList<>(capacity);
        boolean hasPlaceholder = meta.hasPlaceholder();
        for (int i = 1; i < raw.size(); i += 2) {
            String key = asString(raw.get(i));
            if (i + 1 >= raw.size()) break;
            List<Object> kvList = (List<Object>) raw.get(i + 1);
            // 占位符模式下 idFromKey 不可逆, id 传 null; entity 反序列化时 id/placeholder 字段已从 JSON 读出
            // (用户明确"JSON 文档所有字段都存"), 不需要从 key 反推
            String id = hasPlaceholder ? null : meta.idFromKey(key);
            if (json) {
                result.add(parseJsonRow(id, kvList, meta, type));
            } else {
                RecycleLinkedMap<String, String> fields = listToMap(kvList);
                try {
                    result.add(mapToEntity(id, fields, meta, type));
                } finally {
                    fields.recycle();
                }
            }
        }
        return result;
    }

    /**
     * 业务作用：JSON 模式单行解析: kvList 格式为 ["$", "&lt;jsonString&gt;"]
     * <p>
     * id 参数语义: 无占位符模式从 key 反推得到 (用于 @RsId @JsonIgnore 场景从 key 回填; 但 @RsId 现在禁止 @JsonIgnore,
     * 实际不再需要); 占位符模式下 id=null, entity 反序列化时 id 从 JSON 字段读出, 不回填.
     *
     * @param id     条目标识
     * @param kvList 见上述说明
     * @param meta   见上述说明
     * @param type   反序列化目标类型
     * @return 见上述说明。
     */
    private <T> T parseJsonRow(String id, List<Object> kvList, EntityMeta meta, Class<T> type) {
        if (kvList == null || kvList.isEmpty()) {
            throw new RediSearchException("JSON row missing payload, key id=" + id);
        }
        // RETURN 投影或 default ["$" , json] 都按 kv 处理: 找到 "$" 后取下一个值
        String json = null;
        for (int j = 0; j + 1 < kvList.size(); j += 2) {
            String k = asString(kvList.get(j));
            if ("$".equals(k)) {
                json = asString(kvList.get(j + 1));
                break;
            }
        }
        if (json == null || json.isEmpty() || "null".equals(json)) {
            // 投影后没有 $ 字段, 兜底用第一个值
            json = kvList.size() >= 2 ? asString(kvList.get(1)) : null;
        }
        if (json == null || json.isEmpty()) {
            throw new RediSearchException("JSON row missing payload, key id=" + id);
        }
        T entity = this.converter().readJson(json, type);
        // 占位符模式 id=null 跳过回填 (id 已从 JSON 字段反序列化); 无占位符模式仍保留兜底回填 (如 @RsId 用户后续标 @JsonIgnore 但被启动期拦截, 这条线已死, 但保留兜底语义安全)
        if (id != null && meta.idField().get(entity) == null) {
            this.setIdFromString(entity, id, meta);
        }
        return entity;
    }

    /**
     * 业务作用：把聚合命令的原始响应解析成结果对象。
     *
     * @param raw 原始响应
     * @return 聚合结果。
     */
    private AggregationResult parseAggregation(List<Object> raw) {
        if (raw == null || raw.isEmpty()) return new AggregationResult(0, Collections.emptyList());
        long total = asLong(raw.getFirst());
        List<Map<String, Object>> rows = new ArrayList<>(raw.size() - 1);
        for (int i = 1; i < raw.size(); i++) {
            Object row = raw.get(i);
            if (row instanceof List<?> kv) {
                Map<String, Object> m = new LinkedHashMap<>(kv.size() / 2);
                for (int j = 0; j < kv.size(); j += 2) {
                    m.put(asString(kv.get(j)), j + 1 < kv.size() ? asString(kv.get(j + 1)) : null);
                }
                rows.add(m);
            }
        }
        return new AggregationResult(total, rows);
    }

    /**
     * 业务作用：把一份字段映射还原成实体，并回填其主键。
     * <p>
     * 主键单独回填而非从字段中取：主键编码在键里而不一定作为字段存储，
     * 不回填会让取回的实体主键为空，后续按它更新就会写到错误的键上。
     *
     * @param id     文档键中解出的主键
     * @param fields 文档的字段映射
     * @param meta   实体结构元信息
     * @param type   实体类型
     * @param <T>    实体类型
     * @return 还原出的实体。
     */
    @SuppressWarnings("unchecked")
    private <T> T mapToEntity(String id, Map<String, String> fields, EntityMeta meta, Class<T> type) {
        // id 参数语义同 parseJsonRow: 无占位符模式从 key 反推, 占位符模式传 null (id 字段已在 HASH fields 里).
        // 路径分流: 类上有 @JsonCreator → Jackson convertValue 让业务工厂方法 (典型: pool.get()) 接管实例化;
        // 没有 @JsonCreator 时使用 newInstance + 字段 set，支持未接对象池的实体。
        if (meta.hasJsonCreator()) {
            RecycleLinkedMap<String, Object> typed = RecycleLinkedMap.of();
            try {
                // 用 Jackson 属性名做 key (= FieldMeta.redisName(), 已适配 @JsonProperty rename),
                // value 提前用 RsConverter.read 转好 JVM 类型, Jackson 不再二次转换.
                if (id != null) typed.put(meta.idField().redisName(), id);
                for (Map.Entry<String, String> e : fields.entrySet()) {
                    // storedFieldByRedisName 包含 idField，placeholder 模式必须从字段值恢复 id，不能依赖 key 反推。
                    FieldMeta fm = meta.storedFieldByRedisName(e.getKey());
                    if (fm == null) continue;
                    typed.put(fm.redisName(), this.converter().read(e.getValue(), fm.reflect().getType(), fm));
                }
                return this.converter().convertMap(typed, type);
            } finally {
                typed.recycle();
            }
        }
        Constructor<?> ctor = meta.noArgConstructor();
        if (ctor == null) {
            throw new RediSearchException(type.getName() + " has no no-arg constructor; "
                    + "add one or use @JsonCreator factory method");
        }
        try {
            T entity = (T) ctor.newInstance();
            if (id != null) {
                // id 是 String (从 key 反推 / caller 传入), entity.@RsId 字段可能是 Long/Integer/String/etc.,
                // 必须走 RsConverter.read 按字段类型转 (跟其他字段一样), 不能直接 set 否则 String → Long 会 ClassCastException
                Object converted = this.converter().read(id, meta.idField().reflect().getType(), meta.idField());
                meta.idField().set(entity, converted);
            }
            for (Map.Entry<String, String> e : fields.entrySet()) {
                FieldMeta fm = meta.storedFieldByRedisName(e.getKey());
                if (fm == null) continue;
                Object val = this.converter().read(e.getValue(), fm.reflect().getType(), fm);
                fm.set(entity, val);
            }
            return entity;
        } catch (ReflectiveOperationException ex) {
            throw new RediSearchException("Failed to instantiate " + type.getName(), ex);
        }
    }

    /**
     * 业务作用：JSON 路径从 key 取的 String id 兜底回填到 @RsId 字段: 走 converter.read 按字段真实类型转换,
     * 不能直接 set 否则 String→Long/Integer 等会 ClassCastException (跟 HASH mapToEntity 同款语义)。
     *
     * @param entity 见上述说明
     * @param id     条目标识
     * @param meta   见上述说明
     */
    private void setIdFromString(Object entity, String id, EntityMeta meta) {
        Object converted = this.converter().read(id, meta.idField().reflect().getType(), meta.idField());
        meta.idField().set(entity, converted);
    }

    /**
     * 业务作用：返回池化 {@link RecycleLinkedMap}, caller 必须用 try-finally recycle.
     * 空 kvList 也返回 empty 池实例 (统一 recycle 责任, 调用方不必判断类型).
     *
     * @param kvList 见上述说明
     */
    private static RecycleLinkedMap<String, String> listToMap(List<Object> kvList) {
        RecycleLinkedMap<String, String> m = RecycleLinkedMap.of();
        if (kvList == null) return m;
        for (int i = 0; i + 1 < kvList.size(); i += 2) {
            m.put(asString(kvList.get(i)), asString(kvList.get(i + 1)));
        }
        return m;
    }

    /**
     * 业务作用：同 {@link #listToMap}, 返回池化 {@link RecycleLinkedMap}, caller 必须 recycle.
     *
     * @param raw 见上述说明
     */
    private static RecycleLinkedMap<String, String> toStringMap(Map<Object, Object> raw) {
        RecycleLinkedMap<String, String> m = RecycleLinkedMap.of();
        for (Map.Entry<Object, Object> e : raw.entrySet()) {
            m.put(asString(e.getKey()), asString(e.getValue()));
        }
        return m;
    }

    /**
     * 业务作用：把响应中的原始值归一成字符串。
     * 响应元素可能是字节数组也可能已是字符串，在此统一，避免各调用点重复判断。
     *
     * @param o 原始值
     * @return 字符串形式；入参为 null 时为 null。
     */
    private static String asString(Object o) {
        return switch (o) {
            case null -> null;
            case String s -> s;
            case byte[] b -> new String(b, StandardCharsets.UTF_8);
            default -> o.toString();
        };
    }

    /**
     * 业务作用：把响应中的原始值归一成长整型，用于条数、位点一类的数值字段。
     *
     * @param o 原始值
     * @return 长整型值；无法解析时为 0。
     */
    private static long asLong(Object o) {
        if (o instanceof Number n) return n.longValue();
        if (o == null) return 0L;
        return Long.parseLong(asString(o));
    }

    // ============ Actuator<T> 按 Class 绑定的视图 ============

    /**
     * 业务作用：取针对 {@code type} 的 {@link Actuator} 视图, 后续业务调用不必每次重复传 Class.
     * 视图按 Class 缓存, 进程内单例.
     * 用手动 get + putIfAbsent 避开 {@code computeIfAbsent} 每次都 new lambda 实例 (命中缓存也是浪费).
     * 业务侧通常在 static 字段 init 时调一次缓存常驻, 用 lambda 模式 GC 量级不大但没必要.
     *
     * @param type 反序列化目标类型
     * @return 见上述说明。
     */
    @SuppressWarnings("unchecked")
    public <T> Actuator<T> actuator(Class<T> type) {
        Actuator<?> a = actuators.get(type);
        if (a != null) return (Actuator<T>) a;
        a = new Actuator<>(this, type);
        Actuator<?> prev = actuators.putIfAbsent(type, a);
        return (Actuator<T>) (prev != null ? prev : a);
    }

    /**
     * 绑定单一 entity 类型的操作视图. 由 {@link RediSearch#actuator(Class)} 工厂方法构造, 进程内 per-Class 单例.
     * <p>
     * 包装 {@link RediSearch} 上跟 Class 相关的所有 API, 业务侧调用时不再每次传 Class. 各方法语义跟父类方法严格一致, 参考
     * {@link RediSearchOperations} 文档.
     * <p>
     * JSON_ARRAY / JSON_ARRAY_BUCKET 模式仍走 {@link #jsonArrayOps()}, 调用方需自行传 Class 参数 (这一层 Actuator 暂未代理).
     */
    public static final class Actuator<T> {

        private final RediSearch rs;
        private final Class<T> type;
        private final EntityMeta meta;

        /**
         * 业务作用：绑定一个实体类型，产出该类型专用的检索入口，
         * 使调用方在每次调用时不必重复传入类型参数。
         *
         * @param rs   所属检索入口
         * @param type 绑定的实体类型
         */
        Actuator(RediSearch rs, Class<T> type) {
            this.rs = rs;
            this.type = type;
            // 启动期预热 EntityMeta 缓存, 后续调用零反射
            this.meta = MetaResolver.resolve(type);
        }

        /**
         * 业务作用：查询键的数据类型。
         *
         * <p>参数说明: 无。
         *
         * @return 命令的执行结果。
         */
        public Class<T> type() {
            return type;
        }

        /**
         * 业务作用：本 Actuator 绑定的 {@link io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta} (启动期解析好, 后续调用复用).
         *
         * @return 见上述说明。
         */
        public EntityMeta meta() {
            return meta;
        }

        /**
         * 业务作用：持有的 {@link RediSearch} 实例 (业务需要跨 entity 调用 API 时反查).
         *
         * @return 见上述说明。
         */
        public RediSearch rediSearch() {
            return rs;
        }

        // ---- 索引管理 ----

        /**
         * 业务作用：确保本类型的索引已存在，不存在则按注解声明创建。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         */
        public void ensureIndex() {
            rs.ensureIndex(type);
        }

        /**
         * 业务作用：判断本类型的索引是否已存在。
         *
         * <p>参数说明: 无。
         *
         * @return 索引已存在返回 true。
         */
        public boolean indexExists() {
            return rs.indexExists(type);
        }

        /**
         * 业务作用：删除本类型的索引，保留文档本身。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值。
         */
        public void dropIndex() {
            rs.dropIndex(type);
        }

        /**
         * 业务作用：删除本类型的索引，并按需连同文档一并删除。连带删除不可逆。
         *
         * @param deleteDocuments true 表示连同文档一并删除
         * 返回: 无返回值。
         */
        public void dropIndex(boolean deleteDocuments) {
            rs.dropIndex(type, deleteDocuments);
        }

        // ---- CRUD 单条 ----

        /**
         * 业务作用：保存一个实体；同主键重复保存是覆盖而非新增。
         *
         * @param entity 待保存的实体
         * @return 原样返回入参实体。
         */
        public T save(T entity) {
            return rs.save(entity);
        }

        /**
         * 业务作用：批量保存实体，合并进同一批次发出；批次内各条互不构成事务。
         *
         * @param entities 待保存的实体集合
         * @return 原样返回入参实体的列表。
         */
        public List<T> saveAll(Iterable<T> entities) {
            return rs.saveAll(entities);
        }

        /**
         * 业务作用：按主键查实体。
         *
         * @param id 主键
         * @return 实体；不存在时为 null。
         */
        public T findById(String id) {
            return rs.findById(id, type);
        }

        /**
         * 业务作用：按主键查实体，以可空容器返回。
         *
         * @param id 主键
         * @return 实体；不存在时为空容器。
         */
        public Optional<T> findOptionalById(String id) {
            return rs.findOptionalById(id, type);
        }

        /**
         * 业务作用：按主键判断实体是否存在，不取内容。
         *
         * @param id 主键
         * @return 存在返回 true。
         */
        public boolean existsById(String id) {
            return rs.existsById(id, type);
        }

        /**
         * 业务作用：按主键删除实体及其索引项。
         *
         * @param id 主键
         * @return 删除成功返回 true。
         */
        public boolean deleteById(String id) {
            return rs.deleteById(id, type);
        }

        // ---- 占位符 prefix 模式专用 CRUD (entity 模板 / vararg) ----

        /**
         * 业务作用：以一个只填了键字段的模板实体定位并取回完整实体。
         * 用于主键由多个字段拼成、调用方手头只有这些字段的场景。
         *
         * @param template 只需填齐参与键拼装的字段
         * @return 实体；不存在时为 null。
         */
        public T findByEntity(T template) {
            return rs.findByEntity(template, type);
        }

        /**
         * 业务作用：按键片段直接定位实体，省去先构造模板实体。
         * <p>
         * 片段<b>顺序必须与键模板一致</b>：顺序错了会拼出一个语法合法但指向别处的键，
         * 既不报错也查不到，属于难定位的错误。
         *
         * @param parts 按键模板顺序排列的键片段
         * @return 实体；不存在时为 null。
         */
        public T findByParts(Object... parts) {
            return rs.findByParts(type, parts);
        }

        /**
         * 业务作用：以模板实体判断对应实体是否存在，不取内容。
         *
         * @param template 只需填齐参与键拼装的字段
         * @return 存在返回 true。
         */
        public boolean existsByEntity(T template) {
            return rs.existsByEntity(template, type);
        }

        /**
         * 业务作用：按键片段判断实体是否存在。片段顺序必须与键模板一致。
         *
         * @param parts 按键模板顺序排列的键片段
         * @return 存在返回 true。
         */
        public boolean existsByParts(Object... parts) {
            return rs.existsByParts(type, parts);
        }

        /**
         * 业务作用：以模板实体删除对应实体。
         *
         * @param template 只需填齐参与键拼装的字段
         * @return 删除成功返回 true。
         */
        public boolean deleteByEntity(T template) {
            return rs.deleteByEntity(template, type);
        }

        /**
         * 业务作用：按键片段删除实体。片段顺序必须与键模板一致。
         *
         * @param parts 按键模板顺序排列的键片段
         * @return 删除成功返回 true。
         */
        public boolean deleteByParts(Object... parts) {
            return rs.deleteByParts(type, parts);
        }

        // ---- 子字段原子操作 ----

        /**
         * 业务作用：按增量原子调整 JSON 文档中的数值字段。
         * 自增在服务端完成，并发调用不会丢更新。
         *
         * @param id       实体主键
         * @param jsonPath JSON 路径
         * @param delta 增减量
         * 返回: 无返回值。
         */
        public void jsonNumIncrBy(Object id, String jsonPath, long delta) {
            rs.jsonNumIncrBy(type, id, jsonPath, delta);
        }

        /**
         * 业务作用：按增量原子调整 JSON 文档中的数值字段。
         * 自增在服务端完成，并发调用不会丢更新。
         *
         * @param parts    按键模板顺序排列的键片段
         * @param jsonPath JSON 路径
         * @param delta 增减量
         * 返回: 无返回值。
         */
        public void jsonNumIncrBy(Object[] parts, String jsonPath, long delta) {
            rs.jsonNumIncrBy(type, parts, jsonPath, delta);
        }

        // ---- 查询 ----

        /**
         * 业务作用：按查询条件检索本类型的实体列表；未显式分页时按默认条数截断。
         *
         * @param query 查询条件
         * @return 命中的实体列表。
         */
        public List<T> find(RsQuery query) {
            return rs.find(query, type);
        }

        /**
         * 业务作用：按查询条件取第一个命中实体；命中多条时结果取决于排序。
         *
         * @param query 查询条件
         * @return 第一个命中实体；无命中时为 null。
         */
        public T findOne(RsQuery query) {
            return rs.findOne(query, type);
        }

        /**
         * 业务作用：按查询条件取第一个命中实体，以可空容器返回。
         *
         * @param query 查询条件
         * @return 第一个命中实体；无命中时为空容器。
         */
        public Optional<T> findOptionalOne(RsQuery query) {
            return rs.findOptionalOne(query, type);
        }

        /**
         * 业务作用：按查询条件只取命中文档的键，省去反序列化开销。
         *
         * @param query 查询条件
         * @return 命中文档的键列表。
         */
        public List<String> findKeys(RsQuery query) {
            return rs.findKeys(query, type);
        }

        /**
         * 业务作用：统计命中条数；走索引，与实际文档数可能有短暂偏差。
         *
         * @param query 查询条件
         * @return 命中条数。
         */
        public long count(RsQuery query) {
            return rs.count(query, type);
        }

        /**
         * 业务作用：判断键是否存在。
         *
         * @param query 见方法语义
         * @return 命令的执行结果。
         */
        public boolean exists(RsQuery query) {
            return rs.exists(query, type);
        }

        /**
         * 业务作用：按查询条件批量删除；检索与删除之间新写入的文档不会被删掉。
         *
         * @param query 查询条件
         * @return 实际删除的条数。
         */
        public long remove(RsQuery query) {
            return rs.remove(query, type);
        }

        // ---- 聚合 ----

        /**
         * 业务作用：执行聚合检索，把统计下推到服务端完成。
         *
         * @param aggregation 聚合定义
         * @return 聚合结果。
         */
        public AggregationResult aggregate(RsAggregation aggregation) {
            return rs.aggregate(aggregation, type);
        }

        // ---- JSON_ARRAY / BUCKET 模式 ----

        /**
         * 业务作用：取 ARRAY 模式专属操作视图. 注意 {@link JsonArrayOperations} 的方法仍需要 Class 参数 (子文档级路径多, 本 Actuator 暂不代理).
         *
         * @return 见上述说明。
         */
        public JsonArrayOperations jsonArrayOps() {
            return rs.jsonArrayOps();
        }
    }
}
