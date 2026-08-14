package io.github.nasaruntime.redis.cache.redis.search;

import io.github.nasaruntime.core.base.RecycleLinkedList;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.redis.cache.redis.LettucePipeline;
import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import io.github.nasaruntime.redis.cache.redis.search.annotation.DataType;
import io.github.nasaruntime.redis.cache.redis.search.convert.RsConverter;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import io.github.nasaruntime.redis.cache.redis.search.executor.RsCommandExecutor;
import io.github.nasaruntime.redis.cache.redis.search.meta.EntityMeta;
import io.github.nasaruntime.redis.cache.redis.search.meta.MetaResolver;
import io.github.nasaruntime.redis.cache.redis.search.query.JsonPaths;
import io.github.nasaruntime.redis.cache.redis.search.query.RsQuery;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nasa
 * {@link JsonArrayOperations} 默认实现, 同时支持:
 * <ul>
 *   <li>{@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY}: 1 个 ARRAY key 装多个子文档</li>
 *   <li>{@link io.github.nasaruntime.redis.cache.redis.search.annotation.DataType#JSON_ARRAY_BUCKET}: 同一组 {@code @JsonArrayKey} 散到 N 个桶 key, 同 hash tag 锁同 slot</li>
 * </ul>
 * <p>
 * BUCKET 模式的跨桶操作用 LUA 一次性扫所有桶 (KEYS 全传入), 把 N 次 RTT 收敛为 1 次. Redis Cluster 下因为
 * 所有桶 key 共享 {@code {arrayKeyParts}} hash tag, 必在同一 slot, 不会触发 CROSSSLOT.
 */
@Slf4j
public class JsonArraySupport implements JsonArrayOperations {

    private final RedisProxy redisProxy;
    private final RsCommandExecutor executor;
    private final RsConverter converter;
    /**
     * LUA 脚本 → SHA1 缓存. 调用时优先 EVALSHA, 收到 NOSCRIPT (Redis 重启 / SCRIPT FLUSH) fallback 重 load.
     * 用 ConcurrentHashMap 因为 jsonArrayOps() 单例被多线程并发调用.
     */
    private final Map<String, String> scriptSha = new ConcurrentHashMap<>(8);

    /**
     * 业务作用：建出数组存储模式的支撑实现，绑定命令代理、命令执行器与取值转换器。
     *
     * @param redisProxy 命令代理
     * @param executor   命令执行器
     * @param converter  取值转换器
     */
    public JsonArraySupport(RedisProxy redisProxy, RsCommandExecutor executor, RsConverter converter) {
        this.redisProxy = redisProxy;
        this.executor = executor;
        // JSON chunk 解析复用 converter 的业务 mapper，保持配置一致，并直接生成实体以减少二次编码。
        this.converter = converter;
    }

    // ============ save ============

    /**
     * 业务作用：把一个实体作为子文档追加进其所属数组。
     * <b>不判重</b>：同标识的子文档会被追加成第二份，需要覆盖语义应改用带替换的保存。
     *
     * @param entity 待保存的实体
     * @param <T>    实体类型
     * @return 原样返回入参实体。
     */
    @Override
    public <T> T save(T entity) {
        // 走 LettucePipeline 入口跟 HASH/JSON save 嵌套语义一致 (外层 open 时合并 flush)
        checkMeta(entity.getClass());
        LettucePipeline.Actuator ac = LettucePipeline.open(this.redisProxy, this);
        try {
            ac.jsonArraySave(entity);
        } finally {
            ac.pipeline(this);
        }
        return entity;
    }

    /**
     * 业务作用：批量把实体追加进各自所属的数组，按数组分组后合并成批次发出。
     *
     * @param entities 待保存的实体集合
     * @param <T>      实体类型
     * @return 原样返回入参实体的列表。
     */
    @Override
    public <T> List<T> saveAll(Iterable<T> entities) {
        // 按最终 key 分组 (BUCKET 模式下不同 sub id 自然分到不同 bucket key).
        // grouped 只用于临时收集，map 与 value list 都复用池化节点，降低批量路径的对象分配。
        // saved 是返回值业务持有, 不能 recycle, 仍然用 ArrayList.
        RecycleLinkedMap<String, RecycleLinkedList<String>> grouped = RecycleLinkedMap.of();
        List<T> saved = new ArrayList<>();
        try {
            for (T e : entities) {
                if (e == null) throw new RediSearchException("saveAll: entity cannot be null");
                EntityMeta meta = checkMeta(e.getClass());
                String key = meta.arrayKey(e);
                RecycleLinkedList<String> list = grouped.get(key);
                if (list == null) {
                    list = RecycleLinkedList.of();
                    grouped.put(key, list);
                }
                list.add(converter.writeJson(e));
                saved.add(e);
            }
            if (grouped.isEmpty()) return saved;
            // 走 LettucePipeline 把 N 个 EVALSHA 收敛成 1 个 RTT (开发规范第 11 条: 禁止循环内 IO)
            pipelineInitOrAppend(grouped);
            return saved;
        } finally {
            // 级联 recycle: RecycleLinkedMap.recycle 自身不会回收 value 中的 RecycleLinkedList, 手动遍历
            for (RecycleLinkedList<String> list : grouped.values()) list.recycle();
            grouped.recycle();
        }
    }

    /**
     * 业务作用：追加子文档，已存在同标识的则整体替换。
     * <p>
     * 判存与写入由脚本在服务端<b>原子完成</b>——分两步会在两者之间留出窗口，
     * 并发调用会让同一标识出现两份，且事后难以判断哪一份是最新的。
     *
     * @param entity 待保存的实体
     * @param <T>    实体类型
     * @return 原样返回入参实体。
     */
    @Override
    public <T> T saveOrReplace(T entity) {
        // ARRAY 模式: key = prefix+parts; BUCKET 模式: key = prefix+{parts}:bucket:N (N = subId.hashCode() % bucketCount).
        // 两种模式共享同一条 REPLACE_OR_APPEND LUA: 同 @JsonArrayKey + bucketCount 不变时, 同 subId 永远落同一目标桶,
        // 仅需 1 个 key 上 DEL by subId + ARRAPPEND 新值.
        // ⚠ 业务变更 @JsonArrayKey 后调用 saveOrReplace 不会清旧 key 的旧子文档 (跨 key/桶组跨 slot, CROSSSLOT 阻拦);
        // bucketCount 改了同理 (旧 subId 散到不同桶 idx). 业务侧 schema 漂移时必须先显式
        // removeSubDoc(oldParts, subId, type) 再 save 新 entity, 详见 JsonArrayKey API docs.
        checkMeta(entity.getClass());
        LettucePipeline.Actuator ac = LettucePipeline.open(this.redisProxy, this);
        try {
            ac.jsonArraySaveOrReplace(entity);
        } finally {
            ac.pipeline(this);
        }
        return entity;
    }

    // ============ findSubDoc / existsSubDoc / removeSubDoc — 单 key (BUCKET 模式直接定位单桶) ============

    /**
     * 业务作用：按标识在数组中定位一个子文档。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param subId         子文档标识
     * @param type          子文档类型
     * @param <T>           子文档类型
     * @return 子文档；不存在时为 null。
     */
    @Override
    public <T> T findSubDoc(Object[] arrayKeyParts, Object subId, Class<T> type) {
        EntityMeta meta = checkMeta(type);
        if (subId == null) throw new IllegalArgumentException("subId cannot be null");
        String key = keyForSubDoc(meta, arrayKeyParts, subId);
        String filter = idFilter(meta, subId);
        List<T> rows = jsonGetEntities(key, filter, type);
        return rows.isEmpty() ? null : rows.getFirst();
    }

    /**
     * 业务作用：判断数组中是否存在给定标识的子文档，不取内容。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param subId         子文档标识
     * @param type          子文档类型
     * @return 存在返回 true。
     */
    @Override
    public boolean existsSubDoc(Object[] arrayKeyParts, Object subId, Class<?> type) {
        // 这里只判断 JSON.GET filter 是否命中，不反序列化整个子文档，避免为计数构造业务实体或占用对象池。
        EntityMeta meta = checkMeta(type);
        if (subId == null) throw new IllegalArgumentException("subId cannot be null");
        String key = keyForSubDoc(meta, arrayKeyParts, subId);
        return chunkNonEmpty(jsonGetChunk(key, idFilter(meta, subId)));
    }

    /**
     * 业务作用：按标识从数组中移除一个子文档。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param subId         子文档标识
     * @param type          子文档类型
     * @return 实际移除的条数。
     */
    @Override
    public long removeSubDoc(Object[] arrayKeyParts, Object subId, Class<?> type) {
        EntityMeta meta = checkMeta(type);
        if (subId == null) throw new IllegalArgumentException("subId cannot be null");
        String key = keyForSubDoc(meta, arrayKeyParts, subId);
        return jsonDel(key, idFilter(meta, subId));
    }

    // ============ findInArray / removeInArray / countInArray — 全 array (BUCKET 模式跨桶 LUA) ============

    /**
     * 业务作用：按查询条件在数组中筛出子文档列表。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param query         查询条件
     * @param type          子文档类型
     * @param <T>           子文档类型
     * @return 满足条件的子文档列表。
     */
    @Override
    public <T> List<T> findInArray(Object[] arrayKeyParts, RsQuery query, Class<T> type) {
        try {
            EntityMeta meta = checkMeta(type);
            String filter = query.toJsonPathFilter(meta);
            if (meta.dataType() == DataType.JSON_ARRAY_BUCKET) {
                return findInBuckets(meta, arrayKeyParts, filter, type);
            }
            String key = meta.arrayKeyOf(arrayKeyParts);
            return jsonGetEntities(key, filter, type);
        } finally {
            query.recycle();
        }
    }

    /**
     * 业务作用：按查询条件从数组中批量移除子文档。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param query         查询条件
     * @param type          子文档类型
     * @return 实际移除的条数。
     */
    @Override
    public long removeInArray(Object[] arrayKeyParts, RsQuery query, Class<?> type) {
        try {
            EntityMeta meta = checkMeta(type);
            String filter = query.toJsonPathFilter(meta);
            if (meta.dataType() == DataType.JSON_ARRAY_BUCKET) {
                return evalMultiDel(meta.allBucketKeys(arrayKeyParts), filter);
            }
            return jsonDel(meta.arrayKeyOf(arrayKeyParts), filter);
        } finally {
            query.recycle();
        }
    }

    /**
     * 业务作用：统计数组中满足条件的子文档数。
     *
     * @param arrayKeyParts 定位数组所在键的片段
     * @param query         查询条件
     * @param type          子文档类型
     * @return 满足条件的条数。
     */
    @Override
    public long countInArray(Object[] arrayKeyParts, RsQuery query, Class<?> type) {
        try {
            EntityMeta meta = checkMeta(type);
            String filter = query.toJsonPathFilter(meta);
            if (meta.dataType() == DataType.JSON_ARRAY_BUCKET) {
                return evalMultiCount(meta.allBucketKeys(arrayKeyParts), filter);
            }
            // 这里只统计数组元素个数，不构造实体，避免仅为计数占用对象池。
            return converter.countJsonArray(jsonGetChunkOrEmpty(meta.arrayKeyOf(arrayKeyParts), filter));
        } finally {
            query.recycle();
        }
    }

    // ============ BUCKET 跨桶 helpers ============

    /**
     * 业务作用：在按桶分片的多个数组键上并行筛选并合并结果。
     * 分桶把一个逻辑数组拆到多个键上，避免单键过大导致读写整体变慢；
     * 代价是查询要覆盖全部桶，因此桶数不宜过多。
     *
     * @param meta   实体结构元信息
     * @param parts  定位数组所在键的片段
     * @param filter 已渲染的过滤表达式
     * @param type   子文档类型
     * @param <T>    子文档类型
     * @return 各桶命中结果的合并列表。
     */
    private <T> List<T> findInBuckets(EntityMeta meta, Object[] parts, String filter, Class<T> type) {
        String[] bucketKeys = meta.allBucketKeys(parts);
        List<String> jsonChunks = evalMultiGet(bucketKeys, filter);
        List<T> out = new ArrayList<>();
        for (String chunk : jsonChunks) {
            if (chunkNonEmpty(chunk)) out.addAll(converter.readJsonList(chunk, type));
        }
        return out;
    }

    /**
     * 业务作用：执行 {@link JsonArrayLuaScripts#MULTI_GET}, 返回每桶非空的 JSON 字符串列表.
     *
     * @param keys   缓存键集合
     * @param filter 见上述说明
     */
    private List<String> evalMultiGet(String[] keys, String filter) {
        List<Object> raw = evalScript(JsonArrayLuaScripts.MULTI_GET, keys, filter);
        if (raw == null) return List.of();
        List<String> out = new ArrayList<>(raw.size());
        for (Object o : raw) {
            if (o != null) out.add(o.toString());
        }
        return out;
    }

    /**
     * 业务作用：以脚本在多个键上按条件批量删除子文档，一次往返完成。
     * 逐键删除要多次往返且各键之间不一致，脚本使整批删除在服务端连续完成。
     *
     * @param keys   目标键
     * @param filter 已渲染的过滤表达式
     * @return 实际删除的条数。
     */
    private long evalMultiDel(String[] keys, String filter) {
        List<Object> raw = evalScript(JsonArrayLuaScripts.MULTI_DEL, keys, filter);
        return raw == null || raw.isEmpty() ? 0L : toLong(raw.getFirst());
    }

    /**
     * 业务作用：以脚本在多个键上按条件统计子文档数，一次往返完成。
     *
     * @param keys   目标键
     * @param filter 已渲染的过滤表达式
     * @return 满足条件的条数。
     */
    private long evalMultiCount(String[] keys, String filter) {
        List<Object> raw = evalScript(JsonArrayLuaScripts.MULTI_COUNT, keys, filter);
        return raw == null || raw.isEmpty() ? 0L : toLong(raw.getFirst());
    }

    // ============ 单 key 内部工具 ============

    /**
     * 业务作用：saveAll 批量入口: 把 N 个 ARRAY key 的 INIT_OR_APPEND eval 收敛到单 LettucePipeline RTT.
     * <p>
     * 不走 {@link #evalScript} EVALSHA + NOSCRIPT fallback 路径, 是因为 {@link LettucePipeline.Actuator#eval}
     * 走 OP_EVAL_ASYNC 直接传脚本字节, server 端 NOSCRIPT 不可能发生 (脚本随每个命令同发). 单次脚本 ~340 字节,
     * 批量大时这点 overhead 比 N 次 RTT 划算得多 (Cluster 跨 slot RTT 平均 0.5ms+).
     *
     * @param grouped 见上述说明
     */
    private void pipelineInitOrAppend(Map<String, ? extends List<String>> grouped) {
        LettucePipeline.Actuator ac = LettucePipeline.open(this.redisProxy, this);
        try {
            for (Map.Entry<String, ? extends List<String>> kv : grouped.entrySet()) {
                // (Object[]) cast 消除 compiler varargs 不明确警告 (eval 签名 Object...)
                ac.eval(JsonArrayLuaScripts.INIT_OR_APPEND,
                        new String[]{kv.getKey()},
                        (Object[]) kv.getValue().toArray(new String[0]));
            }
        } finally {
            ac.pipeline(this);
        }
    }

    /**
     * 业务作用：路由: ARRAY 模式用 arrayKeyOf 单 key; BUCKET 模式用 bucketKey 算桶定位单桶.
     *
     * @param meta  见上述说明
     * @param parts 见上述说明
     * @param subId 见上述说明
     * @return 见上述说明。
     */
    private static String keyForSubDoc(EntityMeta meta, Object[] parts, Object subId) {
        if (meta.dataType() == DataType.JSON_ARRAY_BUCKET) {
            return meta.bucketKey(parts, subId);
        }
        return meta.arrayKeyOf(parts);
    }

    /**
     * 业务作用：构造 "@.&lt;idField&gt;==&lt;literal&gt;" filter (用于按 sub id 精确查/删).
     *
     * @param meta  见上述说明
     * @param subId 见上述说明
     * @return 见上述说明。
     */
    private static String idFilter(EntityMeta meta, Object subId) {
        return "$[?(@." + meta.idField().redisName() + "==" + jsonLiteral(subId) + ")]";
    }

    // ============ EVALSHA + NOSCRIPT fallback ============

    /**
     * 业务作用：优先 EVALSHA, 收到 NOSCRIPT (脚本被 SCRIPT FLUSH 或 Redis 重启清掉) 时一次 fallback: SCRIPT LOAD 重传并重试.
     * 重试仍失败则向上抛.
     * <p>
     * 用 EVALSHA 比 EVAL 每次都把脚本字节传到 server 省网络: 我们最长的 INIT_OR_APPEND ~340 字节, 高 QPS 下不小.
     *
     * @param script Lua 脚本
     * @param keys   缓存键集合
     * @param argv   见上述说明
     * @return 见上述说明。
     */
    private List<Object> evalScript(String script, String[] keys, String... argv) {
        String sha = shaOf(script);
        String[] args = buildEvalShaArgs(sha, keys, argv);
        try {
            return executor.execute("EVALSHA", args);
        } catch (RediSearchException e) {
            if (!isNoScript(e)) throw e;
            // NOSCRIPT: 脚本不在 server cache 里 (Redis 重启 / SCRIPT FLUSH / failover 到新节点), 重新 LOAD
            String newSha = scriptLoad(script);
            scriptSha.put(script, newSha);
            args[0] = newSha;
            return executor.execute("EVALSHA", args);
        }
    }

    /**
     * 业务作用：首次取 SHA → SCRIPT LOAD; 后续走缓存. ConcurrentHashMap.computeIfAbsent 保证并发只 LOAD 一次.
     *
     * @param script Lua 脚本
     * @return 见上述说明。
     */
    private String shaOf(String script) {
        return scriptSha.computeIfAbsent(script, this::scriptLoad);
    }

    /**
     * 业务作用：把 Lua 脚本载入服务端缓存并取回其摘要。
     *
     * @param script Lua 脚本
     * @return 命令的执行结果。
     */
    private String scriptLoad(String script) {
        List<Object> r = executor.execute("SCRIPT", "LOAD", script);
        if (r == null || r.isEmpty() || r.getFirst() == null) {
            throw new RediSearchException("SCRIPT LOAD returned empty");
        }
        return r.getFirst().toString();
    }

    /**
     * 业务作用：组装按摘要执行脚本的参数：摘要、键个数、各键、其余参数。
     * 键个数必须紧跟摘要且与实际键数一致，这是脚本执行命令的语法要求；
     * 数量不符会让部分键被当成普通参数，脚本随即读到错误的输入。
     *
     * @param sha  脚本摘要
     * @param keys 目标键
     * @param argv 其余参数
     * @return 组装完毕的参数数组。
     */
    private static String[] buildEvalShaArgs(String sha, String[] keys, String... argv) {
        String[] args = new String[2 + keys.length + argv.length];
        args[0] = sha;
        args[1] = String.valueOf(keys.length);
        System.arraycopy(keys, 0, args, 2, keys.length);
        System.arraycopy(argv, 0, args, 2 + keys.length, argv.length);
        return args;
    }

    /**
     * 业务作用：判定异常是否为「服务端未缓存该脚本」。
     * 识别出来才能回退到按内容执行并重新缓存；识别不出会把一个可自愈的情形当成真正的故障。
     *
     * @param t 待判定的异常
     * @return 属于脚本未缓存返回 true。
     */
    private static boolean isNoScript(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("NOSCRIPT")) return true;
            t = t.getCause();
        }
        return false;
    }

    // ============ 其他内部工具 ============

    /**
     * 业务作用：取实体的结构元信息，并确认其确实声明为数组存储模式。
     * 用非数组模式的实体调用本类的方法会拼出错误的键，在此提前拒绝而不是等到运行期产出脏数据。
     *
     * @param type 实体类型
     * @return 实体结构元信息。
     * @throws RuntimeException 该实体未声明为数组存储模式
     */
    private EntityMeta checkMeta(Class<?> type) {
        EntityMeta meta = MetaResolver.resolve(type);
        DataType dt = meta.dataType();
        if (dt != DataType.JSON_ARRAY && dt != DataType.JSON_ARRAY_BUCKET) {
            throw new RediSearchException(type.getSimpleName() + " is not ARRAY mode, got " + dt
                    + " — JsonArrayOperations 仅适用 JSON_ARRAY / JSON_ARRAY_BUCKET");
        }
        return meta;
    }

    /**
     * 业务作用：JSON.GET key filter 取原始 chunk 字符串 (JSONPath filter 命中结果, 通常是 JSON 数组). 无命中返 null。
     *
     * @param key    缓存键
     * @param filter 见上述说明
     * @return 见上述说明。
     */
    private String jsonGetChunk(String key, String filter) {
        List<Object> raw = executor.execute("JSON.GET", key, filter);
        if (raw == null || raw.isEmpty() || raw.getFirst() == null) return null;
        return raw.getFirst().toString();
    }

    /**
     * 业务作用：按过滤表达式取回数组中的一段内容，键不存在或无命中时返回空数组文本。
     * 返回空文本而非 null，使调用方的解析路径对「没有」与「有但为空」保持一致。
     *
     * @param key    目标键
     * @param filter 已渲染的过滤表达式
     * @return JSON 数组文本；无内容时为空数组文本。
     */
    private String jsonGetChunkOrEmpty(String key, String filter) {
        String chunk = jsonGetChunk(key, filter);
        return chunk == null ? "[]" : chunk;
    }

    /**
     * 业务作用：chunk 是否含命中: 非 null 且非空数组 / null 值。
     *
     * @param chunk 见上述说明
     * @return 见上述说明。
     */
    private static boolean chunkNonEmpty(String chunk) {
        return chunk != null && !chunk.isEmpty() && !"[]".equals(chunk) && !"null".equals(chunk);
    }

    /**
     * 业务作用：将 JSON.GET chunk 经业务 converter 一次解析为 {@code List<T>}，复用业务 mapper 并避免 chunk→Map→JSON 二次编码。
     *
     * @param key    缓存键
     * @param filter 见上述说明
     * @param type   反序列化目标类型
     * @return 见上述说明。
     */
    private <T> List<T> jsonGetEntities(String key, String filter, Class<T> type) {
        String chunk = jsonGetChunk(key, filter);
        // 空结果也返回可变 list，保持 findInArray 的返回契约，避免调用方 add 时触发 UnsupportedOperationException。
        if (!chunkNonEmpty(chunk)) return new ArrayList<>(0);
        return converter.readJsonList(chunk, type);
    }

    /**
     * 业务作用：删除 JSON 文档中的某个路径。
     * 路径不存在时不报错。
     *
     * @param key 缓存键
     * @param filter 见方法语义
     * @return 命令的执行结果。
     */
    private long jsonDel(String key, String filter) {
        List<Object> raw = executor.execute("JSON.DEL", key, filter);
        if (raw == null || raw.isEmpty() || raw.getFirst() == null) return 0L;
        return toLong(raw.getFirst());
    }

    /**
     * 业务作用：把脚本返回的原始值归一成长整型条数。
     *
     * @param o 原始值
     * @return 条数；无法解析时为 0。
     */
    private static long toLong(Object o) {
        if (o instanceof Number n) return n.longValue();
        if (o == null) return 0L;
        try {
            return Long.parseLong(o.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * 业务作用：JSONPath 字面量: 走 {@link io.github.nasaruntime.redis.cache.redis.search.query.JsonPaths#literal} (含完整控制字符转义), caller 保证 v != null.
     *
     * @param v 见上述说明
     * @return 见上述说明。
     */
    private static String jsonLiteral(Object v) {
        return JsonPaths.literal(v);
    }
}
