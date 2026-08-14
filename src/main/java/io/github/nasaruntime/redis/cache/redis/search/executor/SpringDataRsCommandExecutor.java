package io.github.nasaruntime.redis.cache.redis.search.executor;

import io.github.nasaruntime.redis.cache.redis.search.RedisCallbackRecycler;
import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DecoratedRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnection;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

/**
 * Nasa
 * 基于 Redis data adapter（Lettuce 底层）的 RediSearch 命令执行器实现。
 * <p>
 * 不走 Redis data adapter 的 {@code conn.execute(cmd, bytes)}，因为它默认走 {@code ByteArrayOutput}，
 * 只支持 {@code set(byte[])}，不支持 {@code set(long)}/{@code set(double)}。RediSearch 的
 * FT.INFO / FT.SEARCH / FT.AGGREGATE 等命令返回 nested 结构（int/long/double/string/list/map 混合），
 * 用 ByteArrayOutput 解析时会抛 {@code UnsupportedOperationException}（RESP2/RESP3 都炸）。
 * <p>
 * 这里直接拿 Lettuce native async commands + {@link NestedMultiOutput}，
 * 对所有类型都正确分派。GraalVM native-image 友好。
 */
@SuppressWarnings({"unchecked", "rawtypes", "ConstantConditions"})
public class SpringDataRsCommandExecutor implements RsCommandExecutor {

    /**
     * 命令执行超时（ms），可配 {@code nasa.redis-search.command-timeout-ms}（默认 10s）。
     * Redis / 网络 / 模块卡住时, 用 {@code future.get(timeout)} 避免业务线程无限阻塞; 超时 cancel + 抛带命令名异常。
     * &lt;=0 表示不限超时，直接使用 {@code future.get()}。
     */
    private static final long COMMAND_TIMEOUT_MS = Long.getLong("nasa.redis-search.command-timeout-ms", 10_000L);

    private final StringRedisTemplate redisTemplate;

    /**
     * 业务作用：建出基于模板的检索命令执行器。
     * 键与取值都按字符串处理：检索命令的参数与响应本身就是文本，按字节处理反而要多一层转换。
     *
     * @param redisTemplate 字符串模板
     */
    public SpringDataRsCommandExecutor(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 业务作用：执行一条检索命令并把响应归一成通用结构。
     * <p>
     * 不同协议版本下同一条命令的响应结构<b>并不相同</b>——新版本会把结果包成映射而旧版本是扁平数组。
     * 在此统一归一，使上层解析逻辑不必为协议版本分叉。
     *
     * @param command 命令名
     * @param args    命令参数
     * @return 归一后的响应。
     */
    @Override
    public List<Object> execute(String command, String... args) {
        try {
            return redisTemplate.execute(
                    RedisCallbackRecycler.ofRecycle(EXECUTE_CB)
                            .ref(0, command)
                            .ref(1, args));
        } catch (DataAccessException e) {
            throw new RediSearchException("RediSearch '" + command + "' failed: " + e.getMessage(), e);
        }
    }

    /**
     * execute 的回调策略 (静态常量, 零 lambda capture).
     * <p>
     * 闭包参数走 {@link io.github.nasaruntime.redis.cache.redis.search.RedisCallbackRecycler#ref}: 0=command (String), 1=args (String[]).
     * 走 Lettuce native async commands + {@link NestedMultiOutput}, 对 RESP2/RESP3 的 nested 结构都正确分派.
     */
    private static final BiFunction<RedisConnection, RedisCallbackRecycler<List<Object>>, List<Object>> EXECUTE_CB =
            (conn, ar) -> {
                String command = ar.ref(0);
                String[] args = ar.ref(1);
                // Redis data adapter 会把 LettuceConnection 包成 JDK 代理 (close-suppressing),
                // 需要先穿透代理拿到真正的 LettuceConnection.
                RedisConnection target = unwrapProxy(conn);
                if (!(target instanceof LettuceConnection lc)) {
                    throw new RediSearchException("SpringDataRsCommandExecutor 仅支持 Lettuce 驱动, 当前: "
                            + target.getClass().getName());
                }
                Object nativeConn = lc.getNativeConnection();
                BaseRedisAsyncCommands<byte[], byte[]> async = resolveAsync(nativeConn);

                ProtocolKeyword cmd = StringCommandType.of(command);
                CommandArgs<byte[], byte[]> cmdArgs = new CommandArgs<>(ByteArrayCodec.INSTANCE);
                for (String s : args) {
                    cmdArgs.add(s.getBytes(StandardCharsets.UTF_8));
                }

                NestedMultiOutput<byte[], byte[]> output = new NestedMultiOutput<>(ByteArrayCodec.INSTANCE);
                RedisFuture<?> future = async.dispatch(cmd, output, cmdArgs);
                try {
                    // bounded get 限制 Redis、网络或模块停滞占用业务线程的时长。
                    Object r = COMMAND_TIMEOUT_MS > 0 ? future.get(COMMAND_TIMEOUT_MS, TimeUnit.MILLISECONDS) : future.get();
                    return switch (r) {
                        case null -> List.of();
                        case List<?> l -> {
                            List<Object> normalized = normalize((List<Object>) l);
                            // RESP3 + NestedMultiOutput 兼容: 某些 Lettuce 版本把 RESP3 map-reply (FT.SEARCH/FT.AGGREGATE)
                            // 摊成 [k1, v1, k2, v2, ...] 的扁平 List 返回 (而不是真正的 Map), Map 分支 (下面 case)
                            // 永远不会触发. 检测特征 key (total_results / results) → 还原 Map 走 flatten 路径,
                            // 否则按 RESP2 [total, key1, kv1, ...] 形态原样透传给 parseSearch.
                            if (looksLikeResp3FlattenedMap(normalized)) {
                                yield flattenResp3SearchMap(listToMap(normalized));
                            }
                            yield normalized;
                        }
                        // RESP3 模式下 FT.SEARCH / FT.AGGREGATE 直接返回 Map (Lettuce 高版本对 map-reply 类型敏感时走这条).
                        case Map<?, ?> m -> flattenResp3SearchMap(m);
                        default -> List.of(normalizeOne(r));
                    };
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RediSearchException("RediSearch '" + command + "' interrupted", ie);
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
                    throw new RediSearchException("RediSearch '" + command + "' failed: " + cause.getMessage(), cause);
                } catch (TimeoutException te) {
                    // 超时: cancel 该 future (尽力释放底层资源), 抛带命令名异常让业务感知 (而非无限阻塞)
                    future.cancel(true);
                    throw new RediSearchException("RediSearch '" + command + "' timed out after "
                            + COMMAND_TIMEOUT_MS + "ms (nasa.redis-search.command-timeout-ms)", te);
                }
            };

    /**
     * 业务作用：穿透 Redis data adapter 的 close-suppressing 代理, 拿到真正的 {@link LettuceConnection}.
     * <p>
     * 直连 (template.execute) 拿到的可能是 JDK 代理 {@code jdk.proxy3.$ProxyN}, 它实现了
     * {@link RedisConnectionUtils.RedisConnectionProxy} 接口, 通过 {@code getTargetConnection()} 取真身.
     *
     * @param conn 见上述说明
     * @return 见上述说明。
     */
    private static RedisConnection unwrapProxy(RedisConnection conn) {
        RedisConnection cur = conn;
        // 多层代理理论上不会嵌很深, 但保险起见循环穿透:
        //  - RedisConnectionUtils.RedisConnectionProxy: 资源管理代理
        //  - DecoratedRedisConnection: StringRedisTemplate 的 DefaultStringRedisConnection 包装
        for (int i = 0; i < 8; i++) {
            RedisConnection next;
            if (cur instanceof RedisConnectionUtils.RedisConnectionProxy proxy) {
                next = proxy.getTargetConnection();
            } else if (cur instanceof DecoratedRedisConnection deco) {
                next = deco.getDelegate();
            } else {
                break;
            }
            if (next == cur) break;
            cur = next;
        }
        return cur;
    }

    /**
     * 业务作用：从 Lettuce 原生连接句柄解析出 byte[] 编解码的 async commands. LettuceConnection.getNativeConnection()
     * 不同版本/部署形态会返回不同对象, 这里全部兼容:
     * <ul>
     *   <li>{@link RedisAsyncCommands} (单机, Redis data adapter 3.x 默认): 直接返回</li>
     *   <li>{@link io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands} (集群): 同样返回 (见下)</li>
     *   <li>{@link StatefulRedisConnection} / {@link StatefulRedisClusterConnection}: 取 async()</li>
     * </ul>
     * <b>返回 {@link BaseRedisAsyncCommands} 而非 RedisAsyncCommands</b>: 集群的 {@code RedisAdvancedClusterAsyncCommands}
     * 不是单机 {@code RedisAsyncCommands} 的子类型 (二者共同父接口是 {@code BaseRedisAsyncCommands}), 强转 RedisAsyncCommands
     * 在集群部署下会 ClassCastException。{@code dispatch(...)} 定义在 BaseRedisAsyncCommands 上, 单机/集群都实现, 故只依赖它。
     *
     * @param nativeConn 见上述说明
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static BaseRedisAsyncCommands<byte[], byte[]> resolveAsync(Object nativeConn) {
        if (nativeConn instanceof RedisAsyncCommands rac) {
            return (RedisAsyncCommands<byte[], byte[]>) rac;
        }
        if (nativeConn instanceof RedisClusterAsyncCommands rcac) {
            return (RedisClusterAsyncCommands<byte[], byte[]>) rcac;
        }
        if (nativeConn instanceof StatefulRedisConnection sc) {
            return ((StatefulRedisConnection<byte[], byte[]>) sc).async();
        }
        if (nativeConn instanceof StatefulRedisClusterConnection cc) {
            return ((StatefulRedisClusterConnection<byte[], byte[]>) cc).async();
        }
        throw new RediSearchException("不支持的 Lettuce 连接类型: "
                + (nativeConn == null ? "null" : nativeConn.getClass().getName()));
    }

    /**
     * 业务作用：byte[] → String (UTF-8) 递归; List/Map 递归; Long/Double/Boolean 保留原类型.
     *
     * @param in 见上述说明
     * @return 见上述说明。
     */
    private static List<Object> normalize(List<Object> in) {
        List<Object> out = new ArrayList<>(in.size());
        for (Object o : in) out.add(normalizeOne(o));
        return out;
    }

    /**
     * 业务作用：把响应中的单个元素归一：字节转字符串、嵌套结构递归处理。
     *
     * @param o 原始元素
     * @return 归一后的元素。
     */
    private static Object normalizeOne(Object o) {
        switch (o) {
            case null -> {
                return null;
            }
            case byte[] b -> {
                return new String(b, StandardCharsets.UTF_8);
            }
            case List<?> l -> {
                List<Object> ll = (List<Object>) l;
                return normalize(ll);
            }
            case Map<?, ?> m -> {
                Map<Object, Object> nm = new LinkedHashMap<>(m.size());
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    nm.put(normalizeOne(e.getKey()), normalizeOne(e.getValue()));
                }
                return nm;
            }
            default -> {
            }
        }
        return o;
    }

    /**
     * 业务作用：RESP3 FT.SEARCH/FT.AGGREGATE Map 结构 → RESP2 扁平 List, 让 RediSearch.parseSearch 单一解析路径.
     * <p>
     * Map 形态: {@code {"total_results": N, "results": [{"id": "...", "extra_attributes": {k:v,...}, ...}, ...]}}
     * 输出形态: {@code [N, key1, [k1, v1, k2, v2, ...], key2, [...], ...]}
     *
     * @param m 见上述说明
     * @return 见上述说明。
     */
    private static List<Object> flattenResp3SearchMap(Map<?, ?> m) {
        Object totalRaw = m.get("total_results");
        if (totalRaw == null) totalRaw = m.get("total");
        Object resultsRaw = m.get("results");
        List<Object> out = new ArrayList<>();
        long total = 0L;
        if (totalRaw instanceof Number n) total = n.longValue();
        else if (totalRaw instanceof byte[] tb) {
            try {
                total = Long.parseLong(new String(tb, StandardCharsets.UTF_8));
            } catch (NumberFormatException ignored) { /* keep 0 */ }
        } else if (totalRaw != null) {
            try {
                total = Long.parseLong(totalRaw.toString());
            } catch (NumberFormatException ignored) { /* keep 0 */ }
        }
        out.add(total);
        if (!(resultsRaw instanceof List results)) return out;
        for (Object row : results) {
            // row 双形态: 真正 Map (Lettuce map-reply 模式) 或 List [k,v,k,v,...] (NestedMultiOutput 把
            // RESP3 inner Map 也摊成 list, 之前漏掉这条分支导致整 results 数组被跳过, total=1 时 raw 只剩 [1])
            Map<?, ?> rowMap;
            if (row instanceof Map<?, ?> m2) {
                rowMap = m2;
            } else if (row instanceof List<?> rowList) {
                rowMap = listToMap((List<Object>) rowList);
            } else {
                continue;
            }
            Object id = rowMap.get("id");
            out.add(id == null ? "" : asStringObj(id));
            Object attrs = rowMap.get("extra_attributes");
            if (attrs == null) attrs = rowMap.get("attributes");
            List<Object> kvFlat = new ArrayList<>();
            if (attrs instanceof Map<?, ?> attrMap) {
                for (Map.Entry<?, ?> e : attrMap.entrySet()) {
                    kvFlat.add(e.getKey() == null ? null : asStringObj(e.getKey()));
                    kvFlat.add(e.getValue() == null ? null : asStringObj(e.getValue()));
                }
            } else if (attrs instanceof List<?> attrList) {
                // 某些 driver 版本 attributes 仍是 array 形式, 透传 normalize
                kvFlat.addAll(normalize((List<Object>) attrList));
            }
            out.add(kvFlat);
        }
        return out;
    }

    /**
     * 业务作用：把响应元素转成字符串，屏蔽字节数组与字符串两种形态。
     *
     * @param o 原始元素
     * @return 字符串形式；入参为 null 时为 null。
     */
    private static String asStringObj(Object o) {
        if (o instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return o.toString();
    }

    /**
     * 业务作用：识别 "RESP3 map-reply 被 NestedMultiOutput 摊平成 List" 这种形态.
     * <p>
     * RESP3 FT.SEARCH 真实 map 结构:
     * <pre>{@code {"attributes": [...], "format": "STRING", "results": [...], "total_results": N, ...}}</pre>
     * NestedMultiOutput 在不支持 map-reply 类型派发的 Lettuce 版本下, 把上述 map 摊成
     * {@code ["attributes", [...], "format", "STRING", "results", [...], "total_results", N, ...]} 的 List.
     * <p>
     * 跟 RESP2 形态 {@code [total, key1, kvList1, key2, kvList2, ...]} 的区别: RESP3 摊平 List 内部存在
     * {@code "total_results"} / {@code "results"} 这类 map key 字符串. 用它们做特征匹配, 比 "判第一个元素是不是数字"
     * 更鲁棒 (RESP2 第一个元素经 normalize 也可能变 String 数字串).
     *
     * @param flat 见上述说明
     * @return 见上述说明。
     */
    private static boolean looksLikeResp3FlattenedMap(List<Object> flat) {
        // 偶数索引位是 map key, 至少 2 个 elements 才可能是 map (1 个 kv pair)
        if (flat.size() < 2) return false;
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            if (flat.get(i) instanceof String s && ("total_results".equals(s) || "results".equals(s))) {
                return true;
            }
        }
        return false;
    }

    /**
     * 业务作用：把 RESP3 摊平 List {@code [k1, v1, k2, v2, ...]} 还原成 {@link LinkedHashMap} 供
     * {@link #flattenResp3SearchMap(Map)} 复用单一路径. 保留插入顺序避免行序漂移.
     * <p>
     * 奇数长度 List (尾巴落单 key 没 value) 容错: 末位 key 跳过, 不抛异常.
     *
     * @param flat 见上述说明
     * @return 见上述说明。
     */
    private static Map<Object, Object> listToMap(List<Object> flat) {
        Map<Object, Object> m = new LinkedHashMap<>(flat.size() / 2 + 1);
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            m.put(flat.get(i), flat.get(i + 1));
        }
        return m;
    }
}
