package io.github.nasaruntime.redis.cache.redis.search;

import java.nio.charset.StandardCharsets;

/**
 * {@link JsonArraySupport} 与 {@link io.github.nasaruntime.redis.cache.redis.LettucePipeline.Actuator} 共用的 Lua 脚本集合，
 * 保证 ARRAY 模式的直接写入和批量写入执行相同的原子语义。
 * <p>
 * 所有脚本配合 {@code JsonArraySupport.evalScript} 的 EVALSHA + NOSCRIPT fallback 机制使用.
 * 同名脚本内容变更会让 SHA1 改变, 部署后首次走 1 次 SCRIPT LOAD 重注册, 稳态 EVALSHA 1 RTT.
 */
public final class JsonArrayLuaScripts {

    /**
     * 业务作用：私有化构造，杜绝实例化——本类只提供脚本常量。
     *
     * <p>参数说明: 无。
     */
    private JsonArrayLuaScripts() {}

    /**
     * "key 不存在则 JSON.SET 初始化 array, 否则 JSON.ARRAPPEND 追加". 单 key, 多 value.
     * <p>
     * JSON.ARRAPPEND 不接受新 key (报 "could not perform this operation on a key that doesn't exist"),
     * 必须先 JSON.SET 创建. LUA 包成 1 RTT + TOCTTOU 安全 (EXISTS + 命令 在脚本原子内).
     */
    public static final String INIT_OR_APPEND = """
            if redis.call('EXISTS', KEYS[1]) == 0 then
              redis.call('JSON.SET', KEYS[1], '$', '[' .. table.concat(ARGV, ',') .. ']')
            else
              for i = 1, #ARGV do
                redis.call('JSON.ARRAPPEND', KEYS[1], '$', ARGV[i])
              end
            end
            return #ARGV
            """;

    /**
     * 替换-或-追加 LUA: 先 JSON.DEL 旧子文档 (若 key 存在), 再判断 key 是否还在 (DEL 可能把数组删空导致 key 整体消失),
     * 不在则 JSON.SET 初始化, 在则 JSON.ARRAPPEND. 二次 EXISTS 是为了规避"DEL 把唯一子文档删除后 key 被回收"的边界.
     */
    public static final String REPLACE_OR_APPEND = """
            if redis.call('EXISTS', KEYS[1]) == 1 then
              redis.call('JSON.DEL', KEYS[1], ARGV[1])
            end
            if redis.call('EXISTS', KEYS[1]) == 0 then
              redis.call('JSON.SET', KEYS[1], '$', '[' .. ARGV[2] .. ']')
            else
              redis.call('JSON.ARRAPPEND', KEYS[1], '$', ARGV[2])
            end
            return 1
            """;

    /**
     * 跨桶 JSON.GET filter: KEYS = N 桶, ARGV[1] = JSONPath filter, 返回每桶非空 JSON 字符串的 list.
     * 单桶为空 ("[]"/nil) 时跳过, 客户端汇总时不用判空.
     */
    public static final String MULTI_GET = """
            local out = {}
            for i = 1, #KEYS do
              if redis.call('EXISTS', KEYS[i]) == 1 then
                local s = redis.call('JSON.GET', KEYS[i], ARGV[1])
                if s and s ~= '[]' and s ~= 'null' then
                  out[#out+1] = s
                end
              end
            end
            return out
            """;

    /**
     * 跨桶 JSON.DEL filter: KEYS = N 桶, ARGV[1] = JSONPath filter, 返回总删除子文档数 (sum).
     * 单桶不存在直接跳过 (JSON.DEL 在 missing key 上某些版本报错).
     */
    public static final String MULTI_DEL = """
            local total = 0
            for i = 1, #KEYS do
              if redis.call('EXISTS', KEYS[i]) == 1 then
                local n = redis.call('JSON.DEL', KEYS[i], ARGV[1])
                if n then total = total + n end
              end
            end
            return total
            """;

    /**
     * 跨桶 count: KEYS = N 桶, ARGV[1] = JSONPath filter, 用 cjson.decode 解每桶 GET 结果 + count 子文档数, 返回 sum.
     */
    public static final String MULTI_COUNT = """
            local total = 0
            for i = 1, #KEYS do
              if redis.call('EXISTS', KEYS[i]) == 1 then
                local s = redis.call('JSON.GET', KEYS[i], ARGV[1])
                if s and s ~= '[]' and s ~= 'null' then
                  local arr = cjson.decode(s)
                  total = total + #arr
                end
              end
            end
            return total
            """;

    // ============ 预编码 byte[] (给 LettucePipeline byte[] 入口用, 避免每次 ARRAY 写都重复 UTF-8 编码) ============

    /**
     * {@link #INIT_OR_APPEND} 的 UTF-8 字节, LettucePipeline ARRAY save 路径直接复用, 零编码开销.
     */
    public static final byte[] INIT_OR_APPEND_BYTES = INIT_OR_APPEND.getBytes(StandardCharsets.UTF_8);

    /**
     * {@link #REPLACE_OR_APPEND} 的 UTF-8 字节.
     */
    public static final byte[] REPLACE_OR_APPEND_BYTES = REPLACE_OR_APPEND.getBytes(StandardCharsets.UTF_8);
}
