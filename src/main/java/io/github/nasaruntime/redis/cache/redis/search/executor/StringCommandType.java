package io.github.nasaruntime.redis.cache.redis.search.executor;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ProtocolKeyword 实现: 承载任意自定义命令名 (RediSearch / RedisJSON 命令不在 Lettuce CommandType enum 里).
 * <p>
 * <b>实例按命令名缓存</b>: RediSearch / RedisJSON 实际只有十几个命令字 (JSON.SET / JSON.DEL / FT.SEARCH ...),
 * 但每次 dispatch 都构造一个新实例 + 重复 UTF-8 编码命令名 byte[] 是无谓 GC. 通过 {@link #of(String)}
 * 工厂方法走 {@code CACHE} 复用实例, 业务 N 次调用同命令名都拿到同一个静态实例.
 * <p>
 * 私有构造强制业务走 {@code of(...)}, 防止漏走缓存。
 */
public final class StringCommandType implements ProtocolKeyword {

    /**
     * 命令名 → 实例缓存. ConcurrentHashMap 因 dispatch 路径多线程并发命中.
     * <p>
     * 命令字符串集合实际固定 (RediSearch / RedisJSON 二十多条), CACHE 稳态大小受限, 不会无限增长.
     */
    private static final ConcurrentHashMap<String, StringCommandType> CACHE = new ConcurrentHashMap<>(32);

    /**
     * 缓存条目上限。正常只有 RediSearch/RedisJSON 二十多条固定命令字，远不到上限；该限制用于防止
     * "命令名被动态/不可信输入污染" (见 {@code execute} 的 trusted-only 约定) 导致 CACHE 无界增长 OOM。
     * 满后新命令名不再缓存 (每次 new 一个临时实例), 功能不受影响, 只是失去复用。
     */
    private static final int MAX_CACHE = Integer.getInteger("nasa.redis-search.command-cache-max", 256);

    private final byte[] bytes;
    private final String name;

    /**
     * 业务作用：绑定命令枚举与其线路名称，并预先算好其字节形式。
     * 预算而非每次编码：命令名在每条命令上都要写一次，现算会在热路径上产生大量重复的字符串编码。
     *
     * @param name 命令的线路名称
     */
    private StringCommandType(String name) {
        this.name = name;
        this.bytes = name.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：拿一个命令名对应的 ProtocolKeyword 实例, 按 name 缓存. 首次 miss 时 new, 之后复用.
     * <p>
     * 用手动 get + putIfAbsent 避开 {@code computeIfAbsent} 每次都 new lambda — 业务热路径每次 dispatch 都走这, 零 GC.
     *
     * @param name 见上述说明
     * @return 见上述说明。
     */
    public static StringCommandType of(String name) {
        StringCommandType c = CACHE.get(name);
        if (c != null) return c;
        c = new StringCommandType(name);
        // 上限保护: 满了不再缓存 (防不可信命令名撑爆 map), 直接返回临时实例, 功能正常只是不复用
        if (CACHE.size() >= MAX_CACHE) return c;
        StringCommandType prev = CACHE.putIfAbsent(name, c);
        return prev != null ? prev : c;
    }

    /**
     * 业务作用：取命令名的字节形式，直接写入线路缓冲。
     *
     * <p>参数说明: 无。
     *
     * @return 命令名的字节形式。
     */
    @Override
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * 业务作用：产出命令的线路名称，供日志与调试使用。
     *
     * <p>参数说明: 无。
     *
     * @return 命令的线路名称。
     */
    @Override
    public String toString() {
        return name;
    }
}
