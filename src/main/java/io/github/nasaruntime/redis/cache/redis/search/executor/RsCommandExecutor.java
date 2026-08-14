package io.github.nasaruntime.redis.cache.redis.search.executor;

import io.github.nasaruntime.redis.cache.redis.Qualifier;

import java.util.List;

/**
 * Nasa
 * RediSearch 命令执行器：屏蔽底层 Redis 客户端
 * <p>
 * 实现可以是基于 Lettuce / Redis data adapter 的 RedisCallback。
 */
public interface RsCommandExecutor extends Qualifier {

    /**
     * 业务作用：声明本执行器服务于哪些 Redis 实例。
     * 默认返回空表示服务全部实例。
     *
     * <p>参数说明: 无。
     *
     * @return 服务的实例名数组；为空表示不限。
     */
    @Override
    default String[] qualifiers() {
        return null;
    }

    /**
     * 业务作用：执行任意 RediSearch 命令，返回原始响应。
     * <p>
     * <b>trusted-only</b>：{@code command} / {@code args} 只接受框架内部或可信代码构造的值，<b>禁止</b>把
     * 终端用户输入直接拼进来 —— 本方法可下发任意 Redis 命令名 (FLUSHALL / CONFIG / EVAL ...), 不可信输入
     * 等于任意命令执行漏洞。需要面向不可信输入时, 走上层带白名单/参数化的 API, 不要直接调本方法。
     *
     * @param command 命令名，如 {@code FT.SEARCH} / {@code FT.CREATE} (可信来源)
     * @param args    命令参数 (可信来源)
     * @return Redis 原始返回（嵌套 List / byte[] / Number 等）
     */
    List<Object> execute(String command, String... args);
}
