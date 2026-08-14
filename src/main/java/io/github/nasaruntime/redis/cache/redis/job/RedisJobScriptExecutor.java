package io.github.nasaruntime.redis.cache.redis.job;

import io.github.nasaruntime.redis.cache.redis.RedisProxy;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 业务作用：加载 Job 专用 Lua 并通过 RedisProxy 的 SHA 缓存执行同 slot 状态迁移。
 */
final class RedisJobScriptExecutor {

    private static final String RESOURCE_BASE = "lua/job/";
    private final RedisProxy redisProxy;
    private final Map<RedisJobScript, String> scripts = new EnumMap<>(RedisJobScript.class);

    /**
     * 业务作用：绑定 Redis 数据源并一次性校验全部脚本资源存在。
     *
     * @param redisProxy Redis 命令代理
     */
    RedisJobScriptExecutor(RedisProxy redisProxy) {
        this.redisProxy = Objects.requireNonNull(redisProxy, "redisProxy must not be null");
        for (RedisJobScript script : RedisJobScript.values()) {
            scripts.put(script, read(script));
        }
    }

    /**
     * 业务作用：执行返回复合协议字段的状态脚本。
     *
     * @param script 脚本标识
     * @param keys   同一 hash slot 的 Redis 键
     * @param args   字符串协议参数
     * @return 已解码为字符串和整数的结果列表。
     */
    List<Object> list(RedisJobScript script, String[] keys, Object... args) {
        return redisProxy.evalDirectConnection(scripts.get(script), List.class, keys, args);
    }

    /**
     * 业务作用：执行返回单一状态码的状态脚本。
     *
     * @param script 脚本标识
     * @param keys   同一 hash slot 的 Redis 键
     * @param args   字符串协议参数
     * @return 状态码。
     */
    String text(RedisJobScript script, String[] keys, Object... args) {
        List<Object> result = list(script, keys, args);
        return result.isEmpty() || result.getFirst() == null ? "" : Objects.toString(result.getFirst());
    }

    /**
     * 业务作用：执行返回整数计数的状态脚本。
     *
     * @param script 脚本标识
     * @param keys   同一 hash slot 的 Redis 键
     * @param args   字符串协议参数
     * @return 整数结果。
     */
    long number(RedisJobScript script, String[] keys, Object... args) {
        Long value = redisProxy.evalDirectConnection(scripts.get(script), Long.class, keys, args);
        return value == null ? 0L : value;
    }

    /**
     * 业务作用：从 jar 类路径读取 UTF-8 Lua 内容，缺失时阻止调度器启动。
     *
     * @param script 脚本标识
     * @return Lua 内容。
     */
    private static String read(RedisJobScript script) {
        ClassPathResource resource = new ClassPathResource(RESOURCE_BASE + script.fileName());
        try (var input = resource.getInputStream()) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("cannot load RedisJob script: " + script.fileName(), e);
        }
    }
}
