package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.IdGenerate;
import io.github.nasaruntime.core.exception.FileException;
import io.github.nasaruntime.core.utils.ColUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Nasa
 * Redis实现分布式雪花ID
 */
@Deprecated
@Slf4j
@Setter
@Getter
public class JedisSnowflake implements IdGenerate {

    private String evalSha;
    private JedisPool jedisPool;
    private JedisCluster jedisCluster;

    /**
     * 业务作用：一次产出多个标识。
     *
     * @param c 需要的标识个数
     * @return 标识数组。
     */
    @Override
    public long[] generate(int c) {
        List<Long> list = this.generate(c, 0);
        long[] arr = new long[list.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = list.get(i);
        return arr;
    }

    /**
     * 业务作用：申请标识并在时钟回拨等瞬时冲突时按次数重试。
     * 重试有上限，超过后抛出而非无限重试——持续冲突说明节点编号分配有问题，
     * 无限重试只会把故障拖成挂起。
     *
     * @param c     需要的标识个数
     * @param times 当前是第几次尝试
     * @return 标识列表。
     */
    @SuppressWarnings("unchecked")
    private List<Long> generate(int c, int times) {
        try {
            List<String> keys = ColUtils.toList("c");
            List<String> args = ColUtils.toList(String.valueOf(c));
            if (Objects.isNull(jedisCluster)) {
                try (Jedis jedis = jedisPool.getResource()) {
                    Object o = jedis.evalsha(this.evalSha, keys, args);
                    return c > 1 || o instanceof List ? (List<Long>) o : ColUtils.toList((Long) o);
                }
            }
            Object o = jedisCluster.evalsha(this.evalSha, keys, args);
            return c > 1 || o instanceof List ? (List<Long>) o : ColUtils.toList((Long) o);
        } catch (JedisNoScriptException e) {
            if (times > 0) {
                throw e;
            }
            if ("NOSCRIPT No matching script. Please use EVAL.".equals(e.getMessage())) {
                // 重载lua
                this.scriptLoad(Objects.isNull(jedisCluster) ? jedisPool.getResource() : jedisCluster);
                return generate(c, 1);
            }
            throw e;
        }
    }

    /**
     * 业务作用：把 Lua 脚本载入服务端缓存并取回其摘要。
     *
     * @param commands 见方法语义
     * 返回: 无返回值。
     */
    public void scriptLoad(JedisCommands commands) {
        if (commands instanceof Jedis jedis) {
            try (jedis) {
//                if (jedis.scriptExists(this.evalSha)) {
//                    return;
//                }
                this.evalSha = jedis.scriptLoad(luaScript());
                log.info("重载eval-sha：" + this.evalSha);
            }
            return;
        }
        JedisCluster jedisCluster = (JedisCluster) commands;
        String simpleName = IdGenerate.class.getSimpleName();
//        if (jedisCluster.scriptExists(this.evalSha, simpleName)) {
//            return;
//        }
        this.evalSha = jedisCluster.scriptLoad(luaScript(), simpleName);
        log.info("重载eval-sha：" + this.evalSha);
    }

    /**
     * 业务作用：产出分配节点编号的脚本内容。
     * 分配必须在服务端原子完成：分两步「查空位再占用」会让两个节点在窗口内拿到同一个编号。
     *
     * <p>参数说明: 无。
     *
     * @return 脚本内容。
     */
    @SuppressWarnings("ConstantConditions")
    private String luaScript() {
        InputStream inputStream = this.getClass().getResourceAsStream("/lua/snowflake.lua");
        if (Objects.isNull(inputStream)) {
            throw new FileException("lua script not found: {}", "/lua/snowflake.lua");
        }
        try (BufferedInputStream bis = new BufferedInputStream(inputStream)) {
            return new String(bis.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FileException(e.getMessage(), e);
        }
    }
}
