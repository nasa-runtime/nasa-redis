package io.github.nasaruntime.redis.cache.redis.search.meta;

/**
 * Nasa
 * Redis key 动态段 (placeholder 值 / @RsId 值 / @JsonArrayKey 值 / subId) 的分隔符校验。
 * <p>
 * 动态段直接拼进 key, 若含 key 分隔符 {@code :} 或 Redis Cluster hash tag 字符 {@code &#123;} / {@code &#125;},
 * 不同输入会拼出相同 key (撞车 / 数据互相覆盖), 或破坏 cluster slot 归属。故拼接前一律校验, fail-fast。
 * <p>
 * 例: {@code prefix="order:{sym}:{dir}:"} 下 {@code sym="A:B",dir="C"} 与 {@code sym="A",dir="B:C"} 都会拼出
 * {@code order:A:B:C:1}, 属数据损坏, 必须在写入前拒绝。
 */
public final class KeyParts {

    /**
     * 业务作用：私有化构造，杜绝实例化——本类只提供键片段的校验。
     *
     * <p>参数说明: 无。
     */
    private KeyParts() {
    }

    /**
     * 业务作用：校验一个键片段不含分隔符与哈希标签括号，防止拼出的键发生歧义。
     * <p>
     * 分隔符出现在片段里会让不同的字段组合拼出<b>同一个键</b>，属于数据损坏，且事后无法分辨谁覆盖了谁；
     * 花括号则会破坏集群模式的哈希标签解析，使本应落在同一槽位的键被分散到不同节点。
     * 两者都必须在写入前拒绝，写入后再发现已无法挽回。
     *
     * @param s         待校验的键片段
     * @param fieldDesc 字段描述，出现在错误消息中便于定位是哪个字段
     * 返回: 无返回值；校验不通过时抛出异常。
     * @throws IllegalArgumentException 片段中含有冒号或花括号
     */
    public static void checkPart(String s, String fieldDesc) {
        if (s.indexOf(':') >= 0 || s.indexOf('{') >= 0 || s.indexOf('}') >= 0) {
            throw new IllegalArgumentException(fieldDesc + " value '" + s
                    + "' must not contain ':' / '{' / '}' "
                    + "(':' is the key separator; '{}' would break Redis Cluster hash tag parsing)");
        }
    }
}
