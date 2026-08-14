package io.github.nasaruntime.redis.cache.redis.event;

import io.github.nasaruntime.core.cache.Clearable;
import io.github.nasaruntime.redis.cache.redis.Subscribe;
import io.github.nasaruntime.core.utils.ContextUtils;
import io.github.nasaruntime.core.utils.MapUtils;
import io.github.nasaruntime.core.utils.ReflectUtils;

import java.util.Map;

/**
 * Nasa 基于redis pub/sub 实现集群 LocalCache 的清理
 */
public class RefreshCacheSub implements Subscribe<String> {

    public static final String CHANNEL = "_refresh_cache";

    /**
     * 业务作用：声明本订阅监听的频道，框架据此接线。
     *
     * <p>参数说明: 无。
     *
     * @return 频道名数组。
     */
    @Override
    public String[] channels() {
        return new String[]{CHANNEL};
    }

    /**
     * 业务作用：收到失效通知后清掉本进程内对应的本地缓存，使集群各节点的本地缓存保持一致。
     * <p>
     * 发布订阅<b>不保证送达</b>：订阅方在通知发出的瞬间不在线就会漏掉这条通知，
     * 其本地缓存要等到自然过期才恢复一致。因此本地缓存的过期时长是最终一致的兜底，不能设为永不过期。
     *
     * @param clazz 要失效的缓存所属类型的名称
     * 返回: 无返回值。
     */
    @Override
    public void consume(String clazz) {
        Class<?> c = ReflectUtils.forName(clazz);
        if (c == null || !Clearable.class.isAssignableFrom(c)) return;

        Map<String, ?> map = ContextUtils.getBeansOfType(c);
        if (MapUtils.isNotEmpty(map)) map.forEach((n, o) -> ((Clearable) o).clear());
    }
}
