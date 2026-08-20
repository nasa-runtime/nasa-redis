package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.AnyHolder;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.core.function.BiConsumerRecycler;
import io.github.nasaruntime.core.function.Consumer3;
import io.github.nasaruntime.core.utils.MapUtils;
import io.github.nasaruntime.core.utils.ReflectUtils;
import io.github.nasaruntime.core.utils.StringUtils;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 业务作用：承载 Redis 消费线程的上下文，保存当前命令代理、消息元数据与跨服务透传数据，
 * 使业务在消费回调内无需注入即可取到这些信息。
 * 消费线程会被复用，因此每批处理结束必须清理，否则上一条消息的上下文会串到下一条。
 */
@SuppressWarnings({"unused"})
public abstract class RedisProxyHolder {

    /**
     * 业务作用：私有化构造，杜绝实例化——本类只提供线程上下文的静态存取。
     *
     * <p>参数说明: 无。
     */
    private RedisProxyHolder() {}

    private static final String KEY = "__RedisHolder";
    private static final String KEY_RP = "redisProxy";
    private static final String KEY_RID = "recordId";
    private static final String KEY_PASSTHROUGH = "passthrough";

    /**
     * 业务作用：读取当前线程绑定的 Redis 上下文 Map，供同一业务调用链传递连接与消息附加信息。
     *
     * <p>参数说明: 无。
     *
     * @return 当前线程的上下文 Map；尚未绑定时返回 null。
     */
    public static <K, V> Map<K, V> get() {
        return AnyHolder.get(KEY);
    }

    /**
     * 业务作用：向当前线程的 Redis 上下文写入一个键值，供同一调用链后续阶段读取。
     *
     * @param key 上下文键
     * @param o   上下文值
     * 返回: 无返回值。
     */
    public static void set(Object key, Object o) {
        AnyHolder.putAsMap(KEY, key, o);
    }

    /**
     * 业务作用：从当前线程的 Redis 上下文读取指定键。
     *
     * @param key 上下文键
     * @return 对应上下文值；上下文或键不存在时返回 null。
     */
    public static <T> T get(Object key) {
        return AnyHolder.getAsMap(KEY, key);
    }

    /**
     * 业务作用：在消费线程上标记本次消息由哪个命令代理投递，使业务无需注入即可就近回发。
     * 多套 Redis 共存时，这也是「从哪来就回哪去」的依据。
     *
     * @param redisProxy 本次消费所属的命令代理
     * 返回: 无返回值。
     */
    public static void setRedisProxy(RedisProxy redisProxy) {
        set(KEY_RP, redisProxy);
    }

    /**
     * 业务作用：在消费回调内取回本次消息所属的命令代理。
     *
     * <p>参数说明: 无。
     *
     * @return 命令代理；非消费线程上为 null。
     */
    public static RedisProxy getRedisProxy() {
        return get(KEY_RP);
    }

    /**
     * 业务作用：记录本次消费记录在 Stream 中的标识，供业务确认消息或定位问题。
     *
     * @param recordId 条目标识
     * 返回: 无返回值。
     */
    public static void setStreamRecordId(Object recordId) {
        set(KEY_RID, recordId);
    }

    /**
     * 业务作用：读取本次消费记录的 Stream 条目标识。
     *
     * <p>参数说明: 无。
     *
     * @return 条目标识；非消费线程上为 null。
     */
    public static Object getStreamRecordId() {
        return get(KEY_RID);
    }

    /**
     * passthrough.forEach 的策略 — 静态共享, 替代每次 passthrough() 调用 new 一个捕获 rtn 的 lambda.
     * <p>
     * 配合 BiConsumerRecycler.of() 借实例 + ref(0)=rtn, ar.ref(0) 取回 rtn, 把命中条目 put 进去.
     * 撮合热路径 publish 每次都会进 passthrough(), 池化省一份闭包 + 一次 capture-array 分配.
     */
    private static final Consumer3<Object, Object, BiConsumerRecycler<Object, Object>> PASSTHROUGH_FILTER = (k, v, ar) -> {
        RecycleLinkedMap<String, Object> rtn = ar.ref(0);
        // 只透传 String key + 可序列化 value (基本类型/String/Map/List/数组)
        // 过滤 Proxy/Lambda/Thread/IO 资源等不可序列化对象
        if (k instanceof String ks && isPassable(v)) rtn.put(ks, v);
    };

    /**
     * 业务作用：获取透传的 map 快照. 返回值是池借的 RecycleLinkedMap, caller 用完必须显式 recycle 还池.
     * <p>
     * 设计原因: 返回类型用具体的 RecycleLinkedMap 而非 Map 接口, 让 caller 一眼看出"必须 recycle",
     * 避免误以为是 GC 管理的普通 Map 导致池泄漏.
     */
    public static RecycleLinkedMap<String, Object> passthrough() {
        Map<Object, Object> passthrough = get(KEY_PASSTHROUGH);
        String traceId = AnyHolder.get(AnyHolder.TRACE_ID);
        if (traceId == null && MapUtils.isEmpty(passthrough)) return null;
        RecycleLinkedMap<String, Object> rtn = RecycleLinkedMap.of();
        if (passthrough != null) {
            BiConsumerRecycler<Object, Object> bc = BiConsumerRecycler.of(PASSTHROUGH_FILTER).ref(0, rtn);
            try {
                passthrough.forEach(bc);
            } finally {
                bc.recycle();
            }
        }
        if (traceId != null) rtn.put(AnyHolder.TRACE_ID, traceId);
        if (rtn.isEmpty()) {
            rtn.recycle();
            return null;
        }
        return rtn;
    }

    /**
     * 业务作用：设置透传上下文，用于业务系统主动调用
     *
     * @param k 见上述说明
     * @param o 见上述说明
     */
    public static void passthrough(Object k, Object o) {
        if (o == null) return;
        Map<Object, Object> passthrough = get(KEY_PASSTHROUGH);
        if (passthrough == null) set(KEY_PASSTHROUGH, passthrough = new LinkedHashMap<>());
        passthrough.put(k, o);
    }

    /**
     * 业务作用：恢复透传上下文
     *
     * @param passthrough 见上述说明
     */
    public static void passthroughAll(Map<String, Object> passthrough) {
        String traceId = MapUtils.remove(passthrough, AnyHolder.TRACE_ID);
        if (StringUtils.isNotBlank(traceId)) AnyHolder.set(AnyHolder.TRACE_ID, traceId);
        MapUtils.forEach(passthrough, RedisProxyHolder::passthrough);
    }

    /**
     * 业务作用：读取一个业务透传项，消费侧据此取回上游随消息带来的上下文。
     *
     * @param key 透传键
     * @param <V> 期望的值类型
     * @return 透传值；本线程无上下文或无该项时为 null。
     */
    public static <V> V passthrough(Object key) {
        return MapUtils.getObject(get(KEY_PASSTHROUGH), key);
    }

    /**
     * 业务作用：取一个业务对象关联的链路标识，取不到时回落到当前线程的链路标识。
     * 回落使日志在透传缺失时仍能关联到本次处理，而不是留下一个空标识。
     *
     * @param key 业务对象或上下文键
     * @return 链路标识。
     */
    public static String traceId(Object key) {
        Object o = get(key);
        return o instanceof Map<?, ?> map && !map.isEmpty() ? MapUtils.getString(map, AnyHolder.TRACE_ID) : AnyHolder.getTraceId();
    }

    /**
     * 业务作用：清理当前线程的上下文。
     * 消费线程会被复用，不清理则上一条消息的代理、条目标识与透传项会串到下一条消息的处理中。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    public static void clear() {
        AnyHolder.remove(KEY);
    }

    /**
     * 业务作用：判断 value 是否可安全序列化到 Redis stream (Jackson).
     * 白名单: null / 基本类型包装 / String / Map / Collection / 数组.
     * 黑名单: Proxy / Lambda / Thread / IO / Class 等.
     *
     * @param v 见上述说明
     * @return 见上述说明。
     */
    private static boolean isPassable(Object v) {
        if (v == null) return false;
        if (v instanceof CharSequence || v instanceof Number || v instanceof Boolean) return true;
        if (v instanceof Map || v instanceof Collection) return true;
        if (v.getClass().isArray()) return true;
        if (ReflectUtils.isProxy(v) || ReflectUtils.isLambda(v)) return false;
        if (v instanceof Thread || v instanceof Class) return false;
        // 其他 POJO 放行, Jackson 能序列化
        return true;
    }

}
