package io.github.nasaruntime.redis.cache.redis.search.meta;

import io.github.nasaruntime.redis.cache.redis.search.exception.RediSearchException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Nasa
 * enum 上 {@code @JsonValue} 的访问器, 统一封装 <b>方法型</b> ({@code @JsonValue String code()}) 与
 * <b>字段型</b>（{@code @JsonValue private final int code;}）两种 Jackson 合法写法，取值前执行 setAccessible(true)，
 * 因而支持非 public 成员。
 * <p>
 * 框架对 enum 的字符串化 / key 渲染 / 查询值必须跟 Jackson 实际序列化路径一致 (Jackson 序列化 enum 时优先用
 * {@code @JsonValue} 的值，否则 {@code name()}）。字段型和非 public 方法型访问器必须统一 setAccessible，
 * 否则会造成索引写入值与查询渲染值不一致。
 */
public final class JsonValueAccessor {

    private final Method method;
    private final Field field;
    private final Class<?> valueType;

    /**
     * 业务作用：绑定取值方式与其产出类型，方法与字段两条取值途径二选一。
     *
     * @param method    取值方法，按字段取值时为 null
     * @param field     取值字段，按方法取值时为 null
     * @param valueType 取值结果的类型
     */
    private JsonValueAccessor(Method method, Field field, Class<?> valueType) {
        this.method = method;
        this.field = field;
        this.valueType = valueType;
    }

    /**
     * 业务作用：按方法建出取值器，用于实体以方法暴露其序列化取值的情形。
     * 建出时即放开访问权限，避免每次取值都做一次权限检查。
     *
     * @param m 取值方法
     * @return 取值器。
     */
    static JsonValueAccessor of(Method m) {
        // 启动期校验合法形态, 避免 get() 热路径才暴雷 (Jackson @JsonValue 方法必须: 实例 / 零参 / 有返回值)
        if (Modifier.isStatic(m.getModifiers())) {
            throw new RediSearchException("@JsonValue method must not be static: " + m);
        }
        if (m.getParameterCount() != 0) {
            throw new RediSearchException("@JsonValue method must take no parameter: " + m);
        }
        if (m.getReturnType() == void.class) {
            throw new RediSearchException("@JsonValue method must return a value (not void): " + m);
        }
        m.setAccessible(true);
        return new JsonValueAccessor(m, null, m.getReturnType());
    }

    /**
     * 业务作用：按字段建出取值器，用于实体直接以字段承载序列化取值的情形。
     *
     * @param f 取值字段
     * @return 取值器。
     */
    static JsonValueAccessor of(Field f) {
        if (Modifier.isStatic(f.getModifiers())) {
            throw new RediSearchException("@JsonValue field must not be static: " + f);
        }
        f.setAccessible(true);
        return new JsonValueAccessor(null, f, f.getType());
    }

    /**
     * 业务作用：读取字符串值。
     * 键不存在时返回空。
     *
     * @param enumConstant 见方法语义
     * @return 命令的执行结果。
     */
    public Object get(Object enumConstant) {
        try {
            return method != null ? method.invoke(enumConstant) : field.get(enumConstant);
        } catch (ReflectiveOperationException | IllegalArgumentException ex) {
            // IllegalArgumentException: invoke 时 receiver/参数不匹配 (理论上启动期校验已挡, 兜底包装避免裸抛热路径)
            throw new RediSearchException("read @JsonValue failed on " + this.describe(), ex);
        }
    }

    /**
     * 业务作用：<p>@JsonValue 的值类型 (方法返回类型 / 字段类型)。用于判定是否数字型 (@TagField 自动转 NUMERIC schema)。
     */
    public boolean returnsNumber() {
        Class<?> rt = valueType;
        return rt == int.class || rt == long.class || rt == short.class
                || rt == byte.class || rt == double.class || rt == float.class
                || Number.class.isAssignableFrom(rt);
    }

    /**
     * 业务作用：产出取值途径的可读描述，用于错误消息中指明是哪个方法或字段出了问题。
     *
     * <p>参数说明: 无。
     *
     * @return 取值途径的描述。
     */
    public String describe() {
        return method != null ? method.toString() : (field != null ? field.toString() : "<none>");
    }
}
