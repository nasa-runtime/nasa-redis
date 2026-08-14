package io.github.nasaruntime.redis.cache.redis.job;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * 业务作用：为 native-image 保留 RedisJob 注解方法反射调用所需的公开成员元数据。
 */
public final class RedisJobRuntimeHints implements RuntimeHintsRegistrar {
    /**
     * 业务作用：登记框架自身公开 API 的反射元数据；业务参数类型由应用自身 AOT 配置负责。
     *
     * @param hints 运行时提示注册表
     * @param classLoader 应用类加载器
     * 返回：无返回值。
     */
    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        hints.reflection().registerType(RedisJob.class, MemberCategory.values());
        hints.reflection().registerType(RedisJobResult.class, MemberCategory.values());
        hints.resources().registerPattern("lua/job/*.lua");
    }
}
