package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.utils.StringUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.lang.NonNull;

import java.util.Map;
import java.util.Objects;

/**
 * Nasa
 * 自动装配选择具体的redis实现
 */
public class RedisImportBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {
    /**
     * 业务作用：GraalVM native-image
     *
     * @param metadata 见上述说明
     * @param registry 见上述说明
     */
    @Override
    public void registerBeanDefinitions(@NonNull AnnotationMetadata metadata, BeanDefinitionRegistry registry) {
        String lettuceClassname = NasaLettuceConfig.class.getName();
        String jedisClassname = NasaJedisConfig.class.getName();
        if (registry.containsBeanDefinition(lettuceClassname) || registry.containsBeanDefinition(jedisClassname)) {
            return;
        }
        Map<String, Object> map = metadata.getAnnotationAttributes(EnableRedis.class.getName());
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(map);
        if (Objects.isNull(attributes)) {
            return;
        }

        String value = attributes.getString("value").toLowerCase();
        switch (value) {
            case "lettuce" -> {
                BeanDefinitionBuilder lettuceBuilder = BeanDefinitionBuilder.genericBeanDefinition(NasaLettuceConfig.class);
                // for native build
                lettuceBuilder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
                registry.registerBeanDefinition(lettuceClassname, lettuceBuilder.getBeanDefinition());
            }
            case "jedis" -> {
                BeanDefinitionBuilder jedisBuilder = BeanDefinitionBuilder.genericBeanDefinition(NasaJedisConfig.class);
                // for native build
                jedisBuilder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
                registry.registerBeanDefinition(jedisClassname, jedisBuilder.getBeanDefinition());
            }
            default -> throw new IllegalArgumentException(StringUtils.concat("无效的配置 @EnableRedis(value = \"", value, "\")"));
        }
    }
}
