package io.github.nasaruntime.redis.cache.redis;

/**
 * 业务作用：声明一个组件服务于哪些 Redis 实例，用于多套 Redis 共存时的归属划分。
 * 不实现本接口或返回空时视为服务全部实例——这也是单实例部署下的常态，无需额外配置。
 */
public interface Qualifier {

    /**
     * 业务作用：服务于哪些 redis server (qualifier 名数组). null = 服务所有 RedisProxy。
     *
     * @return 见上述说明。
     */
    String[] qualifiers();
}
