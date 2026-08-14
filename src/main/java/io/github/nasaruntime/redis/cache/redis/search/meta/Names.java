package io.github.nasaruntime.redis.cache.redis.search.meta;

/**
 * Nasa
 * MetaResolver 内部使用: 解析 @TagField/@NumericField/... 的 name 属性后, 拆分出
 * RediSearch alias 与 JSONPath 两个独立字段, 避免 record 不必要地暴露给外部.
 * <p>
 * package-private, 不对外暴露.
 */
record Names(String alias, String path) {}
