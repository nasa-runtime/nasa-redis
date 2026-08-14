package io.github.nasaruntime.redis.cache.redis.search.query;

/**
 * Nasa
 * Criteria 内部操作类型, package-private 不对外暴露.
 */
enum OpType {
    EQ, IN, GT, GTE, LT, LTE, BETWEEN, MATCH, PHRASE, PREFIX, SUFFIX, FUZZY, GEO, ALL, TEXT_GLOBAL
}
