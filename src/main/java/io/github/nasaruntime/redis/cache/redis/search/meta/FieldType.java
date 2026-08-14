package io.github.nasaruntime.redis.cache.redis.search.meta;

/**
 * Nasa
 * RediSearch 字段类型
 */
public enum FieldType {

    /**
     * TAG：精确匹配，离散值
     */
    TAG,

    /**
     * NUMERIC：数值范围
     */
    NUMERIC,

    /**
     * TEXT：全文分词
     */
    TEXT,

    /**
     * GEO：地理位置
     */
    GEO,

    /**
     * ID：主键标识，不入索引 schema
     */
    ID
}
