package io.github.nasaruntime.redis.cache.redis.search.exception;

import io.github.nasaruntime.core.exception.BaseException;

import java.io.Serial;

/**
 * Nasa
 * RediSearch 操作异常
 */
@SuppressWarnings("unused")
public class RediSearchException extends BaseException {

    @Serial
    private static final long serialVersionUID = -4271599757459078915L;

    /**
     * 业务作用：以描述构造检索异常，用于把底层检索错误转成本组件的统一异常类型，
     * 使调用方不必依赖具体检索实现的异常体系。
     *
     * @param msg 异常描述
     */
    public RediSearchException(String msg) {
        super(msg);
    }

    /**
     * 业务作用：以描述构造检索异常，用于把底层检索错误转成本组件的统一异常类型，
     * 使调用方不必依赖具体检索实现的异常体系。
     *
     * @param msg    异常描述，其中的占位符由后续参数替换
     * @param params 用于替换描述中占位符的参数
     */
    public RediSearchException(String msg, Object... params) {
        super(msg, params);
    }

    /**
     * 业务作用：以状态码与描述构造检索异常，供调用方按状态码分类处置。
     *
     * @param code   异常状态码
     * @param msg    异常描述，其中的占位符由后续参数替换
     * @param params 用于替换描述中占位符的参数
     */
    public RediSearchException(Integer code, String msg, Object... params) {
        super(code, msg, params);
    }
}
