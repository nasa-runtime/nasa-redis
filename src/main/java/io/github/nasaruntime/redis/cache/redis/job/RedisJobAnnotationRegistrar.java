package io.github.nasaruntime.redis.cache.redis.job;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * 业务作用：发现 Spring Bean 上的 {@link RedisJob} 方法并转换为统一 Handler 合同。
 */
public final class RedisJobAnnotationRegistrar implements SmartInitializingSingleton {

    private final ApplicationContext context;
    private final RedisJobScheduler scheduler;

    /**
     * 业务作用：绑定应用上下文和任务调度门面。
     *
     * @param context   应用上下文
     * @param scheduler 调度门面
     */
    public RedisJobAnnotationRegistrar(ApplicationContext context, RedisJobScheduler scheduler) {
        this.context = Objects.requireNonNull(context, "context must not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    }

    /**
     * 业务作用：在所有单例就绪后只实例化确有 RedisJob 方法的 Bean 并完成定义登记。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值。
     */
    @Override
    public void afterSingletonsInstantiated() {
        String[] names = context.getBeanNamesForType(Object.class, false, false);
        for (String beanName : names) {
            Class<?> type = context.getType(beanName, false);
            if (type == null || type == RedisJobAnnotationRegistrar.class || type == RedisJobScheduler.class) continue;
            Map<Method, RedisJob> methods = MethodIntrospector.selectMethods(type,
                    (MethodIntrospector.MetadataLookup<RedisJob>) method ->
                            AnnotatedElementUtils.findMergedAnnotation(method, RedisJob.class));
            if (methods.isEmpty()) continue;
            Object bean = context.getBean(beanName);
            methods.forEach((method, annotation) -> register(bean, method, annotation));
        }
    }

    /**
     * 业务作用：校验注解方法签名并以代理可调用方法建立 Handler。
     *
     * @param bean       Spring Bean
     * @param method     声明方法
     * @param annotation 任务注解
     *                   返回：无返回值。
     */
    private void register(Object bean, Method method, RedisJob annotation) {
        validate(method);
        Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
        RedisJobDefinition definition = RedisJobDefinition.from(annotation);
        scheduler.register(definition, jobContext -> invoke(bean, invocable, jobContext));
    }

    /**
     * 业务作用：限制注解方法只使用上下文和一个确定业务参数，避免运行期猜测绑定方式。
     *
     * @param method 注解方法
     *               返回：签名合法时正常返回。
     */
    private static void validate(Method method) {
        if (method.getParameterCount() > 2) {
            throw new IllegalArgumentException("@RedisJob method accepts at most RedisJobContext and one parameter: " + method);
        }
        long contexts = Arrays.stream(method.getParameterTypes())
                .filter(RedisJobContext.class::isAssignableFrom).count();
        if (contexts > 1 || method.getParameterCount() == 2 && contexts != 1) {
            throw new IllegalArgumentException("two-argument @RedisJob method must contain one RedisJobContext: " + method);
        }
        Class<?> returnType = method.getReturnType();
        if (returnType != void.class && !RedisJobResult.class.isAssignableFrom(returnType)) {
            throw new IllegalArgumentException("@RedisJob method must return void or RedisJobResult: " + method);
        }
    }

    /**
     * 业务作用：按签名注入执行上下文和已登记参数类型，并把 void/null 统一为成功结果。
     *
     * @param bean    目标 Bean
     * @param method  代理可调用方法
     * @param context 执行上下文
     * @return Handler 结果。
     * @throws Exception 业务异常原样交给执行器重试策略
     */
    private static RedisJobResult invoke(Object bean, Method method, RedisJobContext context) throws Exception {
        Object[] arguments = new Object[method.getParameterCount()];
        Parameter[] parameters = method.getParameters();
        for (int index = 0; index < parameters.length; index++) {
            Class<?> parameterType = parameters[index].getType();
            arguments[index] = RedisJobContext.class.isAssignableFrom(parameterType)
                    ? context : context.parameter(parameterType);
        }
        try {
            Object result = method.invoke(bean, arguments);
            return result instanceof RedisJobResult jobResult ? jobResult : RedisJobResult.success();
        } catch (InvocationTargetException error) {
            Throwable target = error.getTargetException();
            if (target instanceof Exception exception) throw exception;
            if (target instanceof Error fatal) throw fatal;
            throw new IllegalStateException(target);
        } catch (IllegalAccessException error) {
            throw new IllegalStateException("cannot invoke @RedisJob method: " + method, error);
        }
    }
}
