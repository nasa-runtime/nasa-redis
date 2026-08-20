package io.github.nasaruntime.redis.cache.redis.job;

import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 业务作用：发现 Spring Bean 上的 {@link RedisJob} 方法并转换为统一 Handler 合同，
 * 同时按注解声明的数据源把定义登记到对应的独立 Scheduler。
 */
public final class RedisJobAnnotationRegistrar implements SmartInitializingSingleton {

    private final ApplicationContext context;
    private final RedisJobSchedulers schedulers;

    /**
     * 业务作用：绑定应用上下文和多数据源调度器注册表。
     *
     * @param context    应用上下文
     * @param schedulers 多数据源调度器注册表
     */
    public RedisJobAnnotationRegistrar(ApplicationContext context, RedisJobSchedulers schedulers) {
        this.context = Objects.requireNonNull(context, "context must not be null");
        this.schedulers = Objects.requireNonNull(schedulers, "schedulers must not be null");
    }

    /**
     * 业务作用：在所有单例就绪后只实例化确有 RedisJob 方法的 Bean 并完成定义登记。
     *
     * <p>参数说明: 无。
     * <p>
     * 返回：无返回值；任一注解声明了未知数据源时抛出异常终止启动。
     */
    @Override
    public void afterSingletonsInstantiated() {
        // 第一阶段：全量发现并解析，全程不触碰 Redis。任何一个任务的签名、泛型或数据源不合法都在这里中止，
        // 此时尚未写入任何定义。若边发现边登记，前面已写入的定义不会随 Spring 启动失败回滚，
        // 会在 Redis 里留下没有 Handler 的陈旧定义，被下次部署收养或被其它存活节点继续扫描。
        List<Registration> registrations = new ArrayList<>();
        Map<JobIdentity, Method> claimed = new LinkedHashMap<>();
        String[] names = context.getBeanNamesForType(Object.class, false, false);
        for (String beanName : names) {
            Class<?> type = context.getType(beanName, false);
            if (type == null || type == RedisJobAnnotationRegistrar.class
                    || type == RedisJobScheduler.class || type == RedisJobSchedulers.class) continue;
            Map<Method, RedisJob> methods = MethodIntrospector.selectMethods(type,
                    (MethodIntrospector.MetadataLookup<RedisJob>) method ->
                            AnnotatedElementUtils.findMergedAnnotation(method, RedisJob.class));
            if (methods.isEmpty()) continue;
            Object bean = context.getBean(beanName);
            methods.forEach((method, annotation) -> {
                Registration registration = resolve(bean, method, annotation);
                claim(claimed, registration, method);
                registrations.add(registration);
            });
        }
        // 第二阶段：全部本地任务都已确认可定址且无重名，才允许写入 Redis
        for (Registration registration : registrations) registration.apply();
    }

    /**
     * 业务作用：按 (qualifier, namespace, jobName) 建立本地唯一索引，重名在任何 Redis 写入之前拒绝。
     *
     * <p>定义摘要不同的重名会在第二阶段被 Redis 拒绝，但那时前面的定义已经写入且建好了消费组，
     * 启动失败仍留下记录；定义完全相同的重名更危险——不会报错，后发现的方法会顶掉前一个 Handler，
     * 最终执行哪个 Bean 取决于 Bean 的发现顺序。两种情况都必须在这里挡住。
     *
     * @param claimed      已占用的任务身份到声明方法的映射
     * @param registration 当前待登记项
     * @param method       当前注解方法，用于定位冲突
     *                     返回：无返回值；身份重复时抛出异常。
     */
    private static void claim(Map<JobIdentity, Method> claimed, Registration registration, Method method) {
        RedisJobScheduler scheduler = registration.scheduler();
        JobIdentity identity = new JobIdentity(scheduler.qualifier(), scheduler.namespace(),
                registration.definition().name());
        Method previous = claimed.putIfAbsent(identity, method);
        if (previous != null) {
            throw new IllegalStateException("duplicate @RedisJob name '" + registration.definition().name()
                    + "' on qualifier=" + scheduler.qualifier() + " namespace=" + scheduler.namespace()
                    + "; declared by " + previous + " and " + method);
        }
    }

    /**
     * 业务作用：承载任务的本地唯一身份。
     *
     * <p>用记录而不是拼接字符串做键：分隔符方案在 namespace 或任务名本身含分隔符时会把两组不同身份
     * 折叠成同一个键，反而放过真正的重名。
     *
     * @param qualifier 语言无关的 source id
     * @param namespace 调度命名空间
     * @param jobName   任务名
     */
    private record JobIdentity(String qualifier, String namespace, String jobName) {
    }

    /**
     * 业务作用：校验注解方法签名、固定参数类型并解析目标数据源，产出一条尚未写入 Redis 的待登记项。
     *
     * @param bean       Spring Bean
     * @param method     声明方法
     * @param annotation 任务注解
     * @return 已完成全部本地校验的待登记项；签名非法或数据源不存在时抛出异常。
     */
    private Registration resolve(Object bean, Method method, RedisJob annotation) {
        validate(method);
        Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
        // 参数类型在登记期一次性解析并固定, 运行期不再依据消息内容推断, 使各语言按同一 Schema 解码
        ArgumentBinder[] binders = binders(invocable);
        RedisJobDefinition definition = RedisJobDefinition.from(annotation);
        RedisJobScheduler scheduler = schedulers.resolve(annotation.qualifier());
        return new Registration(scheduler, definition,
                jobContext -> invoke(bean, invocable, binders, jobContext));
    }

    /**
     * 业务作用：承载一条已完成本地校验、等待统一写入 Redis 的任务登记。
     *
     * @param scheduler  目标数据源的调度器
     * @param definition 任务定义
     * @param handler    绑定好参数注入方式的 Handler
     */
    private record Registration(RedisJobScheduler scheduler, RedisJobDefinition definition,
                                RedisJobHandler handler) {

        /**
         * 业务作用：把本条定义与 Handler 真正写入所属数据源。
         *
         * <p>参数说明: 无。
         * <p>
         * 返回：无返回值；Redis 拒绝登记时抛出异常。
         */
        private void apply() {
            scheduler.register(definition, handler);
        }
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
     * 业务作用：为每个方法参数固定一个取值方式，业务参数按方法泛型签名保留完整元素类型。
     *
     * @param method 代理可调用方法
     * @return 与参数一一对应的取值器。
     */
    private static ArgumentBinder[] binders(Method method) {
        Parameter[] parameters = method.getParameters();
        ArgumentBinder[] binders = new ArgumentBinder[parameters.length];
        for (int index = 0; index < parameters.length; index++) {
            Class<?> raw = parameters[index].getType();
            if (RedisJobContext.class.isAssignableFrom(raw)) {
                binders[index] = jobContext -> jobContext;
                continue;
            }
            if (raw == byte[].class) {
                binders[index] = RedisJobContext::rawParameter;
                continue;
            }
            Type declared = parameters[index].getParameterizedType();
            requireClosedType(declared, method);
            // 用 getParameterizedType 而非 getType: 退化成 List.class 会把元素解成 LinkedHashMap,
            // 业务拿到的集合元素类型与声明不符, 且该错误只在访问元素时才暴露
            TypeReference<Object> reference = new DeclaredTypeReference(declared);
            binders[index] = jobContext -> jobContext.parameter(reference);
        }
        return binders;
    }

    /**
     * 业务作用：拒绝无法形成确定跨语言契约的参数类型，把问题挡在登记期而不是运行期。
     *
     * <p>原始集合、类型变量和通配符都不能确定元素类型，Go、Rust 侧无从生成对应结构；
     * 确需动态 JSON 树时应显式声明 {@code JsonNode}，自定义编码使用 {@code rawParameter()}。
     *
     * @param type   参数的完整声明类型
     * @param method 所属注解方法，用于定位错误
     *               返回：类型闭合时正常返回。
     */
    private static void requireClosedType(Type type, Method method) {
        if (type instanceof TypeVariable<?> || type instanceof WildcardType) {
            throw new IllegalArgumentException(
                    "@RedisJob parameter must be a closed type, found " + type + " on " + method);
        }
        if (type instanceof GenericArrayType array) {
            requireClosedType(array.getGenericComponentType(), method);
            return;
        }
        if (type instanceof ParameterizedType parameterized) {
            for (Type argument : parameterized.getActualTypeArguments()) requireClosedType(argument, method);
            return;
        }
        // 只按"自身仍有未绑定类型参数"判定原始类型: 原始 List、Map 会命中这里;
        // 而 class WalletList extends ArrayList<String> 自身没有类型参数、元素类型已由父类确定, 必须放行
        if (type instanceof Class<?> raw && raw.getTypeParameters().length > 0) {
            throw new IllegalArgumentException(
                    "@RedisJob parameter must not be a raw generic type, found " + raw.getName() + " on " + method);
        }
    }

    /**
     * 业务作用：按登记期固定的取值方式注入参数，并把 void/null 统一为成功结果。
     *
     * @param bean    目标 Bean
     * @param method  代理可调用方法
     * @param binders 登记期固定的参数取值器
     * @param context 执行上下文
     * @return Handler 结果。
     * @throws Exception 业务异常原样交给执行器重试策略
     */
    private static RedisJobResult invoke(Object bean, Method method, ArgumentBinder[] binders,
                                         RedisJobContext context) throws Exception {
        Object[] arguments = new Object[binders.length];
        for (int index = 0; index < binders.length; index++) arguments[index] = binders[index].bind(context);
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

    /**
     * 业务作用：把登记期解析出的方法参数类型交给上下文解码，使自动注入与业务显式调用走同一条安全路径。
     */
    private static final class DeclaredTypeReference extends TypeReference<Object> {

        private final Type type;

        /**
         * 业务作用：保存方法签名上的完整泛型。
         *
         * @param type 参数的完整声明类型
         */
        private DeclaredTypeReference(Type type) {
            this.type = type;
        }

        /**
         * 业务作用：返回方法签名声明的类型而不是父类型变量。
         *
         * <p>参数说明: 无。
         *
         * @return 完整声明类型。
         */
        @Override
        public Type getType() {
            return type;
        }
    }

    /**
     * 业务作用：表示单个方法参数在运行期的取值方式，登记期确定后不再变化。
     */
    @FunctionalInterface
    private interface ArgumentBinder {

        /**
         * 业务作用：从执行上下文取得该位置的实参。
         *
         * @param context 执行上下文
         * @return 该参数的实参值。
         */
        Object bind(RedisJobContext context);
    }
}
