# 变更记录

本项目遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## 2.0.0

坐标 `io.github.nasa-runtime:nasa-redis`。

### 兼容性说明

- RedisJob 的任务身份从 `(namespace, jobName)` 扩展为 `(qualifier, namespace, jobName)`。Redis hash tag、
  `runId`、Fanout `executionKey` 和消息来源声明都会包含语言无关 source id；`1.0.0` 与 `2.0.0` 使用
  不同键空间，不能在运行中混合接管同一组任务。
- `RedisJobContext` 增加数据源 qualifier 与泛型 `TypeReference` 解码合同；自定义实现需要同步提供这些能力。
- RedisJob 不再随 `@EnableRedis` 隐式启用，业务需要添加 `@EnableRedisJob`；框架不再暴露默认
  `RedisJobScheduler` Bean，`@RedisJob.qualifier` 改为必填，编程式访问使用
  `RedisJobSchedulers.scheduler(sourceId)`；根级 `nasa.redis.job.qualifier` 取消，直接构造时从传入的
  RedisProxy 冻结 source id。
- 静态调度门面绑定 JVM 内唯一的活动 `RedisJobSchedulers` 管理器；并行建立第二套管理器会被拒绝，原容器
  关闭后静态入口与旧 Scheduler 引用都不能继续作为新上下文的运行时使用。
- `RedisJobAnnotationRegistrar`、`RedisJobIdentifiers` 与 `RedisJobKeyspace` 的公开构造或方法合同随多数据源
  模型调整。直接实例化这些类型的应用需要按当前签名迁移。
- 升级前必须停止旧节点继续扫描并排空、取消或终态化旧 Run，再以新键空间重新登记任务；跨版本外部副作用
  仍需业务幂等约束保护。详细步骤见 [README](README.md#从-100-升级到-200) 与
  [RedisJob 架构与运行指南](REDIS-JOB.md#从-100-升级)。

### RedisJob

- 新增 `RedisJobSchedulers`，按实际引用的 qualifier 惰性建立并隔离调度器、注册表、线程池与 Fanout；
  不遍历全部 RedisProxy，不提供默认 source，并在启动阶段拒绝不存在的数据源。
- 默认串行模式使用 Redis 槽位保证同名任务在集群中单活；fixed rate 长时间停机后按当前时刻闭式计算下一个
  网格点，避免扫描上限使单个过期任务阻断整轮调度。
- Fanout 按执行器能力与契约快照分片，持久化投递信封和 `assignmentEpoch`；普通通知重试与容量背压使用独立
  预算，已接受但尚未完成的分片不会因 Worker 在线而无限等待。
- 删除后的存量 Run 由有界回收路径逐步终态化并移出可见索引，避免已删除任务持续占用扫描预算。
- RedisJob 不可变布局由共享 marker 校验；实例参数或 source id 不一致时在加入调度前拒绝启动。

### RedisProxy

- nonce 幂等计数在 HASH_BUCKET 布局下保留完整窗口凭据，系统时钟回拨不会提前接受已使用 nonce。
- 拒绝无法安全取绝对值的 `Long.MIN_VALUE` nonce，避免不同输入落入同一桶位。

### 文档与发布合同

- 补充运行架构、控制面与执行面顺序、集群串行、Fanout 背压、故障恢复、多数据源隔离、观测与能力边界。
- 发布流程增加 Central 目标版本占用检查，并要求直接核验主制品、源码制品、Javadoc 制品及归档内公开文档。

## 1.0.0 - 2026-08-14

首个公开版本。

坐标 `io.github.nasa-runtime:nasa-redis`。

### 核心能力

- **RedisJob 分布式任务调度**：不选举应用主节点，以 Redis `TIME`、分片 hash tag、Lua CAS、
  Dispatch Stream、租约和 `XAUTOCLAIM` 协调 Cron、fixed rate、fixed delay 与手工幂等任务；
  提供串行排队、运行中丢弃、并行、误触发补偿、有限重试、协作式取消、定义冲突门禁和有界回收。
- **RedisJob 集群 Fanout**：根任务按实际登记 Worker 能力、契约修订号、Schema 与 codec 冻结执行器快照，
  再按成员数一对一切分参数；持久 inbox、Pub/Sub 回执、重发、`assignmentEpoch` 重分配、三种失败策略
  和根终态对账共同保证通知丢失后仍可恢复。
- **跨语言参数合同**：Job JSON 使用独立且关闭 Default Typing 的 Jackson 映射器，并拒绝 JVM 类型元数据；
  同时支持 Protobuf 与 RAW 字节透传。当前制品提供 Java 运行时，不包含其它语言 SDK。
- **轻量分布式锁**（`LettuceDistributedLock`）：Lettuce + Lua 实现，不需要 Redisson。加锁、解锁与续期分别
  由 Lua 脚本在 Redis 端原子执行；支持可重入；`TimingWheel` 看门狗持锁期间自动续 TTL；
  持有者标识为 JVM 实例 ID + 线程 ID；锁实例走对象池回收。
- **Stream 分区消费**（`RedisPartition`）：Kafka 式分区模型，N 个分区共享一组消费者组，
  每个分区由分布式锁独占，获得锁的节点执行 `XAUTOCLAIM` 与 `XREADGROUP`。节点增减自动重分配。
- **RediSearch 查询 DSL**：注解声明索引与 `Criteria` 组合条件，支持 HASH、JSON 与 JSON_ARRAY 三种存储模式，
  enum 在各路径统一按 `@JsonValue` 渲染。
- **批量 Pipeline**（`LettucePipeline`）：用并行数组存放待执行命令，批量场景不为每条命令分配包装对象。
- **`RedisProxy`**：统一序列化、key 前缀与异常转换；为 String、Hash 与 ZSet 计数提供带 nonce 的幂等入口，
  同一逻辑计数器在默认七天窗口内最多变化一次，并在 HPEXPIRE field TTL 与环形 HASH 桶之间选择布局。
- **雪花 ID**：Redis 负责分配 workerId，纯 JDK 生成算法由 `nasa-core` 提供；保留原有包名和
  `@EnableSnowflake` 入口。
- **集群本地缓存失效**：基于 Redis Pub/Sub 的通知订阅。
- **MyBatis 二级缓存**：`MybatisCache` 与 `MybatisJedisCache`。

### 运行要求

- JDK 21 或更高版本，Maven 3.6.3 或更高版本。
- 编译产物为 Java 21 字节码，低于 JDK 21 的项目无法加载。
- RedisJob 默认 Sharded Pub/Sub 模式要求 Redis 7 或更高版本；广播降级模式最低要求 Redis 6.2。
- 依赖 `io.github.nasa-runtime:nasa-core`，日志只依赖 `slf4j-api`。

### 许可

采用 `Apache-2.0 OR MIT` 双许可证，使用方可任选其一。
