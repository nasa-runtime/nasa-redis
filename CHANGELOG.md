# 变更记录

本项目遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## 1.0.0 - 未发布

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
- **轻量分布式锁**（`LettuceDistributedLock`）：Lettuce + Lua 实现，不需要 Redisson。加锁/解锁/续期各一个
  Lua 脚本在 Redis 端原子执行；可重入；`TimingWheel` 看门狗持锁期间自动续 TTL；
  持有者标识为 JVM 实例 ID + 线程 ID；锁实例走对象池回收。
- **Stream 分区消费**（`RedisPartition`）：Kafka 式分区模型，N 个分区共享一组消费者组，
  每个分区由分布式锁独占，抢到锁的节点执行 XAUTOCLAIM + XREADGROUP。节点增减自动重分配。
- **RediSearch 查询 DSL**：注解声明索引 + `Criteria` 组合条件，支持 HASH / JSON / JSON_ARRAY 三种存储模式，
  enum 在各路径统一按 `@JsonValue` 渲染。
- **批量 Pipeline**（`LettucePipeline`）：用并行数组而非对象链表存放待执行命令，
  批量场景 cache-friendly 且不为每条命令分配包装对象。
- **`RedisProxy`** 命令代理，统一序列化、key 前缀与异常转换；为 String、Hash 与 ZSet 计数提供带 nonce 的
  幂等入口，同一逻辑计数器在默认 7 天窗口内最多变化一次，自动在 HPEXPIRE field TTL 与环形 HASH 桶之间
  选择并以共享 layout marker 固定集群布局。
- **雪花 ID**：Redis 负责分配 workerId，纯 JDK 生成算法由 `nasa-core` 提供；保留原有包名和 `@EnableSnowflake` 入口。
- **集群本地缓存失效**：基于 Redis pub/sub 的通知订阅。
- **MyBatis 二级缓存**：`MybatisCache` / `MybatisJedisCache`。

### 运行要求

- JDK 21 或更高版本，Maven 3.6.3 或更高版本。
- 编译产物为 Java 21 字节码，低于 JDK 21 的项目无法加载。
- RedisJob 默认 Sharded Pub/Sub 模式要求 Redis 7 或更高版本；广播降级模式最低要求 Redis 6.2。
- 依赖 `io.github.nasa-runtime:nasa-core`，日志只依赖 `slf4j-api`。

### 许可

采用 `Apache-2.0 OR MIT` 双许可证，使用方可任选其一。
