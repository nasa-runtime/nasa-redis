# nasa-redis

面向 JDK 21 的 Redis 基础设施库。核心能力是基于 Redis Cluster 的无中心分布式任务调度：
通过 `@RedisJob` 声明 Cron、fixed rate、fixed delay 或手工任务，以持久 Run、租约、fencing token、
Stream 派发和 `XAUTOCLAIM` 完成故障恢复；根任务还可以按实际具备某个 Worker 能力的节点冻结快照，
向全体目标一对一 Fanout 分片，并对通知回执、重发、重分配、取消和聚合终态负责。

组件同时提供 `RedisProxy` 命令代理及面向余额、额度等资金字段的 nonce 幂等计数，默认在 7 天窗口内保证
同一业务事件最多改变一次计数；此外还包括轻量分布式锁、Stream 分区消费、RediSearch 查询 DSL、
显式批量 Pipeline、雪花 ID 与集群缓存失效通知。

```xml
<dependency>
    <groupId>io.github.nasa-runtime</groupId>
    <artifactId>nasa-redis</artifactId>
    <version>1.0.0</version>
</dependency>
```

要求 JDK 21+、Maven 3.6.3+。RedisJob 默认使用 Redis 7+ 的 Sharded Pub/Sub；选择
`BROADCAST` 降级模式时最低要求 Redis 6.2，并需要评估普通 Pub/Sub 在 Cluster 总线上的放大量。

## 接入前必读：必须允许 Bean 定义覆盖

本组件用 `@EnableRedis` 装配自己的 `redisTemplate`，而 Spring Boot 的 `RedisAutoConfiguration`
也定义了同名 Bean。两者的注册顺序决定了 Boot 的 `@ConditionalOnMissingBean` 来不及生效，
因此**不开启覆盖时应用直接启动失败**：

```text
The bean 'redisTemplate', defined in NasaLettuceConfig, could not be registered.
A bean with that name has already been defined ... and overriding is disabled.
```

在应用配置中显式允许覆盖：

```yaml
spring:
  main:
    allow-bean-definition-overriding: true
```

这是**硬性前提**，不是可选优化。该开关是应用级的，开启后同名 Bean 一律后者覆盖前者，
接入方需自行确认工程内没有其它依赖「同名 Bean 冲突即报错」这一保护的地方。


---

## 核心能力

### RedisJob —— 无中心调度、执行权 fencing 与集群 Fanout

RedisJob 不选举应用主节点，也不依赖数据库调度中心。多个实例可以同时扫描自己登记任务所在的固定分片，
真正的触发、领取、完成和恢复都由同 slot Lua 原子复验；重复扫描只会命中同一个持久 Run，不会产生第二个
有效执行权。

```text
@RedisJob 定义
      │
      ▼
分片 Schedule ZSET ──到期扫描──▶ Lua CAS 创建 Run ──▶ Worker Dispatch Stream
                                                        │
                                                        ▼
                              STARTED / ADOPTED + attemptToken + lease
                                                        │
                                    ┌───────────────────┴──────────────────┐
                                    ▼                                      ▼
                              Handler 完成                          租约到期 / PEL 接管
                                    │                                      │
                                    └──────────────▶ 重试或终态 ◀──────────┘

根 Handler: context.fanout(worker)
      │
      ▼
兼容能力快照 ──▶ 按成员数分片 ──▶ 每节点持久 inbox + Pub/Sub 通知/回执
                                      │
                                      ▼
                             分片 Worker 执行与聚合
```

关键保证如下：

- **没有单点调度主节点**：分片扫描可以重复，状态迁移依靠 Redis `TIME`、Lua CAS 与 Redis Cluster hash tag 收敛。
- **执行权可判定**：每个 attempt 都携带单调 `attemptToken`；旧 owner 的续期和完成提交会被拒绝。
- **至少一次恢复**：派发消息、可见性索引、租约索引和 `XAUTOCLAIM` 共同覆盖进程退出与响应丢失，因此 Handler 必须幂等。
- **定义冲突不按启动顺序裁决**：同名任务的修订号与规范摘要持久化；相同修订号但不同定义会进入冲突并停止触发。
- **Fanout 只选择兼容节点**：目标必须登记同一 Worker、`contractRevision`、`schemaId` 和 `codec`，没有该能力的 Java、Go 或 Rust 节点不会收到分片。
- **稳定分片幂等键**：`executionKey` 在通知重发、执行重试和节点重分配期间保持不变；`assignmentEpoch` 只在目标变化时递增。
- **跨语言 JSON 不携带 JVM 类型信息**：Job 使用独立 Jackson 映射器，强制关闭 Default Typing，并拒绝 `@class` / `@type` 字段。

#### Spring 接入

应用入口添加 `@EnableRedis`，并显式开启 RedisJob。`RedisJobConfiguration` 会装配一个
`RedisJobScheduler`，`RedisJobAnnotationRegistrar` 在单例就绪后发现 Spring Bean 上的注解方法；
调度器作为 `SmartLifecycle` 随容器启动、drain 和关闭。

```java
@SpringBootApplication
@EnableRedis
public class Application {
}
```

```yaml
spring:
  application:
    name: settlement-service
  main:
    allow-bean-definition-overriding: true

nasa:
  redis:
    properties:
      primary:
        mode: cluster
        password: ${REDIS_PASSWORD}
        cluster:
          nodes:
            - redis-0:6379
            - redis-1:6379
            - redis-2:6379
    job:
      enabled: true
      namespace: settlement-service
      qualifier: primary
      application-name: ${spring.application.name}
      instance-identity: ${HOSTNAME:}
      shard-count: 64
      fanout-bucket-count: 32
      pubsub-mode: SHARDED
      wire:
        json:
          default-typing: false
```

`namespace`、`shard-count` 和 `fanout-bucket-count` 共同决定持久键路由。一个已经写入任务数据的命名空间
不能原地改变分片数或 Fanout 桶数。`instance-identity` 应在同一逻辑节点重启后保持稳定；留空时回退到主机名，
容器平台需要确认该主机名是否满足自身的稳定身份要求。

注解方法只允许 `RedisJobContext` 和一个业务参数，返回 `void` 或 `RedisJobResult`。以下根任务先收集钱包，
再按当前能够执行 `contract-wallet-sweep-worker` 的成员数切分；Worker 不存在的节点不会进入能力快照：

```java
@Component
public class WalletSweepJobs {

    @RedisJob(
            name = "contract-wallet-sweep",
            cron = "0/30 * * * * *",
            zone = "UTC",
            concurrency = RedisJobConcurrency.SERIAL_QUEUE,
            misfire = RedisJobMisfire.FIRE_ONCE_NOW,
            timeoutMs = 120_000L,
            fanoutReceiptTimeoutMs = 2_000L,
            fanoutReceiptMaxRetries = 3,
            fanoutFailurePolicy = RedisJobFanoutFailurePolicy.REASSIGN_ON_FAILURE
    )
    public RedisJobResult sweep(RedisJobContext context) {
        List<String> wallets = loadWallets();
        return context.fanout("contract-wallet-sweep-worker")
                .partition(wallets, RedisJobPartitioners.balanced())
                .dispatch();
    }

    @RedisJob(
            name = "contract-wallet-sweep-worker",
            trigger = RedisJobTrigger.FANOUT_ONLY,
            schema = "contract-wallet-sweep-shard",
            codecs = RedisJobWireCodec.JSON,
            timeoutMs = 120_000L
    )
    public RedisJobResult sweepShard(RedisJobContext context, List<?> wallets) {
        RedisJobFanoutContext shard = context.fanoutContext().orElseThrow();
        context.checkpoint();
        sweepWallets(wallets, shard.executionKey(), context.attemptToken());
        return RedisJobResult.success();
    }
}
```

`checkpoint()` 只在业务安全点检查取消与本地执行权。它不能撤销已经发出的数据库、HTTP、钱包或消息副作用。
资金与订单类操作应把普通 Run 的 `runId` 或 Fanout 的 `executionKey` 写入目标系统唯一键，并在能够支持
fencing 的目标资源上同时校验 `attemptToken`。Redis 调度状态同一时刻只承认一个当前 attempt；已经失权的
业务线程仍可能运行到下一个 `checkpoint()`，因此框架不承诺外部副作用 exactly-once。

#### Fanout 失败策略

| 策略 | 目标不可用时的行为 | 根结果 |
|---|---|---|
| `REASSIGN_ON_FAILURE` | 递增 `assignmentEpoch`，选择仍然兼容的其它节点 | 全部分片成功时 `SUCCEEDED`，否则按最终结果收敛 |
| `STRICT_SNAPSHOT` | 保留冻结快照，不换目标；持续恢复直到根等待超时或被取消 | 超时后外层 Run 为 `FAILED` |
| `BEST_EFFORT` | 无法执行的分片记为 `SKIPPED`，其余分片继续 | 桶内为 `PARTIAL_FAILED`；外层 Run 为 `FAILED`，`resultCode=PARTIAL_FAILED` |

Pub/Sub 只负责低延迟唤醒，持久 inbox、receipt deadline、ready、lease 和 root 看门狗负责通知丢失后的恢复。
目标节点接受分片时先用 Lua 持久化确认，再通过 Pub/Sub 向根节点发送回执信号。根任务的
`fanoutReceiptTimeoutMs` 默认是 `2000` 毫秒，`fanoutReceiptMaxRetries` 默认允许首次通知后重发 `3` 次；
每次超时都先复验持久确认，次数耗尽后才按 `fanoutFailurePolicy` 重分配、等待原快照或跳过。
每个分片的 `seq`、`executionKey`、assignment、attempt 与结果随 Fanout 记录保留，到
`fanout-retention-ms` 后由有界清理删除，不会永久驻留 Redis。

#### 手工触发、控制与观测

```java
String runId = scheduler.triggerJson("contract-wallet-sweep", requestId, parameter);
RedisJobRun run = scheduler.findRun("contract-wallet-sweep", runId).orElseThrow();

scheduler.pause("contract-wallet-sweep");
scheduler.resume("contract-wallet-sweep");
scheduler.cancel("contract-wallet-sweep", runId);

RedisJobHealth health = scheduler.health();
Map<String, Long> metrics = scheduler.metrics().snapshot();
```

`requestId` 是手工触发的幂等键。`pause` 停止新触发与尚未 start 的积压，不撤销已经取得的 attempt；
取消是协作式的。调度器启动后到首次成功心跳之间 `health()` 可能短暂返回 `DEGRADED`，最长为一个
`heartbeat-ms` 周期；接入 readiness 时应保留相应启动宽限，并把 `DRAINING` 与 `DOWN` 分开处理。

详细键模型、状态机、失败边界、配置关系和运维约束见 [RedisJob 架构与运行指南](REDIS-JOB.md)。

### RedisProxy nonce 幂等计数

普通 `increment`、`decrement`、`hIncrBy`、`hDecrBy` 和 `zIncrBy` 每收到一次命令就改变一次值；
在消息重投、超时重试或任务恢复场景中，调用方无法确定上一次是否已经生效。对应的 `Idempotent` 入口把
目标变化和 nonce 凭证登记放进同一段 Lua：窗口内同一逻辑计数器与 nonce 只执行一次，所有重放都正常
返回首次执行后的值，不把重复请求当作异常。

```java
long balance = redisProxy.hDecrByIdempotent(
        "wallet:{1001}",
        "available:USDT",
        2500L,
        "settle:SPOT:order-9001:fill-31:quote-settlement"
);

double score = redisProxy.zIncrByIdempotent(
        "risk-score:{1001}", accountId, 1.25,
        "risk-event:event-721"
);
```

公开入口包括字符串计数器的 `incrementIdempotent` / `decrementIdempotent`、HASH 字段的
`hIncrByIdempotent` / `hDecrByIdempotent` 和 ZSET 的 `zIncrByIdempotent`。操作方向和增量不参与幂等身份：
同一 nonce 首次扣减后，即使重试误传成增加或换了金额，也只能得到首次扣减结果。ZSET 成员按当前
`valueSerializer` 直接传入 Lua，与普通 `zIncrBy` 操作同一成员字节。

```yaml
nasa:
  redis-proxy:
    idempotent-counter:
      ledger-key-prefix: rpidem
      nonce-ttl-ms: 604800000       # 7 天
      ttl-mode: AUTO
      bucket-span-ms: 86400000      # 整桶回收时每桶 1 天
      ledger-shards: 256
```

`AUTO` 会检查全部 Redis master：全部支持 `HPEXPIRE` 时使用单 HASH field TTL，否则保守使用固定数量的
环形 HASH 桶和整桶 `PEXPIRE`。解析结果与 TTL、桶跨度、shard 数一起写入共享 layout marker；其它应用
节点必须服从同一布局，运行期间不允许改变这些参数。目标键和 sidecar 账本键始终作为 Lua `KEYS` 传入，
框架按目标键的真实 CRC16 slot 派生 canonical hash tag，并在客户端与 Redis Cluster 两侧复验同槽。

跨语言实现幂等身份时必须逐字节一致：`target-digest` 是目标键序列化字节的 SHA-256 小写 hex；凭证 field
依次写入结构类型、字段或成员、nonce 的 4 字节大端长度与内容后取原始 SHA-256，其中结构类型固定为
String=`0x01`、Hash=`0x02`、ZSet=`0x03`，nonce 使用 UTF-8。canonical slot token 按 `s` 加从零递增的
base36 候选枚举，每个 slot 取第一个 CRC16 命中的 token。`ledger-shard` 对结构类型与字段或成员使用相同的
长度前缀摘要，取前 4 字节大端整数后按位与 `ledger-shards - 1`；方向和增量不得进入凭证摘要。

`nonce` 必须来自订单号、成交号、事件号等重试期间不变的业务事实；每次重新生成随机值等于没有幂等保护。
默认保证窗口是 7 天，过期后的历史重放会再次执行。该能力不提供余额非负校验，也不能替代持久资金流水、
唯一约束或 Redis 自身的持久化保障。原生命令因类型、数值、NaN 或 int64 越界被拒绝时，不会登记 nonce，
调用方修正目标后可以用相同 nonce 重试；拒绝通过 `RedisIdempotentCounterException.getCode()` 返回结构化原因。
`redisProxy.idempotentCounterMetrics()` 提供 applied、duplicate、按拒绝原因分类、TTL 未确认和能力探测失败计数。
`snapshot()` 同时返回 `redis_idempotent_counter_resolved_ttl_mode` 与
`redis_idempotent_counter_layout_marker`，运维可以直接核对当前节点最终采用的布局及其共享配置指纹，
不需要根据 Redis 键形态反推；它不绑定特定指标库，应用可连同 RedisProxy qualifier 桥接到 Micrometer
或 Prometheus。

使用幂等入口写入的目标计数器与其 sidecar 凭证账本是一个一致性单元。凭证窗口内禁止绕过应用直接对目标键
执行 `DEL`、`SET` 或字段覆盖：后续重投仍会返回凭证保存的首次结果，而该结果可能已经与手工改写后的目标值
不一致。紧急校正必须先停止相关写入与历史重投，以持久业务流水为依据同时重建目标值和对应账本，并确保旧事件
不会再次投递；单独删除凭证同样不安全，因为它会重新开放窗口内旧 nonce 的执行权。本组件不提供在线绕过该
约束的管理接口。

### 分布式锁 —— Lettuce + Lua，不需要 Redisson

锁入口是容器里的 `DistributedLock` Bean（`@EnableRedis` 装配），也可用 `LettuceDistributedLock.load(redisProxy)` 直接取：

```java
// 推荐：加解锁成对由框架保证
distributedLock.lockAndUnlock("order:1001", () -> {
    // 临界区
});

// 抢不到锁就走降级分支
String r = distributedLock.tryLockAndUnlock("order:1001", () -> doWork(), "skipped");

// 需要自己控制时用 getLock，必须 try/finally 配对
Lock lock = distributedLock.getLock("order:1001");
lock.lock();
try {
    // 临界区
} finally {
    lock.unlock();
}
```

`getLock` 返回的锁实例来自对象池，**`unlock()` 之后即被回收，不可再次 `lock()`**——同一个临界区要重入请在 `unlock()` 前重复 `lock()`。锁不实现 `AutoCloseable`，不能用 try-with-resources。

- 加锁、解锁、续期各一个 Lua 脚本，在 Redis 端原子执行；
- **可重入**：用 Redis Hash 记录（key = 锁名，field = 持有者标识，value = 重入次数）；
- **看门狗续期**：持锁期间由 `TimingWheel` 定时延长 TTL，避免业务没做完锁先过期；
- **持有者标识 = JVM 实例 ID + 线程 ID**，跨节点与跨线程都唯一；
- 锁实例走对象池回收，高并发下不产生锁对象垃圾。

### Stream 分区消费 —— Kafka 式分区模型

N 个分区 stream 共享一组消费者组，每个分区由分布式锁独占：抢到锁的节点执行 `XAUTOCLAIM` + `XREADGROUP` 消费该分区。节点增减时分区自动重新分配。

```yaml
nasa:
  redis:
    properties:
      primary:                          # 分区配置挂在数据源的 stream 节点下
        stream:
          partition:
            enabled: true               # 总开关，不开则整套分区消费不启动
            default-group: SINGLE-CONSUME
            count: 64                   # 默认共享组的分区数
            rebalance-ms: 10000
            min-idle-ms: 30000
            holds-check-interval-ms: 5000   # holds 自检最小间隔，限流以免 NOBLOCK 空轮询打爆 EVAL
            groups:
              contract:settlement:      # → stream = SINGLE-CONSUME:contract:settlement:0..63
                count: 64
                batch-size: 200
```

消费侧实现 `RedisEventBatchListener`（或 Single 形态）并把 `mode()` 标成 `ConsumeMode.PARTITION`，注册为 Bean 即可——分区 stream 与消费组由 `RedisProxy` 在初始化阶段按 listener 声明的 topic 自动建好，业务侧不需要显式调 `init` / `isolate`：

```java
@Component
public class SettlementListener implements RedisEventBatchListener<Order> {
    @Override public String[] topics() { return new String[]{"contract:settlement"}; }
    @Override public String event()    { return "open-position"; }
    @Override public ConsumeMode mode(){ return ConsumeMode.PARTITION; }
    @Override public void onEvent(List<Order> orders) { ... }   // 同分区串行
}

// 发布：分区键决定落到哪个分区，同键必然同分区、同节点、同线程串行
RedisPartition.load(redisProxy).publish("contract:settlement", "open-position", uid, order);
```

`XAUTOCLAIM` 保证节点宕机后其未 ACK 的消息会被其它节点接管，**因此消费逻辑必须幂等**。

再平衡按 `分区数 / 存活节点数` 计算每节点的持有上限，存活节点数由各节点在 `{stream前缀}:nodes`
这个 ZSET 上心跳续约得出，死节点在三个再平衡周期内被自动剔除。

**同机多实例必须隔离节点标识**。节点标识来自 `ME.sequence()`，它把 16 位标识持久化在
`logging.file.path`（未配置时为 `logs/<nasa.application.name>`）下的 `sequence` 文件里。
同一台机器上的多个实例若共用该目录，会读到同一个标识，存活节点数因而恒为 1，
表现为**第一个起来的实例抢光全部分区、其余实例空转**。同机部署多实例时给每个实例配独立目录：

```bash
java -Dnasa.sequence.dir=/data/app-1/seq -jar app.jar
# 或各实例配不同的 logging.file.path
```

一机一实例（容器、独立主机）的部署不受此影响。

### 普通 Stream 消费与保留期

普通 `RedisProxy.subscribe(...)` 消费组可分别配置拉取参数与确认方式：

```yaml
nasa:
  redis:
    properties:
      primary:
        stream:
          auto-trim-enabled: false      # 默认关闭；开启后每个应用实例都会发起幂等 XTRIM
          auto-trim-rate: 60000
          data-expire-millis: 3600000
          group:
            order-stream:
              settlement:
                consumers: 2
                auto-acknowledge: false
                batch-size: 100
                poll-timeout: 500
```

`auto-acknowledge: true` 对应 Redis `XREADGROUP NOACK`：消息读取时不进入 PEL，业务回调失败也不能依靠
待处理消息重投，属于至多一次语义。需要失败接管时设为 `false`，并在业务处理成功后调用
`redisProxy.ack(stream, group, ids...)`；消费逻辑仍须幂等。

自动裁剪默认关闭。开启后无需应用级选主，每个实例都可对同一 Stream 执行幂等 `XTRIM MINID`；
`data-expire-millis` 必须覆盖允许的最大消费积压，否则尚未处理的旧消息也会被永久删除。
本组件不提供基于过期心跳的应用级唯一任务选主；需要全集群唯一副作用时，应使用带 fencing token
并在目标资源侧校验 token 的协调方案，单靠“当前主节点”判断不能阻止失权节点继续写入。

### RediSearch 查询 DSL

用注解声明索引，用 `Criteria` 组合查询条件，避免手写 FT.SEARCH 字符串：

```java
RediSearch rediSearch = RediSearch.load(redisProxy);
rediSearch.ensureIndex(Order.class);          // 幂等，索引不存在才建
rediSearch.save(order);

// 条件之间的「与」在 RsQuery 上组合，不是在 Criteria 上
RsQuery q = RsQuery.query(Criteria.where(Order::getStatus).is(PAID))
        .and(Criteria.where(Order::getAmount).between(100, 1000))
        .sortByDesc("createTime")
        .page(1, 20);         // page 从 1 开始，传 0 抛 IllegalArgumentException

List<Order> list = rediSearch.find(q, Order.class);
long total = rediSearch.count(q, Order.class);
```

实体用 `@RsDocument(index = "...", prefix = "...")` 声明索引，字段用 `@RsId` / `@TextField` / `@NumericField` / `@TagField` 标注。

条件组合的边界：`Criteria.and` / `Criteria.or` **调用即抛异常**，条件之间只能在 `RsQuery` 上按「与」组合。
单字段的多值取或用 `Criteria.in(...)`；跨字段的「或」本 DSL 不生成，需自行拼 FT.SEARCH 查询串。
排序方面 `RsQuery.sortBy` 只接受单字段（FT.SEARCH 的限制），多字段排序用 `RsAggregation.sortBy`。
分页有两个起算点：`page(int, int)` 从 1 开始，`page(Pageable)` 沿用 Spring 的 0 起算——
从 `Pageable` 转过来必须走后者，把 `pageable.getPageNumber()` 直接喂给前者会因传入 0 而抛。

支持 HASH / JSON / JSON_ARRAY 三种存储模式。索引字段与实体字段的映射规则有几条需要注意：

- `@JsonIgnore` 标注的索引字段是 **schema-only**：进 FT.CREATE schema、可被 `Criteria.where` 查询，但不写入 HASH、也不反序列化赋值。典型用法是为嵌套 JSON 里的数据声明索引。
- `@RsId` 与 `@JsonIgnore` 同时标注会 **fail-fast**——主键必须能往返序列化。
- `@JsonCreator` 标注的静态工厂会被用于反序列化，可在工厂内接入对象池。
- enum 在 JSON 值、HASH 值、查询值、key 动态段与 bucket 计算中**统一按 `@JsonValue` 渲染**（无该注解则用 `name()`），保证各路径一致。

### 批量 Pipeline

```java
Actuator act = LettucePipeline.open(redisProxy, null);
try {
    act.hSetAsync(key, hashKey, value);   // 排入批次，不等结果
    act.hSet(key, otherKey, value2);      // 排入批次并等结果，失败可感知
} finally {
    act.pipeline();                       // 收尾：统一发出。必须调用，否则命令留在线程上
}
```

`open()` 是线程级的：收尾前该线程上的命令一律缓冲，收尾后恢复直通。批次只能由 `@EnableRedis` 装配出的
`RedisProxy` 使用——手工 `new` 的实例没有 pipeline 连接池，调用批次 API 会直接报错点名原因。

显式 `LettucePipeline.open(...)` 不依赖 `RedisProxy.before()`。另一条按 OPS 并发阈值把普通命令自动切到
批次队列的路径依赖 `RedisProxy` 的 `Initialization.before()`：同时使用 `nasa-spring-boot-starter` 时由其
初始化编排自动执行；只使用 `@EnableRedis` 时不会自动启用。两条路径不能混为同一种能力。

内部用**并行数组**而非对象链表存放待执行命令，数组连续、cache-friendly，批量场景下比逐条发送显著更快，且不为每条命令分配包装对象。

### 其它

- **`RedisProxy`** —— 命令代理，统一序列化、key 前缀与异常转换。
- **`JedisSnowflake` / `JdkSnowflake`** —— Redis 只负责分配互不重复的 workerId；`JdkSnowflake` 的纯 JDK 生成算法由 `nasa-core` 提供，原有包名和 `@EnableSnowflake` 入口保持不变。
- **`RefreshCacheSub`** —— 基于 Redis pub/sub 的集群本地缓存失效通知。
- **MyBatis 二级缓存**（`MybatisCache` / `MybatisJedisCache`）—— MyBatis 依赖是 `optional`，不使用 MyBatis 的调用方不会被拖入；要用这两个类时自行声明 `org.mybatis:mybatis`。

## 客户端选择

同时集成了三种客户端，按能力各取所长：

| 客户端 | 用途 |
|---|---|
| spring-data-redis（Lettuce） | 常规命令代理、模板操作 |
| 原生 Lettuce | 分布式锁、Pipeline、Stream 分区消费 |
| Jedis | 雪花 ID 的 Lua 脚本执行 |
| Redisson | `@EnableRedisson` 时的高级数据结构 |

## Lua 脚本

`src/main/resources/lua/` 下包含雪花 ID、Stream 清理、Hash 原子自减和 nonce 幂等计数脚本；
`src/main/resources/lua/job/` 下包含 RedisJob 的调度、领取、租约、
Fanout、回收和对账脚本。它们随主 JAR 发布。RedisJob 脚本通过独立连接直发，不进入业务 Pipeline；
同一脚本的全部键共享 hash tag，确保 Redis Cluster 下不会跨 slot。

## 与 nasa-core 的关系

分布式锁的看门狗续期依赖 `nasa-core` 的 `TimingWheel`；锁实例与 Pipeline 缓冲走 `ObjectPool`；分区消费的上下文用 `RecycleLinkedMap` 承载；`JdkSnowflake` 复用 core 中不依赖 Redis 的 ID 生成算法。因此本模块必须与 `nasa-core` 同时使用。

## 构建

```bash
mvn -B -ntp clean verify
```

## 文档与发布

- 贡献方式见 [CONTRIBUTING.md](CONTRIBUTING.md)。
- 安全问题报告方式见 [SECURITY.md](SECURITY.md)。
- Central Portal 和 GitHub 的发布流程见 [RELEASING.md](RELEASING.md)。
- 版本变化见 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

本项目采用 `Apache-2.0 OR MIT` 双许可证，使用方可任选其一。详见 [LICENSE-APACHE](LICENSE-APACHE) 和 [LICENSE-MIT](LICENSE-MIT)。
