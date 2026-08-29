# nasa-redis

> 名称声明：本项目是独立开源项目，与美国国家航空航天局不存在隶属、赞助、背书或官方项目关系；
> 详见 [NOTICE](NOTICE)。

面向 JDK 21 的 Redis 基础设施库。核心能力是基于 Redis Cluster 的无中心分布式任务调度：
通过 `@RedisJob` 声明 Cron、fixed rate、fixed delay 或手工任务，以持久 Run、租约、fencing token、
Stream 派发和 `XAUTOCLAIM` 完成故障恢复；根任务还可以按实际具备某个 Worker 能力的节点冻结快照，
向全体目标一对一 Fanout 分片，并对通知回执、重发、重分配、取消和聚合终态负责。

另一项核心能力是 [RedisPartition](REDIS-PARTITION.md)：按稳定业务键把消息路由到固定 Redis Stream 分区，
以每分区独占锁、Redis 服务端时间成员心跳、通知加周期再平衡、`XAUTOCLAIM` 和 ACK fencing，保证集群中
同一分区任一时刻只有一个有效消费 owner；节点扩缩容、进程退出或租约失效后由其它实例接管未确认消息。

组件同时提供 `RedisProxy` 命令代理及面向余额、额度等资金字段的 nonce 幂等计数，默认在 7 天窗口内保证
同一业务事件最多改变一次计数；此外还包括轻量分布式锁、RediSearch 查询 DSL、显式批量 Pipeline、
雪花 ID 与集群缓存失效通知。

```xml
<dependency>
    <groupId>io.github.nasa-runtime</groupId>
    <artifactId>nasa-redis</artifactId>
    <version>2.0.1</version>
</dependency>
```

要求 JDK 21+、Maven 3.6.3+。RedisJob 默认使用 Redis 7+ 的 Sharded Pub/Sub；选择
`BROADCAST` 降级模式时最低要求 Redis 6.2，并需要评估普通 Pub/Sub 在 Cluster 总线上的放大量。

## 核心价值与运行架构

nasa-redis 把 Redis Cluster 的原子脚本、服务端时间、Stream、Pub/Sub 和稳定键空间组合成应用内控制面，
让业务在不部署独立调度中心或分区协调服务的前提下获得可判定的执行权、可接管的待处理工作与按数据源隔离
的运行时。它负责控制权收敛和 Redis 内状态顺序，不替业务推导容量、保证外部系统事务或生成业务幂等键。

```text
Spring 应用实例
├─ @EnableRedis ─────▶ RedisProxy（命令、Pipeline、Search、nonce、锁）
├─ @EnableRedisJob ──▶ 按 qualifier 隔离的 Scheduler
│                      定义/Run/租约 ─▶ Dispatch Stream ─▶ attempt fencing
└─ RedisPartition ───▶ 稳定业务键 ─▶ Stream 分区 ─▶ 独占 owner ─▶ ACK fencing

Redis Cluster
├─ Redis TIME：租约、心跳与过期判断的统一时间依据
├─ 同 slot Lua：在一次原子提交中复验权威并发布持久状态
├─ Stream + PEL：承载至少一次交付、重试与失联接管
└─ Pub/Sub：只发送唤醒通知；丢失后由持久索引和周期扫描收敛
```

控制面遵守“先持久提交、后发送通知”和“每次副作用前复验执行权”的顺序。RedisJob 的旧 attempt 无法续期或
提交终态，RedisPartition 的旧 owner 无法确认新 epoch 下的消息；Redis 结局不明、角色变化或配置冲突时，
运行时关闭对应入口并等待权威状态收敛。对外部数据库、支付接口等 Redis 之外的副作用，调用方仍须使用稳定
业务键实现幂等，并根据至少一次交付语义处理重复调用。

组件明确不提供跨 Redis slot 事务、外部系统 exactly-once、自动容量证明、业务消息模式推断或旧键布局在线
迁移。运维侧应结合 RedisJob 的 Run/lease/Fanout 指标与 RedisPartition 的 owner epoch、PEL、接管和积压
指标设置告警；具体指标、健康条件和失败语义分别见 [REDIS-JOB.md](REDIS-JOB.md) 与
[REDIS-PARTITION.md](REDIS-PARTITION.md)。

RedisJob 是显式、按数据源启用的运行时：`@EnableRedis` 只建立 RedisProxy 等基础能力；业务还必须添加
`@EnableRedisJob` 并设置 `nasa.redis.job.enabled=true`。每个 `@RedisJob` 都必须声明 `qualifier`，编程式
调用也必须通过 `RedisJobSchedulers.scheduler(sourceId)` 选择 source。没有任务或静态调用引用的数据源
不会创建 Scheduler、扫描线程、订阅或执行器注册记录，框架也不会把未知 source 回退到 `primary`。

## 旧键布局迁移边界

当前 RedisJob 把多数据源 source id 纳入 Redis hash tag、稳定 `runId`、Fanout `executionKey` 和全部公开
构造入口。未包含 source id 的旧键布局与当前布局属于两套独立控制面：即使继续使用相同的 `namespace`，
当前运行时也不会读取或接管旧布局中的定义、Run、Stream、租约与 Fanout。框架不提供两种键布局之间的
在线迁移。

当前装配要求是：`@EnableRedis` 不启用 RedisJob，业务必须显式添加 `@EnableRedisJob`；每个
`@RedisJob` 必须填写 `qualifier`，编程式控制通过 `RedisJobSchedulers.scheduler("<source-id>")`
明确选择数据源。框架不提供默认 `RedisJobScheduler` Bean，也不存在可以改写 Scheduler 来源的根级
qualifier 配置；source id 只能由注解、静态入口或直接传入的 RedisProxy 确定。

从旧布局迁移时，先在全部旧节点暂停新触发，等待普通 Run 和 Fanout 到达终态，再停止所有旧节点；确认没有
旧进程继续扫描后，才能用当前布局启动并重新登记定义。两种布局的稳定幂等标识不同，迁移窗口内仍可能到达的
业务事件必须使用订单号、结算号等布局无关业务键在目标系统去重，不能依赖两边 `runId` 相同。需要保留旧记录
时先归档或按业务方案离线迁移，不能让两套布局同时处理同一业务副作用。

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
兼容能力快照 ──▶ 按成员数分片 ──▶ 持久投递信封 + Pub/Sub 定向通知/回执
                                      │
                                      ▼
                             分片 Worker 执行与聚合
```

关键保证如下：

- **没有单点调度主节点**：分片扫描可以重复，状态迁移依靠 Redis `TIME`、Lua CAS 与 Redis Cluster hash tag 收敛。
- **执行权可判定**：每个 attempt 都携带单调 `attemptToken`；旧 owner 的续期和完成提交会被拒绝。
- **同名任务默认集群串行**：`SERIAL_QUEUE` 在 Redis 中按任务身份占用唯一执行槽；fixed rate 的后续 Run 可以持久排队，但不会在前一 attempt 仍运行时提交第二个业务 Handler。
- **至少一次恢复**：派发消息、可见性索引、租约索引和 `XAUTOCLAIM` 共同覆盖进程退出与响应丢失，因此 Handler 必须幂等。
- **定义冲突不按启动顺序裁决**：同名任务的修订号与规范摘要持久化；相同修订号但不同定义会进入冲突并停止触发。
- **执行器心跳不会越权扩租**：每条逻辑心跳把请求 ID、状态、在途数与已确认 `heartbeatRevision` 绑定；响应丢失只重发原载荷，状态迁移会在旧请求取得结局后使用新 ID 完成发布，在途数变化由下一周期采样，过期但尚未回收的记录不能重新续期。
- **权威不明时准入关闭**：启动和 `activate()` 只在 Redis 确认 `ACTIVE` 后才开放 Fanout 与普通 Dispatcher；心跳或开放步骤的结果不确定时，两个本地入口立即关闭并以 `DRAINING` 为收敛目标，后续心跳不会自行重开。
- **已授予执行权必须收敛**：Redis 已返回 `STARTED` / `ADOPTED` 后，本地 Handler 执行器即使拒绝提交或抛出 `Error`，当前回调也会同步接管完成出口；在途、容量和去重账目只由执行路径的 `finally` 统一释放。
- **能力变更失败即撤销 source**：动态能力登记、过期后的全量能力重建或删除后的能力撤销一旦结局不明，当前 source 永久关闭准入并进入全量 Registry 注销；调用方需重建运行时，不能在可能仍为 `ACTIVE` 的半登记成员上继续服务。
- **Fanout 只选择兼容节点**：目标必须登记同一 Worker、`contractRevision`、`schemaId` 和 `codec`，没有该能力的 Java、Go 或 Rust 节点不会收到分片。
- **稳定分片幂等键**：`executionKey` 在通知重发、执行重试和 assignment 重建期间保持不变；每次重建都会递增 `assignmentEpoch`，包括换节点和原稳定节点出现新启动或心跳证据后的恢复。
- **跨语言 JSON 不携带 JVM 类型信息**：Job 使用独立 Jackson 映射器，强制关闭 Default Typing，并拒绝 `@class` / `@type` 字段。

#### 实现架构与原子边界

RedisJob 把发现、调度、执行和恢复拆成可独立重试的组件，持久状态始终先于进程内通知：

| 组件 | 业务职责 |
|---|---|
| `@EnableRedisJob` / `RedisJobInfrastructureConfiguration` | 显式建立配置绑定、运行时提示、唯一管理器和注解登记器；自身不创建具体 source 的 Scheduler |
| `RedisJobAnnotationRegistrar` | 在全部 Spring 单例就绪后先完成本地签名、泛型、身份和 source 校验，再把任务登记到对应 Scheduler |
| `RedisJobSchedulers` | 按实际引用的 qualifier 惰性建立彼此隔离的 Scheduler，并统一驱动 start、stop 和 close；单 source 的 drain/activate 由对应 Scheduler 提供 |
| `RedisJobScheduler` | 对外提供登记、触发、控制、查询和健康状态，周期驱动 schedule、visible、lease、Fanout 与回收索引 |
| `RedisJobRepository` + Lua | 在单个 Redis slot 内原子复验定义、Run、串行槽、租约、等待队列和终态 |
| `RedisJobDispatcher` | 消费普通 Dispatch Stream，在提交 Handler 前取得 `attemptToken` 与 lease，结束后提交重试或终态 |
| `RedisJobExecutorRegistry` | 维护节点身份、心跳、Worker 能力合同和不可变布局，提供 Fanout 兼容存活快照 |
| `RedisJobFanoutCoordinator` | 把普通根 Run 的完成权威转交给 Fanout，分批创建 shard，并在 commit 后建立投递信封和通知 |
| `RedisJobFanoutDispatcher` | 订阅目标 Pub/Sub 频道，读取权威 shard 后持久确认、取得分片执行权并调用 Worker |
| `RedisJobFanoutMonitor` | 扫描 receipt、ready、lease、root 与 gc 索引，负责重发、容量路由、失联策略、聚合和回收 |
| `RedisJobLeaseRenewer` | 批量续期普通 Run 与 Fanout shard，并维护本地更保守的持权截止点 |

Redis Cluster 中有三个原子域：schedule slot 保存定义、Run、Dispatch、visible、lease、串行等待和回收状态；
registry slot 保存布局、执行器心跳与能力合同；Fanout bucket slot 保存根、shard、投递信封及全部恢复索引。
单段 Lua 只跨同一 hash tag 的键，跨域流程依靠稳定标识、幂等状态码和持久 intent 衔接，不宣称跨 slot 事务。

普通任务的顺序是“扫描候选 → Lua 复验并创建唯一 Run → 写 Dispatch 与 visible → start Lua 取得
`attemptToken`/lease/串行槽 → Handler → finish Lua 提交重试或终态”。Stream 消息只是可重复投递的定位信号，
Run 才是权威状态；消息丢失、裁剪或消费者退出后由 visible、lease 和 PEL 恢复，同一时刻只有通过 owner 与
token 复验的 attempt 可以改变调度状态。

Fanout 需要跨 schedule、registry 和 bucket 三个 slot，提交顺序固定为：

1. 在 schedule slot 写入确定性 `fanoutId`、快照标识和创建期限，使根 Run 进入 `FANOUT_CREATING`；成功后普通 Handler 不再拥有根完成权威。
2. 在 bucket slot 建根记录、分批写 shard 并 commit；任一步响应丢失都能按同一 intent 幂等重放。
3. 回到 schedule slot 把普通根推进为 `WAITING_CHILDREN`，再分批写 inbox 投递信封、receipt deadline 并发布定向通知。
4. 若 bucket 已 commit 但删除 fence 阻止根转换，立即把桶切入 `CANCELLING`，阻止全局看门狗继续开放新分片。
5. 桶内全部 shard 收敛后先形成唯一聚合终态，再回填普通根；保留期到达后才有界删除根、shard 与索引。

状态发布顺序同样是安全边界：持久状态和恢复索引先提交，Pub/Sub 只在其后用于低延迟唤醒。Java Worker 不通过
`XREADGROUP` 轮询 Fanout inbox；它订阅目标频道，再以通知中的 `fanoutId/seq/assignmentEpoch` 读取并复验
持久 shard。通知丢失时，receipt 或 ready 扫描器根据权威状态重新发布；inbox 中的稳定消息 ID 用于接收确认、
assignment 切换时撤销旧投递以及终态清理，旧节点不能凭迟到通知越过新代次。

#### Spring 接入

`@EnableRedis` 只装配 RedisTemplate、RedisProxy 与分布式锁；RedisJob 必须通过 `@EnableRedisJob` 独立开启。
该入口只建立 `RedisJobSchedulers` 管理器、配置绑定、运行时提示和注解登记器，不创建任何具体 source 的
Scheduler，也不向容器暴露默认 `RedisJobScheduler` Bean。注解登记器只为 `@RedisJob.qualifier` 实际声明的
source 建立运行时；编程式任务在首次调用 `RedisJobSchedulers.scheduler(sourceId)` 时建立对应运行时。
同一 source 在进程内只创建一次，全部实际建立的 Scheduler 由管理器统一启动、停止和关闭；滚动发布时的
`drain()` / `activate()` 是单个 `RedisJobScheduler` 的控制入口，不是管理器的全数据源广播操作。
管理器同时承载静态门面的生命周期权威：一个 JVM 同一时刻只允许一套活动管理器，第二个并行应用上下文会
在初始化时被拒绝；容器关闭后静态入口立即失效，不能持有旧 Scheduler 跨上下文继续使用。
`stop()` / `close()` 提交不可逆关闭后，同一实例新发起的 `start()`、`register()`、`drain()` 和 `activate()`
都会在定义持久化、本地接线或 ACTIVE 发布前拒绝；已经进入 Redis 往返的控制调用可以取得原请求结局，但返回后
必须复验终态，不再接线 Handler、登记能力或开放本地准入。并发的重复停机会等待同一次首次停机结果；Spring 完成回调只在
Handler 退出且所有权资源完成最终收口后执行，不能让容器先销毁迟到任务仍依赖的 Redis 连接。普通与 Fanout 派发共享 pre-start
在途屏障；Redis 已授予 attempt 的迟到回包先登记为 Handler
在途再进入排空，避免 `awaitIdle` 在两份账目之间观察到零。pre-start 或 Handler 未在共享截止内排空时，有界 `stop()` 立即发布稳定失败并暂时保留
派发控制、续租器与 `DRAINING` Registry 成员，不会继续注销而留下孤儿 `RUNNING`。唯一 final-cleanup continuation 继续等待权威工作归零，随后关闭
普通与 Fanout 派发、监视任务、续租器并注销成员；continuation 无法启动时由当前停机线程接管，不能遗失唯一收口权。
最终资源边界没有第二个超时时限，也不会把线程中断解释为资源已经结束：尚未开始的扫描、超时和续租调度在执行器关闭时取消，
已经开始的回调必须实际退出后才能注销 Registry 并发布最终完成。这个边界保证资源安全而不保证强制终止；Handler 必须协作结束，
否则 callback/`close()` 可以超过 `max-run-duration-ms` 持续等待。最终失败单独记录，不改写首次停机结果。
`SmartLifecycle` callback 和作为 Spring destroy method 的 `close()` 都等待最终收口，防止 lifecycle phase 超时后下层 Redis 连接被提前销毁；
`close()` 完成等待后仍交还首次稳定失败。安全排空完成后的单个资源关闭失败仍会尝试其它关闭步骤并返回汇总结果；Stream container、Pub/Sub listener/连接和 Registry 注销都会保留未决坐标并继续重试，只有容器非运行、订阅已确认撤销且成员确认不存在后才发布 final。完整停机后
必须由新应用上下文建立新 Scheduler。Spring 实际托管的 `RedisJobSchedulers` 管理器遵守同一回调边界：任一 source
在首次期限内未排空时，管理器先发布稳定停机失败，再异步等待所有 source 的最终收口。管理器从构造时起永久追踪每个 Scheduler；运行期启动失败的动态 source 即使已退出公开路由，也必须进入同一 final 快照。只有底层 listener、执行器、续租器和
Registry 都结束后才执行完成回调；管理器 `close()` 同样在全部 source 最终收口后才允许 Spring 销毁其 RedisProxy 依赖。
回调属于当前调用方，其异常不改写各 source 已发布给其它等待者的资源停机结果。
管理器先在生命周期锁外发布单调终态并封闭当前全部
source 的本地 Dispatcher/Fanout 准入，再取得稳定 Scheduler 快照；即使顺序启动中的后一个 source 阻塞 Redis，
已经开放的 source 也会立即关门。随后按 source 并行发布
`DRAINING`、排空 pre-start 与已持权 Handler 并释放资源。因此慢 source 不会让其它 source 继续领取，在途 Handler 查询公开静态入口时
也能立即观测停机终态，而不会与管理器排空形成反向等待。该本地关门权威不取得 Scheduler monitor，也不等待 Redis；
先前进入的启动、登记或激活调用退出后，资源阶段才继续注销。管理器对首次收口的 source 快照、完成信号和失败结果保持稳定，
即使 `close()` 已清空公开 source 路由，后续 `stop()` / `close()` 仍等待并交还同一资源结果，不能把仍存活的底层资源报告为成功。

```java
@SpringBootApplication
@EnableRedis
@EnableRedisJob
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
            qualifier = "primary",
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
            qualifier = "primary",
            trigger = RedisJobTrigger.FANOUT_ONLY,
            schema = "contract-wallet-sweep-shard",
            codecs = RedisJobWireCodec.JSON,
            timeoutMs = 120_000L
    )
    public RedisJobResult sweepShard(RedisJobContext context, List<String> wallets) {
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

#### 固定周期与同任务串行

`SERIAL_QUEUE` 是注解和编程式定义的默认并发策略，约束范围是同一
`(qualifier, namespace, jobName)`，在多实例和 Redis Cluster 部署下同样成立。以一分钟 fixed rate 为例：
某次 Handler 执行超过一分钟时，下一个逻辑时刻仍会形成持久 Run，但它在原子领取阶段进入 `BLOCKED`
等待队列，不会提交第二个业务线程；当前 attempt 释放串行槽后，队首才重新变为可派发状态。
积压由 `max-serial-backlog` 限制，并按 `serial-overflow-policy` 选择跳过最旧或最新 Run。

若业务语义是“上一 Run 进入终态后再等一分钟”，使用 `fixedDelayMs`；若到点时仍在运行就应放弃本轮，
使用 `DISCARD_IF_RUNNING`；只有显式选择 `PARALLEL` 才允许同名 Run 并行。`timeoutMs` 默认 120 秒且取消是
协作式的，预计执行时间更长时应同时调整定义超时，并保证 Handler 在批量边界和外部副作用前调用
`checkpoint()`。实际取消阈值取 `timeoutMs` 与 `max-run-duration-ms` 的较小值；Handler 未返回前框架不会
强制释放串行槽。

#### Fanout 失败策略

| 策略 | 目标不可用时的行为 | 根结果 |
|---|---|---|
| `REASSIGN_ON_FAILURE` | 递增 `assignmentEpoch`，选择仍然兼容的其它节点 | 全部分片成功时 `SUCCEEDED`，否则按最终结果收敛 |
| `STRICT_SNAPSHOT` | 保留冻结快照，不换目标；持续恢复直到根等待超时或被取消 | 超时后外层 Run 为 `FAILED` |
| `BEST_EFFORT` | 无法执行的分片记为 `SKIPPED`，其余分片继续 | 桶内为 `PARTIAL_FAILED`；外层 Run 为 `FAILED`，`resultCode=PARTIAL_FAILED` |

Pub/Sub 只负责低延迟唤醒。持久 shard 与 receipt deadline、ready、lease、root 索引负责发现未推进状态并
重新发布通知；inbox 保存稳定投递信封和当前消息 ID，Java Worker 不轮询该 Stream。
目标节点接受分片时先用 Lua 持久化确认，再通过 Pub/Sub 向根节点发送回执信号。根任务的
`fanoutReceiptTimeoutMs` 默认是 `2000` 毫秒，`fanoutReceiptMaxRetries` 默认允许首次通知后重发 `3` 次；
每次超时都先复验持久确认，次数耗尽后才按 `fanoutFailurePolicy` 重分配、等待原快照或跳过。
每个分片的 `seq`、`executionKey`、assignment、attempt 与结果随 Fanout 记录保留，到
`fanout-retention-ms` 后由有界清理删除，不会永久驻留 Redis。

目标节点已经确认接收但本地执行槽暂满时，框架把它视为容量背压，而不是节点失联：不累计跨根失联证据，
不产生 `SKIPPED`，也不消耗真实故障的 `assignmentCount`。即使进程在写入显式容量标记前发生控制队列拥塞，
ready 普通唤醒耗尽后也会先读取一次兼容存活快照：当前目标仍在快照中时走同一容量出口，快照暂时不可读时
继续等待，只有目标确定离开快照才进入失败策略。非严格策略跨过 `fanout-capacity-wait-ms` 后只在存在其它
兼容节点时尝试容量路由；没有候选或容量路由处于静默窗口时继续保留当前 assignment。容量路由有独立的
有限突发与静默恢复预算，后来出现空闲节点时仍可重新探测。`STRICT_SNAPSHOT` 始终保留冻结目标。

#### 手工触发、控制与观测

```java
RedisJobScheduler scheduler = RedisJobSchedulers.scheduler("primary");
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
启动时没有任务定义的 Scheduler 保持 `DRAINING`，第一个动态定义完成 Registry 登记和 ACTIVE 确认后
才开放本地派发入口。首次动态登记与 `drain()` / `activate()` 按同一生命周期序列执行；`drain()` 成功
返回后，尚未完成的登记不能再凭旧开门资格恢复本地准入。

#### 多数据源

`@RedisJob.qualifier` 是必填项；`@RedisJob(qualifier = "match")` 把任务登记到指定 Redis 数据源，任务身份是
`(qualifier, namespace, jobName)`：每个数据源独享调度器、执行器注册表、线程池与 Fanout，互不可见；
键前缀、`runId`、`executionKey` 和消息内的来源声明都包含语言无关 source id（`primary`、`match`，
不带 Spring Bean 后缀）。数据源不存在时应用启动失败，不会静默回退到 `primary`；
`nasa.redis.job.sources.<id>.*` 只覆盖参数，不会主动创建 Scheduler。没有注解或静态调用引用的 RedisProxy
不会产生 RedisJob 线程、订阅或注册表成员。多数据源身份、来源复验与不可变布局门禁见
[RedisJob 架构与运行指南](REDIS-JOB.md) 的「多数据源」章节。

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
环形桶只在 Redis 时间向前进入新代次时换代；时钟回拨命中较新代次时保留其中仍处于保证窗口的凭证。

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

`getLock` 返回永久绑定当前 Redis 数据源与业务 key 的稳定 `Lock`。完整 `unlock()` 后可以继续用同一引用
再次 `lock()`，也可以由多个线程按标准 `Lock` 语义竞争；重入仍要求同一线程成对释放。非 owner 调用
`unlock()` 会抛出 `IllegalMonitorStateException`，不会停止真实 owner 的看门狗或破坏其本地状态。
锁不实现 `AutoCloseable`，不能用 try-with-resources。

- 加锁、解锁、续期各一个 Lua 脚本，在 Redis 端原子执行；
- **可重入**：用 Redis Hash 记录（key = 锁名，field = 持有者标识，value = 重入次数）；
- **看门狗续期**：持锁期间由 `TimingWheel` 定时延长 TTL，避免业务没做完锁先过期；
- **持有者标识 = JVM 实例 ID + 线程 ID**，跨节点与跨线程都唯一；
- 未取得锁的线程共享释放频道订阅，收到解锁通知后立即重试，并以剩余 TTL 作为通知丢失时的等待上限；
- 看门狗任务名包含 Redis 数据源、锁入口会话、完整 key、公开句柄和 holder，多数据源同名锁不会覆盖续期任务。

### Stream 分区消费 —— 独占分区与故障接管

RedisPartition 把稳定业务键映射到 N 条 Redis Stream，每条 Stream 由一个分布式锁独占。当前 owner 先用
`XAUTOCLAIM` 接管已达到空闲阈值的 PEL，再用 `XREADGROUP` 读取新消息；处理成功且 ACK 前复验锁权威。
同键在分区数和命名空间不变时落入同一分区，不同分区可以并行。该能力提供至少一次交付与分区内串行入口，
不替外部数据库、HTTP 或其它系统提供 exactly-once，业务回调必须幂等。

```yaml
nasa:
  redis:
    properties:
      primary:
        stream:
          partition:
            enabled: true
            groups:
              settlement:
                count: 64
                topics: [contract:settlement, spot:settlement]
                batch-size: 200
```

除 `enabled: true` 和实际使用的隔离组 `count` 外，其它字段都可以省略并采用内置默认值。默认共享组无需声明
`groups`；完整配置、默认值、参数约束和滚动变更边界见 [REDIS-PARTITION.md](REDIS-PARTITION.md)。

消费侧实现 `RedisEventBatchListener`（或 Single 形态）并把 `mode()` 标成 `ConsumeMode.PARTITION`，注册为 Bean 即可——分区 stream 与消费组由 `RedisProxy` 在初始化阶段按 listener 声明的 topic 自动建好，业务侧不需要显式调 `init` / `isolate`：

```java
@Component
public class SettlementListener implements RedisEventBatchListener<Order> {
    @Override public String[] topics() { return new String[]{"contract:settlement"}; }
    @Override public String event()    { return "open-position"; }
    @Override public ConsumeMode mode(){ return ConsumeMode.PARTITION; }
    @Override public void onEvent(List<Order> orders) { ... }   // 同分区串行
}

// 发布：分区数与命名空间不变时，同一个 uid 始终落入同一分区
redisProxy.partition("contract:settlement", "open-position", uid, order);
```

节点身份由应用名、节点序号和每次运行生成的 UUID 组成，同机多进程与同一 JVM 的多个应用上下文不会合并成
一个成员。每个分区组独立使用 Redis `TIME` 维护成员 ZSET，并按 `ceil(分区数 / 存活节点数)` 限制单节点持有量；
上线、释放和优雅下线通知用于立即唤醒再平衡，周期任务负责在 Pub/Sub 通知不可达时继续收敛。进程突然退出时，
成员通常在三个再平衡周期后被剔除，但真正接管仍必须等待原分区锁租期失效。

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

分布式锁的看门狗续期依赖 `nasa-core` 的 `TimingWheel`；锁实例与 Pipeline 缓冲走 `ObjectPool`；分区消费的上下文用 `RecycleLinkedMap` 承载；`JdkSnowflake` 复用 core 中不依赖 Redis 的 ID 生成算法。当前发布坐标依赖 `io.github.nasa-runtime:nasa-core:1.0.3`，使用方应让 Maven 解析到该版本或兼容的更新版本。

## 构建

```bash
mvn -B -ntp clean verify
```

## 相关文档

- RedisJob 的完整状态机、配置与运维边界见 [REDIS-JOB.md](REDIS-JOB.md)。
- RedisPartition 的路由、集群收敛、配置、交付语义与运维边界见 [REDIS-PARTITION.md](REDIS-PARTITION.md)。
- 贡献方式见 [CONTRIBUTING.md](CONTRIBUTING.md)。
- 安全问题报告方式见 [SECURITY.md](SECURITY.md)。
- 公开归档与 registry 交付要求见 [RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md)。
- 项目名称及独立性声明见 [NOTICE](NOTICE)。

## 许可证

本项目采用 `Apache-2.0 OR MIT` 双许可证，使用方可任选其一。详见 [LICENSE-APACHE](LICENSE-APACHE) 和 [LICENSE-MIT](LICENSE-MIT)。
