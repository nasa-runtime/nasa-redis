# RedisJob 架构与运行指南

RedisJob 是 `nasa-redis` 内置的 Redis 原生分布式任务调度器。它以 Redis 中的任务定义、Run、Stream、
租约与恢复索引作为控制面，不建立应用级主节点，也不要求额外数据库或独立调度服务。

适用场景包括周期任务、手工幂等触发、串行任务、失败重试、长任务租约恢复，以及由一个根任务收集数据后，
按当前兼容执行器数量切分并定向发送到集群节点的 Fanout 任务。

## 能力边界

RedisJob 提供：

- Spring 六段式 Cron、fixed rate、fixed delay 和手工触发；
- `SERIAL_QUEUE`、`DISCARD_IF_RUNNING`、`PARALLEL` 三种并发语义；
- `DO_NOTHING`、`FIRE_ONCE_NOW`、`CATCH_UP` 三种误触发策略；
- requestId 幂等 Run、有限重试、Handler 超时和协作式取消；
- 租约续期、`attemptToken` fencing、PEL 接管和可见性重派发；
- 按 Worker 能力、契约修订号、Schema 和编码筛选节点的 Fanout；
- Fanout 通知回执、重发、目标重分配、根取消、聚合终态和有界回收；
- Redis standalone 与 Redis Cluster；
- JSON、Protobuf 和 RAW 参数合同。

RedisJob 不提供：

- 外部数据库、HTTP、钱包或消息系统的 exactly-once 副作用；
- 跨 Redis slot 的事务；
- 网络分区下对外部资源的自动 fencing；
- 无限任务历史或永久幂等记录；
- 可视化管理控制台；
- Go 或 Rust SDK。本制品只包含 Java 运行时，其线协议和能力合同允许其它运行时实现兼容客户端。

## 旧键布局迁移边界

当前布局把语言无关 source id 纳入全部 RedisJob 键路由和稳定标识。未包含 qualifier 的旧 schedule、
registry 与 Fanout hash tag 和当前统一使用的 `<qualifier>:<namespace>` 属于不同控制面，不存在自动发现、
接管或在线迁移。继续填写相同 namespace 也不会读取旧定义和存量 Run。

当前 source-aware 公开合同包括：

- `RedisJobIdentifiers` 的 scheduled/manual Run 标识计算增加 qualifier，Fanout `executionKey` 随之变化；
- `RedisJobKeyspace` 构造参数增加 qualifier，生成的全部键和频道进入新 hash tag；
- RedisJob 与 `@EnableRedis` 分离，业务必须显式使用 `@EnableRedisJob`；框架不提供默认
  `RedisJobScheduler` Bean，编程式入口改为 `RedisJobSchedulers.scheduler(sourceId)`；
- `@RedisJob.qualifier` 没有默认 source，每个注解任务都必须显式声明；
- 不提供根级 `nasa.redis.job.qualifier`；直接构造 Scheduler 时，source id 从传入 RedisProxy 冻结，不能由
  另一份配置字段伪装成其它来源；
- `RedisJobContext` 提供 `qualifier()` 和泛型 `TypeReference` 解码入口；自行实现该接口的代码必须提供
  相同合同。

安全迁移顺序是：旧控制面 `pause` 停止新触发 → 等待普通 Run/Fanout 全部终态 → 停止全部旧节点 →
确认旧节点不再扫描 → 以当前键空间启动并重新登记任务。若旧批次无法排空，应先取消并确认其外部副作用；
两种布局的稳定标识不同，目标系统必须用订单号、结算号等布局无关业务键去重。需要保存旧控制面记录时先
离线归档，不要在两套布局并行运行时复制活动 Run、租约或 Stream 消息。

## 运行架构

### 组件职责与原子域

| 组件 | 业务职责与持久边界 |
|---|---|
| `@EnableRedisJob` 与内部基础设施配置 | 在业务显式开启后建立配置绑定、运行时提示、唯一管理器和注解登记器，本身不创建具体 source 的 Scheduler |
| `RedisJobAnnotationRegistrar` | 在 Spring 单例就绪后发现注解方法，校验签名与参数合同，再按 qualifier 登记到对应 Scheduler |
| `RedisJobSchedulers` | 为每个 Redis 数据源建立独立运行时，统一驱动 start、stop 和 close；单 source 的 drain/activate 由对应 Scheduler 提供 |
| `RedisJobScheduler` | 提供登记、触发、暂停、恢复、取消、删除、查询和健康门面，并调度所有有界扫描 |
| `RedisJobRepository` 与 job Lua | 在 schedule slot 内维护定义、Run、Dispatch、visible、lease、串行槽、等待队列和回收索引 |
| `RedisJobDispatcher` | 消费普通 Dispatch Stream，执行 start 门禁、Handler 调用、finish/重试和 PEL 接管 |
| `RedisJobExecutorRegistry` | 在 registry slot 内维护不可变布局、节点心跳、能力合同与跨根失联证据 |
| `RedisJobFanoutCoordinator` | 把普通根 Run 的完成权威转给 Fanout，提交根与 shard，并在 commit 后建立投递 |
| `RedisJobFanoutDispatcher` | 订阅目标通知频道，从权威 shard 读取合同和参数，持久确认后领取分片执行权 |
| `RedisJobFanoutMonitor` | 扫描 receipt、ready、lease、root、capability 和 gc 状态，执行重发、路由、聚合与回收 |
| `RedisJobLeaseRenewer` | 批量续期普通与 Fanout 执行权；本地截止点额外扣除 Redis 往返和时钟偏差预算 |

RedisJob 没有一个覆盖全局的事务域。schedule slot、registry slot 和 Fanout bucket slot 使用不同 hash tag：
每段 Lua 只修改一个 slot 内的键，跨域流程使用确定性标识、持久 intent、幂等状态码和看门狗恢复连接。
进程内队列与 Pub/Sub 只用于缩短延迟，不能取代 Redis 中的 Run、shard 和索引。

### 普通任务路径

```text
定义登记
   │
   ▼
schedule ZSET ──扫描──▶ fire Lua
                           │
                           ├─复验命名空间、定义状态、修订号与逻辑时刻
                           ├─创建唯一 Run
                           └─写 Dispatch Stream 与 visible 索引
                                      │
                                      ▼
                              Worker XREADGROUP
                                      │
                                      ▼
                     start Lua 分配 attemptToken 与 lease
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                    Handler 完成             lease / PEL 恢复
                         │                         │
                         └──────▶ 重试或终态 ◀────┘
```

扫描器只处理本进程已经登记任务涉及的分片。多个进程可以扫描同一分片，Lua 会对调度时刻和 Run 标识执行
CAS；扫描重复、响应丢失或进程重启不会产生第二个有效 Run。

派发 Stream 提供持久消息并以 `MAXLEN ~ 100000` 限制单个 Worker Stream 的近似长度，`visible` ZSET
提供消息丢失或裁剪后的重建入口，`leases` ZSET 记录当前执行权截止点，`XAUTOCLAIM` 接管死消费者的 PEL。
任何一层都可能重复观察同一个 Run，因此最终执行语义是至少一次。协议或合同门禁拒绝当前消息时会同时
`XACK` 与 `XDEL`，等待兼容节点的责任只由持久 Run 与 `visible` 承担，已确认消息不作为恢复凭证。

### Fanout 路径

根 Handler 调用 `context.fanout(workerName)` 后，框架先在 registry slot 冻结兼容执行器快照，再把确定性
Fanout intent 写入普通根所在的 schedule slot，随后才建立固定 Fanout 桶：

```text
根 Run RUNNING
   │
   ├─冻结兼容能力快照
   ├─按快照成员数生成同样数量的分片
   ├─根 Run 进入 FANOUT_CREATING
   ├─Fanout 桶分批写 shard 并 commit
   ├─根 Run 进入 WAITING_CHILDREN
   └─commit 后建立持久投递信封、receipt deadline 与通知
             │
             ▼
每目标节点持久投递信封 + Pub/Sub 定向通知
             │
             ├─持久接收确认
             ├─start 分配 shard attemptToken
             └─完成、重试、重分配或跳过
                           │
                           ▼
                    聚合桶内根终态
                           │
                           ▼
                    回填普通根 Run
```

注册表快照、普通根和 Fanout 桶位于三个原子域，无法由一段 Lua 同时提交。其顺序是：

1. `prepare_fanout_root.lua` 在 schedule slot 记录 `fanoutId`、快照标识、分片数与创建期限，根 Run 进入
   `FANOUT_CREATING`；成功后普通 Handler 的完成权威已经转移，不能再走普通 finish 出口。
2. `fanout_begin.lua` 建桶内根，`fanout_add_shards.lua` 按有界批次写 shard，`fanout_commit.lua` 最后开放
   已完整提交的桶；脚本均接受同一稳定标识的幂等重放。
3. `finish_fanout_root.lua` 把普通根推进到 `WAITING_CHILDREN`，随后 `fanout_deliver_batch.lua` 才按批次写
   inbox 信封、receipt deadline，并在持久状态提交之后发布低延迟通知。
4. 桶已 commit 但普通根转换命中删除 fence 时，框架先把桶切入 `CANCELLING`，阻止全局看门狗补投新 shard；
   普通根保留等待，直到桶真实终态后再回填。
5. 根看门狗可以根据 schedule intent 与桶记录补齐中断步骤；桶聚合终态形成后，普通根对账使用幂等完成脚本，
   任一响应丢失都不会形成第二份有效完成权威。

Pub/Sub 不是正确性的唯一来源。Java Worker 不通过 `XREADGROUP` 轮询 Fanout inbox，而是订阅目标频道，收到
`fanoutId/seq/assignmentEpoch` 后重新读取持久 shard 并执行 accept/start 门禁。通知丢失时，receipt deadline
和 ready 索引会按 shard 权威状态重新发布；lease 和 root 看门狗分别恢复失权 attempt 与未收敛批次。
inbox 保存稳定投递信封及当前消息 ID，用于确认、assignment 切换时原子撤销旧消息和终态清理，不单独承担
Java 侧拉取消费职责。

目标节点持久确认接收后若本地执行槽暂满，会在 `fanout-capacity-wait-ms` 窗口内保留当前 assignment、
延后 ready 并清除普通唤醒计数；短时容量背压不会被当作节点失联，也不会产生 `SKIPPED` 或消耗节点
故障配额。连续等待超窗后，非严格策略只在存在其它兼容节点时尝试容量路由；没有候选、能力快照暂时
不可读或容量频率预算处于静默期时继续保留当前 assignment。容量路由改变目标时只增加
`assignmentEpoch` 和独立的容量计数，不增加 `assignmentCount`；当前目标离开兼容存活快照后才进入
真实失联出口。`STRICT_SNAPSHOT` 始终保持固定目标与既有指数退避。

## Redis Cluster 键路由

全部键的 hash tag 都以 `<qualifier>:<namespace>` 开头。qualifier 进入键前缀是隔离前提：两个数据源即使
指向同一台 Redis、又用了相同 namespace，控制面键、Stream、执行器注册表与 Fanout inbox 也完全不相交，
一个数据源的消费者不会读到另一个数据源的派发消息。

qualifier 内不允许出现 `:`，否则 `(qualifier="a:b", namespace="c")` 与 `(qualifier="a", namespace="b:c")`
会拼出同一段前缀，两套 Scheduler 的定义、Run、注册表与 Fanout 全部重合。namespace 仍可使用 `:` 分层。

调度分片使用固定 hash tag：

```text
rjob:{<qualifier>:<namespace>:<schedule-shard>}:schedule
rjob:{<qualifier>:<namespace>:<schedule-shard>}:job:<jobName>
rjob:{<qualifier>:<namespace>:<schedule-shard>}:run:<runId>
rjob:{<qualifier>:<namespace>:<schedule-shard>}:dispatch:<workerKey>
rjob:{<qualifier>:<namespace>:<schedule-shard>}:visible
rjob:{<qualifier>:<namespace>:<schedule-shard>}:leases
rjob:{<qualifier>:<namespace>:<schedule-shard>}:reaping
```

执行器注册表使用独立 registry slot：

```text
rjob:{<qualifier>:<namespace>:registry}:layout
rjob:{<qualifier>:<namespace>:registry}:executors
rjob:{<qualifier>:<namespace>:registry}:executor:<executorId>
rjob:{<qualifier>:<namespace>:registry}:capability:<workerName>
```

Fanout 根据 `fanoutId` 稳定映射到固定桶：

```text
rjob:{<qualifier>:<namespace>:fanout:<bucket>}:root:<fanoutId>
rjob:{<qualifier>:<namespace>:fanout:<bucket>}:shard:<fanoutId>:<seq>
rjob:{<qualifier>:<namespace>:fanout:<bucket>}:inbox:<nodeIdentity>
rjob:{<qualifier>:<namespace>:fanout:<bucket>}:receipts
rjob:{<qualifier>:<namespace>:fanout:<bucket>}:ready
rjob:{<qualifier>:<namespace>:fanout:<bucket>}:lease
rjob:{<qualifier>:<namespace>:fanout:<bucket>}:roots
rjob:{<qualifier>:<namespace>:fanout:<bucket>}:gc
```

`RedisJobKeyspace` 是 namespace 与 slot 路由的唯一生成入口。调用前能确定的键作为 Lua `KEYS` 传入；
只有从同 slot 索引中原子取出成员后才能确定的键，才使用 Java 生成的规范前缀与索引成员组合。

`namespace`、`shard-count` 和 `fanout-bucket-count` 一旦写入数据就不能原地改变。调整这些值需要使用新的
命名空间，否则相同任务或 Fanout 会被路由到另一组 slot，旧记录也无法继续恢复。

## 定义、触发与并发语义

### 调度类型

| 声明 | 行为 |
|---|---|
| `cron` | Spring 六段式 Cron，按 `zone` 计算逻辑时刻 |
| `fixedRateMs` | 按逻辑时刻固定频率推进，不因单次运行耗时漂移 |
| `fixedDelayMs` | 当前 Run 进入终态后再计算下一时刻 |
| 三者都不配置 | 仅允许 `scheduler.trigger(...)` / `triggerJson(...)` 手工触发 |
| `trigger=FANOUT_ONLY` | 只作为定向 Worker，不产生独立调度时刻 |

Cron、fixed rate 与 fixed delay 互斥。手工触发必须提供稳定 `requestId`；相同任务和 requestId 返回同一个
Run 标识，并复验参数合同。

### 并发策略

| 策略 | 同名 Run 已持有执行权时 |
|---|---|
| `SERIAL_QUEUE`（默认） | 新 Run 进入有界等待队列，前序释放后按逻辑时刻继续 |
| `DISCARD_IF_RUNNING` | 新 Run 进入 `SKIPPED` |
| `PARALLEL` | 各 Run 独立竞争本地与集群执行容量 |

串行槽以 `(qualifier, namespace, jobName)` 为边界保存在 Redis，由 start Lua 在提交业务 Handler 前原子占用，
因此它同时约束一台机器内的线程和多节点集群中的执行器。fixed rate 或 Cron 在前一 attempt 仍运行时可以
继续创建持久 Run，但这些 Run 只会进入 `BLOCKED`，不会启动第二个业务 Handler；当前 attempt 释放槽后，
等待最久的 Run 才恢复派发。`max-serial-backlog` 限制串行积压，达到上限时由
`serial-overflow-policy` 决定跳过最旧还是最新 Run。

fixed delay 不建立上述周期积压：下一调度时刻只在当前 Run 进入终态后按 `now + fixedDelayMs` 写回。
因此“保持固定分钟网格但不并行”使用 fixed rate + `SERIAL_QUEUE`；“完成后再等待一个间隔”使用 fixed delay；
“运行中则放弃本轮”使用 `DISCARD_IF_RUNNING`。

### 误触发策略

| 策略 | 调度器发现逻辑时刻已经错过时 |
|---|---|
| `DO_NOTHING` | 跳过旧时刻并推进到未来 |
| `FIRE_ONCE_NOW` | 生成一个补偿 Run，再推进到未来 |
| `CATCH_UP` | 在 `max-catch-up-window-ms` 内保留最多 `max-catch-up-runs` 个最近时刻 |

闲置恢复不受时长限制：应用停机任意时间后，调度器按误触发策略一步推进到严格晚于当前时刻的
逻辑时刻（FIXED_RATE 闭式计算保持网格锚点不漂移，CRON 直接从当前时刻求下一刻度，FIXED_DELAY
以当前时刻重建延迟），`DO_NOTHING` 不补跑、`FIRE_ONCE_NOW` 恰好补一次、`CATCH_UP` 仍受
`max-catch-up-runs` 与 `max-catch-up-window-ms` 约束。单个任务的时刻计算异常（如极端参数溢出）
以 `redis_job_fire{ADVANCE_FAILED}` 计数并跳过该任务，不会中断同批其它任务的扫描。
`CATCH_UP` 直接从补偿窗口下界定位首个刻度（不从陈旧时刻逐刻度追赶），窗口内刻度枚举另有
100000 次迭代预算，超出预算的病态组合按 `DO_NOTHING` 推进并同样计入 `ADVANCE_FAILED`。

定义以单调 `definitionRevision` 和规范摘要持久化。相同修订号但内容不同不会按节点启动顺序覆盖，必须由
管理动作显式选择。需要滚动更新定义修订号时使用 `RedisJobDefinition.builder()` 的
`definitionRevision(...)` 编程式登记；当前 `@RedisJob` 注解入口不暴露该字段。

## 执行权、重试与取消

`attempt` 表示当前 Run 已实际启动的次数，`attemptToken` 是同任务范围单调递增的 fencing token。
续期、完成和租约恢复都会同时复验 owner 与 token；旧进程迟到提交不能覆盖新 attempt。

Handler 应在长循环、批量边界和外部副作用之前调用：

```java
context.checkpoint();
```

门禁失败会抛出 `RedisJobExecutionStoppedException`。取消与超时是协作式的，框架不会强制终止业务线程；
忽略 checkpoint 的 Handler 可能继续运行，但失权后的调度状态提交会被 fencing 拒绝。

定义级 `timeoutMs` 默认 120 秒，全局 `max-run-duration-ms` 默认 1 小时，协作式取消阈值取两者较小值。
到点只设置取消信号；Handler 返回后才提交超时结果并释放串行槽。预计正常执行时间超过定义超时时，必须
显式调大 `timeoutMs`，同时保留周期性 `checkpoint()`，不能依赖线程强制中断结束外部调用。忽略取消的
Handler 仍会占用本地容量和 Redis 执行槽，直到自身返回或执行权因其它控制面条件失效。

普通 Run 使用 `runId`，Fanout shard 使用 `executionKey` 作为业务幂等键。节点可能在外部写入成功后、Redis
终态提交前退出，所以调用方必须在目标系统建立唯一键、条件更新或等价去重。目标系统支持 fencing 时，
同时持久化并比较 `attemptToken`。

## Fanout 能力发现与失败策略

执行器按以下合同登记 Worker：

- `workerName`
- `contractRevision`
- `schemaId`
- 支持的 `RedisJobWireCodec` 集合
- `nodeIdentity`、`startupId`、runtime 与心跳修订号

快照只选择仍存活、`ACTIVE`、`fanoutReady=true` 且合同完全兼容的执行器，并按稳定节点身份排序。同一个
稳定节点出现多个启动实例时只保留当前有效成员。

`RedisJobPartitioners.balanced()` 按快照成员数生成同样数量的连续有序分片；输入少于成员数时保留空分片，
保证分片下标、目标成员和 `seq` 一一对应。自定义 `RedisJobPartitioner` 也必须返回与成员数完全相同的列表。

### 接收确认与重发

根任务的 `@RedisJob` 可以配置以下定义级参数：

| 注解参数 | 默认值 | 语义 |
|---|---:|---|
| `fanoutReceiptTimeoutMs` | `2000` | 单次通知等待目标持久接收确认的时间 |
| `fanoutReceiptMaxRetries` | `3` | 首次通知之后允许再次发送的次数 |
| `fanoutFailurePolicy` | `REASSIGN_ON_FAILURE` | 重发耗尽或执行失败后的收敛方式 |

目标节点收到通知后，先由 `fanout_accept_shard.lua` 复验 assignment 并持久化接收确认，再通过 Pub/Sub
向根执行器发出回执信号。回执信号只缩短等待时间，Redis 中的 shard 状态与 receipt deadline 才是权威证据；
信号丢失不会抹去已经持久化的确认。每次期限到达时监控器先读取权威状态，仍未确认才递增通知次数并重发；
重发次数耗尽后按失败策略处理目标，不会仅凭一次 Pub/Sub 丢包把已确认节点判为下线。

| 失败策略 | 接收或执行失败后的行为 |
|---|---|
| `REASSIGN_ON_FAILURE` | 在 assignment 上限内选择其它兼容节点，`assignmentEpoch` 递增，`seq` 与 `executionKey` 不变 |
| `STRICT_SNAPSHOT` | 不替换冻结成员；通知和恢复只面向原目标，直到完成、取消或根等待超时 |
| `BEST_EFFORT` | 无法执行的 shard 记为 `SKIPPED`，其它 shard 继续执行 |

`BEST_EFFORT` 出现跳过时，桶内根状态为 `PARTIAL_FAILED`。公共 `RedisJobState` 没有该枚举，因此普通根 Run
映射为 `FAILED`，并用 `resultCode=PARTIAL_FAILED` 保留部分完成语义。业务判断批次结果时应同时读取
`state` 和 `resultCode`。

根 Run 在 `WAITING_CHILDREN` 期间收到取消后，桶内根先进入 cancelling，停止开放新 shard 执行权；运行中
shard 通过续期获得取消信号。桶内收敛后再回填普通根，避免根先终态而子任务继续取得新执行权。

### 容量背压与节点失联

Fanout Worker 已持久确认 shard 后，如果本地 `handlerCapacity` 已满，会写入容量等待证据并延后 ready。
这证明目标仍在线且能够理解当前合同，所以容量压力不会累计 `node-unready-evidence-count`、不会把节点改为
`FANOUT_UNREADY`，`BEST_EFFORT` 也不会仅因排队而把 shard 记为 `SKIPPED`。

显式容量证据不是唯一安全出口：若通知执行器在写入 `capacityDeferredAt` 之前持续拥塞，ready 的普通唤醒也会
达到 `ready-max-wakeups`。此时监控器先读取兼容存活快照，再决定后续动作：

- 当前目标仍在快照中，按容量压力处理，使用独立容量预算；
- 快照暂时不可读，证据不足以判断失联，只为当前 assignment 续开等待窗口；
- 当前目标确定不在快照中，才进入失败策略并消耗真实故障 assignment 配额。

在 `fanout-capacity-wait-ms` 窗口内，所有策略都保留当前 assignment。窗口到期后的行为如下：

- `STRICT_SNAPSHOT` 保留冻结目标，并对后续唤醒采用有界指数退避；
- 非严格策略发现当前目标已经离开兼容存活快照时，才按真实失联执行根失败策略；
- 当前目标仍存活且存在其它兼容节点时，可以执行容量路由。它增加 `assignmentEpoch`，但不增加用于真实
  故障的 `assignmentCount`；
- 没有其它候选、能力快照暂时读取失败或容量路由处于静默期时，继续为当前 assignment 开启下一段等待窗口。

容量路由采用独立频率预算：每个窗口最多成功 `fanout-max-assignments - 1` 次，额度耗尽后至少静默
`fanout-capacity-wait-ms × fanout-max-assignments`，再原子开启下一轮有限探测。这个恢复条件使后来空闲的
节点仍可被利用，同时避免所有节点持续满载时反复撤销和重建 inbox。目标在容量等待期间真正离开存活快照
时会清除容量窗口并回到故障计数；对于故障计数已经达到上限的存量 shard，只保留一次带原子容量证据的
逃生机会，后续进入 `AWAITING_CAPABILITY`，不会无限轮转。

排查容量背压时可读取 shard HASH 的 `capacityDeferredAt`、`capacityRouteCount`、
`capacityRouteBlockedAt` 与 `capacityRouteTotal`：前三项分别表示当前 assignment 的等待起点、当前突发
窗口用量和静默起点，最后一项是 shard 生命周期内的容量迁移累计值，只用于观测，不参与裁决。

## 跨语言参数合同

Java Runtime 支持三种线编码：

| 编码 | Java 行为 | 其它运行时要求 |
|---|---|---|
| `JSON` | 独立 `ObjectMapper`，关闭 Default Typing，按 Worker 参数的确定类型解码 | 使用普通 JSON，不发送 JVM 类型元数据 |
| `PROTOBUF` | 框架不解释消息类型，通过 `rawParameter()` 返回字节 | 按双方约定的 Schema 解码 |
| `RAW` | 原样透传字节 | 由业务合同解释 |

每条参数都携带 `contractRevision`、`schemaId` 与 codec。合同不一致的执行器不会进入能力快照，普通 Run 在
start 时也会复验定义修订号和参数合同。

`nasa.redis.job.wire.json.default-typing` 是启动安全门禁，只能为 `false`。Job JSON 不继承
`RedisProxy` 通用 value serializer 的 Default Typing 配置，并递归拒绝 `@class` 和 `@type` 字段。

### 复杂参数与泛型

`RedisJobContext` 提供两个解码入口，两者进入同一个安全 `ObjectMapper`、同一套长度限制和同一套类型
元数据门禁；`TypeReference` 路径不会重新启用 Default Typing：

```java
// 非泛型参数
WalletSweepShard shard = context.parameter(WalletSweepShard.class);

// 集合、Map、泛型 DTO 与嵌套复杂对象
List<WalletSweepItem> items = context.parameter(new TypeReference<List<WalletSweepItem>>() {});
Map<String, List<WalletSweepItem>> groups =
        context.parameter(new TypeReference<Map<String, List<WalletSweepItem>>>() {});
```

注解方法直接声明泛型参数时由框架按方法签名的完整类型注入，元素类型与声明一致，不会退化成
`LinkedHashMap`：

```java
@RedisJob(name = "contract-wallet-sweep-worker", qualifier = "primary",
        trigger = RedisJobTrigger.FANOUT_ONLY)
public RedisJobResult sweepShard(RedisJobContext context, List<WalletSweepItem> items) { ... }
```

类型必须闭合才能形成确定的跨语言契约。以下参数在**登记期**直接拒绝，应用启动失败：

| 写法 | 拒绝原因 |
|---|---|
| `List<?>`、`Map<String, ?>` | 通配符无法确定元素类型 |
| `List<T>`、`T` | 未闭合类型变量 |
| 原始 `List`、`Map` | 未声明元素类型 |

确需动态 JSON 树时显式声明 `JsonNode`；Protobuf 或业务自定义编码使用 `rawParameter()`。

当前制品没有发布 Go 或 Rust SDK。其它运行时要加入同一能力集群，必须实现相同的身份、心跳、能力合同、
inbox、receipt、attempt fencing 和终态协议，不能只做到 JSON 解码就宣称兼容。

## 多数据源

业务先显式开启 RedisJob；该入口只建立共享管理器，不遍历 RedisProxy，也不预建任何 source 的 Scheduler：

```java
@SpringBootApplication
@EnableRedis
@EnableRedisJob
public class Application {
}
```

任务的本地唯一身份是 `(qualifier, namespace, jobName)`。`@RedisJob.qualifier` 是必填项，决定任务登记到哪个
Redis 数据源：

```java
@RedisJob(name = "settle-sweep", qualifier = "primary", cron = "0/30 * * * * *")
public RedisJobResult onPrimary(RedisJobContext context) { ... }

@RedisJob(name = "settle-sweep", qualifier = "match", cron = "0/30 * * * * *")  // 登记到 match
public RedisJobResult onMatch(RedisJobContext context) { ... }
```

每个数据源独立拥有一套 `RedisJobScheduler`、扫描器、Dispatcher、`RedisJobLeaseRenewer`、Fanout Monitor、执行器
注册表、控制线程池与健康状态。两个数据源可以声明同名任务，暂停、恢复、触发、Run 查询、定义冲突检测
和 Fanout 能力发现互不影响；停止其中一个不改变另一个的运行状态与健康状态。

根任务只能发现与自己**同 qualifier、同 namespace** 的 Worker。即使另一个数据源存在同名 Worker，也不会
进入能力快照、收到通知或取得执行权——Fanout 不跨数据源建立半条链路。

`RedisJobContext.qualifier()` 返回当前 Run 实际绑定的语言无关 source id，供日志、指标和业务幂等键区分
数据源。`primary` 与 `redisProxy`、`match` 与 `matchRedisProxy` 指向同一数据源，但对外一律使用不带
Spring Bean 后缀的名字，其它语言才能按同一 id 对账。

### 稳定标识

source id 参与全部对外稳定标识的计算：

```text
runId        = digest(qualifier, namespace, jobName, 逻辑时刻 | MANUAL + requestId)
fanoutId     = digest(rootRunId, rootAttempt)          # rootRunId 已含 qualifier
shardRunId   = digest(fanoutId, seq)
executionKey = qualifier + ":" + namespace + ":" + fanoutId + ":" + seq
```

调用方会把 `runId` 与 `executionKey` 写进数据库唯一键或外部接口的 idempotency key。若标识不含 qualifier，
两个独立 Redis 上同名、同一逻辑时刻的任务会算出同一个值，另一个数据源的合法任务会被误判为重复并被丢弃。
跨语言实现必须按同一顺序参与摘要，否则各语言算出的幂等键不一致。`snapshotDigest` 同样把 qualifier 与
namespace 作为前两个字段参与计算。

### 来源声明与复验

任务定义持久化 `schedulerQualifier`，Run、Fanout shard、Dispatch 消息信封与 Fanout inbox 信封都携带
该字段；执行器在取得执行权**之前**先复验，不一致返回 `SOURCE_MISMATCH` 并拒绝执行：

| 环节 | 复验内容 |
|---|---|
| Dispatch 信封 | 消费者读信封 `schedulerQualifier`，与本 Scheduler 不一致时告警计数（`redis_job_dispatch_envelope`），无需先读 Run 记录即可发现串源；权威隔离仍由 `start_run.lua` 完成 |
| Fanout inbox 信封 | 随消息持久化来源声明，供接管、审计和跨语言实现读取；Java 通知路径不消费 inbox 消息，一律以持久 shard 的 accept/start 复验为准 |
| 普通 Run start | 定义与 Run 两端的 `schedulerQualifier` 都必须等于本 Scheduler；只比对一端时，错误生产者填对 Run 字段即可绕过 |
| Fanout 根记录 | `fanout_begin.lua` 写入来源声明；监视器发现不一致时计数告警（`redis_job_fanout_root`），仍驱动到根等待超时收敛并清理，分片在 accept/start 门禁上无法执行 |
| Fanout accept | 持久确认接收前复验 shard 的 `schedulerQualifier`、`protocolVersion` 与 `contractRevision/schemaId/wireCodec`；不可执行的 shard 不回执，由接收重试按失败策略换兼容节点 |
| Fanout start | 与 accept 相同的来源、协议与合同复验，另复验 shard 自身记录的 `fanoutId/seq/workerName/executionKey` 与本地推导一致 |
| Fanout finish | 除 `owner + attemptToken + assignmentEpoch` 外复验 `executionKey` 与 `workerName`，完成结果不能计入另一个分片的聚合计数 |

accept 与 start 的门禁拒绝会在 shard 上写 `rejectedCode`、`rejectedBy`、`rejectedExpectation` 与
`rejectedAt`，使根侧排查能区分"目标失联"与"目标明确拒绝执行"；门禁在兼容节点上通过时这组字段
被清除。证据组与 `errorType` 是两套独立语义：`errorType` 由重分配、租约恢复和取消等状态推进路径
改写（例如 `NO_CAPABLE_EXECUTOR`、`LEASE_EXPIRED`），拒绝证据不随之改变——一个分片可以同时呈现
"当前无兼容执行器"与"最近一次被 nodeA 以合同不符拒绝"，两者都是事实。拒绝不改变 shard 状态，
收敛仍由接收重试与 ready 唤醒上限按失败策略完成。

`errorType` 的生命周期与状态同步：它只保留**未解除**的推进异常——成功取得执行权时清除（异常已经
解除），成功终态时清除（成功与异常标记不能同时为真，保留期内的审计读取不会把成功记录当作失败）；
失败、死亡与取消终态保留**尚未解除**的推进异常归因（租约判死、根取消等从未成功 start 的路径）。
Handler 执行失败走的是"成功 start（清除旧标记）→ 失败完成"，其归因由 `resultCode` 与
`resultSummary` 承担，此类最常见失败终态的 `errorType` 为空——判断失败原因一律以
`resultCode`/`resultSummary` 为准，不能假设失败记录的 `errorType` 必然可用。
普通 Run 与 Fanout shard 遵循同一规则。

Fanout 的合同复验不能省略：能力快照冻结之后可能发生滚动发布或节点重启，"快照创建时兼容"不等于
"取得执行权时仍兼容"，否则旧 shard 会被新合同的同名 Handler 用错误 Schema 解码执行。
接收阶段先复验的原因是回执即承诺执行：先确认再在 start 门禁前卡住，接收重试机制会把一个确定
不可执行的目标当作健康目标。

**合同变更与滚动发布**：合同门禁意味着提升 `contractRevision`、更换 `schema` 或收窄 `codecs` 会让
**已经在飞的 shard 被升级后的节点拒绝**——这是有意为之，静默用新 Schema 解码旧参数比丢一批分片
严重得多，但它是部署行为的变更：

- `REASSIGN_ON_FAILURE`：每个被拒 shard 先耗尽 `fanout-receipt-max-retries` 次重发，再重分配到尚未
  升级的节点；全部节点完成升级后这些 shard 找不到兼容目标，根在 `fanout-max-wait-ms` 后以
  `WAIT_TIMEOUT` 收敛为 `FAILED`。
- `STRICT_SNAPSHOT`：不换目标，直接等到根等待超时。

因此跨越发布窗口的 Fanout 批次可能整批失败而不是"看起来成功"。需要提升合同修订号时，应先
`pause` 根任务停止新触发——这是唯一集群级生效的手段：`pause` 写入 Redis 并把任务移出 schedule，
所有节点的扫描都不再触发它。`drain()` 达不到这个目的：它只让**本节点**停止领取普通与 Fanout
任务，本节点的调度扫描仍会为到期任务创建 Run，新 Run 会被未 drain 的节点执行，根 Handler 照常
产生新的 Fanout 批次。等在飞批次全部到达终态后再滚动升级；无法排空时要按上述语义预期该窗口内
批次的失败，并依赖 `redis_job_fanout_accept{CONTRACT_MISMATCH}` 告警观察影响范围。

定义已经声明来源 A 时，来源 B 的节点重新登记同摘要定义会被拒绝而不是改写该字段——覆盖等于把需要
告警的串源记录洗成本地来源。来源不一致的普通 Run 进入带保留期的 `FAILED` 终态并移出可见性索引，
不走普通重投；否则每条串源记录都会永久占用一个 Run 与 visible 成员并持续产生新的 Stream 消息。

键前缀只说明记录放在哪里，不能作为来源证明。跨语言客户端、运维补偿脚本或装配错误的生产者都可能把
A 来源的记录写到 B 的前缀下；此时消费者需要一个记录自带的、可独立核对的字段才能发现串源，
而不是按本地 qualifier 构造上下文照常执行。

### 不可变布局门禁

每个 `(qualifier, namespace)` 在自身 Redis 的 registry slot 维护一个 layout marker：

```text
rjob:{<qualifier>:<namespace>:registry}:layout
```

内容是 `protocolVersion|qualifier|namespace|shardCount|fanoutBucketCount|dispatchGroup|pubsubMode` 指纹。
首个节点原子写入，其后所有节点——包括其它语言的实现——必须完全一致才允许登记任务、注册能力或启动线程；
不一致直接抛错终止启动：

```text
RedisJob layout mismatch for qualifier=primary namespace=settle;
this node=1|primary|settle|32|2|redis-job-executor;
already established=1|primary|settle|64|2|redis-job-executor
```

分片数不同会让同一任务落到不同分片、形成两份定义和两套调度时刻并产生重复 Run；Fanout 桶数不同会让相同
`fanoutId` 的 root、receipt、lease 和清理游标分散到不同 slot；消费组不同会让同一条 Stream 被两个消费组
各消费一次；`pubsub-mode` 不同则一侧用 `SSUBSCRIBE/SPUBLISH`、另一侧用 `SUBSCRIBE/PUBLISH`，
两套通道互不可见，健康的 Fanout 目标会被误判失联并触发重分配。
这些偏差都不会自行收敛，因此必须在节点加入前挡住，而不是靠文档约定。

要调整这些不可变项，使用新的 namespace。

数据源不存在时应用**启动失败**，不会静默回退到 `primary`：

```text
unknown RedisJob qualifier: no-such-source; registered=redisProxy,matchRedisProxy;
RedisJob refuses to fall back to primary
```

回退会把任务定义、Run、Stream 和 Fanout 记录写进错误的 Redis，形成一份无人调度或被另一个集群重复调度
的定义，因此这里必须终止启动而不是继续运行。启动被拒时不会在任何数据源留下记录。

编程式访问同样必须显式选择 source：

```java
RedisJobScheduler primary = RedisJobSchedulers.scheduler("primary");
RedisJobScheduler match = RedisJobSchedulers.scheduler("match");
```

静态入口第一次引用 source 时惰性建立 Scheduler，后续返回同一实例。业务没有引用的 RedisProxy 不会产生
RedisJob 扫描线程、订阅或注册表成员；空 source、未知 source 和容器关闭后的访问都会直接拒绝。
静态入口绑定 JVM 内当前唯一的 `RedisJobSchedulers` 管理器，而不是脱离容器生命周期的全局对象：同一 JVM
并行建立第二套管理器会被拒绝；原上下文关闭时先撤销静态入口，再关闭所有已建立 Scheduler。需要在同一 JVM
顺序重建上下文时，必须等旧上下文完成关闭后再建立新管理器，旧 Scheduler 引用不能跨上下文复用。

## 配置

全部属性位于 `nasa.redis.job`：

| 属性 | 默认值 | 作用与约束 |
|---|---:|---|
| `enabled` | `false` | RedisJob 总开关 |
| `namespace` | `default` | 任务隔离与 hash tag 路由命名空间 |
| `application-name` | `application` | 执行器应用身份 |
| `instance-identity` | 空 | 稳定节点身份；空时回退主机名 |
| `protocol-version` | `1` | 普通 Run 协议门禁 |
| `shard-count` | `64` | 普通调度分片数，已有数据时不可原地改变 |
| `fanout-bucket-count` | `32` | Fanout 固定桶数，已有数据时不可原地改变 |
| `pubsub-mode` | `SHARDED` | `SHARDED` 需要 Redis 7+；`BROADCAST` 为 Redis 6.2 降级模式 |
| `min-scan-interval-ms` | `50` | 活跃扫描最短间隔 |
| `max-scan-interval-ms` | `30000` | 空闲扫描最长退避 |
| `schedule-rtt-allowance-ms` | `100` | 判断误触发前允许的 Redis 往返延迟 |
| `need-recompute-max-retries` | `3` | 调度逻辑时刻并发变化后单轮重新计算上限 |
| `scan-batch-size` | `100` | 单轮索引候选上限 |
| `dispatch-group` | `redis-job-executor` | 普通 Run Dispatch Stream 的消费组，同一命名空间内必须一致 |
| `max-dispatch-attempts` | `20` | 连续消息重建阈值；超限后标记 `NO_CAPABLE_EXECUTOR` 并降为最长扫描间隔，取得执行权后归零 |
| `executor-capacity` | `128` | 当前执行器登记容量 |
| `handler-capacity` | `8` | 普通任务按 Worker 限流；全部 Fanout Worker 共享独立的 Fanout 通道总上限；不得大于 executor capacity |
| `lease-ms` | `30000` | attempt 租约时长 |
| `lease-renew-ms` | `10000` | 续期间隔；租约必须覆盖两次续期和允许停顿 |
| `renew-rtt-allowance-ms` | `1000` | 计算本地保守持权截止点时扣除的续期往返预算 |
| `clock-drift-allowance-ms` | `1000` | 计算本地保守持权截止点时扣除的时钟偏差预算 |
| `max-tolerated-gc-pause-ms` | `1000` | 租约安全关系允许的最大进程停顿预算 |
| `max-run-duration-ms` | `3600000` | Handler 协作式取消、重试退避与停机等待的全局上界；不会强制中断业务线程 |
| `visibility-timeout-ms` | `60000` | 派发消息可见性兜底，不得小于两倍租约 |
| `heartbeat-ms` | `10000` | 执行器心跳周期 |
| `executor-expire-ms` | `30000` | 执行器失效期限，至少覆盖两个心跳周期 |
| `node-unready-evidence-count` | `3` | 不同 Fanout 根确认同一心跳代次失联后降级节点的证据数，必须大于 1 |
| `xautoclaim-min-idle-ms` | `30000` | PEL 接管最小 idle，必须位于扫描间隔与可见性期限之间 |
| `max-serial-backlog` | `1000` | 单任务串行等待上限 |
| `serial-overflow-policy` | `SKIP_OLDEST` | 串行溢出策略 |
| `max-catch-up-runs` | `50` | 一次闲置恢复保留的最近补偿 Run 总量上限 |
| `max-catch-up-window-ms` | `3600000` | 补偿逻辑时刻窗口 |
| `fanout-max-members` | `512` | 单个快照最大成员数 |
| `fanout-delivery-batch-size` | `64` | 单段 Fanout Lua 的分片批量 |
| `fanout-max-total-parameter-bytes` | `16777216` | 整批 Fanout 参数总字节上限 |
| `max-parameter-bytes` | `65536` | 单 Run 或单 shard 参数上限 |
| `fanout-create-timeout-ms` | `60000` | 根 intent 建立与桶提交截止时间 |
| `fanout-max-wait-ms` | `1800000` | 根等待全部 shard 的最长时间 |
| `fanout-max-assignments` | `5` | 单 shard 真实故障 assignment 上限；容量路由另行限频，并以该值减一作为单轮突发上限 |
| `ready-max-wakeups` | `3` | 已确认接收但未 start 的普通唤醒上限；耗尽后先复验目标存活性，再分流到容量等待或故障策略 |
| `fanout-capacity-wait-ms` | `10000` | 当前目标连续等待本地容量的窗口；超窗后只在有其它兼容节点时尝试有界容量路由 |
| `fanout-cleanup-batch-size` | `100` | 单轮 Fanout 回收的 shard 数，限制 Lua 工作量 |
| `fanout-retention-ms` | `604800000` | Fanout 终态记录保留期 |
| `run-retention-ms` | `604800000` | 普通 Run 终态保留期 |
| `completion-retention-ms` | `604800000` | 预留完成事件保留期；当前完成 Stream 由近似长度限制裁剪，不读取此值 |
| `tombstone-retention-ms` | `604800000` | 删除定义的 tombstone 保留期 |
| `registry-gc-grace-ms` | `3600000` | 过期执行器回收宽限，必须大于 Fanout 最大等待时间 |
| `max-result-summary-bytes` | `4096` | 持久化 Handler 结果摘要的 UTF-8 字节上限 |
| `wire.json.default-typing` | `false` | 必须保持关闭 |
| `sources` | 空映射 | 按 source id 覆盖根级属性；只影响业务显式引用的数据源，不会主动建立 Scheduler |

构造 `RedisJobProperties` 时会在任何 Redis 写入和任务线程启动前校验租约、可见性、心跳、容量、Fanout
等待、保留期和参数大小之间的关系；不安全组合直接使应用启动失败。

### 逐数据源覆盖

`nasa.redis.job.sources.<source-id>.<属性>` 覆盖单个数据源的参数，未覆盖的属性沿用 `nasa.redis.job`
根级取值。只有配置中显式出现的键才生效，因此逐源配置不会用类型默认值把根级设置抹掉。

```yaml
nasa:
  redis:
    job:
      enabled: true
      namespace: settle
      lease-ms: 30000
      executor-capacity: 128
      sources:
        match:
          namespace: match-settle     # 只改这两项
          executor-capacity: 32       # lease-ms 等其余属性仍取根级值
```

`sources.<id>` 只是覆盖入口，不是启用数据源的前置条件：`@RedisJob.qualifier` 或
`RedisJobSchedulers.scheduler(id)` 实际引用哪个数据源，才为它建立 Scheduler；未配置覆盖项时整套参数取
根级默认值。source id 由实际选中的 RedisProxy 冻结，同时决定键空间与上下文回报，不能由逐源参数改写。

数据源名支持驼峰。`@RedisJob(qualifier = "userCenter")` 对应的覆盖节点可以写成 `user-center`、
`userCenter` 或 `USER_CENTER`，三种写法等价：

```yaml
nasa:
  redis:
    job:
      sources:
        user-center:          # 与 userCenter、USER_CENTER 等价
          namespace: user-center-settle
```

## 生命周期与运维

### 健康状态

`scheduler.health()` 返回：

- `UP`：生命周期运行且最近心跳未过期；
- `DRAINING`：停止领取新任务，已取得执行权的 Handler 仍可完成；
- `DEGRADED`：生命周期运行但最近心跳超过执行器失效期限；启动后首次心跳完成前也可能短暂出现；
- `DOWN`：调度器未运行。

启动探针应给出至少一个 `heartbeat-ms` 的宽限。停止时先 drain 注册表和派发入口，再等待本地 Handler，最后
注销执行器；这样新 Fanout 快照不会继续选择正在退出的节点。

`stop()` / `close()` 是不可逆的资源释放：它们关闭监视线程池、Dispatcher 和订阅，之后不能对同一 Scheduler
再次 `start()`。需要临时停止领取但保留运行时，应使用 `drain()`，恢复时调用 `activate()`；需要完整停机后
重新启动时必须重建 Spring 应用上下文和 Scheduler，不能复用旧引用。

### 管理入口

`RedisJobScheduler` 提供：

- `trigger(...)` / `triggerJson(...)`：requestId 幂等手工触发；
- `pause(...)` / `resume(...)`：单任务门禁；
- `pauseNamespace(...)` / `resumeNamespace(...)`：逐分片控制整个命名空间的新触发；
- `cancel(...)`：协作式取消普通或 Fanout 根 Run；
- `delete(...)`：带单调修订号和 tombstone 的定义删除，立即使未取得执行权的存量 Run 失效（见下方"删除与存量收敛"）；
- `resolveConflict(...)`：显式选择本地定义摘要；
- `findRun(...)`：读取权威 Run 状态；
- `drain()` / `activate()`：滚动发布期间停止或恢复领取；
- `metrics()` / `health()`：基础观测。

命名空间暂停或恢复按调度分片逐一传播，不是跨分片原子事务；每次普通 Run 触发、领取和可见性重建仍在
目标分片 Lua 中复验门禁。控制调用失败时部分分片可能已经进入目标状态，调用方不能据此判定整个命名空间
已暂停或已恢复，必须使用同一 actor 幂等重试直至成功；当前不提供聚合的“部分生效”指标，调用失败本身
就是未收敛信号。暂停不会撤销已经取得的 attempt，已经建立的 Fanout 批次也会继续投递和收敛。

### 指标

`RedisJobMetrics.snapshot()` 返回不绑定具体指标库的只读快照，应用可桥接到 Micrometer 或其它系统。
主要指标包括：

- `redis_job_started_total`
- `redis_job_running`
- `redis_job_handler_duration_ms`
- `redis_job_schedule_lag_ms`
- `redis_job_stale_finish_total`
- `redis_job_fanout_received_total`
- `redis_job_fanout_shard_started_total`
- `redis_job_fanout_stale_assignment_total`
- `redis_job_registry_gc_total`
- `redis_job_tombstone_gc_total`

分类指标会按脚本返回码或终态追加小写后缀，按环节分组：

- `redis_job`（普通 Run 终态）与 `redis_job_fire`、`redis_job_start`；
- `redis_job_dispatch_envelope`：Dispatch 信封声明的来源与本 Scheduler 不一致（`SOURCE_MISMATCH`）；
- `redis_job_fanout_accept` / `redis_job_fanout_start`：Fanout 接收与认领门禁的拒绝码，包括
  `SOURCE_MISMATCH`、`PROTOCOL_UNSUPPORTED`、`CONTRACT_MISMATCH` 与（start 侧）`IDENTITY_MISMATCH`；
- `redis_job_fanout_finish` / `redis_job_fanout_shard`：分片完成拒绝码与终态；
- `redis_job_fanout_root`：根记录声明的来源与本 Scheduler 不一致（`SOURCE_MISMATCH`）。计数口径是
  **观测次数**而不是外来根数量：同一个外来根在收敛前的每轮扫描都会累加，不能用作"存在多少个
  外来根"的面板；配套告警日志按 60 秒节流，多个外来根并发时日志只包含其中一个 `fanoutId`，
  完整清单以 Redis 中根记录自身的来源字段为准。

生产环境至少应对持续 `DEGRADED`、schedule lag、stale finish、stale assignment、`DEAD`、`FAILED`、
`PARTIAL_FAILED` 和 registry 回收量建立告警。此外，所有 `SOURCE_MISMATCH` 与 `CONTRACT_MISMATCH`
类计数都应按"任意增长即告警"处理：它们分别意味着跨数据源串写和合同漂移，是这几道门禁存在的
全部意义。排查不依赖进程本地指标：被拒 shard 会留下 `rejectedCode`、`rejectedBy`、
`rejectedExpectation`、`rejectedAt` 四个证据字段（门禁在兼容节点上通过时自动清除；与状态推进路径
写入的 `errorType` 互相独立、互不覆盖），串源的普通 Run 则直接进入 `FAILED` 终态并携带
`observedSource` 与 `observedDefinitionSource`。

容量压力目前通过 shard 持久字段观测，不提供单独的聚合指标。持续存在 `capacityDeferredAt` 表示目标仍在
响应但没有取得本地 Handler 槽；`capacityRouteCount` 达到 `fanout-max-assignments - 1` 且存在
`capacityRouteBlockedAt` 表示当前处于静默窗口；`capacityRouteTotal` 只累计生命周期迁移次数。它们不应
与 `node-unready-evidence-count` 或 `FANOUT_UNREADY` 混为同一类告警。

### 删除与存量收敛

`delete(jobName, revision)` 是 O(1) 动作：写入 `DELETED` 状态、永久删除修订 fence
（`deletedRevision`）、tombstone 截止点，从 Schedule 撤出并登记分片 `reaping` 标记。
存量 Run 的终态化由执行门禁、完成/租约恢复和后台收敛过程分批完成，不在删除调用内做全量扫描：

后台收敛过程覆盖四类索引（waitq、visible、leases、waiting），不依赖本地定义仍然存在——
删除分片最后一个定义后常规索引扫描不再覆盖该分片，该过程是这些存量唯一的推进者。
共享索引（visible/leases/waiting）用 ZSCAN 游标跨轮接力扫描，游标持久在任务记录上，
其它任务的大量成员不会永久遮挡目标；完整扫描周期无未决存量才判定该索引归零。
单轮终态化总量受 `scan-batch-size` 约束；ZSCAN 的 `COUNT` 仅用于提示返回量，
脚本会在页内到达预算时立即停止，且不会把未完整处理的扫描周期判定为已归零。
同名任务经过更高修订号重建后再次删除时会开启新的扫描周期，不继承上一轮游标的归零结论。
已删除任务的失败重试、容量延后和所有串行槽移交都在写回索引前复验删除 fence；
旧 Run 直接按删除终态收敛，保证收敛期间存量只减不增：
如果在途 Dispatch 消息抵达时 Run 已由后台收敛为终态，容量延后路径只 ACK 当前 PEL 项，
不改写 Run 也不删除 Stream 记录，避免迟到消息被重复接管。

| 存量状态 | 收敛路径 | 结果 |
|---|---|---|
| `QUEUED` / `RETRY_WAIT` | start/promote/defer 的删除 fence，或后台过程直接终态化 visible 存量 | `CANCELLED`，`resultCode=JOB_DELETED`，写 Completion 并进入保留期 |
| `BLOCKED`（waitq 成员） | 任何路径释放串行槽时先复验 fence，单次最多处理 100 个队首，余量由后台分批推进 | 同上 |
| `RUNNING`（租约未到期） | 允许当前 attempt 完成；其租约计入未归零存量，`reaping` 标记保留 | 正常终态；失败重试直接按删除终态提交，不建立新 attempt |
| `RUNNING`（租约已到期） | 租约恢复或后台过程按持久 fence 撤销旧权威 | `CANCELLED`，`resultCode=JOB_DELETED`，并保留 `errorType=LEASE_EXPIRED` 归因 |
| `RUNNING` 尚未写入 Fanout intent | prepare 在转移完成权前复验删除 fence | 拒绝创建新 Fanout，当前 attempt 由普通完成出口收敛 |
| `FANOUT_CREATING` / `WAITING_CHILDREN` | 截止点前允许桶内已取得执行权的分片完成；截止后后台过程返回持久定位证据，由 Java 跨 slot 关闭桶内新执行权，再对账普通根 | 桶已终态则保留其真实结果；桶缺失或因删除取消则根为 `CANCELLED/JOB_DELETED` |

`reaping` 标记只有在四类索引全部无 fence 内存量且扫描未截断时才撤销，它是 tombstone 清理的唯一
存量门禁；在飞 Fanout 的 waiting 成员会保留到桶关闭与普通根回填完成。因此定义删除
必然发生在全部存量终态之后，fence 不会在旧 Run 存活时随定义消失。Lua 不直接终态化带有
`fanoutId` 的普通根：Fanout 桶与调度分片在 Redis Cluster 的不同 slot，先收尾普通根会留下仍可执行的 shard。
桶进入 `CANCELLING` 后，已过期的 shard 租约使用取消专用恢复路径撤销旧 owner；该路径不依赖本地
Worker 定义仍然存在，也不会依据替代配置创建新 attempt、重试或重分配。因此删除 Fanout Worker
不会拆除崩溃 shard 的最终取消驱动器。

删除修订 fence 是永久的：`definitionRevision <= deletedRevision` 的 Run 即使在同名任务以更高修订号
重新登记后也不会重新取得执行权；重新登记的完整写入不触碰 `deletedRevision`。新定义产生的 Run
修订号高于 fence，正常执行。旧修订占用串行槽时，完成、取消、Fanout 根收尾、租约恢复或后台撤权
都会越过并终态化旧队首，再开放首个新修订队首；一次原子段未找到新队首时，`reaping` 标记
会保留余量并继续移交，避免重建后的合法积压失去唤醒者。因删除终态化的 FIXED_DELAY Run 不会重新排程。

Fanout 能力索引只由真正具备 Fanout Handler 的定义（`FANOUT_ONLY`）持有：普通定义即使复用
`workerName` 也不写入能力、不参与快照选择，也不计入删除时的能力引用。删除 Fanout Worker 时
同时撤销本地 Handler、协调器契约与本执行器在集群能力索引中的成员（能力被多个 `FANOUT_ONLY`
定义共享时引用归零才撤销），删除之后创建的新根不会再把该 Worker 冻结进快照；
已取得执行权的 shard 按"允许当前 attempt 完成"继续。需要"暂时停止、以后继续"的可逆语义时使用
`pause`/`resume`，删除没有反悔窗口。收敛量计入 `redis_job_deleted_reap_total`。

## 数据保留与回收

非终态 Run 不设置 TTL，避免长时间积压或运行期间静默消失。进入终态后才设置 `run-retention-ms`；完成事件、
删除定义 tombstone、过期执行器和 Fanout 记录分别由自己的保留期与有界扫描回收。

Fanout 清理顺序是：确认普通根已经对账，再分批删除 shard 和 inbox，最后删除 root 与索引。`seq`、
`executionKey`、receipt、assignment、owner、attempt token 和结果摘要随 shard HASH 一起删除。

Redis 记录删除后，框架幂等窗口同时结束。需要更长或永久业务幂等时，调用方必须把 `runId` 或
`executionKey` 保存到自己的持久系统并按业务策略管理。
