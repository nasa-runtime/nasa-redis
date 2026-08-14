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

## 运行架构

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

派发 Stream 提供持久消息，`visible` ZSET 提供消息丢失后的重建入口，`leases` ZSET 记录当前执行权截止点，
`XAUTOCLAIM` 接管死消费者的 PEL。任何一层都可能重复观察同一个 Run，因此最终执行语义是至少一次。

### Fanout 路径

根 Handler 调用 `context.fanout(workerName)` 后，框架先在 registry slot 冻结兼容执行器快照，再把确定性
Fanout intent 写入一个固定 Fanout 桶：

```text
根 Run RUNNING
   │
   ├─冻结兼容能力快照
   ├─按快照成员数生成同样数量的分片
   ├─根 Run 进入 FANOUT_CREATING
   ├─Fanout 桶分批写 shard 并 commit
   └─根 Run 进入 WAITING_CHILDREN
             │
             ▼
每目标节点持久 inbox + Pub/Sub 通知
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

注册表快照和 Fanout 桶位于不同 slot，无法由一段 Lua 同时提交。框架使用可恢复 intent 连接这两个原子域：
根 Run 先记录 `fanoutId`、快照与创建截止点；桶内 commit 成功后再把普通根推进到等待子任务状态；任一响应
丢失都可以根据持久记录重放或对账。

Pub/Sub 不是正确性的唯一来源。通知丢失后，receipt deadline、ready、lease 和 root 看门狗会重新推动状态机。

## Redis Cluster 键路由

调度分片使用固定 hash tag：

```text
rjob:{<namespace>:<schedule-shard>}:schedule
rjob:{<namespace>:<schedule-shard>}:job:<jobName>
rjob:{<namespace>:<schedule-shard>}:run:<runId>
rjob:{<namespace>:<schedule-shard>}:dispatch:<workerKey>
rjob:{<namespace>:<schedule-shard>}:visible
rjob:{<namespace>:<schedule-shard>}:leases
```

执行器注册表使用独立 registry slot：

```text
rjob:{<namespace>:registry}:executors
rjob:{<namespace>:registry}:executor:<executorId>
rjob:{<namespace>:registry}:capability:<workerName>
```

Fanout 根据 `fanoutId` 稳定映射到固定桶：

```text
rjob:{<namespace>:fanout:<bucket>}:root:<fanoutId>
rjob:{<namespace>:fanout:<bucket>}:shard:<fanoutId>:<seq>
rjob:{<namespace>:fanout:<bucket>}:inbox:<nodeIdentity>
rjob:{<namespace>:fanout:<bucket>}:receipts
rjob:{<namespace>:fanout:<bucket>}:ready
rjob:{<namespace>:fanout:<bucket>}:lease
rjob:{<namespace>:fanout:<bucket>}:roots
rjob:{<namespace>:fanout:<bucket>}:gc
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
| `SERIAL_QUEUE` | 新 Run 进入有界等待队列，前序释放后按逻辑时刻继续 |
| `DISCARD_IF_RUNNING` | 新 Run 进入 `SKIPPED` |
| `PARALLEL` | 各 Run 独立竞争本地与集群执行容量 |

`max-serial-backlog` 限制串行积压，达到上限时由 `serial-overflow-policy` 决定跳过最旧还是最新 Run。

### 误触发策略

| 策略 | 调度器发现逻辑时刻已经错过时 |
|---|---|
| `DO_NOTHING` | 跳过旧时刻并推进到未来 |
| `FIRE_ONCE_NOW` | 生成一个补偿 Run，再推进到未来 |
| `CATCH_UP` | 在 `max-catch-up-window-ms` 内保留最多 `max-catch-up-runs` 个最近时刻 |

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

当前制品没有发布 Go 或 Rust SDK。其它运行时要加入同一能力集群，必须实现相同的身份、心跳、能力合同、
inbox、receipt、attempt fencing 和终态协议，不能只做到 JSON 解码就宣称兼容。

## 配置

全部属性位于 `nasa.redis.job`：

| 属性 | 默认值 | 作用与约束 |
|---|---:|---|
| `enabled` | `false` | RedisJob 总开关 |
| `namespace` | `default` | 任务隔离与 hash tag 路由命名空间 |
| `qualifier` | `primary` | 使用哪个 `RedisProxy` |
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
| `max-dispatch-attempts` | `20` | 可见性消息重建上限，阻止无法派发的 Run 无限循环 |
| `executor-capacity` | `128` | 当前执行器登记容量 |
| `handler-capacity` | `8` | 本地同时运行 Handler 上限，不得大于 executor capacity |
| `lease-ms` | `30000` | attempt 租约时长 |
| `lease-renew-ms` | `10000` | 续期间隔；租约必须覆盖两次续期和允许停顿 |
| `renew-rtt-allowance-ms` | `1000` | 计算本地保守持权截止点时扣除的续期往返预算 |
| `clock-drift-allowance-ms` | `1000` | 计算本地保守持权截止点时扣除的时钟偏差预算 |
| `max-tolerated-gc-pause-ms` | `1000` | 租约安全关系允许的最大进程停顿预算 |
| `max-run-duration-ms` | `3600000` | 全局 Handler attempt 与停机等待上限 |
| `visibility-timeout-ms` | `60000` | 派发消息可见性兜底，不得小于两倍租约 |
| `heartbeat-ms` | `10000` | 执行器心跳周期 |
| `executor-expire-ms` | `30000` | 执行器失效期限，至少覆盖两个心跳周期 |
| `node-unready-evidence-count` | `3` | 不同 Fanout 根确认同一心跳代次失联后降级节点的证据数，必须大于 1 |
| `xautoclaim-min-idle-ms` | `30000` | PEL 接管最小 idle，必须位于扫描间隔与可见性期限之间 |
| `max-serial-backlog` | `1000` | 单任务串行等待上限 |
| `serial-overflow-policy` | `SKIP_OLDEST` | 串行溢出策略 |
| `max-catch-up-runs` | `50` | 单轮最近补偿 Run 上限 |
| `max-catch-up-window-ms` | `3600000` | 补偿逻辑时刻窗口 |
| `fanout-max-members` | `512` | 单个快照最大成员数 |
| `fanout-delivery-batch-size` | `64` | 单段 Fanout Lua 的分片批量 |
| `fanout-max-total-parameter-bytes` | `16777216` | 整批 Fanout 参数总字节上限 |
| `max-parameter-bytes` | `65536` | 单 Run 或单 shard 参数上限 |
| `fanout-create-timeout-ms` | `60000` | 根 intent 建立与桶提交截止时间 |
| `fanout-max-wait-ms` | `1800000` | 根等待全部 shard 的最长时间 |
| `fanout-max-assignments` | `5` | 单 shard 最大 assignment 数 |
| `ready-max-wakeups` | `3` | 已确认接收但未 start 的唤醒上限 |
| `fanout-cleanup-batch-size` | `100` | 单轮 Fanout 回收的 shard 数，限制 Lua 工作量 |
| `fanout-retention-ms` | `604800000` | Fanout 终态记录保留期 |
| `run-retention-ms` | `604800000` | 普通 Run 终态保留期 |
| `completion-retention-ms` | `604800000` | 预留完成事件保留期；当前完成 Stream 由近似长度限制裁剪，不读取此值 |
| `tombstone-retention-ms` | `604800000` | 删除定义的 tombstone 保留期 |
| `registry-gc-grace-ms` | `3600000` | 过期执行器回收宽限，必须大于 Fanout 最大等待时间 |
| `max-result-summary-bytes` | `4096` | 持久化 Handler 结果摘要的 UTF-8 字节上限 |
| `wire.json.default-typing` | `false` | 必须保持关闭 |

构造 `RedisJobProperties` 时会在任何 Redis 写入和任务线程启动前校验租约、可见性、心跳、容量、Fanout
等待、保留期和参数大小之间的关系；不安全组合直接使应用启动失败。

## 生命周期与运维

### 健康状态

`scheduler.health()` 返回：

- `UP`：生命周期运行且最近心跳未过期；
- `DRAINING`：停止领取新任务，已取得执行权的 Handler 仍可完成；
- `DEGRADED`：生命周期运行但最近心跳超过执行器失效期限；启动后首次心跳完成前也可能短暂出现；
- `DOWN`：调度器未运行。

启动探针应给出至少一个 `heartbeat-ms` 的宽限。停止时先 drain 注册表和派发入口，再等待本地 Handler，最后
注销执行器；这样新 Fanout 快照不会继续选择正在退出的节点。

### 管理入口

`RedisJobScheduler` 提供：

- `trigger(...)` / `triggerJson(...)`：requestId 幂等手工触发；
- `pause(...)` / `resume(...)`：单任务门禁；
- `pauseNamespace(...)` / `resumeNamespace(...)`：逐分片控制整个命名空间的新触发；
- `cancel(...)`：协作式取消普通或 Fanout 根 Run；
- `delete(...)`：带单调修订号和 tombstone 的定义删除；
- `resolveConflict(...)`：显式选择本地定义摘要；
- `findRun(...)`：读取权威 Run 状态；
- `drain()` / `activate()`：滚动发布期间停止或恢复领取；
- `metrics()` / `health()`：基础观测。

命名空间暂停是逐调度分片传播的最终一致控制；每次实际触发仍在目标分片 Lua 中复验门禁。暂停不会撤销
已经取得的 attempt。

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

分类指标会按脚本返回码或终态追加小写后缀。生产环境至少应对持续 `DEGRADED`、schedule lag、stale finish、
stale assignment、`DEAD`、`FAILED`、`PARTIAL_FAILED` 和 registry 回收量建立告警。

## 数据保留与回收

非终态 Run 不设置 TTL，避免长时间积压或运行期间静默消失。进入终态后才设置 `run-retention-ms`；完成事件、
删除定义 tombstone、过期执行器和 Fanout 记录分别由自己的保留期与有界扫描回收。

Fanout 清理顺序是：确认普通根已经对账，再分批删除 shard 和 inbox，最后删除 root 与索引。`seq`、
`executionKey`、receipt、assignment、owner、attempt token 和结果摘要随 shard HASH 一起删除。

Redis 记录删除后，框架幂等窗口同时结束。需要更长或永久业务幂等时，调用方必须把 `runId` 或
`executionKey` 保存到自己的持久系统并按业务策略管理。
