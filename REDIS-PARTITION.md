# RedisPartition 架构与运行指南

RedisPartition 是 `nasa-redis` 内置的 Redis Stream 分区消费运行时。它按稳定业务键把消息路由到固定分区，
每个分区由 Redis 分布式锁授予单一消费 owner；应用实例扩缩容、退出或失去锁权威后，其它实例重新分配分区，
并通过 `XAUTOCLAIM` 接管已经进入 PEL 的未确认消息。

Java 入口位于 `io.github.nasaruntime.redis.cache.redis.partition`，公开入口类为 `RedisPartition`，
配置类为 `RedisPartitionProperties`。分区认领、任务执行、恢复、确认与指标实现均位于该包。
应用应按当前包名更新 import、反射类名和附加原生镜像配置，并重新编译引用这些类型的代码；Spring 配置仍使用
`nasa.redis.properties.<qualifier>.stream.partition`。运行环境需要 Redis 6.2+，以支持 `XAUTOCLAIM` 接管。

它适合订单、账户、合约、设备等“同一业务键必须串行，不同执行槽允许并行”的异步处理。业务只需要提供一份
YAML 配置和 `RedisEventSingleListener` Bean，并通过 `partitionKey(data)` 声明单条消息的本地执行键；分区
Stream、consumer group、成员心跳、分区锁、再平衡、本地 Partition 调度、确认复验和停机排干均由组件管理。

Redis 批次是传输单位，单条 listener 调用是业务处理边界。正常读取、历史接管和本地重试共用逐记录执行权，
成功业务交给确认链后不因 ACK 未决而重复调用；历史正文提交前复验 PEL 与当前来源权威。局部成功证据不会跨
进程或 consumer epoch 持久保留，因此业务仍需按至少一次交付实现幂等。

## 能力与边界

RedisPartition 提供：

- String 或 long 业务键到固定分区的稳定路由；
- 集群级每分区单一有效 owner 与持权消费路径；
- Redis standalone 和 Redis Cluster；
- Redis 服务端时间成员心跳，无应用级主节点；
- 节点上线、主动释放和优雅下线时的通知驱动再平衡，以及通知不可达时的周期兜底；
- Redis 批量读取与单条业务事务解耦，本地按有效 hash 进入 Partition 严格顺序 Task；
- holder-fenced Lua 原子复验分区 holder 并 XACK，Cluster 下强制 lock/stream 同 slot；
- BOTH 专用手工确认容器、唯一 consumer epoch、consumer-fenced Lua 和多 field record ledger；
- ACK 结果不确定时只复验 PEL/重试确认，不重复已经成功的 listener；
- 按来源代次协调同一 record 的在途执行与确认，历史 PEL 缺席时不执行迟到正文；
- ordered key 失败门禁、精确 PEL 重试、routeBlocked 与 raw/task/commit/retry 硬容量；
- `XAUTOCLAIM` 接管满足空闲阈值的 PEL，并协调当前 consumer 已有的本地处理责任；
- 默认共享组和按 topic 隔离的独立分区组；
- 同步发布、Pipeline 同步发布与 Pipeline 异步发布；
- 多 Redis 数据源隔离，以及当前节点分区快照。

RedisPartition 不提供：

- 外部数据库、HTTP、钱包或消息系统的 exactly-once 副作用；
- 跨不同分区的顺序；
- 不同 topic 之间的全局顺序；
- 动态修改既有命名空间的分区数；
- 跨 JVM 的任意普通 Stream 本地顺序；BOTH 的顺序能力只限当前 JVM；
- 死信队列、业务补偿或可视化控制台；
- 仅靠客户端 fencing 撤销已经进入外部系统的副作用。

交付语义是至少一次。业务回调必须按订单号、事件号等稳定事实幂等，不能把 Redis Stream 消息 ID 当作
外部副作用的唯一安全门禁。

## 运行架构

```text
publish(topic, event, partitionKey, data)
                    │
                    ▼
       Java hash + 符号位屏蔽 + 取模
                    │
                    ▼
       <streamPrefix>:<partition>
                    │
                    ▼
              Redis Stream
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
  分区独占锁              consumer group / PEL
        │                       │
        └───────────┬───────────┘
                    ▼
          当前 owner 的 Claim runner
                    │
          先 XAUTOCLAIM，后 XREADGROUP
                    │
                    ▼
       逐 record 执行权 + 历史 PEL / holder 复验
                    │
                    ▼
       按 (topic, event) 解析单条消息
                    │
                    ▼
        partitionKey → PartitionRunner Task
                    │
                    ▼
       Future 真实终态 + ordered key 门禁
                    │
                    ▼
       登记 CommitAttempt，交接确认责任
                    │
                    ▼
       holder-fenced Lua 原子复验 + XACK
```

一个 `RedisProxy` 对应一个 `RedisPartition`。每个分区组拥有独立的 Stream 命名空间、consumer group、
成员 ZSET、唤醒频道、再平衡任务和拉取容器；这些容器复用对应 `RedisProxy` 已装配的连接工厂、值序列化方式和
业务执行基础设施。物理 Redis key 固定使用 Spring UTF-8 `StringRedisSerializer`。组之间可以配置不同的分区数、
批大小、阻塞时间和接管阈值。

### 组件职责

| 组件 | 职责 |
|---|---|
| `RedisProxy` | 绑定数据源配置，发现 PARTITION/BOTH listener，先建组、再注册 listener、最后启动消费 |
| `RedisPartition` | 维护 topic 到分区组的路由，提供发布入口，统一管理分区组生命周期 |
| `PartitionGroup` | 复验持久分区合同，创建 Stream 与 consumer group，维护成员心跳并执行再平衡 |
| `Claim` | 代表本节点对一个物理分区的认领，持有分区锁，执行 PEL 接管、正常拉取、fencing 与排干 |
| `LettuceDistributedLock` | 用 Lua 授予可重入锁并由看门狗续租，提供三态持有权复验 |
| `BatchStreamMessageListenerContainer` | 驱动分区 Claim 的拉取生命周期和业务提交 |
| `StreamPartitionRuntime` | 维护计划、硬容量、Partition Task/Future、ordered gate、重试、确认复验与完整排干 |
| `ProxyRecordAckLedger` | 协调两类来源的逐 record 执行权，并保存 BOTH 当前 consumer epoch 的多 field 成功证据 |
| `StreamRetryCoordinator` | 保留失败坐标或完整受阻页，以时间轮退避驱动恢复，终态明确后归还容量 |
| `StreamCommitCoordinator` | 在 ACK I/O 前登记确认责任，结果不确定时只复验 PEL 或重试确认 |
| `StreamPendingRecovery` | 在接管响应不确定时保留扫描屏障，补扫当前 consumer 已接管但尚未交接的 PEL |

Redis 中的锁决定分区唯一 owner，成员 ZSET 只用于计算每个节点应持有的数量。即使成员视图短暂不一致，
两个节点也不能同时合法取得同一分区锁；因此公平分配是收敛目标，锁权威才是互斥门禁。

## Redis 命名空间

`default-group` 是整个 Redis 数据源下 RedisPartition 的命名空间前缀，也是默认共享组的 consumer group 名。
假设 `default-group: SINGLE-CONSUME`、默认分区数为 64：

分区合同、Stream、consumer group、成员键、锁槽与迁移扫描都依赖逻辑 key 到物理字节的一致映射，因此
RedisPartition 只接受 Spring UTF-8 `StringRedisSerializer` 作为 `RedisTemplate` 的 key serializer。框架创建的
模板默认满足该合同；通过 `RedisProxy(String, RedisTemplate)` 或 fallback Bean 提供自定义模板时也必须遵守。
其它序列化器会在首次实际建组或分区发布前被拒绝，不会写入合同或创建 Stream。普通 RedisProxy 命令不受这项
分区协议门禁影响；`stream.partition.enabled=false` 时，显式 `init()` 仍保持静默无副作用返回，但调用分区发布入口
仍会进入物理键协议并执行该门禁。

| 用途 | 默认共享组 | 逻辑隔离组 `settlement` |
|---|---|---|
| Stream | `SINGLE-CONSUME:0` … `SINGLE-CONSUME:63` | `SINGLE-CONSUME:settlement:0` … `SINGLE-CONSUME:settlement:63` |
| consumer group | `SINGLE-CONSUME` | `SINGLE-CONSUME:settlement` |
| 成员 ZSET | `SINGLE-CONSUME:nodes` | `SINGLE-CONSUME:settlement:nodes` |
| 唤醒频道 | `SINGLE-CONSUME:wake` | `SINGLE-CONSUME:settlement:wake` |
| 分区合同 | `SINGLE-CONSUME:partition-contract` | `SINGLE-CONSUME:settlement:partition-contract` |
| 锁业务 key | `SINGLE-CONSUME:lock:<partition>` | `SINGLE-CONSUME:settlement:lock:<partition>` |

锁业务 key 还会加上 `distributed-lock.prefix`。默认前缀是 `DISTRIBUTED-LOCK:`，但运维脚本和监控不应把
默认值写死，应以当前数据源配置为准。

整个命名空间还使用一个 `SINGLE-CONSUME:partition-topic-contracts` HASH。field 是 topic 的 URL-safe Base64，
value 固定该 topic 的实际 Stream 前缀、分区数、解析后键布局和哈希规则；它不属于任一隔离组，所有发布者共享。

隔离组 YAML 的 key 是逻辑短名，不能再带 `default-group` 前缀。`default-group`、隔离组逻辑名和 topic
到组的映射都属于持久键布局，运行期间不应随意改变。

表中是 standalone 默认布局。Redis Cluster 在 `key-layout=AUTO` 下使用共享 hash tag：Stream 为
`{<streamPrefix>}:<partition>`，锁业务 key 为 `{<streamPrefix>}:lock:<partition>`，加锁前缀后仍与 Stream
处于同一 slot，holder 复验和 XACK 才能在一段 Lua 中原子完成。组件会按最终序列化后的 Stream key 与加前缀锁 key
复验真实 slot；锁前缀提前形成其它 hash tag 时，会在合同、Stream 和锁产生副作用前拒绝配置。

首次建立当前组合同前，组件使用分页 `SCAN` 检查两种布局的完整命名空间；Cluster 会遍历全部持有 slot 的 master，
并只把严格非负十进制分区后缀且类型为 Stream 的键视为迁移证据。任一布局存在分区 Stream 都会拒绝建立合同：
相反布局不能安全混用，当前布局的稀疏索引也无法证明历史生产者采用的分区数。任一 master 无法完成扫描同样按证据
不足拒绝。扫描不能替代迁移窗口的写入隔离：应先停止旧生产、排空或迁移旧 Stream 与 PEL，再以全新命名空间统一
开放生产和消费；不能通过删除合同让现存 Stream 被新的 `count` 静默采纳。

## 路由与顺序语义

String 分区键使用：

```text
(partitionKey.hashCode() & Integer.MAX_VALUE) % count
```

long 分区键使用：

```text
(Long.hashCode(partitionKey) & Integer.MAX_VALUE) % count
```

因此只有在以下条件同时不变时，同一个业务键才保持原分区：

- 使用相同的 key 类型和值；
- `count` 不变；
- topic 仍映射到同一个默认组或隔离组；
- 不绕过 RedisPartition 直接拼 Stream key 发布。

String 分区键传 `null` 时使用当前发布进程内的 round-robin，只提供分布均摊，不提供同一业务对象的顺序。
需要顺序时必须传稳定非空业务键。空白 topic、空白 event 或 null data 不会写入消息；同步入口返回 null，
Pipeline 入口跳过该条命令。

分区内的保证是单一持权处理路径，不承诺固定 Java 线程身份。不同批次可能由执行基础设施中的不同线程处理，
业务不能依赖 `ThreadLocal` 在多批消息之间保存状态。

发布入口的 String/long key 只决定物理 Stream，消息协议不保存这个原始值。消费侧若还要求按订单、账户等业务键
进入本地 Partition，必须从消息体稳定字段重新计算 `partitionKey(data)`。框架不读取本地 Partition 的分区数，
也不自行取模或包装 key：

- `null`：每条消息独立提交 `strictOrder=false` Task；
- 任意 `Number`：按 `longValue()` 走 Partition 的 primitive long 入口；
- String 与其它非 Number 对象：原样走 Object 入口，使用其 `hashCode()`；
- 非 Number 对象从 `partitionKey` 返回到同步 `submit` 结束期间 hash 必须不变，之后框架不长期持有原始 key；
- 相同 plan、相同有效 hash 跨批次不会并发；hash 冲突会扩大串行与失败阻断范围，不会形成“不同 key 一定并行”。

`partitionKey` 必须是纯本地计算。其结果需要跨反序列化、进程重启和 PEL 重投保持相同有效 hash；默认身份 hash、
可变集合和可变 StringBuilder 不适合作为业务键。需要自定义执行域时通过 `partition()` 返回显式
`PartitionRunner`；该方法在注册计划建立时只调用一次。每个 RedisProxy 只对应一个 RedisPartition 和一个独立 Runner，
全部 listener 共用这套执行域。首个计划返回 null 时创建代理实例专属 Runner，不会落入 nasa-core 的全局 default
Runner；首个计划显式选择 Runner 时将其永久绑定到当前代理，后续计划只能返回 null 或同一对象。不同代理或应用其它
Partition 任务不得复用该对象，否则无法隔离 taskType、迁移状态和故障门禁。跨代理复用会在 Runner 启动和 Redis
副作用前拒绝。

激活顺序固定为先启动 Runner 对应的 `TimingWheel`，再启动 Runner，二者健康复验通过后才开放 Redis poll。
单个 listener 登记失败不会停止同代理已经承载其它计划的 Runner。RedisPartition 关闭时先封闭发布与读取、排干 Claim
和确认执行域，再停止自己独占的 Runner；只有 Runner 完整停止后才停止对应 TimingWheel。不同代理的执行域对象独立，
一个代理关闭不会改变另一个代理的运行状态。运行期 Runner 失健康会暂停来源，不会切换到其它 Runner。

## 声明式接入

### 最小 YAML

默认共享组只需要开启总开关，其它属性全部使用默认值：

```yaml
nasa:
  redis:
    properties:
      primary:
        stream:
          partition:
            enabled: true
```

默认会建立 64 个分区，命名空间为 `SINGLE-CONSUME`。只有容器中实际存在 `PARTITION` 或 `BOTH` listener
时才建立消费组和认领任务；仅发布消息的应用不会为了默认 64 个分区建立消费者运行时。

### Listener

```java
import com.fasterxml.jackson.core.type.TypeReference;
import io.github.nasaruntime.redis.cache.redis.ConsumeMode;
import io.github.nasaruntime.redis.cache.redis.RedisEventSingleListener;
import org.springframework.stereotype.Component;

@Component
public class SettlementListener implements RedisEventSingleListener<Order> {

    /** 业务作用：声明结算事件来源。参数说明: 无。返回: 结算 topic。 */
    @Override
    public String[] topics() {
        return new String[]{"contract:settlement"};
    }

    /** 业务作用：选择开仓事件。参数说明: 无。返回: 当前计划的 event。 */
    @Override
    public String event() {
        return "open-position";
    }

    /** 业务作用：选择持权分区消费。参数说明: 无。返回: PARTITION 模式。 */
    @Override
    public ConsumeMode mode() {
        return ConsumeMode.PARTITION;
    }

    /** 业务作用：声明订单解码类型。参数说明: 无。返回: 精确订单类型。 */
    @Override
    public TypeReference<Order> paramType() {
        return new TypeReference<>() {};
    }

    /** 业务作用：按账户约束执行顺序。参数说明: order 为当前订单。返回: 稳定账户键。 */
    @Override
    public Object partitionKey(Order order) {
        return order.uid();
    }

    /** 业务作用：处理一条订单事件。参数说明: order 为当前订单。返回: 正常返回后可确认，异常保留重试。 */
    @Override
    public void onEvent(Order order) {
        // 以稳定业务事件键保证外部副作用幂等
    }
}
```

`PARTITION` 与 `BOTH` 只接受 `RedisEventSingleListener<T>`。Redis 仍批量拉取，但业务每次只处理一条 T；同一
有效 hash 的多条消息可以进入同一 Partition Task，并保持 Redis 遇见顺序。第一条 listener 异常会立即阻断该
key，Task 尾部不再调用，失败头按 XPENDING + XRANGE 精确退避重试。`RedisEventBatchListener<T>` 继续只服务
`PROXY`，分区模式在注册期直接拒绝，不能把 transport batch 当作业务事务。Task Future 不等待回调自行派发的
异步工作，`onEvent` 必须在该条消息的业务处理完成后再返回。`mode()` 的含义如下：

| 值 | 行为 |
|---|---|
| `PROXY` | 只走普通 Stream 消费路径，也是默认值 |
| `PARTITION` | 只走 RedisPartition；物理分区 holder 与本地 Partition key 共同约束执行和确认 |
| `BOTH` | 同一计划服务 RedisPartition 与普通 Stream 两个来源；普通 Stream 侧只声明 JVM 内 `local_ordered` |

PARTITION 的 consumer group 由物理分区组命名空间决定，BOTH 还必须声明非空普通 Stream group。`autoDelete()`
默认 false：PARTITION 在 holder-fenced Lua 内、BOTH 在 consumer-fenced Lua 内，只对本次实际 XACK 成功的 id
执行 XDEL。XDEL 是 Stream 全局删除，不按 group 隔离；只有能够证明正文不再服务其它 group 时才可开启。

注册使用不可变计划和同一发布锁内的冲突复验。同一数据源、来源类型、Stream/topic、event 与 group 不能重复，
同一 stream/group 也不能混用 legacy PROXY 自动确认与 BOTH Future 驱动手工确认。任何冲突在开放 container 前
失败，不会覆盖已经发布的 listener。

### BOTH 普通 Stream 来源

`BOTH` 表示同一 listener 计划可以接收 RedisPartition 与普通 Stream 两种独立来源，不表示框架把同一条消息自动
执行两次。普通 Stream 侧要求非空 `group()`，并强制建立 dedicated container、手工 ACK、单本地 consumer；配置的
`consumer-name` 只作为可读前缀，实际名称还会追加进程 session 与 container epoch，重启前后的迟到确认不会被 Redis
误认为同一 consumer。

一个普通 Stream record 可以包含多个 event field，但 Redis 只能按整个 record id 确认。运行时在当前 consumer epoch
维护有界 ledger：field A 成功、field B 失败时只重试 B，A 的成功证据保持等待；全部 required field 成功后才一次
consumer-fenced XACK 并开放相关 ordered gate。consumer epoch 改变后本地证据失效，新 owner 整条重放，因此各
listener 仍必须幂等。`proxy-pending-min-idle-ms` 应高于正常业务高分位耗时，避免另一个节点在旧 listener 尚未结束时
过早 XAUTOCLAIM。普通 Stream 没有 RedisPartition holder，不能把这条路径声明为跨节点全局顺序入口。

### 发布

优先通过业务已经持有的 `RedisProxy` 发布，数据源选择最明确：

```java
redisProxy.partition("contract:settlement", "open-position", order.uid(), order);
```

这里发布端与 listener 都选择账户 `uid`。若业务需要按订单串行，应在两端统一选择订单键；不能把同一账户的
消息按不同订单分散到多个物理分区，却仍期待账户级集群顺序。

也可以使用 RedisPartition 门面：

```java
import io.github.nasaruntime.redis.cache.redis.partition.RedisPartition;

RedisPartition.load(redisProxy)
        .publish("contract:settlement", "open-position", order.uid(), order);

RedisPartition.load("archive")
        .publish("contract:settlement", "open-position", order.uid(), order);
```

省略 event 的重载会令 `event = topic`。发布端不要求本进程启用分区消费者：它会从 YAML 解析 topic 所属组、
分区数和 Stream 前缀并缓存路由，因此生产者与消费者可以分开部署。所有生产者与消费者仍必须使用相同的
分区合同。

### Pipeline 发布

```java
LettucePipeline.Actuator actuator = LettucePipeline.open(redisProxy, null);
try {
    actuator.partition("contract:settlement", "open-position", order.uid(), order);
    actuator.partitionAsync("contract:settlement", "open-position", anotherOrder.uid(), anotherOrder);
} finally {
    actuator.pipeline();
}
```

两种形式都先加入当前线程的批次，采用与普通发布相同的分区算法。同步形式在物理分段发送时等待该命令响应；
达到批次长度会自动发送分段，最终仍需调用 `pipeline()` 发送剩余命令、报告累计失败并关闭批次状态。
未登记成功动作时，异步形式不等待 Redis 结果，失败通过日志报告；需要确认写入结局时使用同步形式。
分段一旦发送就可能生效，最终收尾异常不会撤销已经写入的消息。

## 完整 YAML 与默认值

下面展示分区字段及相关普通 Stream 配置。示例开启消费并覆盖隔离组的批大小与阻塞时间；其它值以随后各表的
默认值为准。使用隔离组时必须声明其 `count`，未使用的普通 Stream group 无需配置：

```yaml
nasa:
  redis:
    properties:
      primary:
        stream:
          poll-timeout: 500
          batch-size: 100
          group:
            order-events:
              order-consumer:
                consumer-name: order-consumer
                consumers: 1
                event-executor-enable: false
                auto-acknowledge: false
                batch-size: 100
                poll-timeout: 500
          partition:
            enabled: true
            default-group: SINGLE-CONSUME
            count: 64
            rebalance-ms: 3000
            min-idle-ms: 30000
            holds-check-interval-ms: 5000
            drain-timeout-ms: 5000
            key-layout: AUTO
            local-consumer:
              max-in-flight-records: 8192
              max-in-flight-tasks: 4096
              max-pending-commit-attempts: 2048
              max-pending-commit-records: 8192
              max-in-flight-retries: 256
              max-blocked-keys-per-claim: 4096
              max-deferred-ids-per-key: 1024
              max-route-blocked-records: 8192
              max-pending-unordered-retries: 8192
              max-proxy-ledger-records: 8192
              max-proxy-fields-per-record: 64
              proxy-pending-min-idle-ms: 30000
              retry-initial-delay-ms: 1000
              retry-max-delay-ms: 30000
              ack-reconcile-initial-delay-ms: 200
              ack-reconcile-max-delay-ms: 5000
              poison-policy: BLOCK_CLAIM
            groups:
              settlement:
                count: 64
                topics:
                  - contract:settlement
                  - spot:settlement
                rebalance-ms: 3000
                min-idle-ms: 30000
                holds-check-interval-ms: 5000
                drain-timeout-ms: 5000
                batch-size: 200
                poll-timeout: 250
        distributed-lock:
          prefix: DISTRIBUTED-LOCK:
          lease-time: 30000
```

### 数据源级字段

| 配置键 | 默认值 | 约束与作用 |
|---|---:|---|
| `stream.partition.enabled` | `false` | 总开关；关闭时不建立分区消费者，纯发布路由仍可按 YAML 计算 |
| `stream.partition.default-group` | `SINGLE-CONSUME` | 非空；默认组和全部隔离组的持久命名空间前缀 |
| `stream.partition.count` | `64` | 大于 0；默认共享组分区数 |
| `stream.partition.rebalance-ms` | `3000` | 大于 0；成员续约与周期再平衡间隔 |
| `stream.partition.min-idle-ms` | `30000` | 不小于 0；`XAUTOCLAIM` 接管 PEL 的最小空闲时间 |
| `stream.partition.holds-check-interval-ms` | `5000` | 大于 0；持锁自检的最小间隔 |
| `stream.partition.drain-timeout-ms` | `5000` | 大于 0；单 Claim 持权排干预算，耗尽后失效来源并交回未决 PEL；最终停机仍等待本地资源结束 |
| `stream.partition.key-layout` | `AUTO` | standalone 保持旧键布局；Cluster 使用带共享 hash tag 的同 slot 布局 |
| `stream.batch-size` | `100` | 大于 0；默认 `XREADGROUP COUNT`，也作为 `XAUTOCLAIM` 单页数量 |
| `stream.poll-timeout` | `500` | 大于 0；默认 `XREADGROUP BLOCK`，也影响空闲时的停机响应 |
| `distributed-lock.prefix` | `DISTRIBUTED-LOCK:` | 非空；进入持久分区合同，不能在原命名空间内滚动改值；COLOCATED 布局下最终锁 key 必须与 Stream 同 slot |
| `distributed-lock.lease-time` | `30000` | 至少 3 ms；看门狗约每三分之一租期续约，突然退出后的锁接管受该值约束 |

`distributed-lock.prefix` 和 `distributed-lock.lease-time` 也可以通过全局
`nasa.redis.distributed-lock.prefix`、`nasa.redis.distributed-lock.lease-time` 提供；数据源级字段优先，
然后回退到全局值和内置默认值。

### 本地消费硬容量

| 配置键，相对于 `stream.partition.local-consumer` | 默认值 | 约束与作用 |
|---|---:|---|
| `max-in-flight-records` | `8192` | XREAD 前预留的 raw record 总量，覆盖 Claim 与 BOTH |
| `max-in-flight-tasks` | `4096` | 当前 RedisProxy 的 Partition Task 硬上限 |
| `max-pending-commit-attempts` | `2048` | 正在确认及 ACK UNKNOWN 的 attempt 上限 |
| `max-pending-commit-records` | `8192` | CommitAttempt 持有的 record id 上限 |
| `max-in-flight-retries` | `256` | 已取得执行权的精确或整批 PEL 重试上限 |
| `max-blocked-keys-per-claim` | `4096` | ordered key 阻断上限，达到后暂停来源 |
| `max-deferred-ids-per-key` | `1024` | 单 key deferred 坐标上限 |
| `max-route-blocked-records` | `8192` | 路由、恢复证据不明确或恢复容量暂满时可保留的整批坐标上限 |
| `max-pending-unordered-retries` | `8192` | null-key 失败坐标上限 |
| `max-proxy-ledger-records` | `8192` | BOTH 当前 consumer epoch 多 field record 账本上限 |
| `max-proxy-fields-per-record` | `64` | 单个普通 Stream record 的 event field 上限 |
| `proxy-pending-min-idle-ms` | `30000` | BOTH XAUTOCLAIM 阈值，应高于正常 listener 高分位耗时 |
| `retry-initial-delay-ms` / `retry-max-delay-ms` | `1000` / `30000` | 业务失败、未执行及整批恢复的重试退避 |
| `ack-reconcile-initial-delay-ms` / `ack-reconcile-max-delay-ms` | `200` / `5000` | ACK UNKNOWN 的 PEL 复验退避 |
| `poison-policy` | `BLOCK_CLAIM` | 唯一支持的策略；未知路由保留 PEL 并暂停来源 |

`max-in-flight-records`、Task/commit/blocked/deferred/retry/ledger 上限都必须覆盖任一有效 batch-size，
`max-route-blocked-records` 还必须不小于 `max-in-flight-records`。非法组合在 container 开放前拒绝。容量暂缺
只暂停新读取，不驱逐已经登记的 ACK UNKNOWN、gate、ledger 和 PEL 恢复状态。Task/确认配额一次取得，ordered
gate 按整批预留；账本或门禁容量拒绝发生在首个 Task 提交前，本批暂存的账本与门禁会撤销。

### 容量暂满后的自动恢复

`PARTITION` 与 `BOTH` 的 unordered 重试表、ordered gate、单 key deferred 或 BOTH ledger 暂满时，只阻断
对应来源的新读取，保留当前 holder/consumer epoch 和已有重试、确认的执行权。尚未接续的批次转入有界整批恢复，
保存原 Redis 顺序的 PEL 坐标并继续持有读取前取得的 raw record 配额，不另建无界等待列表。

如果 listener 已失败而精确重试表暂满，未登记的坐标仍由原批次负责，完成在途 Task 与确认交接后再保留整批。
退避期间归还本批 Task 与未转交的确认配额，使已有重试和 ACK 可以继续释放容量。整批恢复沿用
`retry-initial-delay-ms` / `retry-max-delay-ms`，每次重读 PEL 与正文并复验来源权威；已确认记录跳过，BOTH 已成功
field 沿用当前 epoch 证据。该批接续到执行、精确重试或确认责任后自动开放新读取，不依赖重启、节点迁移或额外唤醒。

容量保护期间 `route_blocked_batches`、`route_blocked_records` 和 raw 容量用量可见，readiness 暂不可用；受阻责任
收敛后清除对应保护状态，其它健康条件仍须满足。持续业务失败或 ACK UNKNOWN 会继续保留责任，不保证固定恢复时限。
单批需求超过总上限、无法安全保留恢复责任、主动停止或明确失权时关闭来源，未完成的 PEL 留给后续合法 owner。

### 隔离组字段

| 配置键 | 默认值 | 约束与作用 |
|---|---:|---|
| `groups.<name>.count` | 无 | 必填且大于 0；决定隔离组路由取模基数 |
| `groups.<name>.topics` | `[<name>]` | 空列表或不配置时，逻辑组名本身就是唯一 topic；非空时多个 topic 共享该组 |
| `groups.<name>.rebalance-ms` | 父级 `3000` | 覆盖本组再平衡周期 |
| `groups.<name>.min-idle-ms` | 父级 `30000` | 覆盖本组 PEL 接管阈值 |
| `groups.<name>.holds-check-interval-ms` | 父级 `5000` | 覆盖本组持锁自检间隔 |
| `groups.<name>.drain-timeout-ms` | 父级 `5000` | 覆盖本组各 Claim 的持权排干预算，不限制最终资源收口时间 |
| `groups.<name>.batch-size` | `stream.batch-size` | 覆盖本组拉取和接管单页数量 |
| `groups.<name>.poll-timeout` | `stream.poll-timeout` | 覆盖本组阻塞拉取时间 |

只配置业务真正需要覆盖的字段。例如隔离慢 topic 时通常只需要 `count`、`topics` 和较长的
`min-idle-ms`；没有必要把父级默认值全部重复一遍。

多数据源时，各 qualifier 在自己的 `nasa.redis.properties.<qualifier>.stream.partition` 下独立配置，
业务 listener 可用 `qualifiers()` 限定数据源。相同业务 key 在不同 Redis 数据源上没有互斥或顺序关系。

## 集群成员与再平衡

RedisPartition 不选举 leader。每个分区组的所有节点都定期执行同一段 Lua：

1. 用 Redis `TIME` 读取服务端时间；
2. 将当前运行会话写入 `<streamPrefix>:nodes`，过期点为当前时间加 `3 * rebalance-ms`；
3. 删除已过期成员；
4. 返回剩余成员数量。

节点 ID 由 `spring.application.name`、节点序号和每次运行生成的 UUID 组成。UUID 把同机多进程、同一 JVM
的多个应用上下文和进程重启区分为不同运行会话；旧 consumer 名下的 PEL 由新 owner 在达到
`min-idle-ms` 后接管。

每轮再平衡计算：

```text
fair = max(1, ceil(partitionCount / aliveNodes))
```

- 当前真锁数量大于 `fair` 时，节点优先排干并释放高编号分区；
- 当前真锁数量小于 `fair` 时，节点从低编号开始非阻塞认领；
- 分区锁已被其它节点持有时保留失败占位并按退避重试，释放通知会立即唤醒重试；
- 同一组周期任务与通知回调使用 single-flight 门禁，不会并发执行两轮再平衡；
- 公平算法限制的是每节点持有上限，不承诺按节点身份生成确定分区表，也不使用一致性哈希。

### 收敛时序

节点上线：

```text
写入成员 ZSET → 初始贪心认领 → 启动周期任务 → 发布 online 通知
                                      │
                                      ▼
                         其它节点排干超额分区并解锁
                                      │
                                      ▼
                         发布 release 通知，缺额节点立即重试
```

优雅下线：

```text
停止再平衡 → ZREM 当前会话 → 停止唤醒订阅 → 排干在途回调
                                                    │
                                                    ▼
                                        复验权威后 ACK / 释放锁
                                                    │
                                                    ▼
                                            发布 offline 通知
```

突然退出或网络隔离：

```text
成员心跳停止 ──通常不超过 3 * rebalance-ms──▶ 成员 ZSET 清理
锁看门狗停止 ─────────────不超过剩余 lease-time──▶ 分区锁过期
                                                     │
                                                     ▼
                                             新节点取得锁
                                                     │
                                                     ▼
                                  达到 min-idle-ms 的 PEL 被 XAUTOCLAIM
```

所以故障接管时间不是单独由 `rebalance-ms` 决定，而是同时受成员过期、锁剩余租期、PEL 空闲阈值、
Redis 命令超时和调度延迟影响。把 `rebalance-ms` 调得很小不能绕过尚未到期的分区锁。

## 持久分区合同与滚动变更

消费节点准备组、纯发布者首次解析路由时，都会在 `<streamPrefix>:partition-contract` 原子建立或复验组合同：

```text
schema=5;count=<count>;layout=<plain|colocated>;lock-prefix-b64=<encoded-prefix>;hash=java-hash-sign-mask-mod
```

合同固定四个会改变消息归属或锁互斥域的事实：分区数、解析后的物理键布局、锁前缀和哈希算法。任何节点不一致
都会在建组或发布前拒绝，防止同一个命名空间同时存在两套路由或两套锁。合同首次建立时还证明两种布局都没有
历史分区 Stream，因而当前 `count` 不是从无法判定原分区数的稀疏键集合中推断出来；已有一致的当前 schema 合同可
直接复验，不在每次启动时重复全拓扑扫描。UTF-8 字符串键编码是进入该合同前的固定协议前提，不作为可滚动调整的
合同字段。

消费方声明或发布方首次解析一个 topic 时，还会在 `<default-group>:partition-topic-contracts` 原子登记完整路由。
后续同步发布、pipeline 发布以及其它进程必须复验相同的 Stream 前缀、分区数、物理布局和哈希规则，复验成功后
才缓存本地路由。因此纯发布者可以先于消费者建立合同，但不能向消费者合同之外的分区写入，也不能把同一 topic
改投另一个隔离组；只部署消费者的进程也会在开放 listener 前约束该 topic。一个 listener 同时声明多个 topic 时，
Lua 会先复验全部 field，只有全部兼容才补写缺失 field；任一冲突都不会留下未激活 topic 的半完成路由。隔离组的
已知 topic 冲突也会在创建 XGROUP、启动 container 和发布本地组之前返回。提交远端 topic 合同前，本地还会先完成
一次只读原子预检，当前已存在的冲突不会启动 Runner。随后完成 TimingWheel、PartitionRunner、路由唯一性和
taskType 预留的完整就绪，再用 Lua 原子复验并登记合同；任何本地就绪失败都不会在 Redis 留下 topic field，远端
成功后的本地阶段只发布已经冻结的内存状态。预检与最终登记之间若发生跨节点竞争或 Redis 调用失败，本次注册只撤销
本地计划与 taskType 预留；代理独占 Runner 可能已经承载其它计划，只有所属 RedisPartition 整体关闭时才按排干顺序停止。

`rebalance-ms`、`min-idle-ms`、`holds-check-interval-ms`、`drain-timeout-ms`、`batch-size`、
`poll-timeout` 属于运行调优参数，不写入持久合同，便于滚动调整。但滚动窗口内节点使用不同参数时，交付延迟和
排干上限会暂时不同；应逐批发布并持续观察分区持有量、PEL 和慢回调。

不能在既有命名空间原地修改 `count`、`default-group`、隔离组逻辑名、topic 归属、键布局或锁前缀。需要改变时应使用新命名空间，
停止旧生产者写入，排空旧 Stream 与 PEL，再统一切换生产者和消费者。直接删除合同 key 不能搬迁历史消息，
也不能重新建立同键顺序，因此不属于安全变更方案。

已有 `schema=1` 组合同没有记录物理键布局，当前实现会明确拒绝把它解释为 `plain` 或 `colocated`。升级时必须先
停止旧生产者和消费者，确认旧 Stream、PEL 与锁域已经排空或完成迁移，再按目标布局替换组合同并统一开放节点；
不能在旧节点仍可能写入时只删除合同 key。

已有 `schema=2` 合同虽然记录了布局，但其建立过程只排除了当时分区数范围内的相反布局，不能证明更高历史分区为空。
当前实现同样拒绝直接继承该结论。升级前应停止旧写入，检查每个 master 上两种布局的完整 Stream 命名空间，处理全部
历史 Stream 与 PEL 后再建立当前合同；仅确认当前 `count` 范围不足以完成迁移。

已有 `schema=3` 合同虽然证明相反布局完整命名空间为空，但没有证明当前布局不存在索引不小于 `count` 的历史 Stream。
当前实现不会把该合同解释为完整的分区数迁移证据。升级前应停止旧写入，检查每个 master 上当前布局的完整 Stream
命名空间，处理新 `count` 不可达的 Stream 与 PEL 后再建立当前合同。

已有 `schema=4` 合同虽然排除了当前布局中索引不小于 `count` 的 Stream，但低索引集合不能证明原分区数，也不能证明
当前 `count` 没有扩大。当前实现同样拒绝直接继承该合同。升级前应停止旧写入，处理当前布局的全部历史 Stream 与 PEL，
再使用全新命名空间建立当前合同；不能只删除合同并沿用现存 Stream。

曾使用其它 key serializer 创建的物理键不属于当前 UTF-8 分区协议可自动证明的命名空间。切换前必须用原序列化配置
停止并排空生产与消费，按物理键确认 Stream、PEL、合同和锁域已完成迁移，再统一启用 UTF-8 字符串键；不能只更换
模板后沿用同一个逻辑 `default-group`。

## PEL 接管与确认

### 恢复调度与执行权

组创建时使用 `0-0` 作为起始位置，所以先于消费者发布的历史消息仍可被读取。节点取得一个分区锁后：

1. 复验当前 holder 仍拥有分区锁；
2. 从 `0-0` 开始分页执行 `XAUTOCLAIM`；
3. `XAUTOCLAIM` 只接管 idle 不小于 `min-idle-ms` 的 PEL；
4. 在 `XAUTOCLAIM` 返回后再次复验锁权威；
5. 接管完成后才进入 `XREADGROUP ... >` 的新消息拉取。

`min-idle-ms` 只决定接管资格，当前 holder 的在途 Task 或精确重试也可能达到该阈值。
两类消费来源都按 Stream/group/consumer/record ID 和来源代次协调本地执行权；已有执行或确认责任的记录
交给原驱动力，取得执行权的历史记录再次查询 PEL，已经缺席的记录不再调用 listener。
PEL 查询及实际 holder 复验不确定时保留完整页和恢复容量，证据明确前不发布本页 Task，也不开放后继读取。
恢复容量暂满时保留整批并暂停新读取，容量归还后自动接续。停止、失权或无法安全保留恢复责任时关闭来源，
未完成的 PEL 留给后续合法 owner。

接管命令超时或连接失败时，Redis 可能已经迁移 PEL 并重置 idle。来源保持本轮游标、新读取屏障和恢复责任，
退避后分页复验当前 consumer 的 PEL，补回已迁移但尚未交接的记录。这些记录的恢复不重新等待 `min-idle-ms`；
补扫先等待该来源已登记的 Task、精确重试和确认收口，再按 record ID 顺序复验 PEL 与正文，避免重复调用成功
listener。holder 复验不确定时，已知页面及其容量继续保留，等待明确证据后才投递。

BOTH 的瞬时接管异常也在当前 consumer epoch 内恢复，不因一次超时永久关闭来源。恢复证据仍不确定时
readiness 保持关闭；全部未交接页面与本轮扫描收敛后，才撤销对应的临时保护原因。
游标未完成或物理分区单轮扫描达到上限时，当前 Claim 保留后续接管标志。锁已确定丢失、主动释放或全局停机时
停止重试与新投递，尚未处理的 PEL 留给后续合法来源。

### 逐记录处理边界

恢复页取得正文并不等于获得业务执行权。执行句柄覆盖 Task、Future 及后续重试或确认责任的交接，
同页中已有驱动力的记录可以继续由原责任推进，其它证据明确的记录按顺序规则接续。

| 观察到的状态 | 当前恢复调用的行为 |
| --- | --- |
| 相同来源代次的 record 正在执行 | 不发布第二个 Task，由原执行者交接重试或确认责任 |
| record 已由未决 CommitAttempt 持有 | 不重新调用成功 listener，由原确认链复验 PEL 或重试 XACK |
| 取得执行权后 PEL 已缺席或迁移 | 收敛当前来源的对应责任，不执行旧正文、不代替新 owner 确认 |
| PEL 仍属于当前来源，且恢复权威有效 | 完整解析并取得容量后发布 Task |
| 任一待处理成员的 PEL 或 holder 证据不明确 | 保留完整页、raw 容量和恢复驱动力，证据明确前不发布本页 Task |
| 恢复状态容量暂满且能够保留原批次 | 暂停新读取，保留来源权威、整批坐标与 raw 配额；已有重试和确认继续推进，接续后自动恢复读取 |
| 单批需求超过总上限、无法保留恢复责任、停止或明确失权 | 关闭对应来源，未完成的 PEL 留给后续合法 owner |

执行句柄受已准入 raw batch 与重试容量约束；物理分区不会占用 BOTH 多 field 账本额度。
这些本地约束不覆盖进程崩溃后已丢失的成功证据，不能据此推导外部副作用 exactly-once。

### 原子确认与数据保留

确认规则：

- RedisPartition 在 Redis I/O 前登记 CommitAttempt，再用一段 Lua 原子检查实际 lock holder、逐 id XACK，并仅对
  本次 XACK 返回 1 的 id 按 `autoDelete` 决定 XDEL；
- BOTH 使用单 Stream Lua 原子读取每个 id 的 PEL consumer，只有仍属于当前唯一 consumer epoch 时才 XACK；
- Lua 明确失权或 consumer 已迁移时，旧来源只交回 PEL，不能确认新 owner 的 pending；
- Redis 超时或断线使结果不确定时，gate 保持 ACK PENDING/UNKNOWN，按时间轮退避查询 XPENDING；PEL 缺席视为
  已收敛，仍属当前来源则只重试 ACK，不重新调用成功 listener；
- listener 失败、Task 未执行和 null-key 失败都登记精确坐标并主动退避重试；ordered key 在最早未确认坐标收敛前
  阻断后继，同一 Claim 的其它 key 仍可推进；
- topic/event/data、反序列化、计划或 `partitionKey` 无法安全解析时采用 `BLOCK_CLAIM`：整批 id 留在 PEL、暂停
  当前来源并按原 Redis 顺序重建，不能降级为 null-key 或直接丢弃；
- PEL 有 id 但正文已被外部删除或裁剪时记录 `pel_tombstone` 数据丢失事实，并在当前 fencing 权威下清理陈旧 PEL，
  不把它记为业务成功。

重试状态只保存 Stream/group/consumer/id/field/plan/hash/generation 和失败摘要，不保存业务 data。框架不会因次数
耗尽而 ACK；业务仍需幂等。若将来接入 DLQ，必须先证明 DLQ 写入成功，再在同一确认权威下处理原消息。

## Fencing 与业务幂等

Claim 取得分区锁时冻结 holder。组件在 PEL 接管前和接管结果提交业务前复验权威。精确重试、routeBlocked
整批重试与当前 consumer PEL 补扫均在所需正文读取完成后查询实际 holder，并在查询前后复验本地代次及停止状态。
证据 UNKNOWN 时不启动业务 Task：精确与整批重试保留原坐标、门禁及容量责任，由时间轮有界退避后重新读取与复验；
当前 consumer 补扫保留整页正文、容量和扫描屏障，在本轮恢复内退避等待。明确失权时立即撤销本地执行权威，
PEL 留给后续合法 owner。迟到的有效 holder 响应不能覆盖停止或本地代次变化。
最终确认不使用
`holdsStatus + XACK` 两条命令，而是把实际锁 Redis key、Stream key、冻结 holder、group、删除策略和精确 id
交给 holder-fenced Lua 原子执行。长时间 GC、续租失败、租期过期或另一节点接管后，旧 Claim 不再确认消息，
避免旧 owner 把新 owner 应处理的 PEL 从 Redis 中移除。

这个门禁只能约束 RedisPartition 自己的读取、ACK 和锁生命周期，不能撤销已经开始的外部操作。例如旧 owner
已经向数据库提交转账，随后在 ACK 前失权，新 owner 会再次收到同一消息。正确做法是让目标系统以稳定业务事件键
执行唯一约束、幂等写或可校验 fencing token，而不是依赖“同一时刻通常只有一个线程”。

`holds-check-interval-ms` 越小，失权发现越快，但空闲轮询中的 Redis EVAL 越多。它应明显小于锁租期，同时保留
足够网络和暂停余量。锁租期也必须覆盖正常业务回调的最长暂停窗口，并由看门狗持续续约。

## 停机与排干

`RedisProxy` 销毁时先关闭关联的 RedisPartition，再关闭普通订阅、分布式锁入口、业务执行器和连接工厂。
这个顺序保证消费者停止领取后仍能等待在途回调、执行最终 ACK 复验并释放分区锁，不会向已经关闭的执行域继续
提交消息。

关闭过程分为代理执行域与物理分区组两个层次：

1. 封闭分区发布入口，取消尚未发送的 Pipeline 命令，并等待已发送的 Redis 写入到达终态；
2. 关闭新读取、dispatcher、record 容量入口和新 retry，取消尚未运行的 Partition Submission；运行中的 listener 不做线程级强制中断；
3. 逐个关闭物理组：先取消再平衡、从成员 ZSET 移除会话并撤销唤醒订阅，再停止 Claim 和容器，在预算内持锁等待在途业务与确认。
   Claim 超时先失效来源 generation，再把未决确认交回 PEL 并释放 owner，迟到 Future 禁止 ACK。组的有界等待结束后发布下线通知，
   仍未释放的锁继续阻止其它节点认领，通知本身不授予执行权；
4. 来源全部封闭后继续等待 PollRecordPermit、batch、Task/Future、CommitAttempt、ACK I/O、RECOVERY、rebalance 和已取得执行权的 retry 收口；
5. 全部本地责任归零且专用等待执行器实际终止后，停止当前 RedisProxy 独占的 PartitionRunner，完整停止后才关闭其 TimingWheel。

`drain-timeout-ms` 是单 Claim 的持权排干预算。全组等待还包含 Claim 收尾的调度余量；多个分区组依次关闭，
因此进程停机宽限应覆盖所有活动组及业务实际结束时间。业务回调在预算内结束时，owner 持锁到最终 ACK；超时后释放远端 owner，
但仍保留迟到 waiter 对 Task 数据和本地资源的唯一所有权。迟到回调仍可能完成外部副作用，所以幂等要求不变。

来源已封闭或单轮等待预算耗尽都不代表运行时已经终止。代理持续保留未决分区，直到所有本地责任归零、
专用等待执行器实际终止，再完成独占 Runner 与 TimingWheel 停机。
同步 `stop()` 的返回和 `stop(Runnable)` 的完成通知都以该共同终态为边界；`destroy()` 发起的一次性资源清理
可以交回尚未排干的执行域，由后台及后续生命周期入口继续收口。物理 Claim 与 BOTH 来源均在提交恢复任务前登记
RECOVERY 责任，提交拒绝或实际恢复任务退出时归还；Redis 响应未返回时不能提前消除这项责任。

代理命令队列在最终排干前关闭实际入队准入。此前取得队列的调用也受同一门禁约束：已接纳槽位由最终消费者
完成，迟到槽位异常完成其 future 并归还池化参数，不再发送 Redis 命令。周期消费者已经取走最后一批时，停机仍等待
其发送与收口退出，不能以队列为空代替命令完成。

业务一般不需要调用 `RedisPartition.shutdown()`。Spring 上下文关闭和 `RedisProxy.destroy()` 已纳入同一生命周期；
手工创建运行时或在容器之外使用时，调用方才需要显式关闭。

Submission 的准入复验、提交和句柄发布与停机取消共用提交屏障；提交结果通知在释放屏障后执行。
同步指标扩展可以请求当前代理的 `stop(Runnable)`：先关闭代理及 dispatcher 准入，再由唯一后台清理者等待指标活动退出并完成停机。
完成通知仍须等待已准入批次、Task、确认责任及指标活动实际收口。指标活动内的同步 `stop()` 或 `destroy()` 无法等待自身退栈，
会在交接停机责任后抛出异常，调用方应使用 `stop(Runnable)` 订阅真实完成状态。
取消计数同样在锁内确认取消成功后于屏障外通知，不因通知延后而重开准入或提前释放业务责任。

## 观测与运维

### 本地持有快照

```java
Map<String, List<Integer>> held = RedisPartition.load(redisProxy).claimedPartitions();
```

返回值按逻辑分区组列出当前节点持有的分区索引，默认组键名为 `<default>`。它是调用时刻的本地快照，不能单独
证明整个集群没有缺口或重复；集群视图还需要结合成员、锁和 Redis Stream 状态。

组件运行时维护不含业务 key、record id、traceId 或异常文本的低基数快照。classpath 存在 Micrometer 且 Spring
容器中存在 `MeterRegistry` 时，组件会自动登记这些 meter；Prometheus registry 可直接输出，不要求业务编写聚合脚本。
装配时保留可供应用查询的顶层 `CompositeMeterRegistry`，并排除已经由它传播到的候选 child。多个顶层 Composite 若共享
同一个实际后端，会同时破坏门面登记和单次记录语义，因此装配会明确拒绝该歧义拓扑。Composite 成员关系必须在指标桥接器
创建前确定；创建后的 `add/remove` 会使当前桥接器关闭并拒绝后续记录，需要随新的 `RedisProxy` 指标桥接器或应用上下文
重新装配。登记返回既有同身份 meter 时，桥接器跳过该冲突项，不写入也不撤销应用对象；只有 Micrometer 同步新增事件能够
证明由本次调用创建的顶层和 leaf 对象才进入关闭坐标。Composite 在门面完成顶层全部 `MeterFilter` 配置后、发布前，使用
该门面的官方 child 构造入口预占初始 leaf。门面的真实 Id 作为 leaf 的 pre-filter 坐标；SLO、百分位、直方图开关、统计窗口、
精度、预期值范围和 Timer 的 `PauseDetector` 随完整定义传播，leaf 的配置按 Micrometer 原有顺序继续合并。leaf 自身的标签
重命名、值替换、标签忽略和名称映射仍由 Micrometer 按原顺序执行。
顶层、全部 leaf 的配置门禁和 meter 创建门禁覆盖预占、门面传播与失败回滚，任一最终传播坐标已被应用占用时，都在
Composite 门面发布前撤销本次创建的其它坐标。事务只锁定当前顶层及其实际 leaf：顶层先于 leaf，共享 leaf 按稳定对象身份
顺序取锁。互不共享 registry 的桥接器可以独立登记，应用同步回调关闭另一独立桥接器或代理时，不会等待跨实例的全局登记锁。
预占直接采用真实传播定义，不会产生额外的未映射探测序列。多级 Composite 按
Micrometer 的 `nonCompositeDescendants` 语义直接触达实际 leaf，中间 Composite 的过滤器不参与顶层 meter 的传播；顶层和
实际 leaf 的过滤器必须在桥接器创建前完成配置，且同一 Id 的映射结果在运行期保持确定、稳定。当前运行时若不再提供 mapped
Id、完整 child 构造入口、可确认的官方传播回调或 register/remove 共用的创建门禁，指标会明确拒绝登记，不会降级为无法证明
所有权的传播删除。关闭当前门面时，桥接器在该对象的内置撤销回调位置，以创建证据替代按 Id 广播：旧门面先从顶层移除，
再在每个初始 leaf 的创建门禁内复验对象身份并删除仍由桥接器持有的 meter，随后才执行应用的顶层 `onMeterRemoved`。
应用可在该回调中按同 Id 登记新门面，新门面不会复用本次待撤销的旧 leaf；应用此前已替换的 leaf 仍保持原样。
实际后端为 Timer 或 DistributionSummary 创建的派生 Gauge 也按同步新增事件及真实父对象关联记录所有权。
父对象撤销时先解除原生按 Id 级联关系，再于父对象移除后、应用父撤销回调前逐个复验并撤销自有派生对象；
应用已替换的派生 Gauge、预先存在的采集项和其它父指标的派生项保持原样。登记异常时，即使父对象尚未发布，
也按同一创建证据收口已发布的派生坐标。后端必须提供可访问的派生关联表；缺少该能力时明确拒绝登记。
应用回调收到原对象，其它应用门面的传播和共享拓扑保持原样。即使 child 已先摘除，也按
相同规则收口。运行期新加入的 child 不在装配期创建证据范围内，拓扑拒绝会撤销原有门面和已证明自有的初始 leaf 对象，
保留新 child 中归属无法证明的 meter；这些对象可能包括应用预先登记项及改图自动传播项，需由修改拓扑的应用处置。
运行期替换桥接器按调用顺序先撤销旧坐标
再绑定新坐标，避免并发安装把旧桥接器对象误当成应用冲突。
启动日志同时打印锁租期、两条接管阈值、排干预算和已经观察到的 listener P99：

- batch、Task、精确 retry、ACK UNKNOWN、routeBlocked、pel_tombstone 和 Proxy ledger 数、最老年龄及容量拒绝；
- batch/Task/Commit/ACK I/O/retry/rebalance 的当前在途所有权；
- 固定桶 listener 调用观察数与近似 P99；
- raw record、Task、CommitAttempt、commit record 与 retry permit 使用量和等待数；
- ordered gate 的 BusinessPhase、CommitPhase、最老阻断年龄、连续失败、deferred、ledger waiting 与 pending-confirm 坐标数；
- 分区认领、释放、锁失效、接管、ACK fencing 结论和停机超时日志。

每个逻辑组直接登记以下集群合同 gauge：

- `node_alive`
- `claimed_partitions`
- `fair_target_partitions`
- `claim_generation`
- `pel_pending`
- `pel_oldest_idle_ms`
- `owner_convergence_ms`
- `pel_takeover_ms`
- `lock_self_checks`
- `wake_signals`
- `rebalance_fallbacks`
- `drain_timeouts`

标签限定为 `qualifier`、`group` 和 `instanceId`。共享 dispatcher 的其它快照登记为
`stream_partition_runtime{result="..."}`；`result` 只取组件预定义状态名。PEL 数量与最大 idle 由组件控制执行域完整分页
采样，采样失败发布 `-1`，不会把局部 Claim 值冒充全组结论。meter scrape 只读缓存，不执行 Redis I/O。

计划与执行链另外直接登记以下低基数 meter：

- `stream_partition_tasks`：标签为 `qualifier`、`instanceId`、稳定 `subscription`、`mode`、`ordered` 和固定 `outcome`；
- `stream_partition_key_duration`：按计划、`null/number/string/object` 粗类型和成功/失败记录 partitionKey 次数与耗时；
- `stream_partition_batch` 与 `stream_partition_ordered_bucket`：记录 Redis batch、拆分 Task 和 ordered bucket 大小；
- `stream_partition_task_latency`：以固定 `queue/listener/future` 阶段记录排队、业务执行和 Future 等待耗时；
- `stream_partition_submissions`、`stream_partition_recovery` 与 `stream_partition_ack`：分别记录提交/取消、exact/route/PEL 恢复及 fencing/XPENDING 固定结果；
- `stream_partition_runner`：按稳定 subscription 与 Runner 名称提供 started、healthy、failed partition 和分区数。

`subscription` 只由配置中的 topic、event 与 group 规范化生成，内部 planId 不进入跨节点标签。上述 meter 与共享快照
共同覆盖容量、等待、未执行终态、门禁阶段和年龄、重试连续失败、ledger 年龄、ACK 分类及 Runner 健康。

Micrometer 是可选依赖；没有 `MeterRegistry` 时核心消费能力不引入 Web 服务或额外采集线程。应用使用 Prometheus 时，
由现有 Micrometer/Actuator HTTP 暴露配置提供抓取端点。任何后端都不得把原始业务 key、消息内容或异常 message 放进 tag。

常用 Redis 诊断命令：

```text
ZRANGE <streamPrefix>:nodes 0 -1 WITHSCORES
GET <streamPrefix>:partition-contract
HGETALL <default-group>:partition-topic-contracts
XINFO STREAM <streamPrefix>:<partition>
XINFO GROUPS <streamPrefix>:<partition>
XPENDING <streamPrefix>:<partition> <streamPrefix>
```

上述 Stream 示例适用于 plain 布局；COLOCATED 布局的实际 Stream key 为 `{<streamPrefix>}:<partition>`，
成员键与合同键不额外添加这层 hash tag，consumer group 仍为 `<streamPrefix>`。
隔离组的 consumer group 名与 `streamPrefix` 相同。Redis Cluster 环境应把命令发往目标 key 所在节点，
并避免对整个 keyspace 执行阻塞式全量扫描。

### 常见现象

| 现象 | 优先检查 |
|---|---|
| 新节点上线后长期没有分区 | `partition.enabled`、listener qualifier、成员 ZSET、分区锁 TTL、周期再平衡日志 |
| 扩容后短时间分布不均 | 原 owner 是否仍在排干、释放通知是否可达、`rebalance-ms` 与 `drain-timeout-ms` |
| 进程退出后仍迟迟未接管 | 原锁剩余租期、成员过期时间、PEL idle 是否达到 `min-idle-ms` |
| 启动或发布报告 partition contract mismatch | `count`、解析后键布局、锁前缀、命名空间以及旧合同迁移状态 |
| 发布报告 partition topic route mismatch | topic 是否在所有生产者和消费者中映射到同一组、分区数与键布局 |
| PEL 持续增长 | listener 连续失败、routeBlocked、ACK UNKNOWN、接管阈值、业务耗时与 Runner 健康 |
| 同一业务对象观察到并行 | 生产者是否使用同类型稳定 key、是否绕过分区入口、分区数或 topic 到组映射是否变化 |
| 来源进入 routeBlocked | topic/event/data 合同、listener 计划、`partitionKey`、序列化配置、PEL/holder 证据及 retry/gate/ledger 容量 |
| 停机等待超过预期 | 活动分区组数量、在途回调时长、`drain-timeout-ms`、Redis 命令超时 |

## 参数调优

### 分区数

分区数决定最大并行度、Stream 和锁数量，也进入持久合同。建议按峰值并发、单条处理耗时和未来节点规模预留，
不要按当前实例数一比一设置。分区过少会限制并行度；分区过多会增加 Stream、consumer group、Claim 和运维
观测成本。需要完全隔离慢 topic 时优先新增隔离组，不要仅靠增大共享组批次掩盖阻塞。

### 批大小与阻塞时间

- 增大 `batch-size` 可以提高 Redis 网络吞吐，也会增加单批分桶、Task/确认容量需求、内存峰值和 routeBlocked 范围；
- 减小 `poll-timeout` 可以提高空闲时的停机响应，但会增加空轮询；
- 隔离组覆盖只影响该组，适合为高吞吐与低延迟 topic 分别设置；
- `batch-size` 同时影响 `XAUTOCLAIM` 单页大小，PEL 很大时需要平衡单轮接管时间。

### 接管与锁租期

默认 `min-idle-ms` 与锁租期都是 30 秒。两者应联合评估：

- `min-idle-ms` 过小，可能把仍由慢 consumer 合法处理的消息提前转移；
- 锁租期过短，长 GC、网络暂停或回调峰值可能造成失权；
- 锁租期过长，进程突然退出后的最坏接管时间增加；
- `holds-check-interval-ms` 应明显小于锁租期；
- `drain-timeout-ms` 应覆盖绝大多数正常回调，但不能代替业务自身的超时和取消合同。

生产调整前应基于真实 listener 延迟分布、Redis RTT、JVM 最大暂停、容器终止预算和允许的恢复时间确定数值，
不能只复制默认配置。

## 使用检查清单

- 所有生产者与消费者对同一 topic 使用 UTF-8 `StringRedisSerializer` 键编码，以及相同 Redis 数据源、命名空间、
  隔离组、分区数、键布局和锁前缀；
- 需要顺序的消息使用稳定且类型语义一致的业务分区键；明确不保序时才返回 null；
- PARTITION/BOTH 只使用 Single listener，并从消息体稳定字段实现 `partitionKey`；
- BOTH 使用独立非空 group，保持单本地 consumer，不与 legacy PROXY 混用同一 stream/group；
- listener 以稳定业务事件键保证外部副作用幂等；
- 隔离组只重复填写确需覆盖的参数，`count` 明确配置；
- 评估 `min-idle-ms`、锁租期、持有检查间隔和排干上限的联合时序；
- 为 PEL、lag、最老 idle、成员数、当前持有数和回调失败建立告警；
- 发布前确认没有在既有命名空间内改变分区合同；
- hard capacity 覆盖任一有效 batch-size，Cluster 迁移前确认旧布局 Stream/PEL 已处理；
- 停机预算覆盖全部活动分区组，PartitionRunner、TimingWheel 和 Redis 连接晚于 RedisPartition 关闭。
