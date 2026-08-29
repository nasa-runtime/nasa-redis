# RedisPartition 架构与运行指南

RedisPartition 是 `nasa-redis` 内置的 Redis Stream 分区消费运行时。它按稳定业务键把消息路由到固定分区，
每个分区由 Redis 分布式锁授予单一消费 owner；应用实例扩缩容、退出或失去锁权威后，其它实例重新分配分区，
并通过 `XAUTOCLAIM` 接管已经进入 PEL 的未确认消息。

它适合订单、账户、合约、设备等“同一业务键必须串行，不同键允许并行”的异步处理。业务只需要提供一份
YAML 配置和 `RedisEventBatchListener` 或 `RedisEventSingleListener` Bean；分区 Stream、consumer group、
成员心跳、分区锁、再平衡和停机排干均由组件管理。

## 能力与边界

RedisPartition 提供：

- String 或 long 业务键到固定分区的稳定路由；
- 集群级每分区单一有效 owner 与持权消费路径；
- Redis standalone 和 Redis Cluster；
- Redis 服务端时间成员心跳，无应用级主节点；
- 节点上线、主动释放和优雅下线时的通知驱动再平衡，以及通知不可达时的周期兜底；
- 持锁自检、ACK 前权威复验和失权后的保守退出；
- `XAUTOCLAIM` 接管其它 consumer 的 PEL；
- 默认共享组和按 topic 隔离的独立分区组；
- 同步发布、Pipeline 同步发布与 Pipeline 异步发布；
- 多 Redis 数据源隔离，以及当前节点分区快照。

RedisPartition 不提供：

- 外部数据库、HTTP、钱包或消息系统的 exactly-once 副作用；
- 跨不同分区的顺序；
- 不同 topic 之间的全局顺序；
- 动态修改既有命名空间的分区数；
- 同一 owner 持续运行期间对业务失败消息的无限自动重投；
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
       按 (topic, event) 路由 listener
                    │
                    ▼
       业务成功 + 锁权威复验 → XACK
```

一个 `RedisProxy` 对应一个 `RedisPartition`。每个分区组拥有独立的 Stream 命名空间、consumer group、
成员 ZSET、唤醒频道、再平衡任务和拉取容器；这些容器复用对应 `RedisProxy` 已装配的连接工厂、序列化方式和
业务执行基础设施。组之间可以配置不同的分区数、批大小、阻塞时间和接管阈值。

### 组件职责

| 组件 | 职责 |
|---|---|
| `RedisProxy` | 绑定数据源配置，发现 PARTITION/BOTH listener，先建组、再注册 listener、最后启动消费 |
| `RedisPartition` | 维护 topic 到分区组的路由，提供发布入口，统一管理分区组生命周期 |
| `PartitionGroup` | 复验持久分区合同，创建 Stream 与 consumer group，维护成员心跳并执行再平衡 |
| `Claim` | 代表本节点对一个分区的认领，持有分区锁，执行 PEL 接管、正常拉取、fencing 与排干 |
| `LettuceDistributedLock` | 用 Lua 授予可重入锁并由看门狗续租，提供三态持有权复验 |
| `BatchStreamMessageListenerContainer` | 驱动分区 Claim 的拉取生命周期和业务提交 |

Redis 中的锁决定分区唯一 owner，成员 ZSET 只用于计算每个节点应持有的数量。即使成员视图短暂不一致，
两个节点也不能同时合法取得同一分区锁；因此公平分配是收敛目标，锁权威才是互斥门禁。

## Redis 命名空间

`default-group` 是整个 Redis 数据源下 RedisPartition 的命名空间前缀，也是默认共享组的 consumer group 名。
假设 `default-group: SINGLE-CONSUME`、默认分区数为 64：

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

隔离组 YAML 的 key 是逻辑短名，不能再带 `default-group` 前缀。`default-group`、隔离组逻辑名和 topic
到组的映射都属于持久键布局，运行期间不应随意改变。

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
@Component
public class SettlementListener implements RedisEventBatchListener<Order> {

    @Override
    public String[] topics() {
        return new String[]{"contract:settlement"};
    }

    @Override
    public String event() {
        return "open-position";
    }

    @Override
    public ConsumeMode mode() {
        return ConsumeMode.PARTITION;
    }

    @Override
    public TypeReference<Order> paramType() {
        return new TypeReference<>() {};
    }

    @Override
    public void onEvent(List<Order> orders) {
        // 以稳定业务事件键保证外部副作用幂等
    }
}
```

`RedisEventSingleListener<T>` 逐条回调：某一条失败时仅该条留在 PEL，其它成功条目可以确认。
`RedisEventBatchListener<T>` 整桶批量回调：回调失败时该桶全部留在 PEL。`mode()` 的含义如下：

| 值 | 行为 |
|---|---|
| `PROXY` | 只走普通 Stream 消费路径，也是默认值 |
| `PARTITION` | 只走 RedisPartition |
| `BOTH` | 同一 listener 同时登记两条消费路径，业务必须分别提供两条消息来源并理解重复处理边界 |

PARTITION 路径忽略 listener 的 `group()` 和 `autoDelete()`；consumer group 由分区组命名空间决定，成功消息
使用 `XACK` 确认，不因 listener 的 `autoDelete()` 自动执行 `XDEL`。

同一数据源、同一 `(topic, event)` 应只注册一个 listener。重复注册会以后登记者覆盖先登记者并记录警告，
不应把覆盖顺序当作业务路由机制。

### 发布

优先通过业务已经持有的 `RedisProxy` 发布，数据源选择最明确：

```java
redisProxy.partition("contract:settlement", "open-position", order.userId(), order);
redisProxy.partition("contract:settlement", "open-position", order.orderNo(), order);
```

也可以使用 RedisPartition 门面：

```java
RedisPartition.load(redisProxy)
        .publish("contract:settlement", "open-position", order.userId(), order);

RedisPartition.load("archive")
        .publish("contract:settlement", "open-position", order.userId(), order);
```

省略 event 的重载会令 `event = topic`。发布端不要求本进程启用分区消费者：它会从 YAML 解析 topic 所属组、
分区数和 Stream 前缀并缓存路由，因此生产者与消费者可以分开部署。所有生产者与消费者仍必须使用相同的
分区合同。

### Pipeline 发布

```java
LettucePipeline.Actuator actuator = LettucePipeline.open(redisProxy, null);
try {
    actuator.partition("contract:settlement", "open-position", order.userId(), order);
    actuator.partitionAsync("contract:settlement", "open-position", audit.userId(), audit);
} finally {
    actuator.pipeline();
}
```

同步形式等待该命令结果，异步形式只把命令加入当前批次。两者使用与普通发布完全相同的分区算法。

## 完整 YAML 与默认值

下面展示所有可配置字段。除 `enabled` 和实际声明的隔离组 `count` 外，均可省略并采用表中默认值：

```yaml
nasa:
  redis:
    properties:
      primary:
        stream:
          poll-timeout: 500
          batch-size: 100
          partition:
            enabled: true
            default-group: SINGLE-CONSUME
            count: 64
            rebalance-ms: 3000
            min-idle-ms: 30000
            holds-check-interval-ms: 5000
            drain-timeout-ms: 5000
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
| `stream.partition.drain-timeout-ms` | `5000` | 大于 0；主动释放或停机时等待在途回调的上限 |
| `stream.batch-size` | `100` | 大于 0；默认 `XREADGROUP COUNT`，也作为 `XAUTOCLAIM` 单页数量 |
| `stream.poll-timeout` | `500` | 大于 0；默认 `XREADGROUP BLOCK`，也影响空闲时的停机响应 |
| `distributed-lock.prefix` | `DISTRIBUTED-LOCK:` | 非空；进入持久分区合同，不能在原命名空间内滚动改值 |
| `distributed-lock.lease-time` | `30000` | 至少 3 ms；看门狗约每三分之一租期续约，突然退出后的锁接管受该值约束 |

`distributed-lock.prefix` 和 `distributed-lock.lease-time` 也可以通过全局
`nasa.redis.distributed-lock.prefix`、`nasa.redis.distributed-lock.lease-time` 提供；数据源级字段优先，
然后回退到全局值和内置默认值。

### 隔离组字段

| 配置键 | 默认值 | 约束与作用 |
|---|---:|---|
| `groups.<name>.count` | 无 | 必填且大于 0；决定隔离组路由取模基数 |
| `groups.<name>.topics` | `[<name>]` | 空列表或不配置时，逻辑组名本身就是唯一 topic；非空时多个 topic 共享该组 |
| `groups.<name>.rebalance-ms` | 父级 `3000` | 覆盖本组再平衡周期 |
| `groups.<name>.min-idle-ms` | 父级 `30000` | 覆盖本组 PEL 接管阈值 |
| `groups.<name>.holds-check-interval-ms` | 父级 `5000` | 覆盖本组持锁自检间隔 |
| `groups.<name>.drain-timeout-ms` | 父级 `5000` | 覆盖本组排干上限 |
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

每个分区组首次建立时会在 `<streamPrefix>:partition-contract` 原子写入合同，后续节点必须完全一致：

```text
schema=1;count=<count>;lock-prefix-b64=<encoded-prefix>;hash=java-hash-sign-mask-mod
```

合同固定三个会改变消息归属或锁互斥域的事实：分区数、锁前缀和哈希算法。任何节点不一致都会拒绝启动该组，
防止同一个命名空间同时存在两套路由或两套锁。

`rebalance-ms`、`min-idle-ms`、`holds-check-interval-ms`、`drain-timeout-ms`、`batch-size`、
`poll-timeout` 属于运行调优参数，不写入持久合同，便于滚动调整。但滚动窗口内节点使用不同参数时，交付延迟和
排干上限会暂时不同；应逐批发布并持续观察分区持有量、PEL 和慢回调。

不能在既有命名空间原地修改 `count`、`default-group`、隔离组逻辑名或锁前缀。需要改变时应使用新命名空间，
停止旧生产者写入，排空旧 Stream 与 PEL，再统一切换生产者和消费者。直接删除合同 key 不能搬迁历史消息，
也不能重新建立同键顺序，因此不属于安全变更方案。

## PEL 接管与确认

组创建时使用 `0-0` 作为起始位置，所以先于消费者发布的历史消息仍可被读取。节点取得一个分区锁后：

1. 复验当前 holder 仍拥有分区锁；
2. 从 `0-0` 开始分页执行 `XAUTOCLAIM`；
3. 只处理 idle 不小于 `min-idle-ms` 的 PEL；
4. 在 `XAUTOCLAIM` 返回后再次复验锁权威；
5. 接管完成后才进入 `XREADGROUP ... >` 的新消息拉取。

网络异常、游标未完成或单轮扫描达到上限时，当前 Claim 保留后续接管标志并继续重试；锁已确定丢失、主动释放
或全局停机时停止重试。

确认规则：

- 业务回调成功后，执行 ACK 前再次复验原 holder；
- 复验返回失权或状态不明时不 ACK，消息留在 PEL；
- batch listener 失败时该 `(topic, event)` 桶全部不 ACK；
- single listener 逐条确认，单条失败不阻止同批其它成功条目确认；
- topic/event 为空、data 缺失、无法反序列化或没有匹配 listener 的消息会记录诊断并 ACK 丢弃，避免永久阻塞 PEL；
- 当前 owner 的业务回调失败后，消息留在其 PEL，但不会在本节点持续持有该分区时立即循环重投；它会在后续
  分区迁移或重新接管条件成立时再次交付。

最后一项意味着永久持有分区的稳定集群不能把业务异常当作自动重试队列。需要固定退避、最大次数或死信语义时，
业务应持久记录处理结局并显式投递，或采用具备相应合同的任务能力。

## Fencing 与业务幂等

Claim 取得分区锁时冻结 holder。组件在 PEL 接管前、接管结果提交业务前、ACK 前和必要的删除动作前用该 holder
复验 Redis Hash 中的所有权。长时间 GC、续租失败、租期过期或另一节点接管后，旧 Claim 不再确认消息，避免旧
owner 把新 owner 应处理的 PEL 从 Redis 中移除。

这个门禁只能约束 RedisPartition 自己的读取、ACK 和锁生命周期，不能撤销已经开始的外部操作。例如旧 owner
已经向数据库提交转账，随后在 ACK 前失权，新 owner 会再次收到同一消息。正确做法是让目标系统以稳定业务事件键
执行唯一约束、幂等写或可校验 fencing token，而不是依赖“同一时刻通常只有一个线程”。

`holds-check-interval-ms` 越小，失权发现越快，但空闲轮询中的 Redis EVAL 越多。它应明显小于锁租期，同时保留
足够网络和暂停余量。锁租期也必须覆盖正常业务回调的最长暂停窗口，并由看门狗持续续约。

## 停机与排干

`RedisProxy` 销毁时先关闭关联的 RedisPartition，再关闭普通订阅、分布式锁入口、业务执行器和连接工厂。
这个顺序保证消费者停止领取后仍能等待在途回调、执行最终 ACK 复验并释放分区锁，不会向已经关闭的线程池继续
提交消息。

每个分区组按以下顺序关闭：

1. 取消周期再平衡；
2. 从成员 ZSET 同步移除当前运行会话；
3. 取消唤醒频道订阅；
4. 快照仍存活的 Claim 并关闭新认领入口；
5. 请求每个 Claim 排干，持锁等待在途 listener 或 PEL 接管任务结束；
6. 停止拉取容器并在有界时间内等待 Claim 退出；
7. 锁真正释放后发布下线通知，提示其它节点立即认领。

`drain-timeout-ms` 是单 Claim 的业务排干上限。全组等待还包含 Claim 收尾的少量调度余量；多个分区组依次关闭，
因此进程总停机预算应覆盖所有活动组。业务回调在上限内结束时，owner 持锁到最终 ACK；超时后停止等待并依靠
ACK fencing 与锁租期兜底，迟到回调仍可能完成外部副作用，所以幂等要求不变。

业务一般不需要调用 `RedisPartition.shutdown()`。Spring 上下文关闭和 `RedisProxy.destroy()` 已纳入同一生命周期；
手工创建运行时或在容器之外使用时，调用方才需要显式关闭。

## 观测与运维

### 本地持有快照

```java
Map<String, List<Integer>> held = RedisPartition.load(redisProxy).claimedPartitions();
```

返回值按逻辑分区组列出当前节点持有的分区索引，默认组键名为 `<default>`。它是调用时刻的本地快照，不能单独
证明整个集群没有缺口或重复；集群视图还需要结合成员、锁和 Redis Stream 状态。

建议把以下数据桥接到应用现有健康检查或指标系统：

- qualifier、逻辑组、配置分区数和当前节点持有数；
- `<streamPrefix>:nodes` 的成员数量与过期分布；
- 每条 Stream 的长度；
- consumer group 的 pending 数量、最老 idle 和 lag；
- 分区认领、释放、锁失效、接管、ACK fencing 拒绝和停机超时日志；
- listener 耗时、失败数和业务幂等拒绝数。

RedisPartition 当前不内置特定指标库，也不提供 Web 控制台。接入 Micrometer、Prometheus 或其它平台时应由
业务运行时读取上述稳定入口和 Redis 原生信息，避免通过日志文本反推协议状态。

常用 Redis 诊断命令：

```text
ZRANGE <streamPrefix>:nodes 0 -1 WITHSCORES
GET <streamPrefix>:partition-contract
XINFO STREAM <streamPrefix>:<partition>
XINFO GROUPS <streamPrefix>:<partition>
XPENDING <streamPrefix>:<partition> <streamPrefix>
```

隔离组的 consumer group 名与 `streamPrefix` 相同。Redis Cluster 环境应把命令发往目标 key 所在节点，
并避免对整个 keyspace 执行阻塞式全量扫描。

### 常见现象

| 现象 | 优先检查 |
|---|---|
| 新节点上线后长期没有分区 | `partition.enabled`、listener qualifier、成员 ZSET、分区锁 TTL、周期再平衡日志 |
| 扩容后短时间分布不均 | 原 owner 是否仍在排干、释放通知是否可达、`rebalance-ms` 与 `drain-timeout-ms` |
| 进程退出后仍迟迟未接管 | 原锁剩余租期、成员过期时间、PEL idle 是否达到 `min-idle-ms` |
| 启动报告 partition contract mismatch | `count`、锁前缀、命名空间是否与已运行节点一致 |
| PEL 持续增长 | listener 异常、同 owner 不立即重投语义、反序列化诊断、ACK fencing 拒绝、业务耗时 |
| 同一业务对象观察到并行 | 生产者是否使用同类型稳定 key、是否绕过分区入口、分区数或 topic 到组映射是否变化 |
| 某些消息被诊断后丢弃 | topic/event/data 合同、listener 是否在组启动前正确注册、序列化配置是否一致 |
| 停机等待超过预期 | 活动分区组数量、在途回调时长、`drain-timeout-ms`、Redis 命令超时 |

## 参数调优

### 分区数

分区数决定最大并行度、Stream 和锁数量，也进入持久合同。建议按峰值并发、单条处理耗时和未来节点规模预留，
不要按当前实例数一比一设置。分区过少会限制并行度；分区过多会增加 Stream、consumer group、Claim 和运维
观测成本。需要完全隔离慢 topic 时优先新增隔离组，不要仅靠增大共享组批次掩盖阻塞。

### 批大小与阻塞时间

- 增大 `batch-size` 可以提高批量 listener 吞吐，也会增加单次回调时长、内存峰值和失败时的重放范围；
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

- 所有生产者与消费者对同一 topic 使用相同 Redis 数据源、命名空间、分区数和锁前缀；
- 每条消息使用稳定、非空且类型一致的业务分区键；
- listener 以稳定业务事件键保证外部副作用幂等；
- 隔离组只重复填写确需覆盖的参数，`count` 明确配置；
- 评估 `min-idle-ms`、锁租期、持有检查间隔和排干上限的联合时序；
- 为 PEL、lag、最老 idle、成员数、当前持有数和回调失败建立告警；
- 发布前确认没有在既有命名空间内改变分区合同；
- 停机预算覆盖全部活动分区组，业务线程池和 Redis 连接晚于 RedisPartition 关闭。
