# 安全策略

## 支持范围

安全维护只面向当前最新发布版本。Central 上的既有版本不可覆盖；相关处置会通过新的补丁版本发布。

## 报告漏洞

请通过仓库的 GitHub Security Advisory 私下报告漏洞：

https://github.com/nasa-runtime/nasa-redis/security/advisories/new

报告中请包含受影响版本、影响范围、复现条件和建议处置方向。请勿在安全版本发布前创建公开 Issue 或披露可直接利用的细节。

维护者确认问题后会评估影响、准备处置并协调披露时间。普通缺陷和功能建议请使用 GitHub Issue。

## 安全边界

- Redis 连接认证、ACL、TLS、网络隔离、凭据轮换和备份策略由部署方负责；凭据不得写入日志、Issue、示例
  或公开配置。
- RedisJob 与 RedisPartition 通过租约和 fencing 限制过期实例继续提交 Redis 控制面状态，但无法撤销已经
  发往数据库、消息系统或远程接口的副作用。Handler 和消费逻辑必须使用稳定业务键实现幂等。
- RedisJob 的跨语言 JSON 映射默认不接受 JVM 类型元数据；任务契约的 `contractRevision`、`schemaId` 或
  `codec` 不一致时应拒绝执行，不能绕过兼容性校验。
- nonce 计数保证只覆盖配置的幂等窗口，不是永久事件账本。窗口长度、Redis 数据持久性和业务对账能力必须
  与资金或额度场景的保留要求一致。
- 分布式锁和分区 owner 都是有期限的执行权。可能超过租期的业务操作必须续期并在提交前复验当前权威，
  不能把曾经获得锁或分区所有权视为永久授权。
- Stream 的逐记录执行权和成功 field 证据属于当前进程与来源代次。它们用于协调本地消费、恢复和确认，
  不构成跨进程持久业务账本；接管、重启或 consumer epoch 改变后，业务仍须使用稳定事件键去重。
