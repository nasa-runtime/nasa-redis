# 公开交付清单

本清单用于核对 nasa-redis 的公开产品合同和不可原地替换的 registry 归档。版本上传授权必须由维护者明确
给出；完成本地构建不等于获得上传授权。

## 产品合同

- 根 README 首屏准确说明组件价值、运行架构、关键安全顺序、能力边界和接入前提。
- RedisJob 的无中心调度与 Fanout、RedisPartition 的分区认领与本地按键执行在根 README 中都有独立架构说明，
  POM 描述与对应公开 Javadoc 使用一致定位。
- [REDIS-JOB.md](REDIS-JOB.md) 与 [REDIS-PARTITION.md](REDIS-PARTITION.md) 的状态机、配置默认值、
  观测指标、故障语义和不提供的能力与实现一致。
- Javadoc、示例、配置键、POM 的 `description`、依赖版本、许可证、SCM 与问题入口保持一致。
- 所有公开文本只描述当前有效的业务能力和约束，不包含开发批次、工作流水、工具归因、短期诊断内容或
  不再成立的入口。
- 同时检查 Markdown、Javadoc、源码与 Lua 注释、配置说明及 POM 元数据，使用语义化标题，不以内部编号或
  问题处置标签命名；真实 API、协议字段和业务状态名称按实现保留。
- 项目名称的独立性声明、双许可证文本、安全报告入口和贡献要求完整可读。

## 最终归档

先从 Maven Central 元数据与 GitHub 已公开的 Release、标签核对版本基线，确认目标版本尚未占用。
POM、README 依赖示例、JAR 文件名、嵌入 POM、manifest 的 `Implementation-Version` 和 Javadoc 标题必须使用同一制品版本；
`Specification-Version` 按 Maven JAR 插件规则保留主版本与次版本，不用它判断具体补丁制品。
GitHub 标签使用 `v` 加该版本，指向本次交付提交。依赖的正式版本必须已在公开仓库可获取。

使用仓库声明的 JDK 与 Maven 执行：

```bash
mvn -B -ntp clean verify
```

直接检查生成的主 JAR、sources JAR 和 javadoc JAR，不以工作树文件代替归档内容。至少确认：

- 坐标和版本与待上传版本一致，规范化 POM 的依赖、许可证、SCM 和开发者信息正确。
- 主 JAR 包含运行所需类、Lua、配置元数据，以及 README、组件说明、许可证、NOTICE、安全策略、贡献指南
  和本清单。
- 直接读取主 JAR 中的公开文档与嵌入 POM，和待交付工作树逐项核对；sources JAR 的源码与运行制品对应，
  javadoc JAR 中可以读到分区执行、PEL 恢复、确认边界和使用示例。
- sources JAR 和 javadoc JAR 能正常读取；公开 API 文档不存在缺失入口或与签名不一致的参数、返回语义。
- 所有归档均不包含本地质量程序及其产物、本机日志、IDE 元数据、密钥、凭据或工作目录。
- 检查重定位后的 Java 包名、公开配置类型、Spring AOT 入口和文档示例一致；需要调用方重新编译的边界须明确说明。
- 检查最终签名的主 JAR、sources JAR、javadoc JAR 与 POM 的摘要，保证 Central 和 GitHub 附件使用同一组内容。

`central-release` profile 会检查正式坐标与依赖并在 `verify` 阶段签名。进入 `deploy` 阶段会向 Central
上传，必须已有明确授权。该 profile 的 `autoPublish=false` 表示上传后仍需在 Central 完成发布操作，
上传成功不能代替公开可获取状态。

## 上传顺序

1. 完成目标分支提交并推送。
2. 等待远端 CI 通过，核对远端提交 SHA、待交付提交与拟使用的 GitHub 标签目标一致。
3. 获得维护者对当前坐标、版本、提交及发布目标的明确授权。
4. 上传并发布 Maven Central 归档。
5. 从 Maven Central 回读 POM、三种 JAR、签名和元数据；重新核对归档中的 README、配置、许可证与摘要。
6. 在授权范围内建立相同提交的 GitHub 标签与 Release。说明当前能力、接入条件和兼容边界，附件与 Central 制品一致。
7. 回读 GitHub 标签目标、Release 状态和附件摘要，确认其与已核对的 Central 版本及提交对应。

Maven Central 中已发布的版本不可覆盖。公开归档出现遗漏时，应停止交付并选择新的补丁版本；本地改动不能
代表既有线上版本已经变化。
