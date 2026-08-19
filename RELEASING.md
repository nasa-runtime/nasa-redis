# 发布指南

本文说明如何把 nasa-redis 发布到 GitHub 和 Central Portal。Central 中的公开版本不可原地替换，代码验收、
文档验收和最终归档验收必须分别完成。

## 发布前条件

- Central Portal 当前账户存在状态为 `Verified`、且覆盖 `io.github.nasa-runtime` 的 namespace；
- Central Portal 已生成 User Token；
- 本机已配置 GPG 主签名密钥，公钥已上传至 Central 支持的公开 key server；
- `pom.xml`、README、架构指南和变更记录中的坐标与版本一致；
- Maven Central 元数据中不存在目标版本，且目标 POM 直链返回 `404`；
- 目标提交已经推送到 `https://github.com/nasa-runtime/nasa-redis`，远端 CI 全部通过；
- 已核对本地目标提交、远端分支和 CI 对应的 commit SHA 完全相同；
- 已获得本次公开发布的明确授权。

Central 凭证放在用户级 `~/.m2/settings.xml`，不要写入项目：

```xml
<settings>
    <servers>
        <server>
            <id>central</id>
            <username>${env.CENTRAL_USERNAME}</username>
            <password>${env.CENTRAL_PASSWORD}</password>
        </server>
    </servers>
</settings>
```

GPG 口令交给 `gpg-agent`，不要用 `-Dgpg.passphrase=`，避免进入 shell 历史和进程列表。

macOS 可以使用 `pinentry-mac`：

```text
pinentry-program /opt/homebrew/bin/pinentry-mac
default-cache-ttl 10800
max-cache-ttl 10800
```

`GNUPGHOME` 不要指向层级过深的目录；gpg-agent 的 Unix socket 路径存在系统长度上限。

## 代码与文档验收

发布前从首次使用者视角逐项通读公开入口：

| 入口 | 必须核对的内容 |
|---|---|
| `README.md` | 首屏定位、依赖坐标、RedisJob 与 nonce 幂等计数的核心价值、快速接入、能力边界 |
| `REDIS-JOB.md` | 调度路径、Fanout、fencing、不变量、失败策略、配置、观测和明确不解决的问题 |
| `pom.xml` | description、坐标、许可证、SCM、开发者、Java 版本和依赖版本 |
| `CHANGELOG.md` | 当前版本的公开能力、运行要求和兼容边界 |
| RedisJob 公开 Javadoc | 调度门面、注解、并发策略、配置语义与协作式超时边界和源码一致 |
| 配置元数据 | `nasa.redis.job.*` 与 `nasa.redis-proxy.idempotent-counter.*` 能被 IDE 和归档中的 Spring metadata 发现 |

尤其核对以下产品合同：

- RedisJob 不依赖应用级主节点；
- `@EnableRedis` 不隐式启用 RedisJob，`@EnableRedisJob` 只建立管理基础设施且零任务时不创建具体 Scheduler；
- 框架不提供默认 `RedisJobScheduler` Bean，`@RedisJob.qualifier` 必填，静态入口也必须显式传入 source id；
- 静态入口受 JVM 内唯一管理器和 Spring 容器关闭边界约束，不能把旧 Scheduler 跨上下文复用；
- `SERIAL_QUEUE` 默认在集群范围阻止同名 Handler 并行，fixed rate 排队与 fixed delay 终态后计时的差异明确；
- 普通任务是至少一次执行，外部副作用不承诺 exactly-once；
- `attemptToken`、`runId` 与 Fanout `executionKey` 的业务责任没有被夸大；
- `BEST_EFFORT` 部分完成映射为外层 `FAILED + resultCode=PARTIAL_FAILED`；
- Fanout 容量背压不会被写成节点失联，容量路由频率预算与真实故障 assignment 配额相互独立；
- Job JSON Default Typing 固定关闭；
- 当前只发布 Java 运行时，没有把跨语言线协议描述成已经提供其它语言 SDK；
- nonce 幂等计数只保证配置窗口内同一凭证最多改变一次目标值，不承诺永久去重或余额非负；
- `AUTO` TTL 模式、共享 layout marker、同 slot sidecar 与目标值/账本一致性边界均有明确说明；
- Sharded Pub/Sub 与广播降级模式的 Redis 版本和网络放大边界准确；
- 启动到首次心跳之间可能短暂 `DEGRADED`；
- 显式 Pipeline 与依赖 `Initialization.before()` 的阈值自动批处理被清楚区分。

## 构建并检查最终归档

先确认不可覆盖的目标坐标尚未存在：

```bash
curl -fsS https://repo1.maven.org/maven2/io/github/nasa-runtime/nasa-redis/maven-metadata.xml
curl -sS -o /dev/null -w '%{http_code}\n' \
  https://repo1.maven.org/maven2/io/github/nasa-runtime/nasa-redis/2.0.0/nasa-redis-2.0.0.pom
```

第二条命令在上传前必须返回 `404`；若返回 `200`，立即选择新的合法版本并同步全部文档与发布命令。

先执行完整 Maven 验证：

```bash
mvn -B -ntp clean verify
```

然后直接检查待发布产物，而不是用工作树内容代替归档：

```bash
jar tf target/nasa-redis-2.0.0.jar
jar tf target/nasa-redis-2.0.0-sources.jar
jar tf target/nasa-redis-2.0.0-javadoc.jar
unzip -p target/nasa-redis-2.0.0.jar META-INF/README.md
unzip -p target/nasa-redis-2.0.0.jar META-INF/REDIS-JOB.md
unzip -p target/nasa-redis-2.0.0.jar META-INF/CHANGELOG.md
unzip -p target/nasa-redis-2.0.0.jar META-INF/spring-configuration-metadata.json
```

主 JAR 必须包含：

- `META-INF/README.md`、`META-INF/REDIS-JOB.md`、`META-INF/CHANGELOG.md`；
- Apache-2.0 与 MIT 许可证；
- RedisJob 的 Java 类、Lua 资源和 Spring AOT hints；
- `EnableRedisJob` 显式入口与 `RedisJobSchedulers.scheduler(sourceId)` 静态门面；
- RedisProxy nonce 幂等计数实现及 `idempotent_counter_hfe.lua`、`idempotent_counter_bucket.lua`；
- `META-INF/spring-configuration-metadata.json`；
- 正确的 `Automatic-Module-Name`、Implementation Version 与 Java 21 字节码。

三个归档都不得包含凭证、本机路径、日志、生成期临时文件、本地质量工程内容或隐藏的本地工具状态。
产品源码、公开文档和 manifest 不得反向引用本地质量工程路径。POM 不得包含快照依赖。

## 提交、CI 与发布授权

默认顺序不可跳步：

1. 完成目标组件及明确关联门面文档的提交；
2. 推送目标分支；
3. 等待远端 CI 全部通过；
4. 核对远端 commit SHA 与待发布提交一致；
5. 展示最终归档和文档验收结果，取得明确发布授权；
6. 上传 Central Portal；
7. 回读 Central 元数据、POM 与公开仓库 README；
8. 创建签名标签和 GitHub Release。

没有明确发布授权时只能完成本地构建和归档检查，不能上传 Central、推送标签或创建 Release。

## 上传 Central Portal

```bash
export CENTRAL_USERNAME='<token username>'
export CENTRAL_PASSWORD='<token password>'
mvn -B -ntp -Pcentral-release clean deploy
```

`central-release` profile 会为 POM、主 JAR、sources JAR 和 Javadoc JAR 生成 GPG 签名，并由 Central
Publishing Maven Plugin 生成校验和与 deployment。配置默认不自动发布；上传验证通过后，在 Portal 中再次
核对 groupId、artifactId、version、文件清单、签名和 POM，再执行 Publish。

## 发布后回读

Central 显示 Published 后，必须从公开入口回读：

- `io.github.nasa-runtime:nasa-redis:<version>` 的 POM description、许可证、SCM 和依赖；
- 主 JAR、sources JAR、Javadoc JAR 与校验和；
- Maven Central 展示页和 GitHub 仓库的 README；
- Javadoc 中的公开 API；
- GitHub Release 的版本说明是否与 `CHANGELOG.md` 一致。

发现公开归档遗漏时立即撤回发布验收结论。Central 版本不可原地替换，工作区补正文档不能改变已经公开的
归档；必须等待新的补丁版本及对应发布授权。

## GitHub 标签与 Release

Central 回读通过后创建签名标签：

```bash
git tag -s v2.0.0 -m "nasa-redis 2.0.0"
git push origin v2.0.0
```

在 GitHub 创建相同版本的 Release，说明以 `CHANGELOG.md` 对应版本为准。不要上传签名私钥、Central
Token、用户级 Maven 配置或其它本机状态。
