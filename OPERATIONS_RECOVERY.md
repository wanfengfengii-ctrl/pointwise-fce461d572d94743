# 门户导出无人值守采集：发布、恢复与上线证据

沿用 `OPERATIONS.md` 的 Python/SQLite 入口、可控 HTTP 源与用户代理。业务核
`pipeline.py` / `runtime.py` 的校验、最高修订、墓碑、整数汇总、回执与唯一 ledger
口径未改动；`checks.py` 的原有业务断言保留。

## 交付物

| 路径 | 说明 |
| --- | --- |
| `ops.py` | 可复现构建（`build`）、包校验安装（`install`）、用户级启动/卸载（`start`/`stop`） |
| `collector.py` | 可恢复下载核：持久检查点、强 ETag 绑定、断点续传、换版丢弃、校验后交接 |
| `lab.py` | `normal` 既有正常上线入口 + `verify` / `demo` 安装产物回归与展示 |
| `checks.py` | 原业务断言 + 录制源协议回归；`verify`/`demo` 改为经安装产物的真实采集核账 |
| `.pointwise-runtime.json` | `setup` 为 argv 数组列表；verify/demo 走安装包 |

同一份源码构建的 ZIP 必须逐字节相同（固定条目时间、存储压缩、排序条目）；包内只有
`app.pyz` 与 `manifest.json`，`manifest.json` 的 `sha256` 是 `app.pyz` 的摘要。
`install` 先校验包再改动目标，坏包不会创建或改变 ROOT。

## 启动 / 查看 / 停止（macOS 13+ 已登录用户会话）

```sh
python3 ops.py build release.zip
python3 ops.py install release.zip ROOT --control CONTROL     # ROOT 不存在或为空
python3 ROOT/app.pyz enqueue ROOT TASK URL                    # 接纳一个任务
python3 ROOT/app.pyz start ROOT                               # 注册并拉起用户 LaunchAgent
python3 ROOT/app.pyz downloads ROOT                           # 进度、错误、回执
python3 ROOT/app.pyz status ROOT                              # 业务状态
python3 ROOT/app.pyz read ROOT BATCH_ID                       # 结果与业务回执
python3 ROOT/app.pyz stop ROOT                                # 卸载本 ROOT 的代理
```

`ROOT` 与 `CONTROL` 可以含空格；脚本一律用 argv 传路径。代理 `KeepAlive`，
worker 崩溃或被杀后由 launchd 用新 PID 自动重开；单实例由 `ROOT/worker.lock` 的
flock 保证。手工 `run` 与后台 worker 不应同时调用。

`downloads` 每项字段：`task_id`、`url`、`state`、`etag`、`length`、`sha256`、
`received`、`error`、`receipt`。

- `state`: `queued` / `downloading` / `failed` / `completed`
- `received`: 该 `etag` 下已持久确认的连续原始字节前缀长度；完成时 `= length`
- `sha256`: 原始文件 SHA-256（不是业务摘要，业务摘要是 `pipeline.encode(batch)`）
- `error`: 传输类失败（断开、超时、截短）留 `queued` 并自动重试；非法输入、非法元数据
  或整文件摘要错误置 `failed`，不交业务核
- `receipt`: 仅 `completed` 非空；重复读取稳定

失败原因查看 `downloads` 的 `error`；服务日志在 `CONTROL/service.stdout`、
`CONTROL/service.stderr`。不需要手改数据库。

## 恢复模型

**持久检查点** = `(strong ETag, Content-Length, X-Content-SHA256, received)`，与
`downloads` 行同库提交。写入顺序是：先 `write` → `flush` → `fsync` 载荷，**再**提交
检查点，因此重启后不会把“只显示过、未落盘”的字节算作已确认。网络调用从不持有
SQLite 写事务（每次只开短事务）。

- **同版续传**：`received > 0` 时先用 `HEAD` 比对强 ETag（HEAD 不算重取载荷）；
  一致则发 `GET Range: bytes=received-` + `If-Range: 强ETag`。源返回 206 且
  `Content-Range` 起点等于 `received` 时，仅追加其后缀；确认前缀以内的字节不再请求。
  崩溃前未确认的尾部会被截掉并允许重新请求。
- **换版丢弃**：`HEAD` 的强 ETag 不同，或续传请求得到完整 `200`（If-Range 不匹配）时，
  立即截断 `.part` 并重置检查点，旧未完成载荷不参与拼接；随后的 200 从 0 开始写入。
  `416` 且 `Content-Range: bytes */长度` 在验证器相同时按已抵达末尾处理；验证器不同则
  重置并从完整 200 重取。
- **整包校验**：长度必须等于 `Content-Length`，整文件 SHA-256 必须等于
  `X-Content-SHA256`；不能只凭 ETag 或长度跳过摘要核验。截短、超时、摘要错误都不会
  `completed`，`.raw` 不发布，也不调用 `runtime.submit`。
- **恰好一次交接**：校验通过的 `.part` 经 `fsync` 后 `os.replace` 成 `TASK.raw` 并
  `fsync` 目录，然后依次 `submit` → `run_one` → 入账。`runtime.submit` 对同 digest
  幂等、`commit_result` 在单事务内写 ledger 与业务回执，因此任一交接窗口重启后都复用
  `TASK.raw`：要么重做未完成加工，要么发现已完成，最终仅一笔 ledger。下载行在业务
  回执可读后才与 `received=length` 一起提交 `completed`，故回执稳定、不会出现
  `completed` 而业务未入账的状态。

## 回归与上线证据

```sh
python3 -B checks.py normal     # 原业务断言（内存 SQLite）
python3 -B checks.py source     # 录制源真实 HTTP 字节上的协议回归（不启动网络）
python3 -B checks.py verify     # 安装产物 + 真实源 + 真实代理的全套恢复回归
python3 -B checks.py demo       # 安装产物真实采集一次并展示来源摘要/结果/回执
python3 -B lab.py normal RELEASE NEW_DIRECTORY   # 既有正常上线入口
```

`verify` 在临时目录（或给定目录，带所有权标记）中构建可复现 ZIP，逐场景安装到新
ROOT，启动安装包内的录制源与用户代理；每个场景结束把证据写入该子目录的
`evidence.json`，汇总写入顶层 `evidence.json`：

| 场景 | 驱动方式 | 关键断言 |
| --- | --- | --- |
| `normal` | 正常安装、enqueue、start | `.raw` 长度/摘要、结果、原回执、唯一 ledger |
| `truncation` | `cut_after=20000` | 首次只落 20000 字节，后续 `HEAD` + `206 bytes=20000-`，未从 0 重取，完成 v1 |
| `interrupted_download` | `chunk` 钩子，SIGKILL PID | 重启后新 PID；`HEAD` + 206 从持久 `received` 起续，源请求无回退 |
| `version_change` | v1 截短到 20000，再切 v2 | 无 206 混版请求；旧载荷丢弃；完成 v2，西站合计 110 |
| `bad_digest` | `bad_digest=true` | `failed`/`DIGEST`，无 `.raw`，ledger=0，业务未接纳 |
| `handoff_verified` | `verified` 钩子，SIGKILL | 重启前 ledger=0；重启后不重取载荷，唯一 ledger |
| `handoff_accepted` | `accepted` 钩子，SIGKILL | 重启前 ledger=0；从 `TASK.raw` 继续，唯一 ledger |
| `handoff_accounted` | `accounted` 钩子，SIGKILL | 重启前 ledger=1；重启后仍唯一 ledger，回执稳定 |

证据字段包括实际命令所用的源 URL、重启前后 PID、源请求的 `method/range/if_range/
version/status/body_bytes`、持久 `received`、原始摘要与最终业务回执，
可直接核对“未重取确认前缀”与“没有混版、重复入账”。

## 已记录的上线证据

仓库内 `evidence/` 是 2026-11-14 在本容器实跑的结果（命令在 `/workspace` 执行）：

| 文件 | 内容 |
| --- | --- |
| `evidence/verify-evidence.json`、`evidence/verify.stdout` | `python3 -B checks.py verify /tmp/pw-final` 的完整场景证据 |
| `evidence/demo-evidence.json`、`evidence/demo.stdout` | `python3 -B checks.py demo /tmp/pw-final-demo` 的安装产物真实采集展示 |

本次发布包摘要 `d0a9318a5f097f18a94fb8108291c08d464188e76b3c1e21f5d044a1494db4cd`
（同一源码构建两次逐字节相同）。各场景均为 `ok: true`，耗时约 15 秒。核对要点：

- `truncation`：源先只发 20000 字节并断开；随后 `HEAD`（0 正文）与
  `GET 206 bytes=20000-` + `If-Range: "portal-1"`，从未从 0 重取；
- `interrupted_download`：`chunk` 钩子处持久 `received=7901` 时 SIGKILL PID
  `3158`，代理以新 PID `3162` 重开，续传请求为 `GET 206 bytes=7901-`；
- `version_change`：v1 留下 20000 字节未完成载荷后切 v2，唯一请求是完整
  `GET 200`（无 206 混版），完成 `"portal-2"`、270336 字节、西站合计 110；
- `bad_digest`：`failed` / `DIGEST`，无 `.raw`、ledger=0、业务 `status` 为空；
- 三个交接窗口（`verified` / `accepted` / `accounted`）：重启前后 PID 不同，
  重启前 ledger 分别为 0 / 0 / 1，重启后均恰为 1 笔，回执与下载行一致，
  且重启后源请求数不增加（不重取已确认载荷，也不重下已保存的 `.raw`）。

不同机器上的 PID、端口与发布包摘要会变化；以此仓库中的文件为准。
以上验证在 Linux 容器完成，`start` 走的是与 LaunchAgent 同契约的本地监督进程
（见下节）。macOS 真机使用同一 `verify` 即可得到 launchd 重开的 PID 证据。

## 非 macOS 开发机

本仓库的回归在 Linux 容器中运行。`start` 在检测不到 `launchctl` 时使用同一份
`~/Library/LaunchAgents/<label>.plist` 契约的本地监督进程：常驻 supervisor 包裹
`app.pyz worker`，worker 退出即重开，PID 写入 `CONTROL/fallback-agent.json`；
`stop` 先终止 supervisor 再终止 worker。macOS 上仍使用真实 `launchctl bootstrap/
kickstart/bootout`，生产路径未改变。回归里的“重启前后 PID”在该模式下是真实的新进程
PID，与 launchd 的重开语义一致。
