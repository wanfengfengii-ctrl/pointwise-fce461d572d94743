# 门户原始导出采集交接

## 提供的系统
Python 3.11+ 标准库、SQLite。pipeline.py 的校验、最高修订、墓碑、整数汇总及回执口径保持不变，checks.py 的原业务断言保留。sample.json 是完整业务输入。runtime.py 接纳本地规范输入，写中间文件并原子提交 ledger 和业务回执；单工作者重开可重做尚未入账的加工。这一能力已提供，不要求重写业务核。

collector.py 是可用的整包手工采集器：下载到 .part，校验长度和原始摘要后保存 .raw，再调用 runtime.submit/run_one/read。它每次重开都从零下载；received 现在只是写入进度，没有下载检查点或文件交接恢复策略。提供的是原始非答案基线。ops.py 已有确定性构建、包校验、安装和用户代理；lab.py 的 normal 走通正常安装采集。旧满盘实验不在本题中。

目标是 macOS 13+ 已登录用户会话，已有长期可用的 Python 3.11+ 解释器。解释器不随开发副本移走；不需要管理员权限或第三方依赖。LaunchAgent 使用当前用户，单实例文件锁已提供。ROOT 是安装及任务数据目录，CONTROL 仅存钩子和服务日志，均可能含空格。源码之外的任务文件均在 ROOT；不要求升级、回退、满盘恢复、载荷回收、多任务并发或浏览器自动化。仅一个源站、一个活动下载。手工 run 与后台 worker 不同时调用；worker 退出后由用户代理重开。

## 入口与数据
以下大写参数均为实际路径或字符串参数，用 argv 调用：
- python3 ops.py build OUT：生成 ZIP，包含 app.pyz、manifest.json；同一代码构建的包须逐字节相同。
- python3 ops.py install OUT ROOT --control CONTROL：ROOT 不存在或为空；先校验包，坏包不改变目标。
- python3 ops.py enqueue ROOT TASK URL：接纳任务，返回 {"task_id":TASK,"url":URL}。TASK 是非空 ASCII 字母、数字、下划线、连字符；绑定原 URL，同号同 URL 重试返回原接纳值，异 URL 报 TASK_CONFLICT。另有活动下载时拒绝新任务。
- python3 ops.py run ROOT：手工执行一次采集；传输失败留 queued，后续再 run；worker 会自动重试。
- python3 ops.py worker ROOT：持续执行，持有单实例锁。
- python3 ops.py start ROOT / stop ROOT：注册启动 / 卸载本 ROOT 的用户级 LaunchAgent。
- python3 ops.py downloads ROOT：按 task_id 返回下载状态数组，每项字段为 task_id、url、state、etag、length、sha256、received、error、receipt。
- python3 ops.py status ROOT / read ROOT BATCH_ID：原业务任务状态 / 分析结果和原业务回执。
- python3 ops.py submit ROOT INPUT：保留原本地批次接纳入口。
安装后均用 python3 ROOT/app.pyz 代替 python3 ops.py；selftest 执行包内业务和录制源回归。

下载状态保留 queued、downloading、failed、completed，可增加说明字段。最终 received 表示该 etag 下已持久确认的连续原始字节前缀长度，故障重开后不能把仅显示过的未落盘字节算为确认。未有远端表示时 etag、length、sha256 可为 null；receipt 仅 completed 才非 null。完成须保留 TASK.raw，且 received=length，摘要为原始文件 SHA-256。已校验完整文件按所捕获版本完成交接，不要求追逐此后的换版。同任务重复入队不重新采集已完成文件。输入非法或摘要错误置 failed，说明 error，不交给业务核；网络断开、超时和截短自动再试，不冒报完成。

URL 限 http，无认证、重定向或压缩传输；客户端请求 identity。源提供强 ETag、Content-Length、X-Content-SHA256，HEAD 与 GET 的表示元数据一致。每个响应内版本不变，但两次请求之间允许换版。支持 HEAD、完整 GET、Range: bytes=N-、If-Range: 强ETag。验证器相同时返回 206 及精确 Content-Range，不同时返回当前完整 200；N>=长度时返回 416 和 bytes */长度。HEAD 不返回正文。正常响应元数据合法；异常仅含传输中断、截短以及单独测试的整文件摘要错误，不要求研发通用 HTTP 客户端。不能仅凭 ETag 或长度跳过整文件摘要核验，也不能对换版 200 直接追加旧文件。

同版恢复不得请求 received 以内的载荷，HEAD 不算重取；崩溃前尚未确认的尾部允许重新请求。确认进度可以分块推进，不限定块大小或每次显示间隔。不能只在整包完成时确认进度：数据流持续到达时须在末尾之前确认非空前缀。检查点、文件就绪、业务接纳和入账间被终止，均须自动继续，既不丢失确认字节也不产生第二笔 ledger。网络调用不能持有 SQLite 写事务。没有多下载并发或跨库分布式事务要求。

原始文件是 UTF-8 JSON，可有尾部空白。业务摘要仍是 pipeline.encode(batch) 的摘要，不是原始下载摘要。sample 的结果为 [{"station":"east","minute":0,"count":2,"sum":0},{"station":"west","minute":1,"count":1,"sum":70}]。receipt 是原来的 batch_id、input_sha256、result_sha256；同批重放不能新增 ledger 行。只有完整校验通过的原始文件才能进入 runtime.submit。

## 可控源与样例
python3 portal.py --port 0 或 python3 ROOT/app.pyz source --port 0 启动仅监听 127.0.0.1 的源，stdout 第一行 JSON 给出实际 /export URL。源保持运行直到采集实验结束；源控制操作与导出请求使用同一端口。

POST /control 接受 JSON 的任意子集：{"version":2,"cut_after":20000,"bad_digest":false}。version 取1或2；cut_after 为非负整数或 null，对下一次 GET 只发送至多这些正文字节但仍宣称完整响应长度，然后断开；之后恢复正常。bad_digest=true 返回错误的整文件摘要，直至改回 false。控制响应为 {}。GET /requests 返回实际接到的导出请求，含 method、range、if_range、version、status、body_bytes；body_bytes 是源计划发送的正文长度，不能当成客户端已落盘量。

初始为版本1，ETag 为 "portal-1"，原始长度262144，来自 sample.json 后补空白，西站合计70。版本2仍为 batch_id=sample，将 a 的 rev=2 值改为11，补空白至270336字节，ETag 为 "portal-2"，西站合计110，东站不变；两版原始摘要由响应给出。每种换版实验用新 ROOT，不向已入账的 sample 重绑另一业务内容。

barrier 位于 runtime.py。CONTROL/hook.json 为 {"phase":"chunk"} 时，首次写入块并显示进度后写 chunk.ready（内为PID），等待 chunk.go，通过后写 chunk.spent。其余 phase：verified 在完整文件校验保存后、业务接纳前；accepted 在业务接纳后；accounted 在业务入账后、下载完成状态前。既有 stage/result/ledger 钩子也保留。恢复测试可在 ready 后向 PID 发 SIGKILL，等待 launchctl 显示不同 PID，再写相应 .go；.spent 使已通过钩子不重复阻塞。新检查点代码可增补必要边界钩子，钩子只控制暂停，不提供恢复行为。

## 回归与上线交付
python3 -B checks.py normal 核对原业务及唯一入账；python3 -B checks.py source 在内存中解析录制源生成的真实 HTTP 响应字节，核对两版整包采集、续传响应、换版200、416、截短拒绝和坏摘要拒绝。这个协议回归不启动网络、不验证断点恢复。python3 -B checks.py verify 运行两者；demo 的 source 模式展示实际结果和来源摘要。

python3 lab.py normal OUT NEW_DIRECTORY 是已提供的真实正常上线入口：从包解出开发副本重新构建、比较包、移走副本，从安装包启动源和用户代理，采集版本1并核对 .raw、分析结果、原回执及唯一 ledger 行。它结束时停止代理和源，将 evidence.json 留在新目录。lab.service_pid、wait_for 可用于恢复回归。若外部中断实验，可用安装包 stop ROOT 清理对应代理。录制源和业务核均在包内，无需开发目录。

本轮还要实际验证截短续传、下载中杀进程后同版前缀复用、未完成换版和错误摘要不发布；在文件就绪、业务接纳及入账后的交接窗口终止并由真实 LaunchAgent 重启，最终都须唯一入账。证据记录实际命令、重启前后 PID、源请求范围与版本、持久进度、原始摘要和最终业务回执，使人能核对“未重取确认前缀”及“没有混版、重复入账”。测试编排方式自选，不增加未公开的命令、文件格式或测试数量门槛。

最终 .pointwise-runtime.json 保持 version=1、kind=cli、setup 为 argv 数组列表；verify 和 demo 应改为经过安装产物的真实采集核账与展示入口。交付可校验发布包、简短操作恢复说明和上线证据，说明如何启动、查看进度和失败原因、核对结果及停止本代理；不要求手改数据库。现有包安装和自启动设施可直接复用，不要求重新研发发布框架。
