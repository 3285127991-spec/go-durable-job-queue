# durablequeue

一个从零实现的 Windows 本地可运行的持久化任务队列（Go，仅标准库）。
包含后端库、命令行工具和自动化测试；不依赖数据库、Redis、Docker 或任何外部服务。

## 特性

- 任务入队 / 领取 / 确认 / 失败重试 / 延迟重试 / 最大尝试次数 / 死信队列
- 按任务 ID 幂等去重：重复入队返回已有任务，不产生副本
- 租约 + token 领取模型：同一任务同一时刻只被一个 worker 有效领取；
  租约过期后可被重新领取，旧 token 无法操作新租约
- 全部状态持久化到项目内本地目录，进程强杀后重启可恢复一致状态
- 自实现追加日志（WAL）+ 快照压缩：
  - 日志尾部写入不完整（撕裂写）→ 截断尾部，保留此前全部有效数据
  - 日志中间损坏（后面仍有有效记录）→ 明确报错，不静默丢数据
- 并发安全：入队 / 领取 / 确认 / 重试 / 压缩可并发执行，不会重复领取或丢状态

## 目录结构

```
types.go            任务模型、状态、配置与错误定义
queue.go            队列核心：入队/领取/确认/失败/重试/死信/重开恢复
snapshot.go         快照写入（临时文件 + fsync + 原子改名）与加载
wal/wal.go          追加日志：CRC 帧、撕裂尾部截断、中间损坏报错
cmd/dq/main.go      CLI 子命令
cmd/dq/demo.go      可重复的脚本化演示
scripts/demo.ps1    CLI 级演示（每次调用都是独立进程）
*_test.go           自动化测试
```

数据目录（默认 `./data`）内只有两个文件：

- `queue.wal` — 追加日志，每条记录：`magic(4B) | seq(8B) | len(4B) | crc32(4B) | payload`，
  CRC 覆盖 `seq||len||payload`，记录体是任务变更后的完整状态（JSON），回放即幂等 upsert
- `queue.snap` — 快照（JSON），记录 `last_seq` 与全部任务；
  恢复时先加载快照，再回放 `seq > last_seq` 的日志

## 设计要点

- **持久化**：每次状态变更先写 WAL 并 fsync，再生效；崩溃后按快照 + 日志重建内存状态
- **撕裂尾部**：恢复时遇到不完整的头 / 不完整的记录体 / 尾部 CRC 错误，
  且其后不存在任何有效记录 → 物理截断到最后一个有效偏移，已提交数据不受影响
- **中间损坏**：损坏记录之后还能找到有效记录 → 返回 `wal.ErrCorrupt`，明确报错
- **快照压缩**：先写 `queue.snap.tmp`、fsync、原子改名为 `queue.snap`，再截断 WAL；
  日志序列号全局单调递增，压缩过程中任意时刻崩溃都能恢复到一致状态
- **租约**：`Claim` 将任务置为 `claimed` 并签发随机 token 与过期时间；
  `Ack`/`Fail` 校验 token 与租约有效期；租约过期后任务可被重新领取并签发新 token
- **重试**：`Fail` 后按指数退避（`BaseBackoff * 2^(attempts-1)`，封顶 `MaxBackoff`）延迟重投；
  达到 `MaxAttempts` 进入死信队列；worker 持有租约宕机也会占用一次尝试次数
- **并发**：单互斥锁串行化全部状态变更与日志写入，`Claim` 在锁内完成“挑选 + 占用”，
  因此并发领取不会重复

## 构建

```powershell
go build -o bin\dq.exe ./cmd/dq
```

## 运行测试

```powershell
go test ./...          # 全部测试
go test ./... -v       # 查看每个用例
```

测试覆盖：并发领取不重复、并发幂等入队、租约过期与旧 token 失效、延迟重试、
死信队列（主动失败与 worker 宕机两条路径）、崩溃恢复（含子进程强杀）、
日志撕裂尾部、日志中间损坏、快照压缩与恢复、并发压缩不丢状态。

## 演示

```powershell
.\bin\dq.exe demo          # 库级脚本化演示（每次运行重置 data-demo\）
powershell -File scripts\demo.ps1   # CLI 级演示（每条命令都是独立进程）
```

## CLI 用法

```powershell
# 所有命令都接受 -dir（默认环境变量 DQ_DIR 或 ./data）
.\bin\dq.exe enqueue -id job1 -payload "hello" -max-attempts 3 -delay 0s
.\bin\dq.exe enqueue -id job1 -payload "other"   # 幂等：提示已存在，不重复入队
.\bin\dq.exe claim -lease 30s                     # 输出 JSON，含 id 和 token
.\bin\dq.exe ack  -id job1 -token <claim 返回的 token>
.\bin\dq.exe fail -id job1 -token <token>         # 触发退避重试或进入死信
.\bin\dq.exe get -id job1
.\bin\dq.exe list -status dead                    # 死信队列
.\bin\dq.exe requeue -id job1                     # 死信任务重新入队
.\bin\dq.exe compact                              # 快照压缩
.\bin\dq.exe stats
```

## 库用法

```go
q, _ := durablequeue.Open(durablequeue.Options{Dir: "data"})
defer q.Close()

q.Enqueue("job-1", []byte("payload"), 3, 0)        // 幂等
task, _ := q.Claim()                                // 带租约和 token
if err := q.Ack(task.ID, task.Token); err != nil {  // 或 q.Fail(...)
    // token 不匹配 / 租约过期 / ...
}
```

可调参数：`LeaseDuration`（默认 30s）、`BaseBackoff`（默认 1s）、
`MaxBackoff`（默认 30s）、`NoSync`（默认 false，即每次提交都 fsync）。

## 限制

- 同一数据目录同一时刻只应由一个进程打开（库内并发安全，跨进程不加文件锁）
- 任务 ID 永久保留（包括已完成任务），用于幂等去重；长期运行需自行清理
- JSON 输出中 `payload` 字段为 base64（Go `[]byte` 的标准 JSON 编码）

