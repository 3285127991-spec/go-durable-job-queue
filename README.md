# go-durable-job-queue

从零实现的 Windows 本地持久化任务队列：Go 后端库 + 命令行工具 + 自动化测试。
只依赖 Go 标准库，不需要数据库、Redis、Docker 或任何云服务；所有数据都在项目目录内。

## 特性

- 任务生命周期：入队 → 领取（租约 + token）→ 确认 / 失败重试（支持延迟）→ 死信队列
- 按任务 ID 幂等去重：重复入队返回已有任务，不产生副本
- 排他领取：同一任务同一时刻只能被一个 worker 有效领取；租约过期后可被重新领取，
  旧 token 对新租约一律失效（`ErrStaleToken`）
- 持久化：自实现追加日志（WAL）+ 快照压缩；进程被强杀后重启可恢复一致状态
- 崩溃安全：日志尾部撕裂写入自动截断、保留此前有效数据；日志中间损坏明确报错
  （`CorruptionError`）；快照与 WAL 通过 LSN 保证重放幂等
- 并发安全：所有变更在单互斥锁下先写日志（fsync）再应用，并发入队 / 领取 / 确认 /
  重试 / 压缩不会重复领取或丢状态

## 存储设计

数据目录（`--dir`，默认 `./data`）内有两个文件：

| 文件 | 说明 |
| --- | --- |
| `queue.wal` | 追加日志。记录格式 `[magic:4][lsn:8][len:4][crc32:4][payload]`，CRC 覆盖 lsn+len+payload；每条变更写入后 fsync |
| `queue.snapshot.json` | 快照：全部任务状态 + 已应用的 LSN。先写 `.tmp`、fsync、再原子 rename |

恢复流程：加载快照 → 扫描 WAL（撕裂尾部截断）→ 跳过 `lsn <= 快照LSN` 的记录 →
重放剩余记录。压缩（`Compact`）= 写快照 + 清空 WAL；若在两者之间崩溃，旧 WAL 记录
会因 LSN 已过而被跳过，不会重复应用。

## 构建

```powershell
go build -o bin/dq.exe ./cmd/dq
```

## 命令行用法

```powershell
# 入队（--id 是幂等键；重复入队不会覆盖已有任务）
.\bin\dq.exe --dir data enqueue --id job-1 --payload "hello" --max-attempts 3

# 领取（输出 JSON，含租约 token；无可领取任务时退出码为 3）
$c = .\bin\dq.exe --dir data claim --lease 30s | ConvertFrom-Json

# 确认 / 失败重试（必须携带 claim 返回的 token）
.\bin\dq.exe --dir data ack  --id $c.id --token $c.token
.\bin\dq.exe --dir data fail --id $c.id --token $c.token --retry-delay 5s --error "boom"

# 查询
.\bin\dq.exe --dir data get --id job-1
.\bin\dq.exe --dir data list --status pending
.\bin\dq.exe --dir data stats

# 压缩：写快照并清空 WAL
.\bin\dq.exe --dir data compact
```

## 可重复演示

```powershell
.\bin\dq.exe --dir data-demo demo
```

演示会先清空 `data-demo` 再执行完整剧本：幂等入队、领取/确认、延迟重试、
死信队列、租约过期与旧 token 拒绝、快照压缩、模拟强杀后重启恢复，可反复运行。

## 运行测试

```powershell
go test ./...          # 全部测试
go test -v .           # 查看每个用例
```

测试覆盖：

| 测试 | 覆盖点 |
| --- | --- |
| `TestWALRoundTrip` / `TestWALReset` | 日志读写、重置 |
| `TestWALTornTailTruncated` / `TestWALTornTailTruncatedPayload` | 尾部撕裂写入：保留有效数据并截断 |
| `TestWALMidCorruptionFails` / `TestWALBadMagicFails` | 中间损坏：明确报 `CorruptionError` |
| `TestEnqueueIdempotent` / `TestConcurrentEnqueueSameID` | 按任务 ID 幂等去重（含并发） |
| `TestClaimExclusiveAndAck` / `TestConcurrentClaim` | 排他领取（含 32 worker 并发） |
| `TestLeaseExpiryReclaimAndStaleToken` / `TestAckAfterLeaseExpiryRejected` | 租约过期重领、旧 token 失效 |
| `TestDelayedRetry` | 延迟重试（注入时钟，无需真实等待） |
| `TestDeadLetterAfterMaxAttempts` | 最大尝试次数后进入死信队列 |
| `TestRecoveryAfterCrash` | 不 Close 直接重开（模拟强杀）后状态一致 |
| `TestCompactAndRecover` / `TestSnapshotOnlyRecovery` / `TestRepeatedCompactionKeepsState` | 快照与压缩恢复 |
| `TestCrashBetweenSnapshotAndWALReset` | 压缩崩溃窗口：快照已改名、WAL 未重置，LSN 跳过防重放 |
| `TestSnapshotCorruptionFails` | 快照损坏明确报错 |
| `TestConcurrentMixedWorkload` / `TestConcurrentClaimDuringCompaction` | 并发入队/领取/确认/重试/压缩不重复领取、不丢状态 |

## 作为库使用

```go
import durablequeue "durablequeue"

q, err := durablequeue.Open(durablequeue.Options{Dir: "data"})
if err != nil { /* 若是 durablequeue.CorruptionError 说明日志中间损坏 */ }
defer q.Close()

task, created, _ := q.Enqueue("job-1", []byte("payload"), durablequeue.EnqueueOptions{MaxAttempts: 3})

claimed, err := q.Claim(30 * time.Second)   // 返回任务 + LeaseToken
_ = q.Ack(claimed.ID, claimed.LeaseToken)   // 成功确认
_ = q.Fail(claimed.ID, claimed.LeaseToken, 5*time.Second, "reason") // 延迟重试 / 超次数进死信

_ = q.Compact() // 快照压缩
```

## 设计说明与限制

- 单进程嵌入式设计：同一数据目录同一时刻只应被一个进程打开（未实现跨进程文件锁）。
- 领取按 FIFO 顺序线性扫描，适合本地中小规模任务量。
- at-least-once 语义：租约过期未确认的任务会被重新投递，消费者需自行保证处理幂等。
- `Options.Now` 可注入时钟，便于确定性测试；`Options.DisableSync` 仅供基准测试使用。
