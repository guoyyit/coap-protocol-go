# coap-protocol-go

一个本地 CoAP（RFC 7252 / RFC 7641 / RFC 7959 子集）协议栈，面向不稳定数据报网络：
请求-响应、可确认重传、去重幂等、观察推送、块传输与崩溃恢复。
测试通过内存管道（`PipeTransport`）模拟丢包、重复与乱序，不依赖真实网络设备。

## 功能概览

| 能力 | 入口 |
|---|---|
| 消息编解码与畸形报文拒绝 | `Encode` / `Decode`（message.go） |
| 可确认请求重传与指数退避 | `Client.Do`（client.go） |
| 重复报文识别与幂等处理 | `Dedup`（dedup.go）+ 服务器响应缓存 |
| 令牌匹配的并发请求 | `Client`（pending 按 token 索引） |
| 观察通知（订阅/推送/顺序号/取消） | `Client.Observe` / `Server.NotifyChanged`（observe.go） |
| 块传输 Block1/Block2 | `Client.GetLarge` / `Client.PutLarge`（blockwise.go） |
| 崩溃恢复（去重 + 订阅持久化） | `NewServer(t, cfg, stateDir)` |

## 快速开始

```go
ct, st := coap.NewPipe("client", "server", coap.PipeConfig{})
srv := coap.NewServer(st, coap.DefaultServerConfig(), "") // stateDir="" 关闭持久化
srv.Handle("/temp", func(req *coap.Message, addr string) (*coap.Message, error) {
    return &coap.Message{Code: coap.CodeContent, Payload: []byte("21")}, nil
})
cli := coap.NewClient(ct, coap.DefaultClientConfig())

req := &coap.Message{Code: coap.CodeGET}
req.SetPath("/temp")
resp, err := cli.Do(context.Background(), req, "server")
```

观察订阅：

```go
obs, _ := cli.Observe(ctx, req, "server")
for n := range obs.C {           // 取消后通道关闭
    fmt.Println(n.Seq, n.Message.Payload) // Seq 缺口即丢包/乱序信号
}
obs.Cancel()                     // 之后服务器不再推送
```

大报文：

```go
data, _ := cli.GetLarge(ctx, "/firmware", "server", coap.Block256) // Block2 拉取
_, err := cli.PutLarge(ctx, "/upload", blob, "server", coap.Block256) // Block1 推送
```

## 超时与重传参数（ClientConfig）

| 参数 | 默认 | 语义 |
|---|---|---|
| `AckTimeout` | 500ms | 首次等待 ACK 的基础超时 |
| `AckRandomFactor` | 1.5 | RFC 7252 抖动因子（保留；当前实现对测试友好地使用确定性退避） |
| `MaxRetransmit` | 4 | 最大重传次数，耗尽后 `Do` 返回 `ErrTimeout` |
| `MaxLatency` | 10s | 迟到响应墓碑的保留时长，超时后旧 message ID 不再被识别 |
| `DedupWindow` | 60s | 入站 CON 去重窗口（见下节） |

第 n 次重传（从 0 计）的等待时间为 `AckTimeout << n`，即指数退避
（30ms 基础超时下依次为 30/60/120ms）。超时路径保证清理全部在途状态：
`pending`、`byMID` 与重传定时器均被移除，不会留下悬挂请求。

## 去重窗口（DedupWindow）

- 键为 `(对端地址, MessageID)`。窗口内再次出现的 CON 判定为重复：
  服务器**不重执行业务处理器**，而是重放缓存响应；客户端对重复通知只回 ACK。
- 窗口过期后旧条目被清除，相同 MessageID 可被安全复用（RFC 7252 的 EXCHANGE_LIFETIME 语义）。
- 服务器重启后 MessageID 起点随机化，避免与对端窗口内的历史编号碰撞。
- `Dedup.Snapshot` / `Restore` 持久化窗口内容；恢复时按快照时间戳推算年龄，
  超过窗口的条目安全丢弃，重启后历史重复报文不会被当作新请求。

## 并发语义

- **客户端**：`Do` 可任意并发。响应按 **token** 匹配，与 MessageID 相互独立，
  多请求乱序返回不会错配。自动分配的 token 内含单调序号，天然不复用；
  手动复用 token 时，已完成请求的墓碑（`MaxLatency` 内）与 dedup 共同阻止
  迟到的旧响应误配给新请求。未知 mid 的 ACK/RST（重复、乱序、迟到）被静默忽略。
- **服务器**：每个请求在独立 goroutine 中处理，慢资源不阻塞接收循环；
  去重与响应缓存保证并发重投递下处理器仍只执行一次。
- **观察**：通知以 CON 推送，订阅者回 RST 即被移除；`ObserverTTL`（默认 5min）
  内无任何交互的订阅者被判定失效并清理，不再接收推送。
- **块传输**：重组器按 `(地址, token)` 隔离，乱序块先缓存、重复块幂等忽略、
  尺寸/SZX 不一致立即报错；只有收齐 `0..total-1` 才输出重组结果，
  不依赖底层可靠传输。

## 崩溃恢复

`NewServer(t, cfg, stateDir)` 非空时在 `stateDir` 下持久化：

- `dedup.json`：去重窗口（含时间戳），重启后历史重复报文仍被识别为重复；
- `observe.json`：观察订阅（含顺序号），重启后 `NotifyChanged` 继续推送且
  顺序号与崩溃前保持单调递增。

`Server.Close` 与每次状态变更时落盘。客户端侧状态（在途请求）不持久化，
重启后应重新发起请求或重新订阅（服务器端订阅仍存活，通知会继续到达）。

## 测试

```sh
go test ./...        # 全部测试
go test -race ./...  # 竞态检测
```

覆盖：畸形报文拒绝、重传/指数退避/超时清理、重复 CON 幂等、重复与乱序 ACK、
并发乱序响应、令牌复用防护、观察订阅/顺序号缺口/取消/失效订阅者清理、
块传输乱序/重复/不一致中止/丢包重传、去重与订阅的崩溃恢复。

`PipeConfig.Drop` / `PipeConfig.Duplicate` 是注入丢包与重复投递的测试钩子。

## 文件结构

```
codes.go       消息类型、方法/响应码、选项号常量
errors.go      类型化错误（解析、超时、重置、块传输等）
message.go     消息编解码与严格校验
transport.go   Transport 接口 + 内存管道（丢包/重复注入）
dedup.go       去重窗口与快照恢复
client.go      客户端：重传、超时、令牌匹配、并发请求
server.go      服务器：路由、幂等、响应缓存、观察推送、持久化
observe.go     观察订阅（客户端注册表 + 服务端存储）
blockwise.go   Block1/Block2 选项、重组器、客户端大块收发
```
