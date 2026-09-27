# coap-protocol-go

一个纯 Go 实现的本地 CoAP 协议栈（RFC 7252 / RFC 7641 / RFC 7959 子集），
用于在不稳定的数据报网络上承载**请求/响应**与**服务器主动推送**。
测试通过内存管道（`MemHub`）或 UDP 本地回环模拟报文收发，
不依赖真实网络设备或外部服务。

## 功能总览

| 能力 | 实现位置 | 说明 |
| --- | --- | --- |
| 报文编解码与校验 | `coap/message.go` | 版本/TKL/选项/截断/空报文校验，畸形报文明确拒绝 |
| 重传与超时 | `coap/client.go` | CON 指数退避重传，超限返回 `ErrTimeout`，无悬挂状态 |
| 幂等去重 | `coap/dedup.go` | 按（来源，消息编号）去重，缓存响应自动重发 |
| 并发令牌匹配 | `coap/client.go` | 响应按令牌+消息编号匹配，乱序/重复/迟到响应安全 |
| 观察推送 | `coap/observe.go` | Observe 订阅/注销，顺序号可检测乱序与丢失 |
| 块传输 | `coap/blockwise.go` | Block1 上传 / Block2 下载，乱序重组、冲突拒绝 |
| 崩溃恢复 | `coap/persist.go` | 去重记录与订阅关系原子落盘，重启后恢复 |

## 快速开始

```go
hub := coap.NewMemHub()

srv := coap.NewServer(hub.Endpoint("srv"), coap.ServerConfig{},
    coap.HandlerFunc(func(w coap.ResponseWriter, r *coap.Request) {
        w.Write(&coap.Message{Code: coap.Content, Payload: []byte("hello")})
    }))
srv.Start()

cli := coap.NewClient(hub.Endpoint("cli"), coap.ClientConfig{})
resp, err := cli.Do(ctx, "srv", &coap.Message{Code: coap.GET})
```

真实 UDP 回环：用 `coap.ListenUDP("127.0.0.1:0")` 替换 `hub.Endpoint(...)` 即可，
二者实现同一 `PacketConn` 接口。

## 超时与重传参数（ClientConfig）

| 参数 | 默认值 | 语义 |
| --- | --- | --- |
| `AckTimeout` | 2s | 首次重传的基准超时 |
| `AckRandomFactor` | 1.5 | 首次超时随机化区间 `[AckTimeout, AckTimeout*AckRandomFactor)` |
| `MaxRetransmit` | 4 | 最大重传次数（首发不计） |
| `MaxMessageSize` | 1024 | 单报文负载上限，超出自动启用块传输 |

重传间隔按 `timeout *= 2` 指数退避。总尝试次数为 `1 + MaxRetransmit`，
全部超时后 `Do` 返回 `ErrTimeout`，并**保证清理全部在途状态**
（pending 表、MID 索引、定时器），不留悬挂请求。
对端 RST 使请求以 `ErrReset` 结束；`ctx` 取消以 `ctx.Err()` 结束。
收到空 ACK（分离响应预告）后停止重传，继续等待后续 CON 响应。

## 去重窗口（DedupConfig）

| 参数 | 默认值 | 语义 |
| --- | --- | --- |
| `Window` | 247s（EXCHANGE_LIFETIME） | 去重条目保留时长，过期惰性清理 |
| `MaxEntries` | 4096 | 容量上限，超出按 LRU 淘汰最旧条目 |

服务器对每条 CON 请求以（来源地址，消息编号）为键记录处理结果：

- 首次到达：处理并缓存响应；
- 重复到达：**不再执行处理器**，直接重发缓存的响应（ACK）；
- 客户端侧同样对入方向 CON（通知/分离响应）去重，重复副本只补 ACK 不再投递。

配置 `ServerConfig.StateDir` 后，去重记录与观察订阅在每次变更时
原子写入 `<StateDir>/coap-state.json`（临时文件 + rename，崩溃不留半截文件）。
重启后：

- 窗口期内的历史报文重放会被识别为重复，直接重发缓存响应，**不会**再次执行处理器；
- 超过窗口的过期记录与损坏记录被安全丢弃；
- 观察订阅恢复且顺序号连续；若订阅者已失效，其会对通知回 RST，
  服务器随即注销该订阅，**不再向其推送**。

## 并发语义

- `Client.Do` 可任意并发调用。每个请求分配全局唯一令牌
  （64 位计数器 + 随机起点，进程生命周期内不复用）与独立消息编号。
- 响应匹配规则：
  - ACK 捎带响应：按 **令牌 + 消息编号** 双重匹配；
  - CON/NON 分离响应与通知：按 **令牌** 匹配（并回 ACK）。
- 乱序响应：各请求独立匹配，互不影响。
- 重复/迟到响应：无匹配在途请求时静默丢弃；
  令牌被显式复用时，旧交换的迟到 ACK 因消息编号不符被丢弃，
  **不会误配**给复用令牌的新请求。
- 同一令牌并发复用会被拒绝（`ErrTokenInUse`）；串行复用受上述规则保护。
- 观察通知以 `Notification.Seq`（Observe 顺序号）暴露给调用方，
  可据此检测乱序与丢失；客户端对重复顺序号幂等去重。

## 观察（RFC 7641 子集）

```go
obs, err := cli.Observe(ctx, "srv", "/sensors/temp")
for n := range obs.C {
    // n.Seq 单调递增；n.Msg.Payload 为资源新值
}
obs.Cancel(ctx) // 发送 Observe:1 注销，之后不再收到推送
```

服务器侧通过 `srv.Notify(path, payload)` 向该路径全部订阅者推送 CON 通知。
订阅者对通知回 RST 会被服务器视为注销。

## 块传输（RFC 7959 子集）

- 请求负载超过 `MaxMessageSize` 时，客户端自动分块（Block1），
  逐块等待 `2.31 Continue`，末块后取最终响应；
- 响应负载超限时，服务器自动切首块（Block2）并缓存完整响应，
  客户端自动逐块拉取重组；
- 重组器容忍块乱序与重复；同号块内容冲突、非末块未满载、
  末块编号冲突均返回 `ErrBlockCorrupted`，**不会拼出错误内容**；
- 块传输不依赖可靠传输：单块丢失由 CON 重传机制补齐。

## 畸形报文处理

`Decode` 对以下情况返回明确错误（绝不 panic）：
长度不足、版本不符、TKL>8、令牌截断、空报文携带内容、
选项 delta/length 保留值 15、选项扩展字节或值截断、裸 payload marker。
收发循环对解码失败的报文直接丢弃，不影响任何状态机。

## 测试

```sh
go test ./...          # 全部测试
go test ./... -race    # 含竞态检测
```

覆盖：编解码往返与畸形报文、重传退避与超时清理、重复报文幂等、
重复/乱序 ACK、并发令牌匹配与令牌复用、观察推送/注销/RST、
Block1/Block2/乱序重组/冲突拒绝、有损网络块传输、崩溃重启恢复、
UDP 回环端到端。

内存中心 `MemHub` 支持注入丢包（`SetLoss`）、重复（`SetDuplicate`）、
乱序（`SetReorder`）与确定性过滤（`SetFilter`），用于构建可复现的故障场景。

## 目录结构

```
coap/
  message.go    报文编解码与校验
  options.go    选项集与 URI-Path
  transport.go  PacketConn 抽象、MemHub、UDPConn
  dedup.go      幂等去重（LRU + 窗口）
  client.go     客户端：重传、令牌匹配、接收循环
  server.go     服务器：分发、去重、块传输、通知
  observe.go    观察订阅（客户端/服务器两侧）
  blockwise.go  Block1/Block2 与重组器
  persist.go    崩溃恢复状态的原子落盘与加载
```
