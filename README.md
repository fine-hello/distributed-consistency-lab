# Consistency Lab

Go 单进程三节点分布式 KV 模拟器（无需外部依赖）。

## 运行

```sh
go run .
go run . -scenario delay -min-delay 100 -max-delay 500
go run . -scenario conflict
go run . -scenario loss -loss 0.3 -rounds 15
go run . -scenario loss -trials 30 -min-delay 5 -max-delay 25 -out results.csv
go run -race . -min-delay 1 -max-delay 4
```

输出 `result/results.csv`。模式 `arrival` 直接用到达版本覆盖，`version` 按 `(Lamport 时钟, 节点 ID)` 最大值仲裁；后者并不能识别真正因果并发。

网络模型：每条复制消息独立随机抽取延迟和丢失事件，异步发送；接收端使用 ACK 表示消息已处理完成。`wait()` 在写入发送方结束后调用。

修复策略：每轮将各节点快照重新广播（反熵）；当每个节点持有相同版本时视为收敛。`rounds` 仅是实验轮次上限，不保证任意丢包条件都能在时限内收敛。

局限：节点在同一进程中，未模拟崩溃重启与磁盘持久性；没有 Raft，不宣称实现线性一致性；随机种子不能完全控制 goroutine 调度。输出结果为模拟，不应当混淆为真实分布式部署的网络或数据库性能。
