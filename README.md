# 应用事务协调库（saga 协调器，第一回合）

本地模拟业务步骤之间的执行与补偿协调。

## 边界与分层

- `txn/`：协调库，只负责协调记录、状态机、恢复与补偿顺序。
- 步骤实现：业务方实现 `txn.Step`（可选实现 `txn.Reconciler` 做结果对账）。
- 业务输入：协调记录只保存 `InputRef`（如业务单号），输入内容由业务方在
  `Drive` 时提供，协调库不持久化业务数据。
- 持久化：`FileStore` 以“临时文件 + fsync + rename”原子写入 JSON 记录。

## 步骤状态

`pending -> executing -> succeeded -> compensating -> compensated`，
异常分支还有 `failed` 与 `comp_failed`。

- 进入 `executing` 先落盘，再调用步骤。超时/进程退出后记录停在
  `executing`：请求可能已经生效，恢复时不会盲目重放。
- 恢复 `executing` 步骤时先调用 `Reconcile` 对账：
  - `OutcomeSucceeded`：补记成功，继续后续步骤（成功确认丢失场景）；
  - `OutcomeFailed`：确认未生效，进入补偿；
  - `OutcomeUnknown`：事务保持 `blocked`，不越过该步骤。
- 只对 `succeeded` 的步骤逆序补偿；补偿请求发出前先落盘 `compensating`，
  确认丢失后重启会重复确认补偿，因此 `Compensate` 必须幂等。
- 补偿明确失败记为 `comp_failed`，事务保持 `blocked` 并保留 `LastError`，
  后续 `Drive` 可重试；不会谎称已经全部回滚。

## 扩展新流程

新流程只需：定义业务输入、实现若干 `Step`、用 `NewRegistry` 声明顺序、
注册到 `Coordinator`。恢复机制、存储与状态机完全复用，协调记录通过
`Kind` 在恢复时重新绑定步骤实现。

## 运行

    go test ./...
    go run ./example/order /tmp/txn-data
