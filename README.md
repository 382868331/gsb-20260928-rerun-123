# txcoord — 应用事务协调库

以 Saga 方式协调一组业务步骤（示例：虚拟扣款 → 预留 → 提交），并把每一步的
**已确认效果 / 明确失败 / 结果未知** 持久化到协调记录中。

## 核心语义

- **持久协调记录**：每次状态迁移先落库（SQLite）再继续，重建协调者后从已有事实接续，
  不会从第一步重做（`Coordinator(Store(path))` + `resume` / `recover_all`）。
- **结果未知不等于没发生**：动作抛出 `OutcomeUnknown`（超时/响应丢失）时记录为
  `UNKNOWN`，优先用 `Step.query` 查询确认；查不清则任务进入 `NEEDS_ATTENTION`，
  绝不盲目重复执行。
- **约定补偿**：后续步骤失败时按逆序补偿已确认步骤；补偿本身失败或响应丢失分别记为
  `COMPENSATION_FAILED` / `COMPENSATION_UNKNOWN`，可用 `query_compensation` 或
  操作者 `confirm_step` 确认后继续。
- **操作者选择**：`pause(key)` 暂缓收尾、`resume(key, steps)` 恢复处理，任务身份
  （key 与记录）保持不变。
- **幂等并发**：同一 key 的并发/重复请求共享同一个任务与同一组业务效果
  （唯一约束 + 每任务锁）。

## 使用

    from txcoord import Coordinator, Store, Step
    coord = Coordinator(Store('coord.db'))
    task = coord.run('order-123', steps, ctx)   # steps: [虚拟扣款, 预留, 提交]

每个 `Step` 可声明 `action` / `compensate` / `query` / `query_compensation`。
完整业务示例见 `examples/payment_flow.py`。

## 测试

    python -m pytest tests -q
