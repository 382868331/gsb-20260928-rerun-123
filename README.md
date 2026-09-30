# 应用事务协调库（Saga 协调者）

一个可执行的 Python 事务协调库，解决“分布式业务步骤 + 结果丢失 + 失败补偿”问题。

## 解决的核心问题

1. **步骤超时 ≠ 失败**：应答丢失时业务可能已经生效。协调者先把步骤记为
   UNKNOWN、事务记为 BLOCKED 并退出，恢复时必须调用查询接口确认真实状态，
   绝不盲目重做或补偿。
2. **按真实生效关系补偿**：只有确认 COMMITTED 的步骤才逆序补偿；
   PENDING / FAILED（确认未生效）跳过；UNKNOWN 暂停补偿先消解。
3. **补偿失败保留停点**：补偿报错记 COMPENSATION_FAILED，补偿应答丢失记
   COMPENSATING，重新创建协调者后从停点重试，不跳过、不重复改变业务状态
   （业务动作按 tx_id 幂等）。
4. **真实记录接续与对清**：每次状态迁移先以原子 JSON 落盘；协调者退出后
   重新 Coordinator(tx_id, steps, store) 即按磁盘记录接续，
   reconcile() 输出每一步实际做了什么。

## 步骤状态

PENDING -> UNKNOWN（查询确认）-> COMMITTED / PENDING（确认未生效，可安全重做）

补偿链：COMMITTED -> COMPENSATING -> COMPENSATED；
失败停点：COMPENSATION_FAILED（可重试）；同步报错未生效：FAILED。

## 运行

    python app_demo.py        # 端到端演示：结果丢失→退出→重建接续→补偿失败重试
    python -m pytest -q       # 8 个场景测试

## 结构

- txcoordinator/store.py：原子 JSON 协调记录存储。
- txcoordinator/coordinator.py：Coordinator / Step / 状态机与查询确认、补偿。
- services/virtual_business.py：虚拟扣款（钱包）、预留+提交（库存）、下单服务，
  全部按 tx_id 幂等，可注入应答丢失与补偿抖动。
- app_demo.py：扣款应答丢失、协调者退出、重建确认、订单失败、补偿重试的完整演练。
- tests/test_coordinator.py：丢失确认、阻塞禁止重做、补偿幂等、停点重试等场景。
