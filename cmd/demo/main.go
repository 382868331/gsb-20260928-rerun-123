// 本地模拟演示：扣款/预留/提交/补偿的协调过程，以及争议记录的
// 证据追加、裁决、矛盾暂停、撤回重审与防重复执行。
package main

import (
    "context"
    "fmt"
    "time"

    "txcoordinator/coordinator"
)

var clock = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func now() time.Time {
    clock = clock.Add(time.Second)
    return clock
}

func section(title string) { fmt.Printf("\n========== %s ==========\n", title) }

func printLog(lines []string) {
    for _, l := range lines {
        fmt.Println("  | " + l)
    }
}

// orderSteps 组装一次下单事务：扣款 -> 预留 -> 提交。
func orderSteps(acct *coordinator.AccountService, inv *coordinator.InventoryService, taskID string) []coordinator.Step {
    return []coordinator.Step{
        {
            Name: "扣款",
            Do: func(ctx context.Context) (string, error) {
                return acct.Debit(ctx, "alice", 100, taskID+"-debit")
            },
            Query: func(ctx context.Context, k string) (coordinator.Outcome, error) {
                return acct.QueryDebit(ctx, k)
            },
            Compensate: func(ctx context.Context, k string) error {
                return acct.Refund(ctx, "alice", 100, taskID+"-refund")
            },
            QueryCompensate: func(ctx context.Context, k string) (coordinator.Outcome, error) {
                return acct.QueryRefund(ctx, taskID+"-refund")
            },
        },
        {
            Name: "预留库存",
            Do: func(ctx context.Context) (string, error) {
                return inv.Reserve(ctx, "sku-1", 1, taskID+"-reserve")
            },
            Query: func(ctx context.Context, k string) (coordinator.Outcome, error) {
                return inv.QueryReserve(ctx, k)
            },
            Compensate: func(ctx context.Context, k string) error {
                return inv.Release(ctx, "sku-1", 1, taskID+"-release")
            },
            QueryCompensate: func(ctx context.Context, k string) (coordinator.Outcome, error) {
                return inv.QueryRelease(ctx, taskID+"-release")
            },
        },
        {
            Name: "提交出库",
            Do: func(ctx context.Context) (string, error) {
                return inv.Commit(ctx, taskID+"-reserve")
            },
            Query: func(ctx context.Context, k string) (coordinator.Outcome, error) {
                return inv.QueryReserve(ctx, k)
            },
            Compensate: func(ctx context.Context, k string) error { return nil },
            QueryCompensate: func(ctx context.Context, k string) (coordinator.Outcome, error) {
                return coordinator.Success, nil
            },
        },
    }
}

func main() {
    ctx := context.Background()

    section("场景一：正常下单（扣款 -> 预留 -> 提交）")
    store, _ := coordinator.NewFileStore("/tmp/txdemo-store")
    acct := coordinator.NewAccountService(map[string]int{"alice": 500}, nil)
    inv := coordinator.NewInventoryService(map[string]int{"sku-1": 3}, nil)
    c := coordinator.NewCoordinator(store)
    if err := c.Run(ctx, "task-ok", "order-1", orderSteps(acct, inv, "task-ok")); err != nil {
        fmt.Println("任务结束：", err)
    }
    printLog(c.Log)
    fmt.Printf("  结果：余额=%d 库存=%d\n", acct.Balances["alice"], inv.Stock["sku-1"])

    section("场景二：扣款响应丢失 -> 先查询核实再继续")
    store2, _ := coordinator.NewFileStore("/tmp/txdemo-store2")
    flaky := &coordinator.Transport{LoseResponse: func(op string) bool { return op == "debit" }}
    acct2 := coordinator.NewAccountService(map[string]int{"alice": 500}, flaky)
    inv2 := coordinator.NewInventoryService(map[string]int{"sku-1": 3}, nil)
    c2 := coordinator.NewCoordinator(store2)
    if err := c2.Run(ctx, "task-lost", "order-2", orderSteps(acct2, inv2, "task-lost")); err != nil {
        fmt.Println("任务结束：", err)
    }
    printLog(c2.Log)
    fmt.Printf("  结果：余额=%d（只扣了一次）库存=%d\n", acct2.Balances["alice"], inv2.Stock["sku-1"])

    section("场景三：预留失败 -> 补偿已扣款项")
    store3, _ := coordinator.NewFileStore("/tmp/txdemo-store3")
    acct3 := coordinator.NewAccountService(map[string]int{"alice": 500}, nil)
    inv3 := coordinator.NewInventoryService(map[string]int{"sku-1": 0}, nil) // 无库存
    c3 := coordinator.NewCoordinator(store3)
    if err := c3.Run(ctx, "task-fail", "order-3", orderSteps(acct3, inv3, "task-fail")); err != nil {
        fmt.Println("任务结束：", err)
    }
    printLog(c3.Log)
    fmt.Printf("  结果：余额=%d（已退款）库存=%d\n", acct3.Balances["alice"], inv3.Stock["sku-1"])

    section("场景四：争议记录 —— 矛盾收据、裁决、途中重开")
    d := coordinator.NewDispute("dispute-1", "task-lost", "扣款 task-lost-debit", now())
    d.AddObservation(now(), "obs-1", "银行回单", coordinator.ObsSuccess, "回单显示扣款 100 成功")
    d.AddObservation(now(), "obs-2", "支付网关", coordinator.ObsFailure, "网关收据显示交易被拒绝")
    d.AddObservation(now(), "obs-3", "对账任务", coordinator.ObsUnknown, "对账文件缺失，无法确认")
    fmt.Println("三条观察并存（成功/失败/未知），互不移除：")
    for _, o := range d.Observations {
        fmt.Printf("  - %s [%s] %s\n", o.ID, o.Verdict, o.Detail)
    }

    err := d.Rule(now(), "ruling-1", coordinator.ObsSuccess, "close_task",
        []string{"notify_user"}, []string{"obs-1", "obs-3"}, "银行回单为资金侧权威证据")
    fmt.Println("裁决：", err)
    fmt.Println("执行后续动作 notify_user：", d.Execute(now(), "notify_user"))

    d.AddObservation(now(), "obs-4", "银行冲正通知", coordinator.ObsFailure, "银行随后发来冲正记录")
    fmt.Println("矛盾证据到达后状态：", d.Status, "|", d.PauseNote)
    fmt.Println("暂停期间尝试执行裁决动作 close_task：", d.Execute(now(), "close_task"))

    fmt.Println("撤回裁决重审：", d.WithdrawRuling(now(), "出现冲正证据，需重新审理"))
    err = d.Rule(now(), "ruling-2", coordinator.ObsFailure, "refund",
        []string{"notify_user"}, []string{"obs-2", "obs-4"}, "冲正记录证明扣款未成立")
    fmt.Println("重新裁决：", err)
    fmt.Println("执行新裁决动作 refund：", d.Execute(now(), "refund"))
    fmt.Println("再次执行 refund（防重）：", d.Execute(now(), "refund"))
    fmt.Println("撤回后重试已执行的 refund：")
    _ = d.WithdrawRuling(now(), "演示：撤回不能重置已执行动作")
    _ = d.Rule(now(), "ruling-3", coordinator.ObsFailure, "refund", nil, []string{"obs-4"}, "重审仍认定失败")
    fmt.Println("  ", d.Execute(now(), "refund"))

    fmt.Println("\n争议处理全程记录：")
    printLog(d.Log)
}
