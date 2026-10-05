// demo 用本地虚构动作演示协调过程与争议处理：
// 场景一：扣款响应丢失 -> 查询核实 -> 继续提交。
// 场景二：预留失败 -> 补偿扣款。
// 场景三：互相矛盾的收据 -> 争议立案 -> 裁决 -> 矛盾证据暂停 -> 撤回重审。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"txcoordinator/coordinator"
)

// fakeLedger 是虚构的账务服务，可脚本化"丢响应"。
type fakeLedger struct {
	balance   int
	charged   map[string]int
	loseReply bool
	loseQuery bool
}

func (l *fakeLedger) charge(key string, amount int) coordinator.Op {
	return coordinator.Op{
		Do: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if l.charged == nil {
				l.charged = map[string]int{}
			}
			if _, ok := l.charged[key]; !ok {
				l.balance -= amount
				l.charged[key] = amount
			}
			if l.loseReply {
				return coordinator.Unknown, "", coordinator.ErrUnknownResult
			}
			return coordinator.Success, fmt.Sprintf("charged %d", amount), nil
		},
		Query: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if l.loseQuery {
				l.loseQuery = false
				return coordinator.Unknown, "", coordinator.ErrUnknownResult
			}
			if amt, ok := l.charged[key]; ok {
				return coordinator.Success, fmt.Sprintf("query: charge of %d found", amt), nil
			}
			return coordinator.Failed, "query: no charge found", nil
		},
	}
}

func (l *fakeLedger) refund(key string) coordinator.Op {
	return coordinator.Op{
		Do: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if amt, ok := l.charged[key]; ok {
				l.balance += amt
				delete(l.charged, key)
			}
			return coordinator.Success, "refunded", nil
		},
		Query: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if _, ok := l.charged[key]; !ok {
				return coordinator.Success, "query: charge absent, refund done", nil
			}
			return coordinator.Failed, "query: charge still present", nil
		},
	}
}

// fakeInventory 是虚构的库存服务，可脚本化"预留失败"。
type fakeInventory struct {
	stock    int
	reserved map[string]int
	failNext bool
}

func (inv *fakeInventory) reserve(key string, n int) coordinator.Op {
	return coordinator.Op{
		Do: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if inv.failNext {
				inv.failNext = false
				return coordinator.Failed, "insufficient stock", nil
			}
			if inv.reserved == nil {
				inv.reserved = map[string]int{}
			}
			if _, ok := inv.reserved[key]; !ok {
				inv.stock -= n
				inv.reserved[key] = n
			}
			return coordinator.Success, fmt.Sprintf("reserved %d", n), nil
		},
		Query: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if _, ok := inv.reserved[key]; ok {
				return coordinator.Success, "query: reservation found", nil
			}
			return coordinator.Failed, "query: no reservation", nil
		},
	}
}

func (inv *fakeInventory) release(key string) coordinator.Op {
	return coordinator.Op{
		Do: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if n, ok := inv.reserved[key]; ok {
				inv.stock += n
				delete(inv.reserved, key)
			}
			return coordinator.Success, "released", nil
		},
	}
}

// fakeOrder 是虚构的订单服务（提交步骤）。
type fakeOrder struct{ committed map[string]bool }

func (o *fakeOrder) commit(key string) coordinator.Op {
	return coordinator.Op{
		Do: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if o.committed == nil {
				o.committed = map[string]bool{}
			}
			o.committed[key] = true
			return coordinator.Success, "order committed", nil
		},
		Query: func(ctx context.Context) (coordinator.Outcome, string, error) {
			if o.committed[key] {
				return coordinator.Success, "query: order committed", nil
			}
			return coordinator.Failed, "query: order not committed", nil
		},
	}
}

func orderSteps(ledger *fakeLedger, inv *fakeInventory, order *fakeOrder, key string, amount, n int) []coordinator.Step {
	return []coordinator.Step{
		{Name: "charge", Forward: ledger.charge(key, amount), Compensate: ledger.refund(key)},
		{Name: "reserve", Forward: inv.reserve(key, n), Compensate: inv.release(key)},
		{Name: "commit", Forward: order.commit(key)},
	}
}

func showTask(coord *coordinator.Coordinator, store *coordinator.FileStore, taskID string) {
	rec, err := store.Load(context.Background(), taskID)
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.MarshalIndent(rec, "  ", "  ")
	fmt.Printf("  [任务记录] %s\n", data)
}

func showDispute(ds *coordinator.DisputeStore, id string) {
	d, err := ds.Load(context.Background(), id)
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.MarshalIndent(d, "  ", "  ")
	fmt.Printf("  [争议记录] %s\n", data)
}

func why(format string, args ...any) { fmt.Printf("  -> "+format+"\n", args...) }

func main() {
	ctx := context.Background()
	dir := filepath.Join(os.TempDir(), "txcoordinator-demo")
	os.RemoveAll(dir)
	store, err := coordinator.NewFileStore(filepath.Join(dir, "tasks"))
	if err != nil {
		log.Fatal(err)
	}
	dstore, err := coordinator.NewDisputeStore(filepath.Join(dir, "disputes"))
	if err != nil {
		log.Fatal(err)
	}
	coord := coordinator.NewCoordinator(store)
	dm := coordinator.NewDisputeManager(dstore, coord)
	coord.SetGate(dm.Gate(ctx))

	fmt.Println("=== 场景一：扣款响应丢失，查询核实后继续 ===")
	ledger := &fakeLedger{balance: 1000}
	inv := &fakeInventory{stock: 10}
	order := &fakeOrder{}
	ledger.loseReply = true // 扣款实际成功，但响应丢失
	ledger.loseQuery = true // 第一次查询的响应也丢失
	steps := orderSteps(ledger, inv, order, "order-1", 100, 2)
	err = coord.Run(ctx, "task-1", "order-1", steps)
	why("第一次 Run 返回: %v（扣款响应丢失 -> 查询核实，查询响应也丢失 -> 暂停等待）", err)
	showTask(coord, store, "task-1")
	why("查询确认扣款已发生，因此可以继续；恢复任务后跑完预留与提交")
	ledger.loseReply = false
	if err := coord.Resume(ctx, "task-1"); err != nil {
		log.Fatal(err)
	}
	if err := coord.Run(ctx, "task-1", "order-1", steps); err != nil {
		log.Fatal(err)
	}
	showTask(coord, store, "task-1")
	why("任务 committed：查询证实的效果没有被重复执行（attempts=1）")

	fmt.Println()
	fmt.Println("=== 场景二：预留明确失败，补偿已扣款项 ===")
	ledger2 := &fakeLedger{balance: 1000}
	inv2 := &fakeInventory{stock: 10, failNext: true}
	order2 := &fakeOrder{}
	steps2 := orderSteps(ledger2, inv2, order2, "order-2", 200, 3)
	err = coord.Run(ctx, "task-2", "order-2", steps2)
	why("Run 返回: %v（预留失败 -> 触发补偿 -> compensated）", err)
	showTask(coord, store, "task-2")
	why("扣款已退款（ledger balance=%d），提交步骤从未执行，可以安全等待人工处理", ledger2.balance)

	fmt.Println()
	fmt.Println("=== 场景三：互相矛盾的收据 -> 争议、裁决、矛盾证据暂停、撤回重审 ===")
	ledger3 := &fakeLedger{balance: 1000}
	inv3 := &fakeInventory{stock: 10}
	order3 := &fakeOrder{}
	ledger3.loseReply = true
	ledger3.loseQuery = true // 查询响应也丢失，任务停在 paused 等待核实
	steps3 := orderSteps(ledger3, inv3, order3, "order-3", 300, 1)
	_ = coord.Run(ctx, "task-3", "order-3", steps3)
	why("扣款响应丢失，任务暂停等待核实；此时收到互相矛盾的两张收据，立案争议")
	disp, err := dm.Open(ctx, "task-3", "charge", "扣款结果存在互相矛盾的收据")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, disp.ID, "支付网关收据", "success", "网关回执显示扣款 300 成功"); err != nil {
		log.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, disp.ID, "银行对账单", "failed", "对账单未见该笔扣款"); err != nil {
		log.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, disp.ID, "内部查询接口", "unknown", "查询接口超时，无法确认"); err != nil {
		log.Fatal(err)
	}
	showDispute(dstore, disp.ID)
	why("三条观察共存：分别支持成功、失败、未知；新证据不会覆盖旧的相反证据")

	disp, _ = dstore.Load(ctx, disp.ID)
	basis := []string{disp.Observations[0].ID}
	if _, err := dm.Rule(ctx, disp.ID, "mark_success", []string{"reserve", "commit"}, basis, "operator-A"); err != nil {
		log.Fatal(err)
	}
	why("操作者基于网关收据裁决：动作 mark_success，允许后续 reserve/commit 步骤")
	if _, err := dm.ExecuteAction(ctx, disp.ID, "mark_success"); err != nil {
		log.Fatal(err)
	}
	why("执行裁决动作 mark_success（计入 ExecutedActions，终身只能执行一次）")

	if _, err := dm.AddObservation(ctx, disp.ID, "银行补充对账单", "failed", "补充对账单仍确认未扣款"); err != nil {
		log.Fatal(err)
	}
	showDispute(dstore, disp.ID)
	showTask(coord, store, "task-3")
	why("新证据与裁决基础矛盾：争议与任务被暂停，已发生的效果（扣款、mark_success 记录）全部保留")

	if _, err := dm.ExecuteAction(ctx, disp.ID, "reserve"); err != nil {
		why("暂停期间尝试后续动作被拒: %v（必须等待）", err)
	}
	if _, err := dm.Withdraw(ctx, disp.ID); err != nil {
		log.Fatal(err)
	}
	why("操作者撤回裁决，争议重新审理（reopened）")
	if _, err := dm.Rule(ctx, disp.ID, "mark_success", []string{"reserve"}, []string{disp.Observations[0].ID}, "operator-B"); err != nil {
		log.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, disp.ID, "mark_success"); err != nil {
		why("重新裁决后再次执行 mark_success 被拒: %v（撤回不能让同一业务动作再执行一次）", err)
	}
	showDispute(dstore, disp.ID)
	why("记录说明：矛盾证据未消除前任务保持 paused；恢复需人工 Resume 且门禁放行")

	fmt.Println()
	fmt.Println("=== 场景四：局部补充——已退款后追加 release 许可，独立动作不被连带暂停 ===")
	disp4, err := dm.Open(ctx, "task-4", "charge", "退款后需局部追加释放库存许可")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, disp4.ID, "支付网关", "failed", "扣款明确失败"); err != nil {
		log.Fatal(err)
	}
	disp4, _ = dstore.Load(ctx, disp4.ID)
	if _, err := dm.Rule(ctx, disp4.ID, "refund", []string{"notify"}, []string{disp4.Observations[0].ID}, "operator-A"); err != nil {
		log.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, disp4.ID, "refund"); err != nil {
		log.Fatal(err)
	}
	why("裁决只授权 refund+notify；refund 已发生，release 尚未获准")
	pv, err := dm.AddScopedEvidence(ctx, disp4.ID, "库存系统", "success", "库存预留已核实，可安全释放",
		[]string{"release"}, []string{"release"}, nil, "退款完成，追加释放库存许可", "operator-A")
	if err != nil {
		log.Fatal(err)
	}
	// 把补充证据结论改为可核实的 success（预览确认后成为 release 的已核实基础）。
	why("预览：已发生=%v 获准=%v 受限=%v", pv.Done, pv.Permitted, pv.Restricted)
	if err := dm.CheckAction(ctx, disp4.ID, "release"); err != nil {
		why("确认前 release 被旧裁决拦住: %v", err)
	}
	if _, err := dm.ConfirmSupplement(ctx, pv.ChangeID, "operator-A"); err != nil {
		log.Fatal(err)
	}
	why("确认后 release 获准；重复执行 refund 仍被拒：%v", dm.CheckAction(ctx, disp4.ID, "refund"))

	// 只与 release 矛盾的证据：notify 独立继续。
	if _, err := dm.AddScopedEvidence(ctx, disp4.ID, "仓库", "failed", "释放前提不成立，库存状态冲突",
		[]string{"release"}, nil, nil, "临时 hold release", "operator-B"); err != nil {
		log.Fatal(err)
	}
	why("release: %v（局部暂停）；notify: %v（独立动作继续）",
		dm.CheckAction(ctx, disp4.ID, "release"), dm.CheckAction(ctx, disp4.ID, "notify"))
	showDispute(dstore, disp4.ID)
}
