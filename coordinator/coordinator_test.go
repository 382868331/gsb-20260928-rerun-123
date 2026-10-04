package coordinator

import (
    "context"
    "errors"
    "testing"
    "time"
)

func testNow() time.Time { return time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC) }

func TestUnknownIsQueriedNotRetried(t *testing.T) {
    dir := t.TempDir()
    store, _ := NewFileStore(dir)
    acct := NewAccountService(map[string]int{"a": 100}, &Transport{LoseResponse: func(op string) bool { return op == "debit" }})
    calls := 0
    steps := []Step{{
        Name: "扣款",
        Do: func(ctx context.Context) (string, error) {
            calls++
            return acct.Debit(ctx, "a", 30, "t1-debit")
        },
        Query: func(ctx context.Context, k string) (Outcome, error) { return acct.QueryDebit(ctx, k) },
    }}
    c := NewCoordinator(store)
    if err := c.Run(context.Background(), "t1", "k1", steps); err != nil {
        t.Fatal(err)
    }
    if calls != 1 {
        t.Fatalf("unknown 结果不得重试，Do 调用 %d 次", calls)
    }
    if acct.Balances["a"] != 70 {
        t.Fatalf("余额应为 70，实际 %d", acct.Balances["a"])
    }
}

func TestFailureTriggersCompensation(t *testing.T) {
    store, _ := NewFileStore(t.TempDir())
    acct := NewAccountService(map[string]int{"a": 100}, nil)
    steps := []Step{
        {
            Name:        "扣款",
            Do:          func(ctx context.Context) (string, error) { return acct.Debit(ctx, "a", 30, "t2-debit") },
            Query:       func(ctx context.Context, k string) (Outcome, error) { return acct.QueryDebit(ctx, k) },
            Compensate:  func(ctx context.Context, k string) error { return acct.Refund(ctx, "a", 30, "t2-refund") },
            QueryCompensate: func(ctx context.Context, k string) (Outcome, error) { return acct.QueryRefund(ctx, "t2-refund") },
        },
        {
            Name: "预留",
            Do:   func(ctx context.Context) (string, error) { return "", errors.New("out of stock") },
        },
    }
    c := NewCoordinator(store)
    if err := c.Run(context.Background(), "t2", "k2", steps); err != nil {
        t.Fatal(err)
    }
    if acct.Balances["a"] != 100 {
        t.Fatalf("补偿后余额应恢复 100，实际 %d", acct.Balances["a"])
    }
    rec, _ := store.Load(context.Background(), "t2")
    if rec.Status != StCompensated {
        t.Fatalf("任务状态应为 compensated，实际 %s", rec.Status)
    }
}

func TestDisputeEvidenceAndRulingRules(t *testing.T) {
    d := NewDispute("d1", "t1", "扣款 x", testNow())
    d.AddObservation(testNow(), "o1", "银行", ObsSuccess, "回单成功")
    d.AddObservation(testNow(), "o2", "网关", ObsFailure, "收据拒绝")
    if len(d.Observations) != 2 {
        t.Fatal("相反证据必须并存，不得覆盖")
    }
    if err := d.Rule(testNow(), "r1", ObsSuccess, "close", []string{"notify"}, []string{"o1"}, "资金侧权威"); err != nil {
        t.Fatal(err)
    }
    if err := d.Rule(testNow(), "r2", ObsFailure, "refund", nil, []string{"o2"}, "重复裁决"); !errors.Is(err, ErrRulingActive) {
        t.Fatal("已有生效裁决时不得再次裁决")
    }
    d.AddObservation(testNow(), "o3", "冲正", ObsFailure, "冲正记录")
    if d.Status != DisputePaused {
        t.Fatal("矛盾证据必须暂停后续动作")
    }
    if err := d.Execute(testNow(), "close"); !errors.Is(err, ErrDisputePaused) {
        t.Fatal("暂停期间不得执行动作")
    }
    if err := d.WithdrawRuling(testNow(), "重审"); err != nil {
        t.Fatal(err)
    }
    if err := d.Rule(testNow(), "r3", ObsFailure, "refund", nil, []string{"o2", "o3"}, "冲正为准"); err != nil {
        t.Fatal(err)
    }
    if err := d.Execute(testNow(), "refund"); err != nil {
        t.Fatal(err)
    }
    _ = d.WithdrawRuling(testNow(), "再次重审")
    _ = d.Rule(testNow(), "r4", ObsFailure, "refund", nil, []string{"o3"}, "仍认定失败")
    if err := d.Execute(testNow(), "refund"); !errors.Is(err, ErrActionDone) {
        t.Fatal("撤回裁决不得允许重复执行同一业务动作")
    }
}
