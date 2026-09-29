package txn

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type fakeStep struct {
	kind       string
	execCalls  int
	compCalls  int
	exec       func(sc StepContext) (Result, error)
	comp       func(sc StepContext, r Result) error
	reconciles []Outcome
}

func (f *fakeStep) Kind() string { return f.kind }

func (f *fakeStep) Execute(sc StepContext) (Result, error) {
	f.execCalls++
	return f.exec(sc)
}

func (f *fakeStep) Compensate(sc StepContext, r Result) error {
	f.compCalls++
	return f.comp(sc, r)
}

func (f *fakeStep) Reconcile(StepContext) (Outcome, error) {
	if len(f.reconciles) == 0 {
		return OutcomeUnknown, nil
	}
	o := f.reconciles[0]
	f.reconciles = f.reconciles[1:]
	return o, nil
}

func newTestCoordinator(t *testing.T, steps ...Step) (*Coordinator, *FileStore) {
	t.Helper()
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]string, 0, len(steps))
	for _, s := range steps {
		kinds = append(kinds, s.Kind())
	}
	reg := NewRegistry(kinds...)
	for _, s := range steps {
		reg.Register(s)
	}
	return NewCoordinator(store, Flow{Name: "test", Registry: reg}), store
}

var okStep = func(StepContext) (Result, error) {
	return Result{Ref: "done"}, nil
}

func TestDriveCommitsAllSteps(t *testing.T) {
	a := &fakeStep{kind: "a", exec: okStep, comp: func(StepContext, Result) error { return nil }}
	b := &fakeStep{kind: "b", exec: okStep, comp: func(StepContext, Result) error { return nil }}
	c, _ := newTestCoordinator(t, a, b)
	ctx := context.Background()
	if _, err := c.Begin(ctx, "test", "t1", Input{Ref: "biz-1"}); err != nil {
		t.Fatal(err)
	}
	rec, err := c.Drive(ctx, "t1", Input{Ref: "biz-1"})
	if err != nil {
		t.Fatalf("Drive: %v", err)
	}
	if rec.Status != TxnCommitted {
		t.Fatalf("status = %s", rec.Status)
	}
	if a.execCalls != 1 || b.execCalls != 1 {
		t.Fatalf("exec calls = %d,%d", a.execCalls, b.execCalls)
	}
}

func TestFailureCompensatesSucceededStepsInReverse(t *testing.T) {
	var mu sync.Mutex
	order := []string{}
	a := &fakeStep{
		kind: "a", exec: okStep,
		comp: func(StepContext, Result) error { mu.Lock(); order = append(order, "a"); mu.Unlock(); return nil },
	}
	b := &fakeStep{
		kind: "b", exec: okStep,
		comp: func(StepContext, Result) error { mu.Lock(); order = append(order, "b"); mu.Unlock(); return nil },
	}
	fail := &fakeStep{
		kind: "c",
		exec: func(StepContext) (Result, error) { return Result{}, errors.New("business reject") },
		comp: func(StepContext, Result) error { return nil },
	}
	c, _ := newTestCoordinator(t, a, b, fail)
	ctx := context.Background()
	_, _ = c.Begin(ctx, "test", "t2", Input{Ref: "biz-2"})
	rec, err := c.Drive(ctx, "t2", Input{Ref: "biz-2"})
	if err != nil {
		t.Fatalf("Drive: %v", err)
	}
	if rec.Status != TxnCompensated {
		t.Fatalf("status = %s, reason = %s", rec.Status, rec.Reason)
	}
	if got := []string{order[0], order[1]}; got[0] != "b" || got[1] != "a" {
		t.Fatalf("compensation order = %v", order)
	}
	if fail.compCalls != 0 {
		t.Fatalf("failed step should never be compensated, calls=%d", fail.compCalls)
	}
}

func TestLostSuccessConfirmationIsRecoveredWithoutReexecute(t *testing.T) {
	a := &fakeStep{
		kind: "a",
		exec: func(StepContext) (Result, error) {
			// 下游已生效，但成功响应丢失（调用方看到超时）。
			return Result{}, ErrUncertain
		},
		comp:       func(StepContext, Result) error { return nil },
		reconciles: []Outcome{OutcomeUnknown, OutcomeSucceeded},
	}
	b := &fakeStep{kind: "b", exec: okStep, comp: func(StepContext, Result) error { return nil }}
	c, store := newTestCoordinator(t, a, b)
	ctx := context.Background()
	_, _ = c.Begin(ctx, "test", "t3", Input{Ref: "biz-3"})

	_, err := c.Drive(ctx, "t3", Input{Ref: "biz-3"})
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected blocked, got %v", err)
	}
	rec, _ := c.Load(ctx, "t3")
	if rec.Status != TxnBlocked || rec.BlockedAt != "t3-a" || rec.Steps[0].Status != StepExecuting {
		t.Fatalf("record did not preserve unknown state: %+v", rec)
	}
	if rec.Steps[1].Status != StepPending {
		t.Fatal("later step must not run while a is undetermined")
	}

	// 模拟进程重启：新建协调器，只依赖持久记录恢复。
	c2 := NewCoordinator(store, Flow{Name: "test", Registry: NewRegistry("a", "b").Register(a).Register(b)})
	_, err = c2.Drive(ctx, "t3", Input{Ref: "biz-3"})
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("unknown reconcile should remain blocked, got %v", err)
	}
	rec, err = c2.Drive(ctx, "t3", Input{Ref: "biz-3"})
	if err != nil {
		t.Fatalf("confirmed success should resume: %v", err)
	}
	if rec.Status != TxnCommitted {
		t.Fatalf("status = %s", rec.Status)
	}
	if a.execCalls != 1 {
		t.Fatalf("uncertain step must not be blindly re-executed, calls=%d", a.execCalls)
	}
}

func TestUncertainStepConfirmedFailedStartsCompensation(t *testing.T) {
	a := &fakeStep{
		kind:       "a",
		exec:       func(StepContext) (Result, error) { return Result{Ref: "applied"}, ErrUncertain },
		comp:       func(StepContext, Result) error { return nil },
		reconciles: []Outcome{OutcomeFailed},
	}
	c, _ := newTestCoordinator(t, a)
	ctx := context.Background()
	_, _ = c.Begin(ctx, "test", "t4", Input{Ref: "biz-4"})
	_, _ = c.Drive(ctx, "t4", Input{Ref: "biz-4"})
	rec, err := c.Drive(ctx, "t4", Input{Ref: "biz-4"})
	if err != nil {
		t.Fatalf("Drive: %v", err)
	}
	if rec.Status != TxnCompensated {
		t.Fatalf("confirmed-not-applied should finish compensation, got %s", rec.Status)
	}
	if a.compCalls != 0 {
		t.Fatalf("a was confirmed failed and had no durable result, comp calls=%d", a.compCalls)
	}
}

func TestCompensationFailureIsRetainedAndCanBeConfirmedAgain(t *testing.T) {
	var mu sync.Mutex
	failures := 2
	a := &fakeStep{
		kind: "a", exec: okStep,
		comp: func(StepContext, Result) error {
			mu.Lock()
			defer mu.Unlock()
			if failures > 0 {
				failures--
				return errors.New("compensation endpoint unavailable")
			}
			return nil
		},
	}
	fail := &fakeStep{
		kind: "b",
		exec: func(StepContext) (Result, error) { return Result{}, errors.New("boom") },
		comp: func(StepContext, Result) error { return nil },
	}
	c, _ := newTestCoordinator(t, a, fail)
	ctx := context.Background()
	_, _ = c.Begin(ctx, "test", "t5", Input{Ref: "biz-5"})

	_, err := c.Drive(ctx, "t5", Input{Ref: "biz-5"})
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("comp failure should block, got %v", err)
	}
	rec, _ := c.Load(ctx, "t5")
	if rec.Status != TxnBlocked || rec.Steps[0].Status != StepCompFailed {
		t.Fatalf("comp failure state not retained: %+v / %s", rec.Steps[0].Status, rec.Status)
	}

	// 第一次重试仍失败；状态保留，不能宣称已经回到原点。
	_, err = c.Drive(ctx, "t5", Input{Ref: "biz-5"})
	if !errors.Is(err, ErrBlocked) || a.compCalls != 2 {
		t.Fatalf("retry one: err=%v calls=%d", err, a.compCalls)
	}
	// 第二次重试补偿确认成功，幂等的补偿被重复确认。
	rec, err = c.Drive(ctx, "t5", Input{Ref: "biz-5"})
	if err != nil {
		t.Fatalf("retry two: %v", err)
	}
	if rec.Status != TxnCompensated || rec.Steps[0].Status != StepCompensated {
		t.Fatalf("status = %s step = %s", rec.Status, rec.Steps[0].Status)
	}
}
