package coordinator

import (
	"context"
	"errors"
	"testing"
)

func okOp(detail string) Op {
	return Op{
		Do:    func(ctx context.Context) (Outcome, string, error) { return Success, detail, nil },
		Query: func(ctx context.Context) (Outcome, string, error) { return Success, "q:" + detail, nil },
	}
}

func newTestEnv(t *testing.T) (*Coordinator, *FileStore, *DisputeManager) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFileStore(dir + "/tasks")
	if err != nil {
		t.Fatal(err)
	}
	dstore, err := NewDisputeStore(dir + "/disputes")
	if err != nil {
		t.Fatal(err)
	}
	coord := NewCoordinator(store)
	dm := NewDisputeManager(dstore, coord)
	coord.SetGate(dm.Gate(context.Background()))
	return coord, store, dm
}

func TestUnknownThenQueryThenCommit(t *testing.T) {
	coord, store, _ := newTestEnv(t)
	ctx := context.Background()
	attempts := 0
	lost := Op{
		Do: func(ctx context.Context) (Outcome, string, error) {
			attempts++
			return Unknown, "", ErrUnknownResult
		},
		Query: func(ctx context.Context) (Outcome, string, error) { return Success, "found", nil },
	}
	steps := []Step{
		{Name: "charge", Forward: lost, Compensate: okOp("refund")},
		{Name: "commit", Forward: okOp("done")},
	}
	if err := coord.Run(ctx, "t1", "k1", steps); err != nil {
		t.Fatalf("run: %v", err)
	}
	rec, _ := store.Load(ctx, "t1")
	if rec.Status != StCommitted {
		t.Fatalf("status = %s", rec.Status)
	}
	if attempts != 1 {
		t.Fatalf("charge executed %d times, want 1 (query confirmed, no retry)", attempts)
	}
}

func TestUnknownUnresolvedPauses(t *testing.T) {
	coord, store, _ := newTestEnv(t)
	ctx := context.Background()
	unknown := Op{
		Do:    func(ctx context.Context) (Outcome, string, error) { return Unknown, "", ErrUnknownResult },
		Query: func(ctx context.Context) (Outcome, string, error) { return Unknown, "", ErrUnknownResult },
	}
	steps := []Step{{Name: "charge", Forward: unknown, Compensate: okOp("refund")}}
	if err := coord.Run(ctx, "t2", "k2", steps); !errors.Is(err, ErrPaused) {
		t.Fatalf("err = %v, want ErrPaused", err)
	}
	rec, _ := store.Load(ctx, "t2")
	if rec.Status != StPaused || rec.Steps[0].Status != StepUnknown {
		t.Fatalf("rec = %+v", rec)
	}
	if err := coord.Run(ctx, "t2", "k2", steps); !errors.Is(err, ErrPaused) {
		t.Fatalf("paused task run = %v, want ErrPaused", err)
	}
}

func TestFailureTriggersCompensation(t *testing.T) {
	coord, store, _ := newTestEnv(t)
	ctx := context.Background()
	refunded := false
	steps := []Step{
		{Name: "charge", Forward: okOp("charged"), Compensate: Op{
			Do: func(ctx context.Context) (Outcome, string, error) { refunded = true; return Success, "refunded", nil },
		}},
		{Name: "reserve", Forward: Op{
			Do: func(ctx context.Context) (Outcome, string, error) { return Failed, "no stock", nil },
		}},
	}
	if err := coord.Run(ctx, "t3", "k3", steps); err != nil {
		t.Fatalf("run: %v", err)
	}
	rec, _ := store.Load(ctx, "t3")
	if rec.Status != StCompensated || !refunded {
		t.Fatalf("rec = %+v refunded=%v", rec, refunded)
	}
	if rec.Steps[0].Status != StepCompensated || rec.Steps[1].Status != StepFailed {
		t.Fatalf("steps = %+v", rec.Steps)
	}
}

func TestDisputeEvidenceRulingAndWithdrawal(t *testing.T) {
	coord, store, dm := newTestEnv(t)
	_ = coord
	ctx := context.Background()
	d, err := dm.Open(ctx, "t4", "charge", "conflicting receipts")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "gateway", "success", "receipt A"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "bank", "failed", "receipt B"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if len(d.Observations) != 2 {
		t.Fatalf("observations = %d, want 2 (contradicting evidence coexists)", len(d.Observations))
	}
	basis := []string{d.Observations[0].ID}
	if _, err := dm.Rule(ctx, d.ID, "mark_success", []string{"commit"}, basis, "op"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "mark_success"); err != nil {
		t.Fatal(err)
	}
	// 矛盾证据 -> 暂停
	if _, err := dm.AddObservation(ctx, d.ID, "bank2", "failed", "still no debit"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if d.Status != DsPaused {
		t.Fatalf("status = %s, want paused", d.Status)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "commit"); !errors.Is(err, ErrDisputePaused) {
		t.Fatalf("execute while paused = %v", err)
	}
	// 撤回 -> 重审 -> 同一动作不能再执行
	if _, err := dm.Withdraw(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.Rule(ctx, d.ID, "mark_success", nil, basis, "op2"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "mark_success"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("re-execute after withdrawal = %v, want ErrActionAlreadyDone", err)
	}
	_ = store
}

func TestRulingGateChecksEveryLaterActionIncludingQuery(t *testing.T) {
	coord, store, dm := newTestEnv(t)
	ctx := context.Background()

	refundCalls := 0
	releaseCalls := 0
	shippingQueryCalls := 0
	steps := []Step{
		{Name: "refund", Forward: Op{
			Do: func(context.Context) (Outcome, string, error) {
				refundCalls++
				return Success, "refunded", nil
			},
		}},
		{Name: "release", Forward: Op{
			Do: func(context.Context) (Outcome, string, error) {
				releaseCalls++
				return Success, "released", nil
			},
		}},
		{Name: "shipping", Forward: Op{
			Do: func(context.Context) (Outcome, string, error) {
				return Unknown, "", ErrUnknownResult
			},
			Query: func(context.Context) (Outcome, string, error) {
				shippingQueryCalls++
				return Unknown, "", ErrUnknownResult
			},
		}},
	}

	now := "2026-10-05T00:00:00Z"
	if _, err := store.Create(ctx, &TaskRecord{
		TaskID:    "t5",
		Key:       "k5",
		Status:    StPaused,
		CreatedAt: now,
		UpdatedAt: now,
		Steps: []StepRecord{
			{Name: "refund", Status: StepSucceeded, Attempts: 1, Result: Success.String(), UpdatedAt: now},
			{Name: "release", Status: StepSucceeded, Attempts: 1, Result: Success.String(), UpdatedAt: now},
			{Name: "shipping", Status: StepUnknown, Attempts: 1, Result: Unknown.String(), UpdatedAt: now},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d, err := dm.Open(ctx, "t5", "refund", "manual payment review")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "support", "success", "refund receipt"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	basis := []string{d.Observations[0].ID}
	ruled, err := dm.Rule(ctx, d.ID, "refund", []string{"release"}, basis, "op")
	if err != nil {
		t.Fatal(err)
	}
	ruled.ExecutedActions = []string{"refund", "release"}
	if err := dm.store.Save(ctx, ruled); err != nil {
		t.Fatal(err)
	}
	d = ruled

	if err := coord.Resume(ctx, "t5"); err != nil {
		t.Fatal(err)
	}
	if err := coord.Run(ctx, "t5", "k5", steps); !errors.Is(err, ErrPaused) {
		t.Fatalf("run unauthorized shipping query = %v, want ErrPaused", err)
	}
	if refundCalls != 0 || releaseCalls != 0 {
		t.Fatalf("confirmed actions were re-executed: refund=%d release=%d", refundCalls, releaseCalls)
	}
	if shippingQueryCalls != 0 {
		t.Fatalf("shipping query executed %d times without authorization", shippingQueryCalls)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "shipping"); !errors.Is(err, ErrActionNotAllowed) {
		t.Fatalf("shipping permission = %v, want ErrActionNotAllowed", err)
	}
	d, err = dm.AddObservation(ctx, d.ID, "bank", "failed", "refund absent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "shipping"); !errors.Is(err, ErrDisputePaused) {
		t.Fatalf("shipping permission while paused = %v, want ErrDisputePaused", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "release"); !errors.Is(err, ErrDisputePaused) {
		t.Fatalf("release while paused = %v, want ErrDisputePaused", err)
	}
	if err := coord.Resume(ctx, "t5"); err != nil {
		t.Fatal(err)
	}
	if err := coord.Run(ctx, "t5", "k5", steps); !errors.Is(err, ErrPaused) {
		t.Fatalf("run paused shipping query = %v, want ErrPaused", err)
	}
	if shippingQueryCalls != 0 {
		t.Fatalf("paused shipping query executed %d times", shippingQueryCalls)
	}

	if _, err := dm.Withdraw(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.Rule(ctx, d.ID, "refund", []string{"release"}, basis, "op2"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "refund"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("refund after re-ruling = %v, want ErrActionAlreadyDone", err)
	}
	if err := coord.Resume(ctx, "t5"); err != nil {
		t.Fatal(err)
	}
	if err := coord.Run(ctx, "t5", "k5", steps); !errors.Is(err, ErrPaused) {
		t.Fatalf("run shipping after re-ruling = %v, want ErrPaused", err)
	}
	if shippingQueryCalls != 0 || refundCalls != 0 || releaseCalls != 0 {
		t.Fatalf("calls after re-ruling: refund=%d release=%d shippingQuery=%d; all known effects must remain skipped", refundCalls, releaseCalls, shippingQueryCalls)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if len(d.Observations) != 2 || len(d.Rulings) != 2 || len(d.ExecutedActions) != 2 {
		t.Fatalf("history not retained: observations=%d rulings=%d executed=%d", len(d.Observations), len(d.Rulings), len(d.ExecutedActions))
	}
	rec, _ := store.Load(ctx, "t5")
	if rec.Status != StPaused || rec.Steps[2].Status != StepUnknown {
		t.Fatalf("record = %+v, want shipping still unknown and task paused", rec)
	}
}

func dmStoreLoad(dm *DisputeManager, ctx context.Context, id string) (*Dispute, error) {
	return dm.store.Load(ctx, id)
}
