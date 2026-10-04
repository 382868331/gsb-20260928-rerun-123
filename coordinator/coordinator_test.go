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

func dmStoreLoad(dm *DisputeManager, ctx context.Context, id string) (*Dispute, error) {
	return dm.store.Load(ctx, id)
}

// 裁决只授权 refund 及后续 release 时，任何未被明确授权的后续动作
// （如查询 shipping）都不得放行；许可查询结果必须与实际执行一致。
func TestRulingOnlyAllowsExplicitlyAuthorizedActions(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, err := dm.Open(ctx, "t5", "charge", "refund then release")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "gateway", "failed", "charge failed"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	basis := []string{d.Observations[0].ID}
	if _, err := dm.Rule(ctx, d.ID, "refund", []string{"release"}, basis, "op"); err != nil {
		t.Fatal(err)
	}

	// 授权范围内的动作：许可与执行一致。
	if err := dm.CheckAction(ctx, d.ID, "refund"); err != nil {
		t.Fatalf("refund permitted check: %v", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "refund"); err != nil {
		t.Fatalf("execute refund: %v", err)
	}
	if err := dm.CheckAction(ctx, d.ID, "release"); err != nil {
		t.Fatalf("release permitted check: %v", err)
	}
	// 范围外的后续动作（含查询 shipping）：许可与执行都拒绝。
	if err := dm.CheckAction(ctx, d.ID, "query_shipping"); !errors.Is(err, ErrActionNotAllowed) {
		t.Fatalf("query_shipping check = %v, want ErrActionNotAllowed", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "query_shipping"); !errors.Is(err, ErrActionNotAllowed) {
		t.Fatalf("execute query_shipping = %v, want ErrActionNotAllowed", err)
	}
	if err := dm.CheckAction(ctx, d.ID, "shipping"); !errors.Is(err, ErrActionNotAllowed) {
		t.Fatalf("shipping check = %v, want ErrActionNotAllowed", err)
	}
	// 已执行的 refund 不能重复执行：许可查询同样拒绝，口径一致。
	if err := dm.CheckAction(ctx, d.ID, "refund"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("duplicate refund check = %v, want ErrActionAlreadyDone", err)
	}

	// 门禁同样拦截范围外步骤，且与争议立案步骤名无关。
	gate := dm.Gate(ctx)
	if err := gate("t5", "release"); err != nil {
		t.Fatalf("gate release: %v", err)
	}
	if err := gate("t5", "shipping"); !errors.Is(err, ErrActionNotAllowed) {
		t.Fatalf("gate shipping = %v, want ErrActionNotAllowed", err)
	}
}

// 裁决撤回或被矛盾新证据暂停后，任何后续动作都不得放行；
// 撤回重裁（重复确认）也不能让已执行的 refund 再执行一次。
// 观察、裁决和已执行动作记录全程保留。
func TestWithdrawnOrPausedRulingBlocksActions(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, err := dm.Open(ctx, "t6", "charge", "refund case")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "gateway", "success", "charge ok"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	basis := []string{d.Observations[0].ID}
	if _, err := dm.Rule(ctx, d.ID, "refund", []string{"release"}, basis, "op"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "refund"); err != nil {
		t.Fatal(err)
	}

	// 矛盾新证据：裁决被暂停，release 等后续动作一律拦截。
	if _, err := dm.AddObservation(ctx, d.ID, "bank", "failed", "no debit seen"); err != nil {
		t.Fatal(err)
	}
	if err := dm.CheckAction(ctx, d.ID, "release"); !errors.Is(err, ErrDisputePaused) {
		t.Fatalf("paused release check = %v, want ErrDisputePaused", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "release"); !errors.Is(err, ErrDisputePaused) {
		t.Fatalf("paused execute release = %v, want ErrDisputePaused", err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	beforeRulings, beforeDone := len(d.Rulings), len(d.ExecutedActions)

	// 撤回裁决：没有生效裁决，任何动作都不放行。
	if _, err := dm.Withdraw(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if err := dm.CheckAction(ctx, d.ID, "release"); !errors.Is(err, ErrNoActiveRuling) {
		t.Fatalf("withdrawn release check = %v, want ErrNoActiveRuling", err)
	}

	// 撤回重裁、重复确认 refund：已执行过的 refund 不得再次执行。
	if _, err := dm.Rule(ctx, d.ID, "refund", []string{"release"}, basis, "op2"); err != nil {
		t.Fatal(err)
	}
	if err := dm.CheckAction(ctx, d.ID, "refund"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("re-rule refund check = %v, want ErrActionAlreadyDone", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "refund"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("re-rule execute refund = %v, want ErrActionAlreadyDone", err)
	}

	// 记录全程保留：观察追加、裁决累计（撤回不删除）、已执行动作不重置。
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if len(d.Observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(d.Observations))
	}
	if len(d.Rulings) != beforeRulings+1 {
		t.Fatalf("rulings = %d, want %d (withdrawn ruling retained)", len(d.Rulings), beforeRulings+1)
	}
	if !d.Rulings[0].Withdrawn {
		t.Fatal("first ruling should be marked withdrawn but retained")
	}
	if len(d.ExecutedActions) != beforeDone || d.ExecutedActions[0] != "refund" {
		t.Fatalf("executed actions = %v, want exactly one refund retained", d.ExecutedActions)
	}
}
