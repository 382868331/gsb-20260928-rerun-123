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

// 已退款后，操作者提交新证据追加 release 许可：
// 预览显示 refund 已执行、release 可新增；确认后 release 可执行，
// 已执行的 refund 终身不能重复执行。
func TestSupplementAddsReleaseAfterRefund(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, err := dm.Open(ctx, "s1", "charge", "refund first")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "gateway", "failed", "charge failed"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	basis := []string{d.Observations[0].ID}
	if _, err := dm.Rule(ctx, d.ID, "refund", nil, basis, "op"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "refund"); err != nil {
		t.Fatal(err)
	}

	// 新证据支持追加 release 许可。
	if _, err := dm.AddObservation(ctx, d.ID, "warehouse", "failed", "goods never shipped"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	newBasis := []string{d.Observations[1].ID}

	// 预览：refund 已执行，release 可新增，范围仍受旧裁决约束。
	p, err := dm.PreviewSupplement(ctx, d.ID, []string{"release"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.ExecutedActions) != 1 || p.ExecutedActions[0] != "refund" {
		t.Fatalf("preview executed = %v", p.ExecutedActions)
	}
	if len(p.AddEffective) != 1 || p.AddEffective[0] != "release" {
		t.Fatalf("preview add = %v", p.AddEffective)
	}
	if len(p.StillUnderRuling) != 2 { // refund + release
		t.Fatalf("preview still under ruling = %v", p.StillUnderRuling)
	}
	// 预览已执行的 refund 再次请求增加许可时被标记为不可再执行。
	p2, _ := dm.PreviewSupplement(ctx, d.ID, []string{"refund"}, nil)
	if len(p2.AddAlreadyExecuted) != 1 || p2.AddAlreadyExecuted[0] != "refund" {
		t.Fatalf("preview add-already-executed = %v", p2.AddAlreadyExecuted)
	}

	// 确认后 release 获准并可执行；refund 仍不可重复。
	if _, err := dm.ConfirmSupplement(ctx, d.ID, SupplementRequest{
		Add: []string{"release"}, Basis: newBasis, Reason: "仓库证实未发货，追加释放许可", By: "op",
		ExpectRulingVersion: p.RulingVersion, ExpectEvidenceVersion: p.EvidenceVersion,
	}); err != nil {
		t.Fatal(err)
	}
	if err := dm.CheckAction(ctx, d.ID, "release"); err != nil {
		t.Fatalf("release check after supplement: %v", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "release"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "refund"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("duplicate refund = %v, want ErrActionAlreadyDone", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "release"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("duplicate release = %v, want ErrActionAlreadyDone", err)
	}
	// 历史记录可解释：rule 与 supplement 各一条。
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if len(d.History) != 2 || d.History[0].Kind != "rule" || d.History[1].Kind != "supplement" {
		t.Fatalf("history = %+v", d.History)
	}
	if got := d.ExplainAction("refund"); got == "" {
		t.Fatal("ExplainAction(refund) empty")
	}
}

// 新证据只暂停与其矛盾的范围：基于独立证据补充的已核实动作继续放行。
func TestContradictionPausesOnlyConflictingScope(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, err := dm.Open(ctx, "s2", "charge", "independent scopes")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "gateway", "success", "charge ok"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "warehouse", "success", "goods returned"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	obsCharge, obsGoods := d.Observations[0].ID, d.Observations[1].ID
	if _, err := dm.Rule(ctx, d.ID, "mark_success", []string{"commit"}, []string{obsCharge}, "op"); err != nil {
		t.Fatal(err)
	}
	// 基于独立证据补充 release 许可。
	p, _ := dm.PreviewSupplement(ctx, d.ID, []string{"release"}, nil)
	if _, err := dm.ConfirmSupplement(ctx, d.ID, SupplementRequest{
		Add: []string{"release"}, Basis: []string{obsGoods}, Reason: "退货已入库", By: "op2",
		ExpectRulingVersion: p.RulingVersion, ExpectEvidenceVersion: p.EvidenceVersion,
	}); err != nil {
		t.Fatal(err)
	}
	// 与裁决基础矛盾的新证据（只涉及扣款相关动作）：只暂停 mark_success/commit，release 不受影响。
	if _, err := dm.AddObservation(ctx, d.ID, "bank", "failed", "no debit", "mark_success", "commit"); err != nil {
		t.Fatal(err)
	}
	if err := dm.CheckAction(ctx, d.ID, "commit"); !errors.Is(err, ErrDisputePaused) {
		t.Fatalf("commit check = %v, want ErrDisputePaused", err)
	}
	if err := dm.CheckAction(ctx, d.ID, "release"); err != nil {
		t.Fatalf("independent release blocked: %v", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "release"); err != nil {
		t.Fatalf("independent release execute: %v", err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if len(d.Pauses) != 2 { // mark_success + commit
		t.Fatalf("pauses = %+v", d.Pauses)
	}
}

// 两名操作者并行补充：确认时核对裁决与证据版本，
// 过期决定不能覆盖后来形成的限制；证据撤回使迟到确认失效。
func TestParallelSupplementsStaleVersionRejected(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, err := dm.Open(ctx, "s3", "charge", "parallel supplements")
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

	// 操作者 A、B 基于同一版本各自预览。
	pa, _ := dm.PreviewSupplement(ctx, d.ID, []string{"ship"}, nil)
	pb, _ := dm.PreviewSupplement(ctx, d.ID, nil, []string{"release"})
	if pa.RulingVersion != pb.RulingVersion || pa.EvidenceVersion != pb.EvidenceVersion {
		t.Fatal("previews should share versions")
	}
	// A 先确认（缩小范围：无），随后 B 的迟到确认必须被拒绝。
	if _, err := dm.AddObservation(ctx, d.ID, "bank", "failed", "confirmed no debit"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ConfirmSupplement(ctx, d.ID, SupplementRequest{
		Add: []string{"ship"}, Basis: basis, Reason: "A 追加发货", By: "opA",
		ExpectRulingVersion: pa.RulingVersion, ExpectEvidenceVersion: pa.EvidenceVersion,
	}); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("A confirm after new evidence = %v, want ErrStaleVersion", err)
	}
	// A 重新预览后确认成功。
	pa2, _ := dm.PreviewSupplement(ctx, d.ID, []string{"ship"}, nil)
	if _, err := dm.ConfirmSupplement(ctx, d.ID, SupplementRequest{
		Add: []string{"ship"}, Basis: basis, Reason: "A 追加发货", By: "opA",
		ExpectRulingVersion: pa2.RulingVersion, ExpectEvidenceVersion: pa2.EvidenceVersion,
	}); err != nil {
		t.Fatal(err)
	}
	// B 仍持旧版本，其收回 release 的过期决定不得生效。
	if _, err := dm.ConfirmSupplement(ctx, d.ID, SupplementRequest{
		Remove: []string{"release"}, Basis: basis, Reason: "B 收回释放", By: "opB",
		ExpectRulingVersion: pb.RulingVersion, ExpectEvidenceVersion: pb.EvidenceVersion,
	}); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("B stale confirm = %v, want ErrStaleVersion", err)
	}
	if err := dm.CheckAction(ctx, d.ID, "release"); err != nil {
		t.Fatalf("stale remove took effect, release blocked: %v", err)
	}

	// 证据撤回与迟到确认交错：撤回使证据版本递增，旧版本确认失效。
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	late, _ := dm.PreviewSupplement(ctx, d.ID, []string{"ship2"}, nil)
	if _, err := dm.WithdrawObservation(ctx, d.ID, d.Observations[1].ID, "对账单系误传"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ConfirmSupplement(ctx, d.ID, SupplementRequest{
		Add: []string{"ship2"}, Basis: basis, Reason: "迟到确认", By: "opC",
		ExpectRulingVersion: late.RulingVersion, ExpectEvidenceVersion: late.EvidenceVersion,
	}); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("late confirm after withdrawal = %v, want ErrStaleVersion", err)
	}
	// 撤回的证据不能再作为补充依据。
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	fresh, _ := dm.PreviewSupplement(ctx, d.ID, []string{"ship2"}, nil)
	if _, err := dm.ConfirmSupplement(ctx, d.ID, SupplementRequest{
		Add: []string{"ship2"}, Basis: []string{d.Observations[1].ID}, Reason: "用已撤回证据", By: "opC",
		ExpectRulingVersion: fresh.RulingVersion, ExpectEvidenceVersion: fresh.EvidenceVersion,
	}); err == nil {
		t.Fatal("confirm on withdrawn evidence should fail")
	}
	// 撤回记录保留，历史含 withdraw_evidence 事件。
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if !d.Observations[1].Withdrawn || len(d.Observations) != 2 {
		t.Fatalf("observations = %+v", d.Observations)
	}
	kinds := map[string]bool{}
	for _, e := range d.History {
		kinds[e.Kind] = true
	}
	if !kinds["supplement"] || !kinds["withdraw_evidence"] {
		t.Fatalf("history kinds = %v", kinds)
	}
}

// 证据撤回解除由它单独导致的暂停，范围恢复后动作可执行；
// 已执行动作在任何交错下都不会重复执行。
func TestWithdrawEvidenceUnpausesInterleaved(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, err := dm.Open(ctx, "s4", "charge", "withdraw evidence")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.AddObservation(ctx, d.ID, "gateway", "success", "charge ok"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	basis := []string{d.Observations[0].ID}
	if _, err := dm.Rule(ctx, d.ID, "mark_success", []string{"commit"}, basis, "op"); err != nil {
		t.Fatal(err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "mark_success"); err != nil {
		t.Fatal(err)
	}
	// 矛盾证据暂停 commit。
	if _, err := dm.AddObservation(ctx, d.ID, "bank", "failed", "no debit"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if err := dm.CheckAction(ctx, d.ID, "commit"); !errors.Is(err, ErrDisputePaused) {
		t.Fatalf("commit paused check = %v", err)
	}
	// 撤回矛盾证据：commit 范围恢复；已执行的 mark_success 仍不可重复。
	if _, err := dm.WithdrawObservation(ctx, d.ID, d.Observations[1].ID, "银行确认对账单有误"); err != nil {
		t.Fatal(err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if d.Status != DsRuled || len(d.Pauses) != 0 {
		t.Fatalf("status=%s pauses=%+v", d.Status, d.Pauses)
	}
	if err := dm.CheckAction(ctx, d.ID, "commit"); err != nil {
		t.Fatalf("commit after unpause: %v", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "mark_success"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("re-run mark_success = %v, want ErrActionAlreadyDone", err)
	}
	// 历史含 pause 与 unpause，可解释当时为何被拒、现在为何获准。
	kinds := map[string]bool{}
	for _, e := range d.History {
		kinds[e.Kind] = true
	}
	if !kinds["pause"] || !kinds["unpause"] {
		t.Fatalf("history kinds = %v", kinds)
	}
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
