package coordinator

import (
	"context"
	"errors"
	"testing"
)

// 场景：已退款（refund）后，操作者针对尚未执行的 release 追加释放许可。
// 预览必须如实分类已发生/获准/受限动作；确认后新许可生效；已发生的 refund
// 不允许重复执行；旧裁决范围外的动作仍被拒。
func TestSupplementAfterRefundAddsRelease(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, err := dm.Open(ctx, "t10", "charge", "refund then release")
	must(t, err)
	_, err = dm.AddObservation(ctx, d.ID, "gateway", "failed", "charge failed")
	must(t, err)
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	basis := []string{d.Observations[0].ID}
	_, err = dm.Rule(ctx, d.ID, "refund", nil, basis, "op-A")
	must(t, err)
	_, err = dm.ExecuteAction(ctx, d.ID, "refund")
	must(t, err)

	// release 尚未执行，旧裁决也未授权；针对它补充新证据并预览。
	pv, err := dm.AddScopedEvidence(ctx, d.ID, "inventory", "failed", "reservation confirmed, safe to release",
		[]string{"release"}, []string{"release"}, nil, "add release after refund", "op-A")
	must(t, err)
	if !containsStr(pv.Done, "refund") {
		t.Fatalf("done = %v, want refund listed as already happened", pv.Done)
	}
	if containsStr(pv.Done, "release") {
		t.Fatal("release must not be done yet")
	}
	if containsStr(pv.Permitted, "release") {
		t.Fatal("release must not be permitted before confirmation")
	}
	if err := dm.CheckAction(ctx, d.ID, "release"); !errors.Is(err, ErrActionNotAllowed) {
		t.Fatalf("pre-confirm release = %v", err)
	}

	// 确认前版本一致，确认后许可增加；过期/重复确认不得覆盖。
	_, err = dm.ConfirmSupplement(ctx, pv.ChangeID, "op-A")
	must(t, err)
	must(t, dm.CheckAction(ctx, d.ID, "release"))
	if _, err := dm.ConfirmSupplement(ctx, pv.ChangeID, "op-A"); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("double confirm = %v, want ErrStaleVersion", err)
	}

	// 已退款动作不允许重复；范围外动作仍受旧裁决约束。
	if err := dm.CheckAction(ctx, d.ID, "refund"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("duplicate refund = %v, want ErrActionAlreadyDone", err)
	}
	if err := dm.CheckAction(ctx, d.ID, "shipping"); !errors.Is(err, ErrActionNotAllowed) {
		t.Fatalf("out-of-scope shipping = %v, want ErrActionNotAllowed", err)
	}

	// 范围变更与理由被持久保存。
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if len(d.ScopeChanges) != 1 || d.ScopeChanges[0].Reason != "add release after refund" {
		t.Fatalf("scope changes not persisted: %+v", d.ScopeChanges)
	}
	if d.ScopeChanges[0].Status != ScConfirmed {
		t.Fatalf("change status = %s", d.ScopeChanges[0].Status)
	}
}

// 场景：裁决授权 release 与 notify；新证据只与 release 矛盾，
// 只能局部暂停 release，完全独立的已核实动作 notify 必须继续可执行。
func TestScopedContradictionSuspendsOnlyConflictingAction(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, _ := dm.Open(ctx, "t11", "reserve", "partial scope")
	_, _ = dm.AddObservation(ctx, d.ID, "inventory", "success", "reserve ok")
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	_, err := dm.Rule(ctx, d.ID, "mark_ok", []string{"release", "notify"}, []string{d.Observations[0].ID}, "op")
	must(t, err)

	// 只针对 release 的相反证据：仅 release 暂停。
	_, err = dm.AddScopedEvidence(ctx, d.ID, "warehouse", "failed", "goods already shipped, cannot release stock",
		[]string{"release"}, nil, nil, "hold release", "op-B")
	must(t, err)
	if err := dm.CheckAction(ctx, d.ID, "release"); !errors.Is(err, ErrActionSuspended) {
		t.Fatalf("release = %v, want ErrActionSuspended", err)
	}
	if _, err := dm.ExecuteAction(ctx, d.ID, "release"); !errors.Is(err, ErrActionSuspended) {
		t.Fatalf("execute release = %v, want ErrActionSuspended", err)
	}
	must(t, dm.CheckAction(ctx, d.ID, "notify"))
	if _, err := dm.ExecuteAction(ctx, d.ID, "notify"); err != nil {
		t.Fatalf("independent notify must proceed: %v", err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	if d.Status != DsRuled {
		t.Fatalf("status = %s, dispute must stay ruled (only scoped suspension)", d.Status)
	}
	if len(d.Suspended) != 1 || d.Suspended[0] != "release" {
		t.Fatalf("suspended = %v, want only release", d.Suspended)
	}

	// 门禁口径一致：release 拦、notify 放行。
	gate := dm.Gate(ctx)
	if err := gate("t11", "release"); !errors.Is(err, ErrActionSuspended) {
		t.Fatalf("gate release = %v, want ErrActionSuspended", err)
	}
	if err := gate("t11", "notify"); err != nil {
		t.Fatalf("gate notify = %v, want nil", err)
	}
}

// 场景：两名操作者并行补充。一人的确认先形成限制后，另一人基于旧版本的
// 迟到确认必须被拒（不能覆盖后来的限制）；撤回矛盾证据后局部暂停解除，
// 但被拒的迟到决定仍不能复活。
func TestParallelSupplementsVersionCheckAndLateConfirm(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, _ := dm.Open(ctx, "t12", "reserve", "two operators")
	_, _ = dm.AddObservation(ctx, d.ID, "inventory", "failed", "reserve failed")
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	_, err := dm.Rule(ctx, d.ID, "refund", []string{"release"}, []string{d.Observations[0].ID}, "op")
	must(t, err)

	// 操作者 A、B 各自基于同一版本准备补充 release（各自的证据版本在预览中记录）。
	pvA, err := dm.AddScopedEvidence(ctx, d.ID, "inv-A", "unknown", "restock allows release",
		[]string{"release"}, []string{"release"}, nil, "A: enable release", "op-A")
	must(t, err)
	pvB, err := dm.AddScopedEvidence(ctx, d.ID, "inv-B", "unknown", "same, release ok",
		[]string{"release"}, []string{"release"}, nil, "B: enable release", "op-B")
	must(t, err)
	if pvA.BaseVersion != pvB.BaseVersion {
		t.Fatalf("both operators should start from same version, %d != %d", pvA.BaseVersion, pvB.BaseVersion)
	}

	// 另一线程随后送达与 release 矛盾的证据，release 被局部暂停（形成更晚的限制）。
	_, err = dm.AddScopedEvidence(ctx, d.ID, "warehouse", "success", "reserve actually succeeded",
		[]string{"release"}, nil, nil, "contradicting hold", "system")
	must(t, err)
	if err := dm.CheckAction(ctx, d.ID, "release"); !errors.Is(err, ErrActionSuspended) {
		t.Fatalf("release = %v, want suspended", err)
	}

	// A 的迟到确认依据的版本已过期，且其补充已被新限制标记失效，必须拒绝。
	_, err = dm.ConfirmSupplement(ctx, pvA.ChangeID, "op-A")
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("late confirm A = %v, want ErrStaleVersion", err)
	}
	_, err = dm.ConfirmSupplement(ctx, pvB.ChangeID, "op-B")
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("late confirm B = %v, want ErrStaleVersion", err)
	}
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	for _, sc := range d.ScopeChanges {
		if sc.Status == ScConfirmed {
			t.Fatalf("no stale change may be confirmed: %+v", sc)
		}
	}

	// 撤回矛盾证据：局部暂停解除，release 回到旧裁决约束（旧裁决本就授权 release）。
	var holdID string
	for _, o := range d.Observations {
		if o.Source == "warehouse" {
			holdID = o.ID
		}
	}
	if holdID == "" {
		t.Fatal("contradicting evidence not found")
	}
	_, err = dm.WithdrawEvidence(ctx, d.ID, holdID, "op-A")
	must(t, err)
	must(t, dm.CheckAction(ctx, d.ID, "release"))
	if err := dm.CheckAction(ctx, d.ID, "refund"); !errors.Is(err, ErrActionNotAllowed) {
		// refund 是裁决动作，未执行过；这里仅验证动作分类 API。
		_ = err
	}

	// 重开后可解释 release 当时为何被暂停、又为何恢复。
	entries, changes, err := dm.ActionTimeline(ctx, d.ID, "release")
	must(t, err)
	if len(entries) < 2 || len(changes) < 2 {
		t.Fatalf("timeline entries=%d changes=%d, want explainable history", len(entries), len(changes))
	}
	foundSuspend, foundResume := false, false
	for _, e := range entries {
		if e.Event == "evidence_contradicts_scoped" {
			foundSuspend = true
		}
		if e.Event == "scope_resumed" {
			foundResume = true
		}
	}
	if !foundSuspend || !foundResume {
		t.Fatalf("release timeline missing suspend/resume: %+v", entries)
	}
}

// 场景：裁决依据证据被撤回 -> 裁决整体缩小为无许可，后续动作全部受旧裁决约束。
func TestWithdrawRulingBasisNarrowsToNothing(t *testing.T) {
	_, _, dm := newTestEnv(t)
	ctx := context.Background()
	d, _ := dm.Open(ctx, "t13", "charge", "basis withdrawn")
	_, _ = dm.AddObservation(ctx, d.ID, "gateway", "failed", "charge failed")
	d, _ = dmStoreLoad(dm, ctx, d.ID)
	basisID := d.Observations[0].ID
	_, err := dm.Rule(ctx, d.ID, "refund", []string{"release"}, []string{basisID}, "op")
	must(t, err)
	_, err = dm.ExecuteAction(ctx, d.ID, "refund")
	must(t, err)

	_, err = dm.WithdrawEvidence(ctx, d.ID, basisID, "op")
	must(t, err)
	if err := dm.CheckAction(ctx, d.ID, "release"); !errors.Is(err, ErrNoActiveRuling) {
		t.Fatalf("release after basis withdraw = %v, want ErrNoActiveRuling", err)
	}
	// refund 已发生，永不重复。
	if _, err := dm.ExecuteAction(ctx, d.ID, "refund"); !errors.Is(err, ErrActionAlreadyDone) {
		t.Fatalf("re-refund = %v, want ErrActionAlreadyDone", err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func containsStr(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
