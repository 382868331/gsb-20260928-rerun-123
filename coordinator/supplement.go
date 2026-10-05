package coordinator

import (
	"context"
	"fmt"
)

// SupplementPreview 是操作者提交局部新证据后看到的许可预览。
type SupplementPreview struct {
	ChangeID    string // 待确认范围变更 ID
	DisputeID   string
	RulingID    string // 所补充的生效裁决
	BaseVersion int    // 预览依据的版本；确认时必须仍等于当前版本
	EvidenceID  string
	Add         []string // 确认后可增加的许可
	Remove      []string // 确认后可缩小的许可
	Done        []string // 已发生的动作（不可重复执行）
	Permitted   []string // 当前获准、可继续的动作（含完全独立的动作）
	Suspended   []string // 被矛盾证据局部暂停的动作
	Restricted  []string // 仍受旧裁决约束、未获准的动作
}

// AddScopedEvidence 针对尚未执行的动作范围提交新证据，并生成一份可预览、
// 待确认的范围变更（pending ScopeChange）。它不改变当前许可：
// 只有 ConfirmSupplement 且版本仍最新时才会形成新许可/新限制。
// 若新证据与该范围已核实结论矛盾，则只暂停这些动作，独立动作不受影响。
func (m *DisputeManager) AddScopedEvidence(ctx context.Context, disputeID, source, verdict, note string, scope []string, add, remove []string, reason, by string) (*SupplementPreview, error) {
	if len(scope) == 0 {
		return nil, fmt.Errorf("coordinator: scoped evidence requires non-empty scope")
	}
	d, err := m.appendObservation(ctx, disputeID, source, verdict, note, scope)
	if err != nil {
		return nil, err
	}
	obs := &d.Observations[len(d.Observations)-1]
	preview, err := m.store.Update(ctx, disputeID, func(d *Dispute) error {
		r := d.ActiveRuling()
		if r == nil || d.Status != DsRuled {
			return fmt.Errorf("%w: dispute %s has no active ruling to supplement", ErrNoActiveRuling, d.ID)
		}
		for _, a := range scope {
			for _, done := range d.ExecutedActions {
				if done == a {
					return fmt.Errorf("%w: cannot supplement executed action %s", ErrActionAlreadyDone, a)
				}
			}
		}
		sc := ScopeChange{
			ID:          m.nextID("scope"),
			RulingID:    r.ID,
			Add:         append([]string(nil), add...),
			Remove:      append([]string(nil), remove...),
			Basis:       []string{obs.ID},
			By:          by,
			At:          m.ts(),
			BaseVersion: d.Version,
			Status:      ScPending,
			Reason:      reason,
		}
		d.ScopeChanges = append(d.ScopeChanges, sc)
		d.UpdatedAt = m.ts()
		m.audit(d, "supplement_proposed", by, "", fmt.Sprintf("scope=%v add=%v remove=%v reason=%s", scope, add, remove, reason))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m.buildPreview(preview, preview.ScopeChanges[len(preview.ScopeChanges)-1].ID)
}

// PreviewSupplement 返回某份待确认补充当前视角下的许可增缩与动作分类。
func (m *DisputeManager) PreviewSupplement(ctx context.Context, changeID string) (*SupplementPreview, error) {
	id, _, err := m.findChange(ctx, changeID)
	if err != nil {
		return nil, err
	}
	return m.buildPreview(id, changeID)
}

// ConfirmSupplement 基于预览时的版本确认局部补充。
// 两名操作者并行补充时，每次确认都核对各自依据的裁决与证据版本：
// 裁决已变更、证据被撤回、或版本过期（后来已形成新限制）一律拒绝，
// 过期决定不能覆盖后来形成的限制。拒绝同样留痕（stale）。
func (m *DisputeManager) ConfirmSupplement(ctx context.Context, changeID, by string) (*Dispute, error) {
	d, idx, err := m.findChange(ctx, changeID)
	if err != nil {
		return nil, err
	}
	disputeID := d.ID
	_ = idx
	return m.store.Update(ctx, disputeID, func(cur *Dispute) error {
		ci := findChangeIndex(cur, changeID)
		if ci < 0 {
			return fmt.Errorf("coordinator: scope change %s not found", changeID)
		}
		c := &cur.ScopeChanges[ci]
		stale := func(reason string) error {
			c.Status = ScStale
			c.Reason = reason
			cur.UpdatedAt = m.ts()
			m.audit(cur, "supplement_rejected", by, "", fmt.Sprintf("%s: %s", changeID, reason))
			return fmt.Errorf("%w: %s", ErrStaleVersion, reason)
		}
		if c.Status != ScPending {
			return stale(fmt.Sprintf("change already %s", c.Status))
		}
		r := cur.ActiveRuling()
		if r == nil || r.ID != c.RulingID {
			return stale("target ruling is no longer active")
		}
		if cur.Version != c.BaseVersion {
			return stale(fmt.Sprintf("base version %d != current %d", c.BaseVersion, cur.Version))
		}
		for _, bid := range c.Basis {
			o := findObservation(cur, bid)
			if o == nil || o.Withdrawn {
				c.Status = ScWithdrawn
				c.Reason = "basis evidence withdrawn"
				cur.UpdatedAt = m.ts()
				m.audit(cur, "supplement_rejected", by, "", changeID+": basis evidence withdrawn")
				return fmt.Errorf("%w: %s", ErrEvidenceWithdrawn, bid)
			}
		}
		// 仅确认那些当前仍允许、且没有被局部暂停的缩小项；新增项不能复活已暂停动作。
		c.Status = ScConfirmed
		c.AppliedVersion = cur.Version + 1
		cur.Version++
		cur.UpdatedAt = m.ts()
		m.audit(cur, "supplement_confirmed", by, "",
			fmt.Sprintf("%s add=%v remove=%v -> version %d", changeID, c.Add, c.Remove, cur.Version))
		return nil
	})
}

// WithdrawEvidence 撤回一条证据。若它是某项待确认补充的唯一依据，则该补充失效；
// 若它是生效裁决的依据，则裁决相应缩小许可（被撤回证据支持的授权不再成立）。
// 仅由该证据造成的局部暂停随之解除，其他限制保留；解除暂停后任务可恢复。
func (m *DisputeManager) WithdrawEvidence(ctx context.Context, disputeID, evidenceID, by string) (*Dispute, error) {
	return m.store.Update(ctx, disputeID, func(d *Dispute) error {
		o := findObservation(d, evidenceID)
		if o == nil {
			return fmt.Errorf("coordinator: observation %s not found", evidenceID)
		}
		if o.Withdrawn {
			return fmt.Errorf("coordinator: observation %s already withdrawn", evidenceID)
		}
		o.Withdrawn = true
		o.WithdrawnAt = m.ts()
		d.Version++
		d.UpdatedAt = m.ts()
		m.audit(d, "evidence_withdrawn", by, "", evidenceID+" ("+o.Source+")")

		r := d.ActiveRuling()
		// 依赖该证据的补充一律失效：待确认的不能再确认；已确认的许可增缩
		// 立即回退（permittedSet 只认 confirmed），动作恢复到旧裁决约束。
		for i := range d.ScopeChanges {
			sc := &d.ScopeChanges[i]
			if (sc.Status == ScPending || sc.Status == ScConfirmed) && inList(sc.Basis, evidenceID) {
				sc.Status = ScWithdrawn
				sc.Reason = "basis evidence withdrawn"
			}
		}
		if r == nil {
			return nil
		}
		// 裁决依据被撤回：裁决整体不再成立，回到重审（保守处理，许可缩小到零）。
		if inList(r.Basis, evidenceID) {
			r.Withdrawn = true
			r.WithdrawnAt = m.ts()
			d.Status = DsReopened
			d.Suspended = nil
			m.superseedePending(d, r.ID, "裁决依据证据被撤回")
			m.audit(d, "ruling_withdrawn", by, r.Action, "ruling basis evidence withdrawn")
			return nil
		}
		// 否则只是作用域证据撤回：重算局部暂停，解除已无矛盾支撑的范围。
		m.recalcSuspended(d, r)
		return nil
	})
}

// ActionTimeline 返回与某动作相关的全部审计事实与范围变更，
// 重开后可据此解释该动作在当时为何获准或被拒。
func (m *DisputeManager) ActionTimeline(ctx context.Context, disputeID, action string) ([]AuditEntry, []ScopeChange, error) {
	d, err := m.store.Load(ctx, disputeID)
	if err != nil {
		return nil, nil, err
	}
	if d == nil {
		return nil, nil, fmt.Errorf("coordinator: dispute %s not found", disputeID)
	}
	var entries []AuditEntry
	for _, e := range d.Audit {
		if e.Action == "" || e.Action == action {
			entries = append(entries, e)
		}
	}
	var changes []ScopeChange
	for _, sc := range d.ScopeChanges {
		if inList(sc.Add, action) || inList(sc.Remove, action) {
			changes = append(changes, sc)
		}
	}
	return entries, changes, nil
}

func (m *DisputeManager) findChange(ctx context.Context, changeID string) (*Dispute, int, error) {
	all, err := m.store.List(ctx)
	if err != nil {
		return nil, -1, err
	}
	for _, d := range all {
		if i := findChangeIndex(d, changeID); i >= 0 {
			return d, i, nil
		}
	}
	return nil, -1, fmt.Errorf("coordinator: scope change %s not found", changeID)
}

func findChangeIndex(d *Dispute, changeID string) int {
	for i := range d.ScopeChanges {
		if d.ScopeChanges[i].ID == changeID {
			return i
		}
	}
	return -1
}

func findObservation(d *Dispute, id string) *Observation {
	for i := range d.Observations {
		if d.Observations[i].ID == id {
			return &d.Observations[i]
		}
	}
	return nil
}

func (m *DisputeManager) buildPreview(d *Dispute, changeID string) (*SupplementPreview, error) {
	idx := findChangeIndex(d, changeID)
	if idx < 0 {
		return nil, fmt.Errorf("coordinator: scope change %s not found", changeID)
	}
	sc := d.ScopeChanges[idx]
	done, permitted, suspended, restricted := d.ActionStates()
	return &SupplementPreview{
		ChangeID:    sc.ID,
		DisputeID:   d.ID,
		RulingID:    sc.RulingID,
		BaseVersion: sc.BaseVersion,
		Add:         append([]string(nil), sc.Add...),
		Remove:      append([]string(nil), sc.Remove...),
		Done:        done,
		Permitted:   permitted,
		Suspended:   suspended,
		Restricted:  restricted,
	}, nil
}
