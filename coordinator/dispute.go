package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// 争议状态。
const (
	DsOpen     = "open"     // 已立案，等待裁决
	DsRuled    = "ruled"    // 已有生效裁决
	DsPaused   = "paused"   // 裁决后出现矛盾证据，受影响后续动作暂停
	DsReopened = "reopened" // 裁决被撤回，重新审理中
)

var (
	// ErrDisputePaused 表示争议因矛盾证据被暂停，相关动作必须等待。
	ErrDisputePaused = errors.New("coordinator: dispute paused by contradictory evidence")
	// ErrActionNotAllowed 表示该业务动作不在当前裁决允许范围内。
	ErrActionNotAllowed = errors.New("coordinator: action not allowed by current ruling")
	// ErrActionAlreadyDone 表示同一业务动作已执行过，撤回裁决不能使其再次执行。
	ErrActionAlreadyDone = errors.New("coordinator: business action already executed, withdrawal cannot re-run it")
	// ErrNoActiveRuling 表示当前没有生效裁决。
	ErrNoActiveRuling = errors.New("coordinator: no active ruling")
	// ErrStaleVersion 表示补充确认所依据的裁决/证据版本已过期：
	// 并行操作者中，过期决定不能覆盖后来形成的限制。
	ErrStaleVersion = errors.New("coordinator: stale ruling/evidence version, refresh and re-preview")
)

// Observation 是一条观察证据。观察只追加，永不删除或覆盖：
// 不同观察可以分别支持成功、失败或仍未知，互相矛盾时共存。
// 证据可以被撤回（标记 Withdrawn，记录保留），撤回会解除由它单独导致的暂停。
type Observation struct {
	ID          string   `json:"id"`
	Source      string   `json:"source"`  // 证据来源（对账单、收据、查询接口等）
	Verdict     string   `json:"verdict"` // success / failed / unknown
	Note        string   `json:"note"`
	Actions     []string `json:"actions,omitempty"` // 证据涉及的动作；为空表示涉及整个争议
	At          string   `json:"at"`
	Withdrawn   bool     `json:"withdrawn,omitempty"`
	WithdrawnAt string   `json:"withdrawn_at,omitempty"`
}

// Ruling 是操作者基于某一组证据作出的裁决。
type Ruling struct {
	ID          string   `json:"id"`
	Action      string   `json:"action"`       // 适用的业务动作，如 compensate / retry / mark_success
	AllowedNext []string `json:"allowed_next"` // 裁决后允许继续的步骤/动作
	Basis       []string `json:"basis"`        // 所依据的观察 ID 列表
	By          string   `json:"by"`
	At          string   `json:"at"`
	Withdrawn   bool     `json:"withdrawn"`
	WithdrawnAt string   `json:"withdrawn_at,omitempty"`
}

// Dispute 是一份可持续处理的争议记录。
type Dispute struct {
	ID              string        `json:"id"`
	TaskID          string        `json:"task_id"`
	StepName        string        `json:"step_name"`
	Status          string        `json:"status"`
	Observations    []Observation `json:"observations"`
	Rulings         []Ruling      `json:"rulings"`
	Supplements     []Supplement  `json:"supplements,omitempty"` // 裁决范围的局部补充，按确认顺序累计
	Pauses          []ActionPause `json:"pauses,omitempty"`      // 当前被矛盾证据暂停的动作范围
	History         []ScopeEvent  `json:"history,omitempty"`     // 每次范围变更及理由，重开后可追溯
	RulingVersion   int           `json:"ruling_version"`        // 裁决版本：裁决/撤回递增
	EvidenceVersion int           `json:"evidence_version"`      // 证据版本：追加/撤回证据递增
	ExecutedActions []string      `json:"executed_actions"`      // 已执行的业务动作，跨裁决累计
	Note            string        `json:"note,omitempty"`
	CreatedAt       string        `json:"created_at"`
	UpdatedAt       string        `json:"updated_at"`
}

// Supplement 是对生效裁决范围的一次局部补充：可增加或缩小许可，
// 只针对尚未执行的动作；确认时记录所依据的裁决与证据版本。
type Supplement struct {
	ID              string   `json:"id"`
	Add             []string `json:"add,omitempty"`    // 新增许可的动作
	Remove          []string `json:"remove,omitempty"` // 收回许可的动作
	Basis           []string `json:"basis"`            // 新提交的证据（观察 ID）
	Reason          string   `json:"reason"`
	By              string   `json:"by"`
	At              string   `json:"at"`
	RulingVersion   int      `json:"ruling_version"`   // 确认时核对过的裁决版本
	EvidenceVersion int      `json:"evidence_version"` // 确认时核对过的证据版本
}

// ActionPause 记录一个被矛盾证据暂停的动作及其原因。
type ActionPause struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
	ByObs  string `json:"by_obs"` // 导致暂停的观察 ID
}

// ScopeEvent 是一次范围变更的审计记录，重开后可据此解释
// 某个动作在当时为何获准或被拒。
type ScopeEvent struct {
	ID      string   `json:"id"`
	Kind    string   `json:"kind"` // rule / supplement / withdraw_ruling / pause / unpause / withdraw_evidence
	Actions []string `json:"actions,omitempty"`
	Reason  string   `json:"reason"`
	By      string   `json:"by,omitempty"`
	At      string   `json:"at"`
}

// ActiveRuling 返回当前生效（未撤回）的裁决，没有则返回 nil。
func (d *Dispute) ActiveRuling() *Ruling {
	for i := len(d.Rulings) - 1; i >= 0; i-- {
		if !d.Rulings[i].Withdrawn {
			return &d.Rulings[i]
		}
	}
	return nil
}

// EffectiveScope 计算当前生效裁决叠加全部补充后的许可范围。
func (d *Dispute) EffectiveScope() map[string]bool {
	scope := map[string]bool{}
	r := d.ActiveRuling()
	if r == nil {
		return scope
	}
	scope[r.Action] = true
	for _, a := range r.AllowedNext {
		scope[a] = true
	}
	for _, s := range d.Supplements {
		for _, a := range s.Add {
			scope[a] = true
		}
		for _, a := range s.Remove {
			delete(scope, a)
		}
	}
	return scope
}

func (d *Dispute) pausedReason(action string) (string, bool) {
	for _, p := range d.Pauses {
		if p.Action == action {
			return p.Reason, true
		}
	}
	return "", false
}

func (d *Dispute) executed(action string) bool {
	for _, done := range d.ExecutedActions {
		if done == action {
			return true
		}
	}
	return false
}

// ExplainAction 解释某动作在当前状态下为何获准或被拒，
// 依据的是持久化的范围与历史记录，进程重开后同样成立。
func (d *Dispute) ExplainAction(action string) string {
	if d.executed(action) {
		return fmt.Sprintf("动作 %s 已执行过（记录于 ExecutedActions），任何裁决或补充都不能让它再执行一次", action)
	}
	r := d.ActiveRuling()
	if r == nil || (d.Status != DsRuled && d.Status != DsPaused) {
		return fmt.Sprintf("动作 %s 被拒：当前没有生效裁决（状态 %s）", action, d.Status)
	}
	if reason, paused := d.pausedReason(action); paused {
		return fmt.Sprintf("动作 %s 被暂停：%s", action, reason)
	}
	if d.EffectiveScope()[action] {
		return fmt.Sprintf("动作 %s 获准：在裁决 %s 及其补充的有效范围内", action, r.ID)
	}
	return fmt.Sprintf("动作 %s 被拒：不在裁决 %s 及其补充的授权范围内", action, r.ID)
}

// Authorize 核对一个业务动作是否被当前有效裁决明确授权。
// 暂停是按范围生效的：只有与新证据矛盾的动作范围被暂停，
// 完全独立的已核实动作不受影响。没有生效裁决（未裁决或裁决已撤回）、
// 或动作不在裁决及补充的明确授权范围内，均不得放行。
func (d *Dispute) Authorize(action string) error {
	r := d.ActiveRuling()
	if r == nil || (d.Status != DsRuled && d.Status != DsPaused) {
		return fmt.Errorf("%w: dispute %s", ErrNoActiveRuling, d.ID)
	}
	if reason, paused := d.pausedReason(action); paused {
		return fmt.Errorf("%w: %s", ErrDisputePaused, reason)
	}
	if d.EffectiveScope()[action] {
		return nil
	}
	return fmt.Errorf("%w: action %s not in ruling %s", ErrActionNotAllowed, action, r.ID)
}

// DisputeStore 以 JSON 文件持久化争议记录（临时文件 + rename 原子写入）。
type DisputeStore struct {
	mu  sync.Mutex
	dir string
}

func NewDisputeStore(dir string) (*DisputeStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("coordinator: create dispute store dir: %w", err)
	}
	return &DisputeStore{dir: dir}, nil
}

func (s *DisputeStore) path(id string) string { return filepath.Join(s.dir, id+".json") }

func (s *DisputeStore) Load(_ context.Context, id string) (*Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var d Dispute
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("coordinator: corrupt dispute %s: %w", id, err)
	}
	return &d, nil
}

func (s *DisputeStore) Save(_ context.Context, d *Dispute) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(d.ID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(d.ID))
}

func (s *DisputeStore) List(_ context.Context) ([]*Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []*Dispute
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var d Dispute
		if err := json.Unmarshal(data, &d); err != nil {
			return nil, fmt.Errorf("coordinator: corrupt dispute file %s: %w", e.Name(), err)
		}
		out = append(out, &d)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// DisputeManager 管理争议的生命周期，并在矛盾证据出现时暂停受影响任务。
type DisputeManager struct {
	store *DisputeStore
	coord *Coordinator
	now   func() time.Time
	seq   int
}

func NewDisputeManager(store *DisputeStore, coord *Coordinator) *DisputeManager {
	return &DisputeManager{store: store, coord: coord, now: time.Now}
}

func (m *DisputeManager) ts() string { return m.now().UTC().Format(time.RFC3339Nano) }

func (m *DisputeManager) nextID(prefix string) string {
	m.seq++
	return fmt.Sprintf("%s-%d-%04d", prefix, m.now().UnixNano(), m.seq)
}

// Open 立案一条争议。
func (m *DisputeManager) Open(ctx context.Context, taskID, stepName, note string) (*Dispute, error) {
	d := &Dispute{
		ID:        m.nextID("dispute"),
		TaskID:    taskID,
		StepName:  stepName,
		Status:    DsOpen,
		Note:      note,
		CreatedAt: m.ts(),
		UpdatedAt: m.ts(),
	}
	return d, m.store.Save(ctx, d)
}

// AddObservation 追加一条观察证据。已有证据永不被覆盖；
// 若新证据与裁决或某条补充的证据基础相矛盾，只暂停与之矛盾的动作范围，
// 完全独立的已核实动作不受影响；已发生的业务效果全部保留。
func (m *DisputeManager) AddObservation(ctx context.Context, disputeID, source, verdict, note string, actions ...string) (*Dispute, error) {
	d, err := m.store.Load(ctx, disputeID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("coordinator: dispute %s not found", disputeID)
	}
	obs := Observation{
		ID:      m.nextID("obs"),
		Source:  source,
		Verdict: verdict,
		Note:    note,
		Actions: actions,
		At:      m.ts(),
	}
	d.Observations = append(d.Observations, obs)
	d.EvidenceVersion++
	d.UpdatedAt = m.ts()

	newlyPaused := m.recomputePauses(d)
	if len(newlyPaused) > 0 {
		d.Note = fmt.Sprintf("观察 %s(%s) 与授权基础矛盾，暂停受影响动作: %v", obs.ID, verdict, newlyPaused)
		m.recordEvent(d, "pause", newlyPaused, d.Note, "")
		if err := m.store.Save(ctx, d); err != nil {
			return nil, err
		}
		// 暂停任务但保留已发生效果：只改任务状态，不回滚任何步骤。
		if m.coord != nil {
			if err := m.coord.Pause(ctx, d.TaskID, d.Note); err != nil && !errors.Is(err, ErrNotResumable) && !errors.Is(err, ErrTaskNotFound) {
				return nil, err
			}
		}
		return d, nil
	}
	return d, m.store.Save(ctx, d)
}

// recomputePauses 依据当前全部未撤回证据重新计算被暂停的动作范围：
// 裁决或补充的证据基础一致认为 success，而某条新证据说 failed（或反之）时，
// 该授权覆盖且尚未执行的动作被暂停；unknown 不构成矛盾。
// 返回本次新暂停的动作列表。
func (m *DisputeManager) recomputePauses(d *Dispute) []string {
	type grant struct {
		owner   string
		actions []string
		basis   []string
	}
	var grants []grant
	if r := d.ActiveRuling(); r != nil {
		actions := append([]string{r.Action}, r.AllowedNext...)
		grants = append(grants, grant{owner: r.ID, actions: actions, basis: r.Basis})
	}
	for _, s := range d.Supplements {
		grants = append(grants, grant{owner: s.ID, actions: s.Add, basis: s.Basis})
	}
	wasPaused := map[string]bool{}
	for _, p := range d.Pauses {
		wasPaused[p.Action] = true
	}
	d.Pauses = nil
	var newly []string
	for _, g := range grants {
		for _, obs := range d.Observations {
			if obs.Withdrawn || !m.contradictsBasis(d, g.basis, obs) {
				continue
			}
			for _, a := range g.actions {
				if d.executed(a) {
					continue
				}
				// 证据限定了涉及动作时，只暂停其涉及的范围；
				// 完全独立的已核实动作不被阻断。
				if len(obs.Actions) > 0 {
					concerned := false
					for _, oa := range obs.Actions {
						if oa == a {
							concerned = true
							break
						}
					}
					if !concerned {
						continue
					}
				}
				dup := false
				for _, p := range d.Pauses {
					if p.Action == a {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				d.Pauses = append(d.Pauses, ActionPause{
					Action: a,
					Reason: fmt.Sprintf("观察 %s(%s) 与授权 %s 的证据基础矛盾", obs.ID, obs.Verdict, g.owner),
					ByObs:  obs.ID,
				})
				if !wasPaused[a] {
					newly = append(newly, a)
				}
			}
		}
	}
	if len(d.Pauses) > 0 {
		d.Status = DsPaused
	} else if d.Status == DsPaused && d.ActiveRuling() != nil {
		d.Status = DsRuled
	}
	return newly
}

// contradictsBasis 判断一条观察是否与某授权所依据的证据集合相矛盾。
func (m *DisputeManager) contradictsBasis(d *Dispute, basis []string, obs Observation) bool {
	basisVerdicts := map[string]bool{}
	for _, o := range d.Observations {
		if o.Withdrawn {
			continue
		}
		for _, id := range basis {
			if o.ID == id {
				basisVerdicts[o.Verdict] = true
			}
		}
	}
	if len(basisVerdicts) == 0 {
		return false
	}
	if obs.Verdict == Success.String() && basisVerdicts[Failed.String()] {
		return true
	}
	if obs.Verdict == Failed.String() && basisVerdicts[Success.String()] {
		return true
	}
	return false
}

// Rule 由操作者基于一组观察作出裁决，明确适用的业务动作与允许的后续步骤。
func (m *DisputeManager) Rule(ctx context.Context, disputeID, action string, allowedNext []string, basis []string, by string) (*Dispute, error) {
	d, err := m.store.Load(ctx, disputeID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("coordinator: dispute %s not found", disputeID)
	}
	if r := d.ActiveRuling(); r != nil {
		return nil, fmt.Errorf("coordinator: dispute %s already has active ruling %s; withdraw it first", d.ID, r.ID)
	}
	valid := map[string]bool{}
	for _, o := range d.Observations {
		valid[o.ID] = true
	}
	for _, id := range basis {
		if !valid[id] {
			return nil, fmt.Errorf("coordinator: basis observation %s not found", id)
		}
	}
	d.Rulings = append(d.Rulings, Ruling{
		ID:          m.nextID("ruling"),
		Action:      action,
		AllowedNext: allowedNext,
		Basis:       basis,
		By:          by,
		At:          m.ts(),
	})
	d.Status = DsRuled
	d.RulingVersion++
	m.recordEvent(d, "rule", append([]string{action}, allowedNext...), fmt.Sprintf("裁决 %s 基于证据 %v", action, basis), by)
	d.UpdatedAt = m.ts()
	return d, m.store.Save(ctx, d)
}

// Withdraw 撤回当前生效裁决，争议重新进入审理。
// 已执行的业务动作保留在 ExecutedActions 中，不会因撤回而可再次执行。
func (m *DisputeManager) Withdraw(ctx context.Context, disputeID string) (*Dispute, error) {
	d, err := m.store.Load(ctx, disputeID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("coordinator: dispute %s not found", disputeID)
	}
	r := d.ActiveRuling()
	if r == nil {
		return nil, ErrNoActiveRuling
	}
	r.Withdrawn = true
	r.WithdrawnAt = m.ts()
	d.Status = DsReopened
	d.RulingVersion++
	m.recordEvent(d, "withdraw_ruling", append([]string{r.Action}, r.AllowedNext...), fmt.Sprintf("撤回裁决 %s，争议重新审理", r.ID), "")
	d.UpdatedAt = m.ts()
	return d, m.store.Save(ctx, d)
}

// recordEvent 追加一条范围变更审计记录。
func (m *DisputeManager) recordEvent(d *Dispute, kind string, actions []string, reason, by string) {
	d.History = append(d.History, ScopeEvent{
		ID:      m.nextID("evt"),
		Kind:    kind,
		Actions: actions,
		Reason:  reason,
		By:      by,
		At:      m.ts(),
	})
}

// WithdrawObservation 撤回一条证据（记录保留，永不删除）。
// 证据版本递增，使基于旧证据版本的迟到补充确认失效；
// 由该证据单独导致的范围暂停随之解除，并记录审计事件。
func (m *DisputeManager) WithdrawObservation(ctx context.Context, disputeID, obsID, reason string) (*Dispute, error) {
	d, err := m.store.Load(ctx, disputeID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("coordinator: dispute %s not found", disputeID)
	}
	found := false
	for i := range d.Observations {
		if d.Observations[i].ID == obsID {
			if d.Observations[i].Withdrawn {
				return nil, fmt.Errorf("coordinator: observation %s already withdrawn", obsID)
			}
			d.Observations[i].Withdrawn = true
			d.Observations[i].WithdrawnAt = m.ts()
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("coordinator: observation %s not found", obsID)
	}
	d.EvidenceVersion++
	m.recordEvent(d, "withdraw_evidence", nil, fmt.Sprintf("撤回证据 %s：%s", obsID, reason), "")
	before := map[string]bool{}
	for _, p := range d.Pauses {
		before[p.Action] = true
	}
	m.recomputePauses(d)
	var unpaused []string
	still := map[string]bool{}
	for _, p := range d.Pauses {
		still[p.Action] = true
	}
	for a := range before {
		if !still[a] {
			unpaused = append(unpaused, a)
		}
	}
	if len(unpaused) > 0 {
		m.recordEvent(d, "unpause", unpaused, fmt.Sprintf("证据 %s 撤回后不再矛盾，恢复这些范围", obsID), "")
	}
	d.UpdatedAt = m.ts()
	return d, m.store.Save(ctx, d)
}

// SupplementRequest 是一次范围补充的确认请求。
type SupplementRequest struct {
	Add                   []string // 要增加许可的动作（仅限尚未执行的动作）
	Remove                []string // 要收回许可的动作
	Basis                 []string // 新提交的证据（观察 ID）
	Reason                string
	By                    string
	ExpectRulingVersion   int // 预览时看到的裁决版本
	ExpectEvidenceVersion int // 预览时看到的证据版本
}

// SupplementPreview 是补充确认前的预览：哪些动作已发生、
// 哪些仍受旧裁决约束、本次可增加或缩小哪些许可。
type SupplementPreview struct {
	DisputeID             string   `json:"dispute_id"`
	RulingVersion         int      `json:"ruling_version"`
	EvidenceVersion       int      `json:"evidence_version"`
	ExecutedActions       []string `json:"executed_actions"`        // 已发生的动作，不可再许可
	CurrentScope          []string `json:"current_scope"`           // 当前有效许可范围
	PausedActions         []string `json:"paused_actions"`          // 当前被矛盾证据暂停的动作
	AddEffective          []string `json:"add_effective"`           // 本次新增后真正获得许可的动作
	AddAlreadyExecuted    []string `json:"add_already_executed"`    // 请求增加但已执行、永远不会再执行的动作
	RemoveEffective       []string `json:"remove_effective"`        // 本次真正收回的许可
	RemoveAlreadyExecuted []string `json:"remove_already_executed"` // 请求收回但已执行的动作（效果已发生，无法收回）
	StillUnderRuling      []string `json:"still_under_ruling"`      // 补充后仍受旧裁决约束的动作
}

// PreviewSupplement 预览一次范围补充的效果，不产生任何变更；
// 返回的版本号必须原样传给 ConfirmSupplement。
func (m *DisputeManager) PreviewSupplement(ctx context.Context, disputeID string, add, remove []string) (*SupplementPreview, error) {
	d, err := m.store.Load(ctx, disputeID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("coordinator: dispute %s not found", disputeID)
	}
	scope := d.EffectiveScope()
	p := &SupplementPreview{
		DisputeID:       disputeID,
		RulingVersion:   d.RulingVersion,
		EvidenceVersion: d.EvidenceVersion,
		ExecutedActions: append([]string{}, d.ExecutedActions...),
	}
	for a := range scope {
		p.CurrentScope = append(p.CurrentScope, a)
	}
	for _, pa := range d.Pauses {
		p.PausedActions = append(p.PausedActions, pa.Action)
	}
	after := map[string]bool{}
	for a := range scope {
		after[a] = true
	}
	for _, a := range add {
		if d.executed(a) {
			p.AddAlreadyExecuted = append(p.AddAlreadyExecuted, a)
			continue
		}
		if !scope[a] {
			p.AddEffective = append(p.AddEffective, a)
		}
		after[a] = true
	}
	for _, a := range remove {
		if d.executed(a) {
			p.RemoveAlreadyExecuted = append(p.RemoveAlreadyExecuted, a)
			continue
		}
		if scope[a] {
			p.RemoveEffective = append(p.RemoveEffective, a)
		}
		delete(after, a)
	}
	for a := range after {
		p.StillUnderRuling = append(p.StillUnderRuling, a)
	}
	sort.Strings(p.CurrentScope)
	sort.Strings(p.PausedActions)
	sort.Strings(p.AddEffective)
	sort.Strings(p.RemoveEffective)
	sort.Strings(p.StillUnderRuling)
	return p, nil
}

// ConfirmSupplement 确认一次范围补充。两名操作者并行补充时，
// 必须核对各自预览所依据的裁决与证据版本：版本过期则拒绝，
// 过期决定不能覆盖后来形成的限制。每次确认都记录理由，持久可审计。
func (m *DisputeManager) ConfirmSupplement(ctx context.Context, disputeID string, req SupplementRequest) (*Dispute, error) {
	d, err := m.store.Load(ctx, disputeID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("coordinator: dispute %s not found", disputeID)
	}
	if d.ActiveRuling() == nil {
		return nil, ErrNoActiveRuling
	}
	if req.ExpectRulingVersion != d.RulingVersion || req.ExpectEvidenceVersion != d.EvidenceVersion {
		return nil, fmt.Errorf("%w: ruling %d->%d evidence %d->%d",
			ErrStaleVersion, req.ExpectRulingVersion, d.RulingVersion, req.ExpectEvidenceVersion, d.EvidenceVersion)
	}
	valid := map[string]Observation{}
	for _, o := range d.Observations {
		valid[o.ID] = o
	}
	for _, id := range req.Basis {
		o, ok := valid[id]
		if !ok {
			return nil, fmt.Errorf("coordinator: basis observation %s not found", id)
		}
		if o.Withdrawn {
			return nil, fmt.Errorf("coordinator: basis observation %s has been withdrawn", id)
		}
	}
	sup := Supplement{
		ID:              m.nextID("sup"),
		Add:             req.Add,
		Remove:          req.Remove,
		Basis:           req.Basis,
		Reason:          req.Reason,
		By:              req.By,
		At:              m.ts(),
		RulingVersion:   d.RulingVersion,
		EvidenceVersion: d.EvidenceVersion,
	}
	d.Supplements = append(d.Supplements, sup)
	actions := append(append([]string{}, req.Add...), req.Remove...)
	m.recordEvent(d, "supplement", actions, fmt.Sprintf("补充 %s：增加 %v，收回 %v，理由：%s", sup.ID, req.Add, req.Remove, req.Reason), req.By)
	// 新补充的证据基础可能与既有证据矛盾，重新计算暂停范围。
	m.recomputePauses(d)
	d.UpdatedAt = m.ts()
	return d, m.store.Save(ctx, d)
}

// ExecuteAction 按当前生效裁决执行一个业务动作。
// 同一动作在整个争议生命周期内只能执行一次，撤回裁决不会重置该限制。
func (m *DisputeManager) ExecuteAction(ctx context.Context, disputeID, action string) (*Dispute, error) {
	d, err := m.checkAction(ctx, disputeID, action)
	if err != nil {
		return nil, err
	}
	d.ExecutedActions = append(d.ExecutedActions, action)
	d.UpdatedAt = m.ts()
	return d, m.store.Save(ctx, d)
}

// CheckAction 查询某业务动作当前是否可执行，判定口径与 ExecuteAction 完全一致：
// 必须被当前有效裁决明确授权，且同一动作终身只能执行一次。
// 这样许可查询结果与实际执行结果一致，不会出现“放行却执行不了”或反之。
func (m *DisputeManager) CheckAction(ctx context.Context, disputeID, action string) error {
	_, err := m.checkAction(ctx, disputeID, action)
	return err
}

func (m *DisputeManager) checkAction(ctx context.Context, disputeID, action string) (*Dispute, error) {
	d, err := m.store.Load(ctx, disputeID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("coordinator: dispute %s not found", disputeID)
	}
	if err := d.Authorize(action); err != nil {
		return nil, err
	}
	for _, done := range d.ExecutedActions {
		if done == action {
			return nil, fmt.Errorf("%w: %s", ErrActionAlreadyDone, action)
		}
	}
	return d, nil
}

// Gate 实现 coordinator.Gate：争议暂停或裁决未允许的步骤必须等待。
func (m *DisputeManager) Gate(ctx context.Context) Gate {
	return func(taskID, stepName string) error {
		disputes, err := m.store.List(ctx)
		if err != nil {
			return err
		}
		for _, d := range disputes {
			if d.TaskID != taskID {
				continue
			}
			// 每个后续动作都必须被当前有效裁决明确授权：
			// 未裁决、裁决已撤回、被新证据暂停或动作不在授权范围内，一律拦截，
			// 与争议立案步骤名无关。
			if err := d.Authorize(stepName); err != nil {
				return err
			}
		}
		return nil
	}
}
