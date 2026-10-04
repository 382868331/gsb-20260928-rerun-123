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
)

// Observation 是一条观察证据。观察只追加，永不删除或覆盖：
// 不同观察可以分别支持成功、失败或仍未知，互相矛盾时共存。
type Observation struct {
	ID      string `json:"id"`
	Source  string `json:"source"`  // 证据来源（对账单、收据、查询接口等）
	Verdict string `json:"verdict"` // success / failed / unknown
	Note    string `json:"note"`
	At      string `json:"at"`
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
	ExecutedActions []string      `json:"executed_actions"` // 已执行的业务动作，跨裁决累计
	Note            string        `json:"note,omitempty"`
	CreatedAt       string        `json:"created_at"`
	UpdatedAt       string        `json:"updated_at"`
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

// Authorize 核对一个业务动作是否被当前有效裁决明确授权。
// 争议暂停（裁决被新证据暂停）、没有生效裁决（未裁决或裁决已撤回）、
// 或动作不在裁决的 Action/AllowedNext 明确授权范围内，均不得放行。
func (d *Dispute) Authorize(action string) error {
	if d.Status == DsPaused {
		return fmt.Errorf("%w: %s", ErrDisputePaused, d.ID)
	}
	r := d.ActiveRuling()
	if r == nil || d.Status != DsRuled {
		return fmt.Errorf("%w: dispute %s", ErrNoActiveRuling, d.ID)
	}
	if action == r.Action {
		return nil
	}
	for _, a := range r.AllowedNext {
		if a == action {
			return nil
		}
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
// 若争议已有生效裁决而新证据与裁决基础相矛盾，则暂停争议及受影响任务，
// 已发生的业务效果全部保留。
func (m *DisputeManager) AddObservation(ctx context.Context, disputeID, source, verdict, note string) (*Dispute, error) {
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
		At:      m.ts(),
	}
	d.Observations = append(d.Observations, obs)
	d.UpdatedAt = m.ts()

	if r := d.ActiveRuling(); r != nil && d.Status == DsRuled && m.contradicts(d, r, obs) {
		d.Status = DsPaused
		d.Note = fmt.Sprintf("观察 %s(%s) 与裁决 %s 的基础矛盾，暂停受影响后续动作", obs.ID, verdict, r.ID)
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

// contradicts 判断新观察是否与裁决所依据的证据集合相矛盾。
func (m *DisputeManager) contradicts(d *Dispute, r *Ruling, obs Observation) bool {
	basisVerdicts := map[string]bool{}
	for _, o := range d.Observations {
		for _, id := range r.Basis {
			if o.ID == id {
				basisVerdicts[o.Verdict] = true
			}
		}
	}
	if len(basisVerdicts) == 0 {
		return false
	}
	// 裁决基础一致认为 success，新证据说 failed（或反之）即矛盾；unknown 不构成矛盾。
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
