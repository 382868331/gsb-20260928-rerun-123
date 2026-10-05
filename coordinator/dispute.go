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
	// ErrActionSuspended 表示该动作所在范围被矛盾证据局部暂停；
	// 其他完全独立的动作不受影响、可以继续。
	ErrActionSuspended = errors.New("coordinator: action suspended by scoped contradictory evidence")
	// ErrActionNotAllowed 表示该业务动作不在当前裁决允许范围内。
	ErrActionNotAllowed = errors.New("coordinator: action not allowed by current ruling")
	// ErrActionAlreadyDone 表示同一业务动作已执行过，撤回裁决不能使其再次执行。
	ErrActionAlreadyDone = errors.New("coordinator: business action already executed, withdrawal cannot re-run it")
	// ErrNoActiveRuling 表示当前没有生效裁决。
	ErrNoActiveRuling = errors.New("coordinator: no active ruling")
	// ErrStaleVersion 表示确认所依据的裁决/证据版本已过期，
	// 后来形成的限制（他人补充、证据撤回等）不能被过期决定覆盖。
	ErrStaleVersion = errors.New("coordinator: decision based on stale ruling/evidence version")
	// ErrEvidenceWithdrawn 表示作为确认基础的证据已被撤回，不能据此形成许可。
	ErrEvidenceWithdrawn = errors.New("coordinator: basis evidence has been withdrawn")
	// ErrRulingChanged 表示待确认补充所针对的裁决已不再是当前生效裁决。
	ErrRulingChanged = errors.New("coordinator: ruling targeted by supplement is no longer active")
)

// Observation 是一条观察证据。观察只追加，永不删除或覆盖：
// 不同观察可以分别支持成功、失败或仍未知，互相矛盾时共存。
type Observation struct {
	ID      string `json:"id"`
	Source  string `json:"source"`  // 证据来源（对账单、收据、查询接口等）
	Verdict string `json:"verdict"` // success / failed / unknown
	Note    string `json:"note"`
	// Scope 是该证据所涉及的业务动作范围（如 release/shipping）。
	// 为空表示作用于裁决基础本身（全局），出现矛盾时暂停全部后续动作。
	Scope       []string `json:"scope,omitempty"`
	Seq         int      `json:"seq"`                 // 证据在争议内的稳定序号（版本标识）
	Withdrawn   bool     `json:"withdrawn,omitempty"` // 证据被撤回
	WithdrawnAt string   `json:"withdrawn_at,omitempty"`
	At          string   `json:"at"`
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
	// Suspended 是被矛盾证据局部暂停的动作集合；仅与证据矛盾的范围相关，
	// 完全独立且已核实的动作不会被暂停。全局暂停（无作用域矛盾）时为空切片，
	// 此时 Status=DsPaused 表示所有后续动作均暂停。
	Suspended []string `json:"suspended,omitempty"`
	// ScopeChanges 是每次裁决范围变更（局部补充确认/失效）的追加记录，
	// 重开后可据此解释某个动作在当时为何获准或被拒。
	ScopeChanges []ScopeChange `json:"scope_changes,omitempty"`
	// Audit 是争议生命周期内关键事件的追加审计流。
	Audit []AuditEntry `json:"audit,omitempty"`
	// Version 在每次形成新限制或改变授权状态的写入时单调递增。
	// 两名操作者并行补充时，确认必须基于当前版本，过期决定一律拒绝。
	Version   int    `json:"version"`
	Note      string `json:"note,omitempty"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// ScopeChange 记录一次裁决范围的局部补充。
type ScopeChange struct {
	ID             string   `json:"id"`
	RulingID       string   `json:"ruling_id"`        // 所补充的生效裁决
	Add            []string `json:"add,omitempty"`    // 预览时可增加的许可
	Remove         []string `json:"remove,omitempty"` // 预览时可缩小的许可
	Basis          []string `json:"basis"`            // 所依据的证据 ID
	By             string   `json:"by"`
	At             string   `json:"at"`
	BaseVersion    int      `json:"base_version"`              // 操作者预览时看到的版本
	AppliedVersion int      `json:"applied_version,omitempty"` // 确认生效时的版本
	Status         string   `json:"status"`                    // pending/confirmed/superseded/withdrawn/stale
	Reason         string   `json:"reason,omitempty"`          // 变更理由 / 失效原因
}

// AuditEntry 是一条不可变的审计事实。
type AuditEntry struct {
	At      string `json:"at"`
	Event   string `json:"event"`
	By      string `json:"by,omitempty"`
	Action  string `json:"action,omitempty"`
	Version int    `json:"version"`
	Detail  string `json:"detail,omitempty"`
}

const (
	ScPending    = "pending"    // 局部补充已提交证据，等待操作者确认
	ScConfirmed  = "confirmed"  // 补充已确认并形成范围变更
	ScSuperseded = "superseded" // 依据版本过期/裁决变更，被后来的限制覆盖
	ScWithdrawn  = "withdrawn"  // 依据证据被撤回
	ScStale      = "stale"      // 确认时版本已过期（拒绝的确认也留痕）
)

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
// 动作不在裁决（含已确认的局部补充）范围内，或动作所在范围被矛盾证据
// 局部暂停，均不得放行；范围之外的独立动作不受影响。
func (d *Dispute) Authorize(action string) error {
	if d.Status == DsPaused {
		return fmt.Errorf("%w: %s", ErrDisputePaused, d.ID)
	}
	r := d.ActiveRuling()
	if r == nil || d.Status != DsRuled {
		return fmt.Errorf("%w: dispute %s", ErrNoActiveRuling, d.ID)
	}
	if !d.permittedSet()[action] {
		return fmt.Errorf("%w: action %s not in ruling %s", ErrActionNotAllowed, action, r.ID)
	}
	if d.isSuspended(action) {
		return fmt.Errorf("%w: action %s suspended in dispute %s", ErrActionSuspended, action, d.ID)
	}
	return nil
}

// permittedSet 是当前生效裁决叠加全部已确认局部补充后的有效许可集合。
// 后形成的限制优先：补充可以新增许可，也可以缩小（移除）先前许可。
func (d *Dispute) permittedSet() map[string]bool {
	r := d.ActiveRuling()
	perm := map[string]bool{}
	if r == nil {
		return perm
	}
	perm[r.Action] = true
	for _, a := range r.AllowedNext {
		perm[a] = true
	}
	for i := range d.ScopeChanges {
		sc := &d.ScopeChanges[i]
		if sc.Status != ScConfirmed || sc.RulingID != r.ID {
			continue
		}
		for _, a := range sc.Add {
			perm[a] = true
		}
		for _, a := range sc.Remove {
			delete(perm, a)
		}
	}
	return perm
}

// isSuspended 判断动作是否处于局部暂停范围。
func (d *Dispute) isSuspended(action string) bool {
	for _, a := range d.Suspended {
		if a == action {
			return true
		}
	}
	return false
}

// ActionStates 分类返回各业务动作当前所处状态，供操作者预览：
// done 已发生；permitted 当前获准且未暂停；suspended 被局部/全局暂停；
// restricted 仍受旧裁决约束、未获准。
func (d *Dispute) ActionStates() (done, permitted, suspended, restricted []string) {
	doneSet := map[string]bool{}
	for _, a := range d.ExecutedActions {
		doneSet[a] = true
		done = append(done, a)
	}
	r := d.ActiveRuling()
	if r != nil && d.Status == DsRuled {
		for a := range d.permittedSet() {
			if doneSet[a] {
				continue
			}
			if d.isSuspended(a) {
				suspended = append(suspended, a)
			} else {
				permitted = append(permitted, a)
			}
		}
	}
	// 仍受旧裁决约束：没有生效裁决、或裁决动作本身被暂停，且尚未执行。
	if r != nil && d.Status != DsRuled {
		if !doneSet[r.Action] {
			restricted = append(restricted, r.Action)
		}
	}
	sort.Strings(done)
	sort.Strings(permitted)
	sort.Strings(suspended)
	sort.Strings(restricted)
	return done, permitted, suspended, restricted
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

// Update 在 store 锁内读取-修改-写回一条争议，避免两名操作者并行补充时
// 后写覆盖先写（丢失后来形成的限制）。fn 返回错误时不写回。
func (s *DisputeStore) Update(ctx context.Context, id string, fn func(d *Dispute) error) (*Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("coordinator: dispute %s not found", id)
	}
	if err != nil {
		return nil, err
	}
	var d Dispute
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("coordinator: corrupt dispute %s: %w", id, err)
	}
	if err := fn(&d); err != nil {
		return nil, err
	}
	out, err := json.MarshalIndent(&d, "", "  ")
	if err != nil {
		return nil, err
	}
	tmp := s.path(id) + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, s.path(id)); err != nil {
		return nil, err
	}
	return &d, nil
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

// SetClock 注入可控时钟（主要用于测试并行补充/迟到确认时的版本顺序）。
func (m *DisputeManager) SetClock(f func() time.Time) {
	if f != nil {
		m.now = f
	}
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
	return m.appendObservation(ctx, disputeID, source, verdict, note, nil)
}

// contradicts 判断新观察是否与裁决所依据的证据集合（或给定作用域内已核实
// 证据）相矛盾。unknown 不构成矛盾；已撤回的证据不参与判断。
func (m *DisputeManager) contradicts(d *Dispute, basisIDs []string, obs Observation) bool {
	basisVerdicts := map[string]bool{}
	valid := map[string]Observation{}
	for _, o := range d.Observations {
		valid[o.ID] = o
	}
	for _, id := range basisIDs {
		o, ok := valid[id]
		if !ok || o.Withdrawn {
			continue
		}
		basisVerdicts[o.Verdict] = true
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

// verdictConflicts 判断 verdict 与裁决基础结论是否相反（作用域局部矛盾用）。
func conflictsVerdict(basis map[string]bool, verdict string) bool {
	if verdict == Success.String() && basis[Failed.String()] {
		return true
	}
	if verdict == Failed.String() && basis[Success.String()] {
		return true
	}
	return false
}

// appendObservation 是追加证据的统一入口。scope 非空时为作用域证据：
// 只暂停与其矛盾的动作范围，不阻断完全独立的已核实动作。
func (m *DisputeManager) appendObservation(ctx context.Context, disputeID, source, verdict, note string, scope []string) (*Dispute, error) {
	return m.store.Update(ctx, disputeID, func(d *Dispute) error {
		obs := Observation{
			ID:      m.nextID("obs"),
			Source:  source,
			Verdict: verdict,
			Note:    note,
			Scope:   append([]string(nil), scope...),
			Seq:     len(d.Observations) + 1,
			At:      m.ts(),
		}
		d.Observations = append(d.Observations, obs)
		d.UpdatedAt = m.ts()
		r := d.ActiveRuling()
		if r == nil || d.Status != DsRuled {
			m.audit(d, "observation_added", source, "", "evidence appended (no active ruling)")
			return nil
		}
		if len(scope) == 0 {
			// 全局证据：与裁决基础矛盾 -> 暂停整个争议与任务，已发生效果保留。
			if m.contradicts(d, r.Basis, obs) {
				d.Status = DsPaused
				d.Suspended = nil
				d.Note = fmt.Sprintf("观察 %s(%s) 与裁决 %s 的基础矛盾，暂停受影响后续动作", obs.ID, verdict, r.ID)
				d.Version++
				m.superseedePending(d, r.ID, "全局矛盾证据形成新限制")
				m.audit(d, "evidence_contradicts_global", source, "", d.Note)
				if m.coord != nil {
					if perr := m.coord.Pause(ctx, d.TaskID, d.Note); perr != nil && !errors.Is(perr, ErrNotResumable) && !errors.Is(perr, ErrTaskNotFound) {
						return perr
					}
				}
			} else {
				m.audit(d, "observation_added", source, "", "global evidence")
			}
			return nil
		}
		// 作用域证据：只评估其作用域内动作。与该范围已核实结论矛盾时，
		// 仅把这些动作局部暂停，其他独立动作继续可执行。
		for _, action := range scope {
			basis := m.scopeBasis(d, r, action)
			if conflictsVerdict(basis, obs.Verdict) {
				if !d.isSuspended(action) {
					d.Suspended = append(d.Suspended, action)
				}
				d.Version++
				m.superseedePendingFor(d, r.ID, action, "作用域矛盾证据形成新限制")
				m.audit(d, "evidence_contradicts_scoped", source, action,
					fmt.Sprintf("观察 %s(%s) 与动作 %s 的已核实结论矛盾，局部暂停", obs.ID, verdict, action))
			}
		}
		d.Status = DsRuled
		d.Note = ""
		m.audit(d, "observation_added", source, "", fmt.Sprintf("scoped evidence: %v", scope))
		return nil
	})
}

// scopeBasis 返回某动作当前“已核实结论”所依据的证据结论集合：
// 裁决基础（仅当裁决本身授权该动作）加上所有针对该动作的已确认补充基础。
func (m *DisputeManager) scopeBasis(d *Dispute, r *Ruling, action string) map[string]bool {
	verdicts := map[string]bool{}
	add := func(ids []string) {
		for _, o := range d.Observations {
			if o.Withdrawn {
				continue
			}
			for _, id := range ids {
				if o.ID == id {
					verdicts[o.Verdict] = true
				}
			}
		}
	}
	if action == r.Action {
		add(r.Basis)
	}
	for _, a := range r.AllowedNext {
		if a == action {
			add(r.Basis)
		}
	}
	for i := range d.ScopeChanges {
		sc := &d.ScopeChanges[i]
		if sc.Status != ScConfirmed || sc.RulingID != r.ID {
			continue
		}
		touches := false
		for _, a := range sc.Add {
			if a == action {
				touches = true
			}
		}
		if touches {
			add(sc.Basis)
		}
	}
	return verdicts
}

// recalcSuspended 在撤回证据/撤回补充后重新计算局部暂停集合：
// 若某动作已不再存在与之矛盾的有效证据，则解除其局部暂停。
func (m *DisputeManager) recalcSuspended(d *Dispute, r *Ruling) {
	var still []string
	for _, action := range d.Suspended {
		basis := m.scopeBasis(d, r, action)
		blocked := false
		for _, o := range d.Observations {
			if o.Withdrawn || !inList(o.Scope, action) {
				continue
			}
			if conflictsVerdict(basis, o.Verdict) {
				blocked = true
				break
			}
		}
		if blocked {
			still = append(still, action)
		} else {
			d.Version++
			m.audit(d, "scope_resumed", "", action, "矛盾证据消失，解除局部暂停")
		}
	}
	d.Suspended = still
}

func inList(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// latestSeqBefore 返回时间戳早于 ts 的最新证据序号（撤回重裁时用于界定
// “撤裁之后才形成的限制”）。RFC3339Nano UTC 时间戳可直接按字符串比较。
func latestSeqBefore(d *Dispute, ts string) int {
	seq := 0
	for _, o := range d.Observations {
		if o.At != "" && o.At <= ts && o.Seq > seq {
			seq = o.Seq
		}
	}
	return seq
}

// superseedePending 使针对当前裁决的所有待确认补充失效。
func (m *DisputeManager) superseedePending(d *Dispute, rulingID, reason string) {
	for i := range d.ScopeChanges {
		sc := &d.ScopeChanges[i]
		if sc.Status == ScPending && sc.RulingID == rulingID {
			sc.Status = ScSuperseded
			sc.Reason = reason
		}
	}
}

// superseedePendingFor 使会触及给定动作的待确认补充失效。
func (m *DisputeManager) superseedePendingFor(d *Dispute, rulingID, action, reason string) {
	for i := range d.ScopeChanges {
		sc := &d.ScopeChanges[i]
		if sc.Status != ScPending || sc.RulingID != rulingID {
			continue
		}
		if inList(sc.Add, action) || inList(sc.Remove, action) {
			sc.Status = ScSuperseded
			sc.Reason = reason
		}
	}
}

func (m *DisputeManager) audit(d *Dispute, event, by, action, detail string) {
	d.Audit = append(d.Audit, AuditEntry{
		At:      m.ts(),
		Event:   event,
		By:      by,
		Action:  action,
		Version: d.Version,
		Detail:  detail,
	})
}

// Rule 由操作者基于一组观察作出裁决，明确适用的业务动作与允许的后续步骤。
func (m *DisputeManager) Rule(ctx context.Context, disputeID, action string, allowedNext []string, basis []string, by string) (*Dispute, error) {
	return m.store.Update(ctx, disputeID, func(d *Dispute) error {
		if r := d.ActiveRuling(); r != nil {
			return fmt.Errorf("coordinator: dispute %s already has active ruling %s; withdraw it first", d.ID, r.ID)
		}
		valid := map[string]bool{}
		for _, o := range d.Observations {
			if o.Withdrawn {
				continue
			}
			valid[o.ID] = true
		}
		for _, id := range basis {
			if !valid[id] {
				return fmt.Errorf("coordinator: basis observation %s not found or withdrawn", id)
			}
		}
		r := Ruling{
			ID:          m.nextID("ruling"),
			Action:      action,
			AllowedNext: append([]string(nil), allowedNext...),
			Basis:       append([]string(nil), basis...),
			By:          by,
			At:          m.ts(),
		}
		d.Rulings = append(d.Rulings, r)
		d.Status = DsRuled
		d.Suspended = nil
		d.UpdatedAt = m.ts()
		d.Version++
		// 首次裁决：操作者已在审理当时全部证据，历史矛盾不再自动暂停。
		// 撤回后重裁：只重评上一裁决被撤回之后新到的证据——若后来形成的限制
		// 仍然存在，过期决定不能覆盖它。
		hadWithdrawn := false
		var sinceSeq int
		for i := len(d.Rulings) - 2; i >= 0; i-- {
			if d.Rulings[i].Withdrawn && d.Rulings[i].WithdrawnAt != "" {
				hadWithdrawn = true
				sinceSeq = latestSeqBefore(d, d.Rulings[i].WithdrawnAt)
				break
			}
		}
		if hadWithdrawn {
			for _, o := range d.Observations {
				if o.Withdrawn || o.Seq <= sinceSeq {
					continue
				}
				if len(o.Scope) == 0 {
					if m.contradicts(d, r.Basis, o) {
						d.Status = DsPaused
						d.Suspended = nil
						d.Note = fmt.Sprintf("重裁后全局证据 %s(%s) 仍与裁决基础矛盾，维持暂停", o.ID, o.Verdict)
						break
					}
					continue
				}
				for _, act := range o.Scope {
					if conflictsVerdict(m.scopeBasis(d, &r, act), o.Verdict) && !d.isSuspended(act) {
						d.Suspended = append(d.Suspended, act)
					}
				}
			}
		}
		m.audit(d, "ruled", by, action, fmt.Sprintf("ruling %s, allowed=%v", r.ID, allowedNext))
		return nil
	})
}

// Withdraw 撤回当前生效裁决，争议重新进入审理。
// 已执行的业务动作保留在 ExecutedActions 中，不会因撤回而可再次执行。
func (m *DisputeManager) Withdraw(ctx context.Context, disputeID string) (*Dispute, error) {
	return m.store.Update(ctx, disputeID, func(d *Dispute) error {
		r := d.ActiveRuling()
		if r == nil {
			return ErrNoActiveRuling
		}
		r.Withdrawn = true
		r.WithdrawnAt = m.ts()
		d.Status = DsReopened
		d.Suspended = nil
		d.UpdatedAt = m.ts()
		d.Version++
		m.superseedePending(d, r.ID, "裁决被撤回")
		m.audit(d, "ruling_withdrawn", "", r.Action, "ruling withdrawn, dispute reopened")
		return nil
	})
}

// ExecuteAction 按当前生效裁决执行一个业务动作。
// 同一动作在整个争议生命周期内只能执行一次，撤回裁决不会重置该限制。
func (m *DisputeManager) ExecuteAction(ctx context.Context, disputeID, action string) (*Dispute, error) {
	return m.store.Update(ctx, disputeID, func(d *Dispute) error {
		for _, done := range d.ExecutedActions {
			if done == action {
				return fmt.Errorf("%w: %s", ErrActionAlreadyDone, action)
			}
		}
		if err := d.Authorize(action); err != nil {
			return err
		}
		d.ExecutedActions = append(d.ExecutedActions, action)
		d.UpdatedAt = m.ts()
		d.Version++
		m.audit(d, "action_executed", "", action, "business effect recorded once")
		return nil
	})
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
	for _, done := range d.ExecutedActions {
		if done == action {
			return nil, fmt.Errorf("%w: %s", ErrActionAlreadyDone, action)
		}
	}
	if err := d.Authorize(action); err != nil {
		return nil, err
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
