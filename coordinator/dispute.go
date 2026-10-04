package coordinator

import (
    "errors"
    "fmt"
    "time"
)

// 争议记录：当不同渠道对同一业务效果给出不同观察（成功/失败/未知）时，
// 需要人工裁决。本模块保证：
//   - 观察只追加、不覆盖，相反证据并存；
//   - 裁决明确适用的业务动作与允许的后续步骤；
//   - 裁决后出现矛盾证据时，暂停受影响的后续动作，已发生的效果保留；
//   - 裁决可撤回重审，但同一业务动作不会被重复执行。

// 观察结论。
const (
    ObsSuccess = "success" // 观察支持效果已发生
    ObsFailure = "failure" // 观察支持效果未发生/被拒绝
    ObsUnknown = "unknown" // 观察无法得出结论
)

// 争议状态。
const (
    DisputeOpen   = "open"   // 收集证据中，可裁决
    DisputeRuled  = "ruled"  // 已有生效裁决
    DisputePaused = "paused" // 裁决后出现矛盾证据，后续动作暂停
    DisputeClosed = "closed" // 裁决动作已执行完毕，争议关闭
)

// Observation 是一条独立证据，追加后不可修改、不可删除。
type Observation struct {
    ID      string `json:"id"`
    Source  string `json:"source"`  // 证据来源（银行回单、网关回调等）
    Verdict string `json:"verdict"` // success / failure / unknown
    Detail  string `json:"detail"`
    At      string `json:"at"`
}

// Ruling 是操作者基于一组证据作出的裁决。
type Ruling struct {
    ID          string   `json:"id"`
    Action      string   `json:"action"`       // 适用的业务动作，如 refund / close_task
    AllowedNext []string `json:"allowed_next"` // 裁决后允许的后续步骤
    BasedOn     []string `json:"based_on"`     // 依据的观察 ID
    Verdict     string   `json:"verdict"`      // 裁决认定的效果
    Status      string   `json:"status"`       // active / withdrawn
    Reason      string   `json:"reason"`
    At          string   `json:"at"`
}

// Dispute 是一份可持续处理的争议记录。
type Dispute struct {
    ID              string        `json:"id"`
    TaskID          string        `json:"task_id"`
    Subject         string        `json:"subject"` // 争议对象，如某笔扣款
    Status          string        `json:"status"`
    Observations    []Observation `json:"observations"`
    Rulings         []Ruling      `json:"rulings"`
    ExecutedActions []string      `json:"executed_actions"` // 已执行的业务动作，防重
    PauseNote       string        `json:"pause_note,omitempty"`
    Log             []string      `json:"log"` // 每一步为何可继续或必须等待
    CreatedAt       string        `json:"created_at"`
    UpdatedAt       string        `json:"updated_at"`
}

var (
    ErrRulingActive     = errors.New("dispute: an active ruling exists; withdraw it before re-adjudicating")
    ErrNoActiveRuling   = errors.New("dispute: no active ruling")
    ErrDisputePaused    = errors.New("dispute: paused by contradictory evidence; follow-up actions suspended")
    ErrActionDone       = errors.New("dispute: business action already executed; withdrawal cannot re-trigger it")
    ErrActionNotAllowed = errors.New("dispute: action not permitted by the active ruling")
)

func NewDispute(id, taskID, subject string, now time.Time) *Dispute {
    ts := now.UTC().Format(time.RFC3339)
    d := &Dispute{ID: id, TaskID: taskID, Subject: subject, Status: DisputeOpen, CreatedAt: ts, UpdatedAt: ts}
    d.logf(now, "争议 %s 建档（任务 %s，对象 %s），开始收集证据", id, taskID, subject)
    return d
}

func (d *Dispute) logf(now time.Time, format string, args ...any) {
    d.Log = append(d.Log, fmt.Sprintf(now.UTC().Format(time.RFC3339)+" "+format, args...))
}

// AddObservation 追加一条观察。观察永不覆盖已有相反证据；
// 若已有生效裁决且新观察与其认定相反，则暂停受影响的后续动作。
func (d *Dispute) AddObservation(now time.Time, id, source, verdict, detail string) {
    d.Observations = append(d.Observations, Observation{
        ID: id, Source: source, Verdict: verdict, Detail: detail,
        At: now.UTC().Format(time.RFC3339),
    })
    d.UpdatedAt = now.UTC().Format(time.RFC3339)
    d.logf(now, "追加观察 %s（来源 %s）：%s — %s；与既有证据并存，不覆盖", id, source, verdict, detail)

    if r := d.activeRuling(); r != nil && contradicts(verdict, r.Verdict) {
        d.Status = DisputePaused
        d.PauseNote = fmt.Sprintf("观察 %s(%s) 与裁决 %s 的认定 %s 矛盾", id, verdict, r.ID, r.Verdict)
        d.logf(now, "新观察与生效裁决 %s 矛盾，暂停后续动作 %v；已发生的效果保留，等待人工处理",
            r.ID, r.AllowedNext)
    }
}

func contradicts(obs, ruled string) bool {
    return (obs == ObsSuccess && ruled == ObsFailure) || (obs == ObsFailure && ruled == ObsSuccess)
}

func (d *Dispute) activeRuling() *Ruling {
    for i := len(d.Rulings) - 1; i >= 0; i-- {
        if d.Rulings[i].Status == "active" {
            return &d.Rulings[i]
        }
    }
    return nil
}

// Rule 基于一组观察作出裁决，明确业务动作与允许的后续步骤。
func (d *Dispute) Rule(now time.Time, id, verdict, action string, allowedNext []string, basedOn []string, reason string) error {
    if d.activeRuling() != nil {
        return ErrRulingActive
    }
    if d.Status == DisputeClosed {
        return fmt.Errorf("dispute: already closed")
    }
    known := map[string]bool{}
    for _, o := range d.Observations {
        known[o.ID] = true
    }
    for _, oid := range basedOn {
        if !known[oid] {
            return fmt.Errorf("dispute: observation %s not found", oid)
        }
    }
    d.Rulings = append(d.Rulings, Ruling{
        ID: id, Verdict: verdict, Action: action, AllowedNext: allowedNext,
        BasedOn: basedOn, Status: "active", Reason: reason,
        At: now.UTC().Format(time.RFC3339),
    })
    d.Status = DisputeRuled
    d.PauseNote = ""
    d.UpdatedAt = now.UTC().Format(time.RFC3339)
    d.logf(now, "裁决 %s 生效：认定 %s，业务动作 %s，允许后续 %v；依据 %v，理由：%s",
        id, verdict, action, allowedNext, basedOn, reason)
    return nil
}

// WithdrawRuling 撤回生效裁决，争议回到 open 重新审理。
// 已执行的业务动作保留在 ExecutedActions 中，不会因撤回而可再次执行。
func (d *Dispute) WithdrawRuling(now time.Time, reason string) error {
    r := d.activeRuling()
    if r == nil {
        return ErrNoActiveRuling
    }
    r.Status = "withdrawn"
    d.Status = DisputeOpen
    d.PauseNote = ""
    d.UpdatedAt = now.UTC().Format(time.RFC3339)
    d.logf(now, "裁决 %s 被撤回（%s），争议重开；已执行动作 %v 保留且不可重复执行",
        r.ID, reason, d.ExecutedActions)
    return nil
}

// Execute 执行生效裁决允许的业务动作。同一动作全局只执行一次。
func (d *Dispute) Execute(now time.Time, action string) error {
    if d.Status == DisputePaused {
        d.logf(now, "动作 %s 被拒绝：%s，必须等待人工处理矛盾证据", action, d.PauseNote)
        return ErrDisputePaused
    }
    r := d.activeRuling()
    if r == nil {
        return ErrNoActiveRuling
    }
    if r.Action != action {
        allowed := false
        for _, a := range r.AllowedNext {
            if a == action {
                allowed = true
                break
            }
        }
        if !allowed {
            d.logf(now, "动作 %s 被拒绝：不在裁决 %s 允许的范围（动作 %s，后续 %v）",
                action, r.ID, r.Action, r.AllowedNext)
            return ErrActionNotAllowed
        }
    }
    for _, done := range d.ExecutedActions {
        if done == action {
            d.logf(now, "动作 %s 被拒绝：已执行过，撤回裁决也不能重复执行同一业务动作", action)
            return ErrActionDone
        }
    }
    d.ExecutedActions = append(d.ExecutedActions, action)
    if action == r.Action {
        d.Status = DisputeClosed
        d.logf(now, "裁决动作 %s 已执行，争议关闭；全部证据与裁决历史保留备查", action)
    } else {
        d.logf(now, "后续动作 %s 已按裁决 %s 执行", action, r.ID)
    }
    d.UpdatedAt = now.UTC().Format(time.RFC3339)
    return nil
}
