package coordinator

import (
    "context"
    "errors"
    "fmt"
    "time"
)

// Op 是一对"执行 + 查询"：Do 产生业务效果，Query 在 Do 结果未知时核实效果。
// Query 必须幂等且无副作用。
type Op struct {
    Do    func(ctx context.Context) (Outcome, string, error)
    Query func(ctx context.Context) (Outcome, string, error)
}

// Step 是一个正向步骤及其补偿。
type Step struct {
    Name       string
    Forward    Op
    Compensate Op // 可以为零值（该步骤无需补偿）
}

// Gate 在执行每个步骤前被调用，返回非 nil 错误则该步骤必须等待。
// 争议管理器用它实现"裁决允许的后续步骤"与"矛盾证据导致的暂停"。
type Gate func(taskID, stepName string) error

// Coordinator 基于持久记录驱动一次分布式事务的正向执行与补偿。
type Coordinator struct {
    store Store
    gate  Gate
    now   func() time.Time
}

func NewCoordinator(store Store) *Coordinator {
    return &Coordinator{store: store, now: time.Now}
}

// SetGate 挂接争议门禁；可为空。
func (c *Coordinator) SetGate(g Gate) { c.gate = g }

func (c *Coordinator) ts() string { return c.now().UTC().Format(time.RFC3339Nano) }

// Run 执行或恢复任务。相同 taskID 可反复调用：已确认的效果不会重做，
// 未知步骤先查询核实，仍未知则暂停任务并返回 ErrPaused。
func (c *Coordinator) Run(ctx context.Context, taskID, key string, steps []Step) error {
    rec, err := c.store.Load(ctx, taskID)
    if err != nil {
        return err
    }
    if rec == nil {
        rec = &TaskRecord{
            TaskID:    taskID,
            Key:       key,
            Status:    StRunning,
            CreatedAt: c.ts(),
        }
        for _, s := range steps {
            rec.Steps = append(rec.Steps, StepRecord{Name: s.Name, Status: StepPending, UpdatedAt: c.ts()})
        }
        if _, err := c.store.Create(ctx, rec); err != nil {
            return err
        }
    }
    if len(rec.Steps) != len(steps) {
        return ErrStepMismatch
    }
    for i := range steps {
        if rec.Steps[i].Name != steps[i].Name {
            return ErrStepMismatch
        }
    }
    switch rec.Status {
    case StCommitted, StCompensated:
        return ErrNotResumable
    case StPaused:
        return ErrPaused
    }

    for i := range rec.Steps {
        sr := &rec.Steps[i]
        def := steps[i]
        switch sr.Status {
        case StepSucceeded, StepCompensated:
            continue
        case StepUnknown:
            // 效果未知：先查询核实，绝不盲目重试。
            if err := c.resolveUnknown(ctx, rec, sr, def.Forward); err != nil {
                return err
            }
            if sr.Status == StepUnknown {
                return c.pause(ctx, rec, fmt.Sprintf("步骤 %s 查询后结果仍未知，等待人工核实", sr.Name))
            }
            if sr.Status == StepFailed {
                return c.compensate(ctx, rec, steps, i)
            }
            continue
        case StepPending:
            if c.gate != nil {
                if err := c.gate(taskID, sr.Name); err != nil {
                    return c.pause(ctx, rec, fmt.Sprintf("步骤 %s 被争议门禁拦下: %v", sr.Name, err))
                }
            }
            sr.Attempts++
            out, detail, err := def.Forward.Do(ctx)
            if errors.Is(err, ErrUnknownResult) {
                out = Unknown
            } else if err != nil {
                return err
            }
            c.applyOutcome(rec, sr, out, detail, false)
            if err := c.store.Save(ctx, rec); err != nil {
                return err
            }
            switch sr.Status {
            case StepSucceeded:
                continue
            case StepFailed:
                return c.compensate(ctx, rec, steps, i)
            default: // unknown：先查询核实
                if err := c.resolveUnknown(ctx, rec, sr, def.Forward); err != nil {
                    return err
                }
                if sr.Status == StepUnknown {
                    return c.pause(ctx, rec, fmt.Sprintf("步骤 %s 响应丢失且查询仍未知，等待人工核实", sr.Name))
                }
                if sr.Status == StepFailed {
                    return c.compensate(ctx, rec, steps, i)
                }
            }
        default:
            return fmt.Errorf("coordinator: step %s in unexpected status %s", sr.Name, sr.Status)
        }
    }
    rec.Status = StCommitted
    rec.UpdatedAt = c.ts()
    return c.store.Save(ctx, rec)
}

// resolveUnknown 用 Query 核实未知步骤；查询本身也可能丢失响应。
func (c *Coordinator) resolveUnknown(ctx context.Context, rec *TaskRecord, sr *StepRecord, op Op) error {
    if op.Query == nil {
        return nil
    }
    out, detail, err := op.Query(ctx)
    if errors.Is(err, ErrUnknownResult) {
        out = Unknown
    } else if err != nil {
        return err
    }
    if out == Unknown {
        return nil
    }
    c.applyOutcome(rec, sr, out, detail, false)
    return c.store.Save(ctx, rec)
}

func (c *Coordinator) applyOutcome(rec *TaskRecord, sr *StepRecord, out Outcome, detail string, comp bool) {
    sr.Result = out.String()
    sr.Detail = detail
    sr.UpdatedAt = c.ts()
    rec.UpdatedAt = sr.UpdatedAt
    if comp {
        switch out {
        case Success:
            sr.Status = StepCompensated
        case Failed:
            sr.Status = StepCompFailed
        default:
            sr.Status = StepCompUnknown
        }
        return
    }
    switch out {
    case Success:
        sr.Status = StepSucceeded
    case Failed:
        sr.Status = StepFailed
    default:
        sr.Status = StepUnknown
    }
}

// compensate 从失败步骤的前一步开始逆序补偿所有已成功步骤。
func (c *Coordinator) compensate(ctx context.Context, rec *TaskRecord, steps []Step, failedIdx int) error {
    rec.Status = StCompensating
    rec.UpdatedAt = c.ts()
    if err := c.store.Save(ctx, rec); err != nil {
        return err
    }
    for i := failedIdx - 1; i >= 0; i-- {
        sr := &rec.Steps[i]
        if sr.Status != StepSucceeded && sr.Status != StepCompUnknown {
            continue
        }
        def := steps[i].Compensate
        if def.Do == nil {
            continue
        }
        if sr.Status == StepCompUnknown {
            // 补偿结果未知：先查询核实。
            if def.Query != nil {
                out, detail, err := def.Query(ctx)
                if errors.Is(err, ErrUnknownResult) {
                    out = Unknown
                } else if err != nil {
                    return err
                }
                if out != Unknown {
                    c.applyOutcome(rec, sr, out, detail, true)
                    if err := c.store.Save(ctx, rec); err != nil {
                        return err
                    }
                    continue
                }
            }
            return c.pause(ctx, rec, fmt.Sprintf("步骤 %s 补偿结果未知，等待人工核实", sr.Name))
        }
        sr.Attempts++
        out, detail, err := def.Do(ctx)
        if errors.Is(err, ErrUnknownResult) {
            out = Unknown
        } else if err != nil {
            return err
        }
        c.applyOutcome(rec, sr, out, detail, true)
        if err := c.store.Save(ctx, rec); err != nil {
            return err
        }
        switch sr.Status {
        case StepCompensated:
            continue
        case StepCompFailed:
            return c.pause(ctx, rec, fmt.Sprintf("步骤 %s 补偿明确失败，需要人工介入", sr.Name))
        default:
            return c.pause(ctx, rec, fmt.Sprintf("步骤 %s 补偿响应丢失，等待人工核实", sr.Name))
        }
    }
    rec.Status = StCompensated
    rec.UpdatedAt = c.ts()
    return c.store.Save(ctx, rec)
}

func (c *Coordinator) pause(ctx context.Context, rec *TaskRecord, note string) error {
    rec.Status = StPaused
    rec.PauseNote = note
    rec.UpdatedAt = c.ts()
    if err := c.store.Save(ctx, rec); err != nil {
        return err
    }
    return ErrPaused
}

// Pause 由操作者/争议管理器暂停任务，保留已发生的效果。
func (c *Coordinator) Pause(ctx context.Context, taskID, note string) error {
    rec, err := c.store.Load(ctx, taskID)
    if err != nil {
        return err
    }
    if rec == nil {
        return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
    }
    if rec.Status == StCommitted || rec.Status == StCompensated {
        return ErrNotResumable
    }
    rec.Status = StPaused
    rec.PauseNote = note
    rec.UpdatedAt = c.ts()
    return c.store.Save(ctx, rec)
}

// Resume 解除暂停，允许下一次 Run 继续；不改变任何步骤已确认的效果。
func (c *Coordinator) Resume(ctx context.Context, taskID string) error {
    rec, err := c.store.Load(ctx, taskID)
    if err != nil {
        return err
    }
    if rec == nil {
        return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
    }
    if rec.Status != StPaused {
        return fmt.Errorf("coordinator: task %s is not paused", taskID)
    }
    rec.Status = StRunning
    rec.PauseNote = ""
    rec.UpdatedAt = c.ts()
    return c.store.Save(ctx, rec)
}
