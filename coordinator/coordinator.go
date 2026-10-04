package coordinator

import (
    "context"
    "errors"
    "fmt"
    "time"
)

// Step 描述一个可协调的业务步骤（如扣款、预留、提交）。
//
// Do 执行正向动作；返回 ErrUnknownResult 表示超时/响应丢失，
// 此时协调者绝不直接重试，而是先调用 Query 核实效果是否已发生。
type Step struct {
    Name string
    // Do 执行正向动作，返回业务方给出的幂等效果标识。
    Do func(ctx context.Context) (effectKey string, err error)
    // Query 按效果标识核实正向效果：Success / Failed / Unknown。
    Query func(ctx context.Context, effectKey string) (Outcome, error)
    // Compensate 执行补偿动作（如退款、释放预留）。
    Compensate func(ctx context.Context, effectKey string) error
    // QueryCompensate 核实补偿效果；补偿同样可能响应丢失。
    QueryCompensate func(ctx context.Context, effectKey string) (Outcome, error)
}

// Coordinator 基于持久记录驱动任务，崩溃后可从记录接续。
type Coordinator struct {
    store Store
    now   func() time.Time
    // Log 记录每一步为何可继续或必须等待，便于事后审计。
    Log []string
}

func NewCoordinator(store Store) *Coordinator {
    return &Coordinator{store: store, now: time.Now}
}

func (c *Coordinator) logf(format string, args ...any) {
    c.Log = append(c.Log, fmt.Sprintf(format, args...))
}

func (c *Coordinator) ts() string { return c.now().UTC().Format(time.RFC3339) }

// Run 执行或恢复一个任务。相同 taskID 重复调用会从持久记录接续，
// 已确认成功的步骤不会重复执行。
func (c *Coordinator) Run(ctx context.Context, taskID, key string, steps []Step) error {
    rec, err := c.store.Load(ctx, taskID)
    if err != nil {
        return err
    }
    if rec == nil {
        rec = &TaskRecord{TaskID: taskID, Key: key, Status: StRunning, CreatedAt: c.ts()}
        for _, s := range steps {
            rec.Steps = append(rec.Steps, StepRecord{Name: s.Name, Status: StepPending, UpdatedAt: c.ts()})
        }
        ok, err := c.store.Create(ctx, rec)
        if err != nil {
            return err
        }
        if !ok {
            return fmt.Errorf("coordinator: task %s created concurrently", taskID)
        }
        c.logf("任务 %s 建档，共 %d 步", taskID, len(steps))
    } else {
        if rec.Status == StCommitted || rec.Status == StCompensated {
            return ErrNotResumable
        }
        if len(rec.Steps) != len(steps) {
            return ErrStepMismatch
        }
        for i := range steps {
            if rec.Steps[i].Name != steps[i].Name {
                return ErrStepMismatch
            }
        }
        c.logf("任务 %s 从持久记录恢复，状态 %s", taskID, rec.Status)
    }
    if rec.Status == StPaused {
        c.logf("任务 %s 被暂停：%s；需人工恢复后才能继续", taskID, rec.PauseNote)
        return ErrPaused
    }

    for i := range steps {
        if err := c.runStep(ctx, rec, &steps[i], i); err != nil {
            if errors.Is(err, errStepFailed) {
                return c.compensate(ctx, rec, steps, i-1)
            }
            return err
        }
    }
    rec.Status = StCommitted
    rec.UpdatedAt = c.ts()
    c.logf("任务 %s 全部步骤确认成功，提交收尾", taskID)
    return c.store.Save(ctx, rec)
}

var errStepFailed = errors.New("coordinator: step failed")

func (c *Coordinator) runStep(ctx context.Context, rec *TaskRecord, s *Step, i int) error {
    sr := &rec.Steps[i]
    switch sr.Status {
    case StepSucceeded:
        c.logf("步骤 %s 已确认成功（效果 %s），跳过不重复执行", sr.Name, sr.EffectKey)
        return nil
    case StepFailed:
        return errStepFailed
    case StepUnknown:
        // 恢复时先核实，不盲目重试。
        return c.resolveUnknown(ctx, rec, s, i)
    }

    sr.Attempts++
    effectKey, err := s.Do(ctx)
    sr.UpdatedAt = c.ts()
    switch {
    case err == nil:
        sr.Status, sr.Result, sr.EffectKey = StepSucceeded, Success.String(), effectKey
        c.logf("步骤 %s 成功，效果标识 %s", sr.Name, effectKey)
    case errors.Is(err, ErrUnknownResult):
        sr.Status, sr.Result, sr.EffectKey = StepUnknown, Unknown.String(), effectKey
        sr.Detail = "响应丢失/超时，效果待核实"
        c.logf("步骤 %s 响应丢失，标记 unknown（效果标识 %s），先查询核实而非重试", sr.Name, effectKey)
        if saveErr := c.store.Save(ctx, rec); saveErr != nil {
            return saveErr
        }
        return c.resolveUnknown(ctx, rec, s, i)
    default:
        sr.Status, sr.Result, sr.Detail = StepFailed, Failed.String(), err.Error()
        c.logf("步骤 %s 明确失败：%v；已确认未生效，可安全补偿前序步骤", sr.Name, err)
        if saveErr := c.store.Save(ctx, rec); saveErr != nil {
            return saveErr
        }
        return errStepFailed
    }
    return c.store.Save(ctx, rec)
}

// resolveUnknown 通过业务方查询接口核实 unknown 步骤的真实效果。
func (c *Coordinator) resolveUnknown(ctx context.Context, rec *TaskRecord, s *Step, i int) error {
    sr := &rec.Steps[i]
    out, err := s.Query(ctx, sr.EffectKey)
    sr.UpdatedAt = c.ts()
    if err != nil || out == Unknown {
        rec.Status = StPaused
        rec.PauseNote = fmt.Sprintf("步骤 %s 效果仍未知，等待人工核实", sr.Name)
        c.logf("步骤 %s 查询仍无法确认，任务暂停等待人工核实，禁止盲目重试", sr.Name)
        _ = c.store.Save(ctx, rec)
        return ErrPaused
    }
    if out == Success {
        sr.Status, sr.Result, sr.Detail = StepSucceeded, Success.String(), ""
        c.logf("步骤 %s 经查询确认已生效（效果 %s），按成功继续，不重发请求", sr.Name, sr.EffectKey)
        return c.store.Save(ctx, rec)
    }
    sr.Status, sr.Result, sr.Detail = StepFailed, Failed.String(), "查询确认未生效"
    c.logf("步骤 %s 经查询确认未生效，可按失败处理并补偿前序步骤", sr.Name)
    if saveErr := c.store.Save(ctx, rec); saveErr != nil {
        return saveErr
    }
    return errStepFailed
}

// compensate 逆序补偿已确认成功的步骤；补偿结果未知时同样先查询。
func (c *Coordinator) compensate(ctx context.Context, rec *TaskRecord, steps []Step, from int) error {
    rec.Status = StCompensating
    rec.UpdatedAt = c.ts()
    c.logf("任务 %s 进入补偿流程，逆序处理已生效步骤", rec.TaskID)
    for i := from; i >= 0; i-- {
        sr := &rec.Steps[i]
        if sr.Status != StepSucceeded {
            continue
        }
        s := &steps[i]
        err := s.Compensate(ctx, sr.EffectKey)
        sr.UpdatedAt = c.ts()
        switch {
        case err == nil:
            sr.Status = StepCompensated
            c.logf("步骤 %s 补偿完成（效果 %s 已撤销）", sr.Name, sr.EffectKey)
        case errors.Is(err, ErrUnknownResult):
            sr.Status = StepCompUnknown
            if saveErr := c.store.Save(ctx, rec); saveErr != nil {
                return saveErr
            }
            out, qerr := s.QueryCompensate(ctx, sr.EffectKey)
            if qerr != nil || out == Unknown {
                rec.Status = StPaused
                rec.PauseNote = fmt.Sprintf("步骤 %s 补偿效果未知，等待人工核实", sr.Name)
                c.logf("步骤 %s 补偿响应丢失且查询未果，暂停等待人工核实；已完成的补偿效果保留", sr.Name)
                _ = c.store.Save(ctx, rec)
                return ErrPaused
            }
            if out == Success {
                sr.Status = StepCompensated
                c.logf("步骤 %s 补偿经查询确认已生效", sr.Name)
            } else {
                sr.Status = StepCompFailed
                rec.Status = StPaused
                rec.PauseNote = fmt.Sprintf("步骤 %s 补偿明确失败，需人工介入", sr.Name)
                c.logf("步骤 %s 补偿明确失败，暂停等待人工介入", sr.Name)
                _ = c.store.Save(ctx, rec)
                return ErrPaused
            }
        default:
            sr.Status = StepCompFailed
            rec.Status = StPaused
            rec.PauseNote = fmt.Sprintf("步骤 %s 补偿失败：%v", sr.Name, err)
            c.logf("步骤 %s 补偿失败：%v，暂停等待人工介入", sr.Name, err)
            _ = c.store.Save(ctx, rec)
            return ErrPaused
        }
        if saveErr := c.store.Save(ctx, rec); saveErr != nil {
            return saveErr
        }
    }
    rec.Status = StCompensated
    rec.UpdatedAt = c.ts()
    c.logf("任务 %s 补偿全部完成，业务上已回滚", rec.TaskID)
    return c.store.Save(ctx, rec)
}

// Resume 在人工核实后恢复暂停的任务：caller 需先通过业务方确认
// 未知步骤的真实效果并修正记录，再调用 Run 接续。
func (c *Coordinator) Resume(ctx context.Context, taskID string) error {
    rec, err := c.store.Load(ctx, taskID)
    if err != nil {
        return err
    }
    if rec == nil {
        return fmt.Errorf("coordinator: task %s not found", taskID)
    }
    if rec.Status != StPaused {
        return fmt.Errorf("coordinator: task %s is not paused", taskID)
    }
    rec.Status = StRunning
    rec.PauseNote = ""
    rec.UpdatedAt = c.ts()
    c.logf("任务 %s 人工核实完毕，恢复运行", taskID)
    return c.store.Save(ctx, rec)
}
