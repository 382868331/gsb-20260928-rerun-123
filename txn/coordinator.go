package txn

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Registry 描述一个流程的步骤顺序和实现。流程定义与协调记录分离：
// 记录只存 Step 的 Kind，恢复时通过 Registry 重新取回实现。
type Registry struct {
	order []string
	steps map[string]Step
}

func NewRegistry(stepKinds ...string) *Registry {
	return &Registry{order: append([]string(nil), stepKinds...), steps: map[string]Step{}}
}

func (r *Registry) Register(s Step) *Registry {
	r.steps[s.Kind()] = s
	return r
}

// Flow 绑定流程名与步骤顺序，多个流程可复用同一个协调器和 Store。
type Flow struct {
	Name     string
	Registry *Registry
}

// Coordinator 按持久记录驱动事务，本身无内存态，可随时重启。
type Coordinator struct {
	store Store
	flows map[string]*Registry
}

func NewCoordinator(store Store, flows ...Flow) *Coordinator {
	c := &Coordinator{store: store, flows: map[string]*Registry{}}
	for _, f := range flows {
		c.flows[f.Name] = f.Registry
	}
	return c
}

func (c *Coordinator) registry(flow string) (*Registry, error) {
	r, ok := c.flows[flow]
	if !ok {
		return nil, fmt.Errorf("txn: unknown flow %q", flow)
	}
	return r, nil
}

func (c *Coordinator) stepImpl(rec *Record, kind string) (Step, error) {
	r, err := c.registry(rec.Flow)
	if err != nil {
		return nil, err
	}
	s, ok := r.steps[kind]
	if !ok {
		return nil, fmt.Errorf("txn: step kind %q not registered for flow %q", kind, rec.Flow)
	}
	return s, nil
}

// Begin 创建一条新事务记录。业务输入不进入记录本体，只保留 InputRef。
func (c *Coordinator) Begin(ctx context.Context, flow, txnID string, input Input) (*Record, error) {
	if _, err := c.registry(flow); err != nil {
		return nil, err
	}
	if _, err := c.store.Load(ctx, txnID); err == nil {
		return nil, fmt.Errorf("txn: transaction %q already exists", txnID)
	} else if err != ErrNotFound {
		return nil, err
	}
	r := c.flows[flow]
	now := time.Now().UTC()
	rec := &Record{
		ID: txnID, Flow: flow, InputRef: input.Ref,
		Status: TxnRunning, CreatedAt: now, UpdatedAt: now,
	}
	for _, kind := range r.order {
		rec.Steps = append(rec.Steps, StepRecord{
			ID: txnID + "-" + kind, Kind: kind,
			Status: StepPending, UpdatedAt: now,
		})
	}
	if err := c.store.Save(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// Load 只读取出持久记录，便于检查仍需人工/外部处理的状态。
func (c *Coordinator) Load(ctx context.Context, txnID string) (*Record, error) {
	return c.store.Load(ctx, txnID)
}

// Drive 按记录继续执行。input.Data 由调用方在每次调用时提供，
// 协调库不缓存业务输入；记录中只有 InputRef。
// 返回 ErrBlocked 时，事务已持久化为 blocked，绝未越过未知步骤。
func (c *Coordinator) Drive(ctx context.Context, txnID string, input Input) (*Record, error) {
	rec, err := c.store.Load(ctx, txnID)
	if err != nil {
		return nil, err
	}
	if input.Ref != "" && input.Ref != rec.InputRef {
		return nil, fmt.Errorf("txn: input ref %q does not match record %q", input.Ref, rec.InputRef)
	}
	if rec.Status == TxnCommitted || rec.Status == TxnCompensated {
		return rec, nil
	}

	// 正向阶段：按顺序推进，遇到未知结果立即停下，绝不越过。
	if rec.Status == TxnBlocked {
		sr := rec.step(rec.BlockedAt)
		if sr != nil && (sr.Status == StepCompensating || sr.Status == StepCompFailed) {
			return c.compensate(ctx, rec, input)
		}
	}
	if rec.Status == TxnRunning || rec.Status == TxnBlocked {
		for i := range rec.Steps {
			sr := &rec.Steps[i]
			switch sr.Status {
			case StepSucceeded:
				continue
			case StepPending:
				if err := c.execute(ctx, rec, sr, input); err != nil {
					return c.handleForwardError(ctx, rec, sr, err)
				}
			case StepExecuting:
				// 重启后遇到“已发出、未确认”的步骤：先对账，不盲目重放。
				if err := c.reconcile(ctx, rec, sr, input); err != nil {
					return c.handleForwardError(ctx, rec, sr, err)
				}
				if sr.Status == StepExecuting {
					return c.block(ctx, rec, sr.ID, "step outcome is still unknown after reconcile")
				}
				if sr.Status == StepFailed {
					return c.startCompensating(ctx, rec, sr)
				}
			case StepFailed:
				return c.startCompensating(ctx, rec, sr)
			default:
				return c.block(ctx, rec, sr.ID, "unexpected step status during forward phase: "+string(sr.Status))
			}
		}
		rec.Status = TxnCommitted
		rec.BlockedAt, rec.Reason = "", ""
		rec.UpdatedAt = time.Now().UTC()
		if err := c.store.Save(ctx, rec); err != nil {
			return nil, err
		}
		return rec, nil
	}

	// 补偿阶段：只补偿已确认成功的步骤，逆序、可重复确认。
	return c.compensate(ctx, rec, input)
}

func (c *Coordinator) handleForwardError(ctx context.Context, rec *Record, sr *StepRecord, err error) (*Record, error) {
	if IsUncertain(err) {
		return c.block(ctx, rec, sr.ID, "step request outcome unknown: "+err.Error())
	}
	sr.Status = StepFailed
	sr.LastError = err.Error()
	sr.UpdatedAt = time.Now().UTC()
	return c.startCompensating(ctx, rec, sr)
}

func (c *Coordinator) block(ctx context.Context, rec *Record, stepID, reason string) (*Record, error) {
	rec.Status = TxnBlocked
	rec.BlockedAt = stepID
	rec.Reason = reason
	rec.UpdatedAt = time.Now().UTC()
	if err := c.store.Save(ctx, rec); err != nil {
		return nil, err
	}
	return rec, fmt.Errorf("%w at step %s: %s", ErrBlocked, stepID, reason)
}

func (c *Coordinator) startCompensating(ctx context.Context, rec *Record, failed *StepRecord) (*Record, error) {
	rec.Status = TxnCompensating
	rec.BlockedAt = failed.ID
	rec.Reason = "forward execution failed: " + failed.LastError
	rec.UpdatedAt = time.Now().UTC()
	if err := c.store.Save(ctx, rec); err != nil {
		return nil, err
	}
	return c.compensate(ctx, rec, Input{Ref: rec.InputRef})
}

func (c *Coordinator) execute(ctx context.Context, rec *Record, sr *StepRecord, input Input) error {
	sr.Status = StepExecuting
	sr.Attempt = newAttemptID()
	sr.LastError = ""
	sr.UpdatedAt = time.Now().UTC()
	if err := c.store.Save(ctx, rec); err != nil {
		return err
	}
	impl, err := c.stepImpl(rec, sr.Kind)
	if err != nil {
		return err
	}
	sc := StepContext{
		Context: ctx, TxnID: rec.ID, StepID: sr.ID,
		Attempt: sr.Attempt, Input: input,
	}
	res, err := impl.Execute(sc)
	if err != nil {
		if !IsUncertain(err) {
			sr.Status = StepFailed
			sr.LastError = err.Error()
			sr.UpdatedAt = time.Now().UTC()
			_ = c.store.Save(ctx, rec)
		}
		// 未知错误不覆盖 executing 状态：超时可能已经生效。
		return err
	}
	sr.Status = StepSucceeded
	sr.Result = &res
	sr.UpdatedAt = time.Now().UTC()
	return c.store.Save(ctx, rec)
}

func (c *Coordinator) reconcile(ctx context.Context, rec *Record, sr *StepRecord, input Input) error {
	impl, err := c.stepImpl(rec, sr.Kind)
	if err != nil {
		return err
	}
	recImpl, ok := impl.(Reconciler)
	if !ok {
		return nil // 无对账能力时保持 executing，由外部确认
	}
	sc := StepContext{
		Context: ctx, TxnID: rec.ID, StepID: sr.ID,
		Attempt: sr.Attempt, Input: input,
	}
	outcome, err := recImpl.Reconcile(sc)
	if err != nil {
		return err
	}
	switch outcome {
	case OutcomeSucceeded:
		// 对账确认成功：执行确认在成功后丢失，现在补上记录。
		sr.Status = StepSucceeded
		sr.LastError = ""
	case OutcomeFailed:
		sr.Status = StepFailed
		sr.LastError = "confirmed failed by reconcile"
	case OutcomeUnknown:
		return nil
	default:
		return fmt.Errorf("txn: invalid reconcile outcome %d", outcome)
	}
	sr.UpdatedAt = time.Now().UTC()
	return c.store.Save(ctx, rec)
}

func (c *Coordinator) compensate(ctx context.Context, rec *Record, input Input) (*Record, error) {
	rec.Status = TxnCompensating
	for i := len(rec.Steps) - 1; i >= 0; i-- {
		sr := &rec.Steps[i]
		if sr.Status != StepSucceeded &&
			sr.Status != StepCompensating &&
			sr.Status != StepCompFailed {
			continue // pending/executing/failed 且未确认生效的步骤不补偿
		}
		impl, err := c.stepImpl(rec, sr.Kind)
		if err != nil {
			return nil, err
		}
		// compensating / comp_failed 都允许再次发出确认：补偿必须幂等可重试。
		var succeeded Result
		if sr.Result != nil {
			succeeded = *sr.Result
		}
		sc := StepContext{
			Context: ctx, TxnID: rec.ID, StepID: sr.ID,
			Attempt: sr.Attempt, Input: input,
		}
		if sr.Status == StepSucceeded || sr.Status == StepCompFailed {
			sr.Status = StepCompensating
			sr.UpdatedAt = time.Now().UTC()
			if err := c.store.Save(ctx, rec); err != nil {
				return nil, err
			}
		}
		if err := impl.Compensate(sc, succeeded); err != nil {
			sr.Status = StepCompFailed
			sr.LastError = err.Error()
			sr.UpdatedAt = time.Now().UTC()
			_ = c.store.Save(ctx, rec)
			return c.block(ctx, rec, sr.ID, "compensation failed: "+err.Error())
		}
		sr.Status = StepCompensated
		sr.LastError = ""
		sr.UpdatedAt = time.Now().UTC()
		if err := c.store.Save(ctx, rec); err != nil {
			return nil, err
		}
	}
	rec.Status = TxnCompensated
	rec.BlockedAt, rec.Reason = "", ""
	rec.UpdatedAt = time.Now().UTC()
	if err := c.store.Save(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func newAttemptID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
