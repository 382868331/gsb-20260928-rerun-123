package main

import (
	"fmt"
	"sync"

	"sagalib/txn"
)

// 本地模拟的下游业务系统。真实环境中各自是独立服务/资源。
type fakeBackend struct {
	mu sync.Mutex
	// 已生效的执行，key = 幂等键 attemptID
	applied map[string]bool
	// 已确认的补偿，同样按幂等键去重
	compensated map[string]bool
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{applied: map[string]bool{}, compensated: map[string]bool{}}
}

func (b *fakeBackend) apply(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.applied[id] {
		return false
	}
	b.applied[id] = true
	return true
}

func (b *fakeBackend) isApplied(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.applied[id]
}

func (b *fakeBackend) undo(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.compensated[id] {
		return false
	}
	b.compensated[id] = true
	return true
}

// ReserveInventory 模拟库存预占。成功响应可被配置为丢失。
type ReserveInventory struct {
	Backend       *fakeBackend
	LoseConfirm   bool // 执行已生效但调用方拿不到成功确认
	confirmedLost bool
}

func (s *ReserveInventory) Kind() string { return "reserve_inventory" }

func (s *ReserveInventory) Execute(sc txn.StepContext) (txn.Result, error) {
	_ = sc.Input.Data.(OrderInput)
	s.Backend.apply(sc.Attempt) // 幂等键去重，重复请求不会重复预占
	if s.LoseConfirm && !s.confirmedLost {
		s.confirmedLost = true
		return txn.Result{}, txn.ErrUncertain
	}
	return txn.Result{Ref: "reservation:" + sc.Attempt, Data: map[string]any{"attempt": sc.Attempt}}, nil
}

func (s *ReserveInventory) Compensate(sc txn.StepContext, _ txn.Result) error {
	s.Backend.undo(sc.Attempt) // 重复调用安全：已补偿则为空操作
	return nil
}

func (s *ReserveInventory) Reconcile(sc txn.StepContext) (txn.Outcome, error) {
	if s.Backend.isApplied(sc.Attempt) {
		return txn.OutcomeSucceeded, nil
	}
	return txn.OutcomeFailed, nil
}

// CreatePayment 模拟支付创建。配置 FailOnce=true 时首次业务失败，触发补偿。
type CreatePayment struct {
	Backend  *fakeBackend
	FailOnce bool
}

func (s *CreatePayment) Kind() string { return "create_payment" }

func (s *CreatePayment) Execute(sc txn.StepContext) (txn.Result, error) {
	in := sc.Input.Data.(OrderInput)
	if s.FailOnce {
		s.FailOnce = false
		return txn.Result{}, fmt.Errorf("payment rejected for order %s", in.OrderID)
	}
	s.Backend.apply(sc.Attempt)
	return txn.Result{Ref: "payment:" + sc.Attempt}, nil
}

func (s *CreatePayment) Compensate(sc txn.StepContext, _ txn.Result) error {
	s.Backend.undo(sc.Attempt)
	return nil
}

// CreateOrder 模拟订单落库，本地步骤。
type CreateOrder struct{ Backend *fakeBackend }

func (s *CreateOrder) Kind() string { return "create_order" }

func (s *CreateOrder) Execute(sc txn.StepContext) (txn.Result, error) {
	s.Backend.apply(sc.Attempt)
	return txn.Result{Ref: sc.Input.Ref}, nil
}

func (s *CreateOrder) Compensate(sc txn.StepContext, _ txn.Result) error {
	s.Backend.undo(sc.Attempt)
	return nil
}
