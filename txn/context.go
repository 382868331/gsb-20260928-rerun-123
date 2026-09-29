package txn

import "context"

var deadlineExceeded = context.DeadlineExceeded

// Input 是业务输入的载体。协调库不解析其内容，只原样传给步骤；
// 协调记录里只保存调用方提供的 InputRef（如业务单号、输入快照路径）。
type Input struct {
	Ref  string
	Data any
}

// StepContext 提供给步骤实现的执行上下文。
// TxnID / StepID / AttemptID 组合可作为下游业务系统的幂等键。
type StepContext struct {
	context.Context
	TxnID   string
	StepID  string
	Attempt string
	Input   Input
}

// Result 是步骤执行成功后留下的业务结果，会写入协调记录，
// 供后续步骤和补偿使用。
type Result struct {
	Ref  string         `json:"ref,omitempty"`
	Data map[string]any `json:"data,omitempty"`
}

// Outcome 是步骤实现对账（Reconcile）后确认的真实结果。
type Outcome int

const (
	// OutcomeUnknown 仍无法确认，事务必须继续停在该步骤。
	OutcomeUnknown Outcome = iota
	// OutcomeSucceeded 确认此前的执行已经成功生效。
	OutcomeSucceeded
	// OutcomeFailed 确认此前的执行没有生效或业务上失败。
	OutcomeFailed
)

// Step 是一个可执行业务步骤。实现必须保证：
//   - Execute 携带 Attempt 幂等键，重复请求不会产生重复业务作用；
//   - Compensate 可被重复调用（确认可能丢失），重复调用结果一致。
type Step interface {
	Kind() string
	Execute(sc StepContext) (Result, error)
	Compensate(sc StepContext, succeeded Result) error
}

// Reconciler 由能够查询下游真实结果的步骤实现。
// 执行结果未知时，协调库只会通过它对账，绝不会盲目重放 Execute。
type Reconciler interface {
	Reconcile(sc StepContext) (Outcome, error)
}
