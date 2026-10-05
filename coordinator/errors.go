package coordinator

import "errors"

// Outcome 表示一次业务调用之后协调者能够确认的结果。
//
// 注意：超时、连接中断等情况绝不允许直接当作"没有发生"，
// 必须记录为 Unknown，先查询确认，再决定是否重试。
type Outcome int

const (
	// Unknown 表示业务方是否已经执行无法确认（超时、响应丢失等）。
	Unknown Outcome = iota
	// Success 表示业务效果已确认发生。
	Success
	// Failed 表示业务方明确拒绝/失败，可以安全地按未发生处理。
	Failed
)

func (o Outcome) String() string {
	switch o {
	case Success:
		return "success"
	case Failed:
		return "failed"
	default:
		return "unknown"
	}
}

// 任务整体状态。
const (
	StRunning      = "running"      // 正向执行中
	StCommitted    = "committed"    // 所有正向步骤成功，已收尾
	StCompensating = "compensating" // 正在按约定补偿
	StCompensated  = "compensated"  // 补偿全部完成（业务上回滚）
	StPaused       = "paused"       // 操作者暂缓收尾，等待人工恢复
)

// 单个步骤的状态。
const (
	StepPending     = "pending"      // 尚未执行
	StepSucceeded   = "succeeded"    // 正向效果已确认
	StepFailed      = "failed"       // 正向明确失败
	StepUnknown     = "unknown"      // 结果未知，需查询
	StepCompensated = "compensated"  // 已补偿
	StepCompFailed  = "comp_failed"  // 补偿明确失败
	StepCompUnknown = "comp_unknown" // 补偿结果未知，需查询
)

var (
	// ErrUnknownResult 由业务操作在超时/响应丢失时返回，
	// 协调者会把该步骤记为 unknown 而不是重试。
	ErrUnknownResult = errors.New("coordinator: operation result unknown (timeout/lost response)")
	// ErrStepMismatch 表示恢复任务时提供的步骤定义与持久记录不一致。
	ErrStepMismatch = errors.New("coordinator: step definitions do not match persisted task")
	// ErrNotResumable 表示任务已处于终态，不能再恢复处理。
	ErrNotResumable = errors.New("coordinator: task is already in a terminal state")
	// ErrPaused 表示任务被操作者暂停，当前调用到此为止。
	ErrPaused = errors.New("coordinator: task paused by operator")
	// ErrTaskNotFound 表示按任务身份找不到协调记录。
	ErrTaskNotFound = errors.New("coordinator: task not found")
)
