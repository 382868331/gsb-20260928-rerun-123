package txn

import "time"

// TxnStatus 是事务整体状态。
type TxnStatus string

const (
	// TxnRunning 仍在正向执行。
	TxnRunning TxnStatus = "running"
	// TxnCommitted 所有步骤确认成功。
	TxnCommitted TxnStatus = "committed"
	// TxnCompensating 已确定失败，正在逆序补偿。
	TxnCompensating TxnStatus = "compensating"
	// TxnCompensated 所有需要补偿的步骤均已确认补偿。
	TxnCompensated TxnStatus = "compensated"
	// TxnBlocked 停在结果未知或补偿失败的步骤上，等待外部处理后恢复。
	TxnBlocked TxnStatus = "blocked"
)

// StepStatus 是单个步骤的状态。
type StepStatus string

const (
	StepPending      StepStatus = "pending"      // 尚未执行
	StepExecuting    StepStatus = "executing"    // 请求已发出，结果未落盘/未知
	StepSucceeded    StepStatus = "succeeded"    // 执行确认成功
	StepFailed       StepStatus = "failed"       // 执行确认失败（未生效或业务失败）
	StepCompensating StepStatus = "compensating" // 补偿请求已发出，确认可能丢失
	StepCompensated  StepStatus = "compensated"  // 补偿确认成功
	StepCompFailed   StepStatus = "comp_failed"  // 补偿明确失败，需处理后重试
)

// StepRecord 是一个步骤在协调记录中的持久状态。
type StepRecord struct {
	ID        string     `json:"id"`
	Kind      string     `json:"kind"`
	Status    StepStatus `json:"status"`
	Attempt   string     `json:"attempt,omitempty"`
	Result    *Result    `json:"result,omitempty"`
	LastError string     `json:"last_error,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// Record 是一个事务实例的完整协调记录，与步骤实现、业务输入分别存储。
type Record struct {
	ID        string       `json:"id"`
	Flow      string       `json:"flow"`
	InputRef  string       `json:"input_ref"`
	Status    TxnStatus    `json:"status"`
	Steps     []StepRecord `json:"steps"`
	BlockedAt string       `json:"blocked_step,omitempty"`
	Reason    string       `json:"reason,omitempty"`
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
	Version   int          `json:"version"`
}

func (r *Record) step(id string) *StepRecord {
	for i := range r.Steps {
		if r.Steps[i].ID == id {
			return &r.Steps[i]
		}
	}
	return nil
}
