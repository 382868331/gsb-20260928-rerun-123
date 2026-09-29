package txn

import "errors"

// ErrUncertain 表示步骤请求已经发出，但执行结果无法确定
// （例如超时、进程在确认落盘前退出）。它不等于失败，也不等于成功。
var ErrUncertain = errors.New("txn: step outcome is uncertain")

// ErrBlocked 表示事务停在仍未确定的步骤上，恢复时不能越过它。
var ErrBlocked = errors.New("txn: blocked on an undetermined step")

// UncertainError 允许步骤实现把自定义错误标记为“结果未知”。
type UncertainError struct{ Err error }

func (e *UncertainError) Error() string { return e.Err.Error() }
func (e *UncertainError) Unwrap() error { return e.Err }

// IsUncertain 判断错误是否表示结果未知。context.DeadlineExceeded 也属于这一类。
func IsUncertain(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrUncertain) || errors.Is(err, deadlineExceeded) {
		return true
	}
	var u *UncertainError
	return errors.As(err, &u)
}
