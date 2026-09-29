package txn

import "context"

// Store 是协调记录存储。第一回合提供文件实现；
// 新流程接入时只需复用同一 Store 接口，即可复用恢复机制。
type Store interface {
	Load(ctx context.Context, txnID string) (*Record, error)
	Save(ctx context.Context, rec *Record) error
}
