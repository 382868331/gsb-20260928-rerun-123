package txn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNotFound 表示协调记录不存在。
var ErrNotFound = errors.New("txn: record not found")

// FileStore 把每个事务记录保存为独立 JSON 文件，
// 写入采用 临时文件 + fsync + 原子 rename，保证重启后可继续。
type FileStore struct {
	dir string
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("txn: create store dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(txnID string) string {
	return filepath.Join(s.dir, txnID+".json")
}

func (s *FileStore) Load(_ context.Context, txnID string) (*Record, error) {
	data, err := os.ReadFile(s.path(txnID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec Record
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("txn: decode record: %w", err)
	}
	return &rec, nil
}

func (s *FileStore) Save(_ context.Context, rec *Record) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(rec.ID) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path(rec.ID)); err != nil {
		return err
	}
	dir, err := os.Open(s.dir)
	if err == nil {
		err = dir.Sync()
		dir.Close()
	}
	return err
}
