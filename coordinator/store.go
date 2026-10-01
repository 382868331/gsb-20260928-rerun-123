package coordinator

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "sync"
)

// StepRecord 是一个业务步骤的持久协调事实。
type StepRecord struct {
    Name       string `json:"name"`
    Status     string `json:"status"`
    Attempts   int    `json:"attempts"`
    Result     string `json:"result"`               // 最近一次确认结果
    Detail     string `json:"detail,omitempty"`    // 失败原因等
    EffectKey  string `json:"effect_key,omitempty" // 业务方返回的幂等效果标识
    UpdatedAt  string `json:"updated_at"`
}

// TaskRecord 是一个业务任务（一次完整的分布式事务）的持久记录。
type TaskRecord struct {
    TaskID    string       `json:"task_id"`
    Key       string       `json:"key"` // 调用方幂等键
    Status    string       `json:"status"`
    Steps     []StepRecord `json:"steps"`
    CreatedAt string       `json:"created_at"`
    UpdatedAt string       `json:"updated_at"`
    PauseNote string       `json:"pause_note,omitempty"`
}

// Store 抽象协调记录的持久化。实现必须保证 Update 的原子性。
type Store interface {
    // Load 按任务身份读取记录；不存在时返回 (nil, nil)。
    Load(ctx context.Context, taskID string) (*TaskRecord, error)
    // Create 仅在记录不存在时创建；已存在返回 false。
    Create(ctx context.Context, rec *TaskRecord) (bool, error)
    // Save 原子更新已有记录。
    Save(ctx context.Context, rec *TaskRecord) error
    // List 返回全部任务记录（按创建时间、任务ID排序），主要用于运维查询。
    List(ctx context.Context) ([]*TaskRecord, error)
}

// FileStore 以 JSON 文件持久化：每个任务一个文件，写入走临时文件+rename，
// 保证进程崩溃/重新创建协调者后仍能从已有事实接续。
type FileStore struct {
    mu  sync.Mutex
    dir string
}

func NewFileStore(dir string) (*FileStore, error) {
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return nil, fmt.Errorf("coordinator: create store dir: %w", err)
    }
    return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(taskID string) string {
    return filepath.Join(s.dir, taskID+".json")
}

func (s *FileStore) Load(_ context.Context, taskID string) (*TaskRecord, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    data, err := os.ReadFile(s.path(taskID))
    if errors.Is(err, os.ErrNotExist) {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    var rec TaskRecord
    if err := json.Unmarshal(data, &rec); err != nil {
        return nil, fmt.Errorf("coordinator: corrupt task %s: %w", taskID, err)
    }
    return &rec, nil
}

func (s *FileStore) Create(_ context.Context, rec *TaskRecord) (bool, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    p := s.path(rec.TaskID)
    if _, err := os.Stat(p); err == nil {
        return false, nil
    } else if !errors.Is(err, os.ErrNotExist) {
        return false, err
    }
    return true, s.writeLocked(rec)
}

func (s *FileStore) Save(_ context.Context, rec *TaskRecord) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.writeLocked(rec)
}

func (s *FileStore) writeLocked(rec *TaskRecord) error {
    data, err := json.MarshalIndent(rec, "", "  ")
    if err != nil {
        return err
    }
    tmp := s.path(rec.TaskID) + ".tmp"
    if err := os.WriteFile(tmp, data, 0o644); err != nil {
        return err
    }
    return os.Rename(tmp, s.path(rec.TaskID))
}

func (s *FileStore) List(_ context.Context) ([]*TaskRecord, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    entries, err := os.ReadDir(s.dir)
    if err != nil {
        return nil, err
    }
    var out []*TaskRecord
    for _, e := range entries {
        if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
            continue
        }
        data, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
        if err != nil {
            return nil, err
        }
        var rec TaskRecord
        if err := json.Unmarshal(data, &rec); err != nil {
            return nil, fmt.Errorf("coordinator: corrupt task file %s: %w", e.Name(), err)
        }
        out = append(out, &rec)
    }
    sort.Slice(out, func(i, j int) bool {
        if out[i].CreatedAt != out[j].CreatedAt {
            return out[i].CreatedAt < out[j].CreatedAt
        }
        return out[i].TaskID < out[j].TaskID
    })
    return out, nil
}
