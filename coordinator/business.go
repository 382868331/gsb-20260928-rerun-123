package coordinator

import (
    "context"
    "fmt"
    "sync"
)

// 本地虚构业务服务：账户与库存。所有动作按效果标识幂等，
// 并提供查询接口，供协调者在结果未知时核实真实效果。

// Transport 模拟不可靠网络：可配置为"执行了但响应丢失"。
type Transport struct {
    // LoseResponse 返回 true 时，业务效果已发生但调用方收到 ErrUnknownResult。
    LoseResponse func(op string) bool
}

func (t *Transport) wrap(op, effectKey string, err error) (string, error) {
    if err != nil {
        return effectKey, err
    }
    if t != nil && t.LoseResponse != nil && t.LoseResponse(op) {
        return effectKey, ErrUnknownResult
    }
    return effectKey, nil
}

// AccountService 模拟账户扣款/退款。
type AccountService struct {
    mu        sync.Mutex
    Balances  map[string]int
    Debits    map[string]bool // effectKey -> 扣款已生效
    Refunds   map[string]bool // effectKey -> 退款已生效
    transport *Transport
}

func NewAccountService(balances map[string]int, t *Transport) *AccountService {
    return &AccountService{Balances: balances, Debits: map[string]bool{}, Refunds: map[string]bool{}, transport: t}
}

func (a *AccountService) Debit(_ context.Context, account string, amount int, effectKey string) (string, error) {
    a.mu.Lock()
    defer a.mu.Unlock()
    if a.Debits[effectKey] {
        return a.transport.wrap("debit", effectKey, nil) // 幂等：同效果标识不重复扣
    }
    if a.Balances[account] < amount {
        return effectKey, fmt.Errorf("account %s: insufficient funds", account)
    }
    a.Balances[account] -= amount
    a.Debits[effectKey] = true
    return a.transport.wrap("debit", effectKey, nil)
}

func (a *AccountService) Refund(_ context.Context, account string, amount int, effectKey string) error {
    a.mu.Lock()
    defer a.mu.Unlock()
    if a.Refunds[effectKey] {
        return nil
    }
    a.Balances[account] += amount
    a.Refunds[effectKey] = true
    return nil
}

// QueryDebit 核实扣款效果；Refund 同理可查询。
func (a *AccountService) QueryDebit(_ context.Context, effectKey string) (Outcome, error) {
    a.mu.Lock()
    defer a.mu.Unlock()
    if a.Debits[effectKey] {
        return Success, nil
    }
    return Failed, nil
}

func (a *AccountService) QueryRefund(_ context.Context, effectKey string) (Outcome, error) {
    a.mu.Lock()
    defer a.mu.Unlock()
    if a.Refunds[effectKey] {
        return Success, nil
    }
    return Failed, nil
}

// InventoryService 模拟库存预留/提交/释放。
type InventoryService struct {
    mu        sync.Mutex
    Stock     map[string]int
    Reserved  map[string]bool // effectKey -> 已预留
    Committed map[string]bool // effectKey -> 已提交出库
    Released  map[string]bool // effectKey -> 预留已释放
    transport *Transport
}

func NewInventoryService(stock map[string]int, t *Transport) *InventoryService {
    return &InventoryService{Stock: stock, Reserved: map[string]bool{}, Committed: map[string]bool{}, Released: map[string]bool{}, transport: t}
}

func (inv *InventoryService) Reserve(_ context.Context, sku string, qty int, effectKey string) (string, error) {
    inv.mu.Lock()
    defer inv.mu.Unlock()
    if inv.Reserved[effectKey] {
        return inv.transport.wrap("reserve", effectKey, nil)
    }
    if inv.Stock[sku] < qty {
        return effectKey, fmt.Errorf("sku %s: out of stock", sku)
    }
    inv.Stock[sku] -= qty
    inv.Reserved[effectKey] = true
    return inv.transport.wrap("reserve", effectKey, nil)
}

func (inv *InventoryService) Commit(_ context.Context, effectKey string) (string, error) {
    inv.mu.Lock()
    defer inv.mu.Unlock()
    if !inv.Reserved[effectKey] {
        return effectKey, fmt.Errorf("reservation %s not found", effectKey)
    }
    inv.Committed[effectKey] = true
    return inv.transport.wrap("commit", effectKey, nil)
}

func (inv *InventoryService) Release(_ context.Context, sku string, qty int, effectKey string) error {
    inv.mu.Lock()
    defer inv.mu.Unlock()
    if inv.Released[effectKey] {
        return nil
    }
    if inv.Reserved[effectKey] && !inv.Committed[effectKey] {
        inv.Stock[sku] += qty
    }
    inv.Released[effectKey] = true
    return nil
}

func (inv *InventoryService) QueryReserve(_ context.Context, effectKey string) (Outcome, error) {
    inv.mu.Lock()
    defer inv.mu.Unlock()
    if inv.Reserved[effectKey] {
        return Success, nil
    }
    return Failed, nil
}

func (inv *InventoryService) QueryRelease(_ context.Context, effectKey string) (Outcome, error) {
    inv.mu.Lock()
    defer inv.mu.Unlock()
    if inv.Released[effectKey] {
        return Success, nil
    }
    return Failed, nil
}
