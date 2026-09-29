package main

// OrderInput 是业务输入。协调库不持久化它的内容，
// 协调记录里只保存 OrderID（InputRef）；重启恢复时由业务侧重新提供。
type OrderInput struct {
	OrderID string
	UserID  string
	Sku     string
	Qty     int
	Amount  int64
}

func (in OrderInput) ref() string { return "order:" + in.OrderID }
