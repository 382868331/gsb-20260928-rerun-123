package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"sagalib/txn"
)

func main() {
	storeDir := "./txn-data"
	if len(os.Args) > 1 {
		storeDir = os.Args[1]
	}
	store, err := txn.NewFileStore(storeDir)
	if err != nil {
		log.Fatal(err)
	}

	backend := newFakeBackend()
	reg := txn.NewRegistry("reserve_inventory", "create_payment", "create_order").
		Register(&ReserveInventory{Backend: backend, LoseConfirm: true}).
		Register(&CreatePayment{Backend: backend}).
		Register(&CreateOrder{Backend: backend})
	coord := txn.NewCoordinator(store, txn.Flow{Name: "order", Registry: reg})

	ctx := context.Background()
	in := OrderInput{OrderID: "O-1001", UserID: "u-7", Sku: "sku-9", Qty: 1, Amount: 1990}
	txnID := "txn-" + in.OrderID

	if _, err := coord.Begin(ctx, "order", txnID, txn.Input{Ref: in.ref()}); err != nil {
		log.Fatal(err)
	}

	// 第一次驱动：库存预占已生效，但成功确认丢失 -> blocked，不越过。
	_, err = coord.Drive(ctx, txnID, txn.Input{Ref: in.ref(), Data: in})
	fmt.Printf("drive 1: %v\n", err)
	printRecord(coord, ctx, txnID)

	// 第二次驱动：重新对账确认预占成功，流程继续直到 committed。
	rec, err := coord.Drive(ctx, txnID, txn.Input{Ref: in.ref(), Data: in})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("drive 2: txn status = %s\n", rec.Status)
	printRecord(coord, ctx, txnID)
}

func printRecord(c *txn.Coordinator, ctx context.Context, id string) {
	rec, err := c.Load(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("record: status=%s blocked_at=%q reason=%q\n", rec.Status, rec.BlockedAt, rec.Reason)
	for _, s := range rec.Steps {
		fmt.Printf("  - %-18s %-12s attempt=%s\n", s.Kind, s.Status, s.Attempt)
	}
}
