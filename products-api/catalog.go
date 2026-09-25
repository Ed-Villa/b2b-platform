package main

import (
	"context"
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("product not found")

type Product struct {
	ProductID   string `json:"productId"`
	Name        string `json:"name"`
	SKU         string `json:"sku"`
	Status      string `json:"status"`
	TaxCategory string `json:"taxCategory"`
}

type ProductRepository interface {
	Find(context.Context, string, string) (Product, error)
}
type Catalog struct{ repository ProductRepository }

func (c Catalog) Find(ctx context.Context, id, market string) (Product, error) {
	return c.repository.Find(ctx, id, market)
}

type MemoryRepository struct{ products map[string]Product }

func (r MemoryRepository) Find(ctx context.Context, id, market string) (Product, error) {
	if err := ctx.Err(); err != nil {
		return Product{}, err
	}
	p, ok := r.products[market+":"+id]
	if !ok {
		return Product{}, ErrNotFound
	}
	return p, nil
}
func Seed() MemoryRepository {
	products := make(map[string]Product)
	markets := []string{"MX", "CO", "PE"}
	categories := []string{"STANDARD", "REDUCED", "EXEMPT"}
	for i := 1; i <= 12; i++ {
		market := markets[(i-1)/4]
		id := fmt.Sprintf("PRD-%03d", i)
		status := "ACTIVE"
		if i%4 == 0 {
			status = "DISCONTINUED"
		}
		products[market+":"+id] = Product{id, "Bebida " + id, "SKU-" + id, status, categories[(i-1)%3]}
	}
	return MemoryRepository{products}
}
