package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRepository(t *testing.T) {
	r := Seed()
	if len(r.products) < 10 {
		t.Fatal("insufficient seed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Find(ctx, "PRD-001", "MX"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := r.Find(context.Background(), "PRD-001", "CO"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if p, err := (Catalog{r}).Find(context.Background(), "PRD-001", "MX"); err != nil || p.Status != "ACTIVE" {
		t.Fatal(p, err)
	}
}
func TestHandler(t *testing.T) {
	for _, test := range []struct {
		path   string
		status int
	}{
		{"/products/PRD-001?market=MX", 200}, {"/products/PRD-001?market=CO", 404},
		{"/products/PRD-001?market=US", 400}, {"/products/PRD-001", 400}, {"/health", 200},
	} {
		t.Run(test.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			req.Header.Set("X-Correlation-Id", "event-1")
			res := httptest.NewRecorder()
			Handler(Catalog{Seed()}).ServeHTTP(res, req)
			if res.Code != test.status {
				t.Fatalf("got %d want %d", res.Code, test.status)
			}
			if res.Header().Get("X-Correlation-Id") != "event-1" {
				t.Fatal("correlation lost")
			}
			var body map[string]any
			if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if test.status >= 400 && body["code"] == nil {
				t.Fatal("missing error contract")
			}
		})
	}
}
