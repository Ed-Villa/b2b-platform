package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"time"
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func Handler(catalog Catalog) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /products/{productId}", func(w http.ResponseWriter, r *http.Request) {
		id, market := r.PathValue("productId"), r.URL.Query().Get("market")
		if !identifier.MatchString(id) || (market != "MX" && market != "CO" && market != "PE") {
			fail(w, r, 400, "INVALID_ARGUMENT", "Valid productId and market are required")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		product, err := catalog.Find(ctx, id, market)
		if errors.Is(err, ErrNotFound) {
			fail(w, r, 404, "NOT_FOUND", "Product not found")
			return
		}
		if err != nil {
			fail(w, r, 503, "UNAVAILABLE", "Product lookup unavailable")
			return
		}
		respond(w, 200, product)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fail(w, r, 404, "NOT_FOUND", "Route not found") })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlation := r.Header.Get("X-Correlation-Id")
		if !identifier.MatchString(correlation) {
			var b [16]byte
			_, _ = rand.Read(b[:])
			correlation = hex.EncodeToString(b[:])
		}
		r.Header.Set("X-Correlation-Id", correlation)
		w.Header().Set("X-Correlation-Id", correlation)
		start := time.Now()
		mux.ServeHTTP(w, r)
		slog.Info("request", "correlationId", correlation, "durationMs", time.Since(start).Milliseconds())
	})
}
func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
func fail(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	respond(w, status, map[string]string{"code": code, "message": message, "correlationId": r.Header.Get("X-Correlation-Id")})
}
