package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	server := &http.Server{Addr: ":" + port, Handler: Handler(Catalog{Seed()}), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		slog.Error("listen failed", "error", err)
		os.Exit(1)
	}
	slog.Info("listening", "port", port)
	if err := serve(ctx, server, listener); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

// Serve returns as soon as Shutdown closes the listener. Wait separately for
// Shutdown to finish draining active handlers before allowing main to exit.
func serve(ctx context.Context, server *http.Server, listener net.Listener) error {
	shutdownDone := make(chan struct{})
	serveDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdown); err != nil {
				slog.Warn("graceful shutdown expired", "error", err)
				_ = server.Close()
			}
		case <-serveDone:
		}
	}()
	err := server.Serve(listener)
	close(serveDone)
	<-shutdownDone
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
