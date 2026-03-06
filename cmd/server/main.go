package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"pubsub-system/internal/api"
	"pubsub-system/internal/broker"
	"pubsub-system/internal/config"
	"pubsub-system/internal/ws"
)

func main() {
	cfg := config.Parse()

	b := broker.NewBroker(cfg)
	b.StartHeartbeat()

	apiHandler := &api.Handler{Broker: b}
	wsHandler := &ws.Handler{Broker: b, QueueSize: cfg.QueueSize}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /topics", apiHandler.CreateTopicHandler)
	mux.HandleFunc("DELETE /topics/{name}", apiHandler.DeleteTopicHandler)
	mux.HandleFunc("GET /topics", apiHandler.ListTopicsHandler)
	mux.HandleFunc("GET /health", apiHandler.HealthHandler)
	mux.HandleFunc("GET /stats", apiHandler.StatsHandler)
	mux.Handle("GET /ws", wsHandler)

	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("listening on %s", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	b.Shutdown(shutdownCtx)
	log.Println("shutdown complete")
}
