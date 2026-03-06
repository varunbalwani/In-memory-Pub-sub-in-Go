package config

import (
	"flag"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration for the broker.
type Config struct {
	Addr               string
	QueueSize          int
	HeartbeatInterval  time.Duration
	BackpressurePolicy string
	HistorySize        int
	ShutdownTimeout    time.Duration
}

// Parse reads configuration from flags, falling back to environment variables,
// then to built-in defaults.
func Parse() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.Addr, "addr", envOrString("ADDR", ":8080"), "HTTP/WebSocket listen address")
	flag.IntVar(&cfg.QueueSize, "queue-size", envOrInt("QUEUE_SIZE", 256), "Per-subscriber send queue depth")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat", envOrDuration("HEARTBEAT_INTERVAL", 30*time.Second), "Server heartbeat interval")
	flag.StringVar(&cfg.BackpressurePolicy, "backpressure", envOrString("BACKPRESSURE_POLICY", "drop"), "Backpressure policy: drop or disconnect")
	flag.IntVar(&cfg.HistorySize, "history-size", envOrInt("HISTORY_SIZE", 100), "Per-topic message history depth")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", envOrDuration("SHUTDOWN_TIMEOUT", 10*time.Second), "Graceful shutdown deadline")

	flag.Parse()
	return cfg
}

func envOrString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envOrDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
