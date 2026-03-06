package config

import (
	"os"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	// Ensure no env vars interfere
	for _, k := range []string{"ADDR", "QUEUE_SIZE", "HEARTBEAT_INTERVAL", "BACKPRESSURE_POLICY", "HISTORY_SIZE", "SHUTDOWN_TIMEOUT"} {
		os.Unsetenv(k)
	}

	cfg := &Config{}
	// Simulate defaults by calling helpers directly
	cfg.Addr = envOrString("ADDR", ":8080")
	cfg.QueueSize = envOrInt("QUEUE_SIZE", 256)
	cfg.HeartbeatInterval = envOrDuration("HEARTBEAT_INTERVAL", 30*time.Second)
	cfg.BackpressurePolicy = envOrString("BACKPRESSURE_POLICY", "drop")
	cfg.HistorySize = envOrInt("HISTORY_SIZE", 100)
	cfg.ShutdownTimeout = envOrDuration("SHUTDOWN_TIMEOUT", 10*time.Second)

	if cfg.Addr != ":8080" {
		t.Errorf("Addr: got %q, want %q", cfg.Addr, ":8080")
	}
	if cfg.QueueSize != 256 {
		t.Errorf("QueueSize: got %d, want 256", cfg.QueueSize)
	}
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Errorf("HeartbeatInterval: got %v, want 30s", cfg.HeartbeatInterval)
	}
	if cfg.BackpressurePolicy != "drop" {
		t.Errorf("BackpressurePolicy: got %q, want %q", cfg.BackpressurePolicy, "drop")
	}
	if cfg.HistorySize != 100 {
		t.Errorf("HistorySize: got %d, want 100", cfg.HistorySize)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout: got %v, want 10s", cfg.ShutdownTimeout)
	}
}

func TestEnvVarOverrides(t *testing.T) {
	os.Setenv("ADDR", ":9090")
	os.Setenv("QUEUE_SIZE", "512")
	os.Setenv("HEARTBEAT_INTERVAL", "1m")
	os.Setenv("BACKPRESSURE_POLICY", "disconnect")
	os.Setenv("HISTORY_SIZE", "50")
	os.Setenv("SHUTDOWN_TIMEOUT", "5s")
	defer func() {
		for _, k := range []string{"ADDR", "QUEUE_SIZE", "HEARTBEAT_INTERVAL", "BACKPRESSURE_POLICY", "HISTORY_SIZE", "SHUTDOWN_TIMEOUT"} {
			os.Unsetenv(k)
		}
	}()

	cfg := &Config{
		Addr:               envOrString("ADDR", ":8080"),
		QueueSize:          envOrInt("QUEUE_SIZE", 256),
		HeartbeatInterval:  envOrDuration("HEARTBEAT_INTERVAL", 30*time.Second),
		BackpressurePolicy: envOrString("BACKPRESSURE_POLICY", "drop"),
		HistorySize:        envOrInt("HISTORY_SIZE", 100),
		ShutdownTimeout:    envOrDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}

	if cfg.Addr != ":9090" {
		t.Errorf("Addr: got %q, want %q", cfg.Addr, ":9090")
	}
	if cfg.QueueSize != 512 {
		t.Errorf("QueueSize: got %d, want 512", cfg.QueueSize)
	}
	if cfg.HeartbeatInterval != time.Minute {
		t.Errorf("HeartbeatInterval: got %v, want 1m", cfg.HeartbeatInterval)
	}
	if cfg.BackpressurePolicy != "disconnect" {
		t.Errorf("BackpressurePolicy: got %q, want disconnect", cfg.BackpressurePolicy)
	}
	if cfg.HistorySize != 50 {
		t.Errorf("HistorySize: got %d, want 50", cfg.HistorySize)
	}
	if cfg.ShutdownTimeout != 5*time.Second {
		t.Errorf("ShutdownTimeout: got %v, want 5s", cfg.ShutdownTimeout)
	}
}

func TestEnvVarInvalidFallsBackToDefault(t *testing.T) {
	os.Setenv("QUEUE_SIZE", "not-a-number")
	os.Setenv("HEARTBEAT_INTERVAL", "not-a-duration")
	defer func() {
		os.Unsetenv("QUEUE_SIZE")
		os.Unsetenv("HEARTBEAT_INTERVAL")
	}()

	if got := envOrInt("QUEUE_SIZE", 256); got != 256 {
		t.Errorf("expected default 256 for invalid env, got %d", got)
	}
	if got := envOrDuration("HEARTBEAT_INTERVAL", 30*time.Second); got != 30*time.Second {
		t.Errorf("expected default 30s for invalid env, got %v", got)
	}
}
