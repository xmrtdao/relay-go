package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Config holds all configuration for the relay daemon.
type Config struct {
	// Server settings
	Host         string        `json:"host"`
	Port         int           `json:"port"`
	ReadTimeout  time.Duration `json:"read_timeout"`
	WriteTimeout time.Duration `json:"write_timeout"`

	// WebSocket settings
	WSHeartbeatInterval time.Duration `json:"ws_heartbeat_interval"`
	WSHeartbeatTimeout  time.Duration `json:"ws_heartbeat_timeout"`

	// Supabase settings
	SupabaseURL    string `json:"supabase_url"`
	SupabaseAPIKey string `json:"supabase_api_key"`

	// Task routing
	TaskQueueSize    int           `json:"task_queue_size"`
	TaskClaimTimeout time.Duration `json:"task_claim_timeout"`

	// Agent management
	AgentOfflineThreshold time.Duration `json:"agent_offline_threshold"`

	// Logging
	LogLevel string `json:"log_level"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Host:         "0.0.0.0",
		Port:         8081,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		WSHeartbeatInterval: 15 * time.Second,
		WSHeartbeatTimeout:  45 * time.Second,

		SupabaseURL:    "",
		SupabaseAPIKey: "",

		TaskQueueSize:    100,
		TaskClaimTimeout: 30 * time.Second,

		AgentOfflineThreshold: 60 * time.Second,

		LogLevel: "info",
	}
}

// Load reads config from a JSON file, overlaying environment variables.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			if err := json.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("parse config: %w", err)
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	// Env var overrides
	if v := os.Getenv("RELAY_HOST"); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv("RELAY_PORT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Port)
	}
	if v := os.Getenv("SUPABASE_URL"); v != "" {
		cfg.SupabaseURL = v
	}
	if v := os.Getenv("SUPABASE_API_KEY"); v != "" {
		cfg.SupabaseAPIKey = v
	}
	if v := os.Getenv("RELAY_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}

	return cfg, nil
}

// ConfigPath returns the default config file path.
func ConfigPath() string {
	if v := os.Getenv("RELAY_CONFIG"); v != "" {
		return v
	}
	return filepath.Join("relay-go", "config.json")
}
