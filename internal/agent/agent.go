package agent

import (
	"sync"
	"time"
)

// AgentStatus represents an agent's current state.
type AgentStatus string

const (
	StatusIdle       AgentStatus = "idle"
	StatusBusy       AgentStatus = "busy"
	StatusOffline    AgentStatus = "offline"
	StatusError      AgentStatus = "error"
)

// Agent represents a connected agent in the fleet.
type Agent struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Role         string            `json:"role"`
	Status       AgentStatus       `json:"status"`
	Capabilities []string          `json:"capabilities"`
	Endpoint     string            `json:"endpoint"`
	LastSeen     time.Time         `json:"last_seen"`
	ConnectedAt  time.Time         `json:"connected_at"`
	Version      string            `json:"version"`
	Metadata     map[string]string `json:"metadata,omitempty"`

	mu sync.RWMutex
}

// UpdateStatus safely updates the agent's status.
func (a *Agent) UpdateStatus(s AgentStatus) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Status = s
}

// Heartbeat updates the agent's last-seen timestamp.
func (a *Agent) Heartbeat() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.LastSeen = time.Now()
}

// IsOnline checks if the agent has been seen recently.
func (a *Agent) IsOnline(timeout time.Duration) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return time.Since(a.LastSeen) < timeout
}

// CanHandle checks if the agent has a required capability.
func (a *Agent) CanHandle(capability string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, c := range a.Capabilities {
		if c == capability {
			return true
		}
	}
	return false
}

// Snapshot returns a thread-safe copy of the agent.
func (a *Agent) Snapshot() Agent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return Agent{
		ID:           a.ID,
		Name:         a.Name,
		Role:         a.Role,
		Status:       a.Status,
		Capabilities: append([]string{}, a.Capabilities...),
		Endpoint:     a.Endpoint,
		LastSeen:     a.LastSeen,
		ConnectedAt:  a.ConnectedAt,
		Version:      a.Version,
	}
}
