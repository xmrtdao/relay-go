package agent

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Manager handles agent registration, lifecycle, and routing.
type Manager struct {
	agents    map[string]*Agent
	mu        sync.RWMutex
	offlineTh time.Duration
	stopCh    chan struct{}
}

// NewManager creates a new agent manager.
func NewManager(offlineThreshold time.Duration) *Manager {
	m := &Manager{
		agents:    make(map[string]*Agent),
		offlineTh: offlineThreshold,
		stopCh:    make(chan struct{}),
	}
	go m.reaperLoop()
	return m
}

// Register adds a new agent or updates an existing one.
func (m *Manager) Register(a *Agent) error {
	if a.ID == "" {
		return fmt.Errorf("agent ID is required")
	}
	if a.Name == "" {
		a.Name = a.ID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.agents[a.ID]
	if exists {
		// Update existing agent
		existing.mu.Lock()
		existing.Status = StatusIdle
		existing.LastSeen = time.Now()
		if a.Endpoint != "" {
			existing.Endpoint = a.Endpoint
		}
		if a.Version != "" {
			existing.Version = a.Version
		}
		if len(a.Capabilities) > 0 {
			existing.Capabilities = a.Capabilities
		}
		existing.mu.Unlock()
		return nil
	}

	a.ConnectedAt = time.Now()
	a.LastSeen = time.Now()
	if a.Status == "" {
		a.Status = StatusIdle
	}
	m.agents[a.ID] = a
	log.Printf("[agent] registered: %s (%s) — capabilities: %v", a.Name, a.Role, a.Capabilities)
	return nil
}

// Unregister removes an agent from the fleet.
func (m *Manager) Unregister(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if a, exists := m.agents[id]; exists {
		a.UpdateStatus(StatusOffline)
		delete(m.agents, id)
		log.Printf("[agent] unregistered: %s", a.Name)
	}
}

// Get retrieves an agent by ID.
func (m *Manager) Get(id string) (*Agent, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.agents[id]
	if !ok {
		return nil, false
	}
	snap := a.Snapshot()
	return &snap, true
}

// List returns all registered agents.
func (m *Manager) List() []Agent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Agent, 0, len(m.agents))
	for _, a := range m.agents {
		result = append(result, a.Snapshot())
	}
	return result
}

// ListByStatus returns agents filtered by status.
func (m *Manager) ListByStatus(status AgentStatus) []Agent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []Agent
	for _, a := range m.agents {
		snap := a.Snapshot()
		if snap.Status == status {
			result = append(result, snap)
		}
	}
	return result
}

// FindBestAgent picks the best idle agent for a given capability.
// Returns nil if no suitable agent is available.
func (m *Manager) FindBestAgent(capability string, preferRole string) *Agent {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var best *Agent
	for _, a := range m.agents {
		snap := a.Snapshot()
		if snap.Status != StatusIdle {
			continue
		}
		if !snap.IsOnline(m.offlineTh) {
			continue
		}
		if capability != "" && !snap.CanHandle(capability) {
			continue
		}
		if best == nil {
			best = &snap
			continue
		}
		// Prefer role match
		if preferRole != "" && snap.Role == preferRole && best.Role != preferRole {
			best = &snap
		}
	}
	return best
}

// Stats returns aggregate fleet statistics.
func (m *Manager) Stats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]int{
		"total":   len(m.agents),
		"idle":    0,
		"busy":    0,
		"offline": 0,
		"error":   0,
	}
	for _, a := range m.agents {
		snap := a.Snapshot()
		switch snap.Status {
		case StatusIdle:
			stats["idle"]++
		case StatusBusy:
			stats["busy"]++
		case StatusOffline:
			stats["offline"]++
		case StatusError:
			stats["error"]++
		}
		if !snap.IsOnline(m.offlineTh) {
			stats["offline"]++
		}
	}
	return stats
}

// Stop shuts down the reaper loop.
func (m *Manager) Stop() {
	close(m.stopCh)
}

// reaperLoop periodically marks stale agents as offline.
func (m *Manager) reaperLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.reapStaleAgents()
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) reapStaleAgents() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, a := range m.agents {
		a.mu.Lock()
		if now.Sub(a.LastSeen) > m.offlineTh && a.Status != StatusOffline {
			a.Status = StatusOffline
			log.Printf("[agent] marked offline (stale): %s (last seen: %v ago)", a.Name, now.Sub(a.LastSeen))
		}
		a.mu.Unlock()
		_ = id
	}
}
