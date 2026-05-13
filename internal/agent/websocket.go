package agent

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSMessage is the envelope for WebSocket messages.
type WSMessage struct {
	Type    string          `json:"type"`
	AgentID string          `json:"agent_id,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// connSession tracks a live WebSocket connection for an agent.
type connSession struct {
	agentID string
	conn    *websocket.Conn
	sendCh  chan []byte // buffered channel for outbound messages
	done    chan struct{}
}

// WSHandler manages WebSocket connections from agents, with bidirectional dispatch.
type WSHandler struct {
	manager           *Manager
	upgrader          websocket.Upgrader
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration

	// Active connection registry: agentID → session
	sessions   map[string]*connSession
	sessionsMu sync.RWMutex

	// Dispatch channel for pushing tasks to agents
	dispatchCh chan DispatchCommand

	stopCh chan struct{}
}

// DispatchCommand is an instruction to send a message to a specific agent.
type DispatchCommand struct {
	AgentID string
	Message WSMessage
	Done    chan error // optional, set for synchronous wait
}

// NewWSHandler creates a new WebSocket handler.
func NewWSHandler(manager *Manager, heartbeatInterval, heartbeatTimeout time.Duration) *WSHandler {
	h := &WSHandler{
		manager:           manager,
		heartbeatInterval: heartbeatInterval,
		heartbeatTimeout:  heartbeatTimeout,
		upgrader: websocket.Upgrader{
			CheckOrigin:    func(r *http.Request) bool { return true },
			ReadBufferSize: 4096,
			WriteBufferSize: 4096,
		},
		sessions:    make(map[string]*connSession),
		dispatchCh:  make(chan DispatchCommand, 100),
		stopCh:      make(chan struct{}),
	}
	go h.dispatchLoop()
	return h
}

// SendToAgent queues a message for delivery to a connected agent.
// Returns nil if the agent is connected and the message was queued.
func (h *WSHandler) SendToAgent(agentID string, msg WSMessage) error {
	h.sessionsMu.RLock()
	session, ok := h.sessions[agentID]
	h.sessionsMu.RUnlock()
	if !ok {
		return nil // agent not connected, that's OK — they'll poll later
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	select {
	case session.sendCh <- data:
		return nil
	default:
		log.Printf("[ws] dropping message to %s: send buffer full", agentID)
		return nil
	}
}

// DispatchChan returns the channel for dispatching commands to agents.
func (h *WSHandler) DispatchChan() chan<- DispatchCommand {
	return h.dispatchCh
}

// ServeHTTP handles the WebSocket upgrade and connection lifecycle.
func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade error: %v", err)
		return
	}

	// Wait for registration message
	_, msgData, err := conn.ReadMessage()
	if err != nil {
		log.Printf("[ws] read error during registration: %v", err)
		conn.Close()
		return
	}

	var regMsg WSMessage
	if err := json.Unmarshal(msgData, &regMsg); err != nil {
		log.Printf("[ws] invalid registration JSON: %v", err)
		conn.Close()
		return
	}

	if regMsg.Type != "register" {
		log.Printf("[ws] expected 'register', got '%s'", regMsg.Type)
		conn.Close()
		return
	}

	var agentInfo struct {
		ID           string            `json:"id"`
		Name         string            `json:"name"`
		Role         string            `json:"role"`
		Capabilities []string          `json:"capabilities"`
		Endpoint     string            `json:"endpoint"`
		Version      string            `json:"version"`
		Metadata     map[string]string `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(regMsg.Payload, &agentInfo); err != nil {
		log.Printf("[ws] invalid agent info: %v", err)
		conn.Close()
		return
	}

	agent := &Agent{
		ID:           agentInfo.ID,
		Name:         agentInfo.Name,
		Role:         agentInfo.Role,
		Capabilities: agentInfo.Capabilities,
		Endpoint:     agentInfo.Endpoint,
		Version:      agentInfo.Version,
		Status:       StatusIdle,
		LastSeen:     time.Now(),
		ConnectedAt:  time.Now(),
		Metadata:     agentInfo.Metadata,
	}

	if err := h.manager.Register(agent); err != nil {
		log.Printf("[ws] register error: %v", err)
		conn.Close()
		return
	}

	// Create session and register in the connection map
	session := &connSession{
		agentID: agent.ID,
		conn:    conn,
		sendCh:  make(chan []byte, 32),
		done:    make(chan struct{}),
	}

	h.sessionsMu.Lock()
	// Close any existing session for this agent
	if old, exists := h.sessions[agent.ID]; exists {
		close(old.done)
	}
	h.sessions[agent.ID] = session
	h.sessionsMu.Unlock()

	// Send acknowledgment
	ackPayload, _ := json.Marshal(WSMessage{
		Type:    "registered",
		Payload: json.RawMessage(`{"status":"ok","agent_id":"` + agent.ID + `"}`),
	})
	conn.WriteMessage(websocket.TextMessage, ackPayload)

	log.Printf("[ws] agent connected: %s (%s) — capabilities: %v", agent.Name, agent.Role, agent.Capabilities)

	// Start writer goroutine for outbound messages
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case data, ok := <-session.sendCh:
				if !ok {
					return
				}
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					log.Printf("[ws] write error for %s: %v", agent.Name, err)
					return
				}
			case <-session.done:
				return
			}
		}
	}()

	// Heartbeat / ping
	heartbeat := time.NewTicker(h.heartbeatInterval)
	defer heartbeat.Stop()

	conn.SetReadDeadline(time.Now().Add(h.heartbeatTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(h.heartbeatTimeout))
		agent.Heartbeat()
		return nil
	})

	// Message reader goroutine
	msgCh := make(chan WSMessage, 10)
	go func() {
		defer close(msgCh)
		for {
			_, msgBytes, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
					log.Printf("[ws] read error from %s: %v", agent.Name, err)
				}
				return
			}
			var msg WSMessage
			if err := json.Unmarshal(msgBytes, &msg); err != nil {
				log.Printf("[ws] invalid message from %s: %v", agent.Name, err)
				continue
			}
			msgCh <- msg
		}
	}()

	// Main event loop
	for {
		select {
		case <-heartbeat.C:
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("[ws] ping error for %s: %v", agent.Name, err)
				h.cleanupSession(agent.ID)
				return
			}

		case msg, ok := <-msgCh:
			if !ok {
				h.cleanupSession(agent.ID)
				return
			}
			h.handleAgentMessage(conn, agent, msg)

		case <-session.done:
			return

		case <-r.Context().Done():
			h.cleanupSession(agent.ID)
			return
		}
	}
}

// cleanupSession removes the agent from the connection registry.
func (h *WSHandler) cleanupSession(agentID string) {
	h.sessionsMu.Lock()
	if s, ok := h.sessions[agentID]; ok {
		close(s.done)
		delete(h.sessions, agentID)
	}
	h.sessionsMu.Unlock()
	h.manager.Unregister(agentID)
}

// dispatchLoop listens for dispatch commands and delivers messages to agents.
func (h *WSHandler) dispatchLoop() {
	for {
		select {
		case cmd := <-h.dispatchCh:
			err := h.SendToAgent(cmd.AgentID, cmd.Message)
			if cmd.Done != nil {
				cmd.Done <- err
			}
		case <-h.stopCh:
			return
		}
	}
}

// Stop shuts down the dispatch loop and cleans up all sessions.
func (h *WSHandler) Stop() {
	close(h.stopCh)
	h.sessionsMu.Lock()
	for id, s := range h.sessions {
		close(s.done)
		delete(h.sessions, id)
	}
	h.sessionsMu.Unlock()
}

func (h *WSHandler) handleAgentMessage(conn *websocket.Conn, agent *Agent, msg WSMessage) {
	switch msg.Type {
	case "heartbeat":
		agent.Heartbeat()
		resp, _ := json.Marshal(WSMessage{Type: "heartbeat_ack"})
		safeWrite(conn, resp)

	case "status_update":
		var status struct {
			Status AgentStatus `json:"status"`
		}
		if err := json.Unmarshal(msg.Payload, &status); err == nil {
			agent.UpdateStatus(status.Status)
		}

	case "task_result":
		// Forwarded to the dispatcher — the server hooks into this
		log.Printf("[ws] task result from %s: %s", agent.Name, string(msg.Payload))
		// The server-level dispatcher receives these via the agentManager events

	case "task_ack":
		log.Printf("[ws] task ack from %s: %s", agent.Name, string(msg.Payload))

	case "log":
		log.Printf("[ws] log from %s: %s", agent.Name, string(msg.Payload))

	default:
		log.Printf("[ws] unknown message type from %s: %s", agent.Name, msg.Type)
	}
}

func safeWrite(conn *websocket.Conn, data []byte) {
	select {
	case <-time.After(5 * time.Second):
		log.Printf("[ws] write timeout — dropping message")
	default:
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		conn.WriteMessage(websocket.TextMessage, data)
		conn.SetWriteDeadline(time.Time{})
	}
}
