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
	sendCh  chan []byte
	done    chan struct{}
}

// WSHandler manages WebSocket connections from agents.
type WSHandler struct {
	manager           *Manager
	upgrader          websocket.Upgrader
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration

	sessions   map[string]*connSession
	sessionsMu sync.RWMutex
	dispatchCh chan DispatchCommand
	stopCh     chan struct{}
}

type DispatchCommand struct {
	AgentID string
	Message WSMessage
	Done    chan error
}

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
		sessions:   make(map[string]*connSession),
		dispatchCh: make(chan DispatchCommand, 100),
		stopCh:     make(chan struct{}),
	}
	go h.dispatchLoop()
	return h
}

func (h *WSHandler) SendToAgent(agentID string, msg WSMessage) error {
	h.sessionsMu.RLock()
	session, ok := h.sessions[agentID]
	h.sessionsMu.RUnlock()
	if !ok {
		return nil
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	select {
	case session.sendCh <- data:
	default:
		log.Printf("[ws] dropping message to %s: buffer full", agentID)
	}
	return nil
}

func (h *WSHandler) DispatchChan() chan<- DispatchCommand {
	return h.dispatchCh
}

// readMessage reads one message from the conn with a deadline.
func readMessage(conn *websocket.Conn, timeout time.Duration) ([]byte, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	_, msg, err := conn.ReadMessage()
	return msg, err
}

// ServeHTTP handles WebSocket upgrade and lifecycle — single goroutine, no races.
func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade error: %v", err)
		return
	}

	// Read registration (no concurrent goroutines yet)
	msgData, err := readMessage(conn, h.heartbeatTimeout)
	if err != nil {
		log.Printf("[ws] registration read error: %v", err)
		conn.Close()
		return
	}

	var regMsg WSMessage
	if err := json.Unmarshal(msgData, &regMsg); err != nil || regMsg.Type != "register" {
		log.Printf("[ws] invalid registration: type=%s err=%v", regMsg.Type, err)
		conn.Close()
		return
	}

	var info struct {
		ID           string            `json:"id"`
		Name         string            `json:"name"`
		Role         string            `json:"role"`
		Capabilities []string          `json:"capabilities"`
		Endpoint     string            `json:"endpoint"`
		Version      string            `json:"version"`
		Metadata     map[string]string `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(regMsg.Payload, &info); err != nil {
		log.Printf("[ws] invalid agent info: %v", err)
		conn.Close()
		return
	}

	agt := &Agent{
		ID:           info.ID,
		Name:         info.Name,
		Role:         info.Role,
		Capabilities: info.Capabilities,
		Endpoint:     info.Endpoint,
		Version:      info.Version,
		Status:       StatusIdle,
		LastSeen:     time.Now(),
		ConnectedAt:  time.Now(),
		Metadata:     info.Metadata,
	}
	if err := h.manager.Register(agt); err != nil {
		log.Printf("[ws] register error: %v", err)
		conn.Close()
		return
	}

	// Session
	session := &connSession{
		agentID: agt.ID,
		conn:    conn,
		sendCh:  make(chan []byte, 32),
		done:    make(chan struct{}),
	}
	h.sessionsMu.Lock()
	if old, exists := h.sessions[agt.ID]; exists {
		close(old.done)
	}
	h.sessions[agt.ID] = session
	h.sessionsMu.Unlock()

	// Send ack
	ack, _ := json.Marshal(WSMessage{Type: "registered", Payload: json.RawMessage(`{"status":"ok"}`)})
	conn.WriteMessage(websocket.TextMessage, ack)
	log.Printf("[ws] agent connected: %s (%s) — caps: %v", agt.Name, agt.Role, agt.Capabilities)

	// Writer goroutine — ONLY writes, no reads
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
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				conn.WriteMessage(websocket.TextMessage, data)
				conn.SetWriteDeadline(time.Time{})
			case <-session.done:
				return
			}
		}
	}()

	// Pong handler resets read deadline
	conn.SetPongHandler(func(string) error {
		agt.Heartbeat()
		return nil
	})

	// Main loop — single goroutine doing both reads and timed checks
	readTimeout := h.heartbeatTimeout
	lastRead := time.Now()

	for {
		// How long until we consider the agent dead?
		sinceLastRead := time.Since(lastRead)
		remaining := readTimeout - sinceLastRead
		if remaining <= 0 {
			log.Printf("[ws] heartbeat timeout for %s", agt.Name)
			break
		}

		// Try to read with remaining timeout
		msgData, err := readMessage(conn, remaining)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[ws] read error from %s: %v", agt.Name, err)
			}
			break
		}

		lastRead = time.Now()

		var msg WSMessage
		if err := json.Unmarshal(msgData, &msg); err != nil {
			log.Printf("[ws] invalid message from %s: %v", agt.Name, err)
			continue
		}

		// Handle message in-line, no channel needed
		switch msg.Type {
		case "heartbeat":
			agt.Heartbeat()

		case "status_update":
			var s struct{ Status AgentStatus `json:"status"` }
			if json.Unmarshal(msg.Payload, &s) == nil {
				agt.UpdateStatus(s.Status)
			}

		case "task_result":
			log.Printf("[ws] task result from %s: %s", agt.Name, string(msg.Payload))

		case "task_ack":
			log.Printf("[ws] task ack from %s: %s", agt.Name, string(msg.Payload))

		case "log":
			log.Printf("[ws] log from %s: %s", agt.Name, string(msg.Payload))

		default:
			log.Printf("[ws] unknown msg type from %s: %s", agt.Name, msg.Type)
		}
	}

	// Cleanup
	h.cleanupSession(agt.ID)
	wg.Wait() // wait for writer to finish
	log.Printf("[ws] agent disconnected: %s", agt.Name)
}

func (h *WSHandler) cleanupSession(agentID string) {
	h.sessionsMu.Lock()
	if s, ok := h.sessions[agentID]; ok {
		close(s.done)
		delete(h.sessions, agentID)
	}
	h.sessionsMu.Unlock()
	h.manager.Unregister(agentID)
}

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

func (h *WSHandler) Stop() {
	close(h.stopCh)
	h.sessionsMu.Lock()
	for id, s := range h.sessions {
		close(s.done)
		delete(h.sessions, id)
	}
	h.sessionsMu.Unlock()
}
