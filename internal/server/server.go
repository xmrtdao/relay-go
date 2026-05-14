package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/xmrtdao/relay-go/internal/agent"
	"github.com/xmrtdao/relay-go/internal/config"
	"github.com/xmrtdao/relay-go/internal/dispatcher"
	"github.com/xmrtdao/relay-go/internal/task"
	"github.com/xmrtdao/relay-go/internal/webhook"
)

// Server is the HTTP relay server.
type Server struct {
	config     *config.Config
	agentMgr   *agent.Manager
	taskMgr    *task.Manager
	wsHandler  *agent.WSHandler
	webhookH   *webhook.Handler
	dispatcher *dispatcher.Dispatcher
	syncH      *dispatcher.SyncHandler
	httpServer *http.Server
	startTime  time.Time
}

// New creates a new relay server.
func New(cfg *config.Config) *Server {
	agentMgr := agent.NewManager(cfg.AgentOfflineThreshold)
	taskMgr := task.NewManager(cfg.TaskQueueSize)
	wsHandler := agent.NewWSHandler(agentMgr, cfg.WSHeartbeatInterval, cfg.WSHeartbeatTimeout)
	webhookH := webhook.NewHandler(taskMgr, cfg.SupabaseAPIKey)
	syncH := dispatcher.NewSyncHandler(cfg.SupabaseURL, cfg.SupabaseAPIKey)
	d := dispatcher.New(agentMgr, taskMgr, wsHandler, syncH)

	return &Server{
		config:     cfg,
		agentMgr:   agentMgr,
		taskMgr:    taskMgr,
		wsHandler:  wsHandler,
		webhookH:   webhookH,
		dispatcher: d,
		syncH:      syncH,
		startTime:  time.Now(),
	}
}

// Start begins listening for connections.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// REST API routes
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/api/v1/agents", s.handleAgents)
	mux.HandleFunc("/api/v1/agents/", s.handleAgentByID)
	mux.HandleFunc("/api/v1/tasks", s.handleTasks)
	mux.HandleFunc("/api/v1/tasks/", s.handleTaskByID)

	// WebSocket for agent connections
	mux.Handle("/ws", s.wsHandler)

	// Dispatch — structured JSON task entry point
	mux.HandleFunc("/dispatch", s.handleDispatch)

	// Eliza ping — dedicated endpoint for fleet heartbeats
	mux.HandleFunc("/eliza-ping", s.handleElizaPing)

	// Webhook from Supabase
	mux.Handle("/webhook/task", s.webhookH)

	// Start the dispatcher (task → agent routing + Supabase sync)
	s.dispatcher.Start()

	log.Printf("[server] dispatcher started — %d agents monitored, %d task queue", s.config.TaskQueueSize, s.config.TaskQueueSize)

	addr := formatAddr(s.config.Host, s.config.Port)
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      withCORS(withLogging(mux)),
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
	}

	log.Printf("[server] starting on %s", addr)
	return s.httpServer.ListenAndServe()
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.httpServer.Shutdown(ctx)
	s.dispatcher.Stop()
	s.wsHandler.Stop()
	s.agentMgr.Stop()
	s.taskMgr.Stop()
}

// ---- Route Handlers ----

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"uptime":    time.Since(s.startTime).String(),
		"version":   "0.1.0",
		"agents":    s.agentMgr.Stats()["total"],
		"tasks":     s.taskMgr.Stats()["total"],
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"uptime":   time.Since(s.startTime).String(),
		"version":  "0.1.0",
		"agents":   s.agentMgr.Stats(),
		"tasks":    s.taskMgr.Stats(),
		"config": map[string]any{
			"host": s.config.Host,
			"port": s.config.Port,
		},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleAgents(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		agents := s.agentMgr.List()
		writeJSON(w, http.StatusOK, map[string]any{
			"total":  len(agents),
			"agents": agents,
		})

	case http.MethodPost:
		var req struct {
			ID           string            `json:"id"`
			Name         string            `json:"name"`
			Role         string            `json:"role"`
			Capabilities []string          `json:"capabilities"`
			Endpoint     string            `json:"endpoint"`
			Version      string            `json:"version"`
			PublicTunnel string            `json:"public_tunnel,omitempty"`
			Metadata     map[string]string `json:"metadata,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
			return
		}
		if req.ID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id is required"})
			return
		}

		// Include public tunnel URL if we have one
		if req.Metadata == nil {
			req.Metadata = make(map[string]string)
		}
		req.Metadata["relay_tunnel"] = s.config.TunnelURL
		req.Metadata["ts_relay_tunnel"] = s.config.TunnelURL

		agt := &agent.Agent{
			ID:           req.ID,
			Name:         req.Name,
			Role:         req.Role,
			Capabilities: req.Capabilities,
			Endpoint:     req.Endpoint,
			Version:      req.Version,
			Status:       agent.StatusIdle,
			LastSeen:     time.Now(),
			ConnectedAt:  time.Now(),
			Metadata:     req.Metadata,
		}
		if err := s.agentMgr.Register(agt); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		log.Printf("[server] agent registered via REST: %s (%s)", agt.Name, agt.Role)
		writeJSON(w, http.StatusCreated, map[string]any{
			"status":   "registered",
			"agent_id": req.ID,
			"tunnel":   s.config.TunnelURL,
			"ws_url":   tunnelWebSocketURL(s.config.TunnelURL),
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleAgentByID(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/api/v1/agents/"):]
	if id == "" {
		http.Error(w, "agent ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		a, ok := s.agentMgr.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "agent not found"})
			return
		}
		writeJSON(w, http.StatusOK, a)

	case http.MethodDelete:
		s.agentMgr.Unregister(id)
		writeJSON(w, http.StatusOK, map[string]string{"status": "unregistered"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		status := r.URL.Query().Get("status")
		var tasks []task.Task
		if status != "" {
			tasks = s.taskMgr.List(task.TaskStatus(status))
		} else {
			tasks = s.taskMgr.List("")
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"total": len(tasks),
			"tasks": tasks,
		})

	case http.MethodPost:
		var req struct {
			Title       string         `json:"title"`
			Description string         `json:"description,omitempty"`
			Capability  string         `json:"capability,omitempty"`
			Priority    int            `json:"priority"`
			Payload     map[string]any `json:"payload,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
			return
		}
		if req.Title == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "title is required"})
			return
		}

		id := s.taskMgr.Enqueue(req.Title, req.Description, req.Capability, req.Priority, req.Payload)
		writeJSON(w, http.StatusCreated, map[string]string{"task_id": id, "status": "queued"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/api/v1/tasks/"):]
	if id == "" {
		http.Error(w, "task ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		t, ok := s.taskMgr.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
			return
		}
		writeJSON(w, http.StatusOK, t)

	case http.MethodPatch:
		var req struct {
			Status string `json:"status"`
			Result string `json:"result,omitempty"`
			Error  string `json:"error,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
			return
		}
		if err := s.taskMgr.UpdateStatus(id, task.TaskStatus(req.Status), req.Result, req.Error); err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleDispatch accepts structured JSON for task dispatch.
// Supports: ping, bash, system-monitor, health, chat, and task creation.
func (s *Server) handleDispatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Type    string         `json:"type"`
		Handler string         `json:"handler"`
		Action  string         `json:"action"`
		Message string         `json:"message"`
		Payload map[string]any `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	// Determine dispatch type from any of the fields
	dispatchType := req.Type
	if dispatchType == "" {
		dispatchType = req.Handler
	}
	if dispatchType == "" {
		dispatchType = req.Action
	}

	log.Printf("[dispatch] received: type=%s message=%s", dispatchType, req.Message)

	switch dispatchType {
	case "ping", "pong", "eliza":
		writeJSON(w, http.StatusOK, map[string]any{
			"pong":      true,
			"received":  req.Message,
			"from":      "vex-go-relay",
			"timestamp": time.Now().UnixMilli(),
			"system": map[string]any{
				"uptime":  time.Since(s.startTime).String(),
				"version": "0.1.0",
				"tunnel":  s.config.TunnelURL,
				"agent":   "Vex (Eliza-Dev)",
				"agents":  s.agentMgr.Stats()["total"],
				"tasks":   s.taskMgr.Stats()["total"],
			},
		})

	case "health", "status":
		writeJSON(w, http.StatusOK, map[string]any{
			"status":    "ok",
			"uptime":    time.Since(s.startTime).String(),
			"version":   "0.1.0",
			"agents":    s.agentMgr.Stats(),
			"tasks":     s.taskMgr.Stats(),
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"tunnel":    s.config.TunnelURL,
			"ws_url":    tunnelWebSocketURL(s.config.TunnelURL),
		})

	case "bash", "cmd", "shell":
		cmd := ""
		if req.Payload != nil {
			if c, ok := req.Payload["command"]; ok {
				cmd = fmt.Sprintf("%v", c)
			}
		}
		if cmd == "" {
			cmd = req.Message
		}
		if cmd == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"status":  "error",
				"message": "command is required in payload.command or message",
			})
			return
		}

		taskID := s.taskMgr.Enqueue(cmd, fmt.Sprintf("bash: %s", cmd), "bash", 5, req.Payload)
		log.Printf("Enqueued bash task: %s", taskID)
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "queued",
			"message": "bash task enqueued — results via agent dispatch or check /api/v1/tasks",
		})

	case "monitor", "system-monitor", "system":
		writeJSON(w, http.StatusOK, map[string]any{
			"status":    "ok",
			"uptime":    time.Since(s.startTime).String(),
			"version":   "0.1.0",
			"agents":    s.agentMgr.Stats(),
			"tasks":     s.taskMgr.Stats(),
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"endpoints": []string{"/health", "/status", "/dispatch", "/eliza-ping", "/api/v1/agents", "/api/v1/tasks", "/ws", "/webhook/task"},
		})

	case "chat", "ask", "ollama":
		msg := req.Message
		if msg == "" && req.Payload != nil {
			if m, ok := req.Payload["message"]; ok {
				msg = fmt.Sprintf("%v", m)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":           "acknowledged",
			"message":          "Ollama chat dispatch noted. Go relay handles this via WS agent dispatch or use TS relay /ollama/chat for direct inference.",
			"note":             "Send via POST /webhook/task with structured payload for agent execution",
			"received_message": msg,
		})

	case "task", "webhook":
		// Create a task from the dispatch payload
		title := req.Message
		if title == "" && req.Payload != nil {
			if t, ok := req.Payload["title"]; ok {
				title = fmt.Sprintf("%v", t)
			}
		}
		if title == "" {
			title = "dispatched task"
		}

		capability := ""
		if req.Payload != nil {
			if c, ok := req.Payload["capability"]; ok {
				capability = fmt.Sprintf("%v", c)
			}
		}

		priority := 5
		if req.Payload != nil {
			if p, ok := req.Payload["priority"]; ok {
				if pi, ok := p.(float64); ok {
					priority = int(pi)
				}
			}
		}

		taskID := s.taskMgr.Enqueue(title, req.Message, capability, priority, req.Payload)
		log.Printf("Enqueued task from dispatch: %s", taskID)
		writeJSON(w, http.StatusCreated, map[string]any{
			"status":  "task_created",
			"message": "Task dispatched and queued for agent assignment",
		})

	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status":  "unrecognized",
			"message": fmt.Sprintf("Unknown dispatch type: %s. Supported: ping, health, bash, monitor, chat, task, eliza", dispatchType),
			"help": map[string]any{
				"ping":   `{"type":"ping","message":"hello"}`,
				"bash":   `{"type":"bash","payload":{"command":"echo hi"}}`,
				"health": `{"type":"health"}`,
				"task":   `{"type":"task","payload":{"title":"my task","capability":"bash"}}`,
			},
		})
	}
}

// handleElizaPing responds to fleet heartbeat pings with system state.
// Matches the TS relay's /eliza-ping schema for agent compatibility.
func (s *Server) handleElizaPing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var received string
	if r.Method == http.MethodPost {
		var req struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			received = req.Message
		}
	}

	// Gather agent list
	agentList := s.agentMgr.List()
	agentNames := make([]string, 0, len(agentList))
	for _, a := range agentList {
		agentNames = append(agentNames, a.Name)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"pong":             true,
		"interaction_type": "ping_pong_telemetry",
		"responder":        "vex_go_relay_server (automated)",
		"context": map[string]string{
			"note":             "This is automated system telemetry from the Go relay, not a real-time message from Vex.",
			"how_to_reach_vex": "Post on GitHub issues or use the eliza-relay edge function for cloud-to-cloud messaging.",
		},
		"received":          received,
		"from":              "vex-go-relay",
		"timestamp":         time.Now().UnixMilli(),
		"tools":             []string{"dispatch", "health", "status", "agents", "tasks", "websocket", "webhook"},
		"registered_agents": agentNames,
		"system": map[string]any{
			"uptime":       time.Since(s.startTime).String(),
			"version":      "0.1.0",
			"tunnel":       s.config.TunnelURL,
			"agent":        "Go Relay (Eliza-Dev laptop)",
			"endpoints":    []string{"/health", "/status", "/dispatch", "/eliza-ping", "/api/v1/agents", "/api/v1/tasks", "/ws", "/webhook/task"},
			"agents_total": s.agentMgr.Stats()["total"],
			"tasks_total":  s.taskMgr.Stats()["total"],
		},
	})
}

// ---- Helpers ----

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func formatAddr(host string, port int) string {
	return host + ":" + fmt.Sprintf("%d", port)
}

func tunnelWebSocketURL(tunnel string) string {
	if tunnel == "" {
		return ""
	}
	// Convert https://... to wss://.../ws
	return "wss://" + tunnel[len("https://"):] + "/ws"
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Webhook-Secret")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("[http] %s %s (%s)", r.Method, r.URL.Path, time.Since(start))
	})
}
