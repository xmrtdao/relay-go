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

// ---- Helpers ----

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func formatAddr(host string, port int) string {
	return host + ":" + fmt.Sprintf("%d", port)
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
