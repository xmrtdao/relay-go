package webhook

import (
	"encoding/json"
	"log"
	"net/http"
)

// Handler receives task webhooks from Supabase.
type Handler struct {
	taskManager TaskManager
	secretKey   string
}

// TaskManager is the interface the webhook needs.
type TaskManager interface {
	Enqueue(title, description, capability string, priority int, payload map[string]any) string
}

// NewHandler creates a new webhook handler.
func NewHandler(tm TaskManager, secretKey string) *Handler {
	return &Handler{
		taskManager: tm,
		secretKey:   secretKey,
	}
}

// WebhookPayload is the expected payload from Supabase.
type WebhookPayload struct {
	Type string `json:"type"`
	Task struct {
		ID          string         `json:"id"`
		Title       string         `json:"title"`
		Description string         `json:"description,omitempty"`
		Capability  string         `json:"capability,omitempty"`
		Priority    int            `json:"priority"`
		Payload     map[string]any `json:"payload,omitempty"`
		Source      string         `json:"source,omitempty"`
	} `json:"task"`
}

// ServeHTTP handles incoming webhook requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Verify secret if configured
	if h.secretKey != "" {
		if r.Header.Get("X-Webhook-Secret") != h.secretKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	var payload WebhookPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Printf("[webhook] invalid payload: %v", err)
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	taskID := h.taskManager.Enqueue(
		payload.Task.Title,
		payload.Task.Description,
		payload.Task.Capability,
		payload.Task.Priority,
		payload.Task.Payload,
	)

	log.Printf("[webhook] received task: %s → queued as %s", payload.Task.ID, taskID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"task_id": taskID,
	})
}
