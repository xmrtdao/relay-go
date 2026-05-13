package dispatcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/xmrtdao/relay-go/internal/task"
)

// SyncHandler pushes task status updates back to Supabase.
type SyncHandler struct {
	supabaseURL string
	apiKey      string
	client      *http.Client
}

// NewSyncHandler creates a new Supabase sync handler.
func NewSyncHandler(supabaseURL, apiKey string) *SyncHandler {
	return &SyncHandler{
		supabaseURL: supabaseURL,
		apiKey:      apiKey,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// SyncTaskStatus pushes a task update to Supabase.
func (s *SyncHandler) SyncTaskStatus(t task.Task) {
	if s.supabaseURL == "" || s.apiKey == "" {
		return // Supabase not configured
	}

	url := fmt.Sprintf("%s/functions/v1/supabase-integration-v2", s.supabaseURL)
	reqBody := map[string]any{
		"action": "execute_sql",
		"query":  fmt.Sprintf(`UPDATE tasks SET status='%s', updated_at=NOW() WHERE id='%s'`, t.Status, t.ID),
	}
	reqPayload, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", url, bytes.NewReader(reqPayload))
	if err != nil {
		log.Printf("[sync] failed to create request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("[sync] failed to sync task %s: %v", t.ID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("[sync] synced task %s → status: %s", t.ID, t.Status)
	} else {
		log.Printf("[sync] sync returned %d for task %s", resp.StatusCode, t.ID)
	}
}

// SyncAgentHeartbeat pushes an agent heartbeat to Supabase.
func (s *SyncHandler) SyncAgentHeartbeat(agentID, status string) {
	if s.supabaseURL == "" || s.apiKey == "" {
		return
	}

	query := fmt.Sprintf(
		`UPDATE agents SET status='%s', last_seen=NOW() WHERE id='%s'`,
		status, agentID,
	)

	payload, _ := json.Marshal(map[string]any{
		"action": "execute_sql",
		"query":  query,
	})

	url := fmt.Sprintf("%s/functions/v1/supabase-integration-v2", s.supabaseURL)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("[sync] heartbeat sync failed for %s: %v", agentID, err)
		return
	}
	resp.Body.Close()
}
