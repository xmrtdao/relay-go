package task

import "time"

// TaskStatus represents the lifecycle state of a task.
type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusClaimed    TaskStatus = "claimed"
	StatusInProgress TaskStatus = "in_progress"
	StatusCompleted  TaskStatus = "completed"
	StatusFailed     TaskStatus = "failed"
	StatusBlocked    TaskStatus = "blocked"
)

// Task represents a unit of work to be routed to an agent.
type Task struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description,omitempty"`
	Status      TaskStatus        `json:"status"`
	Priority    int               `json:"priority"`
	Capability  string            `json:"capability,omitempty"`
	AssignTo    string            `json:"assign_to,omitempty"` // specific agent ID
	Payload     map[string]any    `json:"payload,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	ClaimedBy   string            `json:"claimed_by,omitempty"`
	ClaimedAt   *time.Time        `json:"claimed_at,omitempty"`
	Result      string            `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
}

// TaskEvent is emitted when a task changes state.
type TaskEvent struct {
	Type string `json:"type"` // "created", "claimed", "completed", "failed"
	Task Task   `json:"task"`
}
