package task

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
	"time"
)

// Manager handles task queuing, dispatching, and lifecycle.
type Manager struct {
	tasks    map[string]*Task
	queue    priorityQueue
	mu       sync.RWMutex
	nextID   int
	eventCh  chan TaskEvent
	stopCh   chan struct{}
}

// NewManager creates a new task manager.
func NewManager(queueSize int) *Manager {
	m := &Manager{
		tasks:   make(map[string]*Task),
		queue:   make(priorityQueue, 0),
		eventCh: make(chan TaskEvent, queueSize),
		stopCh:  make(chan struct{}),
	}
	heap.Init(&m.queue)
	return m
}

// Enqueue adds a task to the queue and returns its ID.
func (m *Manager) Enqueue(title, description, capability string, priority int, payload map[string]any) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := fmt.Sprintf("task-%d", m.nextID)
	m.nextID++

	task := &Task{
		ID:          id,
		Title:       title,
		Description: description,
		Status:      StatusPending,
		Priority:    priority,
		Capability:  capability,
		Payload:     payload,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	m.tasks[id] = task
	heap.Push(&m.queue, &priorityItem{task: task, priority: priority})

	m.emit(TaskEvent{Type: "created", Task: *task})
	log.Printf("[task] enqueued: %s [%s] (priority: %d)", id, title, priority)
	return id
}

// Claim reserves a task for an agent.
func (m *Manager) Claim(agentID string, capability string) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for m.queue.Len() > 0 {
		item := heap.Pop(&m.queue).(*priorityItem)
		task := item.task

		if task.Status != StatusPending {
			continue
		}
		if capability != "" && task.Capability != "" && task.Capability != capability {
			continue
		}

		now := time.Now()
		task.Status = StatusClaimed
		task.ClaimedBy = agentID
		task.ClaimedAt = &now
		task.UpdatedAt = now

		m.tasks[task.ID] = task
		m.emit(TaskEvent{Type: "claimed", Task: *task})
		log.Printf("[task] claimed: %s by %s", task.ID, agentID)
		return task, nil
	}

	return nil, fmt.Errorf("no available tasks")
}

// UpdateStatus updates a task's status.
func (m *Manager) UpdateStatus(taskID string, status TaskStatus, result, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, exists := m.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	task.Status = status
	task.UpdatedAt = time.Now()
	if result != "" {
		task.Result = result
	}
	if errMsg != "" {
		task.Error = errMsg
	}

	m.tasks[taskID] = task
	m.emit(TaskEvent{Type: string(status), Task: *task})
	log.Printf("[task] %s: %s (status: %s)", taskID, task.Title, status)
	return nil
}

// Get retrieves a task by ID.
func (m *Manager) Get(taskID string) (*Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return nil, false
	}
	copy := *t
	return &copy, true
}

// List returns all tasks, optionally filtered by status.
func (m *Manager) List(status TaskStatus) []Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []Task
	for _, t := range m.tasks {
		if status != "" && t.Status != status {
			continue
		}
		result = append(result, *t)
	}
	return result
}

// Stats returns task queue statistics.
func (m *Manager) Stats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]int{
		"total":      len(m.tasks),
		"pending":    0,
		"claimed":    0,
		"in_progress": 0,
		"completed":  0,
		"failed":     0,
		"blocked":    0,
	}
	for _, t := range m.tasks {
		switch t.Status {
		case StatusPending:
			stats["pending"]++
		case StatusClaimed:
			stats["claimed"]++
		case StatusInProgress:
			stats["in_progress"]++
		case StatusCompleted:
			stats["completed"]++
		case StatusFailed:
			stats["failed"]++
		case StatusBlocked:
			stats["blocked"]++
		}
	}
	return stats
}

// Events returns the event channel for subscribers.
func (m *Manager) Events() <-chan TaskEvent {
	return m.eventCh
}

// Stop shuts down the task manager.
func (m *Manager) Stop() {
	close(m.stopCh)
}

func (m *Manager) emit(e TaskEvent) {
	select {
	case m.eventCh <- e:
	default:
		// Drop event if channel full
	}
}

// priorityQueue implements heap.Interface for priority-based task dispatch.
type priorityItem struct {
	task     *Task
	priority int
	index    int
}

type priorityQueue []*priorityItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority // higher priority first
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*priorityItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}
