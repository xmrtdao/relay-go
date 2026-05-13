package dispatcher

import (
	"encoding/json"
	"log"
	"time"

	"github.com/xmrtdao/relay-go/internal/agent"
	"github.com/xmrtdao/relay-go/internal/task"
)

// Dispatcher bridges task events to agent dispatch and Supabase sync.
type Dispatcher struct {
	agentMgr *agent.Manager
	taskMgr  *task.Manager
	wsHandler interface {
		SendToAgent(agentID string, msg agent.WSMessage) error
	}
	syncHandler *SyncHandler
	stopCh      chan struct{}
}

// New creates a new dispatcher.
func New(
	agentMgr *agent.Manager,
	taskMgr *task.Manager,
	ws interface {
		SendToAgent(agentID string, msg agent.WSMessage) error
	},
	syncHandler *SyncHandler,
) *Dispatcher {
	return &Dispatcher{
		agentMgr:   agentMgr,
		taskMgr:    taskMgr,
		wsHandler:  ws,
		syncHandler: syncHandler,
		stopCh:     make(chan struct{}),
	}
}

// Start begins listening for task events and dispatching them.
func (d *Dispatcher) Start() {
	go d.dispatchLoop()
	log.Printf("[dispatch] started — listening for task events")
}

// Stop shuts down the dispatcher.
func (d *Dispatcher) Stop() {
	close(d.stopCh)
}

func (d *Dispatcher) dispatchLoop() {
	eventCh := d.taskMgr.Events()

	for {
		select {
		case evt, ok := <-eventCh:
			if !ok {
				return
			}
			d.handleEvent(evt)

		case <-d.stopCh:
			return
		}
	}
}

func (d *Dispatcher) handleEvent(evt task.TaskEvent) {
	switch evt.Type {
	case "created":
		// New task — try to find an agent and dispatch immediately
		d.dispatchTask(evt.Task)

	case "completed", "failed":
		// Sync status back to Supabase
		if d.syncHandler != nil {
			d.syncHandler.SyncTaskStatus(evt.Task)
		}
	}
}

func (d *Dispatcher) dispatchTask(t task.Task) {
	// Find best idle agent for this task's capability
	best := d.agentMgr.FindBestAgent(t.Capability, "")
	if best == nil {
		log.Printf("[dispatch] no available agent for task %s [%s]", t.ID, t.Title)
		return
	}

	// Claim the task for this agent
	claimed, err := d.taskMgr.Claim(best.ID, t.Capability)
	if err != nil {
		log.Printf("[dispatch] failed to claim task %s for %s: %v", t.ID, best.ID, err)
		return
	}

	// Push to the agent via WebSocket
	d.pushTaskToAgent(*claimed, best.ID)
}

func (d *Dispatcher) pushTaskToAgent(t task.Task, agentID string) {
	payload, _ := json.Marshal(t)

	msg := agent.WSMessage{
		Type:    "task_dispatch",
		AgentID: agentID,
		Payload: payload,
	}

	if err := d.wsHandler.SendToAgent(agentID, msg); err != nil {
		log.Printf("[dispatch] failed to send task %s to %s: %v", t.ID, agentID, err)
		// Release the task back to pending
		d.taskMgr.UpdateStatus(t.ID, task.StatusPending, "", "dispatch failed")
		return
	}

	log.Printf("[dispatch] dispatched task %s [%s] → %s", t.ID, t.Title, agentID)

	// Mark as in-progress (agent should have received it)
	time.AfterFunc(2*time.Second, func() {
		d.taskMgr.UpdateStatus(t.ID, task.StatusInProgress, "", "")
	})
}
