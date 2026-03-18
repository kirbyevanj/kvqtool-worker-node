package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// sessionManager creates and tracks Temporal sessions per session group.
type sessionManager struct {
	sessions map[string]workflow.Context
	baseCtx  workflow.Context
}

func newSessionManager(ctx workflow.Context) *sessionManager {
	return &sessionManager{
		sessions: make(map[string]workflow.Context),
		baseCtx:  ctx,
	}
}

// getContext returns the session-pinned context for the given group, creating the session if needed.
// If groupID is empty, returns the base (non-session) context.
func (sm *sessionManager) getContext(groupID string) (workflow.Context, error) {
	if groupID == "" {
		return sm.baseCtx, nil
	}
	if ctx, ok := sm.sessions[groupID]; ok {
		return ctx, nil
	}
	sessCtx, err := workflow.CreateSession(sm.baseCtx, &workflow.SessionOptions{
		CreationTimeout:  5 * time.Minute,
		ExecutionTimeout: 4 * time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("create session for group %s: %w", groupID, err)
	}
	sm.sessions[groupID] = sessCtx
	return sessCtx, nil
}

// completeAll tears down all active sessions.
func (sm *sessionManager) completeAll() {
	for _, ctx := range sm.sessions {
		workflow.CompleteSession(ctx)
	}
}
