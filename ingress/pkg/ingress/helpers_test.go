package ingress_test

import (
	"context"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fmt"
	"strings"

	"fillbrunn.ai/kube-agent/agent/pkg/agent"
	"fillbrunn.ai/kube-agent/agent/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

const (
	testSessionID = "session"
	testArcID     = "arc-1"
	testAgentID   = "test-agent"
)

// fakeLLM replies with a canned response per turn, the way the agent's own
// scripted model does.
type fakeLLM struct {
	replies []*agent.LLMResponse
	calls   int
}

func (l *fakeLLM) StreamResponse(_ context.Context, _ *history.ModelTranscript, _ *agent.LLMOptions, _ common.Sink[agent.RawToken]) (*agent.LLMResponse, error) {
	if l.calls >= len(l.replies) {
		return nil, fmt.Errorf("the model was asked %d times, with %d replies scripted", l.calls+1, len(l.replies))
	}
	reply := l.replies[l.calls]
	l.calls++
	return reply, nil
}

func (l *fakeLLM) Response(ctx context.Context, t *history.ModelTranscript, opts *agent.LLMOptions) (*agent.LLMResponse, error) {
	return l.StreamResponse(ctx, t, opts, nil)
}

func typesOf(stored []*events.Event) string {
	names := make([]string, 0, len(stored))
	for _, ev := range stored {
		names = append(names, string(ev.Typ))
	}
	return strings.Join(names, ", ")
}
