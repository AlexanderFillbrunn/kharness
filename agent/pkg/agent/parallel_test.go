package agent

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/agent/pkg/history"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// parallelRegistry registers each name with the given read-only declaration
// and handler.
func parallelRegistry(t *testing.T, readOnly bool, tools map[string]mcp.ToolHandler) *ToolRegistry {
	t.Helper()
	registry := NewToolRegistry()
	for name, handler := range tools {
		tool := &mcp.Tool{Name: name}
		if readOnly {
			tool.Annotations = &mcp.ToolAnnotations{ReadOnlyHint: true}
		}
		if err := registry.RegisterTool(tool, handler); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}
	return registry
}

// textResult answers a call with the given text.
func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

// concurrency tracks how many handlers are in flight at once, how many have
// ever entered, and the highest in-flight count that was reached.
type concurrency struct {
	inFlight int32
	started  int32
	maxSeen  int32
}

func (c *concurrency) enter() {
	atomic.AddInt32(&c.started, 1)
	cur := atomic.AddInt32(&c.inFlight, 1)
	for {
		m := atomic.LoadInt32(&c.maxSeen)
		if cur <= m || atomic.CompareAndSwapInt32(&c.maxSeen, m, cur) {
			return
		}
	}
}

func (c *concurrency) leave() { atomic.AddInt32(&c.inFlight, -1) }

func (c *concurrency) max() int32 { return atomic.LoadInt32(&c.maxSeen) }

// waitFor blocks until n handlers have entered, or fails the call after a
// timeout — which is what a sequential execution would produce. It waits on
// started rather than inFlight because started never goes down: a sibling
// returning early must not make the barrier unreachable for the rest.
func (c *concurrency) waitFor(n int32) error {
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&c.started) < n {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %d handlers to start, saw %d", n, atomic.LoadInt32(&c.started))
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// scriptedCalls returns an LLM that answers one reply carrying the calls and
// then a plain text reply.
func scriptedCalls(calls []LLMToolCall) *scriptedLLM {
	return &scriptedLLM{replies: []*LLMResponse{
		{ModelID: "gpt-5", Content: "checking", ToolCalls: calls},
		{ModelID: "gpt-5", Content: "done"},
	}}
}

func TestParallelCapResolution(t *testing.T) {
	for _, tc := range []struct {
		configured int
		want       int
	}{
		{0, DefaultMaxParallelToolCalls},
		{-1, -1},
		{1, 1},
		{7, 7},
	} {
		if got := (&AgentConfig{MaxParallelToolCalls: tc.configured}).parallelCap(); got != tc.want {
			t.Errorf("MaxParallelToolCalls=%d resolves to %d, want %d", tc.configured, got, tc.want)
		}
	}
	if DefaultMaxParallelToolCalls != 4 {
		t.Errorf("DefaultMaxParallelToolCalls = %d, want 4", DefaultMaxParallelToolCalls)
	}
}

// Read-only calls of one response run in one wave: all of them in flight at
// the same time, dispatched in one batch in emission order, and settled with
// their own results.
func TestReadOnlyCallsRunInOneParallelWave(t *testing.T) {
	names := []string{"read_a", "read_b", "read_c"}
	tools := map[string]mcp.ToolHandler{}
	var c concurrency
	for _, name := range names {
		tools[name] = func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			c.enter()
			defer c.leave()
			if err := c.waitFor(int32(len(names))); err != nil {
				return nil, err
			}
			return textResult(name), nil
		}
	}

	calls := []LLMToolCall{
		{VendorID: "call_1", ToolName: "read_a"},
		{VendorID: "call_2", ToolName: "read_b"},
		{VendorID: "call_3", ToolName: "read_c"},
	}
	persistor := &fakePersistor{next: 1}
	agent, err := NewAgent(&AgentConfig{}, parallelRegistry(t, true, tools), scriptedCalls(calls))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := c.max(); got != int32(len(names)) {
		t.Errorf("max concurrency = %d, want all %d read-only calls in flight at once", got, len(names))
	}

	folded := h.Arcs[0].Responses[0].ToolCalls
	if len(folded) != len(names) {
		t.Fatalf("arc holds %d calls, want %d", len(folded), len(names))
	}
	for i, call := range folded {
		if call.Status != history.ToolCallStatusSuccess {
			t.Errorf("call %s is %s, want SUCCESS (result %#v)", call.ToolName, call.Status, call.Result)
		}
		if call.Result != names[i] {
			t.Errorf("call %s result = %#v, want %q", call.ToolName, call.Result, names[i])
		}
	}

	// The wave's dispatches are one consecutive batch, in emission order.
	var dispatches []*events.Event
	for _, ev := range persistor.persisted {
		if ev.Typ == events.ToolDispatchedEventType {
			dispatches = append(dispatches, ev)
		}
	}
	if len(dispatches) != len(names) {
		t.Fatalf("persisted %d dispatches, want %d", len(dispatches), len(names))
	}
	for i, ev := range dispatches {
		if got := ev.Payload.(events.ToolDispatchedPayload).CausationID; got != folded[i].ID {
			t.Errorf("dispatch %d names call %q, want emission-order call %q", i, got, folded[i].ID)
		}
	}
	for i := 1; i < len(dispatches); i++ {
		if dispatches[i].Seq != dispatches[i-1].Seq+1 {
			t.Errorf("dispatches %d and %d are not one batch (seqs %d, %d)", i-1, i, dispatches[i-1].Seq, dispatches[i].Seq)
		}
	}

	// The model sees the results in emission order, whatever order they settled in.
	var results []history.ModelToolMessage
	for _, msg := range projectLast(t, h).Messages {
		if tool, ok := msg.(history.ModelToolMessage); ok {
			results = append(results, tool)
		}
	}
	if len(results) != len(names) {
		t.Fatalf("transcript carries %d tool results, want %d", len(results), len(names))
	}
	for i, msg := range results {
		if msg.ToolCallID != folded[i].ID {
			t.Errorf("result %d answers call %q, want emission-order call %q", i, msg.ToolCallID, folded[i].ID)
		}
		if msg.Content != names[i] {
			t.Errorf("result %d content = %q, want %q", i, msg.Content, names[i])
		}
	}
}

// r r w w r r runs as [2r] w w [2r]: the reads overlap, the writes never
// overlap anything, and every call still settles with its own result.
func TestWritesStaySequentialAroundReadWaves(t *testing.T) {
	tools := map[string]mcp.ToolHandler{}
	var c concurrency
	for _, name := range []string{"read_a", "read_b", "write_a", "write_b", "read_c", "read_d"} {
		name := name
		tools[name] = func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			c.enter()
			defer c.leave()
			if name[0] == 'w' {
				if atomic.LoadInt32(&c.inFlight) != 1 {
					return nil, fmt.Errorf("%s ran beside another call", name)
				}
			} else if err := c.waitFor(2); err != nil {
				return nil, err
			}
			return textResult(name), nil
		}
	}
	registry := NewToolRegistry()
	for name, handler := range tools {
		tool := &mcp.Tool{Name: name}
		if name[0] == 'r' {
			tool.Annotations = &mcp.ToolAnnotations{ReadOnlyHint: true}
		}
		if err := registry.RegisterTool(tool, handler); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}

	names := []string{"read_a", "read_b", "write_a", "write_b", "read_c", "read_d"}
	var calls []LLMToolCall
	for i, name := range names {
		calls = append(calls, LLMToolCall{VendorID: fmt.Sprintf("call_%d", i), ToolName: name})
	}
	persistor := &fakePersistor{next: 1}
	agent, err := NewAgent(&AgentConfig{}, registry, scriptedCalls(calls))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := c.max(); got != 2 {
		t.Errorf("max concurrency = %d, want 2 (the read waves only)", got)
	}
	folded := h.Arcs[0].Responses[0].ToolCalls
	if len(folded) != len(names) {
		t.Fatalf("arc holds %d calls, want %d", len(folded), len(names))
	}
	for i, call := range folded {
		if call.Status != history.ToolCallStatusSuccess || call.Result != names[i] {
			t.Errorf("call %d (%s) = %s %#v, want SUCCESS %q", i, call.ToolName, call.Status, call.Result, names[i])
		}
	}
}

// A read run longer than the cap chunks into waves of at most the cap.
func TestWaveCapChunksAReadRun(t *testing.T) {
	const total = 5
	tools := map[string]mcp.ToolHandler{}
	var c concurrency
	for i := 0; i < total; i++ {
		name := fmt.Sprintf("read_%d", i)
		tools[name] = func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			c.enter()
			defer c.leave()
			// Hold the wave open so an overlapping sibling is observable.
			time.Sleep(50 * time.Millisecond)
			return textResult(name), nil
		}
	}

	var calls []LLMToolCall
	for i := 0; i < total; i++ {
		calls = append(calls, LLMToolCall{VendorID: fmt.Sprintf("call_%d", i), ToolName: fmt.Sprintf("read_%d", i)})
	}
	agent, err := NewAgent(&AgentConfig{MaxParallelToolCalls: 2}, parallelRegistry(t, true, tools), scriptedCalls(calls))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: &fakePersistor{next: 1}, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := c.max(); got != 2 {
		t.Errorf("max concurrency = %d, want the cap of 2", got)
	}
	for i, call := range h.Arcs[0].Responses[0].ToolCalls {
		if call.Status != history.ToolCallStatusSuccess {
			t.Errorf("call %d is %s, want SUCCESS", i, call.Status)
		}
	}
}

// A failing call in a wave settles as failed and does not stop its siblings.
func TestAParallelWaveSurvivesOneCallFailing(t *testing.T) {
	tools := map[string]mcp.ToolHandler{
		"read_ok": func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return textResult("fine"), nil
		},
		"read_bad": func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return nil, errors.New("disk on fire")
		},
	}
	calls := []LLMToolCall{
		{VendorID: "call_1", ToolName: "read_ok"},
		{VendorID: "call_2", ToolName: "read_bad"},
	}
	agent, err := NewAgent(&AgentConfig{}, parallelRegistry(t, true, tools), scriptedCalls(calls))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: &fakePersistor{next: 1}, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	folded := h.Arcs[0].Responses[0].ToolCalls
	if folded[0].Status != history.ToolCallStatusSuccess || folded[0].Result != "fine" {
		t.Errorf("sibling call = %s %#v, want SUCCESS \"fine\"", folded[0].Status, folded[0].Result)
	}
	if folded[1].Status != history.ToolCallStatusFailed {
		t.Errorf("failing call = %s, want FAILED", folded[1].Status)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc is %s, want it closed", h.Arcs[0].Status())
	}
}

// Task operations are never in a parallel wave, whatever their name resolves
// to, and a run of reads chunks around them in emission order.
func TestTaskOperationsAreNeverParallel(t *testing.T) {
	tasks := taskSourceWith(t, &fakeController{}, "auditor")
	registry := parallelRegistry(t, true, map[string]mcp.ToolHandler{
		"read_a": func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) { return textResult("a"), nil },
	})
	agent := Agent{config: &AgentConfig{}, tools: registry, tasks: tasks}

	calls := []*history.ToolCall{
		{ToolName: "read_a"},
		{ToolName: "auditor"},
		{ToolName: "read_a"},
	}
	waves := agent.waves(calls)
	if len(waves) != 3 {
		t.Fatalf("waves = %d, want 3 (read, task, read)", len(waves))
	}
	for i, wave := range waves {
		if len(wave) != 1 {
			t.Errorf("wave %d holds %d calls, want 1: %v", i, len(wave), wave)
		}
	}
}

// A wave is chunked to the cap but never reordered: chunk boundaries keep
// emission order.
func TestWavesChunkReadRunsInOrder(t *testing.T) {
	registry := parallelRegistry(t, true, map[string]mcp.ToolHandler{
		"read": func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) { return textResult("x"), nil },
	})
	registry2 := NewToolRegistry()
	if err := registry2.RegisterTool(&mcp.Tool{Name: "write"}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return textResult("w"), nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	for _, tool := range registry.All() {
		if err := registry2.RegisterTool(tool, registry.tools[tool.Name].handler); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	agent := Agent{config: &AgentConfig{MaxParallelToolCalls: 2}, tools: registry2}

	var calls []*history.ToolCall
	for _, name := range []string{"read", "read", "read", "write", "read", "read", "read"} {
		calls = append(calls, &history.ToolCall{ToolName: name})
	}
	waves := agent.waves(calls)
	want := [][]string{{"read", "read"}, {"read"}, {"write"}, {"read", "read"}, {"read"}}
	if len(waves) != len(want) {
		t.Fatalf("waves = %d, want %d", len(waves), len(want))
	}
	for i, wave := range waves {
		var got []string
		for _, call := range wave {
			got = append(got, call.ToolName)
		}
		if fmt.Sprint(got) != fmt.Sprint(want[i]) {
			t.Errorf("wave %d = %v, want %v", i, got, want[i])
		}
	}
}

// The writeMu keeps concurrent settlements folding in seq order: a wave of
// many calls leaves no hole the fold would reject.
func TestConcurrentSettlementsFoldInSeqOrder(t *testing.T) {
	const total = 8
	tools := map[string]mcp.ToolHandler{}
	for i := 0; i < total; i++ {
		name := fmt.Sprintf("read_%d", i)
		tools[name] = func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			time.Sleep(20 * time.Millisecond)
			return textResult(name), nil
		}
	}
	var calls []LLMToolCall
	for i := 0; i < total; i++ {
		calls = append(calls, LLMToolCall{VendorID: fmt.Sprintf("call_%d", i), ToolName: fmt.Sprintf("read_%d", i)})
	}
	agent, err := NewAgent(&AgentConfig{MaxParallelToolCalls: -1}, parallelRegistry(t, true, tools), scriptedCalls(calls))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	h := userArcHistory(t)
	persistor := &fakePersistor{next: 1}
	// The run races the wave against itself here; a fold out of seq order
	// would fail the run.
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, call := range h.Arcs[0].Responses[0].ToolCalls {
		if call.Status != history.ToolCallStatusSuccess {
			t.Errorf("call %d is %s, want SUCCESS", i, call.Status)
		}
	}
}
