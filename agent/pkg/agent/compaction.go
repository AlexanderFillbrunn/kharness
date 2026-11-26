package agent

import (
	"context"
	"errors"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/common"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// This file holds compaction in the agent: the loop's budget checks and the
// admission check, overflow recovery, the bound rule that draws the cut, the
// Compactor and its three implementations, the session-scoped lease, and the
// compaction worker that computes a COMPACTED off the queue and runs the
// completion scan. The log is never rewritten: a COMPACTED records that a
// prefix was summarised, and a reader that ignores it replays the session
// whole.

// Compaction methods, named in the settings file and recorded in the
// COMPACTED payload.
const (
	// CompactionMethodLLM summarises the region below the watermark with a
	// model call.
	CompactionMethodLLM = "llm"
	// CompactionMethodPrune drops the region below the watermark and replaces
	// it with a note naming what was dropped. No model call.
	CompactionMethodPrune = "prune"
	// CompactionMethodProvider asks the provider to compact the transcript
	// itself.
	CompactionMethodProvider = "provider"
)

// Defaults for the compaction numbers the settings file can override. A
// negative value disables the check it belongs to, and -1 is the explicit
// marker for that: the arc threshold, the tool-result cap and the
// user-message cap are all off unless the settings ask for them.
const (
	DefaultKeepRecentTokens   = 4000
	DefaultMaxToolResultChars = -1
	DefaultArcThresholdTokens = -1
)

// ContextWindowReserve is what the higher budget leaves below the context
// window, so one more tool result still fits when the estimate says the
// transcript is at the budget.
const ContextWindowReserve = 2000

// errSuspended ends a run whose arc was suspended for compaction. It is not a
// failure: the records that say what happened are already in the log, so
// recordFailure writes nothing for it and the run ends its in-flight period.
var errSuspended = errors.New("arc suspended for compaction")

// compactionEnabled reports whether the agent runs the compaction checks at
// all: compaction on and a lower budget to check against.
func (c *AgentConfig) compactionEnabled() bool {
	return c.Compaction && c.LowerBudget > 0
}

// CompactionLimits are the worker-side compaction knobs: what the cut and the
// summarised region's projection are drawn under. They are the compaction
// worker's settings, held in its CompactionPlan and resolved from the agent
// definition — not the agent's, whose config carries only the run-side knobs
// (the budgets and the projection limits the run's own transcript is built
// with). The budgets are not here either: they are the run's to check.
type CompactionLimits struct {
	// KeepRecentTokens is the tail of the log the cut must leave above it.
	KeepRecentTokens int

	// MaxToolResultChars caps what one tool result may contribute to a
	// transcript, in characters. The log keeps the whole result.
	MaxToolResultChars int

	// ArcThresholdTokens is the size an arc's log may reach before the bound
	// rule draws the line at its live boundary rather than its opening.
	ArcThresholdTokens int
}

// projectionOptions carries the limits to the worker's projection of the
// region it summarises.
func (l CompactionLimits) projectionOptions() history.ProjectionOptions {
	return history.ProjectionOptions{
		MaxToolResultChars: l.MaxToolResultChars,
		ArcThresholdTokens: l.ArcThresholdTokens,
	}
}

// projectionOptions carries the config's projection limits to the run's
// projection of the arc it is working.
func (c *AgentConfig) projectionOptions() history.ProjectionOptions {
	return history.ProjectionOptions{
		MaxToolResultChars: c.MaxToolResultChars,
		ArcThresholdTokens: c.ArcThresholdTokens,
	}
}

// ContextWindow is the size of the context a model accepts, looked up by the
// model ref the definition names. The catalog holds per-model overrides; a
// model not in it gets DefaultContextWindow, and the settings file can
// override both.
const DefaultContextWindow = 32000

var contextWindowCatalog = map[string]int{
	"gpt-5":           400000,
	"gpt-5-mini":      400000,
	"gpt-4o":          128000,
	"gpt-4o-mini":     128000,
	"claude-sonnet-4": 200000,
	"claude-opus-4":   200000,
}

// ContextWindow returns the context window of the model the ref names.
func ContextWindow(ref ModelRef) int {
	if ref.Source != "" {
		if v, ok := contextWindowCatalog[ref.Source+"/"+ref.Name]; ok {
			return v
		}
	}
	if v, ok := contextWindowCatalog[ref.Name]; ok {
		return v
	}
	return DefaultContextWindow
}

// checkCompaction runs the two budget checks after every round. The lower
// budget writes a COMPACTION_REQUESTED and notifies the run's listener, which
// starts the compaction outside the run, and the run continues — mid-run
// async. The higher budget suspends the run onto the request its own earlier
// rounds already wrote. The check after the final round subsumes a check at
// arc close, so it runs even when the arc is done; a closed arc is never
// suspended.
func (a Agent) checkCompaction(ctx context.Context, state *agentState, done bool) error {
	cfg := a.config
	if !cfg.compactionEnabled() {
		return nil
	}
	if state.history.CompactionDue(cfg.LowerBudget) {
		if _, err := a.ensureCompactionRequest(ctx, state); err != nil {
			return err
		}
	}
	if done || cfg.HigherBudget <= 0 {
		return nil
	}
	if state.history.EstimateAbove(state.history.Watermark()) > cfg.HigherBudget {
		// The lower budget is the smaller number and was checked first on
		// this same round, so the request stands; one compaction per episode.
		requestID, err := a.ensureCompactionRequest(ctx, state)
		if err != nil {
			return err
		}
		if err := a.suspendArc(ctx, state, requestID); err != nil {
			return err
		}
		return errSuspended
	}
	return nil
}

// ensureCompactionRequest returns the ID of the unsatisfied request where one
// stands, and otherwise writes one naming the current watermark, notifies the
// run's listener, and returns its ID. The request in the log is what makes the
// decision durable: a notification that fails or is never made leaves the
// request standing for anyone to take.
func (a Agent) ensureCompactionRequest(ctx context.Context, state *agentState) (string, error) {
	if req := state.history.UnsatisfiedRequest(); req != nil {
		return req.ID, nil
	}
	stored, err := a.persistAndFold(ctx, state, &events.Event{
		Typ: events.CompactionRequestedEventType,
		// No correlation ID: the request is a fact about the session.
		Payload: events.CompactionRequestedPayload{Watermark: state.history.Watermark()},
	})
	if err != nil {
		return "", err
	}
	requestID := stored[0].ID
	if state.onCompactionDue != nil {
		state.onCompactionDue(ctx, requestID)
	}
	return requestID, nil
}

// suspendArc writes the ARC_SUSPENDED that pauses the arc, causally naming the
// request it waits on.
func (a Agent) suspendArc(ctx context.Context, state *agentState, requestID string) error {
	_, err := a.persistAndFold(ctx, state, &events.Event{
		Typ:           events.ArcSuspendedEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.ArcSuspendedPayload{CausationID: requestID},
	})
	return err
}

// recoverOverflow handles a classified provider rejection mid-run: a
// non-fatal Error correlated to the arc carrying the provider's reason, then a
// COMPACTION_REQUESTED where none stands, then the ARC_SUSPENDED, and the end
// of the in-flight period. Bounded to one recovery per arc, read off the
// derived suspension states: a rejection on an arc whose suspension is
// already resolved returns a plain error, which recordFailure records as
// fatal.
func (a Agent) recoverOverflow(ctx context.Context, state *agentState, reason string) error {
	if hasResolvedSuspension(state.history, state.arc) {
		return fmt.Errorf("the provider rejected the transcript as over budget again after this arc recovered once: %s", reason)
	}
	if _, err := a.persistAndFold(ctx, state, &events.Event{
		Typ:           events.ErrorEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.ErrorPayload{Message: reason, IsFatal: false},
	}); err != nil {
		return err
	}
	requestID, err := a.ensureCompactionRequest(ctx, state)
	if err != nil {
		return err
	}
	if err := a.suspendArc(ctx, state, requestID); err != nil {
		return err
	}
	return errSuspended
}

// hasResolvedSuspension reports whether the arc has a suspension a response has
// already followed.
func hasResolvedSuspension(h *history.History, arc *history.Arc) bool {
	for _, s := range arc.Suspensions {
		if h.SuspensionStateOf(arc, s) == history.SuspensionResolved {
			return true
		}
	}
	return false
}

// computeWatermark draws the cut for a compaction over the session as the fold
// holds it:
//
//	ThroughSeq = min( min(bound(arc) for open arcs) - 1, h.TailCap(keep) )
//
// where bound(arc) is the arc's opening when its own log fits the arc
// threshold, and its live boundary otherwise, and the tail cap is the last
// turn boundary at or below the backward accumulation of per-record estimates
// that reaches KeepRecentTokens. The cut never separates a tool call from its
// result, because a turn boundary is where the projection keeps them
// contiguous. The fold carries every record with its seq, so the history
// alone is the source — the raw log is not needed.
func computeWatermark(h *history.History, limits CompactionLimits) int {
	through := h.TailCap(limits.KeepRecentTokens)
	minBound := -1
	for _, arc := range h.Arcs {
		switch arc.Status() {
		case history.ArcStatusDone, history.ArcStatusCancelled, history.ArcStatusError:
			continue
		}
		bound := arcBound(arc, h, limits)
		if minBound < 0 || bound < minBound {
			minBound = bound
		}
	}
	if minBound >= 0 && minBound-1 < through {
		through = minBound - 1
	}
	if through < -1 {
		through = -1
	}
	return through
}

// arcBound is where the line is drawn for one open arc: its opening where the
// arc's own log fits the arc threshold, its live boundary where it does not.
func arcBound(arc *history.Arc, h *history.History, limits CompactionLimits) int {
	if limits.ArcThresholdTokens > 0 && h.EstimateArc(arc) > limits.ArcThresholdTokens {
		return liveBoundary(arc)
	}
	return arc.Seq
}

// liveBoundary is the arc's last turn boundary, or its opening where it has
// taken no turn.
func liveBoundary(arc *history.Arc) int {
	last := arc.Seq
	for _, response := range arc.Responses {
		if response.Seq > last {
			last = response.Seq
		}
	}
	return last
}

// CompactionResult is what one compactor produced. What lands in the log must
// be text: an opaque handle to server-held state would make the session
// unreadable to any other model.
type CompactionResult struct {
	Summary string
	Method  string
	ModelID string
	Usage   *TokenUsage
}

// Compactor turns a transcript prefix into a shorter transcript. Its input
// and output are the same type, which is what makes compaction testable
// without a provider. through is the watermark the summary accounts for.
type Compactor interface {
	Compact(ctx context.Context, t history.ModelTranscript, through int) (CompactionResult, error)
}

// CompactionPlan is what a compaction of one agent is equipped with: the
// compactor its settings name and the limits the cut and the projection are
// drawn under. It is resolved without building an agent, so the compaction
// worker never depends on the parts a run needs.
type CompactionPlan struct {
	Compactor Compactor
	Limits    CompactionLimits
}

// CompactionResolver answers what a compaction of one agent needs. It is
// separate from AgentResolver because building an agent to work an arc and
// equipping a worker to compact a session are two decisions: a process that
// only ever compacts never builds the former.
type CompactionResolver interface {
	ResolveCompaction(ctx context.Context, agentID string) (*CompactionPlan, error)
}

// TranscriptLLM is an LLM that can be called over an already-projected
// transcript rather than a history, which is what the LLM-summary compactor
// needs: its input is a ModelTranscript, not a log.
type TranscriptLLM interface {
	RespondToTranscript(ctx context.Context, t history.ModelTranscript, systemPrompt string) (*LLMResponse, error)
}

// compactionPrompt is what the LLM-summary compactor asks for.
const compactionPrompt = `Summarise the conversation above. The summary replaces everything above the line in the transcript it is read from, so it must carry what the conversation established and decided, in your own words, faithfully — and nothing else. Do not continue the conversation; produce only the summary.`

// LLMCompactor summarises the region below the watermark with a model call:
// the most faithful and the most expensive method.
type LLMCompactor struct {
	LLM TranscriptLLM
}

func (c *LLMCompactor) Compact(ctx context.Context, t history.ModelTranscript, through int) (CompactionResult, error) {
	reply, err := c.LLM.RespondToTranscript(ctx, t, compactionPrompt)
	if err != nil {
		return CompactionResult{}, fmt.Errorf("summarise through seq %d: %w", through, err)
	}
	return CompactionResult{
		Summary: reply.Content,
		Method:  CompactionMethodLLM,
		ModelID: reply.ModelID,
		Usage:   reply.Usage,
	}, nil
}

// PruneCompactor drops the region below the watermark and replaces it with a
// note saying what was dropped: the first user message, so the summary still
// names what the session is about, and counts of the pruned messages by kind.
// No model call, which makes compaction cheap enough to run routinely.
type PruneCompactor struct{}

func (PruneCompactor) Compact(_ context.Context, t history.ModelTranscript, through int) (CompactionResult, error) {
	var firstUser string
	var users, instructions, taskOpenings, assistants, toolCalls, toolResults, notes int
	for _, message := range t.Messages {
		switch m := message.(type) {
		case history.ModelTextMessage:
			switch m.MessageRole {
			case history.ModelRoleUser:
				users++
				if firstUser == "" {
					firstUser = m.Content
				}
			case history.ModelRoleInstruction:
				instructions++
			case history.ModelRoleTask:
				taskOpenings++
			}
		case history.ModelAssistantMessage:
			assistants++
			toolCalls += len(m.ToolCalls)
		case history.ModelToolMessage:
			toolResults++
		case history.ModelNoteMessage:
			notes++
		}
	}
	if firstUser == "" {
		firstUser = "none"
	}
	summary := fmt.Sprintf(
		"[pruned: the events up to seq %d were dropped and replaced by this note]\n"+
			"First user message: %q\n"+
			"Pruned: %d user messages, %d assistant messages carrying %d tool calls, %d tool results, %d notes, %d instructions, %d task openings.",
		through, firstUser, users, assistants, toolCalls, toolResults, notes, instructions, taskOpenings)
	return CompactionResult{Summary: summary, Method: CompactionMethodPrune}, nil
}

// ProviderCompactor delegates to the provider's own compaction endpoint, on a
// model that has one. What the endpoint is sent and how its answer is rendered
// is the provider's business; what comes back must be text.
type ProviderCompactor struct {
	LLM LLM
}

// transcriptCompactor is a model client with a compaction endpoint.
type transcriptCompactor interface {
	CompactTranscript(ctx context.Context, t history.ModelTranscript) (*LLMResponse, error)
}

func (c *ProviderCompactor) Compact(ctx context.Context, t history.ModelTranscript, through int) (CompactionResult, error) {
	endpoint, ok := c.LLM.(transcriptCompactor)
	if !ok {
		return CompactionResult{}, fmt.Errorf("the model client %T has no compaction endpoint", c.LLM)
	}
	reply, err := endpoint.CompactTranscript(ctx, t)
	if err != nil {
		return CompactionResult{}, fmt.Errorf("compact through seq %d: %w", through, err)
	}
	return CompactionResult{
		Summary: reply.Content,
		Method:  CompactionMethodProvider,
		ModelID: reply.ModelID,
		Usage:   reply.Usage,
	}, nil
}

// CompactionWorker serves compaction work items: it computes one COMPACTED
// for the session and runs the completion scan. It holds nothing belonging to
// one session; the session arrives with the item.
type CompactionWorker struct {
	persistor common.EventPersistor
	source    common.EventSource
	resolver  CompactionResolver
	lease     SessionLease
	enqueuer  common.WorkEnqueuer
	modes     history.Modes

	// snapshots is when the worker reads through a stored fold snapshot and
	// saves one. The zero value reads the whole log and never saves.
	snapshots SnapshotPolicy
}

// CompactionWorkerOption configures parts of a compaction worker that not
// every deployment sets.
type CompactionWorkerOption func(*CompactionWorker)

// WithCompactionSnapshotPolicy sets when the worker reads the session through
// a stored fold snapshot and saves one at the end of a compaction. Without
// one, snapshots are neither read nor saved.
func WithCompactionSnapshotPolicy(policy SnapshotPolicy) CompactionWorkerOption {
	return func(w *CompactionWorker) { w.snapshots = policy }
}

// NewCompactionWorker builds a compaction worker from the parts a compaction
// is made of. A nil lease becomes a LocalSessionLease; a nil enqueuer leaves
// the completion scan with nowhere to send resumed arcs.
func NewCompactionWorker(persistor common.EventPersistor, source common.EventSource, resolver CompactionResolver, lease SessionLease, enqueuer common.WorkEnqueuer, modes history.Modes, options ...CompactionWorkerOption) (*CompactionWorker, error) {
	if persistor == nil || source == nil || resolver == nil {
		return nil, errors.New("a compaction worker needs a persistor, a source and a resolver")
	}
	if lease == nil {
		lease = NewLocalSessionLease()
	}
	worker := &CompactionWorker{
		persistor: persistor,
		source:    source,
		resolver:  resolver,
		lease:     lease,
		enqueuer:  enqueuer,
		modes:     modes,
	}
	for _, option := range options {
		option(worker)
	}
	return worker, nil
}

// Work computes one compaction for the session named by the request. It
// returns false without computing when another worker holds the session
// lease, which leaves the item to autoclaim. Everything else — the request
// check, the watermark, the COMPACTED and the completion scan — happens
// under the lease, on one read, so nothing can land between the check and
// the write. An item whose request a landed COMPACTED already covers is
// discarded on that read.
func (w *CompactionWorker) Work(ctx context.Context, sessionID, requestID, agentID string) (bool, error) {
	if agentID == "" {
		return false, errors.New("a compaction item names no agent")
	}
	if requestID == "" {
		return false, errors.New("a compaction item names no request")
	}

	release, ok, err := w.lease.Acquire(ctx, sessionID)
	if err != nil {
		return false, fmt.Errorf("could not take the session lease on %s: %w", sessionID, err)
	}
	if !ok {
		// The lease is held: leave the item unacknowledged and let autoclaim
		// re-offer it.
		return false, nil
	}
	defer release()

	// One read under the lease: the watermark is fixed at this read and the
	// summary computation afterwards cannot move the line.
	h, snapInfo, err := w.fold(ctx, sessionID)
	if err != nil {
		return false, err
	}
	req := h.CompactionRequestByID(requestID)
	if req == nil || h.CompactionRequestSatisfied(req) {
		// Nothing to compute: the request is unknown or already covered.
		return true, nil
	}

	plan, err := w.resolver.ResolveCompaction(ctx, agentID)
	if err != nil {
		return false, fmt.Errorf("resolve the compaction of %s under agent %s: %w", sessionID, agentID, err)
	}

	through := computeWatermark(h, plan.Limits)

	transcript, err := h.ProjectThrough(through, w.modes, plan.Limits.projectionOptions())
	if err != nil {
		return false, fmt.Errorf("project the region through seq %d: %w", through, err)
	}
	result, err := plan.Compactor.Compact(ctx, *transcript, through)
	if err != nil {
		return false, err
	}

	stored, err := common.AsSession(sessionID, common.AsAgent(agentID, w.persistor)).Persist(ctx, &events.Event{
		Typ: events.CompactedEventType,
		// No correlation ID: the cutoff rule gives the record its scope.
		Payload: events.CompactedPayload{
			ThroughSeq: through,
			Summary:    result.Summary,
			Method:     result.Method,
			ModelID:    result.ModelID,
			Usage:      wireUsage(result.Usage),
		},
	})
	if err != nil {
		return false, fmt.Errorf("persist COMPACTED for %s: %w", sessionID, err)
	}

	// The completion scan: enqueue every arc with a released, unresolved
	// suspension the compaction just written covers.
	state := history.FoldStateFromHistory(h)
	if err := h.Update(stored[0], state); err != nil {
		return false, fmt.Errorf("fold the COMPACTED: %w", err)
	}
	// The save point of a compaction: the COMPACTED is folded, so h is at the
	// log's head. Best-effort, like every snapshot save — a failure costs the
	// next read a full replay, nothing more.
	saveSnapshot(ctx, w.persistor, sessionID, h, snapInfo, w.snapshots)
	if w.enqueuer != nil {
		for _, arc := range h.Arcs {
			for _, s := range arc.Suspensions {
				if h.SuspensionStateOf(arc, s) == history.SuspensionReleased {
					ref := common.ArcRef{SessionID: sessionID, ArcID: arc.ID}
					// No single event caused this run — the compaction that
					// landed did — so the item carries no trigger.
					if err := w.enqueuer.Enqueue(ctx, ref, agentID, 0); err != nil {
						return true, fmt.Errorf("completion scan could not requeue arc %s: %w", ref, err)
					}
				}
			}
		}
	}
	return true, nil
}

// fold reads the session once and folds it. Under the worker's snapshot
// policy the read returns the session's latest snapshot, when there is one,
// plus the events above it, and the fold resumes from the snapshot; what is
// returned alongside is only the snapshot's info, so the save point at the
// end of the compaction can tell how stale it is — the snapshot's fold itself
// stays here, in the read.
func (w *CompactionWorker) fold(ctx context.Context, sessionID string) (*history.History, *common.SnapshotInfo, error) {
	snap, stored, err := w.source.GetEventsForSession(ctx, sessionID, w.snapshots.readOptions())
	if err != nil {
		return nil, nil, err
	}
	h, err := common.FoldEvents(snap, stored)
	if err != nil {
		return nil, nil, err
	}
	return h, snap.Info(), nil
}

// wireUsage carries a compactor's reported cost into the record, dropping a
// nil, since a zero in the log is a measured zero.
func wireUsage(usage *TokenUsage) *events.TokenUsage {
	if usage == nil {
		return nil
	}
	return &events.TokenUsage{Input: usage.Input, CachedInput: usage.CachedInput, Output: usage.Output}
}
