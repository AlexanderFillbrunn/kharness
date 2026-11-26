package agent

import (
	"context"
	"errors"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// RawToken is a token emitted from the LLM during streaming.
type RawToken struct {
	Content string
	Seq int
}

// LLMOptions holds information for an LLM to control how new prompts are handled.
type LLMOptions struct {
	SystemPrompt string
	Tools        ToolSet
}

// LLMError is a provider's complaint about a request, typed: the status, the
// type, the code and the message the provider gave. A rejection of an
// over-budget transcript arrives as a *ContextOverflowError carrying these
// fields — the vendor classifies, from its own fields, never from the status
// alone.
type LLMError struct {
	// Status is the HTTP status the error arrived with, or 200 where the
	// provider sent an error object inside an otherwise successful stream.
	Status int
	Type   string
	Code   string
	Message string
}

func (e *LLMError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("llm: %d %s %s: %s", e.Status, e.Type, e.Code, e.Message)
	}
	return fmt.Sprintf("llm: %s %s: %s", e.Type, e.Code, e.Message)
}

// ContextOverflowError is a provider rejection the vendor recognises as the
// context budget: the typed complaint, plus the stop reason where the whole
// signal was a reply that ended early on a length stop. Response and
// StreamResponse return it as the error — never a bare error the caller has
// to classify — and StreamResponse may return a non-nil response alongside
// it, the partial reply the provider produced before stopping.
type ContextOverflowError struct {
	LLMError

	// StopReason is set where the rejection is a final length stop on an
	// otherwise successful reply; the LLMError fields are zero then.
	StopReason string
}

func (e *ContextOverflowError) Error() string {
	if e.StopReason != "" {
		return fmt.Sprintf("llm: the provider ended the reply early on a length stop: %s", e.StopReason)
	}
	return e.LLMError.Error()
}

// Unwrap exposes the provider's typed complaint underneath.
func (e *ContextOverflowError) Unwrap() error { return &e.LLMError }

// LLMResponse is one reply from a model: what it said and the calls it asked
// for. The agent turns it into events.
type LLMResponse struct {
	// VendorID is the provider's own identifier for the message, where the
	// provider issues one.
	VendorID *string

	// ModelID is the model that produced the reply, as the provider reports
	// it, which need not be the model that was asked for.
	ModelID string

	Content string

	// Reasoning is the provider's record of how the model reached this reply,
	// opaque bytes to be handed back verbatim, and nil where the provider
	// returns none.
	Reasoning []byte

	// Usage is what the reply cost. It is nil where the provider reported
	// nothing, which is not the same as zero.
	Usage *TokenUsage

	// StopReason is why the provider ended the reply, in the provider's own
	// vocabulary. A final length stop is reported as a *ContextOverflowError
	// alongside the reply rather than left for the caller to classify; an
	// output-limit stop never is.
	StopReason string

	ToolCalls []LLMToolCall
}

// TokenUsage is what one exchange cost, in tokens.
type TokenUsage struct {
	// Input counts every prompt token, CachedInput included.
	Input int

	// CachedInput is the part of the prompt served from the provider's cache.
	CachedInput int

	// Output counts the tokens the model generated, reasoning included.
	Output int
}

// LLMToolCall is one call a reply requested, under the provider's own ID.
type LLMToolCall struct {
	VendorID string
	ToolName string
	Args     any
}

// LLM represents a large language model with tool calling capabilities.
//
// The transcript handed in is already projected by the caller: an LLM encodes
// what it is given into the provider's wire format and calls the provider,
// and never sees the log. Projecting is a pure function of the history, and
// it lives in pkg/history with the agent that runs it.
type LLM interface {
	// StreamResponse and Response return a *ContextOverflowError where the
	// provider rejects the transcript as over budget. Where the signal was a
	// final length stop, StreamResponse returns the partial reply non-nil
	// alongside the error. The classification is the vendor's, made where the
	// provider's own fields are read — a status with a context code, a 413, a
	// message pattern where no code exists, an error object inside a 200
	// stream, a final length stop — never the status alone, never an
	// output-limit stop. A vendor with no reliable signal returns no
	// ContextOverflowError at all.
	StreamResponse(ctx context.Context, transcript *history.ModelTranscript, opts *LLMOptions, tokSink common.Sink[RawToken]) (*LLMResponse, error)
	Response(ctx context.Context, transcript *history.ModelTranscript, opts *LLMOptions) (*LLMResponse, error)
}

// llmStep projects the arc so far and calls the LLM with the transcript, then
// records what it replied.
func (a Agent) llmStep(ctx context.Context, state *agentState) error {
	// The message is named before the model is called rather than when the
	// reply is written, because the text being streamed has to say which
	// message will record it.
	messageID := common.NewEventID()

	// The projection happens here rather than inside the model client: it is
	// a pure function of the log, and the LLM only encodes what it is handed.
	// The arc being worked is the projection's target.
	transcript, err := state.history.Project(state.arc.ID, state.modes, a.config.projectionOptions())
	if err != nil {
		return fmt.Errorf("project arc %s: %w", state.arc.ID, err)
	}
	tools := CombineTools(a.tools, taskToolSet{a.tasks}, interruptToolSet{a.interruptSpecs}).All()
	req := AssistantRequest{
		ArcID:           state.arc.ID,
		Modes:           state.modes,
		SystemPrompt:    a.systemPrompt,
		Transcript:      transcript,
		Tools:           tools,
		EstimatedTokens: state.history.EstimateArc(state.arc),
	}
	// BeforeAssistant may reshape the request — its changes reach only this
	// call, never the log — or replace the model entirely with a reply of its
	// own, which is recorded like any other.
	req, tools, shortCircuit, herr := applyBeforeAssistant(ctx, req, tools, state)
	if herr != nil {
		return herr
	}
	if shortCircuit != nil {
		return a.recordResponse(ctx, shortCircuit, messageID, state)
	}
	response, err := a.llm.StreamResponse(ctx, req.Transcript, &LLMOptions{
		SystemPrompt: req.SystemPrompt,
		Tools:        toolList(tools),
	}, state.tokenSink(messageID))
	// A rejection the provider recognises as over budget is recovered, not
	// recorded as a failure: the non-fatal Error, the request and the
	// suspension say what happened. A partial reply that came with it — a
	// length stop — is recorded first, so the log holds what the model said.
	var overflow *ContextOverflowError
	if errors.As(err, &overflow) {
		if response != nil {
			if rerr := a.recordResponse(ctx, response, messageID, state); rerr != nil {
				return rerr
			}
		}
		return a.recoverOverflow(ctx, state, overflow.Error())
	}
	if err != nil {
		return fmt.Errorf("error calling LLM: %w", err)
	}
	if err := a.recordResponse(ctx, response, messageID, state); err != nil {
		return err
	}
	return nil
}

// tokenSink labels the model's text with the message that will record it. A
// run nobody is watching discards it, and the watcher's error is dropped
// rather than returned: a screen cannot fail a run.
//
// The provider is handed a plain common.Sink[string], so no LLM implementation learns
// what an arc or a message is.
func (s *agentState) tokenSink(messageID string) common.Sink[RawToken] {
	return common.SinkFromFunc(func(tok RawToken) error {
		if s.tokens != nil {
			_ = s.tokens.Consume(Token{MessageID: messageID, Content: tok.Content, Seq: tok.Seq})
		}
		return nil
	})
}

// recordResponse writes a reply to the log: the message and one event per call
// it requested, in one write.
//
// They go together because a log holding the message alone reads as an answer
// the model never gave, and the next run closes the arc on it. The message ID
// is the one llmStep minted before the call rather than one read back, since
// the calls have to name it before any of them is written and the tokens had
// to name it before that.
func (a Agent) recordResponse(ctx context.Context, response *LLMResponse, messageID string, state *agentState) error {
	reply := AssistantReply{
		ArcID:     state.arc.ID,
		MessageID: messageID,
		VendorID:  response.VendorID,
		ModelID:   response.ModelID,
		Content:   response.Content,
		Reasoning: response.Reasoning,
		Usage:     response.Usage,
		ToolCalls: response.ToolCalls,
	}
	// AfterAssistant sees the reply before anything is persisted, so its
	// changes are what lands in the log — which may deviate from what was
	// streamed to watchers. Abort vetoes the reply and fails the run.
	reply, err := applyAfterAssistant(ctx, reply, state)
	if err != nil {
		return err
	}

	batch := make([]*events.Event, 0, len(reply.ToolCalls)+1)
	batch = append(batch, &events.Event{
		ID:            messageID,
		Typ:           events.AssistantMessageEventType,
		CorrelationID: &state.arc.ID,
		Payload: events.AssistantMessagePayload{
			VendorID:  reply.VendorID,
			ModelID:   reply.ModelID,
			Content:   reply.Content,
			Reasoning: reply.Reasoning,
			Usage:     recordedUsage(reply.Usage),
		},
	})
	for _, call := range reply.ToolCalls {
		batch = append(batch, &events.Event{
			Typ:           events.ToolCallEventType,
			CorrelationID: &state.arc.ID,
			Payload: events.ToolCallPayload{
				MessageID: messageID,
				VendorID:  call.VendorID,
				ToolName:  call.ToolName,
				Args:      call.Args,
			},
		})
	}

	_, err = a.persistAndFold(ctx, state, batch...)
	return err
}

// recordedUsage carries a reply's cost into the record of it. Nothing is
// written where the provider reported nothing, so a zero in the log is a
// measured zero.
func recordedUsage(usage *TokenUsage) *events.TokenUsage {
	if usage == nil {
		return nil
	}
	return &events.TokenUsage{
		Input:       usage.Input,
		CachedInput: usage.CachedInput,
		Output:      usage.Output,
	}
}
