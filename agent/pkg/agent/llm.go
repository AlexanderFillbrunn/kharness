package agent

import (
	"context"
	"fmt"

	"fillbrunn.ai/kube-agent/agent/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
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
