package history

import (
	"unicode/utf8"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// This file holds the one token heuristic every compaction number uses where
// no reported usage is available: ceil(chars/4) over a record's projected
// text. One heuristic for every model, no per-model tokenizer. It feeds the
// tail cap, the arc-threshold fit, the admission estimate and the fallback
// when a reply's usage is nil.

// EstimateTokens estimates the tokens in one piece of text: ceil(chars/4),
// counted in characters (runes), identically for every model.
func EstimateTokens(text string) int {
	return (utf8.RuneCountInString(text) + 3) / 4
}

// EstimateEvent estimates one event's contribution to a transcript: the
// estimate over the event's projected text. Bookkeeping the projection never
// emits (a dispatch, a close, a request, a suspension) estimates to zero.
func EstimateEvent(ev *events.Event) int {
	return EstimateTokens(ProjectedText(ev))
}

// ProjectedText renders the text a projection carries for one event, standing
// alone: what the model would be told for this record by itself. It is the
// material the estimate is taken over and the same material the summary is
// made from.
func ProjectedText(ev *events.Event) string {
	switch pl := ev.Payload.(type) {
	case events.UserMessagePayload:
		return pl.Content
	case events.AssistantMessagePayload:
		text := pl.Content
		if len(pl.Reasoning) > 0 {
			// Reasoning travels back to the provider with its message, so it
			// counts; it is opaque bytes, so it is counted as-is.
			text += "\n" + string(pl.Reasoning)
		}
		return text
	case events.ToolCallPayload:
		return pl.ToolName + " " + formatValue(pl.Args)
	case events.ToolResultPayload:
		return resultText(pl.Result)
	case events.ToolEventPayload:
		return formatValue(pl.Payload)
	case events.ToolDispatchedPayload:
		return ""
	case events.InterruptPayload:
		return pl.Reason + " " + formatValue(pl.Context)
	case events.InterruptResponsePayload:
		return formatValue(pl.Response)
	case events.AsyncTaskInvokePayload:
		return pl.TaskName + " " + formatValue(pl.Args)
	case events.AsyncTaskResultPayload:
		return resultText(pl.Result)
	case events.AsyncTaskEventPayload:
		return resultText(pl.Payload)
	case events.AsyncTaskInputRequestedPayload:
		return pl.Message
	case events.AsyncTaskInputProvidedPayload:
		return pl.Message
	case events.ErrorPayload:
		return pl.Message
	case events.GuardrailBlockedPayload:
		return pl.Message
	case events.CancelPayload:
		if pl.Reason != nil {
			return *pl.Reason
		}
		return ""
	case events.ArcClosedPayload:
		return ""
	case events.CompactedPayload:
		// The summary is projected at the head of the transcript.
		return pl.Summary
	case events.CompactionRequestedPayload:
		return ""
	case events.ArcSuspendedPayload:
		return ""
	}
	return ""
}

// EstimateTranscript estimates the tokens of the transcript a projection of
// this history would produce, over every record the fold holds.
func (h *History) EstimateTranscript() int {
	return h.EstimateAbove(-1)
}

// EstimateAbove estimates the tokens of the transcript a projection would
// produce above a watermark: the summary where the compaction in force reaches
// past the watermark, plus every folded record above it. This is the estimate
// the admission check makes for the transcript a run would read.
func (h *History) EstimateAbove(watermark int) int {
	total := 0
	if h.LastCompaction != nil && h.LastCompaction.ThroughSeq > watermark {
		total += EstimateTokens(h.LastCompaction.Summary)
	}
	for _, arc := range h.Arcs {
		total += h.estimateArcAbove(arc, watermark)
	}
	return total
}

// EstimateArc estimates the tokens of one arc's own log, the measure the arc
// threshold tests when the compaction draws its line.
func (h *History) EstimateArc(arc *Arc) int {
	return h.estimateArcAbove(arc, -1)
}

// estimateArcAbove estimates one arc's projected records above a watermark.
func (h *History) estimateArcAbove(arc *Arc, watermark int) int {
	total := 0
	if arc.Seq > watermark {
		total += EstimateTokens(h.OpeningContent(arc))
	}
	for _, response := range arc.Responses {
		if response.Seq <= watermark {
			continue
		}
		total += EstimateTokens(response.Content)
		total += EstimateTokens(string(response.Reasoning))
		for _, call := range response.ToolCalls {
			total += EstimateTokens(call.ToolName + " " + formatValue(call.Args))
			if call.FinishedSeq != nil && *call.FinishedSeq > watermark {
				total += EstimateTokens(resultText(call.Result))
			}
		}
	}
	for _, failure := range arc.Errors {
		if failure.Seq > watermark {
			total += EstimateTokens(failure.Message)
		}
	}
	for _, task := range h.TasksOfArc(arc.ID) {
		if task.Seq > watermark {
			total += EstimateTokens(taskLabel(task) + " " + formatValue(task.Args))
		}
	}
	return total
}
