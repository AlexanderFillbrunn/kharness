package history

import (
	"slices"
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

// foldedRecord is one record of the log as the fold holds it: where it sits
// in the log, what its projected text estimates to, and whether a turn begins
// there (an arc's opening or an assistant message).
type foldedRecord struct {
	seq          int
	estimate     int
	turnBoundary bool
}

// records returns the session's folded records in log order. The fold stores
// arcs and tasks separately, so the global order — which interleaves parallel
// arcs — is restored by sorting on seq, which is unique across the log. Each
// record's estimate mirrors ProjectedText, so a walk over these is the same
// walk the raw events would give. Records the projection never emits (a
// dispatch, a close, a request, a suspension) are kept with a zero estimate,
// since they occupy a position in the log even where they add no tokens.
func (h *History) records() []foldedRecord {
	var records []foldedRecord
	add := func(seq, estimate int, boundary bool) {
		records = append(records, foldedRecord{seq: seq, estimate: estimate, turnBoundary: boundary})
	}
	for _, arc := range h.Arcs {
		add(arc.Seq, EstimateTokens(h.OpeningContent(arc)), true)
		for _, response := range arc.Responses {
			text := response.Content
			if len(response.Reasoning) > 0 {
				text += "\n" + string(response.Reasoning)
			}
			add(response.Seq, EstimateTokens(text), true)
			for _, call := range response.ToolCalls {
				add(call.Seq, EstimateTokens(call.ToolName+" "+formatValue(call.Args)), false)
				for _, ev := range call.Events {
					add(ev.Seq, EstimateTokens(formatValue(ev.Payload)), false)
				}
				if call.Interrupt != nil {
					add(call.Interrupt.Seq, EstimateTokens(call.Interrupt.Reason+" "+formatValue(call.Interrupt.Context)), false)
					if call.Interrupt.ResolvedSeq != nil {
						add(*call.Interrupt.ResolvedSeq, EstimateTokens(formatValue(call.Interrupt.Response)), false)
					}
				}
				if call.FinishedSeq != nil {
					add(*call.FinishedSeq, EstimateTokens(resultText(call.Result)), false)
				}
			}
		}
		for _, failure := range arc.Errors {
			add(failure.Seq, EstimateTokens(failure.Message), false)
		}
		if arc.Cancel != nil {
			estimate := 0
			if arc.Cancel.Reason != nil {
				estimate = EstimateTokens(*arc.Cancel.Reason)
			}
			add(arc.Cancel.Seq, estimate, false)
		}
		for _, s := range arc.Suspensions {
			add(s.Seq, 0, false)
		}
		if arc.Closed != nil {
			add(arc.Closed.Seq, 0, false)
		}
	}
	for _, task := range h.Tasks {
		add(task.Seq, EstimateTokens(task.TaskName+" "+formatValue(task.Args)), false)
		for i := range task.Events {
			add(task.Events[i].Seq, estimateTaskEvent(&task.Events[i]), false)
		}
		if task.FinishedSeq != nil {
			add(*task.FinishedSeq, EstimateTokens(resultText(task.Result)), false)
		}
	}
	for _, req := range h.CompactionRequests {
		add(req.Seq, 0, false)
	}
	if h.LastCompaction != nil {
		add(h.LastCompaction.Seq, EstimateTokens(h.LastCompaction.Summary), false)
	}
	slices.SortFunc(records, func(a, b foldedRecord) int { return a.seq - b.seq })
	return records
}

// estimateTaskEvent estimates one task transcript entry the way ProjectedText
// renders it: a notification or a result as its text, an input question or
// answer as its message.
func estimateTaskEvent(ev *AsyncTaskEvent) int {
	switch ev.Type {
	case AsyncTaskEventInputRequested, AsyncTaskEventInputProvided:
		if s, ok := ev.Payload.(string); ok {
			return EstimateTokens(s)
		}
	}
	return EstimateTokens(resultText(ev.Payload))
}

// TailCap walks the folded records backwards from the end of the log,
// accumulating the per-record estimate until keep is reached, and returns the
// last turn boundary at or below that point. Where the whole log estimates
// below the keep, the cap constrains nothing and the end of the log stands.
func (h *History) TailCap(keep int) int {
	records := h.records()
	if len(records) == 0 && h.LastSeq == 0 {
		return -1
	}
	last := h.LastSeq
	if keep <= 0 {
		return last
	}
	accumulated := 0
	tokenPoint := -1
	for i := len(records) - 1; i >= 0; i-- {
		accumulated += records[i].estimate
		if accumulated >= keep {
			tokenPoint = records[i].seq
			break
		}
	}
	if tokenPoint < 0 {
		return last
	}
	cap := -1
	for _, r := range records {
		if r.seq > tokenPoint {
			break
		}
		if r.turnBoundary {
			cap = r.seq
		}
	}
	if cap < 0 {
		cap = tokenPoint
	}
	return cap
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
