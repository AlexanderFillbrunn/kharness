package history

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// This file defines the provider-agnostic shape a History is projected into
// before it is sent to a model, and the projection that produces it. A History
// records what happened, including records a model has no place for, such as
// interrupts and cancels; a ModelTranscript records only what the model is
// told. PROJECTION.md specifies the rules this file implements.

// ModelMessageRole identifies the author of a message in a model transcript.
type ModelMessageRole string

const (
	ModelRoleUser      ModelMessageRole = "USER"
	ModelRoleAssistant ModelMessageRole = "ASSISTANT"
	// ModelRoleInstruction is a system or developer prompt rather than
	// anything a participant said.
	ModelRoleInstruction ModelMessageRole = "INSTRUCTION"
	// ModelRoleTask marks the opening message of an arc a background task
	// opened. It is not the user speaking. Which role carries it to a
	// provider is the encoder's decision, so the projection leaves it in a
	// role of its own, like a note. See ENCODING.md.
	ModelRoleTask ModelMessageRole = "TASK"
	// ModelRoleTool is the result of a tool call, fed back to the model as its
	// own message.
	ModelRoleTool ModelMessageRole = "TOOL"
	// ModelRoleNote marks synthesized status text that no participant said.
	// Which role carries it to a provider is the encoder's decision, so the
	// projection leaves it in a role of its own. See ENCODING.md.
	ModelRoleNote ModelMessageRole = "NOTE"
)

// ModelMessage is one entry in a model transcript. The unexported method keeps
// the set of implementations closed to this package, so a projection cannot
// produce a message shape providers do not understand.
type ModelMessage interface {
	Role() ModelMessageRole
	modelMessage()
}

// ModelTextMessage is plain text from the user, an instruction given to the
// model, or the opening of a task-opened arc. MessageRole says which, so the
// three share a type.
type ModelTextMessage struct {
	MessageRole ModelMessageRole // user, instruction, or task
	Content     string
}

// Role returns the role the message was constructed with.
func (m ModelTextMessage) Role() ModelMessageRole { return m.MessageRole }

func (ModelTextMessage) modelMessage() {}

// ModelAssistantMessage is a reply from the model, including any tool calls it
// requested. Content and ToolCalls can both be set: a model may say something
// and call a tool in the same message.
type ModelAssistantMessage struct {
	Content   string
	ToolCalls []ModelToolCall

	// Reasoning is the provider's record of how the model reached this
	// message, opaque to everything here, and is nil where the projection
	// could not carry it. It only ever belongs to the message it sits on.
	Reasoning []byte
}

// Role reports that the message came from the model.
func (ModelAssistantMessage) Role() ModelMessageRole { return ModelRoleAssistant }

func (ModelAssistantMessage) modelMessage() {}

// ModelToolOutcome is how a call ended, as the model is told it. A call that
// never ran ended for one of three different reasons, and the model acts
// differently on each, so they do not collapse into one error flag.
type ModelToolOutcome string

const (
	// ModelToolOutcomeSuccess is a call that ran and returned.
	ModelToolOutcomeSuccess ModelToolOutcome = "SUCCESS"
	// ModelToolOutcomeFailed is a call that ran and failed.
	ModelToolOutcomeFailed ModelToolOutcome = "FAILED"
	// ModelToolOutcomeBlocked is a call an automatic policy refused. Nobody
	// was asked, so there is nobody to go back to.
	ModelToolOutcomeBlocked ModelToolOutcome = "BLOCKED"
	// ModelToolOutcomeRefused is a call a human refused when asked to approve
	// it.
	ModelToolOutcomeRefused ModelToolOutcome = "REFUSED"
	// ModelToolOutcomeCancelled is a call that was still pending when its arc
	// was cancelled.
	ModelToolOutcomeCancelled ModelToolOutcome = "CANCELLED"
	// ModelToolOutcomePending is the synthesized stand-in for a call with no
	// result at the cutoff. The log says nothing about how this call ended,
	// and the projection made the message up so the call is answered.
	ModelToolOutcomePending ModelToolOutcome = "PENDING"
)

// Synthesized reports whether the projection invented this outcome rather than
// reading it from the log.
func (o ModelToolOutcome) Synthesized() bool { return o == ModelToolOutcomePending }

// ModelToolMessage returns the outcome of one tool call to the model.
type ModelToolMessage struct {
	// ToolCallID is the call this answers, matching the ID of the
	// ModelToolCall that requested it.
	ToolCallID string

	// Outcome is how the call ended. Content states the same thing in words,
	// for the model; Outcome is for the encoder and anything else reading the
	// transcript as data.
	Outcome ModelToolOutcome

	// Content is the tool's output, or what became of the call when it never
	// ran. Either way the model is told, so it can react.
	Content string

	// IsError is the flag providers carry on a tool result. Every outcome
	// other than success sets it.
	IsError bool
}

// Role reports that the message carries a tool result.
func (ModelToolMessage) Role() ModelMessageRole { return ModelRoleTool }

func (ModelToolMessage) modelMessage() {}

// ModelNoteMessage is synthesized status text: what the projection has to tell
// the model that no participant said. A note carries no role of its own.
type ModelNoteMessage struct {
	Content string
}

// Role reports that the message is a note rather than anything a participant
// said.
func (ModelNoteMessage) Role() ModelMessageRole { return ModelRoleNote }

func (ModelNoteMessage) modelMessage() {}

// ModelToolCall is one call requested by a ModelAssistantMessage.
type ModelToolCall struct {
	ID string

	// VendorID is the provider's own identifier for the call, carried
	// alongside the log's so an encoder can answer the call in a tool result
	// without reading the log.
	VendorID string

	Name string
	Args any
}

// ModelTranscript is the conversation as the model sees it, in the order the
// messages are sent.
type ModelTranscript struct {
	Messages []ModelMessage
}

// Treatment is how much of one record a projection discloses. The values are
// ordered by how much they tell the model.
type Treatment int

const (
	// TreatmentElide drops the record.
	TreatmentElide Treatment = iota
	// TreatmentNote reduces the record to a synthesized note.
	TreatmentNote
	// TreatmentStandIn replaces the record with a synthesized message of the
	// same shape.
	TreatmentStandIn
	// TreatmentKeep projects the record in full.
	TreatmentKeep
)

// String names the treatment as PROJECTION.md writes it.
func (t Treatment) String() string {
	switch t {
	case TreatmentElide:
		return "elide"
	case TreatmentNote:
		return "note"
	case TreatmentStandIn:
		return "stand_in"
	case TreatmentKeep:
		return "keep"
	}
	return fmt.Sprintf("Treatment(%d)", int(t))
}

// Modes is the session-wide decision about how much of each record kind to
// disclose. Modes are fixed for a session, which is what makes a projection a
// pure function of the log and the arc.
type Modes struct {
	// ForeignArc governs an arc other than the one being projected that the
	// log does not show as closed: still open, dead of a fatal error, or
	// withdrawn before anything was attempted. An arc that closed normally is
	// kept as ordinary conversation history.
	ForeignArc Treatment // keep | note | elide

	// Pending governs tool calls with no result at the cutoff.
	Pending Treatment // stand_in | note | elide

	// ToolEvents governs progress notifications emitted by a running call.
	ToolEvents Treatment // note | elide

	// Error governs a non-fatal error the arc survived.
	Error Treatment // note | elide

	// Interrupt governs an approval that was granted. A refusal is a fixed
	// rule and ignores this.
	Interrupt Treatment // note | elide

	// QuoteLimit is how much of a request a note may quote, in characters.
	// A note names the request it is about so the model can tell which one is
	// meant, which takes less than the whole of it. 0 quotes it whole.
	//
	// It is not a treatment: the modes above decide whether a record is
	// disclosed, and this decides how much of one a note spells out.
	QuoteLimit int
}

// DefaultQuoteLimit is how much of a request a note quotes unless a session
// says otherwise.
const DefaultQuoteLimit = 32

// DefaultModes returns the defaults PROJECTION.md marks with `*`.
func DefaultModes() Modes {
	return Modes{
		ForeignArc: TreatmentKeep,
		Pending:    TreatmentStandIn,
		ToolEvents: TreatmentElide,
		Error:      TreatmentElide,
		Interrupt:  TreatmentElide,
		QuoteLimit: DefaultQuoteLimit,
	}
}

// Validate reports whether every mode holds a value that mode allows. A mode
// set outside its own vocabulary is a programming error rather than a
// condition to project around.
func (m Modes) Validate() error {
	const noteOrElide = 1<<TreatmentNote | 1<<TreatmentElide
	for _, check := range []struct {
		name    string
		value   Treatment
		allowed uint
	}{
		{"foreign_arc", m.ForeignArc, 1<<TreatmentKeep | noteOrElide},
		{"pending", m.Pending, 1<<TreatmentStandIn | noteOrElide},
		{"tool_events", m.ToolEvents, noteOrElide},
		{"error", m.Error, noteOrElide},
		{"interrupt", m.Interrupt, noteOrElide},
	} {
		if check.allowed&(1<<check.value) == 0 {
			return fmt.Errorf("mode %s cannot be %s", check.name, check.value)
		}
	}
	if m.QuoteLimit < 0 {
		return fmt.Errorf("quote_limit cannot be negative, got %d", m.QuoteLimit)
	}
	return nil
}

// ProjectionOptions are the limits a projection applies on top of the modes.
// They are configuration, not modes: compaction has no mode, so Modes itself
// gains nothing for them.
type ProjectionOptions struct {
	// MaxToolResultChars caps what one tool result may contribute to the
	// transcript, in characters. A result over the cap is truncated with a
	// marker naming the call and the omitted size; the log keeps the whole
	// result, and the thinning is never an IsError outcome. 0 leaves results
	// whole.
	MaxToolResultChars int

	// ArcThresholdTokens is the size an arc's log may reach before an arc the
	// cut passes through renders as a skeleton rather than whole. 0 disables
	// the skeleton: a spanning arc renders whole.
	ArcThresholdTokens int
}

// Project builds the transcript for one arc. The history must be the one the
// read model produced for this arc: every event up to the arc's own user
// message, plus every later event correlated to it. That query is the cutoff.
//
// Where the session has compacted, the transcript is the summary of the
// compaction in force — the COMPACTED with the greatest ThroughSeq in this
// history — plus the projection of the events above its watermark.
func (h *History) Project(arcID string, modes Modes, opts ...ProjectionOptions) (*ModelTranscript, error) {
	if h == nil {
		return nil, errors.New("history must not be nil")
	}
	if err := modes.Validate(); err != nil {
		return nil, err
	}

	target := -1
	for i, arc := range slices.Backward(h.Arcs) {
		if arc.ID == arcID {
			target = i
			break
		}
	}
	if target < 0 {
		return nil, fmt.Errorf("no arc with ID=%s in history", arcID)
	}

	options := ProjectionOptions{}
	if len(opts) > 0 {
		options = opts[0]
	}
	projector := newProjector(h, h.Arcs[target], modes, options)
	projector.run()
	return &ModelTranscript{Messages: projector.messages()}, nil
}

// ProjectThrough projects the whole session capped at a sequence number: every
// arc kept, every record above through dropped, and the summary of the
// compaction already in force at the head where its watermark falls below
// through. It is the region a compaction summarises — the previous summary
// plus what has landed since — so the compactor's input and output are the
// same type.
func (h *History) ProjectThrough(through int, modes Modes, opts ProjectionOptions) (*ModelTranscript, error) {
	if h == nil {
		return nil, errors.New("history must not be nil")
	}
	if err := modes.Validate(); err != nil {
		return nil, err
	}
	projector := newProjector(h, nil, modes, opts)
	projector.ceiling = through
	projector.allTargets = true
	projector.run()
	return &ModelTranscript{Messages: projector.messages()}, nil
}

// unit is one emission, keyed by where it belongs in the log. A unit holds
// every message that has to stay contiguous, which is how a call and its
// result travel together and how a note is kept from landing between them.
type unit struct {
	seq      int
	sub      int
	messages []ModelMessage
}

// projector holds one projection in progress.
type projector struct {
	history *History
	target  *Arc
	modes   Modes
	opts    ProjectionOptions

	// floor is the watermark of the compaction in force, or -1 where the
	// session has never compacted: everything at or below it is replaced by
	// the summary. ceiling caps the projection at a sequence number, or -1
	// for none; it is set only when projecting the region a compaction
	// summarises.
	floor   int
	ceiling int

	// allTargets marks a session-wide projection, where every arc is projected
	// as the target would be. It is what the compactor reads.
	allTargets bool

	units []unit
}

func newProjector(history *History, target *Arc, modes Modes, opts ProjectionOptions) *projector {
	return &projector{history: history, target: target, modes: modes, opts: opts, floor: history.Watermark(), ceiling: -1}
}

// run walks the arcs the history holds, once, and emits what survives. A
// foreign arc's records all precede the target's user message, and the target
// owns everything correlated to it.
func (p *projector) run() {
	// The summary goes at the head of the transcript, before the first
	// surviving arc; nothing precedes it but the system prompt. It is
	// synthesized, so it is a note — the one note that is not short.
	if p.floor >= 0 && p.history.LastCompaction != nil && (p.ceiling < 0 || p.floor < p.ceiling) {
		p.emit(-1, 0, ModelNoteMessage{Content: p.history.LastCompaction.Summary})
	}
	if p.ceiling >= 0 && p.floor >= p.ceiling {
		// The whole region is already inside the summary in force.
		return
	}

	for _, arc := range p.history.Arcs {
		p.projectArc(arc)
	}

	// A task whose invocation fell below the cut cannot be introduced by the
	// event that started it — that event is summarised — so the note is
	// synthesized from the task table, with the ID verbatim, since a
	// shortened ID resolves the wrong task.
	if p.ceiling < 0 {
		for _, task := range p.history.TasksInOrder() {
			if task.Seq <= p.floor {
				p.emit(p.floor, -1, ModelNoteMessage{Content: taskInvokeNote(task)})
			}
		}
	}
}

// taskInvokeNote is the invocation note synthesized from the task table for a
// task whose invoke event was summarised away: the name, the ID verbatim, and
// the status, including a waiting task's outstanding question.
func taskInvokeNote(task *AsyncTask) string {
	note := fmt.Sprintf("[task %s started: %s", taskLabel(task), formatValue(task.Args))
	switch task.Status {
	case AsyncTaskWaiting:
		if request := lastInputRequestOf(task); request != nil {
			note += fmt.Sprintf("; it is waiting for an answer to: %s", resultText(request.Payload))
		} else {
			note += "; it is waiting for input"
		}
	case AsyncTaskPending:
		note += "; it is running"
	default:
		note += fmt.Sprintf("; it finished: %s", task.Status)
	}
	return note + "]"
}

// lastInputRequestOf is the question a waiting task last asked, or nil.
func lastInputRequestOf(task *AsyncTask) *AsyncTaskEvent {
	var last *AsyncTaskEvent
	for i := range task.Events {
		if task.Events[i].Type == AsyncTaskEventInputRequested {
			last = &task.Events[i]
		}
	}
	return last
}

// messages returns the emitted messages in log order. Units that share a
// sequence number keep the order they were emitted in.
func (p *projector) messages() []ModelMessage {
	sort.SliceStable(p.units, func(i, j int) bool {
		if p.units[i].seq != p.units[j].seq {
			return p.units[i].seq < p.units[j].seq
		}
		return p.units[i].sub < p.units[j].sub
	})
	messages := make([]ModelMessage, 0, len(p.units))
	for _, u := range p.units {
		messages = append(messages, u.messages...)
	}
	return messages
}

// emit places messages at a position in the log. sub orders several emissions
// that belong to the same position, so a note attached to a record lands after
// it.
func (p *projector) emit(seq, sub int, messages ...ModelMessage) {
	if len(messages) == 0 {
		return
	}
	p.units = append(p.units, unit{seq: seq, sub: sub, messages: messages})
}

// emitNote places one synthesized note.
func (p *projector) emitNote(seq, sub int, format string, args ...any) {
	p.emit(seq, sub, ModelNoteMessage{Content: fmt.Sprintf(format, args...)})
}

// arcPlan is what a projection decided about one arc as a whole.
type arcPlan struct {
	// material is how much of the arc's own records reaches the transcript.
	material Treatment
	// status names the note the arc needs, or none.
	status arcNote
	// statusSeq is where that note belongs, which is where the record that
	// created the status was written.
	statusSeq int
}

// arcNote names the kind of status a foreign arc needs reported.
type arcNote int

const (
	arcNoteNone arcNote = iota
	arcNoteInflight
	arcNoteDead
	arcNoteWithdrawn
	arcNoteStopped
)

// projectArc emits one arc under the mode that governs it, against the cut:
// an arc wholly below the watermark is covered by the summary and skipped, an
// arc the line passes through renders as the skeleton where it does not fit
// the arc threshold, and everything else projects whole.
func (p *projector) projectArc(arc *Arc) {
	if p.ceiling >= 0 && arc.Seq > p.ceiling {
		return
	}
	if end := arcEndSeq(arc); end <= p.floor {
		return
	}

	isTarget := p.allTargets || (p.target != nil && arc.ID == p.target.ID)
	plan := planArc(arc, p.modes, isTarget)

	switch plan.material {
	case TreatmentElide:
		return
	case TreatmentNote:
		p.emitArcNote(arc, plan)
		return
	}

	// The line passes through this arc. Where the arc's own log does not fit
	// the arc threshold, the spent prefix renders as a skeleton and the live
	// part stays whole below the line; where it fits, the arc renders whole
	// against the line. Skeletons are a turn's rendering, so a capped
	// projection — the compactor's input — never draws one.
	if p.ceiling < 0 && arc.Seq <= p.floor && arcEndSeq(arc) > p.floor &&
		p.opts.ArcThresholdTokens > 0 && p.history.EstimateArc(arc) > p.opts.ArcThresholdTokens {
		p.keepSpanningArc(arc)
		return
	}

	p.keepArc(arc)

	// The note goes with the material, including when the material is nothing
	// but the user message. A bare user turn reads as a request addressed to
	// this turn, which a model would answer under the wrong arc.
	if plan.status != arcNoteNone {
		p.emitArcNote(arc, plan)
	}
}

// arcEndSeq is the sequence number of the arc's last record, which is where
// the arc ends as a whole: the opening where nothing followed it.
func arcEndSeq(arc *Arc) int {
	end := arc.Seq
	for _, response := range arc.Responses {
		if response.Seq > end {
			end = response.Seq
		}
	}
	for _, failure := range arc.Errors {
		if failure.Seq > end {
			end = failure.Seq
		}
	}
	if arc.Cancel != nil && arc.Cancel.Seq > end {
		end = arc.Cancel.Seq
	}
	if arc.Closed != nil && arc.Closed.Seq > end {
		end = arc.Closed.Seq
	}
	for _, suspension := range arc.Suspensions {
		if suspension.Seq > end {
			end = suspension.Seq
		}
	}
	for _, interrupt := range arc.Interrupts {
		if interrupt.Seq > end {
			end = interrupt.Seq
		}
		if interrupt.ResolvedSeq != nil && *interrupt.ResolvedSeq > end {
			end = *interrupt.ResolvedSeq
		}
	}
	return end
}

// keepSpanningArc renders an arc the cut passes through: the opening request
// verbatim, the tool calls of the spent prefix above the line without their
// results, a note standing at the cut, and the live part below the line whole.
func (p *projector) keepSpanningArc(arc *Arc) {
	role := ModelRoleUser
	if arc.OpenedByTask != "" {
		role = ModelRoleTask
	}
	p.emit(arc.Seq, 0, ModelTextMessage{MessageRole: role, Content: p.history.OpeningContent(arc)})

	for _, message := range arc.Responses {
		if message.Seq <= p.floor {
			// The spent prefix: the reply's text and its results are dropped —
			// the summary covers that region once, in prose — and the calls
			// stay, so the model can see what was tried.
			assistant := ModelAssistantMessage{}
			for _, call := range message.ToolCalls {
				assistant.ToolCalls = append(assistant.ToolCalls, modelToolCall(call))
			}
			if len(assistant.ToolCalls) > 0 {
				p.emit(message.Seq, 0, assistant)
			}
			continue
		}
		p.projectResponse(message, false)
	}
	p.projectErrors(arc)
	p.projectTasks(arc, 0, false)

	// The note stands at the cut, separating the two regions on the page the
	// way the watermark separates them in the log, and says which record
	// wins: the arc's own log is verbatim, the summary is not.
	note := "[earlier work in this request was summarised into the summary at the head of this transcript. " +
		"This arc's own log below this note is verbatim and wins over the summary where the two disagree.]"
	if len(arc.Suspensions) > 0 {
		note += " [this request was paused while its context was being compacted]"
	}
	p.emit(p.floor, 3, ModelNoteMessage{Content: note})
}

// quote renders a request for a note: one line, cut to the length the session
// allows. A request that was cut ends in an ellipsis, so it cannot be read as
// one that was always that short.
func (p *projector) quote(content string) string {
	folded := strings.Join(strings.Fields(content), " ")
	if p.modes.QuoteLimit <= 0 || len([]rune(folded)) <= p.modes.QuoteLimit {
		return folded
	}
	return strings.TrimRight(string([]rune(folded)[:p.modes.QuoteLimit]), " ") + "…"
}

// planArc decides how much of an arc reaches the transcript and which status
// note it needs.
func planArc(arc *Arc, modes Modes, isTarget bool) arcPlan {
	plan := arcPlan{material: TreatmentKeep, statusSeq: arc.Seq}

	fatal := fatalError(arc)
	status := arc.Status()
	switch {
	case isTarget:
		// The arc being projected is never foreign to itself.
	case fatal != nil:
		plan.material = modes.ForeignArc
		plan.status = arcNoteDead
		plan.statusSeq = fatal.Seq
	case status == ArcStatusCancelled:
		// The cancel closed the arc, so it stopped either work in progress or
		// a request nothing had been done about yet. A cancel that reached
		// only an async task lands under Done instead.
		plan.statusSeq = arc.Cancel.Seq
		if len(arc.Responses) > 0 {
			// Fixed rule: a cancel that stopped work in progress is always
			// disclosed, together with the work it stopped.
			plan.status = arcNoteStopped
		} else {
			plan.material = modes.ForeignArc
			plan.status = arcNoteWithdrawn
		}
	case status == ArcStatusDone:
		// A closed arc is ordinary conversation history. A cancel that arrived
		// too late lands here, because it stopped nothing the arc was doing.
	default:
		plan.material = modes.ForeignArc
		plan.status = arcNoteInflight
		plan.statusSeq = lastResponseSeq(arc)
	}
	return plan
}

// keepArc emits an arc's own records. An arc a task opened carries the TASK
// role rather than USER: the task is not the user, and a provider with an
// operator role can carry the opening as one.
func (p *projector) keepArc(arc *Arc) {
	role := ModelRoleUser
	if arc.OpenedByTask != "" {
		role = ModelRoleTask
	}
	if !p.aboveCeiling(arc.Seq) {
		p.emit(arc.Seq, 0, ModelTextMessage{MessageRole: role, Content: p.history.OpeningContent(arc)})
	}

	// An arc that was cancelled or died is not coming back, which changes what
	// an unanswered call under it means: not "no result yet" but "no result,
	// and none is coming".
	stoppedSeq, stopped := arcEnd(arc)
	for _, message := range arc.Responses {
		if p.aboveCeiling(message.Seq) {
			continue
		}
		p.projectResponse(message, stopped)
	}
	for _, interrupt := range arc.Interrupts {
		if p.aboveCeiling(interrupt.Seq) {
			continue
		}
		p.projectArcInterrupt(interrupt)
	}
	p.projectErrors(arc)
	p.projectTasks(arc, stoppedSeq, stopped)
}

// aboveCeiling reports whether a sequence number is past the projection's cap.
func (p *projector) aboveCeiling(seq int) bool {
	return p.ceiling >= 0 && seq > p.ceiling
}

// projectResponse emits one assistant message with its calls, their results
// and any notes its calls produced.
func (p *projector) projectResponse(message *AssistantMessage, arcStopped bool) {
	assistant := ModelAssistantMessage{Content: message.Content}
	var results []ModelMessage
	var notes []ModelMessage

	for _, call := range message.ToolCalls {
		p.projectInterrupt(call)
		if call.FinishedSeq == nil {
			switch p.modes.Pending {
			case TreatmentStandIn:
				assistant.ToolCalls = append(assistant.ToolCalls, modelToolCall(call))
				results = append(results, ModelToolMessage{
					ToolCallID: call.ID,
					Outcome:    ModelToolOutcomePending,
					Content:    pendingStandIn(call, arcStopped),
					IsError:    false,
				})
			case TreatmentNote:
				notes = append(notes, ModelNoteMessage{
					Content: fmt.Sprintf("[a call to %s has not returned yet]", call.ToolName),
				})
			case TreatmentElide:
				// The call goes with the result it never got. Leaving the
				// request in place would put an unanswered call in front of
				// the provider.
			}
			continue
		}

		assistant.ToolCalls = append(assistant.ToolCalls, modelToolCall(call))
		results = append(results, p.toolResult(call))

		if p.modes.ToolEvents == TreatmentNote {
			for _, event := range call.Events {
				notes = append(notes, ModelNoteMessage{
					Content: fmt.Sprintf("[%s reported: %s]", call.ToolName, formatValue(event.Payload)),
				})
			}
		}
	}

	// A provider that takes reasoning back validates it against the message it
	// is sent with. A message that lost a call to a mode is no longer that
	// message, so its reasoning is dropped.
	if len(assistant.ToolCalls) == len(message.ToolCalls) {
		assistant.Reasoning = message.Reasoning
	}

	// Phase 1: an assistant message left with no text and no surviving calls
	// is dropped. Notes its calls produced are not part of the message and
	// survive it.
	if assistant.Content != "" || len(assistant.ToolCalls) > 0 {
		messages := make([]ModelMessage, 0, len(results)+1)
		messages = append(messages, assistant)
		messages = append(messages, results...)
		p.emit(message.Seq, 0, messages...)
	}
	for _, note := range notes {
		p.emit(message.Seq, 1, note)
	}
}

// pendingStandIn says why a call has no result. The four reasons are not
// interchangeable, since the model acts differently on each.
//
// arcStopped says the arc is over, so nothing further is coming.
func pendingStandIn(call *ToolCall, arcStopped bool) string {
	if interrupt := call.Interrupt; interrupt != nil && interrupt.ResolvedSeq == nil {
		return "[this call is waiting for a human to approve it]"
	}
	if arcStopped {
		if call.DispatchedSeq != nil {
			// The one thing the log cannot say. Claiming either that it ran or
			// that it did not would be inventing the answer, and a model told
			// it did not run is a model that runs it again.
			return "[this call was handed to its tool and then stopped before it reported. Whether it took effect is not known.]"
		}
		return "[this call was never made: the request it belonged to ended first]"
	}
	return "[this call has not returned yet]"
}

// projectErrors emits the non-fatal errors an arc survived. The fatal one is
// the arc's status and is reported by planArc. Errors at or below the
// watermark are part of the summary now.
func (p *projector) projectErrors(arc *Arc) {
	for _, failure := range arc.Errors {
		if failure.IsFatal || p.modes.Error != TreatmentNote {
			continue
		}
		if failure.Seq <= p.floor || p.aboveCeiling(failure.Seq) {
			continue
		}
		if failure.CausationID != nil {
			p.emitNote(failure.Seq, 0, "[an error was recorded while working on this: %s]", failure.Message)
			continue
		}
		p.emitNote(failure.Seq, 0, "[an error was recorded: %s]", failure.Message)
	}
}

// projectArcInterrupt emits the notes for an arc-wide interrupt: the ask where
// it was raised, and the answer where it arrived. A cancel is not among them:
// the arc's own status note covers it.
func (p *projector) projectArcInterrupt(interrupt *Interrupt) {
	if p.modes.Interrupt != TreatmentNote {
		return
	}
	p.emit(interrupt.Seq, 0, ModelNoteMessage{Content: askNote(interrupt)})
	if interrupt.ResolvedSeq == nil || p.aboveCeiling(*interrupt.ResolvedSeq) {
		return
	}
	switch interrupt.Status {
	case InterruptAnswered:
		p.emitNote(*interrupt.ResolvedSeq, 0, "[interrupt %s: a human answered: %s]", interrupt.ID, formatValue(interrupt.Response))
	case InterruptApproved:
		p.emitNote(*interrupt.ResolvedSeq, 0, "[interrupt %s: a human approved it]", interrupt.ID)
	}
}

// projectInterrupt emits the notes for the human decision a call was held up
// for. A refusal is not among them: it settled the call, so it reaches the
// model as that call's result.
//
// Each note sits where its event happened, and the two can be far apart, so
// the decision names the interrupt rather than relying on position.
func (p *projector) projectInterrupt(call *ToolCall) {
	interrupt := call.Interrupt
	if interrupt == nil || p.modes.Interrupt != TreatmentNote {
		return
	}

	// The ask, built from the interrupt alone. It says nothing about the
	// outcome, so every transcript that contains it contains the same bytes
	// and a decision arriving later leaves the cached prefix intact.
	p.emit(interrupt.Seq, 0, ModelNoteMessage{Content: askNote(interrupt)})
	if interrupt.ResolvedSeq == nil {
		// Nothing came back. The ask is what tells the model it is waiting on
		// a human.
		return
	}
	if interrupt.Status == InterruptApproved {
		// An approval leaves no mark on the call, which runs as if nobody had
		// been asked, so a note is the only place it can be told.
		p.emitNote(*interrupt.ResolvedSeq, 0, "[interrupt %s: a human approved it]", interrupt.ID)
	}
}

// askNote renders the note an interrupt gets when it is raised. It reads only
// fields a raised interrupt already has, so the note cannot change once the
// decision arrives.
func askNote(interrupt *Interrupt) string {
	var note string
	if interrupt.CausationID == "" {
		note = fmt.Sprintf("[interrupt %s: the request was held for a human to answer", interrupt.ID)
	} else {
		note = fmt.Sprintf("[interrupt %s: a human was asked to approve this call before it runs", interrupt.ID)
	}
	if interrupt.Reason != "" {
		note += ": " + interrupt.Reason
	}
	if interrupt.Context != nil {
		// What the human was shown. The model is told the same thing, so it
		// can account for a refusal without guessing what was on the screen.
		note += ". They were shown: " + formatValue(interrupt.Context)
	}
	return note + "]"
}

// arcEnd reports where an arc stopped and whether it stopped at all. An arc
// that closed normally did not stop: it finished.
func arcEnd(arc *Arc) (int, bool) {
	switch arc.Status() {
	case ArcStatusCancelled:
		return arc.Cancel.Seq, true
	case ArcStatusError:
		if fatal := fatalError(arc); fatal != nil {
			return fatal.Seq, true
		}
	}
	return 0, false
}

// taskLabel names a task in a note. The ID travels with the name because a
// note is the only place the model learns it, and the tools that address a
// task take an ID. It is written whole, since it is what the model hands
// back.
func taskLabel(task *AsyncTask) string {
	return fmt.Sprintf("%s (id %s)", task.TaskName, task.ID)
}

// projectTasks emits the invocation note for every task the arc started. The
// completion of a task whose result opened an arc of its own is that arc — it
// reaches the transcript as the arc's opening message — so no completion note
// is emitted here for it; a task settled without opening an arc (a cancel, or
// the startup reaper) gets its completion note where it finished.
func (p *projector) projectTasks(arc *Arc, stoppedSeq int, arcStopped bool) {
	for _, task := range p.history.TasksOfArc(arc.ID) {
		// An invocation at or below the watermark is summarised with everything
		// else; its note is synthesized from the task table at the cut instead.
		if task.Seq <= p.floor || p.aboveCeiling(task.Seq) {
			continue
		}
		// Fixed rule: the invocation note stays in the origin arc.
		p.emitNote(task.Seq, 0, "[task %s started: %s]", taskLabel(task), formatValue(task.Args))

		if task.FinishedSeq == nil {
			// An arc that is over will produce no result for the task, so the
			// note says what the log knows: the task was running and nobody
			// reported what became of it. The note belongs where the arc ended,
			// which is after everything the task managed to report.
			if arcStopped {
				p.emitNote(stoppedSeq, 1,
					"[task %s was still running when this request was stopped. Whether it finished is not known.]",
					taskLabel(task))
			}
			continue
		}
		if task.ResultArc != "" || *task.FinishedSeq <= p.floor || p.aboveCeiling(*task.FinishedSeq) {
			continue
		}
		switch task.Status {
		case AsyncTaskFailed:
			p.emitNote(*task.FinishedSeq, 1, "[task %s failed: %s]", taskLabel(task), resultText(task.Result))
		case AsyncTaskCancelled:
			p.emitNote(*task.FinishedSeq, 1, "[task %s was cancelled before it finished]", taskLabel(task))
		default:
			p.emitNote(*task.FinishedSeq, 1, "[task %s finished: %s]", taskLabel(task), resultText(task.Result))
		}
	}
}

// emitArcNote reports a foreign arc's status.
func (p *projector) emitArcNote(arc *Arc, plan arcPlan) {
	switch plan.status {
	case arcNoteInflight:
		p.emitNote(plan.statusSeq, 1,
			"[another request is still being worked on: %q. It is not yours to answer and its result is not available yet.]",
			p.quote(arc.Content))
	case arcNoteDead:
		p.emitNote(plan.statusSeq, 1,
			"[an earlier request, %q, failed and was never answered.]", p.quote(arc.Content))
	case arcNoteWithdrawn:
		p.emitNote(plan.statusSeq, 1,
			"[an earlier request, %q, was withdrawn before it was acted on.]", p.quote(arc.Content))
	case arcNoteStopped:
		// The note must not guess at motive. Cancel.Reason is optional free
		// text, so without it the log records that a human stopped this and
		// not why.
		if reason := cancelReason(arc.Cancel); reason != "" {
			p.emitNote(plan.statusSeq, 1,
				"[a human stopped this request while it was running: %s. The approach above did not finish.]", reason)
			return
		}
		p.emitNote(plan.statusSeq, 1,
			"[a human stopped this request while it was running. The approach above did not finish.]")
	}
}

// fatalError returns the failure that closed the arc, or nil.
func fatalError(arc *Arc) *Error {
	for _, failure := range arc.Errors {
		if failure.IsFatal {
			return failure
		}
	}
	return nil
}

// lastResponseSeq returns where an open arc's status note belongs: after the
// last thing it said, which is where its unanswered calls are.
func lastResponseSeq(arc *Arc) int {
	seq := arc.Seq
	for _, response := range arc.Responses {
		if response.Seq > seq {
			seq = response.Seq
		}
	}
	return seq
}

// modelToolCall carries a call to the model under the log's own ID, with the
// provider's identifier for it alongside, so an encoder can answer the call
// without the log.
func modelToolCall(call *ToolCall) ModelToolCall {
	return ModelToolCall{ID: call.ID, VendorID: call.VendorID, Name: call.ToolName, Args: call.Args}
}

// toolResult renders a settled call's outcome, thinned to the configured cap.
func (p *projector) toolResult(call *ToolCall) ModelToolMessage {
	message := toolResult(call)
	message.Content = p.thinToolResult(call, message.Content)
	return message
}

// thinToolResult caps what one tool result may contribute to the transcript.
// The log keeps the whole result; what the model is told is truncated with a
// marker naming the call and the omitted size. The thinning is the harness's
// own doing, so it is never an IsError outcome — the model must not read it as
// the tool failing.
func (p *projector) thinToolResult(call *ToolCall, content string) string {
	limit := p.opts.MaxToolResultChars
	if limit <= 0 {
		return content
	}
	runes := []rune(content)
	if len(runes) <= limit {
		return content
	}
	omitted := len(runes) - limit
	return string(runes[:limit]) + fmt.Sprintf(
		"\n… [truncated by the harness: the result of call %s (%s) was %d characters, %d omitted; the log holds the whole result]",
		call.ID, call.ToolName, len(runes), omitted)
}

// toolResult renders a settled call's outcome. Every terminal status is
// reported, because a failure the model is not told about is a failure it
// repeats.
//
// A call that never ran says who stopped it, since a policy, a human and a
// cancel are three different things to the model.
func toolResult(call *ToolCall) ModelToolMessage {
	message := ModelToolMessage{
		ToolCallID: call.ID,
		Outcome:    ModelToolOutcomeSuccess,
		Content:    resultText(call.Result),
	}
	switch call.Status {
	case ToolCallStatusFailed:
		message.Outcome = ModelToolOutcomeFailed
	case ToolCallStatusBlocked:
		message.Outcome = ModelToolOutcomeBlocked
		message.Content = refusalText("a policy refused this call, without asking anyone", message.Content)
	case ToolCallStatusRefused:
		message.Outcome = ModelToolOutcomeRefused
		message.Content = refusalText("a human refused this call", message.Content)
	case ToolCallStatusCancelled:
		// A cancel only settles a call its tool never saw, so this one
		// provably never ran. A call that was already dispatched is still
		// pending and is reported by pendingStandIn instead.
		message.Outcome = ModelToolOutcomeCancelled
		message.Content = refusalText("this call was cancelled before it ran", message.Content)
	}
	message.IsError = message.Outcome != ModelToolOutcomeSuccess
	return message
}

// refusalText states who stopped a call, and what they said about it where the
// log has that.
func refusalText(by, detail string) string {
	if detail == "" {
		return fmt.Sprintf("[%s]", by)
	}
	return fmt.Sprintf("[%s: %s]", by, detail)
}

// cancelReason returns the reason a human gave, or an empty string when the
// log recorded none.
func cancelReason(cancel *Cancel) string {
	if cancel == nil || cancel.Reason == nil {
		return ""
	}
	return *cancel.Reason
}

// resultText renders a payload as text for the model. Strings are passed
// through, because a tool that returned text should not reach the model as a
// JSON string literal.
func resultText(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case *string:
		if typed == nil {
			return ""
		}
		return *typed
	}
	return formatValue(value)
}
