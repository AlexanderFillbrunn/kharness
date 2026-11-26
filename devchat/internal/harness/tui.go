package harness

import (
	"context"
	"errors"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"fillbrunn.ai/kube-agent/agent/pkg/agent"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// Messages carrying a run into the interface. A run happens on its own
// goroutine and reports through the program, which is the one way into the
// update loop that is safe from outside it.
type (
	tokenMsg    string
	eventMsg    struct{ event *events.Event }
	turnDoneMsg struct{ err error }

	// arcDoneMsg reports the end of a run the report path started on its own,
	// for an arc a task opened. It is not the turn in the input, so it leaves
	// the run-in-progress tracking alone.
	arcDoneMsg struct{ err error }

	// taskUnfiledMsg is a report a task could not get written at all. It comes
	// from the task rather than the log for the one reason the log cannot
	// carry it: there is nothing in the log to carry.
	taskUnfiledMsg struct {
		ref common.TaskRef
		err error
	}
)

// screenEvents is the event sink the persistor publishes to. Everything that
// is written reaches the screen this way, whoever wrote it, which is why the
// agent needs no notion of an interface.
type screenEvents struct{ relay *sender }

func (s screenEvents) Consume(_ context.Context, evs ...*events.Event) {
	for _, ev := range evs {
		s.relay.send(eventMsg{event: ev})
	}
}

// screenTokens is the token sink the worker streams replies to. The session
// and the arc are dropped here, since this harness only ever shows one.
type screenTokens struct{ relay *sender }

func (t screenTokens) Token(_ context.Context, tok agent.StreamToken) {
	t.relay.send(tokenMsg(tok.Content))
}

// sender hands messages to the program. It exists because the sinks have to be
// built before the program they report to: the model is passed to NewProgram,
// so nothing can hold the program until after that call.
// Messages are queued and one goroutine delivers them to the program, in
// order. The program's own message channel is unbuffered and read only by its
// event loop, so a sink running on that loop — a write the interface itself
// made inside Update, like an interrupt answer — would block forever on the
// send and the update it was part of would never finish.
type sender struct {
	mu      sync.RWMutex
	program *tea.Program

	once  sync.Once
	queue chan tea.Msg
}

func (s *sender) attach(program *tea.Program) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.program = program
}

func (s *sender) send(msg tea.Msg) {
	s.mu.RLock()
	program := s.program
	s.mu.RUnlock()
	if program == nil {
		return
	}
	s.once.Do(func() {
		s.queue = make(chan tea.Msg, 4096)
		go func() {
			for queued := range s.queue {
				program.Send(queued)
			}
		}()
	})
	s.queue <- msg
}

type model struct {
	session    *session
	transcript *transcript
	sender     *sender

	viewport viewport.Model
	input    textarea.Model
	spinner  spinner.Model

	// settings is the definition the resolver reads, held so the header shows
	// the model it names and /model can rewrite it. It is only ever changed
	// between turns, and the resolver rereads the file on the next arc.
	settings     agent.AgentSettings
	settingsPath string

	// cancel stops the run in progress, and is nil when nothing is running.
	cancel context.CancelFunc

	// arc names what the run in progress is working on, and is empty when
	// nothing is running. A cancel is recorded against an arc, so the name has
	// to be to hand before the run has written anything to read it from.
	arc string

	// answering is the questions form being filled, or nil when the input is
	// a message. The form's answers are mirrored into its block's response so
	// the render shows them as they are entered.
	answering *answerForm

	width  int
	height int
	ready  bool
}

// answerForm is one questions block being answered in the chat.
type answerForm struct {
	block   *block
	cursor  int
	answers map[string]any
}

// startAnswering enters answer mode on the first pending questions block, if
// there is one and nothing is running.
func (m *model) startAnswering() {
	if m.running() || m.answering != nil {
		return
	}
	b := m.transcript.pendingQuestions()
	if b == nil {
		return
	}
	m.answering = &answerForm{block: b, answers: map[string]any{}}
	m.transcript.active = b
	m.transcript.cursor = 0
	m.input.Reset()
}

// leaveAnswering drops answer mode without answering; the interrupt stays
// pending and can be answered later with /answer or by pressing a.
func (m *model) leaveAnswering() {
	m.answering = nil
	m.transcript.active = nil
	m.input.Reset()
}

// answerKey routes a key press while a questions form is being answered:
// up/down move between questions, digits pick or toggle choices, typing fills
// a text answer, enter submits, esc leaves the form pending.
func (m model) answerKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	f := m.answering
	if f == nil || len(f.block.questions) == 0 {
		return m, nil
	}
	q := f.block.questions[f.cursor]

	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "esc":
		m.leaveAnswering()
		return m.refresh(), nil
	case "up":
		m.saveTextAnswer(q)
		if f.cursor > 0 {
			f.cursor--
		} else {
			f.cursor = len(f.block.questions) - 1
		}
		m.transcript.cursor = f.cursor
		m.loadTextAnswer(f.block.questions[f.cursor])
		return m.refresh(), nil
	case "down":
		m.saveTextAnswer(q)
		if f.cursor < len(f.block.questions)-1 {
			f.cursor++
		} else {
			f.cursor = 0
		}
		m.transcript.cursor = f.cursor
		m.loadTextAnswer(f.block.questions[f.cursor])
		return m.refresh(), nil
	case "enter":
		return m.submitAnswers()
	}

	switch q.Kind {
	case "single_choice", "multi_choice":
		if len(q.Options) == 0 {
			return m.refresh(), nil
		}
		digit := strings.TrimPrefix(msg.String(), "")
		if n, err := strconv.Atoi(digit); err == nil && n >= 1 && n <= len(q.Options) && n <= 9 {
			option := q.Options[n-1]
			if q.Kind == "single_choice" {
				f.answers[q.ID] = option
			} else {
				current, _ := f.answers[q.ID].([]any)
				next := make([]any, 0, len(current)+1)
				found := false
				for _, item := range current {
					if item == option {
						found = true
						continue
					}
					next = append(next, item)
				}
				if !found {
					next = append(next, option)
				}
				f.answers[q.ID] = next
			}
			f.block.response = map[string]any{"answers": f.answers}
		}
		return m.refresh(), nil
	default:
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		m.saveTextAnswer(q)
		return m.refresh(), cmd
	}
}

// saveTextAnswer puts the input's text on the question being answered.
func (m model) saveTextAnswer(q question) {
	if q.Kind != "text" && !(len(q.Options) == 0) {
		return
	}
	m.answering.answers[q.ID] = m.input.Value()
	m.answering.block.response = map[string]any{"answers": m.answering.answers}
}

// loadTextAnswer puts a question's saved answer back into the input.
func (m *model) loadTextAnswer(q question) {
	m.input.Reset()
	if q.Kind != "text" && len(q.Options) > 0 {
		return
	}
	if text, ok := m.answering.answers[q.ID].(string); ok {
		m.input.SetValue(text)
	}
}

// submitAnswers writes the INTERRUPT_RESPONSE with the collected answers and
// hands the arc back to the worker. Every required question must have one.
func (m model) submitAnswers() (tea.Model, tea.Cmd) {
	f := m.answering
	q := f.block.questions[f.cursor]
	m.saveTextAnswer(q)
	for _, question := range f.block.questions {
		if !question.required() {
			continue
		}
		if answer, ok := f.answers[question.ID]; !ok || emptyAnswer(answer) {
			m.transcript.note(fmt.Sprintf("question %d (%s) needs an answer first", indexOf(f.block.questions, question)+1, question.Text))
			return m.refresh(), nil
		}
	}

	response := map[string]any{"answers": f.answers}
	arcID, interruptID := f.block.arc, f.block.interrupt
	triggerSeq, err := m.session.answerInterrupt(context.Background(), arcID, interruptID, response)
	if err != nil {
		m.transcript.fail(err)
	} else {
		// The write is durable, so the block is answered now — not when the
		// response event comes back around the program.
		m.transcript.markAnswered(interruptID, response)
		resumeArc(m, arcID, triggerSeq)
	}
	m.leaveAnswering()
	return m.refresh(), nil
}

func indexOf(questions []question, q question) int {
	for i, candidate := range questions {
		if candidate.ID == q.ID {
			return i
		}
	}
	return 0
}

// emptyAnswer reports whether an answer counts as not given.
func emptyAnswer(answer any) bool {
	switch v := answer.(type) {
	case nil:
		return true
	case string:
		return v == ""
	case []any:
		return len(v) == 0
	}
	return false
}

func newModel(s *session, sender *sender, settings agent.AgentSettings, settingsPath string, existing *transcript) model {
	input := textarea.New()
	input.Placeholder = "Ask about the cluster…"
	input.ShowLineNumbers = false
	input.CharLimit = 4000
	input.SetHeight(2)
	input.Focus()

	spin := spinner.New(spinner.WithSpinner(spinner.MiniDot), spinner.WithStyle(busyStyle))

	return model{
		session:      s,
		transcript:   existing,
		sender:       sender,
		input:        input,
		spinner:      spin,
		settings:     settings,
		settingsPath: settingsPath,
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.input.Focus(), m.spinner.Tick)
}

func (m model) running() bool { return m.cancel != nil }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		return m.resize(msg.Width, msg.Height), nil

	case tea.KeyPressMsg:
		return m.key(msg)

	case tokenMsg:
		m.transcript.token(string(msg))
		return m.refresh(), nil

	case eventMsg:
		m.transcript.event(msg.event)
		return m.refresh(), nil

	case taskUnfiledMsg:
		m.transcript.taskSaid(msg.ref.ID, "",
			fmt.Sprintf("could not be filed: %v", msg.err), toolFailed)
		return m.refresh(), nil

	case arcDoneMsg:
		switch {
		case msg.err == nil:
		case errors.Is(msg.err, context.Canceled), errors.Is(msg.err, common.ErrCancelled):
			m.transcript.note("stopped")
		default:
			m.transcript.fail(msg.err)
		}
		return m.refresh(), nil

	case turnDoneMsg:
		m.cancel = nil
		m.arc = ""
		m.startAnswering()
		switch {
		case msg.err == nil:
		case errors.Is(msg.err, context.Canceled), errors.Is(msg.err, common.ErrCancelled):
			// The cancel is already written down, so the screen only says the
			// run is over. A run the guard refused a write to arrives as
			// ErrCancelled and reads the same way.
			m.transcript.note("stopped")
		default:
			m.transcript.fail(msg.err)
		}
		return m.refresh(), nil
	}

	var commands []tea.Cmd
	var command tea.Cmd

	m.spinner, command = m.spinner.Update(msg)
	commands = append(commands, command)
	m.input, command = m.input.Update(msg)
	commands = append(commands, command)
	m.viewport, command = m.viewport.Update(msg)
	commands = append(commands, command)

	return m, tea.Batch(commands...)
}

func (m model) key(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if m.answering != nil {
		return m.answerKey(msg)
	}
	switch msg.String() {
	case "ctrl+c":
		// While a run is in flight, the interrupt belongs to the run. It goes
		// the same way as /cancel, since it is the same decision.
		if m.running() {
			return cancelTurn(m, "")
		}
		return m, tea.Quit

	case "esc":
		if m.running() {
			return m, nil
		}
		return m, tea.Quit

	case "enter":
		message := strings.TrimSpace(m.input.Value())
		if message == "" {
			// An empty enter with a questions form pending re-enters answer
			// mode, for after esc left it.
			if !m.running() && m.transcript.pendingQuestions() != nil {
				m.startAnswering()
				return m.refresh(), nil
			}
			return m, nil
		}
		if isCommand(message) {
			// A command that is not written for a run in flight is left in the
			// input rather than dropped, so it can be sent once the turn is
			// over instead of typed again.
			if m.running() && !runsDuringTurn(message) {
				return m, nil
			}
			m.input.Reset()
			return runCommand(m, message)
		}
		if m.running() {
			return m, nil
		}
		m.input.Reset()
		// A plain message while an arc-wide interrupt pends is the answer to
		// it, not a new arc: the interrupted arc is what the words are for,
		// and a new arc would strand it.
		if pending := m.transcript.pendingInterrupt(); pending != nil {
			triggerSeq, err := m.session.answerInterrupt(context.Background(), pending.arc, pending.interrupt, message)
			if err != nil {
				m.transcript.fail(err)
				return m.refresh(), nil
			}
			m.transcript.markAnswered(pending.interrupt, message)
			resumeArc(m, pending.arc, triggerSeq)
			return m.refresh(), nil
		}
		return m.start(message)
	}

	var command tea.Cmd
	m.input, command = m.input.Update(msg)
	return m, command
}

// start runs one turn on its own goroutine. Everything it produces comes back
// as a message, so the update loop stays the only thing touching the model.
func (m model) start(message string) (tea.Model, tea.Cmd) {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.arc = m.session.newArcID()

	session := m.session
	sender := m.sender
	arc := m.arc
	go func() {
		defer cancel()
		sender.send(turnDoneMsg{err: session.turn(ctx, arc, message)})
	}()

	// Nothing is put on the screen here. The message is written to the log
	// before anything else happens, and the persistor publishes it straight
	// back, so showing it as well would show it twice.
	return m.refresh(), m.spinner.Tick
}

func (m model) resize(width, height int) model {
	m.width, m.height = width, height

	inputHeight := lipgloss.Height(m.inputView())
	chrome := lipgloss.Height(m.headerView(width)) + inputHeight + 3

	if !m.ready {
		m.viewport = viewport.New(viewport.WithWidth(width), viewport.WithHeight(max(height-chrome, 3)))
		m.ready = true
	} else {
		m.viewport.SetWidth(width)
		m.viewport.SetHeight(max(height-chrome, 3))
	}
	m.input.SetWidth(width - 4)
	return m.refresh()
}

// refresh redraws the transcript and follows it, so a reply being typed stays
// in view.
func (m model) refresh() model {
	if !m.ready {
		return m
	}
	content := renderTranscript(m.transcript, m.viewport.Width())

	// A short conversation is pushed to the bottom, so the newest line is
	// always in the same place rather than drifting down the screen as the
	// session grows.
	if gap := m.viewport.Height() - lipgloss.Height(content); gap > 0 {
		content = strings.Repeat("\n", gap) + content
	}

	m.viewport.SetContent(content)
	m.viewport.GotoBottom()
	return m
}

func (m model) View() tea.View {
	if !m.ready {
		view := tea.NewView("starting…")
		view.AltScreen = true
		return view
	}

	view := tea.NewView(lipgloss.JoinVertical(lipgloss.Left,
		m.headerView(m.width),
		"",
		m.viewport.View(),
		"",
		m.inputView(),
		m.helpView(),
	))
	view.AltScreen = true
	view.WindowTitle = "kube-agent · devchat"
	return view
}

func (m model) inputView() string {
	return inputStyle.Width(max(m.width, 22)).Render(m.input.View())
}

func (m model) helpView() string {
	if m.answering != nil {
		return helpStyle.Render("answering interrupt " + m.answering.block.interrupt +
			" · ↑/↓ move · 1-9 choose · enter submit · esc leave")
	}
	if m.running() {
		return helpStyle.Render(fmt.Sprintf("%s working · /cancel [why] or ctrl+c stops it", m.spinner.View()))
	}
	if m.transcript.pendingQuestions() != nil {
		return helpStyle.Render(commandHelp() + " · enter answers the pending questions")
	}
	if m.transcript.pendingInterrupt() != nil {
		return helpStyle.Render(commandHelp() + " · enter sends your message as the interrupt's answer")
	}
	return helpStyle.Render(commandHelp() + " · enter send")
}

// headerView draws the bar, naming whatever tasks are still running.
func (m model) headerView(width int) string {
	var running []string
	if m.transcript != nil {
		running = m.transcript.runningTasks()
	}
	return renderHeader(m.settings.Model.Name, running, width)
}
