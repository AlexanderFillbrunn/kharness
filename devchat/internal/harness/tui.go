package harness

import (
	"context"
	"errors"
	"fmt"
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
		ref agent.TaskRef
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
type sender struct {
	mu      sync.RWMutex
	program *tea.Program
}

func (s *sender) attach(program *tea.Program) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.program = program
}

func (s *sender) send(msg tea.Msg) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.program != nil {
		s.program.Send(msg)
	}
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

	width  int
	height int
	ready  bool
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
		case errors.Is(msg.err, context.Canceled), errors.Is(msg.err, agent.ErrCancelled):
			m.transcript.note("stopped")
		default:
			m.transcript.fail(msg.err)
		}
		return m.refresh(), nil

	case turnDoneMsg:
		m.cancel = nil
		m.arc = ""
		switch {
		case msg.err == nil:
		case errors.Is(msg.err, context.Canceled), errors.Is(msg.err, agent.ErrCancelled):
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
	if m.running() {
		return helpStyle.Render(fmt.Sprintf("%s working · /cancel [why] or ctrl+c stops it", m.spinner.View()))
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
