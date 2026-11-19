package main

import (
	"context"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
)

// A line starting with a slash is an instruction to the harness rather than
// something to say to the model. Most commands only run between turns, and the
// few that do not say so.

// slashCommand is one thing that can be typed instead of a message.
type slashCommand struct {
	name string

	// arg names what the command takes, for the help line, and is empty for a
	// command that takes nothing.
	arg     string
	summary string

	// duringTurn lets a command be typed while the agent is working. It is off
	// by default, since most commands would corrupt a run in progress.
	duringTurn bool

	// run is given whatever followed the command name, trimmed.
	run func(model, string) (model, tea.Cmd)
}

// commands are shown in the order they are listed.
var commands = []slashCommand{
	{
		name:       "/cancel",
		arg:        "[task-id | why]",
		summary:    "stop the run, or a task by id",
		duringTurn: true,
		run:        cancelTurn,
	},
	{
		name:    "/tasks",
		summary: "list tasks",
		run:     listTasks,
	},
	{
		name:    "/model",
		arg:     "<name>",
		summary: "switch model",
		run:     switchModel,
	},
	{
		name:    "/clear",
		summary: "new session",
		run:     clearSession,
	},
	{
		name:    "/quit",
		summary: "leave",
		run:     func(m model, _ string) (model, tea.Cmd) { return m, tea.Quit },
	},
}

// isCommand reports whether a line is addressed to the harness.
func isCommand(line string) bool {
	return strings.HasPrefix(line, "/")
}

// runsDuringTurn reports whether a typed line is one of the commands that may
// be sent while the agent is working. An unknown command is not, so that a
// mistyped one is answered between turns rather than mid-run.
func runsDuringTurn(line string) bool {
	name, _, _ := strings.Cut(line, " ")
	for _, command := range commands {
		if command.name == name {
			return command.duringTurn
		}
	}
	return false
}

// runCommand dispatches a typed line. An unknown command is answered rather
// than sent to the model, since a mistyped command is not a question.
func runCommand(m model, line string) (model, tea.Cmd) {
	name, argument, _ := strings.Cut(line, " ")
	for _, command := range commands {
		if command.name == name {
			return command.run(m, strings.TrimSpace(argument))
		}
	}

	m.transcript.note(fmt.Sprintf("no command %s. there is %s", name, strings.Join(commandNames(), ", ")))
	return m.refresh(), nil
}

// cancelTurn withdraws the run in progress, or a task when the argument names
// one in the fold's task table. The record goes down before the run is
// stopped, so the log never shows an arc being worked on after the run has
// ended.
//
// Cancelling an arc also stops every task that arc started: that is a loop
// over the task table, not a mechanism of its own. A cancel naming a task ID
// writes the CANCEL event correlated to that task ID and reaches the runner
// through the controller.
//
// Nothing is printed here: stopping the run brings the turn back through
// turnDoneMsg, which says so once.
func cancelTurn(m model, argument string) (model, tea.Cmd) {
	ctx := context.Background()

	// A task named by ID is cancelled on its own axis, whether or not a run is
	// in flight.
	if argument != "" {
		if h, err := m.session.fullHistory(ctx); err == nil {
			if _, ok := h.Tasks[argument]; ok {
				if err := m.session.cancelTask(ctx, argument, ""); err != nil {
					m.transcript.fail(err)
					return m.refresh(), nil
				}
				m.transcript.note(fmt.Sprintf("task %s cancelled", argument))
				return m.refresh(), nil
			}
		}
	}

	if !m.running() {
		m.transcript.note("nothing is running")
		return m.refresh(), nil
	}

	arc := m.arc
	reason := argument
	if err := m.session.cancel(ctx, arc, reason); err != nil {
		// The run is left alone. A cancel that was not recorded would stop the
		// run and leave the log saying the arc is still being worked on, and a
		// stuck run is easier to explain than a log that disagrees with it.
		m.transcript.fail(err)
		return m.refresh(), nil
	}
	m.cancel()

	// Every task this arc started is stopped as well, each with its own
	// task-addressed cancel.
	if err := m.session.cancelArcTasks(ctx, arc, reason); err != nil {
		m.transcript.fail(err)
	}

	return m.refresh(), nil
}

// listTasks shows the fold's task table: every task in the session with its
// status, which is what the log says rather than what the screen remembers.
func listTasks(m model, _ string) (model, tea.Cmd) {
	h, err := m.session.fullHistory(context.Background())
	if err != nil {
		m.transcript.fail(err)
		return m.refresh(), nil
	}
	tasks := h.TasksInOrder()
	if len(tasks) == 0 {
		m.transcript.note("no tasks")
		return m.refresh(), nil
	}
	for _, task := range tasks {
		m.transcript.note(fmt.Sprintf("task %s (id %s) %s, started in arc %s",
			task.TaskName, task.ID, task.Status, task.CorrelationID))
	}
	return m.refresh(), nil
}

// switchModel points the rest of the session at another model, and with no
// name says which one is being used.
//
// It rewrites the settings file rather than any client, since the file is the
// definition the resolver reads: the change lands on the next arc, and a run
// already asking the old model is left to finish on it. Nothing is checked,
// since whether an endpoint serves a name is only answered by asking it. A
// wrong name shows up as a failed turn. The conversation carries over, and
// every reply records the model that produced it.
func switchModel(m model, name string) (model, tea.Cmd) {
	if name == "" {
		m.transcript.note("using " + m.settings.Model.Name)
		return m.refresh(), nil
	}
	if name == m.settings.Model.Name {
		m.transcript.note("already using " + name)
		return m.refresh(), nil
	}

	was := m.settings.Model.Name
	m.settings.Model.Name = name
	if err := writeSettings(m.settingsPath, m.settings); err != nil {
		m.settings.Model.Name = was
		m.transcript.note(fmt.Sprintf("could not switch: %v", err))
		return m.refresh(), nil
	}
	m.transcript.note(fmt.Sprintf("switched from %s to %s, from the next turn", was, name))
	return m.refresh(), nil
}

// clearSession starts the conversation over. The log is moved aside rather
// than the screen cleared, since the next turn replays the log.
func clearSession(m model, _ string) (model, tea.Cmd) {
	archived, err := m.session.store.Rotate()
	if err != nil {
		m.transcript.fail(err)
		return m.refresh(), nil
	}

	m.transcript = newTranscript()
	if archived == "" {
		m.transcript.note("nothing to clear")
	} else {
		m.transcript.note("new session. the previous one is at " + archived)
	}
	return m.refresh(), nil
}

func commandNames() []string {
	names := make([]string, 0, len(commands))
	for _, command := range commands {
		names = append(names, command.name)
	}
	return names
}

// commandHelp lists the commands for the line under the input.
func commandHelp() string {
	described := make([]string, 0, len(commands))
	for _, command := range commands {
		label := command.name
		if command.arg != "" {
			label += " " + command.arg
		}
		described = append(described, fmt.Sprintf("%s %s", label, command.summary))
	}
	return strings.Join(described, " · ")
}
