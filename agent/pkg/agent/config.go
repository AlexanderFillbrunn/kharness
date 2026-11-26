package agent

import (
	"errors"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// AgentConfig is what an agent is built with, and holds nothing that differs
// from one run to the next. Following a run is not here: notification hangs
// off the persistor, so an agent has no idea whether anybody is watching. See
// common.EventSink and docs/EVENT-SINK.md.
type AgentConfig struct {
	SystemPrompt string

	// Compaction says whether the compaction checks run at all: no budget is
	// checked and no request is written when it is false. Which compactor
	// runs is not here — it is a property of the compaction worker's plan,
	// resolved from the settings, not of the agent.
	Compaction bool

	// LowerBudget is the token count that triggers a compaction request,
	// checked after every round. It sits well below what the provider accepts.
	LowerBudget int

	// HigherBudget is the token count that suspends runs, checked at admission
	// and between rounds. It sits just below the context window of the run's
	// model, leaving a reserve for one more tool result. Zero means the
	// default resolved at build: ContextWindow minus ContextWindowReserve.
	HigherBudget int

	// MaxToolResultChars caps what one tool result may contribute to a
	// transcript, in characters. The log keeps the whole result. Negative
	// disables the cap.
	MaxToolResultChars int

	// ArcThresholdTokens is the size an arc's log may reach before the bound
	// rule draws the line at its live boundary rather than its opening, and
	// before a spanning arc renders as a skeleton. Negative disables both.
	ArcThresholdTokens int

	// ContextWindow is the context window of the run's model, resolved from
	// the catalog at build.
	ContextWindow int

	// MaxParallelToolCalls caps how many read-only calls one wave of a tool
	// step runs at once. Zero means DefaultMaxParallelToolCalls, negative
	// means no cap, and one is fully sequential. See docs/PARALLEL-TOOLS.md.
	MaxParallelToolCalls int
}

// DefaultMaxParallelToolCalls is how many read-only calls run at once when
// the config says nothing.
const DefaultMaxParallelToolCalls = 4

// parallelCap resolves MaxParallelToolCalls: the configured value where it is
// positive, the default for zero, and -1 (no cap) for negative.
func (c *AgentConfig) parallelCap() int {
	switch {
	case c.MaxParallelToolCalls < 0:
		return -1
	case c.MaxParallelToolCalls == 0:
		return DefaultMaxParallelToolCalls
	default:
		return c.MaxParallelToolCalls
	}
}

// AgentOption configures parts of an agent that not every agent has.
type AgentOption func(*Agent) error

// WithTasks gives an agent the set of task kinds its definition names: the
// tool surface is built from it. Starting and reaching tasks is not the
// agent's business — the worker hands each run a controller via
// RunOptions.Controller (WithTaskController). A nil set is refused.
func WithTasks(tasks common.TaskSet) AgentOption {
	return func(a *Agent) error {
		if tasks == nil {
			return errors.New("tasks need a set with metadata to offer")
		}
		a.tasks = tasks
		return nil
	}
}

// WithSkills gives an agent the skills its definition names. Both model-facing
// halves are derived here, once: the catalog appended to the system prompt,
// and the load_skill tool, which NewAgent folds into the tool set. A nil or
// empty set means no skills and changes nothing.
func WithSkills(skills SkillSet) AgentOption {
	return func(a *Agent) error {
		if skills == nil || len(skills.All()) == 0 {
			return nil
		}
		a.skills = skills
		a.systemPrompt = skillsCatalog(a.config.SystemPrompt, skills)
		return nil
	}
}

// NewAgent builds an agent from the parts it runs on. A nil config or tool
// provider means no system prompt and an empty registry. A model is required.
func NewAgent(config *AgentConfig, tools ToolProvider, llm LLM, options ...AgentOption) (*Agent, error) {
	if llm == nil {
		return nil, errors.New("an agent needs a model")
	}
	if config == nil {
		config = &AgentConfig{}
	}
	if tools == nil {
		tools = NewToolRegistry()
	}

	agent := &Agent{config: config, tools: tools, llm: llm, systemPrompt: config.SystemPrompt}
	for _, option := range options {
		if err := option(agent); err != nil {
			return nil, err
		}
	}

	// The skills' load_skill tool is one more tool by name, so a tool
	// registered under that name collides with it the way two tools with one
	// name do.
	if agent.skills != nil {
		if _, taken := tools.Get(LoadSkillToolName); taken {
			return nil, fmt.Errorf("%s is registered as a tool and is the tool skills are loaded with", LoadSkillToolName)
		}
		agent.tools = combineProviders(tools, loadSkillProvider{skills: agent.skills})
	}

	// The model picks by name and the agent routes by name, so one name
	// registered twice is a call that reaches whichever is checked first.
	if agent.tasks != nil {
		for _, meta := range agent.tasks.All() {
			if _, taken := agent.tools.Get(meta.Name); taken {
				return nil, fmt.Errorf("%s is registered as both a tool and a task", meta.Name)
			}
		}
		for _, name := range []string{events.CancelTaskToolName, events.ProvideTaskInputToolName} {
			if _, taken := agent.tools.Get(name); taken {
				return nil, fmt.Errorf("%s is registered as a tool and is one of the tools tasks are addressed with", name)
			}
		}
	}
	// A configured interrupt is a tool by name, so it collides with a
	// registered tool and with a task kind the same way.
	for _, spec := range agent.interruptSpecs {
		if _, taken := agent.tools.Get(spec.Name); taken {
			return nil, fmt.Errorf("%s is registered as a tool and is configured as an interrupt", spec.Name)
		}
		if agent.tasks != nil {
			if _, taken := agent.tasks.Get(spec.Name); taken {
				return nil, fmt.Errorf("%s is registered as a task and is configured as an interrupt", spec.Name)
			}
		}
	}
	return agent, nil
}
