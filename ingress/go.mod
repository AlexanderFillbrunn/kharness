module fillbrunn.ai/kube-agent/ingress

go 1.27.1

require (
	// The external test package wires a real agent worker to serve enqueued arcs,
	// so the agent module is required for tests only. Production code in this
	// module must never import it.
	fillbrunn.ai/kube-agent/agent v0.0.0
	fillbrunn.ai/kube-agent/proto v0.0.0
)

require (
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/modelcontextprotocol/go-sdk v1.8.0 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/oauth2 v0.35.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)

replace (
	fillbrunn.ai/kube-agent/agent => ../agent
	fillbrunn.ai/kube-agent/proto => ../proto
)
