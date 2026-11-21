package events

import (
	"encoding/json"
	"testing"

	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// The three compaction records round-trip through the JSON codec as their
// concrete payload types, so a log line written by one process decodes in
// another.
func TestCompactionPayloadsRoundTrip(t *testing.T) {
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	payloads := []EventPayload{
		CompactionRequestedPayload{Watermark: 120, Actor: &actor},
		ArcSuspendedPayload{CausationID: "req-1"},
		CompactedPayload{
			ThroughSeq: 100,
			Summary:    "the session discussed pods",
			Method:     "llm",
			Modes: ProjectionModes{
				ForeignArc: "keep", Pending: "stand_in", ToolEvents: "elide",
				Error: "elide", Interrupt: "elide", QuoteLimit: 32,
			},
			ModelID: "gpt-5",
			Usage:   &TokenUsage{Input: 40000, CachedInput: 32000, Output: 800},
		},
	}

	codec := JSONCodec{}
	for _, payload := range payloads {
		encoded, err := codec.Encode(payload)
		if err != nil {
			t.Fatalf("encode %T: %v", payload, err)
		}
		decoded, err := codec.Decode(payload.EventType(), encoded)
		if err != nil {
			t.Fatalf("decode %s: %v", payload.EventType(), err)
		}
		again, err := json.Marshal(decoded)
		if err != nil {
			t.Fatalf("re-marshal %T: %v", decoded, err)
		}
		if string(again) != string(encoded) {
			t.Errorf("%s round trip changed the payload:\n got %s\nwant %s", payload.EventType(), again, encoded)
		}
	}
}

// An event decodes its payload by its type, so a COMPACTED line reads back as
// a CompactedPayload with the watermark in the payload, not the position.
func TestEventUnmarshalDecodesCompactedByType(t *testing.T) {
	line := `{
		"session_id": "s1", "agent_id": "a1", "id": "ev-9", "seq": 141,
		"timestamp": "2026-11-19T09:00:00Z", "type": "COMPACTED",
		"payload": {"through_seq": 100, "summary": "...", "method": "prune"}
	}`
	var ev Event
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ev.Typ != CompactedEventType {
		t.Fatalf("type = %s, want COMPACTED", ev.Typ)
	}
	payload, ok := ev.Payload.(CompactedPayload)
	if !ok {
		t.Fatalf("payload is %T, want CompactedPayload", ev.Payload)
	}
	if payload.ThroughSeq != 100 {
		t.Errorf("ThroughSeq = %d, want 100 (the watermark, not the position 141)", payload.ThroughSeq)
	}
	if ev.CorrelationID != nil {
		t.Errorf("a COMPACTED carries no correlation ID, got %s", *ev.CorrelationID)
	}
}
