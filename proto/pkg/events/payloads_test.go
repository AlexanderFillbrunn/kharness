package events

import (
	"strings"
	"testing"
)

// The interrupt payload carries the call it holds in causation_id, following
// the other payloads that act on a prior record. An arc-wide interrupt
// carries no causation at all, and encodes without the field rather than as
// an empty string naming nothing.
func TestInterruptPayloadCausationRoundTrip(t *testing.T) {
	codec := JSONCodec{}

	encoded, err := codec.Encode(InterruptPayload{CausationID: "call-1", Reason: "restart needs approval"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.Contains(string(encoded), `"causation_id":"call-1"`) {
		t.Errorf("encoded = %s, want causation_id", encoded)
	}
	decoded, err := codec.Decode(InterruptEventType, encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := decoded.(InterruptPayload); got.CausationID != "call-1" || got.Reason != "restart needs approval" {
		t.Errorf("decoded = %#v", got)
	}

	arcWide, err := codec.Encode(InterruptPayload{Reason: "which cluster?"})
	if err != nil {
		t.Fatalf("encode arc-wide: %v", err)
	}
	if strings.Contains(string(arcWide), "causation_id") {
		t.Errorf("encoded arc-wide = %s, want causation_id omitted", arcWide)
	}
	decodedArcWide, err := codec.Decode(InterruptEventType, arcWide)
	if err != nil {
		t.Fatalf("decode arc-wide: %v", err)
	}
	if got := decodedArcWide.(InterruptPayload); got.CausationID != "" || got.Reason != "which cluster?" {
		t.Errorf("decoded arc-wide = %#v, want empty causation", got)
	}
}

// The task meta payload round-trips with its guaranteed external_ref field.
func TestAsyncTaskMetaPayloadRoundTrip(t *testing.T) {
	codec := JSONCodec{}

	encoded, err := codec.Encode(AsyncTaskMetaPayload{
		CausationID: "t1", ExternalRef: "srv-9", Launched: true,
		Launch: map[string]any{"status": "working"}, LLMVisibility: LLMVisibilityNone,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.Contains(string(encoded), `"external_ref":"srv-9"`) {
		t.Errorf("encoded = %s, want external_ref", encoded)
	}
	decoded, err := codec.Decode(AsyncTaskMetaEventType, encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := decoded.(AsyncTaskMetaPayload)
	if got.CausationID != "t1" || got.ExternalRef != "srv-9" || !got.Launched || got.LLMVisibility != LLMVisibilityNone {
		t.Errorf("decoded = %#v", got)
	}
	if m, ok := got.Launch.(map[string]any); !ok || m["status"] != "working" {
		t.Errorf("launch = %#v, want the runner's raw response kept", got.Launch)
	}
}
