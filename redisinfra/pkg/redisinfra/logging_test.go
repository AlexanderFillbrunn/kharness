package redisinfra

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
)

func bufferLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// An enqueue logs the item's metadata at Info, tagged with the component and
// the consumer.
func TestEnqueueLogsTheItem(t *testing.T) {
	client, _ := newRedis(t)
	var buf bytes.Buffer
	q, err := NewRedisWorkQueue(client, WithQueueLogger(bufferLogger(&buf)))
	if err != nil {
		t.Fatalf("NewRedisWorkQueue: %v", err)
	}
	ref := common.ArcRef{SessionID: "s-1", ArcID: "arc-1"}
	if err := q.Enqueue(context.Background(), ref, "agent-1", 7); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"component=queue", "work item enqueued", "session=s-1", "arc=arc-1", "agent=agent-1", "trigger_seq=7"} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q\n%s", want, out)
		}
	}
}

// A lease logs its acquisition, a held-out attempt and its release at Debug.
func TestLeaseLogsItsLifecycle(t *testing.T) {
	client, _ := newRedis(t)
	var buf bytes.Buffer
	lease := NewRedisLease(client, WithLeaseLogger(bufferLogger(&buf)))
	ref := common.ArcRef{SessionID: "s-1", ArcID: "arc-1"}

	release, ok, err := lease.Acquire(context.Background(), ref)
	if err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}
	if _, ok, err := lease.Acquire(context.Background(), ref); err != nil || ok {
		t.Fatalf("second Acquire: ok=%v err=%v, want the lease held", ok, err)
	}
	release()

	out := buf.String()
	for _, want := range []string{"component=lease", "lease acquired", "lease held by another holder", "lease released"} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q\n%s", want, out)
		}
	}
}

// A cancel announcement logs at Info, tagged with the arc it names.
func TestNotifyCancelLogsTheAnnouncement(t *testing.T) {
	client, _ := newRedis(t)
	var buf bytes.Buffer
	cancels := NewRedisCancels(client, WithCancelsLogger(bufferLogger(&buf)))
	ref := common.ArcRef{SessionID: "s-1", ArcID: "arc-1"}
	if err := cancels.NotifyCancel(context.Background(), ref); err != nil {
		t.Fatalf("NotifyCancel: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"component=cancels", "cancel announced", "session=s-1", "arc=arc-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q\n%s", want, out)
		}
	}
}
