package ingress_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"fillbrunn.ai/kube-agent/ingress/pkg/ingress"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// A report logs its write and the arc it opened at Info, tagged with the
// component, the session and the task — and never carries the report's
// content.
func TestAReportLogsItsWriteAndEnqueue(t *testing.T) {
	store := runningTaskStore(t)
	enqueuer := &recordingEnqueuer{}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reports, err := ingress.NewTaskReports(store, store, taskRegistry(t), enqueuer, ingress.WithReportsLogger(logger))
	if err != nil {
		t.Fatalf("NewTaskReports: %v", err)
	}

	err = reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{
		Kind:    ingress.TaskReportCompleted,
		Outcome: events.AsyncTaskOutcomeSuccess,
		Result:  "SECRET_RESULT_TEXT",
	})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}

	out := buf.String()
	for _, want := range []string{"component=ingress", "session=" + testSessionID, "task=task-1", "report persisted", "arc enqueued"} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "SECRET_RESULT_TEXT") {
		t.Errorf("log contains the report's content; logs carry metadata only\n%s", out)
	}
}

// A refused report is a warning, not a silence: the caller learns the report
// was not written.
func TestARefusedReportLogsAWarning(t *testing.T) {
	store := runningTaskStore(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reports, err := ingress.NewTaskReports(store, store, taskRegistry(t), &recordingEnqueuer{}, ingress.WithReportsLogger(logger))
	if err != nil {
		t.Fatalf("NewTaskReports: %v", err)
	}

	err = reports.Report(context.Background(), common.TaskRef{SessionID: testSessionID, ID: "no-such-task"}, testAgentID, ingress.TaskReport{
		Kind:    ingress.TaskReportNotification,
		Class:   "detail",
		Message: "SECRET_MESSAGE_TEXT",
	})
	if err == nil {
		t.Fatal("Report succeeded, want the refusal")
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "report refused") {
		t.Errorf("log is missing the refusal warning\n%s", out)
	}
	if strings.Contains(out, "SECRET_MESSAGE_TEXT") {
		t.Errorf("log contains the report's message; logs carry metadata only\n%s", out)
	}
}
