package history

import (
	"encoding/json"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// This file holds the fold's snapshot encoding: a History serialized so a
// reader can resume from it instead of replaying the whole log. The encoding
// lives here, next to the type, so every store encodes and decodes alike.
//
// A snapshot is a cache, never a source of truth: the log is append-only and
// always holds everything, and a reader that ignores snapshots replays the
// session whole. What a snapshot must guarantee is that resuming from it
// yields the same history the full replay yields — same arcs, tasks and
// compaction state — so the tail events alone can be folded on top.
//
// One fidelity note: the fold keeps some payload content as `any` (tool call
// args and results, task event payloads, interrupt context), and everything
// is rendered through encoding/json. A JSON round trip turns typed values
// into maps, so rendered key order can change while the content does not.
// Tool arguments arrive from the model as JSON already, so in practice only
// typed tool results are affected.

// EncodeFold serializes a folded history for storage as a snapshot. The
// history's LastSeq is the snapshot's sequence number: the fold covers every
// event with Seq <= LastSeq.
func EncodeFold(h *History) ([]byte, error) {
	if h == nil {
		return nil, fmt.Errorf("cannot encode a nil history")
	}
	data, err := json.Marshal(h)
	if err != nil {
		return nil, fmt.Errorf("encode fold through seq %d: %w", h.LastSeq, err)
	}
	return data, nil
}

// DecodeFold deserializes a fold written by EncodeFold. The result carries
// LastSeq, so Resume accepts exactly the events above it.
func DecodeFold(data []byte) (*History, error) {
	var h History
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, fmt.Errorf("decode fold: %w", err)
	}
	if h.Tasks == nil {
		h.Tasks = map[string]*AsyncTask{}
	}
	// Folds written before the invoke stamped the trail carry no field and
	// read zero; normalize to the whole trail, the fold's own default.
	for _, task := range h.Tasks {
		if task.InResultTrail == 0 {
			task.InResultTrail = -1
		}
	}
	if h.LastSeq < -1 {
		return nil, fmt.Errorf("decoded fold has invalid last seq %d", h.LastSeq)
	}
	return &h, nil
}

// Resume folds events on top of a decoded fold, returning the same history a
// full replay would have produced. It takes the same slice of event pointers
// Replay takes, so a caller folds a read either way without converting. The
// events must be the log's entries above the fold's LastSeq, in increasing
// sequence order; Update refuses any that are not, so a snapshot and a tail
// that disagree stop the resume rather than fold a wrong history.
//
// Each task's InResultTrail is log content — stamped on its invoke event —
// so it rides along in the encoding and resumes with the rest of the task
// table; folds written before the stamp existed are normalized to -1 at
// decode.
func Resume(fold *History, evs []*events.Event) (*History, error) {
	if fold == nil {
		return nil, fmt.Errorf("cannot resume from a nil fold")
	}
	state := FoldStateFromHistory(fold)
	for _, ev := range evs {
		if err := fold.Update(ev, state); err != nil {
			return nil, fmt.Errorf("fold %s event %q at seq %d onto seq %d: %w", ev.Typ, ev.ID, ev.Seq, fold.LastSeq, err)
		}
	}
	return fold, nil
}
