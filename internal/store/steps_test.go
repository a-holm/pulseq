package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The cases here are the ones the transition tests do not carry: the typed
// refusals of the step lifecycle, and the guarantees that survive outside the
// transaction that wrote them.

// aRunningStep seeds one manual run of singleStepSpec, claims it and opens
// its one step, which is the state every verdict starts from.
func aRunningStep(t *testing.T, s *store.Store) string {
	t.Helper()

	ctx := context.Background()
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build"); err != nil {
		t.Fatalf("start: %v", err)
	}
	return runID
}

// A step already running cannot start again: double starting would lose the
// first attempt's started_at and inflate the attempt counter.
func TestStartStepRefusesAStepThatIsNotPending(t *testing.T) {
	s, _ := coreStore(t)
	runID := aRunningStep(t, s)

	before := mustGetRun(t, context.Background(), s, runID).Steps[0]
	err := s.StartStep(context.Background(), runID, "build")
	if !errors.Is(err, store.ErrStepNotPending) {
		t.Fatalf("second start returned %v, want ErrStepNotPending", err)
	}
	after := mustGetRun(t, context.Background(), s, runID).Steps[0]
	if after.Attempt != before.Attempt || after.State != before.State {
		t.Errorf("the refused start moved the step: %+v -> %+v", before, after)
	}
}

// The acceptance criterion that sends people here: the error tail outlives
// the log file. Nothing in this read touches the filesystem.
func TestErrorTailOutlivesTheLogFile(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aRunningStep(t, s)

	_, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedTimeout,
		LogMeta: store.LogMeta{
			RelPath:   "2026-09-17/run/build.1.ndjson",
			Bytes:     99,
			ErrorTail: "last words of the job",
		},
	})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	tail := mustStep(t, ctx, s, runID, "build").ErrorTail
	if tail != "last words of the job" {
		t.Fatalf("error_tail = %q, want the mirrored text", tail)
	}
}

// The verdict stamps how long the attempt took, computed by the database from
// started_at, so it cannot disagree with the two timestamps.
func TestRecordStepOutcomeStoresTheDuration(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRunningStep(t, s)
	clk.Advance(1500 * time.Millisecond)

	if _, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		ExitCode:   ptr(0),
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	step := mustStep(t, ctx, s, runID, "build")
	if step.DurationMS != 1500 {
		t.Errorf("duration_ms = %d, want 1500", step.DurationMS)
	}
	// An attempt without a sink reports no log at all rather than zeroes
	// pretending to be a file.
	if step.LogPath != "" || step.LogBytes != 0 || step.LogTruncated {
		t.Errorf("log metadata = %q/%d/%v, want empty for an attempt without a sink",
			step.LogPath, step.LogBytes, step.LogTruncated)
	}
}

func TestOpenReadOnlyServesReadsAndRefusesWrites(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)

	ro, err := store.OpenReadOnly(ctx, s.Path(), store.Options{})
	if err != nil {
		t.Fatalf("open read only: %v", err)
	}
	defer func() { _ = ro.Close() }()

	detail, err := ro.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("read through the read only store: %v", err)
	}
	if len(detail.Steps) != 1 {
		t.Fatalf("read only store saw %d steps", len(detail.Steps))
	}

	err = ro.AppendRunEvent(ctx, store.RunEvent{RunID: runID, At: time.Now(), Kind: "run.queued"})
	if err == nil {
		t.Fatal("the read only store accepted a write")
	}
	if !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("write error is %v, want ErrReadOnly", err)
	}
}
