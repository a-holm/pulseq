package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/reason"
)

// internalStore is a migrated store for the in-package tests, which exist to
// reach the writer pool directly. The external tests in transitions_test.go
// cover the behaviour; this one covers the guarantee that needs the handle.
func internalStore(t *testing.T) *Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func aJobInternal(t *testing.T, s *Store, name string) JobVersion {
	t.Helper()

	version, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:     name,
		Description: "the " + name + " job",
		SourcePath:  "jobs/" + name + ".yaml",
		SpecHash:    "sha256:" + name,
		SpecJSON:    `{"steps":[{"name":"build"}]}`,
	})
	if err != nil {
		t.Fatalf("record job %s: %v", name, err)
	}
	return version
}

func aRunningStepInternal(t *testing.T, s *Store) string {
	t.Helper()

	ctx := context.Background()
	version := aJobInternal(t, s, "nightly")
	run, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}
	if _, err := s.ClaimRun(ctx, run.ID, LeaseInput{Owner: "exec-1"}); err != nil {
		t.Fatalf("claim the run: %v", err)
	}
	if err := s.StartStep(ctx, run.ID, "extract"); err != nil {
		t.Fatalf("start the step: %v", err)
	}
	return run.ID
}

// TestVerdictRollsBackWholesale is the atomicity criterion for the log
// metadata: error_tail, log_path, log_bytes and log_truncated are written in
// the SAME transaction as the step's terminal transition. A failure between
// the two would leave a step whose verdict and its evidence disagree, and
// that drift is what this rule exists to prevent.
//
// The failure is injected with a trigger that aborts exactly the statement
// that writes log_path: the earliest moment the metadata write can fail on
// its own. The verdict may not survive it, and neither may its event.
func TestVerdictRollsBackWholesale(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)
	runID := aRunningStepInternal(t, s)

	// Abort the transaction from inside the metadata columns of the
	// verdict statement.
	_, err := s.w.ExecContext(ctx, `CREATE TRIGGER paceq_test_abort_on_log_metadata
AFTER UPDATE OF log_path ON steps
WHEN NEW.log_path IS NOT NULL AND OLD.log_path IS NULL
BEGIN SELECT RAISE(ABORT, 'injected failure inside the verdict write'); END`)
	if err != nil {
		t.Fatalf("install the injection trigger: %v", err)
	}
	eventsBefore, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events before the injected failure: %v", err)
	}

	_, err = s.RecordStepOutcome(ctx, runID, "extract", StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode:   intPtr(1),
		LogMeta: LogMeta{
			RelPath:   "2026-09-17/run/extract.1.ndjson",
			Bytes:     4096,
			Truncated: true,
			ErrorTail: "the tail",
		},
	})
	if err == nil {
		t.Fatal("the verdict succeeded although its log metadata aborted")
	}
	if !strings.Contains(err.Error(), "injected failure") {
		t.Fatalf("error does not name the injected failure: %v", err)
	}

	detail, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	step := detail.Steps[0]
	if step.State != "running" {
		t.Fatalf("state = %q after the failed finish, want running: the verdict"+
			" must not outlive its log metadata", step.State)
	}
	if step.LogPath != "" || step.ErrorTail != "" || step.LogBytes != 0 || step.LogTruncated {
		t.Fatalf("log metadata survived the failed finish: %+v", step)
	}
	eventsAfter, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events after the injected failure: %v", err)
	}
	if len(eventsBefore) != len(eventsAfter) {
		t.Errorf("events went from %d to %d: a rolled back transition left an event",
			len(eventsBefore), len(eventsAfter))
	}
}

func intPtr(i int) *int { return &i }
