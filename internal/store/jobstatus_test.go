package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// JobLastRuns feeds paceq status: one row per job, carrying that job's newest
// run and how far its steps got. The cases here pin the shape a status line
// renders from, so the command cannot quietly invent a different one.

// oneHour separates two seeded runs so their ids and timestamps cannot tie.
const oneHour = 60 * 60 * 1000 * time.Millisecond

// aVersion records one job with one version and returns the version id.
func aVersion(t *testing.T, s *store.Store, name string) string {
	t.Helper()

	version := aJob(t, s, name)
	return version.ID
}

// seedRun materialises a run of the given steps, drives every step to a
// terminal state and finishes the run. A failed first step leaves the rest
// skipped, which is what a real failed run looks like.
func seedRun(t *testing.T, s *store.Store, job, versionID string, steps []store.NewStep, failFirst bool) store.Run {
	t.Helper()

	ctx := context.Background()
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      job,
		JobVersionID: versionID,
		Origin:       "manual",
		Steps:        steps,
	})
	if err != nil {
		t.Fatalf("create a run of %s: %v", job, err)
	}
	if _, err := s.ClaimRun(ctx, run.ID, store.LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim run %s: %v", run.ID, err)
	}
	for i, step := range steps {
		if err := s.StartStep(ctx, run.ID, step.Name); err != nil {
			t.Fatalf("start %s of run %s: %v", step.Name, run.ID, err)
		}
		out := store.StepOutcome{
			Event:      "step_succeeded",
			ReasonCode: reason.STEPSucceeded,
		}
		if failFirst && i == 0 {
			out = store.StepOutcome{
				Event:      "step_failed",
				ReasonCode: reason.STEPFailedNonzeroExit,
			}
		}
		if _, err := s.RecordStepOutcome(ctx, run.ID, step.Name, out); err != nil {
			t.Fatalf("record %s of run %s: %v", step.Name, run.ID, err)
		}
	}
	code := reason.RUNSucceeded
	if failFirst {
		code = reason.RUNFailedStep
	}
	if _, err := s.FinishRun(ctx, run.ID, "test", store.FinishReason{Code: code}); err != nil {
		t.Fatalf("finish run %s: %v", run.ID, err)
	}
	return run
}

func TestJobLastRunsIsEmptyWithoutJobs(t *testing.T) {
	s, _ := coreStore(t)

	rows, err := s.JobLastRuns(context.Background(), "")
	if err != nil {
		t.Fatalf("JobLastRuns on an empty database: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("an empty database returned %d rows, want none", len(rows))
	}
}

func TestJobLastRunsCarriesEachJobsNewestRun(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	version := aVersion(t, s, "nightly")

	// Two runs of one job: the second is the one status must show.
	first := seedRun(t, s, "nightly", version, twoSteps(), false)
	clk.Advance(oneHour)
	second := seedRun(t, s, "nightly", version, twoSteps(), false)

	// A second job whose only run failed on its first step.
	clk.Advance(oneHour)
	otherVersion := aVersion(t, s, "import")
	clk.Advance(oneHour)
	failed := seedRun(t, s, "import", otherVersion, twoSteps(), true)

	// A third job that has never run.
	clk.Advance(oneHour)
	aVersion(t, s, "idle")

	rows, err := s.JobLastRuns(ctx, "")
	if err != nil {
		t.Fatalf("JobLastRuns: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want one per job (3)", len(rows))
	}
	if rows[0].JobName != "idle" || rows[1].JobName != "import" || rows[2].JobName != "nightly" {
		t.Errorf("jobs came back in order %s, %s, %s, want name order",
			rows[0].JobName, rows[1].JobName, rows[2].JobName)
	}

	idle, imp, nightly := rows[0], rows[1], rows[2]
	if idle.RunID != "" || idle.State != "" {
		t.Errorf("idle carried a run: %s in state %q, want none", idle.RunID, idle.State)
	}
	if imp.RunID != failed.ID || imp.State != "failed" {
		t.Errorf("import showed %s in state %q, want %s failed", imp.RunID, imp.State, failed.ID)
	}
	if imp.ReasonCode != string(reason.RUNFailedStep) {
		t.Errorf("import reason = %q, want %q", imp.ReasonCode, reason.RUNFailedStep)
	}
	if imp.StepsTotal != 2 || imp.StepsDone != 2 {
		t.Errorf("import steps = %d/%d, want 2/2 (a failed step and a skip are both done)",
			imp.StepsDone, imp.StepsTotal)
	}
	if nightly.RunID != second.ID {
		t.Errorf("nightly showed run %s, want the newest one %s (not %s)",
			nightly.RunID, second.ID, first.ID)
	}
	if nightly.State != "succeeded" || nightly.StepsDone != 2 || nightly.StepsTotal != 2 {
		t.Errorf("nightly = %s %d/%d, want succeeded 2/2",
			nightly.State, nightly.StepsDone, nightly.StepsTotal)
	}
	if nightly.StartedAt.IsZero() || nightly.FinishedAt.IsZero() {
		t.Errorf("nightly carries no timestamps: started %v finished %v",
			nightly.StartedAt, nightly.FinishedAt)
	}
}

func TestJobLastRunsFiltersByJob(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	seedRun(t, s, "nightly", aVersion(t, s, "nightly"), twoSteps(), false)
	clk.Advance(oneHour)
	seedRun(t, s, "import", aVersion(t, s, "import"), twoSteps(), false)

	rows, err := s.JobLastRuns(ctx, "nightly")
	if err != nil {
		t.Fatalf("JobLastRuns filtered by job: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want the one nightly row", len(rows))
	}
	if rows[0].JobName != "nightly" || rows[0].RunID == "" || rows[0].State != "succeeded" {
		t.Errorf("row is for %s (%s, %s), want the nightly run succeeded",
			rows[0].JobName, rows[0].RunID, rows[0].State)
	}
}

func TestJobNamesListsEveryJobInNameOrder(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	aVersion(t, s, "nightly")
	aVersion(t, s, "import")

	names, err := s.JobNames(ctx)
	if err != nil {
		t.Fatalf("JobNames: %v", err)
	}
	if len(names) != 2 || names[0] != "import" || names[1] != "nightly" {
		t.Errorf("names = %v, want [import nightly]", names)
	}
}
