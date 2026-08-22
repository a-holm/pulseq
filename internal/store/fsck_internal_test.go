package store

import (
	"context"
	"strings"
	"testing"
)

// fsck is only as good as its ability to catch what it claims to catch. Each
// test below plants one violation straight into the database and requires the
// sweep to name it. A checker that stayed silent here would be lying about
// coverage, which is worse than having no checker.
//
// The reason codes planted here are the catalogue's real values spelled out,
// because the point is what the database holds, not what the compiler knows.

func TestFsckFindsNoViolationOnAHealthyDatabase(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)
	runID := aRunningStepInternal(t, s)
	if _, err := s.RecordStepOutcome(ctx, runID, "extract", StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: "STEP_SUCCEEDED",
	}); err != nil {
		t.Fatalf("record the verdict: %v", err)
	}
	if _, err := s.FinishRun(ctx, runID, "exec-1", FinishReason{Code: "RUN_SUCCEEDED"}); err != nil {
		t.Fatalf("finish the run: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("fsck flagged a healthy database: %+v", violations)
	}
}

// plantSeededRun gives every planting test a queued run of two steps, where
// the second needs the first.
func plantSeededRun(t *testing.T) (*Store, string) {
	t.Helper()

	s := internalStore(t)
	version := aJobInternal(t, s, "nightly")
	run, err := s.CreateRunWithSteps(context.Background(), NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []NewStep{{Name: "build"}, {Name: "deploy", DependsOn: []string{"build"}}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}
	return s, run.ID
}

func TestFsckCatchesAnOpenStepUnderATerminalRun(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	// A terminal run over a pending step is exactly what I2 forbids.
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'succeeded',
reason_code = 'RUN_SUCCEEDED', finished_at = created_at + 1000 WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the violation: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if !hasCheck(violations, "I2") {
		t.Fatalf("fsck missed an open step under a terminal run: %+v", violations)
	}
}

func TestFsckCatchesADisagreeingAggregate(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	// Both steps succeeded on paper, but the run claims failure. Only the
	// aggregate rule can see the lie.
	for _, name := range []string{"build", "deploy"} {
		if _, err := s.w.ExecContext(ctx, `UPDATE steps SET state = 'succeeded',
reason_code = 'STEP_SUCCEEDED', finished_at = started_at WHERE run_id = ? AND name = ?`, runID, name); err != nil {
			t.Fatalf("plant the step verdict: %v", err)
		}
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'failed',
reason_code = 'RUN_FAILED_STEP', finished_at = created_at + 1000 WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the violation: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	found := false
	for _, v := range violations {
		if v.Check == "I10" && strings.Contains(v.Detail, "aggregate to succeeded") {
			found = true
		}
	}
	if !found {
		t.Fatalf("fsck missed the failed run with no failed step: %+v", violations)
	}
}

func TestFsckCatchesTimeGoingBackwards(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	// A run that claims to have started before it was created.
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET started_at = created_at - 5000
WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the violation: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if !hasCheck(violations, "I13") {
		t.Fatalf("fsck missed time going backwards: %+v", violations)
	}
}

func TestFsckCatchesAHeldBackRunWithNoDeferReason(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	// Deferred is not a state: queued with available_at ahead of created
	// and nothing that says why. The schema's own CHECK refuses this at
	// write time, so it is deferred for the plant; fsck exists for rows
	// that arrived around or before such a constraint.
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 1`); err != nil {
		t.Fatalf("defer the check constraints: %v", err)
	}
	_, err := s.w.ExecContext(ctx, `UPDATE runs SET available_at = created_at + 60000
WHERE id = ?`, runID)
	if _, err2 := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 0`); err2 != nil {
		t.Fatalf("restore the check constraints: %v", err2)
	}
	if err != nil {
		t.Fatalf("plant the violation: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if !hasCheck(violations, "I14") {
		t.Fatalf("fsck missed a deferral without its reason: %+v", violations)
	}
}

func TestFsckCatchesABrokenEventChain(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	if _, err := s.ClaimRun(ctx, runID, LeaseInput{Owner: "exec-1"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build"); err != nil {
		t.Fatalf("start build: %v", err)
	}

	// Rewind the run's own chain: the run.started event now claims a
	// from_state nothing could have been in.
	if _, err := s.w.ExecContext(ctx, `UPDATE run_events SET from_state = 'failed'
WHERE run_id = ? AND kind = 'run.started'`, runID); err != nil {
		t.Fatalf("plant the violation: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	found := false
	for _, v := range violations {
		if v.Check == "I15" && strings.Contains(v.Detail, "run.started") {
			found = true
		}
	}
	if !found {
		t.Fatalf("fsck missed a broken event chain: %+v", violations)
	}
}

func TestFsckCatchesATerminalRowWithoutAReasonCode(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	// Straight to failed with no explanation at all. The CHECK that
	// refuses this is deferred for the plant, for the same reason the
	// other plants are: fsck is the second line of defence.
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 1`); err != nil {
		t.Fatalf("defer the check constraints: %v", err)
	}
	_, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'failed',
finished_at = created_at + 1000, reason_code = NULL WHERE id = ?`, runID)
	if _, err2 := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 0`); err2 != nil {
		t.Fatalf("restore the check constraints: %v", err2)
	}
	if err != nil {
		t.Fatalf("plant the violation: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if !hasCheck(violations, "reason") {
		t.Fatalf("fsck missed a terminal row without a reason code: %+v", violations)
	}
}

// The claim predicate has to read idx_steps_claimable, or every scheduling
// decision scans the steps table instead.
func TestNextRunnableStepQueryPlansThroughTheClaimIndex(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	rows, err := s.r.QueryContext(ctx, "EXPLAIN QUERY PLAN "+nextRunnableStepSQL,
		runID, int64(0))
	if err != nil {
		t.Fatalf("explain the claim predicate: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var plan []string
	for rows.Next() {
		var id, parent int64
		var notused, detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan a plan row: %v", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the plan: %v", err)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "idx_steps_claimable") {
		t.Fatalf("the claim predicate does not read idx_steps_claimable:\n%s", joined)
	}
}

func hasCheck(violations []Violation, check string) bool {
	for _, v := range violations {
		if v.Check == check {
			return true
		}
	}
	return false
}
