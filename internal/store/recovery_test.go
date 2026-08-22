//go:build unix

package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// seedRunningRunWithLease opens a fake clock store, applies a one step job,
// materialises a run and claims it under the given lease TTL. The caller
// decides when that lease expires by advancing the clock.
func seedRunningRunWithLease(t *testing.T, ttl time.Duration) (*Store, string) {
	t.Helper()

	s, err := Open(context.Background(), t.TempDir()+"/state.db",
		Options{Clock: clock.NewFake(time.Date(2026, 9, 17, 3, 0, 0, 0, time.UTC))})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	spec := `{"schema":"paceq.job.v1","name":"recovery","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["true"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:  "recovery",
		SpecHash: "sha256:recovery",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	res, err := s.MaterializeManualTrigger(context.Background(), ManualTriggerInput{
		JobName: "recovery", Actor: "test",
	})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	if _, err := s.ClaimRun(context.Background(), res.Run.ID,
		LeaseInput{Owner: "exec-doomed", TTL: ttl}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return s, res.Run.ID
}

// crashCount reads the column GetRun does not project. Only tests care about
// the number itself; everyone else sees its effect through the machine.
func crashCount(t *testing.T, s *Store, runID string) int {
	t.Helper()

	var n int
	if err := s.w.QueryRowContext(context.Background(),
		`SELECT crash_count FROM runs WHERE id = ?`, runID).Scan(&n); err != nil {
		t.Fatalf("read crash_count: %v", err)
	}
	return n
}

// TestRequeueCrashedRunRefusesALivingLease is the safety catch's own proof:
// while a lease has time left on it, the run may have a living executor, and
// recovery must refuse to touch it no matter who asks.
func TestRequeueCrashedRunRefusesALivingLease(t *testing.T) {
	s, runID := seedRunningRunWithLease(t, 5*time.Minute)
	defer func() { _ = s.Close() }()

	err := s.RequeueCrashedRun(context.Background(), runID)
	if err == nil {
		t.Fatal("the requeue succeeded while the lease was still valid")
	}
	if !strings.Contains(err.Error(), "not expired") {
		t.Errorf("the refusal says %q, want it to name the unexpired lease", err)
	}

	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the run: %v", err)
	}
	if detail.Run.State != "running" {
		t.Errorf("the run moved to %s behind a living lease", detail.Run.State)
	}
}

// TestRequeueCrashedRunRequeuesAfterExpiry drives the whole recovery
// transition against a fake clock: the lease lapses, the requeue lands, and
// the row carries everything the machine demanded, with one event telling the
// story.
func TestRequeueCrashedRunRequeuesAfterExpiry(t *testing.T) {
	s, runID := seedRunningRunWithLease(t, 5*time.Minute)
	defer func() { _ = s.Close() }()

	// The lease expires; nothing renews it, because the owner is dead.
	advance := s.clk.(*clock.Fake)
	advance.Advance(6 * time.Minute)

	if err := s.RequeueCrashedRun(context.Background(), runID); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the run: %v", err)
	}
	if detail.Run.State != "queued" {
		t.Errorf("state is %s, want queued", detail.Run.State)
	}
	if detail.Run.DeferReason != model.DeferReasonAfterCrash {
		t.Errorf("defer_reason is %q, want %q", detail.Run.DeferReason, model.DeferReasonAfterCrash)
	}
	if detail.Run.LeaseOwner != "" || !detail.Run.LeaseExpiresAt.IsZero() {
		t.Errorf("the lease survived the requeue: owner %q expires %v",
			detail.Run.LeaseOwner, detail.Run.LeaseExpiresAt)
	}
	if crashCount(t, s, runID) != 1 {
		t.Errorf("crash_count is %d, want 1", crashCount(t, s, runID))
	}
	if detail.Run.AvailableAt.Before(detail.Run.CreatedAt) {
		t.Errorf("available_at %v sits before created_at %v",
			detail.Run.AvailableAt, detail.Run.CreatedAt)
	}

	events, err := s.RunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != "run.requeued" {
		t.Errorf("the last event is %s, want run.requeued", last.Kind)
	}
	if last.FromState != "running" || last.ToState != "queued" {
		t.Errorf("the requeue event moved %s->%s, want running->queued",
			last.FromState, last.ToState)
	}

	violations, err := s.Fsck(context.Background())
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found %v after a legal requeue", violations)
	}
}

// TestRequeueCrashedRunRefusesATerminalRun pins the shape of the refusal for
// a run that needs no recovery at all.
func TestRequeueCrashedRunRefusesATerminalRun(t *testing.T) {
	s, runID := seedRunningRunWithLease(t, 5*time.Minute)
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	// The doomed executor starts its step, finishes it, and finishes the
	// run before dying in the test's imagination: the point is only that
	// the run is terminal when recovery is asked about it.
	if err := s.StartStep(ctx, runID, "only"); err != nil {
		t.Fatalf("start the step: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "only", StepOutcome{
		Event:      string(model.EvStepSucceeded),
		ReasonCode: reason.STEPSucceeded,
	}); err != nil {
		t.Fatalf("record the verdict: %v", err)
	}
	if _, err := s.FinishRun(ctx, runID, "exec-doomed",
		FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	s.clk.(*clock.Fake).Advance(6 * time.Minute)

	err := s.RequeueCrashedRun(ctx, runID)
	if err == nil {
		t.Fatal("a terminal run was requeued")
	}
}
