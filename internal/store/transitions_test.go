package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// The transition layer is where guarantee G10 lives: every state change and
// exactly one event row, in one transaction. Each test below drives real
// transitions through a migrated store and reads back both sides of that
// bargain.

const testOwner = "exec-1"

// aMaterializedRun seeds the single step job, materialises one manual run of
// it and returns the run's id.
func aMaterializedRun(t *testing.T, s *store.Store) string {
	t.Helper()

	aCanonicalJob(t, s, "nightly", singleStepSpec)
	out, err := s.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{
		JobName: "nightly",
	})
	if err != nil {
		t.Fatalf("materialize nightly: %v", err)
	}
	return out.Run.ID
}

func TestClaimRunStartsTheRunAndTakesTheLease(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	state, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner, TTL: time.Minute})
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if state != "running" {
		t.Fatalf("state = %q, want running", state)
	}

	run := mustGetRun(t, ctx, s, runID)
	if run.StartedAt.IsZero() || !run.StartedAt.Equal(clk.Now()) {
		t.Errorf("started_at = %s, want the clock time", run.StartedAt)
	}
	if run.LeaseOwner != testOwner {
		t.Errorf("lease_owner = %q, want %q", run.LeaseOwner, testOwner)
	}
	if run.LeaseEpoch != 1 {
		t.Errorf("lease_epoch = %d, want 1 after the first claim", run.LeaseEpoch)
	}
	if run.LeaseExpiresAt.IsZero() {
		t.Error("lease_expires_at was not set")
	}

	want := []struct{ kind, from, to string }{
		{"run.queued", "", "queued"},
		{"run.started", "queued", "running"},
	}
	assertEventChain(t, ctx, s, runID, want)
}

func TestClaimRunOnACancelledRequestCancelsInstead(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "operator changed their mind"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	state, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner})
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if state != "cancelled" {
		t.Fatalf("state = %q, want cancelled: a requested cancel is observed before any work starts", state)
	}

	run := mustGetRun(t, ctx, s, runID)
	if run.State != "cancelled" || run.ReasonCode != string(reason.RUNCancelledManual) {
		t.Errorf("run = %s/%s, want cancelled/%s", run.State, run.ReasonCode, reason.RUNCancelledManual)
	}
	if run.FinishedAt.IsZero() || !run.FinishedAt.Equal(clk.Now()) {
		t.Errorf("finished_at = %s, want the clock time", run.FinishedAt)
	}
	events, _ := s.RunEvents(ctx, runID)
	last := events[len(events)-1]
	if last.Kind != "run.cancelled" || last.Actor != "cli:1000" {
		t.Errorf("last event = %s by %q, want run.cancelled by cli:1000", last.Kind, last.Actor)
	}
}

func TestClaimRunRefusesASecondClaim(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "exec-2"}); !errors.Is(err, store.ErrNotClaimable) {
		t.Errorf("second claim error = %v, want ErrNotClaimable", err)
	}
}

func TestStartStepOpensTheFirstAttempt(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	err := s.StartStep(ctx, runID, "build")
	if err != nil {
		t.Fatalf("StartStep: %v", err)
	}

	step := mustStep(t, ctx, s, runID, "build")
	if step.State != "running" || step.Attempt != 1 {
		t.Errorf("step = %s attempt %d, want running attempt 1", step.State, step.Attempt)
	}
	if step.StartedAt.IsZero() || !step.StartedAt.Equal(clk.Now()) {
		t.Errorf("step started_at = %s, want the clock time", step.StartedAt)
	}
	want := []struct{ kind, from, to string }{
		{"run.queued", "", "queued"},
		{"run.started", "queued", "running"},
		{"step.started", "pending", "running"},
	}
	assertEventChain(t, ctx, s, runID, want)
}

func TestRecordStepOutcomeSuccessStoresTheWholeVerdict(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build"); err != nil {
		t.Fatalf("start: %v", err)
	}
	clk.Advance(2 * time.Second)

	_, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		ExitCode:   ptr(0),
		FinishedAt: clk.Now(),
		LogMeta:    store.LogMeta{RelPath: "/logs/r/s/build.1.ndjson", Bytes: 128},
	})
	if err != nil {
		t.Fatalf("RecordStepOutcome: %v", err)
	}

	run := mustGetRun(t, ctx, s, runID)
	step := run.Steps[0]
	if step.State != "succeeded" {
		t.Errorf("state = %s, want succeeded", step.State)
	}
	if !step.HasExitCode || step.ExitCode != 0 {
		t.Errorf("exit code = %d/%v, want a stored zero", step.ExitCode, step.HasExitCode)
	}
	if step.FinishedAt.IsZero() || !step.FinishedAt.Equal(clk.Now()) {
		t.Errorf("finished_at = %s, want the advanced clock time", step.FinishedAt)
	}
	if step.LogPath != "/logs/r/s/build.1.ndjson" {
		t.Errorf("log_path = %q", step.LogPath)
	}
	want := []struct{ kind, from, to string }{
		{"run.queued", "", "queued"},
		{"run.started", "queued", "running"},
		{"step.started", "pending", "running"},
		{"step.succeeded", "running", "succeeded"},
	}
	assertEventChain(t, ctx, s, runID, want)
	events, _ := s.RunEvents(ctx, runID)
	if got := events[len(events)-1].ReasonCode; got != string(reason.STEPSucceeded) {
		t.Errorf("event reason = %q, want %q", got, reason.STEPSucceeded)
	}
	testutil.AssertNoUnknownReasons(t, ctx, s)
}

func TestRecordStepOutcomeFailureKeepsExitCodeSignalAndTail(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build"); err != nil {
		t.Fatalf("start: %v", err)
	}
	clk.Advance(time.Second)

	_, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedTimeout,
		Signal:     "SIGKILL",
		FinishedAt: clk.Now(),
		LogMeta:    store.LogMeta{Bytes: 4096, Truncated: true, ErrorTail: "last line before the kill"},
		DetailJSON: `{"timeout_ms":1500}`,
	})
	if err != nil {
		t.Fatalf("RecordStepOutcome: %v", err)
	}

	step := mustStep(t, ctx, s, runID, "build")
	if step.State != "failed" {
		t.Errorf("state = %s, want failed", step.State)
	}
	if step.Signal != "SIGKILL" {
		t.Errorf("signal = %q", step.Signal)
	}
	if step.HasExitCode {
		t.Error("a signalled step has no exit code, but one was stored")
	}
	if !step.NextAttemptAt.IsZero() {
		t.Error("M1 schedules no retries, next_attempt_at must stay unset")
	}
	events, _ := s.RunEvents(ctx, runID)
	last := events[len(events)-1]
	if last.DetailJSON != `{"timeout_ms":1500}` {
		t.Errorf("detail_json = %q", last.DetailJSON)
	}
}

func TestRecordSkipMovesAPendingStepStraightOut(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// A skip never ran: no start, no exit code, and its own event.
	_, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      "upstream_failed",
		ReasonCode: reason.STEPSkippedUpstreamFailed,
		FinishedAt: clk.Now(),
	})
	if err != nil {
		t.Fatalf("RecordStepOutcome: %v", err)
	}
	step := mustStep(t, ctx, s, runID, "build")
	if step.State != "skipped" {
		t.Errorf("state = %s, want skipped", step.State)
	}
	if step.Attempt != 0 || step.HasExitCode || !step.StartedAt.IsZero() {
		t.Errorf("a skipped step never ran: %+v", step)
	}
	want := []struct{ kind, from, to string }{
		{"run.queued", "", "queued"},
		{"run.started", "queued", "running"},
		{"step.skipped", "pending", "skipped"},
	}
	assertEventChain(t, ctx, s, runID, want)
}

func TestRecordOutcomeRefusalWritesNothing(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Success on a pending step is an illegal transition: the machine
	// refuses it, and the refusal moves nothing.
	before, _ := s.RunEvents(ctx, runID)
	_, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
	})
	if err == nil {
		t.Fatal("an illegal transition was accepted")
	}
	after, _ := s.RunEvents(ctx, runID)
	if len(before) != len(after) {
		t.Errorf("events went from %d to %d: a refusal wrote an event", len(before), len(after))
	}
	if step := mustStep(t, ctx, s, runID, "build"); step.State != "pending" {
		t.Errorf("state = %s after a refused transition, want pending", step.State)
	}
}

func TestFinishRunAggregatesThroughTheMachine(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failStep   bool
		wantState  string
		wantReason reason.Code
	}{
		{"all steps succeed", false, "succeeded", reason.RUNSucceeded},
		{"a failed step fails the run", true, "failed", reason.RUNFailedStep},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, clk := coreStore(t)
			runID := aMaterializedRun(t, s)
			if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
				t.Fatalf("claim: %v", err)
			}
			if err := s.StartStep(ctx, runID, "build"); err != nil {
				t.Fatalf("start: %v", err)
			}
			out := store.StepOutcome{Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0), FinishedAt: clk.Now()}
			reasonData := `{}`
			if tc.failStep {
				out = store.StepOutcome{Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit, ExitCode: ptr(7), FinishedAt: clk.Now()}
			}
			if _, err := s.RecordStepOutcome(ctx, runID, "build", out); err != nil {
				t.Fatalf("record: %v", err)
			}
			clk.Advance(time.Second)

			state, err := s.FinishRun(ctx, runID, testOwner, store.FinishReason{
				Code: tc.wantReason, Data: reasonData,
			})
			if err != nil {
				t.Fatalf("FinishRun: %v", err)
			}
			if state != tc.wantState {
				t.Errorf("FinishRun = %s, want %s", state, tc.wantState)
			}
			run := mustGetRun(t, ctx, s, runID)
			if run.ReasonCode != string(tc.wantReason) {
				t.Errorf("reason_code = %q, want %q", run.ReasonCode, tc.wantReason)
			}
			if run.ReasonData != reasonData {
				t.Errorf("reason_data = %q, want %q", run.ReasonData, reasonData)
			}
			if run.FinishedAt.IsZero() {
				t.Error("finished_at not set")
			}
			if run.LeaseOwner != "" || !run.LeaseExpiresAt.IsZero() {
				t.Errorf("lease not released: owner %q expires %s", run.LeaseOwner, run.LeaseExpiresAt)
			}
			testutil.AssertNoUnknownReasons(t, ctx, s)
		})
	}
}

func TestFinishRunRefusesWhileAStepIsStillOpen(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build"); err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, err := s.FinishRun(ctx, runID, testOwner, store.FinishReason{Code: reason.RUNSucceeded}); err == nil {
		t.Fatal("finished a run whose step was still running; I2 says no")
	}
	if run := mustGetRun(t, ctx, s, runID); run.State != "running" {
		t.Errorf("state = %s after the refusal, want running", run.State)
	}
}

func TestFinishRunRefusesAWrongOwner(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	if _, err := s.FinishRun(ctx, runID, "exec-9", store.FinishReason{Code: reason.RUNSucceeded}); err == nil {
		t.Fatal("a writer without the lease finished the run")
	}
}

func TestObserveRunCancelCarriesTheRequestersActor(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "too slow"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	// The step is cancelled first (its process group already killed outside
	// any transaction), then the run.
	if _, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event: "cancel_observed", ReasonCode: reason.STEPCancelled, FinishedAt: clk.Now(),
	}); err != nil {
		t.Fatalf("cancel the step: %v", err)
	}
	if err := s.ObserveRunCancel(ctx, runID, testOwner, "cli:1000", reason.RUNCancelledManual); err != nil {
		t.Fatalf("ObserveRunCancel: %v", err)
	}

	run := mustGetRun(t, ctx, s, runID)
	if run.State != "cancelled" {
		t.Errorf("state = %s, want cancelled (not failed)", run.State)
	}
	if run.ReasonCode != string(reason.RUNCancelledManual) {
		t.Errorf("reason = %q, want %q", run.ReasonCode, reason.RUNCancelledManual)
	}
	events, _ := s.RunEvents(ctx, runID)
	last := events[len(events)-1]
	if last.Kind != "run.cancelled" || last.Actor != "cli:1000" {
		t.Errorf("last event = %s by %q, want run.cancelled by the requester", last.Kind, last.Actor)
	}
	testutil.AssertNoUnknownReasons(t, ctx, s)
}

func TestObserveRunCancelRefusedWithoutARequest(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.ObserveRunCancel(ctx, runID, testOwner, "cli:1000", reason.RUNCancelledManual); err == nil {
		t.Fatal("cancelled a run nobody asked to cancel")
	}
}

func TestRequestCancelIsMonotoneAndIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)

	first, err := s.RequestCancel(ctx, runID, "cli:1000", "first ask")
	if err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	if first.CancelRequestedBy != "cli:1000" {
		t.Errorf("requested_by = %q", first.CancelRequestedBy)
	}

	// A second ask changes nothing: the first request stands.
	second, err := s.RequestCancel(ctx, runID, "cli:2000", "second ask")
	if err != nil {
		t.Fatalf("second RequestCancel: %v", err)
	}
	if second.CancelRequestedBy != "cli:1000" || second.CancelReason != "first ask" {
		t.Errorf("the second request overwrote the first: %+v", second)
	}
}

func TestEveryTransitionCarriesExactlyOneEvent(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	// After every method call the event count is exactly the number of
	// transitions performed so far, and the chain stays continuous.
	transitions := 0
	expect := func() {
		t.Helper()
		events, err := s.RunEvents(ctx, runID)
		if err != nil {
			t.Fatalf("RunEvents: %v", err)
		}
		if len(events) != transitions {
			t.Fatalf("%d transitions performed, %d event rows: G10 broken", transitions, len(events))
		}
	}

	transitions++ // queued
	expect()
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	transitions++ // started
	expect()
	if err := s.StartStep(ctx, runID, "build"); err != nil {
		t.Fatalf("start: %v", err)
	}
	transitions++ // step.started
	expect()
	if _, err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0), FinishedAt: clk.Now(),
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	transitions++ // step.succeeded
	expect()
	if _, err := s.FinishRun(ctx, runID, testOwner, store.FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("finish: %v", err)
	}
	transitions++ // run.succeeded
	expect()

	// And the whole chain reads as one story per actor.
	want := []struct{ kind, from, to string }{
		{"run.queued", "", "queued"},
		{"run.started", "queued", "running"},
		{"step.started", "pending", "running"},
		{"step.succeeded", "running", "succeeded"},
		{"run.succeeded", "running", "succeeded"},
	}
	assertEventChain(t, ctx, s, runID, want)
}

func TestNextRunnableStepHonoursIndexOrderAndUpstream(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)

	aCanonicalJob(t, s, "graph", diamondSpec)
	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "graph"})
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	runID := out.Run.ID

	// a needs b, and b sorts after a, so index order alone would pick a.
	// The upstream predicate is what keeps a waiting.
	name, ok, err := s.NextRunnableStep(ctx, runID)
	if err != nil || !ok {
		t.Fatalf("NextRunnableStep: %v %v", name, err)
	}
	if name != "b" {
		t.Fatalf("next runnable = %q, want b: a's upstream has not succeeded", name)
	}

	// Once b succeeds, a becomes runnable and still comes first.
	if _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "b"); err != nil {
		t.Fatalf("start b: %v", err)
	}
	if _, err := s.RecordStepOutcome(ctx, runID, "b", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
	}); err != nil {
		t.Fatalf("succeed b: %v", err)
	}
	name, ok, err = s.NextRunnableStep(ctx, runID)
	if err != nil || !ok {
		t.Fatalf("NextRunnableStep: %v %v", name, err)
	}
	if name != "a" {
		t.Errorf("next runnable = %q, want a now that b succeeded", name)
	}

	pending, err := s.PendingSteps(ctx, runID)
	if err != nil {
		t.Fatalf("PendingSteps: %v", err)
	}
	var names []string
	for _, st := range pending {
		names = append(names, st.Name)
	}
	if len(names) != 2 || names[0] != "a" || names[1] != "c" {
		t.Errorf("pending = %v, want [a c] in index order", names)
	}
}

func TestCancelRequestedReadsTheFlagBack(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)

	requested, by, err := s.CancelRequested(ctx, runID)
	if err != nil {
		t.Fatalf("CancelRequested: %v", err)
	}
	if requested || by != "" {
		t.Errorf("nobody asked yet, got %v by %q", requested, by)
	}

	if _, err := s.RequestCancel(ctx, runID, "cli:7", "stop"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	requested, by, err = s.CancelRequested(ctx, runID)
	if err != nil {
		t.Fatalf("CancelRequested: %v", err)
	}
	if !requested || by != "cli:7" {
		t.Errorf("got %v by %q, want true by cli:7", requested, by)
	}
}

// mustGetRun reads one run back or fails the test.
func mustGetRun(t *testing.T, ctx context.Context, s *store.Store, runID string) store.RunDetail {
	t.Helper()

	detail, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun %s: %v", runID, err)
	}
	return detail
}

// mustStep reads one step of a run back or fails the test.
func mustStep(t *testing.T, ctx context.Context, s *store.Store, runID, name string) store.Step {
	t.Helper()

	for _, step := range mustGetRun(t, ctx, s, runID).Steps {
		if step.Name == name {
			return step
		}
	}
	t.Fatalf("run %s has no step %s", runID, name)
	return store.Step{}
}

// assertEventChain reads the event list and checks it is exactly want, in
// order, with a continuous from/to chain.
func assertEventChain(t *testing.T, ctx context.Context, s *store.Store, runID string, want []struct{ kind, from, to string }) {
	t.Helper()

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("RunEvents: %v", err)
	}
	if len(events) != len(want) {
		t.Fatalf("event rows = %d, want %d:", len(events), len(want))
		for i, e := range events {
			t.Logf("  [%d] %s %s->%s", i, e.Kind, e.FromState, e.ToState)
		}
		return
	}
	for i, e := range events {
		if e.Kind != want[i].kind || e.FromState != want[i].from || e.ToState != want[i].to {
			t.Errorf("event[%d] = %s %s->%s, want %s %s->%s",
				i, e.Kind, e.FromState, e.ToState, want[i].kind, want[i].from, want[i].to)
		}
	}
}

func ptr(i int) *int { return &i }
