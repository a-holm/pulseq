package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// recoverPoll is how often Recover re-reads a lease that has not expired yet.
// The wait exists for correctness, not for speed: recovery may not touch a
// run whose executor could still be alive, so it waits out the lease and
// polls rather than sleeping one long block, which keeps tests fast and a
// production caller responsive to context cancellation.
const recoverPoll = 10 * time.Millisecond

// Recover closes out what a dead executor left behind and makes the run
// claimable again. It is the restart half of the guarantee the crash harness
// (#75) proves: a run interrupted by SIGKILL converges on restart, without an
// invariant violation and without inventing a verdict.
//
// Three steps, in this order:
//
//  1. Wait for the abandoned lease to expire. A lease that still lives may
//     belong to a process that is only slow, and two executors on one run is
//     the one outcome fencing exists to prevent.
//  2. Close every step the dead attempt left running. Each goes through the
//     machine as a failed attempt with STEP_FAILED_EXECUTOR_LOST: the verdict
//     was lost with the executor, so recovery records exactly that instead of
//     guessing. A step with attempts left comes back pending for its next
//     attempt; otherwise it fails and the run will follow.
//  3. Requeue the run through the store's lease_expired transition, which
//     bumps the epoch, counts the crash and writes defer_reason.
//
// A run that is not running needs nothing and is returned untouched, so a
// caller may call Recover before every ExecuteRun without checking first.
func (e *Engine) Recover(ctx context.Context, runID string) (string, error) {
	if err := e.waitForDeadLease(ctx, runID); err != nil {
		return "", err
	}

	detail, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return "", fmt.Errorf("recover run %s: %w", runID, err)
	}
	if detail.Run.State != string(model.RunRunning) {
		return detail.Run.State, nil
	}

	now := e.Clock.Now().UTC()
	for _, step := range detail.Steps {
		if model.StepState(step.State) != model.StepRunning {
			continue
		}
		outcome := storeStepOutcomeLost(now, e.Owner)
		if err := e.Store.RecordStepOutcome(ctx, runID, step.Name, outcome); err != nil {
			return "", fmt.Errorf("recover step %s of run %s: %w", step.Name, runID, err)
		}
	}

	if err := e.Store.RequeueCrashedRun(ctx, runID); err != nil {
		return "", fmt.Errorf("recover run %s: %w", runID, err)
	}
	return string(model.RunQueued), nil
}

// waitForDeadLease blocks until the run's lease has lapsed or the context
// ends. An expired lease is evidence enough that the previous owner is gone;
// the state directory lock, which a crashed process cannot keep, backs it up.
func (e *Engine) waitForDeadLease(ctx context.Context, runID string) error {
	for {
		detail, err := e.Store.GetRun(ctx, runID)
		if err != nil {
			return fmt.Errorf("recover run %s: %w", runID, err)
		}
		if detail.Run.State != string(model.RunRunning) {
			return nil // nothing is claimed; nothing to wait for
		}
		if detail.Run.LeaseExpiresAt.IsZero() || !detail.Run.LeaseExpiresAt.After(e.Clock.Now()) {
			return nil
		}
		timer := e.Clock.NewTimer(recoverPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("recover run %s: %w", runID, ctx.Err())
		case <-timer.C:
		}
	}
}

// storeStepOutcomeLost builds the verdict recovery writes over a dead
// attempt. The event is a failure because the machine retries failures and
// never retries cancellations; the code says plainly why no exit code, signal
// or log tail accompanies it.
func storeStepOutcomeLost(now time.Time, owner string) store.StepOutcome {
	return store.StepOutcome{
		Event:      string(model.EvStepFailed),
		ReasonCode: reason.STEPFailedExecutorLost,
		FinishedAt: now,
		DetailJSON: detailJSON(map[string]any{"recovered_by": owner}),
	}
}
