package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// This file is the transition layer. Every state change a run or a step goes
// through is decided here by internal/model's machines and written here as
// exactly one UPDATE plus exactly one run_events row, in one transaction
// (G10). A state change without its event, or an event without its state
// change, cannot be produced by any caller, because no caller reaches these
// rows except through this file.
//
// The machines decide; this file only performs. When a machine refuses, the
// refusal returns before a single row is touched, so a refused transition
// leaves neither state nor history behind.

// ErrNotClaimable is returned when a run cannot be claimed: it is not queued,
// it is not available yet, or a cancellation is waiting to be observed. It is
// an ordinary outcome for a worker polling for work, not a fault.
var (
	ErrNotClaimable   = errors.New("the run cannot be claimed")
	ErrStepNotPending = errors.New("the step is not pending")
)

// DefaultLeaseTTL is the lease a claim takes when the caller names none. M1
// has one executor per state directory and no reaper, so the expiry is a
// fencing formality; M2 makes it a real deadline.
const DefaultLeaseTTL = 5 * time.Minute

// LeaseInput says who claims a run and for how long.
type LeaseInput struct {
	// Owner is the executor's name. Every later write to this run has to
	// come from the same name, which is what makes the lease a fence and
	// not a decoration.
	Owner string

	// TTL is how long the claim lasts. Zero means DefaultLeaseTTL.
	TTL time.Duration
}

// CancelRequest is the durable cancellation record read back after it was
// written. The request is not a transition and carries no event: the event
// belongs to whoever observes the request and stops the run (02 section 5.8).
type CancelRequest struct {
	CancelRequestedAt time.Time
	CancelRequestedBy string
	CancelReason      string
}

// ClaimRun takes a queued run for execution. The machine decides which way
// the claim goes: a run whose cancellation was requested before it ever
// started is cancelled instead of started, which is the cheapest cancellation
// in the system, because there is no process group to kill.
//
// A run that is not queued, not yet available or already claimed is
// ErrNotClaimable. The claim stamps started_at, takes the lease and writes
// the run.started event in the transaction that moves the state.
func (s *Store) ClaimRun(ctx context.Context, runID string, in LeaseInput) (string, error) {
	if in.TTL <= 0 {
		in.TTL = DefaultLeaseTTL
	}
	now := s.clk.Now().UTC()
	next := ""

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}

		guards := model.Guards{
			Now:             now.UnixMilli(),
			AvailableAt:     run.AvailableAt.UnixMilli(),
			CancelRequested: !run.CancelRequestedAt.IsZero(),
		}
		if guards.CancelRequested {
			// The request is durable and names a person; the run
			// never started, so CANCELLED_MANUAL is the whole story.
			guards.ReasonCode = string(reason.RUNCancelledManual)
		}

		state, effects, err := model.NextRunState(cur, model.EvClaim, guards)
		if err != nil {
			return fmt.Errorf("claim run %s: %w", runID, errors.Join(err, ErrNotClaimable))
		}

		kind := emitKind(effects)
		switch state {
		case model.RunRunning:
			expires := now.Add(in.TTL)
			next = string(state)
			return finishTransition(tx, "claim", func() error {
				_, err := tx.Exec(`UPDATE runs SET state = 'running',
					lease_owner = ?, lease_epoch = lease_epoch + 1, lease_expires_at = ?,
					started_at = ?, updated_at = ?
				WHERE id = ? AND state = 'queued'`,
					in.Owner, expires.UnixMilli(), now.UnixMilli(), now.UnixMilli(), runID)
				return err
			}, tx, RunEvent{
				RunID: runID, At: now, Kind: kind,
				FromState: string(cur), ToState: string(state),
				Actor: in.Owner,
			})
		case model.RunCancelled:
			// The steps never ran; they leave pending the same way any
			// step leaves pending when its run ends without it, so the
			// run cannot go terminal over open steps.
			if err := skipPendingStepsTx(tx, runID, now, string(reason.STEPCancelled)); err != nil {
				return err
			}
			next = string(state)
			return finishTransition(tx, "claim_cancel", func() error {
				_, err := tx.Exec(`UPDATE runs SET state = 'cancelled',
					finished_at = ?, reason_code = ?, updated_at = ?
				WHERE id = ? AND state = 'queued'`,
					now.UnixMilli(), string(reason.RUNCancelledManual), now.UnixMilli(), runID)
				return err
			}, tx, RunEvent{
				RunID: runID, At: now, Kind: kind,
				FromState: string(cur), ToState: string(state),
				ReasonCode: string(reason.RUNCancelledManual),
				Actor:      run.CancelRequestedBy,
			})
		default:
			return fmt.Errorf("claim run %s: the machine sent a run to %s", runID, state)
		}
	})
	if err != nil {
		return "", err
	}
	return next, nil
}

// RequestCancel records that somebody wants the run stopped, durably and
// before anything is killed. The first request stands: a second one changes
// nothing, so two people asking at once cannot disagree about who asked or
// why. Observing the request is a different act, done by whoever holds the
// lease, and that is the transition with the event.
func (s *Store) RequestCancel(ctx context.Context, runID, by, why string) (CancelRequest, error) {
	now := s.clk.Now().UTC()
	var out CancelRequest

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE runs SET
			cancel_requested_at = COALESCE(cancel_requested_at, ?),
			cancel_requested_by = COALESCE(cancel_requested_by, ?),
			cancel_reason = COALESCE(cancel_reason, ?),
			updated_at = ?
		WHERE id = ?`, now.UnixMilli(), nullIfEmpty(by), why, now.UnixMilli(), runID)
		if err != nil {
			return fmt.Errorf("request the cancellation of run %s: %w", runID, err)
		}
		written, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("request the cancellation of run %s: %w", runID, err)
		}
		if written == 0 {
			return fmt.Errorf("request the cancellation of run %s: %w", runID, ErrRunNotFound)
		}
		var at sql.NullInt64
		var byName, reason sql.NullString
		if err := tx.QueryRow(`SELECT cancel_requested_at, cancel_requested_by, cancel_reason
FROM runs WHERE id = ?`, runID).Scan(&at, &byName, &reason); err != nil {
			return fmt.Errorf("read back the cancellation of run %s: %w", runID, err)
		}
		out = CancelRequest{
			CancelRequestedAt: timeOrZero(at),
			CancelRequestedBy: byName.String,
			CancelReason:      reason.String,
		}
		return nil
	})
	if err != nil {
		return CancelRequest{}, err
	}
	return out, nil
}

// CancelRequested reports whether a cancellation is waiting, and who asked.
// The executor reads this between steps and on its poll while a step runs.
func (s *Store) CancelRequested(ctx context.Context, runID string) (bool, string, error) {
	var (
		at      sql.NullInt64
		by      sql.NullString
		ignored sql.NullString
	)
	err := s.r.QueryRowContext(ctx, `SELECT cancel_requested_at, cancel_requested_by, cancel_reason
FROM runs WHERE id = ?`, runID).Scan(&at, &by, &ignored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", fmt.Errorf("read the cancellation of run %s: %w", runID, ErrRunNotFound)
	}
	if err != nil {
		return false, "", fmt.Errorf("read the cancellation of run %s: %w", runID, err)
	}
	return at.Valid, by.String, nil
}

// StartStep opens the first attempt of a pending step: running, attempt up by
// one, started_at stamped, its event in the same transaction. The run has to
// be running: a step may not move while its run is queued, whatever the step
// machine alone would allow.
func (s *Store) StartStep(ctx context.Context, runID, name string) error {
	now := s.clk.Now().UTC()

	return s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		if run.State != string(model.RunRunning) {
			return fmt.Errorf("start step %s of run %s: the run is %s, not running",
				name, runID, run.State)
		}

		step, err := readStepTx(tx, runID, name)
		if err != nil {
			return err
		}
		if step.State != string(model.StepPending) {
			return fmt.Errorf("start step %s of run %s: %w", name, runID, ErrStepNotPending)
		}
		cur, err := model.ParseStepState(step.State)
		if err != nil {
			return err
		}

		state, effects, err := model.NextStepState(cur, model.EvStepStarted, model.Guards{})
		if err != nil {
			return fmt.Errorf("start step %s of run %s: %w", name, runID, err)
		}

		return finishTransition(tx, "start_step", func() error {
			_, err := tx.Exec(`UPDATE steps SET state = 'running', attempt = attempt + 1,
				started_at = ?, finished_at = NULL
			WHERE run_id = ? AND name = ? AND state = 'pending'`,
				now.UnixMilli(), runID, name)
			return err
		}, tx, RunEvent{
			RunID: runID, StepName: name, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
		})
	})
}

// LogMeta is what the log sink reported for one attempt: where the file is
// relative to the log root, how big it grew, whether the quota cut it, and
// the last lines of output. It lands beside the verdict, in the same
// transaction, because a verdict whose evidence went missing is exactly the
// drift the write model refuses.
type LogMeta struct {
	RelPath   string
	Bytes     int64
	Truncated bool
	ErrorTail string
}

// StepOutcome is one event the engine hands to a step: the runner came back,
// or an upstream ended in a way that closes this step, or a cancellation was
// observed. Event names the model event, and the machine decides whether the
// step may take it.
type StepOutcome struct {
	// Event is one of the step machine's input names: step_succeeded,
	// step_failed, cancel_observed or upstream_failed.
	Event string

	// ReasonCode explains a terminal outcome. The machine refuses a
	// terminal transition without one.
	ReasonCode reason.Code

	// ExitCode is what the process exited with. Nil means there is none:
	// a signalled step and a step that never ran have no exit code, and
	// zero is a perfectly ordinary success.
	ExitCode *int

	Signal     string
	FinishedAt time.Time

	// LogMeta carries the attempt's log facts. Empty means the attempt
	// produced no log, which is what a skip is.
	LogMeta LogMeta

	// DetailJSON is the canonical detail object on the event and on the
	// row's reason_data, empty for none.
	DetailJSON string
}

// RecordStepOutcome applies one event to one step. The machine decides
// whether the step may take it and what the transition demands; this writes
// the verdict, its log facts and its event in one transaction. A step that
// fails with attempts left goes back to pending for its next attempt, which
// is the machine's own retry transition; M1 computes no backoff, so the next
// attempt is immediately runnable.
//
// It reports the state the machine landed the step in: pending means a retry
// was scheduled and its transaction is committed. The report says what
// happened only when the error is nil.
func (s *Store) RecordStepOutcome(ctx context.Context, runID, name string, out StepOutcome) (model.StepState, error) {
	finishedAt := out.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = s.clk.Now().UTC()
	}
	detail := out.DetailJSON
	if detail == "" {
		detail = "{}"
	}

	var landed model.StepState
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		step, err := readStepTx(tx, runID, name)
		if err != nil {
			return err
		}
		cur, err := model.ParseStepState(step.State)
		if err != nil {
			return err
		}

		guards := model.Guards{
			ReasonCode:   string(out.ReasonCode),
			AttemptsLeft: step.Attempt < step.MaxAttempts,
		}
		state, effects, err := model.NextStepState(cur, model.Event(out.Event), guards)
		if err != nil {
			return fmt.Errorf("record %s on step %s of run %s: %w", out.Event, name, runID, err)
		}

		var exitCode any
		if out.ExitCode != nil {
			exitCode = *out.ExitCode
		}
		var signal any
		if out.Signal != "" {
			signal = out.Signal
		}
		var nextAttempt any
		if state == model.StepPending {
			// The retry transition: back to pending, runnable at once.
			// Backoff arithmetic is M1-09; it will fill this value and
			// no reader changes.
			nextAttempt = finishedAt.UnixMilli()
		}
		truncated := 0
		if out.LogMeta.Truncated {
			truncated = 1
		}

		landed = state
		return finishTransition(tx, "record_outcome", func() error {
			_, err := tx.Exec(`UPDATE steps SET state = ?, reason_code = ?, reason_data = ?,
				exit_code = ?, signal = ?,
				finished_at = CASE WHEN ? THEN ? ELSE finished_at END,
				duration_ms = CASE WHEN started_at IS NULL OR NOT ? THEN NULL
					ELSE ? - started_at END,
				log_path = ?, log_bytes = ?, log_truncated = ?, error_tail = ?,
				next_attempt_at = ?
			WHERE run_id = ? AND name = ? AND state = ?`,
				string(state), nullIfEmpty(string(out.ReasonCode)), detail,
				exitCode, signal,
				state != model.StepPending, finishedAt.UnixMilli(),
				state != model.StepPending,
				finishedAt.UnixMilli(),
				nullIfEmpty(out.LogMeta.RelPath), out.LogMeta.Bytes, truncated,
				nullIfEmpty(out.LogMeta.ErrorTail),
				nextAttempt,
				runID, name, string(cur))
			return err
		}, tx, RunEvent{
			RunID: runID, StepName: name, At: finishedAt, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			ReasonCode: string(out.ReasonCode),
			DetailJSON: detail,
		})
	})
	if err != nil {
		return "", err
	}
	return landed, nil
}

// FinishReason is how a run ends: the reason code the machine validated and
// the canonical detail object beside it, such as which step failed.
type FinishReason struct {
	Code reason.Code
	Data string
}

// FinishRun closes a run whose steps are all terminal. The aggregate comes
// from the machine's guards, which read the step rows inside the same
// transaction, so the verdict can never disagree with the steps it describes:
// any failed step fails the run, and a run with a step still open is refused
// outright. The lease is released here; the owner that lost it cannot finish
// anything.
func (s *Store) FinishRun(ctx context.Context, runID, owner string, fr FinishReason) (string, error) {
	now := s.clk.Now().UTC()
	next := ""

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}

		steps, err := readStepsTx(tx, runID)
		if err != nil {
			return err
		}
		allTerminal, anyFailed := true, false
		for _, step := range steps {
			st, err := model.ParseStepState(step.State)
			if err != nil {
				return err
			}
			if !st.IsTerminal() {
				allTerminal = false
			}
			if st == model.StepFailed || st == model.StepCancelled {
				anyFailed = true
			}
		}

		guards := model.Guards{
			LeaseValid:       run.LeaseOwner == owner && (run.LeaseExpiresAt.IsZero() || run.LeaseExpiresAt.After(now)),
			AllStepsTerminal: allTerminal,
			AnyStepFailed:    anyFailed,
			ReasonCode:       string(fr.Code),
			Now:              now.UnixMilli(),
			AvailableAt:      run.AvailableAt.UnixMilli(),
		}
		state, effects, err := model.NextRunState(cur, model.EvAllStepsDone, guards)
		if err != nil {
			return fmt.Errorf("finish run %s: %w", runID, err)
		}

		data := fr.Data
		if data == "" {
			data = "{}"
		}
		if err := finishTransition(tx, "finish_run", func() error {
			_, err := tx.Exec(`UPDATE runs SET state = ?, reason_code = ?, reason_data = ?,
				finished_at = ?, duration_ms = CASE WHEN started_at IS NULL THEN NULL
					ELSE ? - started_at END,
				lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE id = ? AND state = ?`,
				string(state), string(fr.Code), data, now.UnixMilli(), now.UnixMilli(),
				now.UnixMilli(), runID, string(cur))
			return err
		}, tx, RunEvent{
			RunID: runID, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			ReasonCode: string(fr.Code),
			DetailJSON: data,
		}); err != nil {
			return err
		}
		next = string(state)
		return nil
	})
	if err != nil {
		return "", err
	}
	return next, nil
}

// ObserveRunCancel effectuates a cancellation somebody requested earlier: the
// caller has already killed the process group outside any transaction (the
// machine lists that effect first, and the engine performs it before calling
// here), the steps that were running are cancelled by their own events, and
// this closes whatever pending steps remain and then the run itself.
//
// It refuses when nobody asked: a run nobody asked to cancel is never
// cancelled, whatever the caller claims. The event names the person who made
// the request, not the executor that observed it.
func (s *Store) ObserveRunCancel(ctx context.Context, runID, owner, actor string, code reason.Code) error {
	now := s.clk.Now().UTC()

	return s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		if run.CancelRequestedAt.IsZero() {
			return fmt.Errorf("observe the cancellation of run %s: nobody requested one", runID)
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}

		guards := model.Guards{
			LeaseValid: run.LeaseOwner == owner && (run.LeaseExpiresAt.IsZero() || run.LeaseExpiresAt.After(now)),
			ReasonCode: string(code),
		}
		state, effects, err := model.NextRunState(cur, model.EvCancelObserved, guards)
		if err != nil {
			return fmt.Errorf("observe the cancellation of run %s: %w", runID, err)
		}

		if err := skipPendingStepsTx(tx, runID, now, string(reason.STEPCancelled)); err != nil {
			return err
		}
		return finishTransition(tx, "observe_cancel", func() error {
			_, err := tx.Exec(`UPDATE runs SET state = 'cancelled',
				finished_at = ?, reason_code = ?, reason_data = ?,
				lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE id = ? AND state = ?`,
				now.UnixMilli(), string(code), "{}", now.UnixMilli(), runID, string(cur))
			return err
		}, tx, RunEvent{
			RunID: runID, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			ReasonCode: string(code),
			Actor:      actor,
		})
	})
}

// nextRunnableStepSQL is the claim predicate: the lowest index that is
// pending, past its retry gate, and whose every frozen upstream has
// succeeded. It is a constant so the query plan test asserts the plan of the
// exact statement that runs, not of a copy of it.
const nextRunnableStepSQL = `SELECT s.name FROM steps s
WHERE s.run_id = ? AND s.state = 'pending'
	AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= ?)
	AND NOT EXISTS (
		SELECT 1 FROM step_deps d JOIN steps up
			ON up.run_id = d.run_id AND up.name = d.depends_on
		WHERE d.run_id = s.run_id AND d.step_name = s.name AND up.state <> 'succeeded')
ORDER BY s.idx LIMIT 1`

// NextRunnableStep names the next step the engine may start: the lowest index
// that is pending, past its retry gate, and whose every frozen upstream has
// succeeded. A step waiting on upstream that has not succeeded is skipped
// over, which is the degenerated claim predicate M4-02 replaces with the
// whole graph.
func (s *Store) NextRunnableStep(ctx context.Context, runID string) (string, bool, error) {
	var name string
	err := s.r.QueryRowContext(ctx, nextRunnableStepSQL, runID,
		s.clk.Now().UTC().UnixMilli()).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("find the next runnable step of run %s: %w", runID, err)
	}
	return name, true, nil
}

// PendingSteps lists a run's steps that have not started, in index order.
func (s *Store) PendingSteps(ctx context.Context, runID string) ([]Step, error) {
	var out []Step
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT name, idx, state, attempt, max_attempts, exit_code,
signal, started_at, finished_at, duration_ms, reason_code, reason_text, reason_data, error,
log_path, log_bytes, log_truncated, error_tail, next_attempt_at
FROM steps WHERE run_id = ? AND state = 'pending' ORDER BY idx`, runID)
		if err != nil {
			return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
		}
		out, err = scanSteps(rows)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// skipPendingStepsTx moves every pending step of a run to skipped, each
// through the machine with its own event. It is what keeps a terminal run
// from sitting over open steps: a run ends, or its steps all do.
func skipPendingStepsTx(tx *sql.Tx, runID string, now time.Time, code string) error {
	rows, err := tx.Query(`SELECT name FROM steps WHERE run_id = ? AND state = 'pending' ORDER BY idx`, runID)
	if err != nil {
		return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
	}

	for _, name := range names {
		state, effects, err := model.NextStepState(model.StepPending, model.EvUpstreamFailed,
			model.Guards{ReasonCode: code})
		if err != nil {
			return fmt.Errorf("skip step %s of run %s: %w", name, runID, err)
		}
		if err := finishTransition(tx, "skip_pending", func() error {
			_, err := tx.Exec(`UPDATE steps SET state = 'skipped', finished_at = ?,
				reason_code = ?, reason_data = '{}'
			WHERE run_id = ? AND name = ? AND state = 'pending'`,
				now.UnixMilli(), code, runID, name)
			return err
		}, tx, RunEvent{
			RunID: runID, StepName: name, At: now, Kind: emitKind(effects),
			FromState: string(model.StepPending), ToState: string(state),
			ReasonCode: code,
		}); err != nil {
			return err
		}
	}
	return nil
}

// finishTransition pairs the state write with the event write and refuses to
// commit one without the other. fn performs the UPDATE; if it touched no row,
// the transition never happened and the event is not written either.
//
// where names the transition for the crash harness (#75). The fault point
// between the two writes is the exact window G10 closes: a process killed
// there loses both writes, because neither was committed. In a build without
// the pulseq_faults tag the call folds away to nothing.
func finishTransition(tx *sql.Tx, where string, fn func() error, txForEvent *sql.Tx, e RunEvent) error {
	if err := fn(); err != nil {
		return fmt.Errorf("record %s on run %s: %w", e.Kind, e.RunID, err)
	}
	faults.Point("M1:transition:after_update:" + where)
	return appendRunEvent(txForEvent, e)
}

// emitKind reads the event name the machine put in its emit effect. A
// transition without one is a machine bug, not a caller's.
func emitKind(effects []model.Effect) string {
	for _, fx := range effects {
		if fx.Kind == model.EffectEmit {
			return fx.Arg
		}
	}
	return ""
}

// readRunTx reads one run by its whole id inside a transaction. Prefixes are
// a read-side convenience; a writer names the row it changes in full.
func readRunTx(tx *sql.Tx, runID string) (Run, error) {
	var run Run
	var (
		trigger, runKey, concurrency, params sql.NullString
		deferReason, reasonCode, reasonText  sql.NullString
		reasonData, failure                  sql.NullString
		leaseOwner, cancelBy, cancelWhy      sql.NullString
		scheduledFor, startedAt, finishedAt  sql.NullInt64
		leaseExpiresAt, cancelRequestedAt    sql.NullInt64
		availableAt, createdAt, updatedAt    int64
		leaseEpoch                           int64
	)
	err := tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID).Scan(
		&run.ID, &run.JobName, &run.JobVersionID, &trigger, &run.Origin,
		&runKey, &run.State, &concurrency, &availableAt, &deferReason, &scheduledFor, &params,
		&run.Attempt, &run.MaxAttempts, &leaseOwner, &leaseEpoch, &leaseExpiresAt,
		&cancelRequestedAt, &cancelBy, &cancelWhy, &reasonCode, &reasonText, &reasonData,
		&failure, &createdAt, &startedAt, &finishedAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("look up run %q: %w", runID, ErrRunNotFound)
	}
	if err != nil {
		return Run{}, fmt.Errorf("look up run %q: %w", runID, err)
	}
	run.TriggerID = trigger.String
	run.RunKey = runKey.String
	run.ConcurrencyKey = concurrency.String
	run.DeferReason = deferReason.String
	run.ParamsJSON = params.String
	run.ReasonCode = reasonCode.String
	run.ReasonText = reasonText.String
	run.ReasonData = reasonData.String
	run.Error = failure.String
	run.LeaseOwner = leaseOwner.String
	run.LeaseEpoch = leaseEpoch
	run.LeaseExpiresAt = timeOrZero(leaseExpiresAt)
	if cancelRequestedAt.Valid {
		run.CancelRequestedAt = time.UnixMilli(cancelRequestedAt.Int64).UTC()
	}
	run.CancelRequestedBy = cancelBy.String
	run.CancelReason = cancelWhy.String
	run.AvailableAt = time.UnixMilli(availableAt).UTC()
	run.ScheduledFor = timeOrZero(scheduledFor)
	run.CreatedAt = time.UnixMilli(createdAt).UTC()
	run.StartedAt = timeOrZero(startedAt)
	run.FinishedAt = timeOrZero(finishedAt)
	run.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	return run, nil
}

// stepRow is the columns readStepTx needs, in the order the steps projection
// names them.
func readStepTx(tx *sql.Tx, runID, name string) (Step, error) {
	var step Step
	var (
		exitCode      sql.NullInt64
		signal        sql.NullString
		reasonCode    sql.NullString
		reasonText    sql.NullString
		reasonData    sql.NullString
		failure       sql.NullString
		logs          sql.NullString
		errorTail     sql.NullString
		startedAt     sql.NullInt64
		finishedAt    sql.NullInt64
		durationMS    sql.NullInt64
		nextAttemptAt sql.NullInt64
	)
	err := tx.QueryRow(`SELECT name, idx, state, attempt, max_attempts, exit_code, signal,
started_at, finished_at, duration_ms, reason_code, reason_text, reason_data, error,
log_path, log_bytes, log_truncated, error_tail, next_attempt_at
FROM steps WHERE run_id = ? AND name = ?`, runID, name).Scan(
		&step.Name, &step.Index, &step.State, &step.Attempt, &step.MaxAttempts,
		&exitCode, &signal, &startedAt, &finishedAt, &durationMS, &reasonCode, &reasonText,
		&reasonData, &failure, &logs, &step.LogBytes, &step.LogTruncated, &errorTail,
		&nextAttemptAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Step{}, fmt.Errorf("look up step %s of run %s: %w", name, runID, ErrRunNotFound)
	}
	if err != nil {
		return Step{}, fmt.Errorf("look up step %s of run %s: %w", name, runID, err)
	}
	step.ExitCode = int(exitCode.Int64)
	step.HasExitCode = exitCode.Valid
	step.Signal = signal.String
	step.StartedAt = timeOrZero(startedAt)
	step.FinishedAt = timeOrZero(finishedAt)
	step.DurationMS = durationMS.Int64
	step.ReasonCode = reasonCode.String
	step.ReasonText = reasonText.String
	step.ReasonData = reasonData.String
	step.Error = failure.String
	step.LogPath = logs.String
	step.ErrorTail = errorTail.String
	step.NextAttemptAt = timeOrZero(nextAttemptAt)
	return step, nil
}

// readStepsTx is readSteps for inside a transaction, so FinishRun judges the
// steps from the same snapshot it writes the verdict into.
func readStepsTx(tx *sql.Tx, runID string) ([]Step, error) {
	rows, err := tx.Query(`SELECT name, idx, state, attempt, max_attempts, exit_code, signal,
started_at, finished_at, duration_ms, reason_code, reason_text, reason_data, error,
log_path, log_bytes, log_truncated, error_tail, next_attempt_at
FROM steps WHERE run_id = ? ORDER BY idx`, runID)
	if err != nil {
		return nil, fmt.Errorf("read the steps of run %s: %w", runID, err)
	}
	return scanSteps(rows)
}

// RequeueCrashedRun puts a run whose executor died back in the queue. It is
// the store half of the restart story the crash harness (#75) proves: a run
// left running by a SIGKILL is requeued through the machine's own
// lease_expired transition, never reset behind its back.
//
// The refusal is the safety catch. A lease that has not expired yet belongs
// to a process that may still be alive, and requeuing under it would let two
// executors drive one run. Only an expired lease counts as evidence that the
// previous owner is gone; the state directory lock adds its own guarantee on
// this platform, but the machine's guard does not rely on it.
//
// The transition writes what the machine demands: the epoch goes up so any
// writer from the dead attempt is fenced out, crash_count counts the loss of
// the executor against the run, and defer_reason records why the requeued run
// sits waiting (I14). One event row tells the story, in the same transaction.
func (s *Store) RequeueCrashedRun(ctx context.Context, runID string) error {
	now := s.clk.Now().UTC()

	return s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}

		expired := !run.LeaseExpiresAt.IsZero() && !run.LeaseExpiresAt.After(now)
		if cur != model.RunRunning || !expired {
			return fmt.Errorf("requeue run %s: the lease is not expired"+
				" (state %s, expires at %s)", runID, run.State, run.LeaseExpiresAt)
		}

		state, effects, err := model.NextRunState(cur, model.EvLeaseExpired, model.Guards{
			LeaseValid:      false,
			CrashBudgetLeft: true,
		})
		if err != nil {
			return fmt.Errorf("requeue run %s: %w", runID, err)
		}
		if state != model.RunQueued {
			// M1 carries no poison budget, so the machine always
			// requeues here. A refusal to believe that is a bug,
			// not a state.
			return fmt.Errorf("requeue run %s: the machine sent a running run to %s", runID, state)
		}

		return finishTransition(tx, "requeue_crashed", func() error {
			_, err := tx.Exec(`UPDATE runs SET state = 'queued',
				lease_owner = NULL, lease_expires_at = NULL,
				lease_epoch = lease_epoch + 1, crash_count = crash_count + 1,
				defer_reason = ?, available_at = ?, updated_at = ?
			WHERE id = ? AND state = 'running'`,
				model.DeferReasonAfterCrash, now.UnixMilli(), now.UnixMilli(), runID)
			return err
		}, tx, RunEvent{
			RunID: runID, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			DetailJSON: `{"defer_reason":"` + model.DeferReasonAfterCrash + `"}`,
		})
	})
}

// The fault injection writes. Fsck's negative proofs (#75) plant broken rows
// behind the machines' backs, and these five statements are the only doors
// for that: each is named for the exact corruption it writes, and nothing in
// the engine or the CLI may call them. They sit here, beside the real state
// writes, so the architecture's one rule stays true: a run or step row only
// ever changes in this file, even when the change is a deliberate lie.

// plantStepPendingTx flips one step of a terminal run back to pending: the
// row I2 exists for.
func plantStepPendingTx(tx *sql.Tx, runID, step string) error {
	_, err := tx.Exec(`UPDATE steps SET state = 'pending', reason_code = NULL
WHERE run_id = ? AND name = ?`, runID, step)
	return err
}

// plantFirstSucceededStepFailedTx marks a run's first succeeded step failed
// while the run still says succeeded: the disagreement I10 exists for.
func plantFirstSucceededStepFailedTx(tx *sql.Tx, runID string) error {
	_, err := tx.Exec(`UPDATE steps SET state = 'failed'
WHERE run_id = ? AND state = 'succeeded'`, runID)
	return err
}

// plantStepFinishedBeforeStartedTx moves every finished step's end before
// its beginning: the shape I13 refuses.
func plantStepFinishedBeforeStartedTx(tx *sql.Tx) error {
	_, err := tx.Exec(`UPDATE steps SET finished_at = started_at - 1
WHERE started_at IS NOT NULL AND finished_at IS NOT NULL`)
	return err
}

// plantUnexplainedDeferralTx pushes a queued run's availability into the
// future and clears its defer_reason: a run held back that says no more why.
// The CHECK constraint refuses this shape, so the caller lifts the checks
// around the call.
func plantUnexplainedDeferralTx(tx *sql.Tx, runID string) error {
	_, err := tx.Exec(`UPDATE runs SET available_at = created_at + 3600000,
		defer_reason = NULL WHERE id = ?`, runID)
	return err
}

// plantUnexplainedTerminalRunTx clears a terminal run's reason code: the
// catalogue rule swept along with the invariants.
func plantUnexplainedTerminalRunTx(tx *sql.Tx, runID string) error {
	_, err := tx.Exec(`UPDATE runs SET reason_code = '' WHERE id = ?`, runID)
	return err
}
