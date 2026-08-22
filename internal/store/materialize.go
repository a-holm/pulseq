package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/spec"
)

// ErrNotFound wraps every refusal of this file that means "no such row". A
// caller that only cares whether something existed matches this; one that
// cares which table it asked about reads the message.
var ErrNotFound = errors.New("no such row")

// ManualTriggerInput is one person's decision to run a job now.
type ManualTriggerInput struct {
	// JobName is the job whose current version runs.
	JobName string

	// Actor is who typed the command, recorded on the queued event and on
	// the tick's session trail. Empty becomes "system".
	Actor string

	// ParamsJSON is the parameter object for the run, empty for none.
	ParamsJSON string
}

// ManualTriggerResult names everything one manual decision created. The three
// rows are born in one transaction, so any later reader sees all of them or
// none of them.
type ManualTriggerResult struct {
	TickID    string
	TriggerID string
	Run       Run
}

// MaterializeManualTrigger records a manual decision end to end: the tick, the
// trigger and the run with its steps and edges, in one transaction. It is the
// same chain a schedule or a sensor produces, which is what keeps explain
// complete for hand started runs from day one and lets M2 and M3 add new
// source kinds without a second code path.
//
// The steps come out of job_versions.spec_json, the immutable version the job
// currently points at. Nothing re-reads the YAML file, so an apply that lands
// after the decision cannot change what this run does.
//
// A paused job still runs here. A pause governs automatic firing; a person
// typing the command has already decided.
func (s *Store) MaterializeManualTrigger(ctx context.Context, in ManualTriggerInput) (ManualTriggerResult, error) {
	// Ids are minted before the transaction opens: each costs a read of the
	// system entropy source, and the write model forbids that while the
	// write lock is held.
	now := s.clk.Now().UTC()
	at := now.UnixMilli()
	tickID, err := id.New(now)
	if err != nil {
		return ManualTriggerResult{}, fmt.Errorf("mint a tick id: %w", err)
	}
	triggerID, err := id.New(now)
	if err != nil {
		return ManualTriggerResult{}, fmt.Errorf("mint a trigger id: %w", err)
	}
	runID, err := id.New(now)
	if err != nil {
		return ManualTriggerResult{}, fmt.Errorf("mint a run id: %w", err)
	}

	params := in.ParamsJSON
	if params == "" {
		params = "{}"
	}
	actor := in.Actor
	if actor == "" {
		actor = "system"
	}

	out := ManualTriggerResult{TickID: tickID, TriggerID: triggerID}
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		// The version is chosen inside the transaction, so two decisions
		// racing an apply still each freeze one whole version rather than
		// a mix of two.
		var versionID, specJSON string
		err := tx.QueryRow(`SELECT j.current_version_id, v.spec_json
FROM jobs j JOIN job_versions v ON v.id = j.current_version_id
WHERE j.name = ?`, in.JobName).Scan(&versionID, &specJSON)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("materialise %s: no job version is current: %w", in.JobName, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("find the current version of job %s: %w", in.JobName, err)
		}

		job, err := spec.FromIR([]byte(specJSON))
		if err != nil {
			return fmt.Errorf("read the frozen spec of job %s (version %s): %w", in.JobName, versionID, err)
		}

		// One tick per decision. Manual ticks leave scheduled_for NULL,
		// which the unique index treats as always distinct, so nobody is
		// ever refused their turn at the keyboard.
		if _, err := tx.Exec(`INSERT INTO ticks
(id, source_kind, source_name, started_at, last_started_at, outcome, trigger_count)
VALUES (?, 'manual', ?, ?, ?, 'triggered', 1)`, tickID, in.JobName, at, at); err != nil {
			return fmt.Errorf("record the manual tick for job %s: %w", in.JobName, err)
		}
		faults.Point("M1:materialize:after_tick")
		if _, err := tx.Exec(`INSERT INTO triggers
(id, tick_id, job_name, params_json, created_at, outcome)
VALUES (?, ?, ?, ?, ?, 'accepted')`, triggerID, tickID, in.JobName, params, at); err != nil {
			return fmt.Errorf("record the manual trigger for job %s: %w", in.JobName, err)
		}
		faults.Point("M1:materialize:after_trigger")

		run := Run{
			ID:           runID,
			JobName:      in.JobName,
			JobVersionID: versionID,
			TriggerID:    triggerID,
			Origin:       "manual",
			State:        "queued",
			AvailableAt:  now,
			ScheduledFor: time.Time{},
			ParamsJSON:   params,
			MaxAttempts:  1,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if _, err := tx.Exec(`INSERT INTO runs
(id, job_name, job_version_id, trigger_id, origin, state, available_at,
 params_json, max_attempts, created_at, updated_at)
VALUES (?, ?, ?, ?, 'manual', 'queued', ?, ?, 1, ?, ?)`,
			run.ID, run.JobName, run.JobVersionID, nullIfEmpty(run.TriggerID),
			run.AvailableAt.UnixMilli(), run.ParamsJSON, at, at); err != nil {
			return fmt.Errorf("create a run of job %s: %w", run.JobName, err)
		}

		if err := insertSteps(tx, run.ID, job.Steps); err != nil {
			return err
		}
		faults.Point("M1:materialize:after_run")

		out.Run = run
		return appendRunEvent(tx, RunEvent{
			RunID:   run.ID,
			At:      now,
			Kind:    "run.queued",
			ToState: "queued",
			Actor:   actor,
		})
	})
	if err != nil {
		return ManualTriggerResult{}, err
	}
	return out, nil
}

// insertSteps writes one pending step per spec step, in spec order, together
// with its frozen dependency edges. Shared by every materialisation path so
// the M1 order and the M4 graph can never disagree about what a step row
// looks like.
func insertSteps(tx *sql.Tx, runID string, steps []spec.Step) error {
	for i, step := range steps {
		maxAttempts := 1
		if step.Retry != nil && step.Retry.Max >= 0 {
			maxAttempts = step.Retry.Max + 1
		}
		if _, err := tx.Exec(`INSERT INTO steps (run_id, name, idx, state, max_attempts)
VALUES (?, ?, ?, 'pending', ?)`, runID, step.Name, i, maxAttempts); err != nil {
			return fmt.Errorf("create step %s of run %s: %w", step.Name, runID, err)
		}
		for _, upstream := range step.Needs {
			if _, err := tx.Exec(`INSERT INTO step_deps (run_id, step_name, depends_on)
VALUES (?, ?, ?)`, runID, step.Name, upstream); err != nil {
				return fmt.Errorf("freeze the edge %s -> %s of run %s: %w",
					upstream, step.Name, runID, err)
			}
		}
	}
	return nil
}

// CurrentJobVersion returns the version a job currently points at. This is a
// read outside any transaction: a caller that decides based on it races an
// apply by design, and the callers that must not race choose versions inside
// MaterializeManualTrigger instead.
func (s *Store) CurrentJobVersion(ctx context.Context, jobName string) (JobVersion, error) {
	row := s.r.QueryRowContext(ctx, `SELECT id, job_name, version, spec_hash, spec_json, source_path, created_at
FROM job_versions WHERE id = (SELECT current_version_id FROM jobs WHERE name = ?)`, jobName)
	return scanJobVersion(row, fmt.Sprintf("find the current version of job %s", jobName))
}

// JobVersionByID returns exactly the version named, however old. This is how
// execution reads the bytes a run was frozen with.
func (s *Store) JobVersionByID(ctx context.Context, versionID string) (JobVersion, error) {
	row := s.r.QueryRowContext(ctx, `SELECT id, job_name, version, spec_hash, spec_json, source_path, created_at
FROM job_versions WHERE id = ?`, versionID)
	return scanJobVersion(row, fmt.Sprintf("look up job version %s", versionID))
}

func scanJobVersion(row *sql.Row, what string) (JobVersion, error) {
	var v JobVersion
	var source sql.NullString
	var createdAt int64
	err := row.Scan(&v.ID, &v.JobName, &v.Version, &v.SpecHash, &v.SpecJSON, &source, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return JobVersion{}, fmt.Errorf("%s: %w", what, ErrNotFound)
	}
	if err != nil {
		return JobVersion{}, fmt.Errorf("%s: %w", what, err)
	}
	v.SourcePath = source.String
	v.CreatedAt = time.UnixMilli(createdAt).UTC()
	return v, nil
}

// StepDep is one edge as it was frozen when the run was materialised. M1 runs
// steps in index order and never reads these; they exist so the record of why
// a run waited survives, and so M4 inherits its graph from rows rather than
// from a spec that may since have changed.
type StepDep struct {
	RunID     string
	StepName  string
	DependsOn string
}

// StepDeps lists a run's frozen edges, ordered the way explain prints them.
func (s *Store) StepDeps(ctx context.Context, runID string) ([]StepDep, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT run_id, step_name, depends_on
FROM step_deps WHERE run_id = ? ORDER BY step_name, depends_on`, runID)
	if err != nil {
		return nil, fmt.Errorf("list the edges of run %s: %w", runID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []StepDep
	for rows.Next() {
		var d StepDep
		if err := rows.Scan(&d.RunID, &d.StepName, &d.DependsOn); err != nil {
			return nil, fmt.Errorf("scan an edge of run %s: %w", runID, err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list the edges of run %s: %w", runID, err)
	}
	return out, nil
}

// RunEvents returns a run's transition history, oldest first. This is the
// explain backbone: one row per state change, written in the transaction that
// made the change.
func (s *Store) RunEvents(ctx context.Context, idOrPrefix string) ([]RunEvent, error) {
	span, err := id.PrefixRange(idOrPrefix)
	if err != nil {
		return nil, fmt.Errorf("look up events for run %q: %w", idOrPrefix, err)
	}
	rows, err := s.r.QueryContext(ctx, `SELECT run_id, step_name, at, kind, from_state, to_state, reason_code, actor, detail_json
FROM run_events WHERE run_id >= ? AND run_id < ? ORDER BY id`, span.Lower, span.Upper)
	if err != nil {
		return nil, fmt.Errorf("list events for run %q: %w", idOrPrefix, err)
	}
	defer func() { _ = rows.Close() }()

	var out []RunEvent
	for rows.Next() {
		var (
			e         RunEvent
			stepName  sql.NullString
			at        int64
			fromState sql.NullString
			toState   sql.NullString
			reason    sql.NullString
		)
		if err := rows.Scan(&e.RunID, &stepName, &at, &e.Kind, &fromState, &toState,
			&reason, &e.Actor, &e.DetailJSON); err != nil {
			return nil, fmt.Errorf("scan an event of run %q: %w", idOrPrefix, err)
		}
		e.StepName = stepName.String
		e.At = time.UnixMilli(at).UTC()
		e.FromState = fromState.String
		e.ToState = toState.String
		e.ReasonCode = reason.String
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list events for run %q: %w", idOrPrefix, err)
	}
	return out, nil
}

// SetJobPaused flips the pause flag. It exists for the doctor and the CLI
// pause command; tests use it to stage paused jobs.
func (s *Store) SetJobPaused(ctx context.Context, jobName string, paused bool) error {
	flag := 0
	if paused {
		flag = 1
	}
	result, err := s.w.ExecContext(ctx, `UPDATE jobs SET paused = ?, updated_at = ? WHERE name = ?`,
		flag, s.clk.Now().UTC().UnixMilli(), jobName)
	if err != nil {
		return fmt.Errorf("set paused on job %s: %w", jobName, err)
	}
	written, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set paused on job %s: %w", jobName, err)
	}
	if written == 0 {
		return fmt.Errorf("set paused on job %s: %w", jobName, ErrNotFound)
	}
	return nil
}

// JobPaused reports whether a job is paused. An unknown job is ErrNotFound.
func (s *Store) JobPaused(ctx context.Context, jobName string) (bool, error) {
	var paused int
	err := s.r.QueryRowContext(ctx, `SELECT paused FROM jobs WHERE name = ?`, jobName).Scan(&paused)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read the pause flag of job %s: %w", jobName, ErrNotFound)
	}
	if err != nil {
		return false, fmt.Errorf("read the pause flag of job %s: %w", jobName, err)
	}
	return paused == 1, nil
}
