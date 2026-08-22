package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/runner"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// ExecuteRun takes one queued run and drives it to a terminal state: claim,
// then steps one at a time in index order, each requiring its every upstream
// step to have succeeded, then the run's own verdict.
//
// The shape of the loop is where the two hard rules live. Every state change
// is a store method, and each of those commits the state change with exactly
// one event row. The process runs strictly between those transactions: the
// transaction that starts the step is closed before the command is spawned,
// and the transaction that records its verdict is opened after it is reaped.
//
// The spec is read once, from job_versions.spec_json, before anything runs.
// A job file applied while the run executes changes nothing here; the run was
// frozen when it was materialised.
func (e *Engine) ExecuteRun(ctx context.Context, runID string) (string, error) {
	detail, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return "", fmt.Errorf("execute run %s: %w", runID, err)
	}
	runID = detail.Run.ID

	job, err := e.frozenSpec(ctx, detail.Run.JobVersionID)
	if err != nil {
		return "", err
	}
	stepsByName := make(map[string]spec.Step, len(job.Steps))
	for _, st := range job.Steps {
		stepsByName[st.Name] = st
	}

	var deadline time.Time
	if job.Timeout > 0 {
		deadline = e.Clock.Now().Add(job.Timeout)
	}

	state, err := e.Store.ClaimRun(ctx, runID, store.LeaseInput{Owner: e.Owner, TTL: e.LeaseTTL})
	if err != nil {
		return "", fmt.Errorf("execute run %s: %w", runID, err)
	}
	if state == string(model.RunCancelled) {
		// The cancellation arrived before the run ever started; there is
		// no process group to kill and nothing else to do.
		return state, nil
	}

	timedOut := false
	for {
		// Between steps is where a cancellation is cheapest to observe:
		// nothing is running, so nothing has to be killed.
		requested, by, err := e.Store.CancelRequested(ctx, runID)
		if err != nil {
			return "", fmt.Errorf("execute run %s: %w", runID, err)
		}
		if requested {
			return e.observeCancel(ctx, runID, by)
		}

		name, ok, err := e.Store.NextRunnableStep(ctx, runID)
		if err != nil {
			return "", fmt.Errorf("execute run %s: %w", runID, err)
		}
		if !ok {
			break
		}

		stepHitRunDeadline, err := e.runStep(ctx, detail.Run, name, stepsByName[name], job, deadline)
		if err != nil {
			return "", err
		}
		if stepHitRunDeadline {
			// The run's own budget is spent. Whatever else is pending
			// cannot fit inside it either, so the loop ends and the
			// verdict below is TIMED_OUT.
			timedOut = true
			break
		}
	}

	// Whatever is still pending never became runnable: some upstream ended
	// failed, cancelled or skipped, so it never will be. Each leaves
	// through the machine's skip transition, with its own event, in index
	// order.
	if err := e.skipPending(ctx, runID); err != nil {
		return "", err
	}

	finish, err := e.finishReason(ctx, runID, timedOut)
	if err != nil {
		return "", err
	}
	return e.Store.FinishRun(ctx, runID, e.Owner, finish)
}

// skipPending closes out every step still waiting, in index order.
func (e *Engine) skipPending(ctx context.Context, runID string) error {
	pending, err := e.Store.PendingSteps(ctx, runID)
	if err != nil {
		return fmt.Errorf("skip the pending steps of run %s: %w", runID, err)
	}
	now := e.Clock.Now()
	for _, p := range pending {
		_, err := e.Store.RecordStepOutcome(ctx, runID, p.Name, store.StepOutcome{
			Event:      string(model.EvUpstreamFailed),
			ReasonCode: reason.STEPSkippedUpstreamFailed,
			FinishedAt: now,
		})
		if err != nil {
			return fmt.Errorf("skip step %s of run %s: %w", p.Name, runID, err)
		}
	}
	return nil
}

// frozenSpec reads the version this run points at out of the database and
// decodes it. It is called once per execution; everything downstream works
// from what it returned.
func (e *Engine) frozenSpec(ctx context.Context, versionID string) (*spec.Job, error) {
	version, err := e.Store.JobVersionByID(ctx, versionID)
	if err != nil {
		return nil, fmt.Errorf("read the frozen spec: %w", err)
	}
	job, err := spec.FromIR([]byte(version.SpecJSON))
	if err != nil {
		return nil, fmt.Errorf("decode the frozen spec of version %s: %w", versionID, err)
	}
	return job, nil
}

// runStep carries one step from pending to terminal: start it in one
// transaction, run the process outside any transaction, record the verdict in
// another. The bool reports whether the step ended because the run's own
// deadline ran out, which is the engine's signal to stop scheduling work.
func (e *Engine) runStep(ctx context.Context, run store.Run, name string, st spec.Step, job *spec.Job, deadline time.Time) (bool, error) {
	fail := func(err error) (bool, error) {
		return false, fmt.Errorf("run step %s of run %s: %w", name, run.ID, err)
	}

	current, err := e.Store.GetRun(ctx, run.ID)
	if err != nil {
		return fail(err)
	}
	var before store.Step
	for _, s := range current.Steps {
		if s.Name == name {
			before = s
			break
		}
	}

	if err := e.Store.StartStep(ctx, run.ID, name); err != nil {
		return fail(err)
	}
	attempt := before.Attempt + 1

	sink, err := logsink.Open(e.LogRoot, run.ID, name, attempt,
		logsink.Options{Clock: e.Clock})
	if err != nil {
		return fail(fmt.Errorf("open the log: %w", err))
	}

	// The crash window between the committed start and the spawn. A
	// process killed here leaves a running step whose command never ran,
	// which is the safest window to recover: retrying cannot duplicate an
	// effect that never happened.
	faults.Point("M1:step:before_exec")

	timeout := st.Timeout
	if timeout <= 0 {
		timeout = runner.DefaultTimeout
	}
	runDeadlineHit := false
	if !deadline.IsZero() {
		if remaining := deadline.Sub(e.Clock.Now()); remaining < timeout {
			timeout = remaining
			runDeadlineHit = true
		}
	}

	// The poll: while the process runs, its cancellation request is
	// re-read on the clock. Seeing one cancels the step's context, and
	// the runner answers a dead context by killing the whole process
	// group. No sleep touches the wall clock directly.
	watchCtx, cancelWatch := context.WithCancel(ctx)
	cancelledBy := make(chan string, 1)
	go func() {
		defer close(cancelledBy)
		ticker := e.Clock.NewTicker(e.pollInterval())
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				requested, by, err := e.Store.CancelRequested(watchCtx, run.ID)
				if err != nil || !requested {
					continue
				}
				// The request is durable in the database; killing is
				// the answer to it. The context cancel is what takes
				// the process group down.
				cancelWatch()
				select {
				case cancelledBy <- by:
				default:
				}
				return
			}
		}
	}()

	result, runErr := runner.Run(watchCtx, runner.Spec{
		Argv:       st.Run,
		Shell:      st.Shell,
		Workdir:    st.Workdir,
		Env:        job.Env,
		InheritEnv: job.InheritEnv,
		Timeout:    timeout,
		Clock:      e.Clock,
		Stdout:     crashOnFirstWrite(sink.Writer(logsink.StreamStdout), "M1:step:under_exec"),
		Stderr:     sink.Writer(logsink.StreamStderr),
		Ctx: runner.RunContext{
			RunID:   run.ID,
			Job:     run.JobName,
			Step:    name,
			Attempt: attempt,
			RunKey:  run.RunKey,
			Params:  paramsMap(run.ParamsJSON),
		},
	})
	cancelWatch()
	by := <-cancelledBy

	tail, bytes, truncated, err := sink.Finish()
	if err != nil {
		return fail(fmt.Errorf("close the log: %w", err))
	}
	// The crash window between a finalized log file and the metadata write
	// that records it. The bytes are on disk, the database knows nothing
	// yet; recovery has to leave both sides telling the same story.
	faults.Point("M1:step:after_log_finish")
	if runErr != nil {
		return fail(fmt.Errorf("the process could not start: %w", runErr))
	}

	outcome := verdictFor(result, tail, bytes, truncated)
	outcome.LogMeta.RelPath = sink.RelPath()
	switch {
	case outcome.Event == string(model.EvCancelObserved):
		// The kill happened outside any transaction, as it must. What
		// remains is bookkeeping, and the bookkeeping names who asked.
		outcome.DetailJSON = detailJSON(map[string]any{"requested_by": by})
	case result.Outcome == runner.TimedOut && runDeadlineHit:
		outcome.DetailJSON = detailJSON(map[string]any{
			"scope":      "run",
			"timeout_ms": timeout.Milliseconds(),
		})
	}

	state, err := e.Store.RecordStepOutcome(ctx, run.ID, name, outcome)
	if err != nil {
		return fail(fmt.Errorf("record the verdict: %w", err))
	}
	if state == model.StepPending {
		// The crash window between attempts: the failed attempt's
		// verdict and its retry schedule are committed, and the next
		// attempt has not started. A process killed here leaves a
		// durable retry that the restart must pick up exactly once.
		faults.Point("M1:step:between_attempts")
	}
	return runDeadlineHit && result.Outcome == runner.TimedOut, nil
}

// observeCancel closes a run whose cancellation somebody requested: the
// pending steps leave through their own events and the run follows, both
// inside the store method's one transaction.
func (e *Engine) observeCancel(ctx context.Context, runID, by string) (string, error) {
	err := e.Store.ObserveRunCancel(ctx, runID, e.Owner, by, reason.RUNCancelledManual)
	if err != nil {
		return "", fmt.Errorf("cancel run %s: %w", runID, err)
	}
	return string(model.RunCancelled), nil
}

// finishReason decides why the run ended. A step failure fails the run and
// names the step; a spent run budget ends it as TIMED_OUT; a cancelled step
// means the run was cancelled; otherwise the run succeeded, a skip counting
// as success.
func (e *Engine) finishReason(ctx context.Context, runID string, timedOut bool) (store.FinishReason, error) {
	detail, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return store.FinishReason{}, fmt.Errorf("finish run %s: %w", runID, err)
	}
	if timedOut {
		return store.FinishReason{Code: reason.RUNTimedOut}, nil
	}
	for _, s := range detail.Steps {
		switch model.StepState(s.State) {
		case model.StepFailed:
			return store.FinishReason{
				Code: reason.RUNFailedStep,
				Data: detailJSON(map[string]any{"step": s.Name}),
			}, nil
		case model.StepCancelled:
			return store.FinishReason{Code: reason.RUNCancelledManual}, nil
		}
	}
	return store.FinishReason{Code: reason.RUNSucceeded}, nil
}

// verdictFor translates what the runner observed into the step machine's
// vocabulary. The mapping is total: every Outcome the runner can report has a
// row here, so an unclassified result cannot reach the store. The log's
// relative path is filled in by the caller, which is the one holding the sink.
func verdictFor(res runner.Result, tail string, bytes int64, truncated bool) store.StepOutcome {
	out := store.StepOutcome{
		LogMeta:    store.LogMeta{Bytes: bytes, Truncated: truncated, ErrorTail: tail},
		FinishedAt: msToTime(res.FinishedAt),
	}
	code := res.ExitCode
	switch res.Outcome {
	case runner.Succeeded:
		out.Event = string(model.EvStepSucceeded)
		out.ReasonCode = reason.STEPSucceeded
		out.ExitCode = &code
	case runner.Failed:
		out.Event = string(model.EvStepFailed)
		out.ReasonCode = reason.STEPFailedNonzeroExit
		out.ExitCode = &code
		out.DetailJSON = detailJSON(res.ReasonData)
	case runner.TimedOut:
		out.Event = string(model.EvStepFailed)
		out.ReasonCode = reason.STEPFailedTimeout
		out.Signal = res.Signal
		out.DetailJSON = detailJSON(res.ReasonData)
	case runner.Signalled:
		if res.ReasonData["cancelled"] == true {
			// The signal came from us answering a cancellation.
			out.Event = string(model.EvCancelObserved)
			out.ReasonCode = reason.STEPCancelled
		} else {
			out.Event = string(model.EvStepFailed)
			out.ReasonCode = reason.STEPFailedSignal
			out.Signal = res.Signal
		}
		out.DetailJSON = detailJSON(res.ReasonData)
	default:
		// SpawnFailed: the process never existed, so nothing about the
		// job failed; the launch did.
		out.Event = string(model.EvStepFailed)
		out.ReasonCode = reason.STEPFailedSpawn
		out.DetailJSON = detailJSON(res.ReasonData)
	}
	return out
}

// paramsMap decodes a run's parameter object for the runner's environment
// contract. Empty or malformed JSON is no parameters; the store only ever
// writes what materialisation canonicalised, so this is a formality.
func paramsMap(paramsJSON string) map[string]any {
	if paramsJSON == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(paramsJSON), &m); err != nil {
		return nil
	}
	return m
}

// detailJSON renders a detail object canonically. encoding/json sorts map
// keys, so the same facts always read the same way back.
func detailJSON(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func msToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// crashOnFirstWrite arms the under_exec crash point on a step's stdout. The
// point fires while the command is alive and producing output, which is what
// makes the kill land inside execution rather than beside it: the first line
// a job prints proves a process exists to print it. In a build without the
// pulseq_faults tag Point does nothing and the wrapper is pass through.
type firstWriteWriter struct {
	next io.Writer
	name string
}

func crashOnFirstWrite(next io.Writer, name string) io.Writer {
	return &firstWriteWriter{next: next, name: name}
}

func (w *firstWriteWriter) Write(p []byte) (int, error) {
	faults.Point(w.name)
	return w.next.Write(p)
}
