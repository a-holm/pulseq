package crash

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The matrix. Every row: crash a real child process, prove the death was
// SIGKILL, reopen the database, require integrity, zero fsck violations and
// no abandoned chain, converge the run through recovery, and hold the effect
// count to its bound. A row that cannot name what it proves is not in the
// matrix.
func TestCrashMatrix(t *testing.T) {
	for _, sc := range allRows() {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			runRow(t, sc)
		})
	}
}

func runRow(t *testing.T, sc Scenario) {
	t.Helper()

	ws := newWorkspace(t)
	ctx := context.Background()

	out, killed := runChild(t, ws, sc, sc.KillAt, nil)
	if sc.KillAt == "" || sc.KillAt == unknownPoint {
		requireChildSurvived(t, sc, out, killed)
	} else {
		requireChildKilled(t, sc, out, killed)
	}

	runID := childRunID(sc, out)
	if sc.Kind == "execute" && runID == "" {
		t.Fatalf("%s: the child printed no run id\n%s", sc.describe(), out)
	}

	s := openStore(t, ws)
	defer closeStore(t, s)

	requireIntegrity(t, ctx, s, "after the crash")
	requireFsckFindings(t, ctx, s, "after the crash", sc.TransientFindings)
	requireNoAbandonedChains(t, ctx, s, "after the crash")

	if sc.WaitsForOrphan && sc.KillAt != "" {
		waitOrphansGone(t, ws.EffectFile)
	}

	finalState, finalRunID := converge(t, ctx, s, ws, sc, runID)
	if !contains(sc.allowedFinalStates(), finalState) {
		t.Fatalf("%s: the restart converged to %q, want one of %v",
			sc.describe(), finalState, sc.allowedFinalStates())
	}

	requireEffects(t, sc, readEffects(t, ws.EffectFile))
	requireEventStory(t, ctx, s, sc, finalRunID)

	requireIntegrity(t, ctx, s, "after convergence")
	requireFsckClean(t, ctx, s, "after convergence")
}

// converge is the restart: a fresh engine recovers what it can and drives the
// run to a terminal state. It returns the final state and the id of the run
// that reached it, which for materialisation rows is the run the retried
// decision created.
func converge(t *testing.T, ctx context.Context, s *store.Store, ws *workspace,
	sc Scenario, runID string,
) (string, string) {
	t.Helper()

	eng := restartEngine(s, ws)
	finalRunID := runID

	switch sc.Kind {
	case "materialize":
		res, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{
			JobName: jobName, Actor: "crash-restart",
		})
		if err != nil {
			t.Fatalf("re-materialise the trigger after the crash: %v", err)
		}
		finalRunID = res.Run.ID

	case "execute":
		state, err := eng.Recover(ctx, runID)
		if err != nil {
			t.Fatalf("recover run %s: %v", runID, err)
		}
		t.Logf("recovery left the run in %q", state)
		if isTerminal(state) {
			return state, finalRunID
		}

	default:
		t.Fatalf("unknown scenario kind %q", sc.Kind)
	}

	state, err := eng.ExecuteRun(ctx, finalRunID)
	if err != nil {
		t.Fatalf("execute run %s after recovery: %v", finalRunID, err)
	}
	return state, finalRunID
}

func isTerminal(state string) bool {
	return contains([]string{"succeeded", "failed", "cancelled"}, state)
}

func contains(set []string, want string) bool {
	for _, s := range set {
		if s == want {
			return true
		}
	}
	return false
}

// requireIntegrity is the WAL recovery proof: SQLite itself says the file is
// sound after the process died inside it.
func requireIntegrity(t *testing.T, ctx context.Context, s *store.Store, when string) {
	t.Helper()

	report, err := s.IntegrityCheck(ctx)
	if err != nil {
		t.Fatalf("integrity check %s: %v", when, err)
	}
	if report.Integrity != "ok" {
		t.Errorf("integrity_check %s says %q, want \"ok\"", when, report.Integrity)
	}
	if report.ForeignKeyIssues != 0 {
		t.Errorf("foreign_key_check %s reports %d issues, want 0", when, report.ForeignKeyIssues)
	}
}

// requireFsckClean is the invariant proof. Every finding is named, so a
// failure points at the check that caught it.
func requireFsckClean(t *testing.T, ctx context.Context, s *store.Store, when string) {
	t.Helper()

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck %s: %v", when, err)
	}
	for _, v := range violations {
		t.Errorf("fsck %s: %s on %s: %s", when, v.Check, v.Subject, v.Detail)
	}
	if len(violations) > 0 {
		t.Fatalf("fsck %s found %d violations", when, len(violations))
	}
}

// requireFsckFindings is the invariant sweep with the row's transient
// contract applied, and it cuts both ways. Every finding present must be one
// the window predicts, and every predicted finding must BE present: a kill
// in the finish window always leaves the missing-verdict finding behind, and
// its absence would mean the kill landed somewhere else. Anything unexpected
// fails, naming the check that produced it.
func requireFsckFindings(t *testing.T, ctx context.Context, s *store.Store, when string, predicted []string) {
	t.Helper()

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck %s: %v", when, err)
	}
	seen := map[string]bool{}
	for _, v := range violations {
		seen[v.Check] = true
		if contains(predicted, v.Check) {
			t.Logf("predicted finding %s on %s: %s", v.Check, v.Subject, v.Detail)
			continue
		}
		t.Errorf("fsck %s: %s on %s: %s (predicted here: %v)",
			when, v.Check, v.Subject, v.Detail, predicted)
	}
	for _, check := range predicted {
		if !seen[check] {
			t.Errorf("fsck %s found no %s finding, but this window always leaves one",
				when, check)
		}
	}
}

func requireNoAbandonedChains(t *testing.T, ctx context.Context, s *store.Store, when string) {
	t.Helper()

	chains, err := s.AbandonedChains(ctx)
	if err != nil {
		t.Fatalf("abandoned chain sweep %s: %v", when, err)
	}
	for _, c := range chains {
		t.Errorf("abandoned chain %s: %s", when, c)
	}
	if len(chains) > 0 {
		t.Fatalf("abandoned chain sweep %s found %d chains", when, len(chains))
	}
}

// requireEffects holds the count to its bound and checks the attribution: all
// lines share one idempotency key, and no two lines claim the same attempt.
func requireEffects(t *testing.T, sc Scenario, effects []effect) {
	t.Helper()

	if len(effects) < sc.MinEffects || len(effects) > sc.MaxEffects {
		t.Fatalf("%s: %d effects landed, want between %d and %d (lines: %v)",
			sc.describe(), len(effects), sc.MinEffects, sc.MaxEffects, effects)
	}
	keys := map[string]bool{}
	attempts := map[int]bool{}
	for _, e := range effects {
		keys[e.Key] = true
		if attempts[e.Attempt] {
			t.Errorf("%s: two effect lines claim attempt %d", sc.describe(), e.Attempt)
		}
		attempts[e.Attempt] = true
	}
	if len(keys) != 1 {
		t.Errorf("%s: effect lines carry %d idempotency keys, want exactly one",
			sc.describe(), len(keys))
	}
}

// requireEventStory checks the named events the recovery owes the history: a
// run.requeued row when the crash caught the run running, and a
// STEP_FAILED_EXECUTOR_LOST verdict when a running attempt was closed.
func requireEventStory(t *testing.T, ctx context.Context, s *store.Store, sc Scenario, runID string) {
	t.Helper()

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read the events of run %s: %v", runID, err)
	}
	var requeued, lost bool
	for _, e := range events {
		if e.Kind == "run.requeued" {
			requeued = true
		}
		if e.ReasonCode == string(reason.STEPFailedExecutorLost) {
			lost = true
		}
	}
	if sc.ExpectRequeue && !requeued {
		t.Errorf("%s: no run.requeued event, but the crash caught the run running", sc.describe())
	}
	if !sc.ExpectRequeue && requeued {
		t.Errorf("%s: unexpected run.requeued event", sc.describe())
	}
	if sc.ExpectExecutorLost && !lost {
		t.Errorf("%s: no %s verdict, but a running attempt had to be closed",
			sc.describe(), reason.STEPFailedExecutorLost)
	}
	if !sc.ExpectExecutorLost && lost {
		t.Errorf("%s: unexpected %s verdict", sc.describe(), reason.STEPFailedExecutorLost)
	}
}

// TestCrashInsideRecovery kills the recovery itself. The first child crashes
// under exec; the second is armed with the requeue point and dies inside the
// recovery transition. The restart after that must still converge, which is
// the proof that recovery is a transition like any other: killed mid-write,
// it either happened whole or not at all.
func TestCrashInsideRecovery(t *testing.T) {
	sc, ok := scenarioByName("step_under_exec")
	if !ok {
		t.Fatal("the step_under_exec row is missing from the matrix")
	}
	ws := newWorkspace(t)
	ctx := context.Background()

	// Phase 1: the original crash, under exec.
	out, killed := runChild(t, ws, sc, underExec, nil)
	requireChildKilled(t, sc, out, killed)
	runID := childRunID(sc, out)
	if runID == "" {
		t.Fatalf("phase 1 printed no run id\n%s", out)
	}
	waitOrphansGone(t, ws.EffectFile)

	// Phase 2: recovery, armed. The child dies between the requeue's
	// state write and its event row.
	phase2, killed2 := runChild(t, ws, sc, requeueCrashedPt,
		map[string]string{envRecoverRun: runID})
	requireChildKilled(t, sc, phase2, killed2)

	s := openStore(t, ws)
	defer closeStore(t, s)

	requireIntegrity(t, ctx, s, "after the killed recovery")
	requireFsckClean(t, ctx, s, "after the killed recovery")
	requireNoAbandonedChains(t, ctx, s, "after the killed recovery")

	// The rollback left the exact shape the first crash made: still
	// running, still unrecovered.
	detail, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("read run %s: %v", runID, err)
	}
	if detail.Run.State != "running" {
		t.Fatalf("after the killed recovery the run is %q, want still \"running\"",
			detail.Run.State)
	}

	// Phase 3: an unarmed recovery converges, and the story is complete.
	finalState, _ := converge(t, ctx, s, ws, sc, runID)
	if finalState != "succeeded" {
		t.Fatalf("the restart after the killed recovery ended %q, want succeeded", finalState)
	}
	requireEffects(t, sc, readEffects(t, ws.EffectFile))
	requireEventStory(t, ctx, s, sc, runID)
	requireFsckClean(t, ctx, s, "after the restart")
}

// TestChildProcess is the crashing half. The parent runs this test binary
// with the scenario in the environment; without it, the test skips, so a
// plain go test of this package only exercises the matrix.
func TestChildProcess(t *testing.T) {
	name := os.Getenv(envScenario)
	if name == "" {
		t.Skip("driven by the crash harness parent")
	}
	sc, ok := scenarioByName(name)
	if !ok {
		t.Fatalf("unknown scenario %q", name)
	}
	dir := os.Getenv(envDir)
	if dir == "" {
		t.Fatal("no workspace directory in the environment")
	}
	ws := &workspace{
		Dir:        dir,
		StateDir:   filepath.Join(dir, "state"),
		DBPath:     filepath.Join(dir, "state", "state.db"),
		EffectFile: filepath.Join(dir, "effects.txt"),
	}

	ctx := context.Background()
	s, err := store.Open(ctx, ws.DBPath, store.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()

	// A child armed to die claims on the short clock: it will never carry
	// this run to a finish, and recovery after its death must not wait.
	// A child that means to survive (control, self-test) runs on the long
	// clock, exactly like any real executor.
	ttl := execLeaseTTL
	if sc.KillAt != "" && sc.KillAt != unknownPoint {
		ttl = crashLeaseTTL
	}
	eng := &engine.Engine{
		Store:        s,
		LogRoot:      logsink.NewRoot(ws.StateDir),
		Clock:        clock.System(),
		Owner:        "exec-child",
		PollInterval: pollInterval,
		LeaseTTL:     ttl,
	}

	if rec := os.Getenv(envRecoverRun); rec != "" {
		// Recovery mode: the parent wants this process to die inside
		// the recovery transition of a run an earlier child crashed.
		state, err := eng.Recover(ctx, rec)
		if err != nil {
			t.Fatalf("recover run %s: %v", rec, err)
		}
		fmt.Printf("PACEQ-RECOVERED %s\n", state)
		return
	}

	applyJob(t, s, sc, os.Getenv(envAppend), ws.EffectFile)

	res, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{
		JobName: jobName, Actor: "crash-harness",
	})
	if err != nil {
		t.Fatalf("materialise the trigger: %v", err)
	}
	// Unbuffered stdout: the parent reads the run id off this line even
	// though the process is about to die.
	fmt.Printf("PACEQ-RUN %s\n", res.Run.ID)

	if sc.Kind == "materialize" {
		// The fault points of a materialisation row are inside the
		// call above; reaching this line means nothing was armed.
		return
	}

	state, err := eng.ExecuteRun(ctx, res.Run.ID)
	if err != nil {
		t.Fatalf("execute run %s: %v", res.Run.ID, err)
	}
	fmt.Printf("PACEQ-DONE %s\n", state)
}

// applyJob records the frozen job version a scenario runs. One step, whose
// whole effect is one line in the workspace's effect file; the drip variant
// keeps attempt 1 alive so an under-exec kill lands inside execution.
func applyJob(t *testing.T, s *store.Store, sc Scenario, appendBin, effectFile string) {
	t.Helper()

	mode := "append"
	if sc.WaitsForOrphan {
		mode = "first-drip"
	}
	quote := func(s string) string { return strconv.Quote(s) }
	step := fmt.Sprintf(`{"name":"only","run":[%s,%s,%s],"shell":false`,
		quote(appendBin), quote(mode), quote(effectFile))
	if sc.RetryMax > 0 {
		step += fmt.Sprintf(`,"retry":{"max":%d}`, sc.RetryMax)
	}
	step += "}"
	spec := fmt.Sprintf(`{"schema":"paceq.job.v1","name":%q,"max_concurrent":1,`+
		`"timeout_ms":60000,"steps":[%s]}`, jobName, step)

	if _, _, err := s.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  jobName,
		SpecHash: "sha256:crash-" + sc.Name,
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job version: %v", err)
	}
}

// TestFsckDetectsEveryPlantedViolation is the negative proof of the checks
// themselves: each invariant is planted as a real broken row, and the sweep
// must return the named finding for it. This is the red-first half of every
// crash row: the states a half written transaction would leave behind are
// exactly these, and if a check stops catching one, the row here fails
// naming it.
func TestFsckDetectsEveryPlantedViolation(t *testing.T) {
	t.Run("I2_terminal_run_over_pending_step", func(t *testing.T) {
		s, _ := seedSucceededRun(t)
		subject, err := s.InjectTerminalStepPending(context.Background())
		if err != nil {
			t.Fatalf("plant: %v", err)
		}
		requireFinding(t, s, "I2", subject)
		// The aggregate check speaks about the run as a whole, so its
		// subject carries no step name.
		runSubject, _, _ := strings.Cut(subject, " step ")
		requireFinding(t, s, "I10", runSubject)
	})

	t.Run("I10_state_disagrees_with_steps", func(t *testing.T) {
		s, _ := seedSucceededRun(t)
		vs, _ := s.Fsck(context.Background())
		t.Logf("pre-plant fsck: %v", vs)
		subject, err := s.InjectFailedStepUnderSucceededRun(context.Background())
		if err != nil {
			t.Fatalf("plant: %v", err)
		}
		requireFinding(t, s, "I10", subject)
	})

	t.Run("I13_timestamps_run_backwards", func(t *testing.T) {
		s, _ := seedSucceededRun(t)
		subject, err := s.InjectBackwardsTimestamp(context.Background())
		if err != nil {
			t.Fatalf("plant: %v", err)
		}
		requireFinding(t, s, "I13", subject)
	})

	t.Run("I14_deferred_run_says_nothing", func(t *testing.T) {
		s, _ := seedQueuedRun(t)
		subject, err := s.InjectUnexplainedDeferral(context.Background())
		if err != nil {
			t.Fatalf("plant: %v", err)
		}
		requireFinding(t, s, "I14", subject)
	})

	t.Run("I15_event_chain_breaks", func(t *testing.T) {
		s, _ := seedSucceededRun(t)
		subject, err := s.InjectBrokenEventChain(context.Background())
		if err != nil {
			t.Fatalf("plant: %v", err)
		}
		requireFinding(t, s, "I15", subject)
	})

	t.Run("reason_terminal_without_a_code", func(t *testing.T) {
		s, _ := seedSucceededRun(t)
		subject, err := s.InjectUnexplainedTerminal(context.Background())
		if err != nil {
			t.Fatalf("plant: %v", err)
		}
		requireFinding(t, s, "reason", subject)
	})

	t.Run("chains_tick_without_trigger", func(t *testing.T) {
		s, cleanup := seedQueuedRun(t)
		defer cleanup()
		subject, err := s.InjectOrphanTick(context.Background())
		if err != nil {
			t.Fatalf("plant: %v", err)
		}
		chains, err := s.AbandonedChains(context.Background())
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if !containsString(chains, subject) {
			t.Fatalf("the chain sweep reported %v, want the planted finding %q",
				chains, subject)
		}
	})

	t.Run("healthy_database_reports_nothing", func(t *testing.T) {
		s, cleanup := seedSucceededRun(t)
		defer cleanup()
		requireFsckClean(t, context.Background(), s, "on a healthy seed")
		requireNoAbandonedChains(t, context.Background(), s, "on a healthy seed")
	})
}

// requireFinding asserts that the sweep returns a violation with exactly the
// named check and subject. The assertion message names both, which is what
// makes removing a check from fsck a visible failure instead of silent
// coverage loss.
func requireFinding(t *testing.T, s *store.Store, check, subject string) {
	t.Helper()

	violations, err := s.Fsck(context.Background())
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	for _, v := range violations {
		if v.Check == check && v.Subject == subject {
			return
		}
	}
	t.Fatalf("fsck reported %v, want a %q finding on %q", describeViolations(violations), check, subject)
}

func describeViolations(vs []store.Violation) string {
	parts := make([]string, 0, len(vs))
	for _, v := range vs {
		parts = append(parts, v.Check+"/"+v.Subject)
	}
	if len(parts) == 0 {
		return "no violations"
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func containsString(set []string, want string) bool {
	return contains(set, want)
}

// seedSucceededRun builds a workspace whose single job ran to success.
func seedSucceededRun(t *testing.T) (*store.Store, func()) {
	t.Helper()

	return seedRun(t, true)
}

// seedQueuedRun builds a workspace with a job applied and one run queued.
func seedQueuedRun(t *testing.T) (*store.Store, func()) {
	t.Helper()

	return seedRun(t, false)
}

func seedRun(t *testing.T, execute bool) (*store.Store, func()) {
	t.Helper()

	ws := newWorkspace(t)
	ctx := context.Background()
	s := openStore(t, ws)

	sc := Scenario{Name: "seed", Kind: "execute", RetryMax: 0}
	applyJob(t, s, sc, appendFixture(t), ws.EffectFile)

	res, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{
		JobName: jobName, Actor: "seed",
	})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	if execute {
		eng := restartEngine(s, ws)
		if _, err := eng.ExecuteRun(ctx, res.Run.ID); err != nil {
			t.Fatalf("execute: %v", err)
		}
	}
	return s, func() { closeStore(t, s) }
}
