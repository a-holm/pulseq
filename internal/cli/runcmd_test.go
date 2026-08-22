package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The run command is the product: a multi step job driven to the end by an
// executor inside this process, with the exit code telling a cron migration
// whether paceq broke (1) or the job did (5).

// writeJob drops a job file into the project's jobs directory.
func writeJob(t *testing.T, dir, name, body string) {
	t.Helper()

	path := filepath.Join(dir, jobsDir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// runProject is an initialised project with the given job files applied. Every
// job here runs real processes: /bin/echo and /bin/sh exist on anything that
// runs these tests.
func runProject(t *testing.T, jobs map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("paceq init = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	for name, body := range jobs {
		writeJob(t, dir, name, body)
	}
	if got := runCLI(t, dir, nil, "apply"); got.code != ExitOK {
		t.Fatalf("paceq apply = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	return dir
}

const twoStepJob = `name: twostep
steps:
  - name: first
    run: ["/bin/echo", "one"]
  - name: second
    run: ["/bin/echo", "two"]
`

const failingJob = `name: failer
steps:
  - name: fine
    run: ["/bin/true"]
  - name: boom
    run: ["/bin/sh", "-c", "echo boom >&2; exit 3"]
`

const sleepingJob = `name: sleeper
steps:
  - name: nap
    run: ["/bin/sleep", "30"]
`

// runRecord is the JSON document the run command writes to stdout in pipe
// mode, success or failure.
type runRecord struct {
	Run struct {
		ID         string `json:"id"`
		Job        string `json:"job"`
		State      string `json:"state"`
		ReasonCode string `json:"reason_code"`
		DurationMS int64  `json:"duration_ms"`
		Steps      []struct {
			Name  string `json:"name"`
			State string `json:"state"`
		} `json:"steps"`
	} `json:"run"`
}

func TestRunWaitsForATwoStepJobAndLeavesEvidenceBehind(t *testing.T) {
	ctx := context.Background()
	dir := runProject(t, map[string]string{"twostep.yaml": twoStepJob})

	got := runCLI(t, dir, nil, "run", "twostep", "--wait")

	if got.code != ExitOK {
		t.Fatalf("paceq run twostep = %d, want %d\n%s%s",
			got.code, ExitOK, got.stdout, got.stderr)
	}
	var record runRecord
	if err := json.Unmarshal([]byte(got.stdout), &record); err != nil {
		t.Fatalf("stdout is not one run record:\n%s", got.stdout)
	}
	if record.Run.State != "succeeded" || record.Run.Job != "twostep" {
		t.Errorf("the run ended %s (%s), want succeeded", record.Run.State, record.Run.Job)
	}
	if len(record.Run.Steps) != 2 {
		t.Fatalf("the record carries %d steps, want 2", len(record.Run.Steps))
	}
	for i, want := range []string{"first", "second"} {
		step := record.Run.Steps[i]
		if step.Name != want || step.State != "succeeded" {
			t.Errorf("step %d = %s/%s, want %s/succeeded", i, step.Name, step.State, want)
		}
	}

	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStore(t, stateDir)
	defer func() { _ = s.Close() }()
	detail, err := s.GetRun(ctx, record.Run.ID)
	if err != nil {
		t.Fatalf("the run left no row behind: %v", err)
	}
	if detail.State != "succeeded" || len(detail.Steps) != 2 {
		t.Errorf("database has run %s in %s with %d steps",
			detail.ID, detail.State, len(detail.Steps))
	}

	// Logs are on disk under the log root, one file per step.
	root := filepath.Join(stateDir, "logs")
	found := 0
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".ndjson") {
			found++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the logs: %v", err)
	}
	if found < 2 {
		t.Errorf("%d log files under %s, want one per step", found, root)
	}

	if !strings.Contains(got.stderr, record.Run.ID) {
		t.Errorf("the progress on stderr never names the run:\n%s", got.stderr)
	}
}

func TestRunAFailedJobExitsFiveAndSaysWhy(t *testing.T) {
	dir := runProject(t, map[string]string{"failer.yaml": failingJob})

	got := runCLI(t, dir, nil, "run", "failer")

	// The load bearing distinction: the JOB failed, so 5, never 1.
	if got.code != ExitRunFailed {
		t.Fatalf("a failed job exits %d, want %d (exit 1 means paceq itself failed)\n%s%s",
			got.code, ExitRunFailed, got.stdout, got.stderr)
	}
	for _, want := range []string{"boom", "RUN_FAILED_STEP"} {
		if !strings.Contains(got.stderr, want) {
			t.Errorf("the failure does not mention %q:\n%s", want, got.stderr)
		}
	}
	var record runRecord
	if err := json.Unmarshal([]byte(got.stdout), &record); err != nil {
		t.Fatalf("stdout carries no run record for the failed job:\n%s", got.stdout)
	}
	if record.Run.State != "failed" || record.Run.ReasonCode != "RUN_FAILED_STEP" {
		t.Errorf("record says %s/%s, want failed/RUN_FAILED_STEP",
			record.Run.State, record.Run.ReasonCode)
	}
}

func TestRunWithABrokenStoreExitsOne(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("paceq init = %d\n%s", got.code, got.stderr)
	}
	dbPath := filepath.Join(dir, stateDirName, store.DatabaseFileName)
	// A file at the right mode holding none of the right bytes.
	if err := os.WriteFile(dbPath, []byte("this is not a database"), 0o600); err != nil {
		t.Fatalf("corrupt the database: %v", err)
	}

	got := runCLI(t, dir, nil, "run", "hello")

	if got.code != ExitInternal {
		t.Fatalf("a broken store exits %d, want %d (the job never ran)\n%s%s",
			got.code, ExitInternal, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stderr, "bug") {
		t.Errorf("an internal failure does not own up to being one:\n%s", got.stderr)
	}
}

func TestRunAnUnknownJobExitsThreeWithASuggestion(t *testing.T) {
	dir := runProject(t, nil)

	got := runCLI(t, dir, nil, "run", "helo")

	if got.code != ExitNotFound {
		t.Fatalf("an unknown job exits %d, want %d\n%s%s", got.code, ExitNotFound, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stderr, "hello") {
		t.Errorf("the refusal does not suggest the example job:\n%s", got.stderr)
	}
}

func TestRunWhileAnotherProcessHoldsTheStateExitsSix(t *testing.T) {
	dir := runProject(t, map[string]string{"twostep.yaml": twoStepJob})
	stateDir := filepath.Join(dir, stateDirName)

	// Somebody else is mid run: their process holds the state lock through
	// its own OpenState.
	holder, err := store.OpenState(context.Background(), stateDir,
		store.Options{Clock: clock.NewFake(testOrigin)})
	if err != nil {
		t.Fatalf("take the state lock: %v", err)
	}
	defer func() { _ = holder.Close() }()

	got := runCLI(t, dir, nil, "run", "hello")

	if got.code != ExitBusy {
		t.Fatalf("a contended state directory exits %d, want %d\n%s%s",
			got.code, ExitBusy, got.stdout, got.stderr)
	}
}

func TestRunInterruptedMidFlightCancelsTheRunWithExitEight(t *testing.T) {
	ctx := context.Background()
	dir := runProject(t, map[string]string{"sleeper.yaml": sleepingJob})

	// Drive the command from another goroutine; the test pulls the same
	// durable cancellation lever a SIGINT handler pulls.
	done := make(chan result, 1)
	go func() { done <- runCLIContext(t, ctx, dir, nil, "run", "sleeper") }()

	stateDir := filepath.Join(dir, stateDirName)
	probe := openFixtureStore(t, stateDir)
	defer func() { _ = probe.Close() }()

	var runID string
	deadline := time.Now().Add(20 * time.Second)
	for runID == "" {
		if time.Now().After(deadline) {
			t.Fatal("the sleeping step never started running")
		}
		time.Sleep(20 * time.Millisecond)
		runs, err := probe.ListRuns(ctx, store.RunFilter{})
		if err != nil {
			t.Fatalf("list runs: %v", err)
		}
		for _, r := range runs {
			detail, err := probe.GetRun(ctx, r.ID)
			if err == nil && len(detail.Steps) > 0 && detail.Steps[0].State == "running" {
				runID = r.ID
			}
		}
	}
	if _, err := probe.RequestCancel(ctx, runID, "test", "the test asked"); err != nil {
		t.Fatalf("request the cancel: %v", err)
	}

	select {
	case got := <-done:
		if got.code != ExitInterrupted {
			t.Fatalf("an interrupted run exits %d, want %d\n%s%s",
				got.code, ExitInterrupted, got.stdout, got.stderr)
		}
		detail, err := probe.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("read the cancelled run: %v", err)
		}
		if detail.State != "cancelled" {
			t.Errorf("the run ended %s, want cancelled", detail.State)
		}
		if detail.Steps[0].State != "cancelled" {
			t.Errorf("the step ended %s, want cancelled", detail.Steps[0].State)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the run command never came back after cancellation")
	}
}

// TestRunTextAtATerminalJSONInAPipe pins both halves of the output rule for
// the command people will type most: a terminal gets text progress and an
// empty stdout, a pipe gets one JSON document on stdout.
func TestRunTextAtATerminalJSONInAPipe(t *testing.T) {
	dir := runProject(t, map[string]string{"twostep.yaml": twoStepJob})

	stdout, readOut := terminalFile(t)
	stderr, readErr := pipeFile(t)
	env := Env{Stdout: stdout, Stderr: stderr, Dir: dir, Getenv: lookup(nil)}
	code := run(context.Background(), env, []string{"run", "twostep"})
	if err := stdout.Close(); err != nil {
		t.Fatalf("close the terminal: %v", err)
	}
	if err := stderr.Close(); err != nil {
		t.Fatalf("close stderr: %v", err)
	}
	written, textErr := readOut(), readErr()

	if code != ExitOK {
		t.Fatalf("at a terminal paceq run = %d\n%s", code, textErr)
	}
	if strings.TrimSpace(written) != "" {
		t.Errorf("a terminal got data on stdout:\n%s", written)
	}
	if !strings.Contains(textErr, "twostep") {
		t.Errorf("a terminal got no progress on stderr:\n%s", textErr)
	}

	piped := runCLI(t, dir, nil, "run", "twostep")
	var record runRecord
	if err := json.Unmarshal([]byte(piped.stdout), &record); err != nil {
		t.Fatalf("a pipe got no JSON document:\n%s", piped.stdout)
	}
}

// TestRunsListIsATableHereAndJSONThere extends the same rule to the read
// commands: the terminal shows the table, the pipe gets the array.
func TestRunsListIsATableHereAndJSONThere(t *testing.T) {
	dir, _ := finishedRunsFixture(t)

	stdout, readOut := terminalFile(t)
	stderr, readErr := pipeFile(t)
	env := Env{Stdout: stdout, Stderr: stderr, Dir: dir, Getenv: lookup(nil)}
	code := run(context.Background(), env, []string{"runs", "list"})
	_ = stdout.Close()
	_ = stderr.Close()
	if code != ExitOK {
		t.Fatalf("runs list at a terminal = %d\n%s", code, readErr())
	}
	table := readOut()

	for _, want := range []string{"nightly", "succeeded", "import", "failed"} {
		if !strings.Contains(table, want) {
			t.Errorf("the table does not mention %q:\n%s", want, table)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(table), "[") {
		t.Error("a terminal got a JSON array instead of a table")
	}

	piped := runCLI(t, dir, nil, "runs", "list")
	if !json.Valid([]byte(strings.TrimSpace(piped.stdout))) {
		t.Fatalf("a pipe got no JSON array:\n%s", piped.stdout)
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(piped.stdout), &rows); err != nil {
		t.Fatalf("decode the array: %v\n%s", err, piped.stdout)
	}
	if len(rows) != 2 {
		t.Fatalf("the listing holds %d rows, want both runs", len(rows))
	}
	// Newest first: ids are ULIDs, so id order is time order.
	first, _ := rows[0]["id"].(string)
	second, _ := rows[1]["id"].(string)
	if first <= second {
		t.Errorf("the rows are not newest first: %s then %s", first, second)
	}
	if code, _ := rows[0]["reason_code"].(string); code == "" {
		t.Error("a finished run lists without its reason")
	}
}

func TestRunsListFlagsNarrowTheListing(t *testing.T) {
	dir, _ := finishedRunsFixture(t)

	byJob := runCLI(t, dir, nil, "runs", "list", "--job", "import")
	var importRows []map[string]any
	if err := json.Unmarshal([]byte(byJob.stdout), &importRows); err != nil {
		t.Fatalf("--job gave no JSON array: %v\n%s", err, byJob.stdout)
	}
	if len(importRows) == 0 {
		t.Fatal("--job import matched nothing")
	}
	for _, row := range importRows {
		if row["job"] != "import" {
			t.Errorf("--job let through %v", row["job"])
		}
	}

	byState := runCLI(t, dir, nil, "runs", "list", "--state", "failed")
	var failedRows []map[string]any
	if err := json.Unmarshal([]byte(byState.stdout), &failedRows); err != nil {
		t.Fatalf("--state gave no JSON array: %v\n%s", err, byState.stdout)
	}
	if len(failedRows) == 0 {
		t.Fatal("no failed row came back, though the fixture made one")
	}
	for _, row := range failedRows {
		if row["state"] != "failed" {
			t.Errorf("--state let through %v", row["state"])
		}
	}

	badState := runCLI(t, dir, nil, "runs", "list", "--state", "nope")
	if badState.code != ExitUsage {
		t.Errorf("an unknown state exits %d, want %d", badState.code, ExitUsage)
	}

	badSince := runCLI(t, dir, nil, "runs", "list", "--since", "yesterday")
	if badSince.code != ExitUsage {
		t.Errorf("an unknown duration exits %d, want %d", badSince.code, ExitUsage)
	}
}

// TestRunsListSinceFollowsTheCommandClock is the wall clock guard: --since is
// answered against the clock the environment brought, so a fake clock decides
// what recent means. An implementation reading time.Now answers a different
// question and this test fails.
func TestRunsListSinceFollowsTheCommandClock(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)

	clk := clock.NewFake(testOrigin)
	s := openFixtureStoreAt(t, stateDir, clk)
	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}
	if _, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      fixtureJobInput.JobName,
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	}); err != nil {
		t.Fatalf("create the old run: %v", err)
	}
	clk.Advance(3 * time.Hour)
	recent, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      fixtureJobInput.JobName,
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the recent run: %v", err)
	}
	clk.Advance(10 * time.Minute)
	if err := s.Close(); err != nil {
		t.Fatalf("close the seeding store: %v", err)
	}

	// The command sees the same clock, ten minutes later still.
	envClk := clock.NewFake(clk.Now())
	stdout, readOut := pipeFile(t)
	stderr, readErr := pipeFile(t)
	cmdEnv := Env{Stdout: stdout, Stderr: stderr, Dir: dir, Getenv: lookup(nil), Clk: envClk}
	code := run(ctx, cmdEnv, []string{"runs", "list", "--since", "1h"})
	_ = stdout.Close()
	_ = stderr.Close()
	if code != ExitOK {
		t.Fatalf("runs list --since 1h = %d\n%s", code, readErr())
	}

	var rows []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(readOut()), &rows); err != nil {
		t.Fatalf("--since gave no JSON array: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != recent.ID {
		t.Errorf("--since 1h returned %d rows (%v), want only the recent run %s",
			len(rows), rows, recent.ID)
	}
}

// testOrigin is when every cli fixture happens: far from any real today, so
// an implementation that reads the wall clock lands in a different world and
// fails loudly.
var testOrigin = time.Date(2026, 9, 17, 3, 0, 0, 0, time.UTC)

// finishedRunsFixture seeds a project whose job nightly has one finished run
// (succeeded) and whose job import has one failed one.
func finishedRunsFixture(t *testing.T) (dir, newestRun string) {
	t.Helper()

	dir = t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(testOrigin))
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}
	okRun, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the good run: %v", err)
	}
	if _, err := s.ClaimRun(ctx, okRun.ID, store.LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, okRun.ID, "extract"); err != nil {
		t.Fatalf("start extract: %v", err)
	}
	zero := 0
	if _, err := s.RecordStepOutcome(ctx, okRun.ID, "extract", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		ExitCode:   &zero,
	}); err != nil {
		t.Fatalf("finish extract: %v", err)
	}
	if _, err := s.FinishRun(ctx, okRun.ID, "test",
		store.FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("finish the good run: %v", err)
	}

	badVersion, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "import",
		SpecHash: "sha256:import",
		SpecJSON: `{"steps":[{"name":"load"}]}`,
	})
	if err != nil {
		t.Fatalf("record import: %v", err)
	}
	badRun, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "import",
		JobVersionID: badVersion.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "load"}},
	})
	if err != nil {
		t.Fatalf("create the bad run: %v", err)
	}
	if _, err := s.ClaimRun(ctx, badRun.ID, store.LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, badRun.ID, "load"); err != nil {
		t.Fatalf("start load: %v", err)
	}
	one := 1
	if _, err := s.RecordStepOutcome(ctx, badRun.ID, "load", store.StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode:   &one,
	}); err != nil {
		t.Fatalf("fail load: %v", err)
	}
	if _, err := s.FinishRun(ctx, badRun.ID, "test",
		store.FinishReason{Code: reason.RUNFailedStep}); err != nil {
		t.Fatalf("finish the bad run: %v", err)
	}
	return dir, okRun.ID
}

// openFixtureStoreAt is openFixtureStore with a stated clock.
func openFixtureStoreAt(t *testing.T, stateDir string, clk clock.Clock) *store.Store {
	t.Helper()

	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	path := filepath.Join(stateDir, store.DatabaseFileName)
	s, err := store.Open(context.Background(), path, store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}
