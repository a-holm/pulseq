package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// ptr is the exit code a fixture verdict carries.
func ptr(i int) *int { return &i }

// fixtureJobInput is the one job every fixture run is a run of. Re-upserting
// it returns the version already stored, so extra fixtures need no lookup.
var fixtureJobInput = store.JobVersionInput{
	JobName:     "nightly",
	Description: "the nightly job",
	SourcePath:  "jobs/nightly.yaml",
	SpecHash:    "sha256:nightly",
	SpecJSON:    `{"steps":[{"name":"extract"},{"name":"load"}]}`,
}

// logsFixture builds a state directory with one run of two steps: extract,
// which ran twice and failed then succeeded, and load, which ran once and
// succeeded. Every log file is written through the real sink, so what the
// command reads is what the sink writes.
func logsFixture(t *testing.T) (dir, runID string) {
	t.Helper()
	dir = t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)

	s := openFixtureStore(t, stateDir)
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps: []store.NewStep{
			{Name: "extract", MaxAttempts: 2},
			{Name: "load"},
		},
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	root := logsink.NewRoot(stateDir)
	at := time.Date(2026, 9, 17, 3, 0, 1, 0, time.UTC)

	// The engine holds the run while its steps take their turns.
	if _, err := s.ClaimRun(ctx, run.ID, store.LeaseInput{Owner: "engine"}); err != nil {
		t.Fatalf("claim run: %v", err)
	}

	// Attempt 1 of extract fails after two lines. It still has an attempt
	// left, so the machine sends it back to pending for attempt 2.
	finishWithSink(t, s, root, run.ID, "extract", 1, at, func(sk *logsink.Sink) {
		writeFixtureLine(t, sk, logsink.StreamStdout, "connecting to warehouse")
		writeFixtureLine(t, sk, logsink.StreamStderr, "warning: slow response")
	}, store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode:   ptr(1),
		FinishedAt: at.Add(time.Minute),
	})

	// Attempt 2 succeeds with one line.
	at2 := at.Add(2 * time.Minute)
	finishWithSink(t, s, root, run.ID, "extract", 2, at2, func(sk *logsink.Sink) {
		writeFixtureLine(t, sk, logsink.StreamStdout, "second attempt connected")
	}, store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
		ExitCode:   ptr(0),
		FinishedAt: at2.Add(time.Minute),
	})

	// load runs once and succeeds.
	at3 := at.Add(5 * time.Minute)
	finishWithSink(t, s, root, run.ID, "load", 1, at3, func(sk *logsink.Sink) {
		writeFixtureLine(t, sk, logsink.StreamStdout, "loaded 42 rows")
	}, store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
		ExitCode:   ptr(0),
		FinishedAt: at3.Add(time.Minute),
	})

	return dir, run.ID
}

// finishWithSink starts a step, lets the test write through a real sink, then
// records the verdict with what the sink reported.
func finishWithSink(t *testing.T, s *store.Store, root logsink.Root, runID, step string,
	attempt int, at time.Time, write func(*logsink.Sink), out store.StepOutcome,
) {
	t.Helper()
	ctx := context.Background()

	if err := s.StartStep(ctx, runID, step); err != nil {
		t.Fatalf("start %s attempt %d: %v", step, attempt, err)
	}
	sink, err := logsink.Open(root, runID, step, attempt, logsink.Options{Clock: clock.NewFake(at)})
	if err != nil {
		t.Fatalf("open the sink of %s attempt %d: %v", step, attempt, err)
	}
	write(sink)
	tail, bytes, truncated, err := sink.Finish()
	if err != nil {
		t.Fatalf("finish the sink of %s attempt %d: %v", step, attempt, err)
	}
	out.LogMeta = store.LogMeta{RelPath: sink.RelPath(), Bytes: bytes, Truncated: truncated, ErrorTail: tail}
	if _, err := s.RecordStepOutcome(ctx, runID, step, out); err != nil {
		t.Fatalf("record %s attempt %d: %v", step, attempt, err)
	}
}

func writeFixtureLine(t *testing.T, s *logsink.Sink, stream, text string) {
	t.Helper()
	if _, err := fmt.Fprintln(s.Writer(stream), text); err != nil {
		t.Fatalf("write a fixture line: %v", err)
	}
}

func openFixtureStore(t *testing.T, stateDir string) *store.Store {
	t.Helper()
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	path := filepath.Join(stateDir, store.DatabaseFileName)
	s, err := store.Open(context.Background(), path, store.Options{Clock: clock.NewFake(time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC))})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func TestLogsRendersTextForOneStep(t *testing.T) {
	dir, runID := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", runID[:12], "--step", "extract", "-o", "text")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	// Default is the newest attempt only: attempt 2's line, not attempt 1's.
	for _, want := range []string{"second attempt connected", "stdout"} {
		if !strings.Contains(res.stdout, want) {
			t.Fatalf("output misses %q:\n%s", want, res.stdout)
		}
	}
	for _, unwanted := range []string{"connecting to warehouse", "warning: slow response"} {
		if strings.Contains(res.stdout, unwanted) {
			t.Fatalf("an older attempt leaked into the default view: %q\n%s", unwanted, res.stdout)
		}
	}
}

func TestLogsShowsAllStepsWithoutAStepFlag(t *testing.T) {
	dir, runID := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", runID[:12], "-o", "text")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	for _, want := range []string{"extract", "load", "loaded 42 rows"} {
		if !strings.Contains(res.stdout, want) {
			t.Fatalf("output misses %q:\n%s", want, res.stdout)
		}
	}
}

func TestLogsJSONCarriesStepAndAttempt(t *testing.T) {
	dir, runID := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", runID[:12], "--step", "extract", "-o", "json")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	var sawAttempt2 bool
	for _, doc := range jsonLines(t, res.stdout) {
		if doc["stream"] == nil || doc["seq"] == nil {
			t.Fatalf("a line misses stream or seq: %v", doc)
		}
		if doc["step"] != "extract" {
			t.Fatalf("line names step %v, want extract", doc["step"])
		}
		if doc["attempt"] == float64(2) {
			sawAttempt2 = true
		}
	}
	if !sawAttempt2 {
		t.Fatalf("no line from attempt 2:\n%s", res.stdout)
	}
}

// The jq example from the plan has to work: every line carries stream and the
// raw text under line.
func TestLogsJSONSupportsTheJqOneLiner(t *testing.T) {
	dir, runID := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", runID[:12], "--all-attempts", "-o", "json")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	var stderrLines []string
	for _, doc := range jsonLines(t, res.stdout) {
		if doc["stream"] == "stderr" {
			stderrLines = append(stderrLines, doc["line"].(string))
		}
	}
	if len(stderrLines) != 1 || stderrLines[0] != "warning: slow response" {
		t.Fatalf(`select(.stream=="stderr").line gave %q`, stderrLines)
	}
}

// Attempt 1's log survives attempt 2's success: that is US-05, and the whole
// point of one file per attempt.
func TestLogsAllAttemptsShowsTheFailedAttempt(t *testing.T) {
	dir, runID := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", runID[:12], "--step", "extract", "--all-attempts", "-o", "text")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	first := strings.Index(res.stdout, "warning: slow response")
	second := strings.Index(res.stdout, "second attempt connected")
	if first < 0 || second < 0 {
		t.Fatalf("an attempt is missing:\n%s", res.stdout)
	}
	if first > second {
		t.Fatalf("attempt 1 comes after attempt 2:\n%s", res.stdout)
	}
	if !strings.Contains(res.stdout, "attempt 1") || !strings.Contains(res.stdout, "attempt 2") {
		t.Fatalf("the attempts are not separated:\n%s", res.stdout)
	}
}

func TestLogsAttemptSelectorPicksOneAttempt(t *testing.T) {
	dir, runID := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", runID[:12], "--step", "extract", "--attempt", "1", "-o", "text")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	if !strings.Contains(res.stdout, "warning: slow response") {
		t.Fatalf("attempt 1 is not shown:\n%s", res.stdout)
	}
	if strings.Contains(res.stdout, "second attempt connected") {
		t.Fatalf("attempt 2 leaked into --attempt 1:\n%s", res.stdout)
	}

	res = runCLI(t, dir, nil, "logs", runID[:12], "--step", "extract", "--attempt", "9")
	if res.code != ExitNotFound {
		t.Fatalf("a missing attempt exits %d, want %d: %s", res.code, ExitNotFound, res.stderr)
	}
}

func TestLogsRefusesConflictingAttemptFlags(t *testing.T) {
	dir, runID := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", runID[:12], "--attempt", "1", "--all-attempts")
	if res.code != ExitUsage {
		t.Fatalf("conflicting flags exit %d, want %d", res.code, ExitUsage)
	}
}

func TestLogsUnknownStepExitsNotFound(t *testing.T) {
	dir, runID := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", runID[:12], "--step", "ghost")
	if res.code != ExitNotFound {
		t.Fatalf("exit %d, want %d: %s", res.code, ExitNotFound, res.stderr)
	}
}

func TestLogsAmbiguousPrefixExitsThreeAndListsTheRuns(t *testing.T) {
	dir, runID := logsFixture(t)
	other := queueSiblingRun(t, dir)

	prefix := commonPrefix(runID, other)
	if len(prefix) < 4 {
		t.Fatalf("the two runs share no usable prefix: %s vs %s", runID, other)
	}

	res := runCLI(t, dir, nil, "logs", prefix)
	if res.code != ExitNotFound {
		t.Fatalf("ambiguous prefix exits %d, want %d: %s", res.code, ExitNotFound, res.stderr)
	}
	if !strings.Contains(res.stderr, runID) || !strings.Contains(res.stderr, other) {
		t.Fatalf("the alternatives are not listed:\n%s", res.stderr)
	}
}

func TestLogsUnknownRunExitsNotFound(t *testing.T) {
	dir, _ := logsFixture(t)
	res := runCLI(t, dir, nil, "logs", "01ZZZZZZZZZZZZZZZZZZZZZZZZ")
	if res.code != ExitNotFound {
		t.Fatalf("exit %d, want %d", res.code, ExitNotFound)
	}
}

func TestLogsWorksWithoutAnyState(t *testing.T) {
	res := runCLI(t, t.TempDir(), nil, "logs", "01A")
	if res.code == 0 {
		t.Fatal("logs outside a project succeeded")
	}
}

// queueSiblingRun queues a second run of the fixture job, so two runs share
// the time ordered prefix of their ids.
func queueSiblingRun(t *testing.T, dir string) string {
	t.Helper()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStore(t, stateDir)
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the second run: %v", err)
	}
	return run.ID
}

func commonPrefix(a, b string) string {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return a[:n]
}

// jsonLines parses a stdout of one JSON object per line.
func jsonLines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var docs []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			t.Fatalf("stdout line is not JSON: %v\n%s", err, line)
		}
		docs = append(docs, doc)
	}
	return docs
}

// streamingRun starts the command with a live pipe for stdout and returns the
// exit code through a channel, so a test can read output while the command is
// still following a file.
func streamingRun(t *testing.T, dir string, ctx context.Context, args ...string) (io.ReadCloser, <-chan int) {
	t.Helper()
	pr, pw := io.Pipe()
	code := make(chan int, 1)
	go func() {
		env := Env{Stdout: pw, Stderr: io.Discard, Dir: dir, Getenv: func(string) string { return "" }}
		code <- run(ctx, env, args)
		_ = pw.Close()
	}()
	return pr, code
}

// readLineWithin reads one line of output or fails the test. Every wait is
// bounded: a mutant that never prints or never exits turns into a red test,
// never into a hung gate.
func readLineWithin(t *testing.T, r *bufio.Reader, within time.Duration) string {
	t.Helper()
	type outcome struct {
		line string
		err  error
	}
	ch := make(chan outcome, 1)
	go func() {
		line, err := r.ReadString('\n')
		ch <- outcome{line, err}
	}()
	select {
	case got := <-ch:
		if got.err != nil && got.line == "" {
			t.Fatalf("read output: %v", got.err)
		}
		return strings.TrimRight(got.line, "\n")
	case <-time.After(within):
		t.Fatalf("no output within %s", within)
		return ""
	}
}

func exitWithin(t *testing.T, code <-chan int, within time.Duration) int {
	t.Helper()
	select {
	case got := <-code:
		return got
	case <-time.After(within):
		t.Fatalf("the command did not exit within %s", within)
		return -1
	}
}

// The follow loop shows new lines while the run is live and ends cleanly,
// with exit 0, once the run reaches a terminal state and the files are
// drained. Following starts even before the log file exists.
func TestLogsFollowShowsNewLinesAndExitsWhenTheRunEnds(t *testing.T) {
	dir, _ := logsFixture(t)
	stateDir := filepath.Join(dir, stateDirName)

	// A second run with one live step whose log does not exist yet.
	s := openFixtureStore(t, stateDir)
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}
	live, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "notify"}},
	})
	if err != nil {
		t.Fatalf("create the followed run: %v", err)
	}
	at := time.Date(2026, 9, 17, 9, 0, 0, 0, time.UTC)
	if _, err := s.ClaimRun(ctx, live.ID, store.LeaseInput{Owner: "engine"}); err != nil {
		t.Fatalf("claim the followed run: %v", err)
	}
	if err := s.StartStep(ctx, live.ID, "notify"); err != nil {
		t.Fatalf("start notify: %v", err)
	}

	// The whole id: two runs now share a time ordered prefix.
	followCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, code := streamingRun(t, dir, followCtx, "logs", live.ID, "--step", "notify", "-f")

	sink, err := logsink.Open(logsink.NewRoot(stateDir), live.ID, "notify", 1,
		logsink.Options{Clock: clock.NewFake(at)})
	if err != nil {
		t.Fatalf("open the live sink: %v", err)
	}
	reader := bufio.NewReader(r)

	writeFixtureLine(t, sink, logsink.StreamStdout, "notification queued")
	if got := readLineWithin(t, reader, 5*time.Second); !strings.Contains(got, "notification queued") {
		t.Fatalf("first followed line is %q", got)
	}

	writeFixtureLine(t, sink, logsink.StreamStderr, "delivery failed")
	if got := readLineWithin(t, reader, 5*time.Second); !strings.Contains(got, "delivery failed") {
		t.Fatalf("second followed line is %q", got)
	}

	// The run ends: the command drains and exits 0 on its own.
	tail, bytes, truncated, err := sink.Finish()
	if err != nil {
		t.Fatalf("finish the live sink: %v", err)
	}
	if _, err := s.RecordStepOutcome(ctx, live.ID, "notify", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode: ptr(2), FinishedAt: time.Now(),
		LogMeta: store.LogMeta{RelPath: sink.RelPath(), Bytes: bytes, Truncated: truncated, ErrorTail: tail},
	}); err != nil {
		t.Fatalf("finish notify: %v", err)
	}
	if got := exitWithin(t, code, 10*time.Second); got != 0 {
		t.Fatalf("exit %d after the run ended, want 0", got)
	}
}

// Cancelling the context is what SIGINT does through signal.NotifyContext.
// The command must treat it as a normal end, not a failure.
func TestLogsFollowExitsCleanlyOnInterrupt(t *testing.T) {
	dir, runID := logsFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	r, code := streamingRun(t, dir, ctx, "logs", runID[:12], "--step", "extract", "-f")

	if got := readLineWithin(t, bufio.NewReader(r), 5*time.Second); !strings.Contains(got, "second attempt connected") {
		t.Fatalf("the first line is %q", got)
	}
	cancel()
	if got := exitWithin(t, code, 5*time.Second); got != 0 {
		t.Fatalf("exit %d on interrupt, want 0", got)
	}
}

// TestLogsMarkerAndGapRenderForPeople pins the human rendering of the
// truncation marker and of an unexplained seq gap, which are the two things a
// reader must not miss.
func TestLogsMarkerAndGapRenderForPeople(t *testing.T) {
	var out bytes.Buffer
	u := &ui{out: &out, err: io.Discard, mode: modeText, symbols: symbols(false)}
	w := &logRenderer{u: u, text: true}

	mustRender := func(l logsink.Line) {
		t.Helper()
		if err := w.emit("extract", 1, l); err != nil {
			t.Fatalf("render %+v: %v", l, err)
		}
	}
	mustRender(logsink.Line{TS: 1789635601123, Stream: logsink.StreamStdout, Seq: 1, Line: "hello"})
	mustRender(logsink.Line{TS: 1789635601999, Stream: logsink.StreamPulseq, Seq: 900, Event: "truncated", DroppedBytes: 19327352832})
	mustRender(logsink.Line{TS: 1789635602000, Stream: logsink.StreamStdout, Seq: 904, Line: "after the gap"})
	mustRender(logsink.Line{TS: 1789635603000, Stream: logsink.StreamStdout, Seq: 920, Line: "past a silent loss"})

	text := out.String()
	if !strings.Contains(text, "hello") {
		t.Fatalf("the plain line is missing:\n%s", text)
	}
	if !strings.Contains(text, "truncated") || !strings.Contains(text, "18.0 GiB") {
		t.Fatalf("the marker is not rendered for people:\n%s", text)
	}
	// The marker explains the 898 seq numbers it covers; repeating that as a
	// separate warning would be noise. A later gap gets no such cover.
	if strings.Contains(text, "lines missing before seq 900") {
		t.Fatalf("the marker is doubled by a gap warning:\n%s", text)
	}
	if !strings.Contains(text, "15 lines missing") {
		t.Fatalf("an unexplained seq gap is silent:\n%s", text)
	}
}

// The JSON mode passes the marker through as data, so a script can select it.
func TestLogsMarkerStaysDataInJSON(t *testing.T) {
	var out bytes.Buffer
	u := &ui{out: &out, err: io.Discard, mode: modeJSON, symbols: symbols(false)}
	w := &logRenderer{u: u, text: false}
	if err := w.emit("extract", 1, logsink.Line{TS: 1, Stream: logsink.StreamPulseq, Seq: 2, Event: "truncated", DroppedBytes: 42}); err != nil {
		t.Fatalf("render: %v", err)
	}
	docs := jsonLines(t, out.String())
	if len(docs) != 1 || docs[0]["event"] != "truncated" || docs[0]["dropped_bytes"] != float64(42) {
		t.Fatalf("marker as JSON = %v", docs)
	}
}
