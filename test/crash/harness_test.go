package crash

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/store"
)

// The environment variables the parent hands the child. Their presence is
// what turns TestChildProcess into the crashing half of the harness.
const (
	envScenario   = "PACEQ_CRASH_SCENARIO"
	envDir        = "PACEQ_CRASH_DIR"
	envAppend     = "PACEQ_CRASH_APPEND"
	envRecoverRun = "PACEQ_CRASH_RECOVER_RUN"

	// armEnv is the variable internal/faults reads in a pulseq_faults
	// build. Naming it here is what makes an unarmed child harmless.
	armEnv = "PULSEQ_CRASH_AT"

	jobName = "crashjob"

	childTimeout = "90s" // a wedged child fails instead of hanging CI

	// crashLeaseTTL is the lease a child claims with when it is armed to
	// die: its executor will be gone in milliseconds, so recovery after
	// the kill waits milliseconds for the lease to lapse. Arming is what
	// makes it safe for the lease to be this short; the child never
	// finishes a run it expects to be killed inside.
	crashLeaseTTL = 250 * time.Millisecond

	// execLeaseTTL is the lease of every engine that means to carry a run
	// to completion: the control and self-test children, the seeds, and
	// the restarted executor. A finish refused because its own lease died
	// mid-flight is correct fencing meeting a bad configuration, so these
	// never run on the short clock.
	execLeaseTTL = 5 * time.Minute

	pollInterval   = 5 * time.Millisecond
	orphanDeadline = 10 * time.Second
)

// workspace is one state directory for one row of the matrix.
type workspace struct {
	Dir        string // everything lives under here
	StateDir   string // state.db and logs/
	DBPath     string // <StateDir>/state.db
	EffectFile string // the fake job's effect log
}

func newWorkspace(t *testing.T) *workspace {
	t.Helper()

	dir := t.TempDir()
	ws := &workspace{
		Dir:        dir,
		StateDir:   filepath.Join(dir, "state"),
		DBPath:     filepath.Join(dir, "state", "state.db"),
		EffectFile: filepath.Join(dir, "effects.txt"),
	}
	if err := os.MkdirAll(ws.StateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	s := openStore(t, ws)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	closeStore(t, s)
	return ws
}

func openStore(t *testing.T, ws *workspace) *store.Store {
	t.Helper()

	s, err := store.Open(context.Background(), ws.DBPath, store.Options{})
	if err != nil {
		t.Fatalf("open store at %s: %v", ws.DBPath, err)
	}
	return s
}

func closeStore(t *testing.T, s *store.Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Errorf("close store: %v", err)
	}
}

// moduleRoot walks up from the working directory until it finds go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("find the working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the harness")
		}
		dir = parent
	}
}

// durableTempDir makes a directory that lives for the whole test binary
// run. Subtest tempdirs are removed when the subtest ends, which would
// delete a once-built fixture out from under later rows; the fixtures here
// must outlive whichever row built them. Nothing registers cleanup: the OS
// temp directory is swept by the environment, not by the test.
func durableTempDir(t *testing.T, pattern string) string {
	t.Helper()

	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	return dir
}

var (
	appendOnce sync.Once
	appendPath string
)

// appendFixture builds testdata/fakecmd once per run of the package.
func appendFixture(t *testing.T) string {
	t.Helper()

	appendOnce.Do(func() {
		path := filepath.Join(durableTempDir(t, "paceq-crash-append"), "append-fixture")
		build := exec.Command("go", "build", "-o", path, "./testdata/fakecmd")
		build.Dir = moduleRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build the append fixture: %v\n%s", err, out)
		}
		appendPath = path
	})
	return appendPath
}

var (
	childOnce sync.Once
	childPath string
)

// taggedChild builds this package's test binary with -tags pulseq_faults, the
// only build anywhere in the pipeline that may kill itself.
func taggedChild(t *testing.T) string {
	t.Helper()

	childOnce.Do(func() {
		path := filepath.Join(durableTempDir(t, "paceq-crash-child"), "crash-child.test")
		build := exec.Command("go", "test", "-c",
			"-tags", "pulseq_faults",
			"-o", path, ".")
		build.Dir = filepath.Join(moduleRoot(t), "test", "crash")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build the crashing child: %v\n%s", err, out)
		}
		childPath = path
	})
	return childPath
}

// runChild starts the child process for one scenario and waits for it to end.
// arm names the fault point to set; pass sc.KillAt for a matrix row or an
// override for the special proofs. It returns the combined output and whether
// the death was a real SIGKILL.
func runChild(t *testing.T, ws *workspace, sc Scenario, arm string, extra map[string]string) (string, bool) {
	t.Helper()

	cmd := exec.Command(taggedChild(t),
		"-test.run=TestChildProcess",
		"-test.count=1",
		"-test.timeout="+childTimeout)
	cmd.Env = os.Environ()
	if arm != "" {
		cmd.Env = append(cmd.Env, armEnv+"="+arm)
	}
	cmd.Env = append(cmd.Env,
		envScenario+"="+sc.Name,
		envDir+"="+ws.Dir,
		envAppend+"="+appendFixture(t))
	for k, v := range extra {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	runErr := cmd.Run()
	killed := false
	if exitErr, ok := runErr.(*exec.ExitError); ok {
		status := exitErr.Sys().(syscall.WaitStatus)
		killed = status.Signaled() && status.Signal() == syscall.SIGKILL
	}
	return out.String(), killed
}

// requireChildKilled fails unless the child died by SIGKILL.
func requireChildKilled(t *testing.T, sc Scenario, out string, killed bool) {
	t.Helper()
	if !killed {
		t.Fatalf("%s: the child was not killed by SIGKILL\n%s", sc.describe(), out)
	}
}

// requireChildSurvived fails if the child died at all.
func requireChildSurvived(t *testing.T, sc Scenario, out string, killed bool) {
	t.Helper()
	if killed {
		t.Fatalf("%s: the child died although its point does not exist\n%s",
			sc.describe(), out)
	}
}

// childRunID pulls the run id the child printed before executing. The print
// goes through an unbuffered stdout write, so it survives the SIGKILL.
func childRunID(sc Scenario, out string) string {
	for _, line := range strings.Split(out, "\n") {
		if after, ok := strings.CutPrefix(line, "PACEQ-RUN "); ok {
			return strings.TrimSpace(after)
		}
	}
	return ""
}

// procCarriesMarker reports whether any live process names the marker in its
// command line. The scan is read-only: /proc holds one numeric directory per
// process on Linux, which is exactly the orphan check issue #75 asks for.
func procCarriesMarker(marker string) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue // not a process directory
		}
		raw, err := os.ReadFile("/proc/" + entry.Name() + "/cmdline")
		if err != nil {
			continue // gone between listing and reading
		}
		if bytes.Contains(raw, []byte(marker)) {
			return true
		}
	}
	return false
}

// waitOrphansGone blocks until no process names the marker any more. A first
// attempt killed under exec dies by itself once its output pipe breaks, which
// happens promptly after the executor's death; waiting turns that into an
// assertion instead of a hope.
func waitOrphansGone(t *testing.T, marker string) {
	t.Helper()

	deadline := time.Now().Add(orphanDeadline)
	for procCarriesMarker(marker) {
		if time.Now().After(deadline) {
			t.Fatalf("an orphaned command carrying %q is still alive after %s",
				marker, orphanDeadline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// restartEngine wires a fresh engine onto the crashed workspace: the new
// executor a restart would start. It claims with the long lease, because it
// intends to finish what it starts; the wait for the DEAD owner's lease is
// governed by whatever expiry that owner wrote, not by this value.
func restartEngine(s *store.Store, ws *workspace) *engine.Engine {
	return &engine.Engine{
		Store:        s,
		LogRoot:      logsink.NewRoot(ws.StateDir),
		Clock:        clock.System(),
		Owner:        "exec-restart",
		PollInterval: pollInterval,
		LeaseTTL:     execLeaseTTL,
	}
}

// effect is one parsed line of the effect file.
type effect struct {
	Key     string
	Attempt int
	Nano    int64
}

func readEffects(t *testing.T, path string) []effect {
	t.Helper()

	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("read the effect file: %v", err)
	}
	var out []effect
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" {
			continue
		}
		parts := strings.FieldsFunc(line, func(r rune) bool { return r == '\t' })
		if len(parts) != 3 {
			t.Fatalf("effect line %q does not have three fields", line)
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Fatalf("effect line %q has a bad attempt: %v", line, err)
		}
		nano, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			t.Fatalf("effect line %q has a bad stamp: %v", line, err)
		}
		out = append(out, effect{Key: parts[0], Attempt: n, Nano: nano})
	}
	return out
}
