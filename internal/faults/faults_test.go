package faults_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/faults"
)

// TestPointIsANoOpInTheDefaultBuild calls the point every which way. If this
// build somehow carried the crash machinery, arming it through an environment
// variable would kill the test process and the failure would be loud.
func TestPointIsANoOpInTheDefaultBuild(t *testing.T) {
	t.Setenv("PULSEQ_CRASH_AT", "M1:materialize:after_tick")

	faults.Point("M1:materialize:after_tick")
	faults.Point("")
	faults.Point("anything")

	if faults.Enabled() {
		t.Fatal("faults.Enabled is true in a build without the pulseq_faults tag")
	}
}

// TestReleaseBinaryCarriesNoCrashSwitch builds cmd/paceq exactly as a
// release does, with no build tag, and reads the binary back. The string that
// arms a crash point must not appear in it: a release binary with the switch
// present but inert would still be an attack surface and a confession that
// the crash paths ship.
//
// The check runs go from inside a test, so it needs the same toolchain the
// rest of the build uses and nothing else. It builds once per run of the
// package; afterwards the build cache makes it nearly free.
func TestReleaseBinaryCarriesNoCrashSwitch(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the release binary; skipped in short mode")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "paceq")

	build := exec.Command("go", "build", "-o", bin, "github.com/a-holm/paceq/cmd/paceq")
	build.Env = append(os.Environ(), "GOFLAGS=-mod=readonly -buildvcs=false")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	raw, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read the built binary: %v", err)
	}
	if strings.Contains(string(raw), "PULSEQ_CRASH_AT") {
		t.Error("the default build contains PULSEQ_CRASH_AT; the crash switch leaked into the release path")
	}
}
