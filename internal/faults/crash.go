//go:build pulseq_faults

package faults

import (
	"os"
	"syscall"
)

// target is the one crash point this process will die at, named by the
// environment the crash harness starts it with. Read once at startup: a
// scenario must not be able to change its mind halfway through.
var target = os.Getenv("PULSEQ_CRASH_AT")

// Point kills the process with SIGKILL when name is the point the harness
// asked for. The kill is deliberately brutal: no deferred function runs, no
// log buffer is flushed and the database is left exactly as a power cut would
// leave it. Every other call returns at once.
func Point(name string) {
	if name != target {
		return
	}
	// SIGKILL cannot be caught, blocked or ignored, so nothing in this
	// process can tidy up first. That is what makes the harness honest.
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
}

// Enabled reports whether PULSEQ_CRASH_AT named a point. The harness checks
// it to tell a child that died on purpose from one that never armed itself.
func Enabled() bool { return target != "" }
