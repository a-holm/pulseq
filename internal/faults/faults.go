//go:build !pulseq_faults

// Package faults names the points where the crash harness (#75) kills this
// process. Every point sits in a window the write model makes a promise
// about: between two writes of one transaction, or between one committed
// transaction and the work that follows it.
//
// The default build, the one above, has none of it. Point is an empty
// function the compiler folds away and Enabled reports false, so a release
// binary carries no crash machinery and no trace of the environment variable
// that would drive it. A test in this package holds that promise by building
// cmd/paceq without the tag and reading the bytes back.
//
// The harness builds its child process with -tags pulseq_faults instead.
// There, Point compares its argument against PULSEQ_CRASH_AT and kills the
// process with SIGKILL on a match: no defer runs, no buffer is flushed and no
// exit handler fires, which is the whole point. Anything gentler would test
// less than reality (docs/plans 02 section 7.1).
package faults

// Point marks a named crash window. In the default build it does nothing and
// costs nothing: an empty leaf function is inlined away by the compiler.
func Point(string) {}

// Enabled reports whether this binary can crash itself at all. It is false
// for every build a user ever runs.
func Enabled() bool { return false }
