// Package crash is the crash test harness of issue #75. It kills a real
// process at every named point the M1 write model can be interrupted, then
// requires the restart to converge without an invariant violation, without an
// orphaned row and with the job's effects still inside their bound.
//
// Everything here lives in _test.go files: the harness is test code, and a
// plain build of the module carries none of it. The crashing side is this
// package's own test binary, compiled once per run with -tags pulseq_faults,
// which is the only build in the pipeline where internal/faults carries the
// SIGKILL machinery.
//
// Layout:
//
//	scenarios_test.go  one struct literal per row of the matrix
//	harness_test.go    workspace, child process, restart, assertions
//	crash_test.go      the matrix itself, the child entry point, the
//	                   planted-violation proofs, the recovery-kill proof
package crash
