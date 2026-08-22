// append is the effect counting fixture for the crash harness (#75). It plays
// the part of a user job whose whole effect is one line in a file, so a test
// can count exactly how many times the job's work happened, and attribute
// each line to the attempt that caused it.
//
// It lives under testdata so no build tag, test or gate picks it up as package
// code. The harness builds it once per run with the repository module, the
// same way the runner tests build their fixture.
//
// Modes:
//
//	append <file>      write one effect line, then exit 0
//	first-drip <file>  write one effect line, then, on attempt 1 only,
//	                   keep writing to stdout until the reader goes away
//	                   or a hard cap passes; any later attempt exits at once
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "append: "+format+"\n", args...)
	os.Exit(3)
}

// attempt is the runner's PACEQ_ATTEMPT, one based, zero when absent.
func attempt() int {
	n, err := strconv.Atoi(os.Getenv("PACEQ_ATTEMPT"))
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// effectLine is the whole observable effect of this fake job. The
// idempotency key comes from the runner's context contract and survives
// crashes and retries; the attempt number says which try wrote the line; the
// nanosecond stamp breaks ties. Two attempts of one step therefore show up as
// two lines sharing one key, attempts 1 and 2.
func effectLine() string {
	key := os.Getenv("PACEQ_IDEMPOTENCY_KEY")
	if key == "" {
		key = "no-key"
	}
	return fmt.Sprintf("%s	%d	%d", key, attempt(), time.Now().UnixNano())
}

func appendEffect(path string) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		die("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := fmt.Fprintln(f, effectLine()); err != nil {
		die("write %s: %v", path, err)
	}
}

// dripCap bounds the first attempt's hang. The crash harness kills the
// executor long before this, so reaching the cap means the kill never came
// and the scenario should fail loudly rather than hang.
const dripCap = 60 * time.Second

func main() {
	if len(os.Args) < 2 {
		die("usage: append MODE [file]")
	}
	mode, args := os.Args[1], os.Args[2:]

	switch mode {
	case "append":
		if len(args) < 1 {
			die("append needs a file")
		}
		appendEffect(args[0])

	case "first-drip":
		if len(args) < 1 {
			die("first-drip needs a file")
		}
		appendEffect(args[0])
		if attempt() != 1 {
			// A retry after a crash must finish on its own: the
			// second attempt's exit is what lets the restarted
			// run converge.
			os.Exit(0)
		}
		// Keep the first attempt alive with its stdout pipe open, so
		// the under-exec kill lands inside execution. When the
		// executor dies the read end closes, the next write fails,
		// and the orphaned attempt tidies itself up instead of
		// outliving the test.
		deadline := time.Now().Add(dripCap)
		for time.Now().Before(deadline) {
			if _, err := os.Stdout.WriteString("drip\n"); err != nil {
				os.Exit(0)
			}
			time.Sleep(50 * time.Millisecond)
		}
		// Nobody killed us. Say so with a distinctive exit code the
		// harness reports as the failure it is.
		os.Exit(7)

	default:
		die("unknown mode %q", mode)
	}
}
