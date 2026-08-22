package engine

import (
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/store"
)

// DefaultPollInterval is how often a running step's cancellation request is
// re-read while the process runs. Between steps the request is read directly,
// so this only bounds how long a cancel can stay invisible mid-run.
const DefaultPollInterval = 2 * time.Second

// Engine executes runs. It owns no state of its own beyond its wiring: every
// fact about a run lives in the store, and every decision about a transition
// lives in internal/model. What is left here is sequencing and the one thing
// nothing else may do, running processes, which happens strictly between
// transactions, never inside one.
type Engine struct {
	// Store is the database. All reads and writes go through it; the
	// engine holds no handle of its own and opens no transaction.
	Store *store.Store

	// LogRoot is where step logs are written. Paths stored in the
	// database are relative to it.
	LogRoot logsink.Root

	// Clock drives every timing decision: poll ticks, deadlines, the
	// stamps handed to the runner. Never time directly.
	Clock clock.Clock

	// Owner is the name this executor claims leases under. Every write
	// the engine makes on a claimed run must come from the same name.
	Owner string

	// PollInterval bounds how long a cancellation can wait to be seen
	// while a step runs. Zero means DefaultPollInterval.
	PollInterval time.Duration

	// StepTimeoutDefault applies when neither the job nor the step names
	// a timeout. Zero means runner.DefaultTimeout.
	StepTimeoutDefault time.Duration

	// LeaseTTL is how long ExecuteRun's claim lasts. Zero means
	// store.DefaultLeaseTTL. The crash harness sets it short so its
	// recovery after a SIGKILL does not have to wait out five minutes of
	// lease nobody will ever renew; production leaves it alone.
	LeaseTTL time.Duration
}

func (e *Engine) pollInterval() time.Duration {
	if e.PollInterval > 0 {
		return e.PollInterval
	}
	return DefaultPollInterval
}
