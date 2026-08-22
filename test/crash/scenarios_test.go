package crash

import "fmt"

// Scenario is one row of the crash matrix. Adding a row is one struct
// literal; the harness code does not change (issue #75: M2 through M4 extend
// the matrix without touching the driver).
//
// Kind says what the crashing child was doing when it died:
//
//	materialize  applying the job and materialising the manual trigger.
//	             The fault points sit between the tick, trigger and run
//	             writes inside the one transaction.
//	execute      claiming the run and driving its step. The fault points
//	             sit in the transition windows and around the step process.
type Scenario struct {
	// Name identifies the row in test output and in the child's selection
	// environment variable.
	Name string

	// KillAt is the faults.Point name the child is armed with. Empty
	// means no crash: the control row that must run clean through.
	KillAt string

	// Kind is materialize or execute, as above.
	Kind string

	// RetryMax sets the job's per step retry budget in the frozen spec.
	// Zero means one attempt. The recovery semantics hand a crashed
	// attempt back to the step's own policy, so the rows around the step
	// process carry RetryMax 1: they prove the bounded duplication of an
	// open window instead of a terminal failure.
	RetryMax int

	// MinEffects and MaxEffects bound how many effect lines the job may
	// have produced once the restart has converged. The contract every
	// row honours is 1 <= n <= 1 + crashes.
	MinEffects int
	MaxEffects int

	// ExpectRequeue says whether recovery must have written a
	// run.requeued event: true whenever the crash caught the run in
	// state running.
	ExpectRequeue bool

	// ExpectExecutorLost says whether recovery must have closed a running
	// attempt with STEP_FAILED_EXECUTOR_LOST: true whenever a kill landed
	// after a committed step start and before the committed verdict.
	ExpectExecutorLost bool

	// WaitsForOrphan marks the row whose crashed attempt leaves a live
	// command behind. The harness scans /proc for a process carrying the
	// workspace marker and requires it gone before it counts effects.
	WaitsForOrphan bool

	// TransientFindings names the fsck findings the crashed instant is
	// ALLOWED to carry, because the window itself produces them. The
	// finish window is the one case: the run's verdict transaction never
	// landed, so the stored state (running) genuinely disagrees with the
	// steps' aggregate (succeeded) until recovery finishes the run. The
	// harness requires the finding to be exactly the predicted one before
	// recovery, and none at all after.
	TransientFindings []string
}

// Crash points, named after the window they sit in so a failing row names
// its window in the output. These are the strings faults.Point compares
// against; they live here and nowhere else.
const (
	afterTick        = "M1:materialize:after_tick"
	afterTrigger     = "M1:materialize:after_trigger"
	afterRun         = "M1:materialize:after_run"
	claimUpdate      = "M1:transition:after_update:claim"
	startStepUpdate  = "M1:transition:after_update:start_step"
	recordUpdate     = "M1:transition:after_update:record_outcome"
	finishUpdate     = "M1:transition:after_update:finish_run"
	beforeExec       = "M1:step:before_exec"
	underExec        = "M1:step:under_exec"
	afterLogFinish   = "M1:step:after_log_finish"
	requeueCrashedPt = "M1:transition:after_update:requeue_crashed"
	unknownPoint     = "M1:nowhere:at_all"
)

func succeeded() []string { return []string{"succeeded"} }

// scenarios is the matrix. Every fault point the harness can reach has a row.
var scenarios = []Scenario{
	// The materialisation chain: three points inside one transaction. A
	// kill in any of them must leave no tick, no trigger, no run and no
	// steps behind, and the retried decision must run the job exactly
	// once.
	{
		Name: "materialize_after_tick", KillAt: afterTick, Kind: "materialize",
		MinEffects: 1, MaxEffects: 1,
	},
	{
		Name: "materialize_after_trigger", KillAt: afterTrigger, Kind: "materialize",
		MinEffects: 1, MaxEffects: 1,
	},
	{
		Name: "materialize_after_run", KillAt: afterRun, Kind: "materialize",
		MinEffects: 1, MaxEffects: 1,
	},

	// The claim window: everything rolled back, the run is still queued,
	// and execution simply claims it again. No requeue, no lost work.
	{
		Name: "claim_after_update", KillAt: claimUpdate, Kind: "execute",
		MinEffects: 1, MaxEffects: 1,
	},

	// The start-step window: the step rolls back to pending, so the
	// command never ran and cannot be duplicated. Recovery requeues the
	// run; the step then runs exactly once. This is the W5 proof.
	{
		Name: "start_step_after_update", KillAt: startStepUpdate, Kind: "execute",
		RetryMax:   1,
		MinEffects: 1, MaxEffects: 1,
		ExpectRequeue: true,
	},

	// The verdict window: the command ran and its effect landed, but the
	// verdict transaction rolled back, so the step sits running with
	// nothing recorded. Recovery closes the dead attempt with
	// STEP_FAILED_EXECUTOR_LOST and hands it to the step's retry policy;
	// the second attempt produces the second, bounded effect. Exactly
	// two effects is the measured statement of the open window.
	{
		Name: "record_outcome_after_update", KillAt: recordUpdate, Kind: "execute",
		RetryMax:   1,
		MinEffects: 2, MaxEffects: 2,
		ExpectRequeue: true, ExpectExecutorLost: true,
	},

	// The finish window: every step terminal and committed, only the
	// run's own verdict rolled back. Recovery requeues; the second
	// execution finds nothing runnable and finishes the run. No command
	// ever ran twice. The crashed instant carries one predicted finding:
	// I10, the run still saying running over succeeded steps, which is
	// the missing verdict itself. Recovery erases it.
	{
		Name: "finish_run_after_update", KillAt: finishUpdate, Kind: "execute",
		MinEffects: 1, MaxEffects: 1,
		ExpectRequeue:     true,
		TransientFindings: []string{"I10"},
	},

	// Around the step process. Armed before the spawn: the command never
	// ran, and one effect proves it. Armed while the command is alive
	// and writing: the effect landed once before the kill and once
	// after, which is the bound the write model promises for this
	// window. Armed after the log file was finalized but before its
	// metadata was committed: same bound, and the log facts must agree
	// in the end.
	{
		Name: "step_before_exec", KillAt: beforeExec, Kind: "execute",
		RetryMax:   1,
		MinEffects: 1, MaxEffects: 1,
		ExpectRequeue: true, ExpectExecutorLost: true,
	},
	{
		Name: "step_under_exec", KillAt: underExec, Kind: "execute",
		RetryMax:   1,
		MinEffects: 2, MaxEffects: 2,
		ExpectRequeue: true, ExpectExecutorLost: true,
		WaitsForOrphan: true,
	},
	{
		Name: "step_after_log_finish", KillAt: afterLogFinish, Kind: "execute",
		RetryMax:   1,
		MinEffects: 2, MaxEffects: 2,
		ExpectRequeue: true, ExpectExecutorLost: true,
	},
}

// control is the no-crash row: the same child recipe with nothing armed. It
// must run clean through, which proves the harness manufactures no crashes of
// its own.
var control = Scenario{
	Name: "no_crash_control", KillAt: "", Kind: "execute",
	MinEffects: 1, MaxEffects: 1,
}

// selfTest is a row armed with a point name that exists nowhere. The child
// must survive it and finish the run: the negative proof that arming is exact,
// not approximate.
var selfTest = Scenario{
	Name: "unknown_point_survives", KillAt: unknownPoint, Kind: "execute",
	MinEffects: 1, MaxEffects: 1,
}

// allRows is every row the matrix walks.
func allRows() []Scenario {
	return append([]Scenario{control, selfTest}, scenarios...)
}

func scenarioByName(name string) (Scenario, bool) {
	for _, sc := range allRows() {
		if sc.Name == name {
			return sc, true
		}
	}
	return Scenario{}, false
}

// allowedFinalStates is the expected set per row. Every row here converges to
// succeeded: the rows with RetryMax 1 because the recovered step takes its
// second attempt, the rest because nothing was ever half done. A row that
// legitimately may end failed would list both states; the comparison is
// against the set, not one value (issue #75 design notes).
func (sc Scenario) allowedFinalStates() []string { return succeeded() }

func (sc Scenario) describe() string {
	return fmt.Sprintf("%s [kind=%s point=%q retry=%d]", sc.Name, sc.Kind, sc.KillAt, sc.RetryMax)
}
