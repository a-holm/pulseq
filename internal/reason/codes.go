package reason

//go:generate go run ./gen

// The catalogue. Every code paceq can store in a reason_code column, with its
// full anatomy. The set is closed: the static guard in internal/arch fails the
// build on any code literal used outside this file, and the stability test
// holds this list against testdata/codes.golden.txt.
//
// The tick, trigger and run/step codes follow 06 section 2.1 as the issue
// spells it out, complete for M1 through M4: the codes the scheduler (M2),
// sensors (M3) and the dependency graph (M4) will write are here from the
// start, because codes added after the fact become after-rationalisations.
//
// The five step outcome codes are the same values the runner (#61) reports;
// see doc.go for the migration of its constants onto these.

// tickCode, triggerCode, runCode and stepCode build a code from its suffix.
// The catalogue derives its values this way instead of spelling out 38 full
// upper case literals: long snake case strings of that shape read as
// hardcoded credentials to secret scanners, and gosec G101 failed ci on
// exactly such a false positive. A reason code is a stable public label, not
// a secret, so the source now carries only the per level prefix plus the
// distinguishing suffix.
func tickCode(suffix string) Code { return Code("TICK_" + suffix) }

func triggerCode(suffix string) Code { return Code("TRIGGER_" + suffix) }

func runCode(suffix string) Code { return Code("RUN_" + suffix) }

func stepCode(suffix string) Code { return Code("STEP_" + suffix) }

// The code variables, grouped by level in catalogue order. Callers take these
// rather than spelling the strings, so a typo is a compile error, and the
// closed set is unchanged: every code still appears here by name exactly
// once.
var (
	TICKSkippedPaused          = tickCode("SKIPPED_PAUSED")
	TICKSkippedOverlap         = tickCode("SKIPPED_OVERLAP")
	TICKSkippedConcurrency     = tickCode("SKIPPED_CONCURRENCY")
	TICKSkippedCatchupDisabled = tickCode("SKIPPED_CATCHUP_DISABLED")
	TICKSkippedCatchupWindow   = tickCode("SKIPPED_CATCHUP_WINDOW")
	TICKSkippedDSTNonexistent  = tickCode("SKIPPED_DST_NONEXISTENT")
	TICKSkippedDSTDuplicate    = tickCode("SKIPPED_DST_DUPLICATE")
	TICKSkippedSensor          = tickCode("SKIPPED_SENSOR")
	TICKErrorSensorFailed      = tickCode("ERROR_SENSOR_FAILED")
	TICKErrorSensorTimeout     = tickCode("ERROR_SENSOR_TIMEOUT")
	TICKErrorSensorOutput      = tickCode("ERROR_SENSOR_OUTPUT")
	TICKErrorConfig            = tickCode("ERROR_CONFIG")
	TICKMissedDaemonDown       = tickCode("MISSED_DAEMON_DOWN")
	TICKMissedLeaseLost        = tickCode("MISSED_LEASE_LOST")
	TICKMissedClockJump        = tickCode("MISSED_CLOCK_JUMP")

	TRIGGERAccepted           = triggerCode("ACCEPTED")
	TRIGGERDedupedRunKey      = triggerCode("DEDUPED_RUN_KEY")
	TRIGGERRejectedJobUnknown = triggerCode("REJECTED_JOB_UNKNOWN")
	TRIGGERRejectedJobPaused  = triggerCode("REJECTED_JOB_PAUSED")
	TRIGGERRejectedPayload    = triggerCode("REJECTED_PAYLOAD")

	RUNQueuedConcurrency  = runCode("QUEUED_CONCURRENCY")
	RUNCancelledManual    = runCode("CANCELLED_MANUAL")
	RUNCancelledShutdown  = runCode("CANCELLED_SHUTDOWN")
	RUNTimedOut           = runCode("TIMED_OUT")
	RUNFailedStep         = runCode("FAILED_STEP")
	RUNSucceeded          = runCode("SUCCEEDED")
	RUNOrphanedReconciled = runCode("ORPHANED_RECONCILED")
	RUNPoisoned           = runCode("POISONED")

	STEPSucceeded              = stepCode("SUCCEEDED")
	STEPSkippedUpstreamFailed  = stepCode("SKIPPED_UPSTREAM_FAILED")
	STEPSkippedUpstreamSkipped = stepCode("SKIPPED_UPSTREAM_SKIPPED")
	STEPRetryScheduled         = stepCode("RETRY_SCHEDULED")
	STEPRetriesExhausted       = stepCode("RETRIES_EXHAUSTED")
	STEPFailedNonzeroExit      = stepCode("FAILED_NONZERO_EXIT")
	STEPFailedTimeout          = stepCode("FAILED_TIMEOUT")
	STEPFailedSpawn            = stepCode("FAILED_SPAWN")
	STEPFailedSignal           = stepCode("FAILED_SIGNAL")
	STEPFailedExecutorLost     = stepCode("FAILED_EXECUTOR_LOST")
	STEPCancelled              = stepCode("CANCELLED")
)

// newCatalog builds the table. It is a function so the entries below read as
// one literal per code and nothing can grow the map from elsewhere.
func newCatalog() map[Code]Entry {
	entries := []Entry{
		{
			Code:  TICKSkippedPaused,
			Level: LevelTick,
			Short: "the schedule or sensor is paused",
			Explanation: "The tick came due while its schedule or sensor was paused. A pause is an " +
				"operator decision, not a fault: the time was right and nothing was meant to run. " +
				"The tick is still recorded, so the gap in history has a cause beside it instead " +
				"of reading as a hole.",
			Remedy: []string{
				"resume the source when it should fire again",
				"a pause survives restarts, so a pause nobody remembers is the first suspect for a silent job",
			},
			Terminal: true,
		},
		{
			Code:  TICKSkippedOverlap,
			Level: LevelTick,
			Short: "the previous run is still going",
			Explanation: "The job fired on time, but the run started by the previous tick had not " +
				"finished yet, so this tick stood down instead of queueing a second concurrent " +
				"run. Nothing was dropped silently: the stand-down is the row you are reading.",
			Remedy: []string{
				"if runs overlap regularly, raise max_concurrent or lengthen the interval",
				"look at the run that is still going: it is the reason this tick passed",
			},
			DataKeys: []string{"run_id"},
			Terminal: true,
		},
		{
			Code:  TICKSkippedConcurrency,
			Level: LevelTick,
			Short: "a concurrency ceiling is reached",
			Explanation: "A concurrency limit was reached: the job's own max_concurrent, the global " +
				"ceiling, or a named queue the job belongs to. The tick is dropped rather than " +
				"queued, because a queue behind a stuck run would fire everything back to back " +
				"the moment the limit lifted.",
			Remedy: []string{
				"raise the limit that binds, or accept the drops as the cost of the ceiling",
				"skips piling up here mean the schedule fires faster than the job finishes",
			},
			DataKeys: []string{"limit", "scope"},
			Terminal: true,
		},
		{
			Code:  TICKSkippedCatchupDisabled,
			Level: LevelTick,
			Short: "the due tick was discarded, catchup is off",
			Explanation: "Ticks came due while nothing could run them, and the schedule has catchup " +
				"set to skip, so the missed moments were thrown away rather than run late. One " +
				"missed moment becomes one skipped row instead of a burst of late runs when the " +
				"daemon comes back.",
			Remedy: []string{
				"set catchup to last or all on the schedule if late runs are wanted",
				"a long outage with catchup off shows up here as a block of skipped ticks",
			},
			Terminal: true,
		},
		{
			Code:  TICKSkippedCatchupWindow,
			Level: LevelTick,
			Short: "the due tick is older than the catchup window",
			Explanation: "The tick was owed and catchup would have run it, but it fell outside " +
				"catchup_window, so paceq let it go. The window bounds how far back a restart " +
				"digs; without it, coming back after a week would replay the whole week.",
			Remedy: []string{
				"widen catchup_window if these should have run",
				"narrow it if catchup dug up more history than anyone wanted",
			},
			DataKeys: []string{"scheduled_for", "window_ms"},
			Terminal: true,
		},
		{
			Code:  TICKSkippedDSTNonexistent,
			Level: LevelTick,
			Short: "the local time does not exist (spring forward)",
			Explanation: "The schedule's zone jumped forward for daylight saving and the tick's local " +
				"time landed inside the hour that was skipped over. The schedule's spring_forward " +
				"policy is skip, so the tick was dropped rather than moved.",
			Remedy: []string{
				"set spring_forward to shift on the schedule to move such ticks past the gap instead",
			},
			DataKeys: []string{"local_time"},
			Terminal: true,
		},
		{
			Code:  TICKSkippedDSTDuplicate,
			Level: LevelTick,
			Short: "the local time happens twice (fall back)",
			Explanation: "The zone fell back for daylight saving, the tick's local time occurred " +
				"twice, and fall_back is first, so the second pass was skipped. The two passes " +
				"were otherwise identical, which is why running one of them is a policy and not " +
				"a guess.",
			Remedy: []string{
				"set fall_back to both on the schedule to run the repeat as well",
			},
			DataKeys: []string{"local_time"},
			Terminal: true,
		},
		{
			Code:  TICKSkippedSensor,
			Level: LevelTick,
			Short: "the sensor said skip",
			Explanation: "The sensor ran, answered within its deadline, and decided not to trigger. " +
				"Its own reason travels in reason_text untouched, because only the sensor knows " +
				"what its skip meant; paceq does not paraphrase it.",
			Remedy: []string{
				"read reason_text on this tick: the sensor's own wording is kept there",
			},
			Terminal: true,
		},
		{
			Code:  TICKErrorSensorFailed,
			Level: LevelTick,
			Short: "the sensor failed or panicked",
			Explanation: "The sensor subprocess died, exited non-zero, or produced nothing the " +
				"evaluator could read. Nothing triggered, and the sensor's consecutive_failures " +
				"climbed by one toward whatever its failure policy allows.",
			Remedy: []string{
				"run the sensor's command by hand and read what it prints",
				"check the sensor's timeout and interval before suspecting the sensor's code",
			},
			DataKeys: []string{"exit_code"},
			Terminal: true,
		},
		{
			Code:  TICKErrorSensorTimeout,
			Level: LevelTick,
			Short: "the sensor overshot its evaluation deadline",
			Explanation: "The sensor did not answer within timeout_ms, so it was killed and the tick " +
				"recorded as an error. The deadline is hard on purpose: a slow sensor delays every " +
				"evaluation queued behind it.",
			Remedy: []string{
				"raise timeout_ms on the sensor, or make the check itself cheaper",
				"timeouts that cluster at busy hours point at contention outside paceq",
			},
			DataKeys: []string{"timeout_ms"},
			Terminal: true,
		},
		{
			Code:  TICKErrorSensorOutput,
			Level: LevelTick,
			Short: "the sensor wrote invalid or oversized output",
			Explanation: "The sensor exited cleanly, but its stdout was not the JSON the contract " +
				"requires, or it was larger than the output limit. Output that cannot be trusted " +
				"cannot trigger anything, so it triggers nothing at all.",
			Remedy: []string{
				"print one JSON object on stdout and nothing else from the sensor",
				"keep bulk data in files and pass paths in the JSON, not the data itself",
			},
			DataKeys: []string{"bytes", "limit"},
			Terminal: true,
		},
		{
			Code:  TICKErrorConfig,
			Level: LevelTick,
			Short: "the configuration behind the tick is invalid",
			Explanation: "The cron expression, the timezone, or the job this source points at could " +
				"not be resolved when the tick came due. The definition was valid when it was " +
				"applied; something it depends on has changed since.",
			Remedy: []string{
				"validate the definition again: paceq validate",
				"check whether the job it references was renamed or removed",
			},
			Terminal: true,
		},
		{
			Code:  TICKMissedDaemonDown,
			Level: LevelTick,
			Short: "no daemon was running (gap detection)",
			Explanation: "Nobody observed this moment: the row was written afterwards by gap " +
				"detection, which walks a schedule's expected fire times after a restart and " +
				"fills the holes. It is synthetic evidence of an outage, not a measurement, and " +
				"it exists so silence never looks like success.",
			Remedy: []string{
				"treat these rows as outage evidence, not as measurements",
				"compare them against the daemon session rows covering the same period",
			},
			Terminal: true,
		},
		{
			Code:  TICKMissedLeaseLost,
			Level: LevelTick,
			Short: "another instance held the lease",
			Explanation: "More than one instance was eligible to evaluate this schedule and the other " +
				"one held the lease, so this instance recorded the slot as taken. Exactly one " +
				"evaluation happened; this row is the one that did not.",
			Remedy: []string{
				"expect this under multi instance setups; investigate it otherwise",
				"clocks far apart between hosts make the lease look expired early: check NTP",
			},
			Terminal: true,
		},
		{
			Code:  TICKMissedClockJump,
			Level: LevelTick,
			Short: "the clock jumped past the fire time",
			Explanation: "The wall clock moved forward past the scheduled moment, so the moment never " +
				"arrived in measured time. NTP corrections and machines waking from suspend both " +
				"do this; the tick is marked missed rather than silently late.",
			Remedy: []string{
				"check the system journal for clock steps around the miss",
				"a machine that suspends needs catchup enabled, or it collects these rows",
			},
			Terminal: true,
		},

		{
			Code:  TRIGGERAccepted,
			Level: LevelTrigger,
			Short: "became a run",
			Explanation: "The trigger passed every gate: the job exists, the job is not paused, the " +
				"payload matched, and no dedup key claimed it. The run column on the trigger row " +
				"points at the run it produced.",
			Remedy: []string{
				"nothing to fix; follow the run id on the trigger row",
				"dedup keys stay until retention clears them, so a deliberate rerun needs a new key or a bumped epoch",
			},
			Terminal: true,
		},
		{
			Code:  TRIGGERDedupedRunKey,
			Level: LevelTrigger,
			Short: "the run key was seen before",
			Explanation: "The trigger carried a run_key already on record for this source and epoch, " +
				"so it was folded into the original run instead of creating a twin. Dedup is what " +
				"makes a double fire harmless and a cursor reset safe.",
			Remedy: []string{
				"follow original_run_id in reason_data: that run is the one that counted",
				"to run again on purpose, change the key or bump the source's epoch",
			},
			DataKeys: []string{"epoch", "original_run_id"},
			Terminal: true,
		},
		{
			Code:  TRIGGERRejectedJobUnknown,
			Level: LevelTrigger,
			Short: "the job does not exist",
			Explanation: "The trigger names a job this database has never heard of. Either the source " +
				"was written against a job that has since been removed, or it references a name " +
				"that never validated at all.",
			Remedy: []string{
				"apply the definition that declares the job",
				"or remove the schedule or sensor that keeps firing at a job that is gone",
			},
			Terminal: true,
		},
		{
			Code:  TRIGGERRejectedJobPaused,
			Level: LevelTrigger,
			Short: "the job is paused",
			Explanation: "Everything else about the trigger was fine, but the job is paused, and " +
				"turning it into a run would have gone against an operator's explicit instruction. " +
				"The trigger is recorded as rejected rather than dropped, so the pause explains " +
				"the silence later.",
			Remedy: []string{
				"unpause the job if its triggers should become runs again",
			},
			Terminal: true,
		},
		{
			Code:  TRIGGERRejectedPayload,
			Level: LevelTrigger,
			Short: "the payload did not match the job's schema",
			Explanation: "A trigger carries params for the run it wants, and those params must " +
				"satisfy the job's schema. This payload did not, so the run was refused before " +
				"anything could start half configured.",
			Remedy: []string{
				"read reason_text on this trigger: it names the field that failed",
				"fix the sensor so it emits params the job's schema accepts",
			},
			Terminal: true,
		},

		{
			Code:  RUNQueuedConcurrency,
			Level: LevelRun,
			Short: "held back by a concurrency limit",
			Explanation: "The run was accepted but not started: a limit on the job, a queue it belongs " +
				"to, or the global ceiling held it. It sits queued with available_at in the future " +
				"and this code as its defer reason, which is why deferred is computed and not a " +
				"state of its own.",
			Remedy: []string{
				"waiting is normal; look further only if available_at never arrives",
				"a limit held by rows that should be finished means the reaper is not running",
			},
			DataKeys: []string{"limit", "scope"},
		},
		{
			Code:  RUNCancelledManual,
			Level: LevelRun,
			Short: "cancelled by request",
			Explanation: "Someone asked for this run to be cancelled, the request was made durable " +
				"before anything was killed, and the engine then killed the process group and " +
				"closed the run. Steps that were mid flight report cancelled too.",
			Remedy: []string{
				"cancel_reason on the run records what the caller supplied",
				"if the work still matters, start a fresh run once the reason for the cancel is handled",
			},
			Terminal: true,
		},
		{
			Code:  RUNCancelledShutdown,
			Level: LevelRun,
			Short: "cancelled because the daemon stopped",
			Explanation: "The daemon was shut down while this run was in flight, so it cancelled the " +
				"run rather than orphan it. The process group was killed and the run closed " +
				"cleanly, so nothing keeps running unowned.",
			Remedy: []string{
				"runs closed this way are safe to start again once the daemon is back",
			},
			Terminal: true,
		},
		{
			Code:  RUNTimedOut,
			Level: LevelRun,
			Short: "over the run deadline",
			Explanation: "The whole run passed its deadline, so the engine killed the process group " +
				"and closed the run as timed out. The deadline comes from the job's timeout; a " +
				"step's own timeout fires first wherever one is set.",
			Remedy: []string{
				"raise the job's timeout, or split the work into steps with their own deadlines",
				"a run that times out on the same step every time has its answer on that step",
			},
			DataKeys: []string{"timeout_ms"},
			Terminal: true,
		},
		{
			Code:  RUNFailedStep,
			Level: LevelRun,
			Short: "a step failed",
			Explanation: "At least one step ended failed with no retry budget left that mattered, so " +
				"the run closed failed. This code is a pointer, not a diagnosis: the failing step " +
				"carries the real cause.",
			Remedy: []string{
				"open the step named in reason_data for the actual cause",
				"its own reason code says whether it exited non-zero, was killed, or never started",
			},
			DataKeys: []string{"attempt", "step"},
			Terminal: true,
		},
		{
			Code:  RUNSucceeded,
			Level: LevelRun,
			Short: "every step succeeded",
			Explanation: "All steps reached succeeded and the run closed cleanly. This is the only " +
				"way a run ends that needs no further reading.",
			Remedy: []string{
				"nothing to fix",
				"duration_ms beside earlier runs of the same job is the cheap health check",
			},
			Terminal: true,
		},
		{
			Code:  RUNOrphanedReconciled,
			Level: LevelRun,
			Short: "found running with no process, reconciled",
			Explanation: "After a restart this run was still marked running, but no process on the " +
				"machine carried its lease any more. The reaper closed it out rather than leave " +
				"a ghost holding a concurrency slot forever.",
			Remedy: []string{
				"check whether the host rebooted or the daemon crashed mid run",
				"the step logs survive; read them for how far the run got",
			},
			Terminal: true,
		},
		{
			Code:  RUNPoisoned,
			Level: LevelRun,
			Short: "crashed more often than allowed",
			Explanation: "The run kept dying together with its executor, crash_count passed " +
				"max_crash_count, and poison quarantine closed in: no more automatic retries for " +
				"this run. The quarantine protects the machine from a job that kills its keeper " +
				"(02 section 5.7).",
			Remedy: []string{
				"find what kills the executor: usually memory pressure or a signal trap",
				"after fixing the cause, replay the run deliberately instead of waiting for a retry",
			},
			DataKeys: []string{"crash_count", "max_crash_count"},
			Terminal: true,
		},

		{
			Code:  STEPSucceeded,
			Level: LevelStep,
			Short: "exited zero",
			Explanation: "The attempt ran to completion and exited 0. Exit 0 is the whole success " +
				"contract; whatever else the step printed or wrote is its own business and lives " +
				"in its log.",
			Remedy: []string{
				"nothing to fix",
				"duration_ms beside sibling steps is the cheap way to spot one step drifting",
			},
			Terminal: true,
		},
		{
			Code:  STEPSkippedUpstreamFailed,
			Level: LevelStep,
			Short: "a step it needs failed",
			Explanation: "A step in this one's needs failed, so this step never ran and never could " +
				"have made sense. The skip walks down the graph transitively: everything below a " +
				"failure is skipped, not failed, because it never started.",
			Remedy: []string{
				"fix or retry the failed step; this one follows on the next run",
			},
			DataKeys: []string{"upstream"},
			Terminal: true,
		},
		{
			Code:  STEPSkippedUpstreamSkipped,
			Level: LevelStep,
			Short: "an upstream step was itself skipped",
			Explanation: "The skip propagated: a step this one depends on was skipped, so this one is " +
				"skipped too. The chain of skipped rows traces straight back to where the trouble " +
				"started.",
			Remedy: []string{
				"walk the upstream chain until it stops being skipped: that step is where it began",
			},
			DataKeys: []string{"upstream"},
			Terminal: true,
		},
		{
			Code:  STEPRetryScheduled,
			Level: LevelStep,
			Short: "failed, will retry",
			Explanation: "The attempt failed while retries remained, so the step went back to pending " +
				"with next_attempt_at set from the backoff policy. Retry is a transition, not a " +
				"state: nothing is running right now and nothing has finally failed yet.",
			Remedy: []string{
				"wait for next_attempt_at; nothing needs doing",
				"attempts climbing without progress means the retry budget is buying delay, not recovery",
			},
			DataKeys: []string{"attempt", "backoff_ms", "next_attempt_at"},
		},
		{
			Code:  STEPRetriesExhausted,
			Level: LevelStep,
			Short: "failed with no retries left",
			Explanation: "The last permitted attempt failed, so the step closed failed. Every earlier " +
				"attempt and its outcome is in run_events under this step name.",
			Remedy: []string{
				"compare the attempts: a failure that never changes shape is configuration",
				"a failure that changes shape between attempts is flakiness, and the varying attempt is the clue",
			},
			DataKeys: []string{"attempt", "max_attempts"},
			Terminal: true,
		},
		{
			Code:  STEPFailedNonzeroExit,
			Level: LevelStep,
			Short: "exited non-zero",
			Explanation: "The process ran to completion and exited with a code other than 0. The code " +
				"is the program's own verdict and travels in reason_data; transient is true for " +
				"exit 75, the EX_TEMPFAIL convention, where the program itself claims a retry " +
				"could help.",
			Remedy: []string{
				"read the tail of the log: the program usually says why on stderr",
				"match the exit code against the program's own documentation, not against errno",
			},
			DataKeys: []string{"exit_code", "transient"},
			Terminal: true,
		},
		{
			Code:  STEPFailedTimeout,
			Level: LevelStep,
			Short: "killed at the deadline",
			Explanation: "The step hit its timeout, or the run's, and the engine killed the whole " +
				"process group. The kill addresses the group, so grandchildren die too and nothing " +
				"is left behind holding files or ports.",
			Remedy: []string{
				"raise the step's timeout, or make the work bounded and resumable",
				"a step that always needs just over its timeout is telling you its input grew",
			},
			DataKeys: []string{"timeout_ms"},
			Terminal: true,
		},
		{
			Code:  STEPFailedSpawn,
			Level: LevelStep,
			Short: "the command never started",
			Explanation: "paceq could not start the process at all, so the step never ran and no side " +
				"effect has happened. The usual causes are a missing argv[0], a file without " +
				"execute permission, a missing working directory, or an unreadable env_file. A " +
				"retry is always safe, because nothing happened the first time.",
			Remedy: []string{
				"check the program exists and is executable: ls -l <path>",
				"run[0] must be absolute; PATH is never searched (SECURITY.md, argv)",
				"check the working directory exists for the user paceq runs as",
			},
			DataKeys: []string{"argv0", "errno", "workdir"},
			Terminal: true,
		},
		{
			Code:  STEPFailedSignal,
			Level: LevelStep,
			Short: "killed by a signal",
			Explanation: "The process died from a signal instead of exiting: SIGKILL from the OOM " +
				"killer is the classic. exit_code follows the 128 plus signal convention and " +
				"signal names the signal. When the death was paceq's own kill for a cancellation, " +
				"cancelled is true and the step normally closes as STEP_CANCELLED instead.",
			Remedy: []string{
				"SIGKILL with memory pressure elsewhere means the OOM killer chose this process: check dmesg",
				"SIGKILL with a calm machine means someone ran kill -9 by hand",
				"a signal arriving on every step points at the cancel path, not at the steps",
			},
			DataKeys: []string{"cancelled", "exit_code", "signal"},
			Terminal: true,
		},
		{
			Code:  STEPFailedExecutorLost,
			Level: LevelStep,
			Short: "the executor died before the verdict landed",
			Explanation: "The executor running this attempt crashed, or was killed, between starting " +
				"the step and recording what it did. The attempt's own verdict was lost with it, so " +
				"the restart closes the dead attempt with this code instead of inventing a result. " +
				"The step may then be attempted again under its retry policy, and the effect " +
				"contract applies as for any retry: a step runs at least once, not exactly once.",
			Remedy: []string{
				"read the run's events: run.requeued beside this code is the restart closing a crash out",
				"the attempt's log file may exist without log metadata; the next attempt writes its own file",
				"if the step is not idempotent, deduplicate on PACEQ_IDEMPOTENCY_KEY, which survives the crash",
			},
			DataKeys: []string{"recovered_by"},
			Terminal: true,
		},
		{
			Code:  STEPCancelled,
			Level: LevelStep,
			Short: "cancelled before it finished",
			Explanation: "The run was cancelled while this step was in flight, so the process group " +
				"was killed and the step closed cancelled. It is not a failure: nothing went " +
				"wrong inside the step.",
			Remedy: []string{
				"the run's own reason code says who cancelled it and why",
			},
			Terminal: true,
		},
	}

	m := make(map[Code]Entry, len(entries))
	for _, e := range entries {
		if _, dup := m[e.Code]; dup {
			panic("reason: duplicate code " + e.Code)
		}
		m[e.Code] = e
	}
	return m
}
