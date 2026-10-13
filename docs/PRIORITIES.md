# Priority evidence (RAM-21)

Priority is resolved in one place, `priority_with_source`, and the precedence is
fixed rather than implied:

1. An explicit rule, most specific first, ties to the earlier rule in the file.
2. Serving a protected workload: a workload whose cgroup path is an ancestor of a
   protected workload's cgroup path is protected too, so it is never reclaimed out
   from under the thing it serves. This is read from the cgroup tree the inventory
   already collected, not guessed, and it holds for `cgroup:` scopes where nesting
   is real.
3. A live activity hint: a known active interaction, audio stream, inference, or
   build promotes a workload to protected.
4. Otherwise the standalone normal default. A missing hint is never read as idle.

The planner and the status path both use `explain_priority`. The daemon folds
each pass's provider activity into a per-scope `HintState` (a `HintDebouncer` per
scope) and reasons about the debounced view through `DebouncedActivity`, so
promotion is immediate and demotion waits for `MIN_IDLE_MS` of stable absence.
A clock reversal resets the wait rather than manufacturing elapsed time. No
freeze and no memory write is introduced.

`WorkloadSummary` carries the effective `priority` and its `priority_source`
(protocol minor 5, both defaulted), and `breadram workloads` prints them, so the
decision is explained where an operator can see it rather than only in a test.

Fixtures cover conflicting rules with a specificity tie-break, a live hint that
protects a workload with no rule, a workload serving a protected cgroup, and
missing activity data that leaves the normal default. Worker groups are collapsed
to one workload in the inventory (`groups_a_service_and_its_workers_into_one_workload`),
so a worker group carries one priority.

Bounded protection budgets beyond the per scope budget and desktop hint plumbing
(the compositor or bread activity that would supply a real interaction hint) are
separate cards; this one resolves and explains the priority.

## Effective budgets (RAM-22)

A configured budget is a wish. `effective_budget` returns the smaller of that
wish, the tightest parent `memory.max`, and host RAM minus a 1 GiB reserve for
the desktop and unmanaged consumers. Unknown host memory, or a remainder below
the 1 MiB minimum soft limit, refuses the limit instead of inventing one. The
planner uses the host total and the inventory's parent-chain reading for that
calculation and says so when it refuses. The executor clamps again at apply
time against the live parent chain, where a parent that changed since the
inventory is still caught, and the clamp is reported rather than silent. The
unit's own `memory.max` is never the ancestor: it is the cap a soft limit
writes, so clamping against it would clamp the budget against itself. A parent
that reads `max` is unlimited, and an unreadable parent constrains nothing.

The applied control is `MemoryHigh`. It throttles and reclaims. It is not a
hard cap and breadram sets no default `MemoryMax`, so a budget cannot be what
kills an application. An explicit hard cap, where a caller asks for one, is
the existing `MemoryMax` path on a supervised job and is not added here.

The number written is the effective budget, not the configured wish.
`value_to_apply` calls `effective_budget` with the snapshot's host total, so a
wish that would fill the host is shrunk by the desktop reserve before the
write. A soft limit with no configured budget is refused rather than invented
from current usage. The live ancestor clamp still happens at apply time, where
the cgroup limit is known.

A stall is judged per scope, not per host. Host PSI `avg10` cannot move by a
point inside one short monitor tick, so a rule written against it never fires.
`StallWatch` instead follows each controllable unit's own cgroup
`memory.pressure` `total` counter, which accumulates in the same tick the
scope thrashes. Every pass judges every unit, so a baseline exists before any
limit is written. When a scope limited on the previous pass accumulates half a
CPU-second more stall than the tick before (`STALL_SCOPE_DELTA_RISE_US`), the
daemon puts back only that action's values through `recover_actions` and
records the action `Ineffective`, which starts its cooldown and escalates the
planner past it. Every other owned value is untouched. A scope with no pressure
reading is never reversed: a missing signal keeps the limit. The same journal
restore, over all owned values, runs when automatic mode turns off, which is
the user disable. A target that was replaced between planning and writing is
refused before the write. A value another manager changed is left alone, a
gone target is skipped, and a crash reloads the journal rather than
re-admitting work it cannot prove it still owns. A rollback re-arms its action
id, so the limit may be tried again once policy allows it; a recorded duplicate
stays transparent and never resets the confirmed state. Cooling skips the
target for the cooldown window rather than restoring it: a limit that is
helping stays in place, and a limit that thrashed was already put back by its
reversal. Every pass also records its outcomes in the bounded action history,
so cooldowns and escalation engage: successes cool the target down, failures
count, and refusals that say nothing (duplicates, replacements, unsupported
targets) are left out.

Measured on this host with `scripts/measure-budget.sh`, two disposable user
scopes capped at 80 MiB plus 48 MiB swap so the host cannot be exhausted. The
worker touches every page of a 48 MiB buffer for one second. The stall column
is the scope's own pressure `total` sampled 0.4s into the run.

| MemoryHigh | rounds in 1s | memory.current | stall µs |
| --- | --- | --- | --- |
| 72 MiB | 206 | 54,243,328 | 0 |
| 20 MiB | 7 | 20,725,760 | 106,071 |

A second run gave 224 rounds at 54,517,760 and 0 stall microseconds loose,
6 rounds at 20,697,088 and 165,876 stall microseconds tight. The tight limit
held the scope to about 20 MiB and cut throughput from over 200 rounds to
under 10. The stall signal moves where the ten-second average showed 0.00 on
both runs, and the verdict on these runs is Keep: the accumulation stays below
the half CPU-second the daemon reverses on. A scope that accumulates past it
is Reverse and only its limit is put back.