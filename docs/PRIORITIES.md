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
wish, the ancestor `memory.max`, and host RAM minus a 1 GiB reserve for the
desktop and unmanaged consumers. Unknown host memory, or a remainder below the
1 MiB minimum soft limit, refuses the limit instead of inventing one. The
planner uses the host total for that calculation and says so when it refuses.
The live ancestor clamp still happens at apply time, where the cgroup limit is
known, and it is still reported rather than silent.

The applied control is `MemoryHigh`. It throttles and reclaims. It is not a
hard cap and breadram sets no default `MemoryMax`, so a budget cannot be what
kills an application. An explicit hard cap, where a caller asks for one, is
the existing `MemoryMax` path on a supervised job and is not added here.

`stall_verdict` compares PSI percent before and after a limit. A rise of one
point or more is `Reverse`: the previous value goes back through the journal,
which restores only a value breadram still owns. A flat or falling reading is
`Keep`. Cooling, a user disable, a replaced target, and a foreign manager are
already the journal's ownership rules: a changed value is left alone, a gone
target is skipped, and a crash reloads the journal rather than re-admitting
work it cannot prove it still owns.

Measured on this host with `scripts/measure-budget.sh`, two disposable user
scopes capped at 80 MiB plus 48 MiB swap so the host cannot be exhausted. The
worker touches every page of a 48 MiB buffer for one second.

| MemoryHigh | rounds in 1s | memory.current | PSI some avg10 |
| --- | --- | --- | --- |
| 72 MiB | 234 | 54,247,424 | 0.00 |
| 20 MiB | 7 | 20,787,200 | 0.00 |

The tight limit held the scope to about 20 MiB and cut throughput from 234
rounds to 7. PSI did not rise, so the verdict on this run is Keep. The
throughput cost is the stall the scope paid inside its own cap. The benefit is
the memory it no longer held. A run whose PSI rises by a point is Reverse and
the limit is put back.