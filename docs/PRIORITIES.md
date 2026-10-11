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