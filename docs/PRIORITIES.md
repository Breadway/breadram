# Priority evidence (RAM-21, partial)

Explicit rules retain existing specificity and file-order precedence. The new
priority explanation helper makes explicit rules override activity hints and
service relationship hints. A known active interaction, audio stream, inference,
or build can promote a workload; serving a protected dependency can promote it.
Missing hints leave the standalone normal default, never implicit idle status.

The debouncer promotes immediately and delays demotion until stable inactivity;
clock reversal resets the wait. No freeze or memory write is introduced.

Remaining: attach real optional hints and service dependency discovery, wire the
helper into planner and status with a versioned payload, enforce bounded protection
budgets, and test worker/dependency groups end to end. These helpers are tested
core contracts only, not a claim that desktop hints already drive management.
