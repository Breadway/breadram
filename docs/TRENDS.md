# Numeric trend core (RAM-28)

TrendHistory bounds samples globally by configured count and age. Numeric samples
use an opaque target key plus generation, timestamp, optional bytes, measured
versus estimated accounting, a numeric kind, and an optional signed delta. JSON
export cannot contain argv, environment, prompts, completion text, endpoints or
credentials because its schema has no text fields. No disk is written. It starts
empty after restart.

Four kinds are recorded. A **workload** sample carries the per-workload byte
figure from the existing completed inventory pass, without another procfs scan.
A **host** sample carries host available memory, and a **pressure** sample
carries the pressure state as an ordered level, both once per pass. A
**recovery** sample carries the measured host memory change from a journal
entry, on the same opaque key as its target, so the following workload samples
can be read as relief rather than mistaken for growth.

At least three strictly increasing, timestamp-ordered workload values establish
sustained growth, with interval, byte increase and accounting confidence. Growth
is not a memory-leak diagnosis. Missing readings, decreasing counters, reused
generation, and duplicate times produce unknown. Clock reversal clears the
timeline; aged samples are excluded even when collection has stopped.
Estimated/shared RSS is never promoted to measured memory.

The growth judgement is reachable by an operator, not only by unit tests:
`breadram diagnostics` (including `--json`) reports one `growth` entry per
retained workload, paired with the net measured relief recorded for the same
target, so a rising figure is never read without the relief that also happened.
`breadram trends --json` exports the raw samples. Both are in the diagnostics
payload at protocol 1.4 (the `trends` field arrived at 1.3 and `growth` at 1.4;
both are defaulted). Keys use a per-daemon randomized hash, not scope names or a
stable cross-restart identity. Samples restart empty rather than joining epochs.

Limits use `[history]`: `max_records`, `max_age_days`, and `max_bytes`.
The collector reserves 256 bytes per sample and additionally caps retained count
at 1024 records to leave room in the 1 MiB IPC response. This is an in-memory
budget, not a durable disk-history claim. Limits shrink and expired samples are
removed on the next monitor pass, even with no workloads. Export also excludes
expired records. Both exact cgroup accounting and estimated RSS retain their
original confidence.

Remaining acceptance: the bos-settings memory panel is RAM-27, which builds on
this card. Durable storage or cross-restart migration, if ever required, and
measured collection overhead are not claimed here. Every figure above is
in-memory and observation based.