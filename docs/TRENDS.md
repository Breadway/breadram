# Numeric trend core (RAM-28, partial)

TrendHistory bounds samples globally by configured count and age. Numeric samples
use an opaque target key plus generation, timestamp, optional bytes and measured
versus estimated accounting. JSON export cannot contain argv, environment, prompts,
completion text, endpoints or credentials because its schema has no text fields.
No disk is written. It starts empty after restart.

At least three strictly increasing, timestamp-ordered values establish sustained
growth, with interval, byte increase and accounting confidence. Growth is not a
memory-leak diagnosis. Missing readings, decreasing counters, reused generation,
and duplicate times produce unknown. Clock reversal clears the timeline; aged
samples are excluded even when collection has stopped. Estimated/shared RSS is
never promoted to measured memory.

The daemon collects workload memory from its existing completed inventory pass,
without another procfs scan. `breadram trends --json` exports those samples;
`breadram diagnostics --json` includes them in the defaulted `trends` field
(protocol 1.3). Keys use a per-daemon randomized hash, not scope names or a
stable cross-restart identity. Samples restart empty rather than joining epochs.

Limits use `[history]`: `max_records`, `max_age_days`, and `max_bytes`.
The collector reserves 256 bytes per sample and additionally caps retained count
at 1024 records to leave room in the 1 MiB IPC response. This is an in-memory
budget, not a durable disk-history claim. Limits shrink and expired samples are
removed on the next monitor pass, even with no workloads. Export also excludes
expired records. Both exact cgroup accounting and estimated RSS retain their
original confidence.

Remaining acceptance: pressure/action/recovery samples, action-effectiveness
correlation, settings integration, durable storage/restart migration if required,
and measured collection overhead. RAM-28 remains doing.
