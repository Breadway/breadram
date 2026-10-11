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

Remaining acceptance: daemon collection of pressure/action/recovery samples,
action-effectiveness correlation, CLI JSON and settings integration, bounded
durable storage/restart migration, and measured collection overhead. This core
is not yet a user-facing history command and RAM-28 remains doing.
