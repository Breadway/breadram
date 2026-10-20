#!/bin/sh
# Missing scopes must not be accepted as measurements of zero.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$root/lib.sh"
for path in "" /breadram-bench-intentionally-missing; do
    eval "$(sample_scope "$path")"
    [ "$valid" = 0 ] || exit 1
done
echo 'ok: missing cgroups invalidate the sample'
