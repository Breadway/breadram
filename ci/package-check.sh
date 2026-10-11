#!/usr/bin/env bash
# Validate CPU package metadata against the workspace. Service files belong to
# RAM-31 and are not assumed present before that branch merges.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
meta="$root/packaging/breadram.pkg.toml"
version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$root/Cargo.toml" | head -n 1)
name=$(sed -n 's/^name = "\(.*\)"/\1/p' "$meta")
meta_version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$meta")
test "$name" = "breadram"
test "$meta_version" = "$version"
test -f "$root/README.md"
test -f "$root/docs/INSTALL.md" || test ! -e "$root/packaging/breadram.service"
grep -q 'no_gpu = true' "$meta"
grep -q 'release_automation = false' "$meta"
if [[ ! -x "$root/packaging/install.sh" || ! -f "$root/packaging/breadram.service" ]]; then
  echo "package metadata consistent for breadram $version; service artifacts pending RAM-31 merge" >&2
  exit 0
fi
echo "package metadata and service artifacts consistent: breadram $version"
