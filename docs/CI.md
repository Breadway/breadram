# CPU checks (RAM-32, partial)

`bash ci/check.sh` runs formatting, warnings-as-errors lint, build and all tests
with Cargo.lock enforced. No model download, GPU, bus, credentials, or provider
configuration is required. Forgejo's self-hosted runner checks out the exact push
SHA into a fresh owned temporary directory and runs the same script on main,
feature and fix branch pushes. Rust/Cargo/rustfmt/clippy and git must already be
installed on the runner. Tests use loopback sockets and read-only host telemetry.

The workflow has not run remotely: publication is blocked and no push was retried.
Local script success is not remote CI success or clean-clone reproducibility proof.

`ci/package-check.sh` validates the CPU package name/version and explicitly
keeps release automation disabled. It reports service/config artifacts as pending
until RAM-31 merges; it does not publish or install anything.

Remaining: validated bakery package/registry metadata, binary/service/config/doc
artifacts, fresh install/upgrade/uninstall and release automation. RAM-31 service
files are only in review and are not assumed merged. RAM-29 isolated pressure and
hardware gating is still partial. No release or registry support is claimed by
this check-only implementation. RAM-32 remains doing.
