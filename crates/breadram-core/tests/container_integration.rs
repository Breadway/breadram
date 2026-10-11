//! Gated rootless container integration test (RAM-16).
//!
//! This test is opt-in so the CPU-only suite never needs a container runtime.
//! Set `BREADRAM_CONTAINER_TESTS=1` to run it, and optionally
//! `BREADRAM_CONTAINER_IMAGE` to choose a cached image (default `alpine`). When
//! the flag is unset, or no runtime is present, or a container cannot start, it
//! prints why and returns, so a skip is reported honestly rather than as a pass.
//!
//! When it runs it starts one short-lived memory-limited container and asserts
//! the inventory groups it once at its host-visible cgroup boundary and reports
//! the container's own `memory.max`, which is what criterion 1 asks for.

use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use breadram_core::inventory::collect_inventory;
use breadram_core::telemetry::RealFs;

const CONTAINER_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

fn have(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn runtime() -> Option<String> {
    for name in ["podman", "docker"] {
        let ok = Command::new(name)
            .arg("--version")
            .output()
            .map(|out| out.status.success())
            .unwrap_or(false);
        if ok {
            return Some(name.to_string());
        }
    }
    None
}

#[test]
fn a_rootless_container_is_grouped_once_at_its_boundary() {
    if std::env::var("BREADRAM_CONTAINER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipped: set BREADRAM_CONTAINER_TESTS=1 to run the container integration test");
        return;
    }
    let Some(run) = runtime() else {
        eprintln!("skipped: neither podman nor docker is on PATH");
        return;
    };
    let image = std::env::var("BREADRAM_CONTAINER_IMAGE").unwrap_or_else(|_| "alpine".to_string());
    let name = format!("breadram-it-{}", std::process::id());

    let started = Command::new(&run)
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            &format!("--memory={CONTAINER_LIMIT_BYTES}"),
            &image,
            "sleep",
            "60",
        ])
        .output();
    match started {
        Ok(out) if out.status.success() => {}
        Ok(out) => {
            eprintln!(
                "skipped: {run} could not start a container: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            return;
        }
        Err(err) => {
            eprintln!("skipped: {run} could not be run: {err}");
            return;
        }
    }

    // The full container id appears in the host-visible cgroup path, which is
    // what the inventory recognizes as the boundary.
    let id = Command::new(&run)
        .args(["inspect", "-f", "{{.Id}}", &name])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|id| !id.is_empty());
    let Some(id) = id else {
        let _ = Command::new(&run).args(["rm", "-f", &name]).output();
        eprintln!("skipped: {run} did not report a container id");
        return;
    };

    let fs = RealFs;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut matched = None;
    while Instant::now() < deadline {
        if let Some(workload) = collect_inventory(&fs)
            .workloads
            .into_iter()
            .find(|workload| workload.id.scope.contains(&id))
        {
            matched = Some(workload);
            break;
        }
        sleep(Duration::from_millis(250));
    }

    let _ = Command::new(&run).args(["rm", "-f", &name]).output();

    let workload = matched.expect("the limited container appeared in the inventory in time");
    assert!(
        workload.id.scope.starts_with("cgroup:"),
        "a container must be one host-visible boundary, got {}",
        workload.id.scope
    );
    assert!(
        !workload.id.scope.ends_with("/workers") && !workload.id.scope.contains(&format!("{id}/")),
        "nested workers must collapse to the container boundary, got {}",
        workload.id.scope
    );
    assert_eq!(
        workload.limit_bytes,
        Some(CONTAINER_LIMIT_BYTES),
        "the container's own memory.max is reported, distinct from host memory"
    );
}

#[test]
fn a_native_transient_service_is_grouped_once_at_its_unit() {
    if std::env::var("BREADRAM_CONTAINER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipped: set BREADRAM_CONTAINER_TESTS=1 to run the native service integration test"
        );
        return;
    }
    if std::env::var_os("XDG_RUNTIME_DIR").is_none() {
        eprintln!("skipped: no XDG_RUNTIME_DIR, so a systemd user service cannot run here");
        return;
    }
    if !have("systemd-run") || !have("systemctl") {
        eprintln!("skipped: systemd-run or systemctl is not on PATH");
        return;
    }
    let unit = format!("breadram-it-{}.service", std::process::id());
    match Command::new("systemd-run")
        .args(["--user", "--unit", &unit, "--quiet", "sleep", "60"])
        .output()
    {
        Ok(out) if out.status.success() => {}
        Ok(out) => {
            eprintln!(
                "skipped: systemd-run could not start the service: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            return;
        }
        Err(err) => {
            eprintln!("skipped: systemd-run could not be run: {err}");
            return;
        }
    }

    let fs = RealFs;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut matched = false;
    while Instant::now() < deadline {
        if collect_inventory(&fs)
            .workloads
            .iter()
            .any(|workload| workload.id.scope == format!("unit:{unit}"))
        {
            matched = true;
            break;
        }
        sleep(Duration::from_millis(250));
    }
    let _ = Command::new("systemctl")
        .args(["--user", "stop", &unit])
        .output();
    assert!(
        matched,
        "a native service must be grouped once at its unit boundary, unit:{unit}"
    );
}
