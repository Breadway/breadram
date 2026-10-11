#![cfg(unix)]
//! End to end tests for the command surface the CLI drives.
//!
//! These spawn the real daemon and talk to it with the real client, so the new
//! commands are verified through the same interface a user reaches. What is
//! checked here is that the daemon answers with live host data, that an unknown
//! figure stays unknown, and that a configuration change is validated, applied,
//! and only written to disk when it is asked for.

use std::path::{Path, PathBuf};
mod support;
use support::{wait_until_ready, Daemon};

use breadram_core::config::Config;
use breadram_core::ipc::Client;
use breadram_core::ManagementMode;

fn temp_socket(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "breadram-surface-{tag}-{}.sock",
        std::process::id()
    ))
}

fn temp_config(tag: &str, text: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "breadram-surface-{tag}-{}.toml",
        std::process::id()
    ));
    std::fs::write(&path, text).expect("write the test configuration");
    path
}

async fn connect(socket: &Path) -> Client {
    let mut client = Client::connect(socket).await.expect("connect");
    client.handshake("surface-test").await.expect("handshake");
    client
}

#[tokio::test]
async fn every_command_answers_with_live_data() {
    let socket = temp_socket("live");
    let _daemon = Daemon::start(&socket, None);
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let status = client.status().await.expect("status");
    assert_eq!(status.mode, ManagementMode::Monitor);
    assert!(status.total_bytes.unwrap_or(0) > 0, "the host has memory");

    let workloads = client.workloads().await.expect("workloads");
    assert!(
        workloads.capabilities.is_usable("proc_inventory"),
        "the inventory capability must be reported"
    );
    for workload in &workloads.workloads {
        assert!(
            workload.cgroup_bytes.is_some() || workload.rss_bytes.is_some(),
            "{} has no memory figure at all",
            workload.scope
        );
        assert!(
            workload.control == "eligible" || workload.control == "observe_only",
            "{} reported an unknown control level {}",
            workload.scope,
            workload.control
        );
        if workload.estimated {
            assert!(
                workload.cgroup_bytes.is_none(),
                "{} claims exact accounting and an estimate",
                workload.scope
            );
        }
    }

    let plan = client.explain().await.expect("explain");
    assert_eq!(
        plan.channel, "dry_run",
        "the daemon explains what automatic mode would do"
    );
    assert!(
        !plan.would_execute,
        "nothing may claim it would run while the daemon does not execute"
    );
    assert!(
        ["healthy", "recovering", "pressured", "critical"].contains(&plan.state.as_str()),
        "unknown pressure state {}",
        plan.state
    );
    assert!(
        !plan.explanation.is_empty(),
        "a plan always explains itself, even when it proposes nothing"
    );

    let rules = client.rules().await.expect("rules");
    assert_eq!(rules.mode, ManagementMode::Monitor);
    assert!(
        rules.allowed_actions.is_empty(),
        "the default configuration allows no automatic action"
    );
    assert!(
        rules.config_path.is_none(),
        "no configuration file was given"
    );

    let actions = client.actions().await.expect("actions");
    assert!(actions.history.is_empty(), "nothing has been executed");
    assert!(
        actions.history_note.is_some(),
        "an empty history must say why it is empty"
    );
    assert!(actions.history_capacity > 0);

    let diagnostics = client.diagnostics().await.expect("diagnostics");
    assert!(
        diagnostics.memory.ram_total_bytes.unwrap_or(0) > 0,
        "the host has RAM"
    );
    match diagnostics.memory.swap_total_bytes {
        Some(0) => assert!(
            diagnostics.memory.swap_used_bytes.is_none(),
            "no swap must not be reported as full swap"
        ),
        Some(_) => {}
        None => panic!("swap_total_bytes must be known"),
    }
    assert!(
        diagnostics.memory.vram_bytes.is_none(),
        "device memory must not be invented from host memory"
    );
    assert!(
        diagnostics
            .memory
            .notes
            .iter()
            .any(|note| note.contains("VRAM")),
        "an unknown figure must carry its reason"
    );
    assert!(
        diagnostics.uptime_ms >= diagnostics.observed_at_ms,
        "the uptime and the observation come from the same monotonic clock"
    );
    assert!(!diagnostics.journal.executing);
    assert_eq!(diagnostics.socket, socket.display().to_string());
}

#[tokio::test]
async fn a_mode_change_without_a_configuration_file_is_refused_and_changes_nothing() {
    let socket = temp_socket("no-config");
    let _daemon = Daemon::start(&socket, None);
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let err = client
        .set_mode(ManagementMode::Automatic, true)
        .await
        .expect_err("a change that cannot be written must be refused");
    let message = err.to_string();
    assert!(
        message.contains("no configuration file was loaded"),
        "the refusal must explain itself: {message}"
    );

    let rules = client.rules().await.expect("rules");
    assert_eq!(
        rules.mode,
        ManagementMode::Monitor,
        "a refused change must not be applied in memory either"
    );

    // The same change without persistence is allowed and clearly temporary.
    let applied = client
        .set_mode(ManagementMode::Automatic, false)
        .await
        .expect("an in-memory change");
    assert!(!applied.persisted);
    assert_eq!(applied.rules.mode, ManagementMode::Automatic);
    assert_eq!(
        client.status().await.expect("status").mode,
        ManagementMode::Automatic
    );
}

#[tokio::test]
async fn a_mode_change_can_be_written_to_the_configuration_file() {
    let socket = temp_socket("persist");
    let config_path = temp_config("persist", "version = 1\nmode = \"monitor\"\n");
    let _daemon = Daemon::start(&socket, Some(&config_path));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let rules = client.rules().await.expect("rules");
    assert_eq!(
        rules.config_path.as_deref(),
        Some(config_path.display().to_string().as_str())
    );

    let applied = client
        .set_mode(ManagementMode::Automatic, true)
        .await
        .expect("a persisted change");
    assert!(applied.persisted, "the change was asked to be written");
    assert_eq!(applied.rules.mode, ManagementMode::Automatic);

    // The file must be readable as a configuration in its own right, so a
    // restart comes back with the same settings.
    let text = std::fs::read_to_string(&config_path).expect("read the configuration back");
    let reloaded = Config::from_toml(&text).expect("the written configuration must be valid");
    assert_eq!(reloaded.mode, ManagementMode::Automatic);

    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
async fn protection_is_applied_idempotently_and_is_reversible() {
    let socket = temp_socket("protect");
    let config_path = temp_config("protect", "version = 1\nmode = \"monitor\"\n");
    let _daemon = Daemon::start(&socket, Some(&config_path));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let applied = client
        .set_protection(vec!["unit:sshd.service".to_string()], Vec::new(), false)
        .await
        .expect("protect a workload");
    assert!(applied.summary.contains("protected unit:sshd.service"));
    assert_eq!(
        applied.rules.protected,
        vec!["unit:sshd.service".to_string()]
    );

    let err = client
        .set_protection(vec!["unit:sshd.service".to_string()], Vec::new(), false)
        .await
        .expect_err("protecting twice must not be reported as a change");
    assert!(err.to_string().contains("nothing to change"), "{err}");

    let applied = client
        .set_protection(Vec::new(), vec!["unit:sshd.service".to_string()], false)
        .await
        .expect("unprotect a workload");
    assert!(applied.rules.protected.is_empty());

    let _ = std::fs::remove_file(&config_path);
}
