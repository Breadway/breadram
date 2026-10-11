//! breadram, the command line client for breadramd.
//!
//! Every command is a thin client of the daemon's versioned local protocol, so
//! the CLI reads exactly what the daemon knows and validates exactly what it
//! validates. Nothing here reimplements policy, and nothing here needs breadd,
//! breadsearch, BOS, or a model runtime to be installed: the only requirement is
//! a reachable daemon socket.
//!
//! Output has two forms and both are stable. `--json` prints the daemon's own
//! response types, so a script parses the same names the protocol documents. The
//! default human form is a summary of the same values and never invents one:
//! a figure that could not be read prints as `unknown`, with the reason.
//!
//! Exit codes are part of the contract and are documented in `docs/CLI.md`.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use breadram_core::config::ActionKind;
use breadram_core::ipc::{
    default_socket_path, ActionsPayload, AppliedPayload, Client, ClientError, DiagnosticsPayload,
    EventPayload, PlanPayload, RulesPayload, StatusPayload, WorkloadsPayload,
};
use breadram_core::telemetry::{SwapKind, SwapLayout, WorkloadSwap};
use breadram_core::trends::{Growth, GrowthReport};
use breadram_core::{CapabilityReport, ManagementMode};
use clap::{Parser, Subcommand};

/// The stable exit codes.
mod exit {
    /// The command succeeded.
    pub const OK: u8 = 0;
    /// Something unexpected went wrong.
    pub const FAILURE: u8 = 1;
    /// The command line was invalid.
    pub const USAGE: u8 = 2;
    /// No daemon is listening on the socket.
    pub const UNAVAILABLE: u8 = 3;
    /// The daemon understood the request and refused it.
    pub const REFUSED: u8 = 4;
    /// The daemon speaks an incompatible protocol version.
    pub const INCOMPATIBLE: u8 = 5;
}

#[derive(Parser)]
#[command(
    name = "breadram",
    version,
    about = "Inspect and steer the breadram memory daemon",
    after_help = "Exit codes: 0 success, 1 failure, 2 invalid command line, 3 no daemon, \
                  4 refused by the daemon, 5 incompatible protocol version."
)]
struct Cli {
    /// Unix socket path. Defaults to $XDG_RUNTIME_DIR/breadram.sock.
    #[arg(long, global = true, value_name = "PATH")]
    socket: Option<PathBuf>,

    /// Print JSON instead of a human summary.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show the current mode, memory status, and capabilities.
    Status,
    /// Follow status events until interrupted.
    Watch,
    /// List the workloads breadram can see.
    Workloads,
    /// Show what breadram can read and control on this host.
    Capabilities,
    /// Explain what the policy would do right now, without doing it.
    Explain,
    /// Show the settings in effect: mode, permitted actions, protection, rules.
    Rules,
    /// Show the current plan and the recorded actions, with their effect.
    Actions,
    /// Show a diagnostic bundle for a bug report.
    Diagnostics,
    /// Export bounded numeric workload memory samples from the daemon.
    Trends,
    /// Change the management mode.
    Mode {
        /// The mode to switch to: monitor or automatic.
        mode: String,
        /// Also write the change to the configuration file.
        #[arg(long)]
        persist: bool,
    },
    /// Add workloads that must not be touched automatically.
    Protect {
        /// Scope keys, for example unit:ollama.service.
        #[arg(required = true)]
        scopes: Vec<String>,
        /// Also write the change to the configuration file.
        #[arg(long)]
        persist: bool,
    },
    /// Stop protecting workloads.
    Unprotect {
        /// Scope keys to unprotect.
        #[arg(required = true)]
        scopes: Vec<String>,
        /// Also write the change to the configuration file.
        #[arg(long)]
        persist: bool,
    },
}

/// Why a command did not complete.
enum CliError {
    /// The daemon call failed.
    Client(ClientError),
    /// The command line was invalid in a way clap cannot see.
    Usage(String),
    /// A response could not be rendered.
    Encode(String),
    /// Reading or writing the terminal failed.
    Io(std::io::Error),
}

impl From<ClientError> for CliError {
    fn from(err: ClientError) -> Self {
        CliError::Client(err)
    }
}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        CliError::Io(err)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Client(err) => write!(f, "{err}"),
            CliError::Usage(message) => write!(f, "{message}"),
            CliError::Encode(message) => write!(f, "{message}"),
            CliError::Io(err) => write!(f, "{err}"),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let socket = cli.socket.clone().unwrap_or_else(default_socket_path);
    match run(&cli, &socket).await {
        Ok(()) => ExitCode::from(exit::OK),
        Err(err) => report(&err, &socket),
    }
}

async fn run(cli: &Cli, socket: &Path) -> Result<(), CliError> {
    // Validate the command line before touching the socket, so a typo is a usage
    // error with the accepted values named, not an error about a missing daemon.
    let mode = match &cli.command {
        Command::Mode { mode, .. } => Some(
            mode.parse::<ManagementMode>()
                .map_err(|err: breadram_core::ModeParseError| CliError::Usage(err.to_string()))?,
        ),
        _ => None,
    };

    let mut client = Client::connect(socket).await?;
    client.handshake("breadram-cli").await?;

    match &cli.command {
        Command::Status => {
            let status = client.status().await?;
            emit(cli.json, &status, human_status)
        }
        Command::Watch => {
            let interval_ms = client.subscribe().await?;
            if !cli.json {
                print(format!(
                    "watching status events every {interval_ms} ms, press Ctrl+C to stop"
                ))?;
            }
            loop {
                let event = client.next_event().await?;
                emit(cli.json, &event, human_event)?;
            }
        }
        Command::Workloads => {
            let payload = client.workloads().await?;
            emit(cli.json, &payload, human_workloads)
        }
        Command::Capabilities => {
            let report = client.capabilities().await?;
            emit(cli.json, &report, human_capability_text)
        }
        Command::Explain => {
            let plan = client.explain().await?;
            emit(cli.json, &plan, human_plan)
        }
        Command::Rules => {
            let rules = client.rules().await?;
            emit(cli.json, &rules, human_rules)
        }
        Command::Actions => {
            let actions = client.actions().await?;
            emit(cli.json, &actions, human_actions)
        }
        Command::Diagnostics => {
            let diagnostics = client.diagnostics().await?;
            emit(cli.json, &diagnostics, human_diagnostics)
        }
        Command::Trends => {
            let diagnostics = client.diagnostics().await?;
            if cli.json {
                print_json(&diagnostics.trends)
            } else {
                print(format!(
                    "{} memory samples retained; use --json for numeric export. Target keys are daemon-local; growth is not a leak verdict.",
                    diagnostics.trends.len()
                ))
            }
        }
        Command::Mode { persist, .. } => {
            let mode = mode.expect("the mode was parsed before the socket was opened");
            let applied = client.set_mode(mode, *persist).await?;
            emit(cli.json, &applied, human_applied)
        }
        Command::Protect { scopes, persist } => {
            let applied = client
                .set_protection(scopes.clone(), Vec::new(), *persist)
                .await?;
            emit(cli.json, &applied, human_applied)
        }
        Command::Unprotect { scopes, persist } => {
            let applied = client
                .set_protection(Vec::new(), scopes.clone(), *persist)
                .await?;
            emit(cli.json, &applied, human_applied)
        }
    }
}

/// Render a response as JSON or as a human summary.
fn emit<T: serde::Serialize>(
    json: bool,
    value: &T,
    human: fn(&T) -> String,
) -> Result<(), CliError> {
    if json {
        print_json(value)
    } else {
        print(human(value))
    }
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), CliError> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|err| CliError::Encode(format!("cannot render the response: {err}")))?;
    print(text)
}

fn print(text: String) -> Result<(), CliError> {
    use std::io::Write;
    let mut out = std::io::stdout();
    out.write_all(text.as_bytes())?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}

/// Report a failure on stderr and choose the exit code.
fn report(err: &CliError, socket: &Path) -> ExitCode {
    match err {
        CliError::Client(ClientError::Unavailable { .. }) => {
            eprintln!(
                "error: no breadram daemon is listening on {}",
                socket.display()
            );
            eprintln!("hint: start breadramd, or pass --socket with the path it is using");
            ExitCode::from(exit::UNAVAILABLE)
        }
        CliError::Client(ClientError::Version(err)) => {
            eprintln!("error: {err}");
            ExitCode::from(exit::INCOMPATIBLE)
        }
        CliError::Client(ClientError::Api(err)) => {
            eprintln!("error: the daemon refused the request: {err}");
            ExitCode::from(exit::REFUSED)
        }
        CliError::Client(err) => {
            eprintln!("error: {err}");
            ExitCode::from(exit::FAILURE)
        }
        CliError::Usage(message) => {
            eprintln!("error: {message}");
            ExitCode::from(exit::USAGE)
        }
        CliError::Encode(message) => {
            eprintln!("error: {message}");
            ExitCode::from(exit::FAILURE)
        }
        // A closed pipe is how a pager or `head` says it has seen enough.
        CliError::Io(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {
            ExitCode::from(exit::OK)
        }
        CliError::Io(err) => {
            eprintln!("error: {err}");
            ExitCode::from(exit::FAILURE)
        }
    }
}

fn human_status(status: &StatusPayload) -> String {
    let mut lines = vec![
        format!("mode:       {}", status.mode),
        format!("version:    {}", status.version),
        format!("observed:   {} ms (monotonic)", status.observed_at_ms),
        format!(
            "available:  {}",
            human_optional_bytes(status.available_bytes)
        ),
        format!("total:      {}", human_optional_bytes(status.total_bytes)),
        format!("workloads:  {}", status.workloads),
    ];
    lines.extend(human_capabilities(&status.capabilities));
    lines.join("\n")
}

fn human_event(event: &EventPayload) -> String {
    format!(
        "{} at {} ms: available {}",
        event.kind,
        event.observed_at_ms,
        human_optional_bytes(event.available_bytes)
    )
}

fn human_capability_text(report: &CapabilityReport) -> String {
    human_capabilities(report).join("\n")
}

fn human_capabilities(report: &CapabilityReport) -> Vec<String> {
    if report.capabilities.is_empty() {
        return vec!["capabilities: none reported".to_string()];
    }
    let mut lines = vec!["capabilities:".to_string()];
    for capability in &report.capabilities {
        lines.push(match capability.state.reason() {
            Some(reason) if capability.state.is_usable() => {
                format!("  {:<16} degraded: {reason}", capability.name)
            }
            Some(reason) => format!("  {:<16} unavailable: {reason}", capability.name),
            None => format!("  {:<16} available", capability.name),
        });
    }
    lines
}

fn human_workloads(payload: &WorkloadsPayload) -> String {
    let mut lines = vec![
        format!("observed:   {} ms (monotonic)", payload.observed_at_ms),
        format!("workloads:  {}", payload.workloads.len()),
    ];
    if payload.workloads.is_empty() {
        lines.push("no workloads were discovered".to_string());
    }
    for workload in &payload.workloads {
        let memory = match (workload.cgroup_bytes, workload.rss_bytes) {
            (Some(bytes), _) => format!("{} (cgroup)", human_bytes(bytes)),
            (None, Some(bytes)) => {
                format!("{} (RSS, over-counts shared pages)", human_bytes(bytes))
            }
            (None, None) => "unknown".to_string(),
        };
        let limit = match workload.limit_bytes {
            Some(bytes) => format!(", limit {}", human_bytes(bytes)),
            None => String::new(),
        };
        lines.push(format!(
            "  {:<28} {} proc, {:<12} {:<10} {:<16}{} {} (priority: {})",
            workload.scope,
            workload.processes,
            workload.control,
            workload.priority,
            memory,
            limit,
            workload.name,
            workload.priority_source
        ));
    }
    lines.extend(human_capabilities(&payload.capabilities));
    lines.join("\n")
}

fn human_plan(plan: &PlanPayload) -> String {
    let mut lines = vec![
        format!("pressure:   {}", plan.state),
        format!("channel:    {}", plan.channel),
        format!(
            "would run:  {}",
            if plan.would_execute { "yes" } else { "no" }
        ),
        format!("observed:   {} ms (monotonic)", plan.observed_at_ms),
    ];
    if plan.actions.is_empty() {
        lines.push("actions:    none".to_string());
    }
    for action in &plan.actions {
        lines.push(format!(
            "action:     {} on {} (expected host change: {})",
            action_name(action.kind),
            action.target,
            human_delta(action.expected_host_ram_delta)
        ));
        if action.requires_confirmed_idle {
            lines.push("            requires confirmed idleness".to_string());
        }
        lines.push(format!("            {}", action.reason));
    }
    lines.push("explanation:".to_string());
    lines.extend(plan.explanation.iter().map(|line| format!("  {line}")));
    lines.join("\n")
}

fn human_rules(rules: &RulesPayload) -> String {
    let mut lines = vec![
        format!("mode:       {}", rules.mode),
        format!(
            "config:     {}",
            rules
                .config_path
                .clone()
                .unwrap_or_else(|| "none loaded".to_string())
        ),
    ];
    if rules.allowed_actions.is_empty() {
        lines.push("allowed:    none, so automation would never act".to_string());
    } else {
        lines.push(format!(
            "allowed:    {}",
            rules
                .allowed_actions
                .iter()
                .map(|kind| action_name(*kind))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if rules.protected.is_empty() {
        lines.push("protected:  none".to_string());
    } else {
        lines.push(format!("protected:  {}", rules.protected.join(", ")));
    }
    for budget in &rules.budgets {
        lines.push(format!(
            "budget:     {} limited to {}",
            budget.scope,
            human_bytes(budget.max_bytes)
        ));
    }
    for rule in &rules.rules {
        lines.push(format!(
            "rule:       {} priority {:?} match {:?}",
            rule.name, rule.priority, rule.match_on
        ));
    }
    lines.join("\n")
}

fn human_actions(payload: &ActionsPayload) -> String {
    let mut lines = vec![
        format!("pressure:   {}", payload.plan.state),
        format!("planned:    {} action(s)", payload.plan.actions.len()),
        format!(
            "history:    {} of {} entries",
            payload.history.len(),
            payload.history_capacity
        ),
    ];
    if let Some(note) = &payload.history_note {
        lines.push(format!("note:       {note}"));
    }
    for action in &payload.plan.actions {
        lines.push(format!(
            "planned:    {} on {} for {}",
            action_name(action.kind),
            action.target,
            human_delta(action.expected_host_ram_delta)
        ));
    }
    for record in &payload.history {
        let kind = record
            .kind
            .map(action_name)
            .unwrap_or_else(|| "not an action".to_string());
        lines.push(format!(
            "#{} {} {} {} at {} ms: {}",
            record.id, record.state, kind, record.scope, record.at_ms, record.reason
        ));
        if record.requested_host_ram_delta.is_some() || record.observed_host_ram_delta.is_some() {
            lines.push(format!(
                "            host memory: requested {}, observed {}",
                human_delta(record.requested_host_ram_delta.unwrap_or(0)),
                match record.observed_host_ram_delta {
                    Some(delta) => human_delta(delta),
                    None => "not measured".to_string(),
                }
            ));
        }
        if record.requested_device_ram_delta.is_some() || record.observed_device_ram_delta.is_some()
        {
            lines.push(format!(
                "            device memory: requested {}, observed {}",
                human_delta(record.requested_device_ram_delta.unwrap_or(0)),
                match record.observed_device_ram_delta {
                    Some(delta) => human_delta(delta),
                    None => "not measured".to_string(),
                }
            ));
        }
        for value in &record.owned_values {
            lines.push(format!(
                "            {}: {} (was {})",
                value.key, value.last_applied, value.original
            ));
        }
    }
    lines.join("\n")
}

/// The swap, compression, and OOM layout, as reported and never changed.
fn human_swap(swap: &SwapLayout) -> Vec<String> {
    let mut lines = vec![
        format!(
            "swap areas: {} (total {}, used {})",
            swap.areas.len(),
            human_bytes(swap.total_bytes),
            human_bytes(swap.used_bytes)
        ),
        format!(
            "compression: {}",
            if swap.compressed() {
                "compressed swap in use"
            } else {
                "none detected"
            }
        ),
        format!(
            "OOM:        {} and {} OOM kill(s) since boot",
            if swap.oom.systemd_oomd_installed {
                "a systemd OOM manager is installed"
            } else {
                "no separate OOM manager detected"
            },
            human_optional_u64(swap.oom.kernel_oom_kills)
        ),
    ];
    for area in &swap.areas {
        lines.push(format!(
            "  {} ({}, {}, used {}{})",
            area.path,
            match area.kind {
                SwapKind::Partition => "partition",
                SwapKind::File => "file",
                SwapKind::Unknown => "unknown kind",
            },
            human_bytes(area.size_bytes),
            human_bytes(area.used_bytes),
            match area.priority {
                Some(priority) => format!(", priority {priority}"),
                None => String::new(),
            }
        ));
    }
    for device in &swap.zram {
        lines.push(format!(
            "  {} (compressed device, size {}, in use {})",
            device.name,
            human_optional_bytes(device.disksize_bytes),
            human_optional_bytes(device.mem_used_bytes)
        ));
    }
    if let Some(zswap) = &swap.zswap {
        // A share that could not be read is reported as unknown rather than as a
        // percentage of zero.
        let pool = match zswap.max_pool_percent {
            Some(percent) => format!("{percent}% of RAM"),
            None => "unknown".to_string(),
        };
        lines.push(format!(
            "  zswap ({}), pool up to {pool}, compressor {}",
            if zswap.enabled { "enabled" } else { "disabled" },
            zswap
                .compressor
                .clone()
                .unwrap_or_else(|| "unknown".to_string())
        ));
    }
    lines
}

/// The effective swap limit of each workload breadram can control.
fn human_workload_swap(workloads: &[WorkloadSwap]) -> Vec<String> {
    if workloads.is_empty() {
        return Vec::new();
    }
    let mut lines = vec!["workload swap:".to_string()];
    for workload in workloads {
        lines.push(format!(
            "  {} (in use {}, {})",
            workload.scope,
            human_optional_bytes(workload.swap.current_bytes),
            workload.swap.limit_phrase()
        ));
    }
    lines
}

fn human_diagnostics(diagnostics: &DiagnosticsPayload) -> String {
    let memory = &diagnostics.memory;
    let mut lines = vec![
        format!("mode:       {}", diagnostics.mode),
        format!(
            "version:    {} (IPC {}.{})",
            diagnostics.version, diagnostics.protocol_major, diagnostics.protocol_minor
        ),
        format!("socket:     {}", diagnostics.socket),
        format!("uptime:     {} ms (monotonic)", diagnostics.uptime_ms),
        format!("observed:   {} ms (monotonic)", diagnostics.observed_at_ms),
        format!(
            "config:     {}",
            diagnostics
                .config_path
                .clone()
                .unwrap_or_else(|| "none loaded".to_string())
        ),
        format!(
            "system RAM: total {}, available {}",
            human_optional_bytes(memory.ram_total_bytes),
            human_optional_bytes(memory.ram_available_bytes)
        ),
        format!(
            "swap:       total {}, used {}",
            human_optional_bytes(memory.swap_total_bytes),
            human_optional_bytes(memory.swap_used_bytes)
        ),
        format!("shared:     {}", human_optional_bytes(memory.shared_bytes)),
        format!("VRAM:       {}", human_optional_bytes(memory.vram_bytes)),
        format!(
            "iGPU GTT:   used {}, total {}",
            human_optional_bytes(memory.gtt_used_bytes),
            human_optional_bytes(memory.gtt_total_bytes)
        ),
        format!(
            "swap rate:  pages in {}, out {}",
            human_optional_u64(memory.swap_pages_in),
            human_optional_u64(memory.swap_pages_out)
        ),
        format!(
            "pressure:   {} (some avg10 {}, full avg10 {}, available {})",
            diagnostics.pressure.state,
            human_optional_float(diagnostics.pressure.some_avg10),
            human_optional_float(diagnostics.pressure.full_avg10),
            human_optional_fraction(diagnostics.pressure.available_fraction)
        ),
        format!(
            "workloads:  {} ({}, {} controllable units)",
            diagnostics.workloads, "discovered", diagnostics.control_targets
        ),
        format!(
            "jobs:       {}",
            if diagnostics.jobs_enabled {
                "admission enabled"
            } else {
                "admission disabled"
            }
        ),
        format!(
            "journal:    {} of {} entries, executing: {}",
            diagnostics.journal.entries,
            diagnostics.journal.capacity,
            diagnostics.journal.executing
        ),
    ];
    lines.extend(human_swap(&diagnostics.swap));
    lines.extend(human_workload_swap(&diagnostics.workload_swap));
    lines.extend(human_growth(&diagnostics.growth));
    lines.extend(human_capabilities(&diagnostics.capabilities));
    if !memory.notes.is_empty() {
        lines.push("memory notes:".to_string());
        lines.extend(memory.notes.iter().map(|note| format!("  {note}")));
    }
    if !diagnostics.notes.is_empty() {
        lines.push("notes:".to_string());
        lines.extend(diagnostics.notes.iter().map(|note| format!("  {note}")));
    }
    lines.join("\n")
}

fn human_growth(growth: &[GrowthReport]) -> Vec<String> {
    if growth.is_empty() {
        return Vec::new();
    }
    let mut lines = vec![format!("growth:     {} workload series", growth.len())];
    for report in growth {
        let verdict = match &report.growth {
            Growth::Sustained {
                increase_bytes,
                interval_ms,
                measured,
            } => format!(
                "sustained +{} over {} ms ({})",
                human_bytes(*increase_bytes),
                interval_ms,
                if *measured { "measured" } else { "estimated" }
            ),
            Growth::NotSustained => "not sustained".to_string(),
            Growth::Unknown => "unknown".to_string(),
        };
        lines.push(format!(
            "  series {:#018x}: {} (recorded relief {})",
            report.target_key,
            verdict,
            human_delta(report.recorded_relief_bytes)
        ));
    }
    lines
}

fn human_applied(applied: &AppliedPayload) -> String {
    let lines = [
        format!("changed:    {}", applied.summary),
        format!(
            "persisted:  {}",
            if applied.persisted {
                "written to the configuration file"
            } else {
                "no, this change lasts until the daemon restarts"
            }
        ),
        human_rules(&applied.rules),
    ];
    lines.join("\n")
}

fn action_name(kind: ActionKind) -> String {
    match serde_json::to_value(kind) {
        Ok(serde_json::Value::String(name)) => name,
        _ => format!("{kind:?}"),
    }
}

fn human_delta(delta: i64) -> String {
    match delta {
        0 => "no change".to_string(),
        delta if delta < 0 => format!("release {}", human_bytes(delta.unsigned_abs())),
        delta => format!("increase {}", human_bytes(delta as u64)),
    }
}

fn human_optional_bytes(bytes: Option<u64>) -> String {
    match bytes {
        Some(bytes) => human_bytes(bytes),
        None => "unknown".to_string(),
    }
}

fn human_optional_u64(value: Option<u64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "unknown".to_string(),
    }
}

fn human_optional_float(value: Option<f64>) -> String {
    match value {
        Some(value) => format!("{value:.2}"),
        None => "unknown".to_string(),
    }
}

fn human_optional_fraction(value: Option<f64>) -> String {
    match value {
        Some(value) => format!("{:.1}%", value * 100.0),
        None => "unknown".to_string(),
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
