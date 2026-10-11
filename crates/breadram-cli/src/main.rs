//! breadram: the command line client.
//!
//! Talks to the daemon over the versioned local socket. Exit codes are stable:
//! 0 on success, 1 when the daemon is unavailable or returns an error, and 2 for
//! a usage error.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use breadram_core::ipc::{
    default_socket_path, Client, ClientError, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use clap::{Parser, Subcommand};

const CLIENT_NAME: &str = "breadram-cli";

#[derive(Parser)]
#[command(
    name = "breadram",
    version,
    about = "breadram memory management client"
)]
struct Cli {
    /// Unix socket path. Defaults to $XDG_RUNTIME_DIR/breadram.sock.
    #[arg(long, global = true, value_name = "PATH")]
    socket: Option<PathBuf>,

    /// Print machine readable JSON.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print the client version and the IPC contract it speaks.
    Version,
    /// Print the daemon's current status snapshot.
    Status,
    /// Print the daemon's capability report.
    Capabilities,
    /// Stream status events until interrupted.
    Watch,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let socket = cli.socket.clone().unwrap_or_else(default_socket_path);

    match cli.command.unwrap_or(Command::Status) {
        Command::Version => {
            println!("{} {}", breadram_core::PRODUCT_NAME, breadram_core::VERSION);
            ExitCode::SUCCESS
        }
        Command::Status => match run_status(&socket, cli.json).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(code) => code,
        },
        Command::Capabilities => match run_capabilities(&socket, cli.json).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(code) => code,
        },
        Command::Watch => match run_watch(&socket).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(code) => code,
        },
    }
}

async fn connect(socket: &Path) -> Result<Client, ExitCode> {
    let mut client = match Client::connect(socket).await {
        Ok(client) => client,
        Err(err) => {
            report_unavailable(&err, socket);
            return Err(ExitCode::from(1));
        }
    };
    if let Err(err) = client.handshake(CLIENT_NAME).await {
        report_unavailable(&err, socket);
        return Err(ExitCode::from(1));
    }
    Ok(client)
}

fn report_unavailable(err: &ClientError, socket: &Path) {
    eprintln!("error: {err}");
    eprintln!("socket: {}", socket.display());
    eprintln!("hint: start the daemon with `breadramd`, or `systemctl --user start breadramd`");
}

async fn run_status(socket: &Path, json: bool) -> Result<(), ExitCode> {
    let mut client = connect(socket).await?;
    match client.status().await {
        Ok(status) => {
            if json {
                print_json(&status)?;
            } else {
                println!(
                    "breadramd {} (IPC {PROTOCOL_MAJOR}.{PROTOCOL_MINOR})",
                    status.version
                );
                println!("mode: {}", status.mode);
                println!("workloads: {}", status.workloads);
                match (status.total_bytes, status.available_bytes) {
                    (Some(total), Some(available)) => {
                        println!(
                            "memory: {} available of {}",
                            human_bytes(available),
                            human_bytes(total)
                        );
                    }
                    _ => println!("memory: memory information unavailable"),
                }
                println!("observed_at_ms: {}", status.observed_at_ms);
            }
            Ok(())
        }
        Err(err) => {
            eprintln!("error: {err}");
            Err(ExitCode::from(1))
        }
    }
}

async fn run_capabilities(socket: &Path, json: bool) -> Result<(), ExitCode> {
    let mut client = connect(socket).await?;
    match client.capabilities().await {
        Ok(report) => {
            if json {
                print_json(&report)?;
            } else if report.capabilities.is_empty() {
                println!("no capabilities reported");
            } else {
                for capability in &report.capabilities {
                    match capability.state.reason() {
                        Some(reason) => {
                            println!("{}: unavailable or degraded ({reason})", capability.name)
                        }
                        None => println!("{}: available", capability.name),
                    }
                }
            }
            Ok(())
        }
        Err(err) => {
            eprintln!("error: {err}");
            Err(ExitCode::from(1))
        }
    }
}

async fn run_watch(socket: &Path) -> Result<(), ExitCode> {
    let mut client = connect(socket).await?;
    if let Err(err) = client.subscribe().await {
        eprintln!("error: {err}");
        return Err(ExitCode::from(1));
    }
    loop {
        match client.next_event().await {
            Ok(event) => {
                let available = event
                    .available_bytes
                    .map_or_else(|| "unknown".to_string(), human_bytes);
                println!(
                    "{} available={} at={}",
                    event.kind, available, event.observed_at_ms
                );
            }
            Err(ClientError::Closed) => {
                eprintln!("daemon closed the connection");
                return Err(ExitCode::from(1));
            }
            Err(err) => {
                eprintln!("error: {err}");
                return Err(ExitCode::from(1));
            }
        }
    }
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), ExitCode> {
    match serde_json::to_string_pretty(value) {
        Ok(text) => {
            println!("{text}");
            Ok(())
        }
        Err(err) => {
            eprintln!("error: cannot serialize output: {err}");
            Err(ExitCode::from(1))
        }
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
