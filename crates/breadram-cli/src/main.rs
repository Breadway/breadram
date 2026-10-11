//! breadram: the command line client.
//!
//! This is the RAM-1 bootstrap of the CLI. It carries the version and IPC
//! contract so scripts can depend on them now. The full command surface
//! (status, watch, workloads, capabilities, dry run explanations, actions, and
//! diagnostics) lands in RAM-10 and talks to the daemon over the versioned
//! local socket from RAM-4.

use std::process::ExitCode;

use breadram_core::{ManagementMode, PRODUCT_NAME, PROTOCOL_MAJOR, PROTOCOL_MINOR, VERSION};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "breadram",
    version,
    about = "breadram memory management client"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print the client version and the IPC contract it speaks.
    Version,
    /// Print daemon and host status.
    Status,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Command::Status) {
        Command::Version => {
            println!("{PRODUCT_NAME} {VERSION} (IPC {PROTOCOL_MAJOR}.{PROTOCOL_MINOR})");
        }
        Command::Status => {
            println!("{PRODUCT_NAME} {VERSION} (IPC {PROTOCOL_MAJOR}.{PROTOCOL_MINOR})");
            println!("default mode: {}", ManagementMode::default());
            println!("daemon: not connected, the local socket lands in RAM-4");
        }
    }

    ExitCode::SUCCESS
}
