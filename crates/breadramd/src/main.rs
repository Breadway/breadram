//! breadramd: the breadram memory management daemon.
//!
//! This is the RAM-1 bootstrap entry point. It establishes argument parsing,
//! logging, and a startup contract summary so that later cards have a real
//! binary to grow into. The daemon event loop, single instance protection, the
//! versioned local socket, telemetry, and policy land in RAM-2 through RAM-10.
//!
//! It is honest about that instead of pretending to run: it prints what it is
//! and the version of the IPC contract it speaks, then exits.

use std::path::PathBuf;
use std::process::ExitCode;

use breadram_core::{ManagementMode, PRODUCT_NAME, PROTOCOL_MAJOR, PROTOCOL_MINOR, VERSION};
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "breadramd",
    version,
    about = "breadram memory management daemon"
)]
struct Args {
    /// Configuration file path.
    ///
    /// Validation and profiles land in RAM-5. The flag is accepted now so that
    /// service files and operator scripts can pass it unchanged later.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Management mode: "monitor" observes and explains, "automatic" may act.
    #[arg(long, value_name = "MODE", default_value = "monitor")]
    mode: String,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let mode = match args.mode.parse::<ManagementMode>() {
        Ok(mode) => mode,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::from(2);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    if let Some(path) = args.config.as_deref() {
        if path.exists() {
            tracing::info!(
                config = %path.display(),
                "configuration file found, parsing lands in RAM-5"
            );
        } else {
            tracing::warn!(
                config = %path.display(),
                "configuration file does not exist, using built in defaults"
            );
        }
    }

    println!("{PRODUCT_NAME}d {VERSION} (IPC {PROTOCOL_MAJOR}.{PROTOCOL_MINOR})");
    println!("mode: {mode}");
    println!("state: bootstrap build, lifecycle and IPC land in RAM-4");

    ExitCode::SUCCESS
}
