//! breadramd binary entry point.

use std::path::PathBuf;
use std::process::ExitCode;

use breadram_core::config::{Config, Profile};
use breadram_core::ipc::default_socket_path;
use breadram_core::ManagementMode;
use breadramd::daemon;
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "breadramd",
    version,
    about = "breadram memory management daemon"
)]
struct Args {
    /// Configuration file path.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Starting profile: monitor, desktop, or headless-server.
    #[arg(long, value_name = "PROFILE")]
    profile: Option<String>,

    /// Override the management mode: monitor or automatic.
    #[arg(long, value_name = "MODE")]
    mode: Option<String>,

    /// Unix socket path. Defaults to $XDG_RUNTIME_DIR/breadram.sock.
    #[arg(long, value_name = "PATH")]
    socket: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    let config = match resolve_config(&args) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::from(2);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let socket = args.socket.clone().unwrap_or_else(default_socket_path);
    tracing::info!(socket = %socket.display(), mode = %config.mode, "breadramd starting");

    match daemon::run(config, socket).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn resolve_config(args: &Args) -> Result<Config, String> {
    let mut config = match args.profile.as_deref() {
        Some(name) => name
            .parse::<Profile>()
            .map_err(|err| err.to_string())?
            .resolve(),
        None => Profile::Monitor.resolve(),
    };
    if let Some(path) = &args.config {
        let text = std::fs::read_to_string(path)
            .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
        config = Config::from_toml(&text).map_err(|err| err.to_string())?;
    }
    if let Some(mode) = &args.mode {
        config.mode = mode
            .parse::<ManagementMode>()
            .map_err(|err| err.to_string())?;
    }
    Ok(config)
}
