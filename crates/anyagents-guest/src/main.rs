//! Long-lived sandbox guest supervisor (`anyagents-guest`).
//!
//! Runs inside an isolated sandbox microVM/container:
//! - `serve`: embeds all 9 local harness adapters with streaming `stream-json`,
//!   mid-turn `steer`, `cancel`, `TurnUsage`, git repo checkout, and session persistence.
//! - `mcp-proxy`: bridges stdio JSON-RPC to the coordinator's HTTPS `/mcp?profile=full`
//!   endpoint with dynamic bearer capability token reload.
//! - `health`: readiness probe.

mod dsh_grok;
mod guest;
mod mcp_proxy;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "anyagents-guest", about = "AnyAgents sandbox guest runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run a long-lived embedded harness supervisor speaking `stream-json`.
    Serve(guest::ServeArgs),
    /// Bridge stdio MCP JSON-RPC requests to the coordinator HTTPS `/mcp` endpoint.
    #[command(name = "mcp-proxy")]
    McpProxy(mcp_proxy::McpProxyArgs),
    /// Print readiness JSON for container/sandbox healthchecks.
    Health,
}

fn main() {
    let cli = Cli::parse();
    let res = match cli.command {
        Command::Serve(args) => guest::run_serve(args),
        Command::McpProxy(args) => mcp_proxy::run_mcp_proxy(args),
        Command::Health => {
            println!(
                "{{\"status\":\"ok\",\"binary\":\"anyagents-guest\",\"harnesses\":9}}"
            );
            Ok(())
        }
    };
    if let Err(err) = res {
        eprintln!("anyagents-guest error: {err}");
        std::process::exit(1);
    }
}
