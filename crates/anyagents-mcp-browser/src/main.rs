//! Entry point for the standalone `anyagents-mcp-browser` stdio MCP server.

use clap::Parser;
use rmcp::ServiceExt;

use anyagents_mcp_browser::{BrowserMcpServer, BrowserServerConfig};

#[tokio::main]
async fn main() {
    let config = BrowserServerConfig::parse();
    let server = BrowserMcpServer::new(config);
    let shutdown_handle = server.clone();

    let exit_code = match server.serve(rmcp::transport::io::stdio()).await {
        Ok(running) => match running.waiting().await {
            Ok(_) => 0,
            Err(err) => {
                eprintln!("anyagents-mcp-browser transport error: {err}");
                1
            }
        },
        Err(err) => {
            eprintln!("anyagents-mcp-browser failed to initialize MCP stdio transport: {err}");
            1
        }
    };

    shutdown_handle.shutdown().await;
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}
