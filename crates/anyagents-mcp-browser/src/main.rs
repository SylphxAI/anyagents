//! Entry point for the standalone `anyagents-mcp-browser` stdio MCP server.

use clap::Parser;
use rmcp::ServiceExt;

use anyagents_mcp_browser::signals::terminate_signal;
use anyagents_mcp_browser::{BrowserMcpServer, BrowserServerConfig};

#[tokio::main]
async fn main() {
    let config = BrowserServerConfig::parse();
    let server = BrowserMcpServer::new(config);
    let shutdown_handle = server.clone();

    let exit_code = match server.serve(rmcp::transport::io::stdio()).await {
        Ok(running) => {
            let cancel = running.cancellation_token();
            let mut code = 0i32;
            tokio::select! {
                res = running.waiting() => {
                    if let Err(err) = res {
                        eprintln!("anyagents-mcp-browser transport error: {err}");
                        code = 1;
                    }
                }
                sig = terminate_signal() => {
                    eprintln!(
                        "anyagents-mcp-browser: received {sig}; closing browser and syncing profile"
                    );
                    cancel.cancel();
                }
            }
            code
        }
        Err(err) => {
            eprintln!("anyagents-mcp-browser failed to initialize MCP stdio transport: {err}");
            1
        }
    };

    shutdown_handle.shutdown().await;
    std::process::exit(exit_code);
}
