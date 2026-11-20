//! Entry point for the standalone `anyagents-mcp-web` stdio MCP server.

use clap::Parser;
use rmcp::ServiceExt;

use anyagents_mcp_web::{WebMcpServer, WebServerConfig};

#[tokio::main]
async fn main() {
    let config = WebServerConfig::parse();
    let server = WebMcpServer::new(config);

    let exit_code = match server.serve(rmcp::transport::io::stdio()).await {
        Ok(running) => match running.waiting().await {
            Ok(_) => 0,
            Err(err) => {
                eprintln!("anyagents-mcp-web transport error: {err}");
                1
            }
        },
        Err(err) => {
            eprintln!("anyagents-mcp-web failed to initialize MCP stdio transport: {err}");
            1
        }
    };

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}
