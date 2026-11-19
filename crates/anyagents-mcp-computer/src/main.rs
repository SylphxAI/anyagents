//! Entry point for the standalone `anyagents-mcp-computer` stdio MCP server.

use clap::Parser;
use rmcp::ServiceExt;

use anyagents_mcp_computer::{ComputerMcpServer, ComputerServerConfig};

#[tokio::main]
async fn main() {
    let config = ComputerServerConfig::parse();
    let server = ComputerMcpServer::new(&config);

    let exit_code = match server.serve(rmcp::transport::io::stdio()).await {
        Ok(running) => match running.waiting().await {
            Ok(_) => 0,
            Err(err) => {
                eprintln!("anyagents-mcp-computer transport error: {err}");
                1
            }
        },
        Err(err) => {
            eprintln!("anyagents-mcp-computer failed to initialize MCP stdio transport: {err}");
            1
        }
    };

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}
