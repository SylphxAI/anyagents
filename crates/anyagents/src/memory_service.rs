//! `anyagents memory-service`: the dense-retrieval and rerank service, run as
//! its own process beside the server. It reads the server's host index and
//! raw history read-only and owns only its dense index under
//! `<data>/memory-service/`. The server answers lexically and says so when
//! no service runs.
use anyagents_memory::service::Config;
use clap::{Args, Subcommand};
use std::path::{Path, PathBuf};

#[derive(Args)]
pub struct MemoryServiceArgs {
    #[command(subcommand)]
    command: MemoryServiceCommand,
    /// Where pinned models and ONNX Runtime live (default
    /// `<data>/memory-service/models`).
    #[arg(long, global = true)]
    models: Option<PathBuf>,
    /// Cores the background embedder may use.
    #[arg(long, global = true, default_value_t = 1)]
    index_threads: usize,
    /// Cores one query's embedding or rerank may use.
    #[arg(long, global = true, default_value_t = 2)]
    query_threads: usize,
    /// A pinned embedder other than the default (see `pins.rs`).
    #[arg(long, global = true)]
    embedder: Option<String>,
    /// A pinned reranker other than the default.
    #[arg(long, global = true)]
    reranker: Option<String>,
}

#[derive(Subcommand)]
enum MemoryServiceCommand {
    /// Download the pinned embedder, reranker and ONNX Runtime, verifying
    /// every file against its pinned SHA-256.
    Fetch,
    /// Serve dense search and rerank on `<data>/memory-service.sock` and embed
    /// new history in the background at the lowest CPU priority.
    Serve {
        /// Only answer queries; embed nothing in the background.
        #[arg(long)]
        no_index: bool,
        /// Nice value of the background embedder.
        #[arg(long, default_value_t = 19)]
        nice: i32,
    },
    /// Print the running service's model pair and embedding progress.
    Status,
    /// Build an evaluation sample from a data directory's `state.db`, read-only:
    /// the agents' mailboxes and the queries their history implies.
    EvalSample {
        /// Data directory whose `state.db` is read (read-only).
        #[arg(long)]
        source: PathBuf,
        /// Agent ids to sample.
        #[arg(long = "agent", required = true)]
        agents: Vec<String>,
        /// Only history at or after this UTC millisecond.
        #[arg(long, default_value_t = 0)]
        since_ms: i64,
        /// Directory to write the sample into.
        #[arg(long)]
        out: PathBuf,
    },
    /// Score BM25, dense, their fusion and the reranked fusion on a sample.
    Eval {
        /// A directory written by `eval-sample`.
        #[arg(long)]
        sample: PathBuf,
        /// Evaluate at most this many queries.
        #[arg(long)]
        max_queries: Option<usize>,
    },
}

fn config(data: &Path, args: &MemoryServiceArgs) -> Result<Config, anyagents_core::Error> {
    let mut config = Config::new(data);
    if let Some(models) = &args.models {
        config.models = models.clone();
    }
    config.index_threads = args.index_threads.max(1);
    config.query_threads = args.query_threads.max(1);
    for (name, slot) in [
        (&args.embedder, &mut config.embedder),
        (&args.reranker, &mut config.reranker),
    ] {
        if let Some(name) = name {
            *slot = anyagents_memory::pins::by_name(name).ok_or_else(|| {
                anyagents_core::Error::Message(format!("no pinned model named {name}"))
            })?;
        }
    }
    Ok(config)
}

pub fn run(data: &Path, args: MemoryServiceArgs) -> Result<(), anyagents_core::Error> {
    let fail =
        |e: Box<dyn std::error::Error + Send + Sync>| anyagents_core::Error::Message(e.to_string());
    let mut config = config(data, &args)?;
    match args.command {
        MemoryServiceCommand::Fetch => {
            let fetched = anyagents_memory::model::fetch(&config.models, &config.pins())?;
            for url in &fetched {
                println!("fetched {url}");
            }
            anyagents_memory::model::check(&config.models, &config.pins())?;
            println!("verified under {}", config.models.display());
            Ok(())
        }
        MemoryServiceCommand::Serve { no_index, nice } => {
            config.no_index = no_index;
            config.nice = nice;
            anyagents_memory::service::serve(config).map_err(fail)
        }
        MemoryServiceCommand::Status => {
            let client = anyagents_memory::protocol::Client::for_data_dir(
                data,
                std::time::Duration::from_secs(10),
            )
            .ok_or_else(|| {
                anyagents_core::Error::Message(
                    "no memory service socket in this data directory".into(),
                )
            })?;
            let reply = client
                .call(&anyagents_memory::protocol::Request::Status)
                .map_err(anyagents_core::Error::Message)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&reply)
                    .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
            );
            Ok(())
        }
        MemoryServiceCommand::EvalSample {
            source,
            agents,
            since_ms,
            out,
        } => {
            std::fs::create_dir_all(&out)?;
            let stats = anyagents_memory::eval::build_sample(
                &source.join("state.db"),
                &agents,
                since_ms,
                &out,
            )
            .map_err(fail)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&stats)
                    .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
            );
            Ok(())
        }
        MemoryServiceCommand::Eval {
            sample,
            max_queries,
        } => {
            let report =
                anyagents_memory::eval::run(&sample, &config, max_queries).map_err(fail)?;
            let text = serde_json::to_string_pretty(&report)
                .map_err(|e| anyagents_core::Error::Message(e.to_string()))?;
            std::fs::write(sample.join("report.json"), &text)?;
            println!("{text}");
            Ok(())
        }
    }
}
