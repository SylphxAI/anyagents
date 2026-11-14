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
    /// Background embedding workers, one core each, at the lowest priority.
    #[arg(long, global = true, default_value_t = 1)]
    index_threads: usize,
    /// Cores one query's embedding or rerank may use.
    #[arg(long, global = true, default_value_t = 2)]
    query_threads: usize,
    /// Which embedder and reranker run: a JSON file naming a local pinned
    /// model or a remote one bound to a profile (default
    /// `<data>/memory-service/config.json`; without it, the pinned local pair).
    /// A file named here must exist.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// A pinned local embedder, overriding the configuration (see `pins.rs`).
    #[arg(long, global = true)]
    embedder: Option<String>,
    /// A pinned local reranker, overriding the configuration.
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
    /// Remove the dense indexes of every vector space but the configured one,
    /// once that one is built. Nothing else ever removes a space.
    Prune,
    /// Estimate what building the configured embedder's vector space over
    /// the host index as it stands would read and cost, and whether `serve`
    /// would start it within `max_rebuild_usd`. Calls no model, reads no
    /// profile key and writes nothing; exits non-zero when `serve` would
    /// wait.
    Estimate,
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
    /// Score BM25, BM25 reranked, and per embedder dense, fusion and the
    /// reranked fusion on a sample. Remote candidates use this data
    /// directory's profiles; the run refuses to start above the plan's budget
    /// and stops when reported spend reaches it.
    Eval {
        /// A directory written by `eval-sample`.
        #[arg(long)]
        sample: PathBuf,
        /// A JSON plan: `embedders`, `rerankers`, `max_usd` (default: the
        /// configured pair, US$5).
        #[arg(long)]
        plan: Option<PathBuf>,
        /// Count what each embedder would read and stop: no model is called.
        #[arg(long)]
        estimate: bool,
        /// Evaluate at most this many queries.
        #[arg(long)]
        max_queries: Option<usize>,
    },
}

fn config(data: &Path, args: &MemoryServiceArgs) -> Result<Config, anyagents_core::Error> {
    use anyagents_memory::providers::{EmbedderConfig, ProviderConfig, RerankerConfig};
    let fail =
        |e: Box<dyn std::error::Error + Send + Sync>| anyagents_core::Error::Message(e.to_string());
    let mut config = Config::new(data);
    if let Some(models) = &args.models {
        config.models = models.clone();
    }
    config.index_threads = args.index_threads.max(1);
    config.query_threads = args.query_threads.max(1);
    // A path the operator named must exist; only the default path may be
    // absent, and then the pinned local pair runs, which is said.
    config.providers = match &args.config {
        Some(path) => ProviderConfig::load_required(path).map_err(fail)?,
        None => {
            let path = ProviderConfig::path(data);
            if !path.exists() {
                eprintln!(
                    "memory-service: no {}; the pinned local models run",
                    path.display()
                );
            }
            ProviderConfig::load(&path).map_err(fail)?
        }
    };
    if let Some(name) = &args.embedder {
        eprintln!("memory-service: --embedder {name} replaces the configured embedder");
        config.providers.embedder = EmbedderConfig::Local {
            model: Some(name.clone()),
        };
    }
    if let Some(name) = &args.reranker {
        eprintln!("memory-service: --reranker {name} replaces the configured reranker");
        config.providers.reranker = RerankerConfig::Local {
            model: Some(name.clone()),
        };
    }
    // Refuse a pin name that does not exist before any work starts.
    config.local_pins().map_err(fail)?;
    Ok(config)
}

pub fn run(data: &Path, args: MemoryServiceArgs) -> Result<(), anyagents_core::Error> {
    let fail =
        |e: Box<dyn std::error::Error + Send + Sync>| anyagents_core::Error::Message(e.to_string());
    let mut config = config(data, &args)?;
    match args.command {
        MemoryServiceCommand::Fetch => {
            let pins = config.local_pins().map_err(fail)?;
            let fetched = anyagents_memory::model::fetch(&config.models, &pins)?;
            for url in &fetched {
                println!("fetched {url}");
            }
            anyagents_memory::model::check(&config.models, &pins)?;
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
        MemoryServiceCommand::Prune => {
            for path in anyagents_memory::service::prune(&config).map_err(fail)? {
                println!("removed {}", path.display());
            }
            Ok(())
        }
        MemoryServiceCommand::Estimate => {
            let estimate = anyagents_memory::service::estimate_configured(&config).map_err(fail)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&estimate)
                    .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
            );
            if estimate.rebuild.state.starts_with("waiting") {
                return Err(anyagents_core::Error::Message(estimate.rebuild.state));
            }
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
            plan,
            estimate,
            max_queries,
        } => {
            let plan: anyagents_memory::eval::Plan = match plan {
                Some(path) => serde_json::from_slice(&std::fs::read(&path)?).map_err(|e| {
                    anyagents_core::Error::Message(format!("{}: {e}", path.display()))
                })?,
                None => anyagents_memory::eval::Plan {
                    embedders: vec![config.providers.embedder.clone()],
                    rerankers: vec![config.providers.reranker.clone()],
                    ..anyagents_memory::eval::Plan::default()
                },
            };
            if estimate {
                for (model, tokens) in
                    anyagents_memory::eval::estimate(&sample, &config, &plan).map_err(fail)?
                {
                    println!("{model}\t{tokens} tokens");
                }
                return Ok(());
            }
            let report =
                anyagents_memory::eval::run(&sample, &config, &plan, max_queries).map_err(fail)?;
            let text = serde_json::to_string_pretty(&report)
                .map_err(|e| anyagents_core::Error::Message(e.to_string()))?;
            std::fs::write(sample.join("report.json"), &text)?;
            println!("{text}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(config: Option<PathBuf>) -> MemoryServiceArgs {
        MemoryServiceArgs {
            command: MemoryServiceCommand::Status,
            models: None,
            index_threads: 1,
            query_threads: 1,
            config,
            embedder: None,
            reranker: None,
        }
    }

    /// A configuration the operator named is never silently replaced by the
    /// local default: a wrong path is an error. Only the default path may be
    /// absent.
    #[test]
    fn a_named_configuration_must_exist() {
        let dir = crate::test_tempdir();
        let missing = dir.path().join("typo.json");
        let error = config(dir.path(), &args(Some(missing.clone())))
            .err()
            .unwrap();
        assert!(error.to_string().contains("typo.json"), "{error}");
        let default = config(dir.path(), &args(None)).unwrap();
        assert_eq!(default.providers, Default::default());
    }
}
