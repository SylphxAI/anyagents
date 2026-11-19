//! `anyagents memory-service`: the dense-retrieval and rerank service, run as
//! its own process beside the server. It reads the server's host index and
//! raw history read-only and owns only its dense index under
//! `<data>/memory-service/`. The server answers lexically and says so when
//! no service runs.
use agent_memory::service::Config;
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
    use agent_memory::providers::{EmbedderConfig, ProviderConfig, RerankerConfig};
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

#[cfg(test)]
fn host_index_available(host_path: &Path) -> bool {
    host_path.exists()
        || anyagents_core::db::configured_database_url_for_data_dir(
            host_path.parent().unwrap_or_else(|| Path::new("")),
        )
        .is_some()
}

/// Estimate rebuild cost across both on-disk SQLite (`agent_memory`) and
/// PostgreSQL-backed `anyagents_core::host_index::HostIndex` data directories.
fn estimate_configured(
    config: &Config,
) -> Result<agent_memory::service::Estimate, Box<dyn std::error::Error + Send + Sync>> {
    let host_path = config.data_dir.join(agent_memory::service::HOST_INDEX);
    if host_path.exists() {
        return agent_memory::service::estimate_configured(config);
    }
    let Some(pg_url) = anyagents_core::db::configured_database_url_for_data_dir(&config.data_dir)
    else {
        return Err(format!("no host index at {}", host_path.display()).into());
    };
    let (embedder, name, width_known) = match &config.providers.embedder {
        agent_memory::providers::EmbedderConfig::OpenAi(remote) => {
            let mut offline = (**remote).clone();
            let known = offline.dimensions.is_some();
            offline.dimensions = Some(offline.dimensions.unwrap_or(1));
            (
                agent_memory::providers::Embedder::remote(
                    offline,
                    agent_memory::providers::Endpoint::offline(),
                )?,
                format!("{} ({})", remote.model, remote.profile),
                known,
            )
        }
        local => {
            let environment = agent_memory::service::local_runtime(config)?;
            let runtime = environment.map(|environment| agent_memory::providers::Local {
                environment,
                models: &config.models,
                threads: 1,
            });
            let embedder =
                agent_memory::providers::Embedder::load(local, &config.data_dir, runtime.as_ref())?;
            let name = embedder.stamp().model;
            (embedder, name, true)
        }
    };
    let stamp = embedder.stamp();
    let dense_path = config.dense_path(&stamp);
    let built = width_known
        && dense_path.exists()
        && matches!(
            agent_memory::dense::DenseIndex::open_read_only(
                &dense_path,
                &stamp.key(),
                stamp.dimensions,
            )?,
            Some(dense) if dense.progress("built")?.is_some()
        );
    let index = anyagents_core::host_index::HostIndex::open_postgres(&pg_url)?;
    let mut documents = 0u64;
    let mut cursor = 0i64;
    loop {
        let batch = index.list_catalog_entries(Some(cursor), None, None, false, 4096)?;
        let Some(last) = batch.last() else { break };
        documents = documents.saturating_add(batch.len() as u64);
        cursor = last.rowid;
        if batch.len() < 4096 {
            break;
        }
    }
    let sample = index.list_catalog_entries(None, None, None, true, 500)?;
    let mut tokens = 0u64;
    let mut read = 0u64;
    for entry in &sample {
        let Ok(raw) = anyagents_core::raw_segment::read_raw(&entry.raw) else {
            continue;
        };
        read += 1;
        tokens += embedder
            .chunks_of(&raw)?
            .iter()
            .map(|c| c.tokens as u64)
            .sum::<u64>();
    }
    let estimated_tokens = if read == 0 {
        0
    } else {
        (tokens as f64 / read as f64 * documents as f64).round() as u64
    };
    let mut rebuild = agent_memory::protocol::Rebuild {
        documents,
        sampled_documents: read,
        estimated_tokens,
        estimated_usd: embedder
            .price()
            .map(|per_million| per_million * estimated_tokens as f64 / 1e6),
        max_usd: config.max_rebuild_usd(),
        state: String::new(),
        spent_usd: 0.0,
    };
    rebuild.state = if built {
        "done".into()
    } else {
        agent_memory::service::rebuild_state(&rebuild, embedder.is_remote())
    };
    Ok(agent_memory::service::Estimate {
        embedder: name,
        space: width_known.then(|| stamp.key()),
        dense_index: width_known.then_some(dense_path),
        built,
        rebuild,
    })
}

pub fn run(data: &Path, args: MemoryServiceArgs) -> Result<(), anyagents_core::Error> {
    let fail =
        |e: Box<dyn std::error::Error + Send + Sync>| anyagents_core::Error::Message(e.to_string());
    let mut config = config(data, &args)?;
    match args.command {
        MemoryServiceCommand::Fetch => {
            let pins = config.local_pins().map_err(fail)?;
            let fetched = agent_memory::model::fetch(&config.models, &pins)?;
            for url in &fetched {
                println!("fetched {url}");
            }
            agent_memory::model::check(&config.models, &pins)?;
            println!("verified under {}", config.models.display());
            Ok(())
        }
        MemoryServiceCommand::Serve { no_index, nice } => {
            config.no_index = no_index;
            config.nice = nice;
            agent_memory::service::serve(config).map_err(fail)
        }
        MemoryServiceCommand::Status => {
            let client = agent_memory::protocol::Client::for_data_dir(
                data,
                std::time::Duration::from_secs(10),
            )
            .ok_or_else(|| {
                anyagents_core::Error::Message(
                    "no memory service socket in this data directory".into(),
                )
            })?;
            let reply = client
                .call(&agent_memory::protocol::Request::Status)
                .map_err(anyagents_core::Error::Message)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&reply)
                    .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
            );
            Ok(())
        }
        MemoryServiceCommand::Prune => {
            for path in agent_memory::service::prune(&config).map_err(fail)? {
                println!("removed {}", path.display());
            }
            Ok(())
        }
        MemoryServiceCommand::Estimate => {
            let estimate = estimate_configured(&config).map_err(fail)?;
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
            let stats =
                agent_memory::eval::build_sample(&source.join("state.db"), &agents, since_ms, &out)
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
            let plan: agent_memory::eval::Plan = match plan {
                Some(path) => serde_json::from_slice(&std::fs::read(&path)?).map_err(|e| {
                    anyagents_core::Error::Message(format!("{}: {e}", path.display()))
                })?,
                None => agent_memory::eval::Plan {
                    embedders: vec![config.providers.embedder.clone()],
                    rerankers: vec![config.providers.reranker.clone()],
                    ..agent_memory::eval::Plan::default()
                },
            };
            if estimate {
                let rows = agent_memory::eval::estimate(&sample, &config, &plan).map_err(fail)?;
                if crate::ui::is_pretty() {
                    crate::ui::heading("Token estimate");
                    let mut table = crate::ui::Table::new(&[("MODEL", false), ("TOKENS", true)]);
                    for (model, tokens) in rows {
                        table.add_row(vec![model, tokens.to_string()]);
                    }
                    table.print();
                } else {
                    for (model, tokens) in rows {
                        println!("{model}\t{tokens} tokens");
                    }
                }
                return Ok(());
            }
            let report =
                agent_memory::eval::run(&sample, &config, &plan, max_queries).map_err(fail)?;
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

    #[test]
    fn estimate_configured_supports_postgres_pointer_without_sqlite_file() {
        use agent_memory::providers::{EmbedderConfig, ProviderConfig, Remote, RerankerConfig};

        let dir = crate::test_tempdir();
        let host_path = dir.path().join(agent_memory::service::HOST_INDEX);
        assert!(!host_index_available(&host_path));

        let mut cfg = Config::new(dir.path());
        cfg.providers = ProviderConfig {
            embedder: EmbedderConfig::OpenAi(Box::new(Remote {
                profile: "local/stub".into(),
                model: "test-embed".into(),
                base_url: Some("http://127.0.0.1:1".into()),
                dimensions: Some(8),
                piece_tokens: Some(512),
                usd_per_million_tokens: Some(0.01),
                usd_per_search: None,
                passage_chars: None,
                query_prefix: String::new(),
                passage_prefix: String::new(),
                body: Default::default(),
                query_body: Default::default(),
                passage_body: Default::default(),
            })),
            reranker: RerankerConfig::default(),
            max_rebuild_usd: Some(10.0),
            max_daily_usd: None,
        };

        // Without SQLite file or PG URL, estimate_configured reports missing host index.
        let missing_err = estimate_configured(&cfg).unwrap_err().to_string();
        assert!(missing_err.contains("no host index"), "{missing_err}");

        // With `database-url` pointing to PostgreSQL (and NO SQLite file on disk),
        // `estimate_configured` skips the file-existence check and routes to PostgreSQL.
        let pg_url = std::env::var("ANYAGENTS_TEST_POSTGRES_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if let Some(live_pg) = pg_url {
            std::fs::write(dir.path().join("database-url"), format!("{live_pg}\n")).unwrap();
            assert!(!host_path.exists());
            let est = estimate_configured(&cfg).unwrap();
            assert_eq!(est.embedder, "test-embed (local/stub)");
            assert!(!host_path.exists());
        } else {
            std::fs::write(
                dir.path().join("database-url"),
                "postgres://user:pass@127.0.0.1:1/nonexistent_db\n",
            )
            .unwrap();
            assert!(host_index_available(&host_path));
            let pg_err = estimate_configured(&cfg).unwrap_err().to_string();
            assert!(
                !pg_err.contains("no host index"),
                "PG-configured estimate_configured must not bail out on missing SQLite file: {pg_err}"
            );
            assert!(!host_path.exists());
        }
    }
}
