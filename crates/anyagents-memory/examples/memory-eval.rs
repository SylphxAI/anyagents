//! The evaluation without the full `anyagents` binary, for a developer
//! machine: `cargo run -p anyagents-memory --features service --release
//! --example memory-eval -- <sample|fetch|estimate|eval> ...`. The same
//! library code backs `anyagents memory-service eval-sample|fetch|eval`.
//! `--data` names the data directory whose profiles remote models use;
//! `--plan` a JSON `eval::Plan` of candidates and budget.
use anyagents_memory::{eval, model, providers, service};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let data = PathBuf::from(arg("--data").unwrap_or_else(|| ".anyagents".into()));
    let mut config = service::Config::new(&data);
    if let Some(models) = arg("--models") {
        config.models = models.into();
    }
    config.providers = providers::ProviderConfig::load(
        &arg("--config").map_or_else(|| providers::ProviderConfig::path(&data), PathBuf::from),
    )?;
    if let Some(name) = arg("--embedder") {
        config.providers.embedder = providers::EmbedderConfig::Local { model: Some(name) };
    }
    if let Some(name) = arg("--reranker") {
        config.providers.reranker = providers::RerankerConfig::Local { model: Some(name) };
    }
    if let Some(n) = arg("--threads") {
        config.index_threads = n.parse()?;
        config.query_threads = config.index_threads;
    }
    // An evaluation plan: the candidates and the budget. Without one, the
    // configured pair alone.
    let plan: eval::Plan = match arg("--plan") {
        Some(path) => serde_json::from_slice(&std::fs::read(path)?)?,
        None => eval::Plan {
            embedders: vec![config.providers.embedder.clone()],
            rerankers: vec![config.providers.reranker.clone()],
            ..eval::Plan::default()
        },
    };
    match args.first().map(String::as_str) {
        Some("fetch") => {
            let mut pins = config.local_pins()?;
            for embedder in &plan.embedders {
                pins.extend(providers::local_embedder_pin(embedder)?);
            }
            for reranker in &plan.rerankers {
                pins.extend(providers::local_reranker_pin(reranker)?);
            }
            for url in model::fetch(&config.models, &pins)? {
                println!("fetched {url}");
            }
            model::check(&config.models, &pins)?;
        }
        Some("sample") => {
            let agents: Vec<String> = args
                .windows(2)
                .filter(|w| w[0] == "--agent")
                .map(|w| w[1].clone())
                .collect();
            let out = PathBuf::from(arg("--out").ok_or("--out")?);
            std::fs::create_dir_all(&out)?;
            let since = arg("--since-ms").map_or(Ok(0), |s| s.parse())?;
            let stats = eval::build_sample(&data.join("state.db"), &agents, since, &out)?;
            println!("{}", serde_json::to_string_pretty(&stats)?);
        }
        Some("estimate") => {
            let sample = PathBuf::from(arg("--sample").ok_or("--sample")?);
            for (model, tokens) in eval::estimate(&sample, &config, &plan)? {
                println!("{model}\t{tokens} tokens");
            }
        }
        Some("eval") => {
            let sample = PathBuf::from(arg("--sample").ok_or("--sample")?);
            let max = arg("--max-queries").map(|s| s.parse()).transpose()?;
            let report = eval::run(&sample, &config, &plan, max)?;
            let text = serde_json::to_string_pretty(&report)?;
            let out = arg("--report").map_or_else(|| sample.join("report.json"), PathBuf::from);
            std::fs::write(out, &text)?;
            println!("{text}");
        }
        Some("serve") => service::serve(config)?,
        Some("search") => {
            let client = anyagents_memory::protocol::Client::for_data_dir(
                &data,
                std::time::Duration::from_secs(30),
            )
            .ok_or("no service socket")?;
            let owners: Vec<String> = args
                .windows(2)
                .filter(|w| w[0] == "--owner")
                .map(|w| w[1].clone())
                .collect();
            let query = arg("--query").ok_or("--query")?;
            let (hits, status) = client.search(&query, &owners, None, None, None, 5)?;
            println!("{}", serde_json::to_string_pretty(&status)?);
            let passages: Vec<String> = hits
                .iter()
                .map(|h| format!("{} {}..{}", h.document_id, h.offset, h.offset + h.length))
                .collect();
            for (hit, line) in hits.iter().zip(&passages) {
                println!("{:.3} {line}", hit.score);
            }
            let scores = client.rerank(&query, &passages)?;
            println!("rerank scores {scores:?}");
        }
        _ => {
            return Err(
                "usage: memory-eval <fetch|sample|estimate|eval|serve|search> [--data DIR] [--models DIR] [--plan FILE] ..."
                    .into(),
            );
        }
    }
    Ok(())
}
