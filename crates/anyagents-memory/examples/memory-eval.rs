//! The evaluation without the full `anyagents` binary, for a developer
//! machine: `cargo run -p anyagents-memory --features service --release
//! --example memory-eval -- <sample|fetch|eval> ...`. The same library code
//! backs `anyagents memory-service eval-sample|fetch|eval`.
use anyagents_memory::{eval, model, service};
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
    for (flag, slot) in [
        ("--embedder", &mut config.embedder),
        ("--reranker", &mut config.reranker),
    ] {
        if let Some(name) = arg(flag) {
            *slot =
                anyagents_memory::pins::by_name(&name).ok_or(format!("no pinned model {name}"))?;
        }
    }
    if let Some(n) = arg("--threads") {
        config.index_threads = n.parse()?;
        config.query_threads = config.index_threads;
    }
    match args.first().map(String::as_str) {
        Some("fetch") => {
            for url in model::fetch(&config.models, &config.pins())? {
                println!("fetched {url}");
            }
            model::check(&config.models, &config.pins())?;
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
        Some("eval") => {
            let sample = PathBuf::from(arg("--sample").ok_or("--sample")?);
            let max = arg("--max-queries").map(|s| s.parse()).transpose()?;
            let report = eval::run(&sample, &config, max)?;
            let text = serde_json::to_string_pretty(&report)?;
            std::fs::write(sample.join("report.json"), &text)?;
            println!("{text}");
        }
        _ => {
            return Err(
                "usage: memory-eval <fetch|sample|eval> [--data DIR] [--models DIR] ...".into(),
            );
        }
    }
    Ok(())
}
