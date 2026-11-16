//! A file-service workload against an isolated standalone N1, without M1.
//! Binaries, members and reports are supplied explicitly; existing pools are never opened.
#[path = "mixed_workload/api.rs"]
mod api;
#[path = "mixed_workload/observe.rs"]
mod observe;
#[path = "mixed_workload/setup.rs"]
mod setup;
#[path = "mixed_workload/workload.rs"]
mod workload;

use serde_json::json;
use std::{path::PathBuf, time::Duration};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
struct Options {
    diagnostics: bool,
    upload_only: bool,
    policy: workload::PartPolicy,
}
impl Options {
    fn parse(flags: &[String]) -> Result<Self> {
        let mut options = Self::default();
        for flag in flags {
            match flag.as_str() {
                "--diagnostics" => options.diagnostics = true,
                "--upload-only" => options.upload_only = true,
                "--normal-only" | "--durable-only" => {
                    if options.policy != workload::PartPolicy::Mixed {
                        return Err("select exactly one part durability policy".into());
                    }
                    options.policy = if flag == "--normal-only" {
                        workload::PartPolicy::Normal
                    } else {
                        workload::PartPolicy::Durable
                    };
                }
                _ => return Err(format!("unknown workload option: {flag}").into()),
            }
        }
        Ok(options)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 7 {
        eprintln!(
            "usage: mixed_workload N1-binary first-private-root second-private-root output-root light-seconds busy-seconds [--diagnostics] [--upload-only] [--normal-only|--durable-only]"
        );
        std::process::exit(2);
    }
    let output = PathBuf::from(&args[4]);
    if let Err(error) = run(&args, &output).await {
        let _ = api::save(
            &output.join("FAILED.json"),
            &json!({"error":error.to_string()}),
        );
        eprintln!("mixed workload failed: {error}");
        std::process::exit(1);
    }
}

async fn run(args: &[String], output: &std::path::Path) -> Result<()> {
    if cfg!(debug_assertions) {
        return Err("use a release-profile workload client".into());
    }
    let options = Options::parse(&args[7..])?;
    let light: u64 = args[5].parse()?;
    let busy: u64 = args[6].parse()?;
    if !(2..=300).contains(&light) || !(2..=600).contains(&busy) {
        return Err("workload windows must be 2..300 and 2..600 seconds".into());
    }
    let mut stack = setup::Stack::start(&args[1], &args[2], &args[3], output).await?;
    let api = stack.api.clone();
    let catalog = workload::seed(&api, output).await?;
    let mut results = Vec::new();
    for (label, seconds, bulk, video, downloads, interactive, deletes) in [
        ("light-before", light, 1, 1, 1, 1, 1),
        ("busy", busy, 2, 4, 2, 4, 2),
        ("light-after", light, 1, 1, 1, 1, 1),
    ] {
        let profile = workload::Profile {
            label: label.into(),
            seconds,
            bulk,
            video: if options.upload_only { 0 } else { video },
            downloads: if options.upload_only { 0 } else { downloads },
            interactive: if options.upload_only { 0 } else { interactive },
            deletes: if options.upload_only { 0 } else { deletes },
            policy: options.policy,
        };
        let row = workload::phase(
            &api,
            catalog.clone(),
            profile,
            stack.pid(),
            output,
            options.diagnostics,
        )
        .await?;
        results.push(row);
        // Allow a short natural quiet period, without dropping caches or changing governors.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    workload::verify(&api, &catalog, output, "before-restart").await?;
    stack.restart().await?;
    workload::verify(&stack.api, &catalog, output, "after-restart").await?;
    let cleanup = workload::cleanup(&stack.api, &catalog, output).await?;
    stack.stop().await?;
    let census = stack.census()?;
    api::save(
        &output.join("PASSED.json"),
        &json!({
            "passed":true,"scope":"standalone N1 native TLS, private 1+1 pool on two independent filesystems",
            "scenario":if options.upload_only { "upload-only" } else { "mixed" },
            "part_policy":options.policy,
            "diagnostics":options.diagnostics,
            "no_m1_process_or_requests":true,"phases":results,"cleanup":cleanup,"census":census,
            "restart":"clean process restart; not power loss or SIGKILL",
            "acceptance":"functional integrity and completed workload; latency is reported without an invented production SLA"
        }),
    )?;
    println!(
        "{}",
        json!({"passed":true,"report":output.join("PASSED.json")})
    );
    Ok(())
}
