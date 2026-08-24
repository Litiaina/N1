use super::*;

pub(super) async fn interactive_shell(
    client: &mut N2NativeClient,
    cli: &Cli,
) -> NativeClientResult<()> {
    println!("N2 shell connected over n2://. Type 'help' for commands, 'exit' to quit.");
    let mut line = String::new();
    loop {
        print!("n2> ");
        io::stdout().flush()?;
        line.clear();
        if io::stdin().read_line(&mut line)? == 0 {
            break;
        }
        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if matches!(input, "exit" | "quit") {
            break;
        }
        if input == "help" {
            print_shell_help();
            continue;
        }
        if let Err(err) = run_shell_line(client, cli, input).await {
            eprintln!("error: {err}");
        }
    }
    Ok(())
}

pub(super) async fn run_shell_line(
    client: &mut N2NativeClient,
    cli: &Cli,
    input: &str,
) -> NativeClientResult<()> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        ["ping"] => print_value(client.raw_json("ping", &json!({}), None).await?, cli.json),
        ["session"] => print_value(
            client.raw_json("session.info", &json!({}), None).await?,
            cli.json,
        ),
        ["health"] => print_op(client, "maintenance.health", json!({}), cli.json).await,
        ["cluster"] | ["cluster", "health"] => {
            print_op(client, "cluster.health", json!({}), cli.json).await
        }
        ["membership"] => print_op(client, "cluster.membership", json!({}), cli.json).await,
        ["layout"] => print_op(client, "cluster.layout", json!({}), cli.json).await,
        ["usage"] => print_op(client, "cluster.usage", json!({}), cli.json).await,
        ["usage", "refresh"] | ["usage", "--refresh"] => {
            print_op(client, "cluster.usage", json!({"refresh": true}), cli.json).await
        }
        ["monitor"] => {
            print_op(
                client,
                "cluster.monitor",
                json!({"fragment": cli.fragment.clone(), "object_limit": 64}),
                cli.json,
            )
            .await
        }
        ["monitor", "deep"] => {
            print_op(
                client,
                "cluster.monitor",
                json!({"fragment": cli.fragment.clone(), "object_limit": 64, "deep_scan": true}),
                cli.json,
            )
            .await
        }
        ["monitor", "probe"] => print_op(
            client,
            "cluster.monitor",
            json!({"fragment": cli.fragment.clone(), "object_limit": 64, "probe_objects": true}),
            cli.json,
        )
        .await,
        ["dashboard"] => {
            print_dashboard(
                client,
                DashboardArgs {
                    target_fragment: cli.fragment.clone(),
                    object_limit: 64,
                    probe_objects: false,
                    refresh: false,
                },
                cli,
            )
            .await
        }
        ["dashboard", "probe"] => {
            print_dashboard(
                client,
                DashboardArgs {
                    target_fragment: cli.fragment.clone(),
                    object_limit: 64,
                    probe_objects: true,
                    refresh: false,
                },
                cli,
            )
            .await
        }
        ["fragments"] => print_op(client, "fragment.list", json!({}), cli.json).await,
        ["fragment", "check", fragment] => {
            print_op(
                client,
                "fragment.check",
                json!({"fragment": fragment}),
                cli.json,
            )
            .await
        }
        ["fragment", "hash", fragment] => {
            print_op(
                client,
                "fragment.hash",
                json!({"fragment": fragment}),
                cli.json,
            )
            .await
        }
        ["list"] => {
            print_op(
                client,
                "namespace.list",
                json!({"path": "", "limit": 200}),
                cli.json,
            )
            .await
        }
        ["list", path] => {
            print_op(
                client,
                "namespace.list",
                json!({"path": path, "limit": 200}),
                cli.json,
            )
            .await
        }
        ["trash-list"] => {
            print_op(
                client,
                "namespace.list",
                json!({"path": "", "limit": 200, "trash": true}),
                cli.json,
            )
            .await
        }
        ["stat", path] => print_op(client, "namespace.stat", json!({"path": path}), cli.json).await,
        ["rename", source, destination] | ["move", source, destination] => {
            print_op(
                client,
                "namespace.rename",
                json!({"source_path": source, "destination_path": destination}),
                cli.json,
            )
            .await
        }
        ["trash", path] => {
            print_op(client, "namespace.trash", json!({"path": path}), cli.json).await
        }
        ["recover", path] => {
            print_op(client, "namespace.recover", json!({"path": path}), cli.json).await
        }
        ["uploads"] => print_op(client, "uploads.list", json!({"limit": 50}), cli.json).await,
        ["upload-status", version_id] => {
            print_op(
                client,
                "uploads.status",
                json!({"version_id": version_id}),
                cli.json,
            )
            .await
        }
        ["object", "meta", object_key] => {
            print_op(
                client,
                "object.meta",
                json!({"object_key": object_key}),
                cli.json,
            )
            .await
        }
        ["object", "versions", object_key] => {
            print_op(
                client,
                "object.versions",
                json!({"object_key": object_key}),
                cli.json,
            )
            .await
        }
        ["object", "status", object_key] => {
            print_op(
                client,
                "object.status",
                json!({"object_key": object_key}),
                cli.json,
            )
            .await
        }
        ["object", "repair", object_key] => {
            print_op(
                client,
                "object.repair",
                json!({"object_key": object_key, "mode": "strict"}),
                cli.json,
            )
            .await
        }
        ["object", "redistribute", object_key] => {
            print_op(
                client,
                "object.redistribute",
                json!({"object_key": object_key}),
                cli.json,
            )
            .await
        }
        ["object", "redistribute", object_key, "dry-run"]
        | ["object", "redistribute", object_key, "--dry-run"] => {
            print_op(
                client,
                "object.redistribute",
                json!({"object_key": object_key, "dry_run": true}),
                cli.json,
            )
            .await
        }
        ["repair"] | ["repair", "status"] => {
            print_op(client, "cluster.repair.status", json!({}), cli.json).await
        }
        ["repair", "plan"] => print_op(client, "cluster.repair.plan", json!({}), cli.json).await,
        ["repair", "stop"] => print_op(client, "cluster.repair.stop", json!({}), cli.json).await,
        ["repair", "watch"] => watch_job(client, "cluster.repair", 5, cli.json).await,
        ["repair", "start"] => {
            let fragment = cli.fragment.clone().ok_or_else(|| {
                NativeClientError::protocol("repair start requires global --fragment")
            })?;
            print_op(
                client,
                "cluster.repair.start",
                json!({"fragment": fragment}),
                cli.json,
            )
            .await
        }
        ["rebalance"] | ["rebalance", "status"] => {
            print_op(client, "cluster.rebalance.status", json!({}), cli.json).await
        }
        ["rebalance", "plan"] => {
            print_op(client, "cluster.rebalance.plan", json!({}), cli.json).await
        }
        ["rebalance", "stop"] => {
            print_op(client, "cluster.rebalance.stop", json!({}), cli.json).await
        }
        ["rebalance", "watch"] => watch_job(client, "cluster.rebalance", 5, cli.json).await,
        ["rebalance", "start"] => {
            let fragment = cli.fragment.clone().ok_or_else(|| {
                NativeClientError::protocol("rebalance start requires global --fragment")
            })?;
            print_op(
                client,
                "cluster.rebalance.start",
                json!({"fragment": fragment, "mismatched_only": true}),
                cli.json,
            )
            .await
        }
        ["gc"] | ["gc", "status"] => {
            print_op(client, "maintenance.gc.status", json!({}), cli.json).await
        }
        ["gc", "scrub"] | ["gc", "start"] => {
            print_op(
                client,
                "maintenance.gc.start",
                json!({"mode": "scrub", "fast_mode": false}),
                cli.json,
            )
            .await
        }
        ["gc", "stop"] => print_op(client, "maintenance.gc.stop", json!({}), cli.json).await,
        ["gc", "watch"] => watch_job(client, "maintenance.gc", 5, cli.json).await,
        _ => {
            println!("unknown command. Type 'help'.");
            Ok(())
        }
    }
}

pub(super) fn print_shell_help() {
    println!(
        "commands:\n  ping\n  session\n  health\n  cluster [health]\n  membership\n  layout\n  dashboard [probe]\n  monitor [deep|probe]\n  usage [refresh]\n  fragments\n  fragment check <fragment>\n  fragment hash <fragment>\n  list [path]\n  trash-list\n  stat <path>\n  rename <source> <destination>\n  trash <path>\n  recover <path>\n  uploads\n  upload-status <version_id>\n  object meta <object_key>\n  object versions <object_key>\n  object status <object_key>\n  object repair <object_key>\n  object redistribute <object_key> [dry-run]\n  repair status|plan|start|watch|stop\n  rebalance status|plan|start|watch|stop\n  gc status|scrub|watch|stop\n  exit"
    );
}

pub(super) async fn authenticate_from_cli(
    client: &mut N2NativeClient,
    cli: &Cli,
) -> NativeClientResult<()> {
    let fragment = cli.fragment.as_deref().ok_or_else(|| {
        NativeClientError::protocol("pass --fragment <name-or-hash> for authenticated commands")
    })?;
    let secret = cli.secret.as_deref().ok_or_else(|| {
        NativeClientError::protocol("pass --secret <fragment-secret> for authenticated commands")
    })?;
    client.auth_fragment(fragment, secret).await
}

pub(super) async fn authenticate_if_configured(
    client: &mut N2NativeClient,
    cli: &Cli,
) -> NativeClientResult<()> {
    match (cli.fragment.as_deref(), cli.secret.as_deref()) {
        (Some(fragment), Some(secret)) => client.auth_fragment(fragment, secret).await,
        (None, None) => Ok(()),
        _ => Err(NativeClientError::protocol(
            "--fragment and --secret must be provided together",
        )),
    }
}
