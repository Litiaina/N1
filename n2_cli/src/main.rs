use clap::{Parser, Subcommand};
use n2_native_client::{N2NativeClient, NativeClientResult};
use serde_json::{Value, json};
use std::path::PathBuf;
use tokio::fs::File;

#[derive(Parser)]
#[command(name = "litiaina-n2-cli")]
#[command(about = "Native N2 operator CLI")]
struct Cli {
    #[arg(long, default_value = "n2://127.0.0.1:50002")]
    native_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Ping,
    Auth {
        #[arg(long)]
        fragment: String,
        #[arg(long)]
        secret: String,
    },
    Session,
    Raw {
        op: String,
        #[arg(long, default_value = "{}")]
        json: String,
        #[arg(long)]
        fragment: Option<String>,
        #[arg(long)]
        secret: Option<String>,
    },
    Health(AuthArgs),
    Membership(AuthArgs),
    Layout(AuthArgs),
    Usage {
        #[arg(long)]
        fragment: String,
        #[arg(long)]
        secret: String,
        #[arg(long)]
        target_fragment: Option<String>,
    },
    List {
        #[arg(long)]
        fragment: String,
        #[arg(long)]
        secret: String,
        #[arg(long, default_value = "")]
        path: String,
        #[arg(long, default_value_t = 200)]
        limit: usize,
        #[arg(long, default_value_t = false)]
        trash: bool,
    },
    UploadOneShot {
        #[arg(long)]
        fragment: String,
        #[arg(long)]
        secret: String,
        #[arg(long)]
        object_key: String,
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        content_type: Option<String>,
    },
}

#[derive(clap::Args)]
struct AuthArgs {
    #[arg(long)]
    fragment: String,
    #[arg(long)]
    secret: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> NativeClientResult<()> {
    let cli = Cli::parse();
    let mut client = N2NativeClient::connect(&cli.native_url)
        .await?
        .with_request_prefix("n2-cli");

    match cli.command {
        Command::Ping => print_json(client.raw_json("ping", &json!({}), None).await?),
        Command::Auth { fragment, secret } => {
            client.auth_fragment(&fragment, &secret).await?;
            print_json(client.raw_json("session.info", &json!({}), None).await?);
        }
        Command::Session => print_json(client.raw_json("session.info", &json!({}), None).await?),
        Command::Raw {
            op,
            json,
            fragment,
            secret,
        } => {
            auth_if_present(&mut client, fragment.as_deref(), secret.as_deref()).await?;
            let payload = parse_json(&json)?;
            let response = client.operation(&op, &payload, None).await?;
            print_operation_body(response.body);
        }
        Command::Health(auth) => {
            auth_fragment(&mut client, &auth).await?;
            print_operation(&mut client, "cluster.health", json!({})).await?;
        }
        Command::Membership(auth) => {
            auth_fragment(&mut client, &auth).await?;
            print_operation(&mut client, "cluster.membership", json!({})).await?;
        }
        Command::Layout(auth) => {
            auth_fragment(&mut client, &auth).await?;
            print_operation(&mut client, "cluster.layout", json!({})).await?;
        }
        Command::Usage {
            fragment,
            secret,
            target_fragment,
        } => {
            client.auth_fragment(&fragment, &secret).await?;
            print_operation(
                &mut client,
                "cluster.usage",
                json!({ "fragment": target_fragment }),
            )
            .await?;
        }
        Command::List {
            fragment,
            secret,
            path,
            limit,
            trash,
        } => {
            client.auth_fragment(&fragment, &secret).await?;
            print_operation(
                &mut client,
                "namespace.list",
                json!({ "path": path, "limit": limit, "trash": trash }),
            )
            .await?;
        }
        Command::UploadOneShot {
            fragment,
            secret,
            object_key,
            file,
            content_type,
        } => {
            client.auth_fragment(&fragment, &secret).await?;
            let mut file_handle = File::open(&file).await?;
            let size = file_handle.metadata().await?.len();
            let response = client
                .operation_reader(
                    "upload.one_shot",
                    &json!({
                        "object_key": object_key,
                        "content_type": content_type,
                    }),
                    &mut file_handle,
                    size,
                )
                .await?;
            print_operation_body(response.body);
        }
    }
    Ok(())
}

async fn auth_fragment(client: &mut N2NativeClient, auth: &AuthArgs) -> NativeClientResult<()> {
    client.auth_fragment(&auth.fragment, &auth.secret).await
}

async fn auth_if_present(
    client: &mut N2NativeClient,
    fragment: Option<&str>,
    secret: Option<&str>,
) -> NativeClientResult<()> {
    match (fragment, secret) {
        (Some(fragment), Some(secret)) => client.auth_fragment(fragment, secret).await,
        (None, None) => Ok(()),
        _ => Err(n2_native_client::NativeClientError::protocol(
            "--fragment and --secret must be provided together",
        )),
    }
}

async fn print_operation(
    client: &mut N2NativeClient,
    op: &str,
    payload: Value,
) -> NativeClientResult<()> {
    let response = client.operation(op, &payload, None).await?;
    print_operation_body(response.body);
    Ok(())
}

fn parse_json(raw: &str) -> NativeClientResult<Value> {
    serde_json::from_str(raw).map_err(Into::into)
}

fn print_json(value: Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(&value).unwrap_or_else(|_| "null".to_string())
    );
}

fn print_operation_body(body: Vec<u8>) {
    if body.is_empty() {
        println!("null");
        return;
    }
    if let Ok(value) = rmp_serde::from_slice::<Value>(&body) {
        print_json(value);
        return;
    }
    if let Ok(value) = serde_json::from_slice::<Value>(&body) {
        print_json(value);
        return;
    }
    println!("{}", String::from_utf8_lossy(&body));
}
