use clap::Parser;
use futures_util::future::join_all;
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
};
use tokio_util::io::ReaderStream;

include!("proof_model.rs");
include!("proof_scenarios.rs");
include!("proof_environment.rs");
include!("proof_workloads.rs");
include!("proof_client.rs");
include!("proof_native.rs");
include!("proof_native_bench.rs");
include!("proof_upload_bench.rs");
include!("proof_multipart_resume.rs");
include!("proof_auto_healing.rs");
include!("proof_maintenance_coverage.rs");
include!("proof_maintenance.rs");
include!("proof_jobs.rs");
include!("proof_utils.rs");

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut proof = match Proof::new(args) {
        Ok(proof) => proof,
        Err(err) => {
            eprintln!("N1_RELEASE_PROOF FAIL {err}");
            std::process::exit(1);
        }
    };
    let result = if proof.args.maintenance_coverage_proof {
        proof.run_maintenance_coverage_proof().await
    } else if proof.args.trash_maintenance_proof {
        proof.run_trash_maintenance_proof().await
    } else if proof.args.auto_healing_proof {
        proof.run_auto_healing_proof().await
    } else if proof.args.object_maintenance_gate_proof {
        proof.run_object_maintenance_gate_proof().await
    } else if proof.args.multipart_resume_proof {
        proof.run_multipart_resume_proof().await
    } else if proof.args.upload_benchmark {
        proof.run_upload_benchmark().await
    } else if proof.args.native_transport_benchmark {
        proof.run_native_transport_benchmark().await
    } else if proof.args.native_protocol_proof {
        proof.run_native_protocol_smoke().await
    } else if proof.args.maintenance_interrupt_proof {
        proof.run_maintenance_interrupt_proof().await
    } else {
        proof.run().await
    };
    match result {
        Ok(()) => {
            println!("N1_RELEASE_PROOF PASS");
        }
        Err(err) => {
            proof.summary = json!({
                "status": "FAIL",
                "error": err.to_string(),
            });
            let _ = proof.write_summary();
            eprintln!("N1_RELEASE_PROOF FAIL {err}");
            std::process::exit(1);
        }
    }
}
