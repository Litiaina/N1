use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Duration;

use rand::{RngCore, rngs::OsRng};
use rayon::prelude::*;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::install::{
    BlockDisk, CommandOutcome, Confirmation, Emit, HostOps, InstallSettings, OutputKind,
    ServiceAction, atomic_write, find_disk, format_disk_details, human_bytes, render_gateway_unit,
    render_node_unit, valid_ndisk_label,
};

const AGENT_PROTOCOL_VERSION: u32 = 2;
const PLAN_FORMAT_VERSION: u32 = 3;
const MEMBERSHIP_FORMAT_VERSION: u32 = 5;

static PROVISION_HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(900))
        .build()
        .expect("build N2 provisioning HTTP client")
});

mod agent;
mod commissioner;
#[path = "provision/disk_commands.rs"]
mod disk_commands;
mod support;

use support::*;

pub use agent::{
    AgentCommand, AgentController, AgentIdentity, AgentResponse, AgentServiceAction, PairRequest,
    PairResponse, ServiceTarget,
};

mod model;

use model::CommissionerPlan;
pub use model::{
    AgentBinding, ClusterSpec, DeploymentEnvelope, DiskSpec, GatewaySpec, HostSpec, NodeSpec,
    PoolSpec,
};

pub struct ProvisionShell {
    settings: InstallSettings,
    allow_shared_failure_domain: bool,
}

impl ProvisionShell {
    pub fn new(
        settings: InstallSettings,
        allow_shared_failure_domain: bool,
    ) -> Result<Self, String> {
        Ok(Self {
            settings,
            allow_shared_failure_domain,
        })
    }

    pub fn execute(
        &self,
        line: &str,
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> CommandOutcome {
        match self.execute_inner(line, confirmation, emit) {
            Ok(mut outcome) => {
                outcome.prompt_state = self.current_prompt_state();
                outcome
            }
            Err(error) => {
                emit(OutputKind::Error, error);
                CommandOutcome {
                    success: false,
                    prompt_state: self.current_prompt_state(),
                    ..CommandOutcome::default()
                }
            }
        }
    }

    pub fn current_prompt_state(&self) -> String {
        let Ok(plan) = self.load_plan() else {
            return "unconfigured".into();
        };
        if plan.committed_hash.is_some() {
            format!("{}:committed", plan.spec.cluster_id)
        } else {
            format!("{}:draft", plan.spec.cluster_id)
        }
    }

    fn execute_inner(
        &self,
        line: &str,
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let arguments = split_command(line)?;
        if arguments.is_empty() {
            return Ok(success_outcome());
        }
        match arguments[0].as_str() {
            "help" => {
                emit(OutputKind::Normal, HELP_TEXT.into());
                Ok(success_outcome())
            }
            "clear" => Ok(CommandOutcome {
                success: true,
                clear: true,
                ..CommandOutcome::default()
            }),
            "version" => {
                emit(
                    OutputKind::Normal,
                    format!(
                        "N2 Shell {} / agent protocol v{AGENT_PROTOCOL_VERSION}",
                        env!("CARGO_PKG_VERSION")
                    ),
                );
                Ok(success_outcome())
            }
            "cluster" => self.cluster_command(&arguments[1..], confirmation, emit),
            "host" => self.host_command(&arguments[1..], emit),
            "node" => self.node_command(&arguments[1..], emit),
            "pool" => self.pool_command(&arguments[1..], emit),
            "reserve" => self.reserve_command(&arguments[1..], emit),
            "lsblk" => self.lsblk_command(&arguments[1..], emit),
            "disk" => self.disk_command(&arguments[1..], confirmation, emit),
            "config" => self.config_command(&arguments[1..], emit),
            "gateway" => self.gateway_command(&arguments[1..], emit),
            "logs" => self.logs_command(&arguments[1..], emit),
            command => Err(format!("unknown command '{command}'; type 'help'")),
        }
    }

    fn cluster_command(
        &self,
        arguments: &[String],
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        match arguments.first().map(String::as_str).unwrap_or("") {
            "init" => {
                let cluster_id = arguments.get(1).ok_or("usage: cluster init <cluster-id>")?;
                validate_id(cluster_id, "cluster ID")?;
                if self.plan_path().exists() {
                    return Err("a commissioner plan already exists; use 'cluster show'".into());
                }
                let commissioner_id = format!("commissioner-{}", Uuid::new_v4());
                let plan = CommissionerPlan {
                    spec: ClusterSpec {
                        format_version: PLAN_FORMAT_VERSION,
                        cluster_id: cluster_id.clone(),
                        topology_epoch: 1,
                        hosts: BTreeMap::new(),
                        nodes: BTreeMap::new(),
                        pools: BTreeMap::new(),
                        reserve_nodes: BTreeSet::new(),
                        gateways: BTreeMap::new(),
                    },
                    commissioner_id,
                    tls_certificate_path: None,
                    tls_private_key_path: None,
                    env: generated_env(),
                    committed_hash: None,
                };
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("cluster '{cluster_id}' draft initialized; enroll every physical host agent next"),
                );
                Ok(success_outcome())
            }
            "show" => {
                emit(OutputKind::Normal, self.plan_summary()?);
                Ok(success_outcome())
            }
            "validate" => {
                let plan = self.load_plan()?;
                self.validate_remote_plan(&plan, emit)?;
                Ok(success_outcome())
            }
            "commit" => {
                let mut plan = self.load_plan()?;
                self.validate_remote_plan(&plan, emit)?;
                let envelope = self.envelope(&plan)?;
                let expected = format!("COMMIT {} {}", plan.spec.cluster_id, envelope.plan_hash);
                if confirmation != Some(expected.as_str()) {
                    return Ok(CommandOutcome {
                        confirmation: Some(Confirmation {
                            prompt: format!(
                                "Prepare and commit topology {} to {} independent physical agents. Services remain stopped.\nType '{}' to continue:",
                                envelope.plan_hash,
                                plan.spec.hosts.len(),
                                expected
                            ),
                            expected,
                        }),
                        ..CommandOutcome::default()
                    });
                }
                self.distributed_commit(&plan, &envelope, emit)?;
                plan.committed_hash = Some(envelope.plan_hash.clone());
                self.save_plan(&plan)?;
                write_json(&self.last_committed_plan_path(), &plan, 0o600)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "deployment {} committed on every physical node; no engine or gateway was started",
                        envelope.plan_hash
                    ),
                );
                Ok(success_outcome())
            }
            "evolve" => {
                let mut plan = self.require_committed_plan()?;
                let epoch = parse_u64(
                    arguments.get(1),
                    "usage: cluster evolve <new-topology-epoch>",
                )?;
                if epoch <= plan.spec.topology_epoch {
                    return Err(format!(
                        "new topology epoch must be greater than {}",
                        plan.spec.topology_epoch
                    ));
                }
                write_json(&self.last_committed_plan_path(), &plan, 0o600)?;
                plan.spec.topology_epoch = epoch;
                plan.committed_hash = None;
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "topology epoch {epoch} opened as a draft; the currently committed cluster keeps running until a new prepare/commit succeeds"
                    ),
                );
                Ok(success_outcome())
            }
            "discard-draft" => {
                let plan = self.load_plan()?;
                if plan.committed_hash.is_some() {
                    return Err("there is no uncommitted topology draft to discard".into());
                }
                let committed: CommissionerPlan =
                    read_json(&self.last_committed_plan_path(), "last committed plan")?;
                self.save_plan(&committed)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "draft discarded; commissioner restored committed topology {}",
                        committed.committed_hash.as_deref().unwrap_or("unknown")
                    ),
                );
                Ok(success_outcome())
            }
            "start" => {
                let plan = self.require_committed_plan()?;
                self.cluster_start(&plan, emit)?;
                Ok(success_outcome())
            }
            "stop" => {
                let plan = self.runtime_plan()?;
                self.service_cluster(&plan, AgentServiceAction::Stop, true, emit)?;
                Ok(success_outcome())
            }
            "restart" => {
                let plan = self.require_committed_plan()?;
                self.service_cluster(&plan, AgentServiceAction::Stop, true, emit)?;
                self.cluster_start(&plan, emit)?;
                Ok(success_outcome())
            }
            "enable" | "disable" => {
                let plan = if arguments[0] == "enable" {
                    self.require_committed_plan()?
                } else {
                    self.runtime_plan()?
                };
                let action = if arguments[0] == "enable" {
                    AgentServiceAction::Enable
                } else {
                    AgentServiceAction::Disable
                };
                self.service_cluster(&plan, action, true, emit)?;
                Ok(success_outcome())
            }
            "status" => {
                emit(OutputKind::Normal, self.plan_summary()?);
                let plan = self.runtime_plan()?;
                self.cluster_status(&plan, emit)?;
                Ok(success_outcome())
            }
            _ => Err("usage: cluster <init|show|validate|commit|evolve|discard-draft|start|stop|restart|enable|disable|status>".into()),
        }
    }

    fn host_command(&self, arguments: &[String], emit: Emit<'_>) -> Result<CommandOutcome, String> {
        match arguments.first().map(String::as_str).unwrap_or("") {
            "pair" => {
                let usage = "usage: host pair <host-id> <agent-url> <agent-setup-token>";
                let host_id = arguments.get(1).ok_or(usage)?;
                let agent_url = normalize_agent_url(arguments.get(2).ok_or(usage)?)?;
                let pair_token = arguments.get(3).ok_or(usage)?;
                validate_id(host_id, "host ID")?;
                let mut plan = self.load_plan()?;
                self.ensure_draft(&plan)?;
                if plan.spec.hosts.contains_key(host_id) {
                    return Err(format!(
                        "host '{host_id}' is already paired; physical identity cannot be overwritten"
                    ));
                }
                let response = self.pair_agent(
                    &agent_url,
                    pair_token,
                    &PairRequest {
                        cluster_id: plan.spec.cluster_id.clone(),
                        commissioner_id: plan.commissioner_id.clone(),
                    },
                )?;
                if response.identity.protocol_version != AGENT_PROTOCOL_VERSION {
                    return Err(format!(
                        "agent protocol {} is incompatible with required protocol {}",
                        response.identity.protocol_version, AGENT_PROTOCOL_VERSION
                    ));
                }
                if plan.spec.hosts.values().any(|existing| {
                    existing.agent.agent_id == response.identity.agent_id
                        || (!self.allow_shared_failure_domain
                            && existing.agent.machine_fingerprint
                                == response.identity.machine_fingerprint)
                }) {
                    return Err(
                        "this physical agent or machine fingerprint is already enrolled".into(),
                    );
                }
                plan.spec.hosts.insert(
                    host_id.clone(),
                    HostSpec {
                        id: host_id.clone(),
                        agent: AgentBinding {
                            url: agent_url,
                            agent_id: response.identity.agent_id,
                            machine_fingerprint: response.identity.machine_fingerprint,
                            mount_root: response.identity.mount_root,
                            credential: response.credential,
                        },
                    },
                );
                plan.committed_hash = None;
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "enrolled physical host {host_id} ({}) through {}",
                        response.identity.hostname, plan.spec.hosts[host_id].agent.url
                    ),
                );
                Ok(success_outcome())
            }
            "list" => {
                let plan = self.load_plan()?;
                for host in plan.spec.hosts.values() {
                    let nodes = plan
                        .spec
                        .nodes_for_host(&host.id)
                        .iter()
                        .map(|node| node.id.as_str())
                        .collect::<Vec<_>>()
                        .join(",");
                    emit(
                        OutputKind::Normal,
                        format!(
                            "{}  agent={}  nodes=[{}]  failure-domain={}",
                            host.id,
                            host.agent.url,
                            nodes,
                            &host.agent.machine_fingerprint
                                [..16.min(host.agent.machine_fingerprint.len())]
                        ),
                    );
                }
                Ok(success_outcome())
            }
            _ => Err("usage: host <pair|list> ...".into()),
        }
    }

    fn node_command(&self, arguments: &[String], emit: Emit<'_>) -> Result<CommandOutcome, String> {
        match arguments.first().map(String::as_str).unwrap_or("") {
            "create" => {
                let usage = "usage: node create <node-id> <host-id> <advertised-host> <https-port> <native-port>";
                let node_id = arguments.get(1).ok_or(usage)?;
                let host_id = arguments.get(2).ok_or(usage)?;
                let advertised_host = arguments.get(3).ok_or(usage)?;
                let https_port = parse_port(arguments.get(4), usage)?;
                let native_port = parse_port(arguments.get(5), usage)?;
                validate_id(node_id, "node ID")?;
                validate_host(advertised_host)?;
                if https_port == native_port {
                    return Err("HTTPS and native ports must differ".into());
                }
                let mut plan = self.load_plan()?;
                self.ensure_draft(&plan)?;
                if !plan.spec.hosts.contains_key(host_id) {
                    return Err(format!("physical host '{host_id}' is not enrolled"));
                }
                if plan.spec.nodes.contains_key(node_id) {
                    return Err(format!("node '{node_id}' already exists"));
                }
                for existing in plan.spec.nodes.values() {
                    if existing.advertised_host == *advertised_host
                        && [existing.https_port, existing.native_port]
                            .iter()
                            .any(|port| *port == https_port || *port == native_port)
                    {
                        return Err(format!("endpoint collides with node '{}'", existing.id));
                    }
                }
                plan.spec.nodes.insert(
                    node_id.clone(),
                    NodeSpec {
                        id: node_id.clone(),
                        host_id: host_id.clone(),
                        advertised_host: advertised_host.clone(),
                        https_port,
                        native_port,
                        disks: BTreeMap::new(),
                    },
                );
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "created node {node_id} on {host_id} at https://{advertised_host}:{https_port} / n2://{advertised_host}:{native_port}"
                    ),
                );
                Ok(success_outcome())
            }
            "list" => {
                let plan = self.load_plan()?;
                for node in plan.spec.nodes.values() {
                    emit(
                        OutputKind::Normal,
                        format!(
                            "{}  physical-host={}  endpoint={}:{}  native={}:{}  disks=[{}]",
                            node.id,
                            node.host_id,
                            node.advertised_host,
                            node.https_port,
                            node.advertised_host,
                            node.native_port,
                            node.disks.keys().cloned().collect::<Vec<_>>().join(",")
                        ),
                    );
                }
                Ok(success_outcome())
            }
            _ => Err("usage: node <create|list> ...".into()),
        }
    }

    fn pool_command(&self, arguments: &[String], emit: Emit<'_>) -> Result<CommandOutcome, String> {
        let mut plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        match arguments.first().map(String::as_str).unwrap_or("") {
            "create" => {
                let usage = "usage: pool create <pool-id> <generation> <data> <parity>";
                let id = arguments.get(1).ok_or(usage)?;
                validate_id(id, "pool ID")?;
                let generation = parse_u64(arguments.get(2), usage)?;
                let data_shards = parse_usize(arguments.get(3), usage)?;
                let parity_shards = parse_usize(arguments.get(4), usage)?;
                if data_shards == 0 || parity_shards == 0 {
                    return Err("N2 EC requires at least 1 data and 1 parity node".into());
                }
                if plan.spec.pools.contains_key(id) {
                    return Err(format!(
                        "pool '{id}' already exists; static pool contracts cannot be overwritten"
                    ));
                }
                plan.spec.pools.insert(
                    id.clone(),
                    PoolSpec {
                        id: id.clone(),
                        generation,
                        data_shards,
                        parity_shards,
                        node_ids: Vec::new(),
                        writable: true,
                    },
                );
                plan.spec.topology_epoch = plan.spec.topology_epoch.max(generation);
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "pool {id} generation {generation} created with static EC {data_shards}+{parity_shards}"
                    ),
                );
                Ok(success_outcome())
            }
            "enroll" => {
                let pool_id = arguments
                    .get(1)
                    .ok_or("usage: pool enroll <pool-id> <node-id> [node-id ...]")?;
                if arguments.len() < 3 {
                    return Err("pool enroll requires at least one node ID".into());
                }
                let pool = plan
                    .spec
                    .pools
                    .get_mut(pool_id)
                    .ok_or_else(|| format!("pool '{pool_id}' is not defined"))?;
                for node_id in &arguments[2..] {
                    if !plan.spec.nodes.contains_key(node_id) {
                        return Err(format!("node '{node_id}' is not paired"));
                    }
                    if !pool.node_ids.contains(node_id) {
                        pool.node_ids.push(node_id.clone());
                    }
                }
                if pool.node_ids.len() > pool.width() {
                    return Err(format!(
                        "pool '{}' accepts exactly {} nodes",
                        pool.id,
                        pool.width()
                    ));
                }
                let count = pool.node_ids.len();
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("pool {pool_id} now has {count} enrolled node(s)"),
                );
                Ok(success_outcome())
            }
            "seal" | "writable" => {
                let pool_id = arguments
                    .get(1)
                    .ok_or("usage: pool <seal|writable> <pool-id>")?;
                let pool = plan
                    .spec
                    .pools
                    .get_mut(pool_id)
                    .ok_or_else(|| format!("pool '{pool_id}' is not defined"))?;
                pool.writable = arguments[0] == "writable";
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("pool {pool_id} state set to {}", arguments[0]),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: pool <create|enroll|seal|writable> ...".into()),
        }
    }

    fn reserve_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let mut plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        match arguments.first().map(String::as_str).unwrap_or("") {
            "enroll" => {
                let node_id = arguments.get(1).ok_or("usage: reserve enroll <node-id>")?;
                if !plan.spec.nodes.contains_key(node_id) {
                    return Err(format!("node '{node_id}' is not paired"));
                }
                plan.spec.reserve_nodes.insert(node_id.clone());
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("node {node_id} enrolled in the cluster reserve set"),
                );
                Ok(success_outcome())
            }
            "remove" => {
                let node_id = arguments.get(1).ok_or("usage: reserve remove <node-id>")?;
                plan.spec.reserve_nodes.remove(node_id);
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("node {node_id} removed from the draft reserve set"),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: reserve <enroll|remove> <node-id>".into()),
        }
    }

    fn config_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let mut plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        if arguments.first().map(String::as_str) != Some("tls") {
            return Err("usage: config tls <certificate> <private-key>".into());
        }
        let certificate = absolute_path(
            arguments
                .get(1)
                .ok_or("usage: config tls <certificate> <private-key>")?,
        )?;
        let private_key = absolute_path(
            arguments
                .get(2)
                .ok_or("usage: config tls <certificate> <private-key>")?,
        )?;
        ensure_regular_file(&certificate, "TLS certificate")?;
        ensure_regular_file(&private_key, "TLS private key")?;
        plan.tls_certificate_path = Some(certificate.to_string_lossy().into_owned());
        plan.tls_private_key_path = Some(private_key.to_string_lossy().into_owned());
        self.save_plan(&plan)?;
        emit(
            OutputKind::Success,
            "shared TLS sources configured for distributed staging".into(),
        );
        Ok(success_outcome())
    }

    fn gateway_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let mut plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        match arguments.first().map(String::as_str).unwrap_or("") {
            "add" => {
                let usage = "usage: gateway add <gateway-id> <node-id> <bind-host> <https-port>";
                let id = arguments.get(1).ok_or(usage)?;
                let node_id = arguments.get(2).ok_or(usage)?;
                let bind_host = arguments.get(3).ok_or(usage)?;
                let https_port = parse_port(arguments.get(4), usage)?;
                validate_id(id, "gateway ID")?;
                validate_host(bind_host)?;
                if plan.spec.gateways.contains_key(id) {
                    return Err(format!(
                        "gateway '{id}' already exists; remove it from this draft before redefining it"
                    ));
                }
                let host_id = plan
                    .spec
                    .nodes
                    .get(node_id)
                    .ok_or_else(|| format!("node '{node_id}' is not enrolled"))?
                    .host_id
                    .clone();
                if plan
                    .spec
                    .gateways
                    .values()
                    .filter(|gateway| gateway.id != *id)
                    .filter_map(|gateway| plan.spec.nodes.get(&gateway.node_id))
                    .any(|node| node.host_id == host_id)
                {
                    return Err(format!("physical host '{host_id}' already owns a gateway"));
                }
                plan.spec.gateways.insert(
                    id.clone(),
                    GatewaySpec {
                        id: id.clone(),
                        node_id: node_id.clone(),
                        bind_host: bind_host.clone(),
                        https_port,
                    },
                );
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "gateway {id} assigned to {node_id} at https://{bind_host}:{https_port}"
                    ),
                );
                Ok(success_outcome())
            }
            "remove" => {
                let id = arguments
                    .get(1)
                    .ok_or("usage: gateway remove <gateway-id>")?;
                plan.spec.gateways.remove(id);
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("gateway {id} removed from draft"),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: gateway <add|remove> ...".into()),
        }
    }

    fn logs_command(&self, arguments: &[String], emit: Emit<'_>) -> Result<CommandOutcome, String> {
        let usage = "usage: logs <node-id|gateway-id> [count]";
        let target = arguments.first().ok_or(usage)?;
        let lines = arguments
            .get(1)
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| usage)?
            .unwrap_or(100);
        let plan = self.load_plan()?;
        let (node, service_target) = if let Some(node) = plan.spec.nodes.get(target) {
            (
                node,
                ServiceTarget::Node {
                    node_id: node.id.clone(),
                },
            )
        } else if let Some(gateway) = plan.spec.gateways.get(target) {
            (
                plan.spec
                    .nodes
                    .get(&gateway.node_id)
                    .expect("validated gateway node"),
                ServiceTarget::Gateway,
            )
        } else {
            return Err(format!("unknown node or gateway '{target}'"));
        };
        let host = plan.spec.host_for_node(node);
        let response = self.agent_call(
            &host.agent,
            &AgentCommand::Logs {
                target: service_target,
                lines,
            },
        )?;
        emit_agent_response(&response, emit)?;
        Ok(success_outcome())
    }

    fn plan_summary(&self) -> Result<String, String> {
        let plan = self.load_plan()?;
        let mut output = format!(
            "N2 DISTRIBUTED CLUSTER PLAN\n  cluster: {} / epoch {}\n  commissioner: {}\n  state: {}\n  physical hosts: {}\n  logical N2 nodes: {}\n  server pools: {}\n  reserve nodes: {}\n  gateways: {}",
            plan.spec.cluster_id,
            plan.spec.topology_epoch,
            plan.commissioner_id,
            if plan.committed_hash.is_some() {
                "committed"
            } else {
                "draft"
            },
            plan.spec.hosts.len(),
            plan.spec.nodes.len(),
            plan.spec.pools.len(),
            plan.spec.reserve_nodes.len(),
            plan.spec.gateways.len(),
        );
        for host in plan.spec.hosts.values() {
            output.push_str(&format!(
                "\n  host {}  agent={}  nodes=[{}]",
                host.id,
                host.agent.url,
                plan.spec
                    .nodes_for_host(&host.id)
                    .iter()
                    .map(|node| node.id.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        for pool in plan.spec.pools.values() {
            output.push_str(&format!(
                "\n  pool {} generation {} / EC {}+{} / {}/{} nodes / {}",
                pool.id,
                pool.generation,
                pool.data_shards,
                pool.parity_shards,
                pool.node_ids.len(),
                pool.width(),
                if pool.writable { "writable" } else { "sealed" }
            ));
        }
        for node in plan.spec.nodes.values() {
            output.push_str(&format!(
                "\n  {}  physical-host={}  https://{}:{}  n2://{}:{}  disks=[{}]",
                node.id,
                node.host_id,
                node.advertised_host,
                node.https_port,
                node.advertised_host,
                node.native_port,
                node.disks.keys().cloned().collect::<Vec<_>>().join(",")
            ));
        }
        if let Some(hash) = plan.committed_hash {
            output.push_str(&format!("\n  committed hash: {hash}"));
        }
        Ok(output)
    }

    fn ensure_draft(&self, plan: &CommissionerPlan) -> Result<(), String> {
        if plan.committed_hash.is_some() {
            return Err("the cluster plan is committed; topology evolution requires a new staged epoch, not mutation of this committed plan".into());
        }
        Ok(())
    }

    fn require_committed_plan(&self) -> Result<CommissionerPlan, String> {
        let plan = self.load_plan()?;
        if plan.committed_hash.is_none() {
            return Err("run 'cluster commit' before activation".into());
        }
        Ok(plan)
    }

    fn runtime_plan(&self) -> Result<CommissionerPlan, String> {
        let plan = self.load_plan()?;
        if plan.committed_hash.is_some() {
            return Ok(plan);
        }
        read_json(&self.last_committed_plan_path(), "last committed plan")
    }

    fn plan_path(&self) -> PathBuf {
        self.settings
            .setup_root
            .join("n2_cluster_commissioner.json")
    }

    fn last_committed_plan_path(&self) -> PathBuf {
        self.settings
            .setup_root
            .join("n2_cluster_last_committed.json")
    }

    fn load_plan(&self) -> Result<CommissionerPlan, String> {
        read_json(&self.plan_path(), "commissioner plan")
    }

    fn save_plan(&self, plan: &CommissionerPlan) -> Result<(), String> {
        write_json(&self.plan_path(), plan, 0o600)
    }
}

const HELP_TEXT: &str = "COMMANDS
  help
  clear
  version
  cluster init <cluster-id>
  host pair <host-id> <agent-url> <token>
  host list
  node create <node-id> <host-id> <advertised-host> <https-port> <native-port>
  node list
  lsblk <host-id>
  disk inspect <host-id> <device-or-label>
  disk format <host-id> <device> <N2DISKxxx>
  disk mount <host-id> <N2DISKxxx>
  disk assign <node-id> <N2DISKxxx>
  disk unassign <node-id> <N2DISKxxx>
  pool create <pool-id> <generation> <data> <parity>
  pool enroll <pool-id> <node-id> [node-id ...]
  pool seal|writable <pool-id>
  reserve enroll|remove <node-id>
  config tls <certificate> <private-key>
  gateway add <gateway-id> <node-id> <bind-host> <https-port>
  gateway remove <gateway-id>
  cluster show|validate|commit
  cluster evolve <new-topology-epoch>
  cluster discard-draft
  cluster start|stop|restart|enable|disable|status
  logs <node-id|gateway-id> [count]

Disk discovery, formatting, and mounting execute only on the selected physical
host agent. Assignment binds a prepared disk to one logical N2 node. Cluster commit
uses canonical SHA-256 identity, parallel prepare, and idempotent local commit.
The commissioner and Shell are not part of the N2 data path after activation.";

#[cfg(test)]
#[path = "provision/tests.rs"]
mod tests;
