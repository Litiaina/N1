use super::*;

impl ProvisionShell {
    pub(super) fn validate_remote_plan(
        &self,
        plan: &CommissionerPlan,
        emit: Emit<'_>,
    ) -> Result<(), String> {
        plan.spec.validate()?;
        if !self.allow_shared_failure_domain {
            let distinct_fingerprints = plan
                .spec
                .hosts
                .values()
                .map(|host| &host.agent.machine_fingerprint)
                .collect::<BTreeSet<_>>();
            if distinct_fingerprints.len() != plan.spec.hosts.len() {
                return Err("multiple enrolled hosts share one physical machine fingerprint; this is not an independent failure-domain topology".into());
            }
            let shared = plan
                .spec
                .hosts
                .values()
                .filter(|host| plan.spec.nodes_for_host(&host.id).len() != 1)
                .map(|host| host.id.clone())
                .collect::<Vec<_>>();
            if !shared.is_empty() {
                return Err(format!(
                    "production commissioning requires exactly one N2 node per physical host; laboratory multi-instance hosts: {} (use --allow-shared-failure-domain only for simulation)",
                    shared.join(", ")
                ));
            }
        }
        let certificate = plan
            .tls_certificate_path
            .as_ref()
            .ok_or("TLS certificate is not configured")?;
        let private_key = plan
            .tls_private_key_path
            .as_ref()
            .ok_or("TLS private key is not configured")?;
        ensure_regular_file(Path::new(certificate), "TLS certificate")?;
        ensure_regular_file(Path::new(private_key), "TLS private key")?;
        validate_env(&plan.env)?;

        let results = plan
            .spec
            .hosts
            .par_iter()
            .map(|(host_id, host)| {
                let result = self
                    .agent_call(&host.agent, &AgentCommand::Inventory)
                    .and_then(|response| {
                        ensure_agent_success(&response)?;
                        let disks: Vec<BlockDisk> = serde_json::from_value(response.data)
                            .map_err(|error| format!("decode inventory: {error}"))?;
                        for node in plan.spec.nodes_for_host(host_id) {
                            for (label, binding) in &node.disks {
                                let disk = find_disk(&disks, label)
                                    .ok_or_else(|| format!("{label} is absent"))?;
                                let expected = Path::new(&host.agent.mount_root).join(label);
                                if disk.filesystem.as_deref() != Some("xfs")
                                    || !disk
                                        .mountpoints
                                        .iter()
                                        .any(|mount| Path::new(mount) == expected)
                                {
                                    return Err(format!(
                                        "{label} is not mounted XFS at {}",
                                        expected.display()
                                    ));
                                }
                                if disk.uuid.as_deref() != Some(binding.filesystem_uuid.as_str()) {
                                    return Err(format!("{label} filesystem UUID changed"));
                                }
                                if !binding.serial.is_empty() && disk.serial != binding.serial {
                                    return Err(format!("{label} hardware serial changed"));
                                }
                            }
                        }
                        Ok(())
                    });
                (host_id.clone(), result)
            })
            .collect::<Vec<_>>();
        let failures = results
            .into_iter()
            .filter_map(|(node, result)| result.err().map(|error| format!("{node}: {error}")))
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            return Err(format!(
                "physical-host validation failed:\n  {}",
                failures.join("\n  ")
            ));
        }
        let envelope = self.envelope(plan)?;
        emit(
            OutputKind::Success,
            format!(
                "Validation: PASS\nCluster: {}\nTopology hash: {}\nServer pools: {}\nPhysical hosts: {}\nLogical N2 nodes: {}\nPhysical failure domains: {}\nStorage disks: {}\nReserve nodes: {}\nGateways: {}",
                plan.spec.cluster_id,
                envelope.plan_hash,
                plan.spec.pools.len(),
                plan.spec.hosts.len(),
                plan.spec.nodes.len(),
                plan.spec
                    .hosts
                    .values()
                    .map(|host| &host.agent.machine_fingerprint)
                    .collect::<BTreeSet<_>>()
                    .len(),
                plan.spec
                    .nodes
                    .values()
                    .map(|node| node.disks.len())
                    .sum::<usize>(),
                plan.spec.reserve_nodes.len(),
                plan.spec.gateways.len(),
            ),
        );
        Ok(())
    }

    pub(super) fn distributed_commit(
        &self,
        plan: &CommissionerPlan,
        envelope: &DeploymentEnvelope,
        emit: Emit<'_>,
    ) -> Result<(), String> {
        emit(
            OutputKind::Info,
            format!(
                "[1/2] Preparing {} physical hosts in parallel",
                plan.spec.hosts.len()
            ),
        );
        let prepared = plan
            .spec
            .hosts
            .par_iter()
            .map(|(host_id, host)| {
                let result = self
                    .agent_call(
                        &host.agent,
                        &AgentCommand::Prepare {
                            envelope: envelope.clone(),
                            host_id: host_id.clone(),
                        },
                    )
                    .and_then(|response| ensure_agent_success(&response));
                (host_id.clone(), result)
            })
            .collect::<Vec<_>>();
        let failures = prepared
            .iter()
            .filter_map(|(node, result)| {
                result
                    .as_ref()
                    .err()
                    .map(|error| format!("{node}: {error}"))
            })
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            plan.spec.hosts.par_iter().for_each(|(_, host)| {
                let _ = self.agent_call(
                    &host.agent,
                    &AgentCommand::Abort {
                        plan_hash: envelope.plan_hash.clone(),
                    },
                );
            });
            return Err(format!(
                "prepare failed; every successful stage was aborted:\n  {}",
                failures.join("\n  ")
            ));
        }
        emit(
            OutputKind::Success,
            "[1/2] Every physical host acknowledged the same topology hash".into(),
        );

        emit(
            OutputKind::Info,
            "[2/2] Committing prepared local state in parallel".into(),
        );
        let committed = plan
            .spec
            .hosts
            .par_iter()
            .map(|(host_id, host)| {
                let result = self
                    .agent_call(
                        &host.agent,
                        &AgentCommand::Commit {
                            plan_hash: envelope.plan_hash.clone(),
                        },
                    )
                    .and_then(|response| ensure_agent_success(&response));
                (host_id.clone(), result)
            })
            .collect::<Vec<_>>();
        let failures = committed
            .into_iter()
            .filter_map(|(node, result)| result.err().map(|error| format!("{node}: {error}")))
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            return Err(format!(
                "commit acknowledgements are incomplete; no service was activated. Re-run the identical commit to converge idempotently:\n  {}",
                failures.join("\n  ")
            ));
        }
        emit(
            OutputKind::Success,
            "[2/2] Every physical host committed; activation remains separate".into(),
        );
        Ok(())
    }

    pub(super) fn cluster_start(
        &self,
        plan: &CommissionerPlan,
        emit: Emit<'_>,
    ) -> Result<(), String> {
        self.service_cluster(plan, AgentServiceAction::Start, false, emit)?;
        thread::sleep(Duration::from_millis(1000));
        let statuses = self.status_results(plan);
        let unhealthy = statuses
            .iter()
            .filter_map(|(node, result)| match result {
                Ok(value) if value.get("node_active").and_then(Value::as_bool) == Some(true) => {
                    None
                }
                Ok(_) => Some(format!("{node}: service inactive")),
                Err(error) => Some(format!("{node}: {error}")),
            })
            .collect::<Vec<_>>();
        if !unhealthy.is_empty() {
            let _ = self.service_cluster(plan, AgentServiceAction::Stop, false, emit);
            return Err(format!(
                "node activation failed; all reachable nodes were stopped and gateways were never started:\n  {}",
                unhealthy.join("\n  ")
            ));
        }
        for gateway in plan.spec.gateways.values() {
            let node = plan
                .spec
                .nodes
                .get(&gateway.node_id)
                .expect("validated gateway");
            let host = plan.spec.host_for_node(node);
            let response = self.agent_call(
                &host.agent,
                &AgentCommand::Service {
                    target: ServiceTarget::Gateway,
                    action: AgentServiceAction::Start,
                },
            )?;
            ensure_agent_success(&response)?;
            emit(
                OutputKind::Success,
                format!("gateway '{}' started on {}", gateway.id, gateway.node_id),
            );
        }
        Ok(())
    }

    pub(super) fn service_cluster(
        &self,
        plan: &CommissionerPlan,
        action: AgentServiceAction,
        include_gateways: bool,
        emit: Emit<'_>,
    ) -> Result<(), String> {
        if include_gateways
            && matches!(
                action,
                AgentServiceAction::Stop | AgentServiceAction::Disable
            )
        {
            for gateway in plan.spec.gateways.values() {
                let node = plan
                    .spec
                    .nodes
                    .get(&gateway.node_id)
                    .expect("validated gateway");
                let host = plan.spec.host_for_node(node);
                let response = self.agent_call(
                    &host.agent,
                    &AgentCommand::Service {
                        target: ServiceTarget::Gateway,
                        action: action.clone(),
                    },
                )?;
                ensure_agent_success(&response)?;
            }
        }
        let results = plan
            .spec
            .nodes
            .par_iter()
            .map(|(node_id, node)| {
                let host = plan.spec.host_for_node(node);
                let result = self
                    .agent_call(
                        &host.agent,
                        &AgentCommand::Service {
                            target: ServiceTarget::Node {
                                node_id: node_id.clone(),
                            },
                            action: action.clone(),
                        },
                    )
                    .and_then(|response| ensure_agent_success(&response));
                (node_id.clone(), result)
            })
            .collect::<Vec<_>>();
        let failures = results
            .into_iter()
            .filter_map(|(node, result)| result.err().map(|error| format!("{node}: {error}")))
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            return Err(format!(
                "cluster service action incomplete:\n  {}",
                failures.join("\n  ")
            ));
        }
        if include_gateways
            && matches!(
                action,
                AgentServiceAction::Start
                    | AgentServiceAction::Restart
                    | AgentServiceAction::Enable
            )
        {
            for gateway in plan.spec.gateways.values() {
                let node = plan
                    .spec
                    .nodes
                    .get(&gateway.node_id)
                    .expect("validated gateway");
                let host = plan.spec.host_for_node(node);
                let response = self.agent_call(
                    &host.agent,
                    &AgentCommand::Service {
                        target: ServiceTarget::Gateway,
                        action: action.clone(),
                    },
                )?;
                ensure_agent_success(&response)?;
            }
        }
        emit(
            OutputKind::Success,
            format!(
                "{} completed across {} physical hosts",
                service_action_name(&action),
                plan.spec.hosts.len()
            ),
        );
        Ok(())
    }

    pub(super) fn cluster_status(
        &self,
        plan: &CommissionerPlan,
        emit: Emit<'_>,
    ) -> Result<(), String> {
        for (node_id, status) in self.status_results(plan) {
            match status {
                Ok(value) => {
                    let active = value
                        .get("node_active")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    emit(
                        if active {
                            OutputKind::Success
                        } else {
                            OutputKind::Warning
                        },
                        format!("{node_id}: {}", if active { "active" } else { "inactive" }),
                    );
                }
                Err(error) => emit(
                    OutputKind::Error,
                    format!("{node_id}: agent unreachable: {error}"),
                ),
            }
        }
        for gateway in plan.spec.gateways.values() {
            let node = plan.spec.nodes.get(&gateway.node_id).expect("gateway node");
            let host = plan.spec.host_for_node(node);
            let response = self.agent_call(
                &host.agent,
                &AgentCommand::Status {
                    node_id: node.id.clone(),
                    gateway: true,
                },
            );
            match response {
                Ok(response) if response.success => {
                    let active = response
                        .data
                        .get("gateway_active")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    emit(
                        if active {
                            OutputKind::Success
                        } else {
                            OutputKind::Warning
                        },
                        format!(
                            "gateway {} on {}: {}",
                            gateway.id,
                            gateway.node_id,
                            if active { "active" } else { "inactive" }
                        ),
                    );
                }
                Ok(response) => emit(
                    OutputKind::Error,
                    format!("gateway {}: {}", gateway.id, response.message),
                ),
                Err(error) => emit(
                    OutputKind::Error,
                    format!("gateway {}: {error}", gateway.id),
                ),
            }
        }
        Ok(())
    }

    pub(super) fn status_results(
        &self,
        plan: &CommissionerPlan,
    ) -> Vec<(String, Result<Value, String>)> {
        plan.spec
            .nodes
            .par_iter()
            .map(|(node_id, node)| {
                let host = plan.spec.host_for_node(node);
                let gateway = plan
                    .spec
                    .gateways
                    .values()
                    .any(|item| item.node_id == *node_id);
                let result = self
                    .agent_call(
                        &host.agent,
                        &AgentCommand::Status {
                            node_id: node_id.clone(),
                            gateway,
                        },
                    )
                    .and_then(|response| {
                        ensure_agent_success(&response)?;
                        Ok(response.data)
                    });
                (node_id.clone(), result)
            })
            .collect()
    }

    pub(super) fn inventory(&self, agent: &AgentBinding) -> Result<Vec<BlockDisk>, String> {
        let response = self.agent_call(agent, &AgentCommand::Inventory)?;
        ensure_agent_success(&response)?;
        serde_json::from_value(response.data).map_err(|error| format!("decode inventory: {error}"))
    }

    pub(super) fn pair_agent(
        &self,
        url: &str,
        token: &str,
        request: &PairRequest,
    ) -> Result<PairResponse, String> {
        let response = PROVISION_HTTP_CLIENT
            .post(format!("{url}/agent/v1/pair"))
            .header("x-n2-setup-token", token)
            .json(request)
            .send()
            .map_err(|error| format!("pair agent {url}: {error}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "agent pairing failed ({}): {}",
                response.status(),
                response.text().unwrap_or_default()
            ));
        }
        response
            .json()
            .map_err(|error| format!("decode pair response: {error}"))
    }

    pub(super) fn agent_call(
        &self,
        binding: &AgentBinding,
        command: &AgentCommand,
    ) -> Result<AgentResponse, String> {
        let response = PROVISION_HTTP_CLIENT
            .post(format!("{}/agent/v1/command", binding.url))
            .bearer_auth(&binding.credential)
            .json(command)
            .send()
            .map_err(|error| format!("agent {} request failed: {error}", binding.url))?;
        if !response.status().is_success() {
            return Err(format!(
                "agent {} returned HTTP {}: {}",
                binding.url,
                response.status(),
                response.text().unwrap_or_default()
            ));
        }
        response
            .json()
            .map_err(|error| format!("decode agent {} response: {error}", binding.url))
    }

    pub(super) fn envelope(&self, plan: &CommissionerPlan) -> Result<DeploymentEnvelope, String> {
        let certificate = fs::read(
            plan.tls_certificate_path
                .as_ref()
                .ok_or("TLS certificate is not configured")?,
        )
        .map_err(|error| format!("read TLS certificate: {error}"))?;
        let private_key = fs::read(
            plan.tls_private_key_path
                .as_ref()
                .ok_or("TLS private key is not configured")?,
        )
        .map_err(|error| format!("read TLS private key: {error}"))?;
        DeploymentEnvelope::new(
            plan.spec.clone(),
            plan.env.clone(),
            certificate,
            private_key,
        )
    }
}
