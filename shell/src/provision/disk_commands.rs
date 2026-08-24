use super::*;

impl ProvisionShell {
    pub(super) fn lsblk_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let host_id = arguments.first().ok_or("usage: lsblk <host-id>")?;
        let plan = self.load_plan()?;
        let host = plan
            .spec
            .hosts
            .get(host_id)
            .ok_or_else(|| format!("physical host '{host_id}' is not enrolled"))?;
        let response = self.agent_call(&host.agent, &AgentCommand::Inventory)?;
        ensure_agent_success(&response)?;
        let disks: Vec<BlockDisk> = serde_json::from_value(response.data)
            .map_err(|error| format!("decode {host_id} inventory: {error}"))?;
        emit(
            OutputKind::Info,
            format!("PHYSICAL HOST {host_id} / {}", host.agent.url),
        );
        emit(
            OutputKind::Normal,
            format!(
                "{:<12} {:>10}  {:<10} {:<12} {:<16} {}",
                "NAME", "SIZE", "FORMAT", "STATE", "LABEL", "MOUNT"
            ),
        );
        for disk in disks {
            emit(
                if disk.system {
                    OutputKind::Warning
                } else {
                    OutputKind::Normal
                },
                format!(
                    "{:<12} {:>10}  {:<10} {:<12} {:<16} {}",
                    disk.name,
                    human_bytes(disk.size_bytes),
                    disk.filesystem.as_deref().unwrap_or("—"),
                    disk.state(),
                    disk.label.as_deref().unwrap_or("—"),
                    if disk.mountpoints.is_empty() {
                        "—".into()
                    } else {
                        disk.mountpoints.join(",")
                    }
                ),
            );
        }
        Ok(success_outcome())
    }

    pub(super) fn disk_command(
        &self,
        arguments: &[String],
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        match arguments.first().map(String::as_str).unwrap_or("") {
            "inspect" => {
                let usage = "usage: disk inspect <host-id> <device-or-label>";
                let host = plan
                    .spec
                    .hosts
                    .get(arguments.get(1).ok_or(usage)?)
                    .ok_or_else(|| "physical host is not enrolled".to_string())?;
                let target = arguments.get(2).ok_or(usage)?;
                let response = self.agent_call(
                    &host.agent,
                    &AgentCommand::InspectDisk {
                        target: target.clone(),
                    },
                )?;
                ensure_agent_success(&response)?;
                emit(OutputKind::Normal, response.message);
                Ok(success_outcome())
            }
            "format" => {
                let usage = "usage: disk format <host-id> <device> <N2DISKxxx>";
                let host_id = arguments.get(1).ok_or(usage)?;
                let host = plan
                    .spec
                    .hosts
                    .get(host_id)
                    .ok_or_else(|| format!("physical host '{host_id}' is not enrolled"))?;
                let target = arguments.get(2).ok_or(usage)?;
                let label = arguments.get(3).ok_or(usage)?;
                if !valid_ndisk_label(label) {
                    return Err("disk label must be N2DISK followed by exactly three digits".into());
                }
                if plan
                    .spec
                    .nodes
                    .values()
                    .any(|existing| existing.disks.contains_key(label))
                {
                    return Err(format!(
                        "disk label {label} is already assigned in this cluster draft"
                    ));
                }
                let inventory = self.inventory(&host.agent)?;
                let disk = find_disk(&inventory, target)
                    .ok_or_else(|| format!("disk '{target}' was not found on {host_id}"))?;
                if disk.system || disk.read_only || !disk.mountpoints.is_empty() {
                    return Err(format!("{} is not a safe formatting candidate", disk.path));
                }
                let expected = format!(
                    "FORMAT {} {label} {}",
                    host.agent.agent_id,
                    disk_serial(disk)
                );
                if confirmation != Some(expected.as_str()) {
                    return Ok(CommandOutcome {
                        confirmation: Some(Confirmation {
                            prompt: format!(
                                "WARNING: destroy signatures on physical host {host_id}, device {}, model '{}', serial '{}'.\nType '{}' to continue:",
                                disk.path,
                                disk.model,
                                disk_serial(disk),
                                expected
                            ),
                            expected,
                        }),
                        ..CommandOutcome::default()
                    });
                }
                let response = self.agent_call(
                    &host.agent,
                    &AgentCommand::FormatDisk {
                        target: target.clone(),
                        label: label.clone(),
                        confirmation: expected,
                    },
                )?;
                emit_agent_response(&response, emit)?;
                Ok(success_outcome())
            }
            "mount" => {
                let usage = "usage: disk mount <host-id> <N2DISKxxx>";
                let host_id = arguments.get(1).ok_or(usage)?;
                let host = plan
                    .spec
                    .hosts
                    .get(host_id)
                    .ok_or_else(|| format!("physical host '{host_id}' is not enrolled"))?;
                let label = arguments.get(2).ok_or(usage)?;
                let response = self.agent_call(
                    &host.agent,
                    &AgentCommand::MountDisk {
                        label: label.clone(),
                    },
                )?;
                emit_agent_response(&response, emit)?;
                Ok(success_outcome())
            }
            "assign" => {
                let usage = "usage: disk assign <node-id> <N2DISKxxx>";
                let node_id = arguments.get(1).ok_or(usage)?;
                let label = arguments.get(2).ok_or(usage)?;
                if !valid_ndisk_label(label) {
                    return Err("invalid N2 disk label".into());
                }
                if plan
                    .spec
                    .nodes
                    .values()
                    .any(|node| node.id != *node_id && node.disks.contains_key(label))
                {
                    return Err(format!("{label} belongs to another physical node"));
                }
                let node = plan
                    .spec
                    .nodes
                    .get(node_id)
                    .ok_or_else(|| format!("node '{node_id}' does not exist"))?;
                let host = plan.spec.host_for_node(node);
                let inventory = self.inventory(&host.agent)?;
                let disk = find_disk(&inventory, label)
                    .ok_or_else(|| format!("{label} is not visible on host '{}'", host.id))?;
                let expected_mount = Path::new(&host.agent.mount_root).join(label);
                if disk.filesystem.as_deref() != Some("xfs")
                    || !disk
                        .mountpoints
                        .iter()
                        .any(|mount| Path::new(mount) == expected_mount)
                {
                    return Err(format!(
                        "{label} is not mounted XFS at {} on host '{}'",
                        expected_mount.display(),
                        host.id
                    ));
                }
                let mut plan = plan;
                let filesystem_uuid = disk
                    .uuid
                    .clone()
                    .ok_or_else(|| format!("{label} has no filesystem UUID"))?;
                plan.spec
                    .nodes
                    .get_mut(node_id)
                    .expect("present")
                    .disks
                    .insert(
                        label.clone(),
                        DiskSpec {
                            label: label.clone(),
                            filesystem_uuid,
                            stable_id: disk.stable_id.clone(),
                            serial: disk.serial.clone(),
                            size_bytes: disk.size_bytes,
                        },
                    );
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("{label} assigned to node {node_id}"),
                );
                Ok(success_outcome())
            }
            "unassign" => {
                let usage = "usage: disk unassign <node-id> <N2DISKxxx>";
                let node_id = arguments.get(1).ok_or(usage)?;
                let label = arguments.get(2).ok_or(usage)?;
                let mut plan = plan;
                let node = plan
                    .spec
                    .nodes
                    .get_mut(node_id)
                    .ok_or_else(|| format!("node '{node_id}' is not paired"))?;
                if node.disks.remove(label).is_none() {
                    return Err(format!("{label} is not assigned to {node_id}"));
                }
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("{label} unassigned from {node_id}"),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: disk <inspect|format|mount|assign|unassign> ...".into()),
        }
    }
}
