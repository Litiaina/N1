impl Proof {
    async fn online_control(&self, secret: &str, payload: Value) -> ProofResult<Value> {
        let response=self.client.post(format!("{}/noa/v1/maintenance/backups/operation",self.base_url))
            .bearer_auth(secret).json(&payload).send().await?;
        if !response.status().is_success() { return Err(format!("online control {}: {}",response.status(),response.text().await?).into()); }
        Ok(response.json().await?)
    }
    async fn online_complete(&self, secret: &str, id: &str) -> ProofResult<Value> {
        let end=Instant::now()+Duration::from_secs(600);
        while Instant::now()<end {
            let status=self.online_control(secret,json!({"operation":"operations"})).await?;
            let job=status["operations"].as_array().and_then(|a|a.iter().find(|j|j["id"]==id)).ok_or("online operation disappeared")?;
            match job["state"].as_str() {
                Some("complete")=>return Ok(job["result"].clone()),
                Some("failed")=>return Err(format!("online capture failed: {}",job["error"]).into()),
                _=>tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        Err("online capture completion deadline".into())
    }
    async fn run_online_backup_proof(&mut self) -> ProofResult<()> {
        use std::os::unix::fs::PermissionsExt;
        self.validate()?;self.prepare_dirs()?;self.write_config()?;
        let evidence=self.args.out_dir.clone();let config=evidence.join("n1_config.ini");
        fs::write(&config,fs::read_to_string(&config)?.replace("file_ttl_seconds=30","file_ttl_seconds=86400"))?;
        let key=evidence.join("backup.key");let repo=self.disk_roots()?[0].join(format!("n1-online-repository-{}",self.stamp));
        let replica=self.disk_roots()?.last().ok_or("missing disk")?.join(format!("n1-online-replica-{}",self.stamp));
        self.backup_cli(&repo,&key,"keygen",&[],true)?;self.backup_cli(&repo,&key,"init",&[],true)?;self.backup_cli(&replica,&key,"init",&[],true)?;
        let token_file=evidence.join("operator.token");let secret=format!("{}{}",ulid::Ulid::new(),ulid::Ulid::new());
        fs::write(&token_file,&secret)?;fs::set_permissions(&token_file,fs::Permissions::from_mode(0o600))?;
        let settings=json!({"bind":"127.0.0.1:0","token_file":token_file,"engine_config":config,"capture_boundary_seconds":30,"repositories":{"primary":{"path":repo,"key_file":key}}});
        let operator=evidence.join("live-operator.json");fs::write(&operator,serde_json::to_vec(&settings)?)?;fs::set_permissions(operator,fs::Permissions::from_mode(0o600))?;
        self.start_server("online-source").await?;self.create_fragment().await?;self.auth().await?;
        let original_pid=self.server.as_ref().ok_or("missing engine")?.id();
        let mut expected=BTreeSet::new();let lineage=self.rename_version_lineage_proof(&mut expected).await?;
        self.concurrent_object_storm(&mut expected,"online-small",128).await?;
        let large_key="online-large.bin";let bytes=self.args.source_file.metadata()?.len() as usize;
        self.multipart_file_benchmark(&self.token,large_key,&self.args.source_file,bytes,16*1024*1024).await?;
        expected.insert(large_key.into());let large_hash=self.sha256_file(&self.args.source_file).await?;
        let mut hashes=Vec::new();for path in &expected { hashes.push((path.clone(),self.download_sha256(path).await?)); }
        let other=self.create_named_fragment(&format!("online-other-{}",self.stamp),"other-key").await?;
        let other_token=self.auth_fragment(&other,"other-key").await?;
        self.upload_bytes_with_token(&other_token,"other.bin",b"snapshot survives source fragment deletion".to_vec(),"application/octet-stream").await?;
        self.upload_bytes("online-trash/file.bin",b"trash committed before view".to_vec(),"application/octet-stream").await?;
        self.posix_post("/noa/v1/posix/trash",json!({"path":"online-trash"})).await?;
        // Acknowledged durable parts for both a NEW key and an EXISTING lineage
        // must not become committed versions in the recovery point.
        let mut unfinished_versions=Vec::new();
        for pending_key in ["online-unfinished.bin",large_key] {
            let response=self.client.post(format!("{}/noa/v1/upload/multipart/durable/init/{pending_key}",self.base_url)).bearer_auth(&self.token)
                .json(&json!({"content_type":"application/octet-stream","total_size_bytes":bytes,"expected_parts":bytes.div_ceil(16*1024*1024),"part_size_bytes":16*1024*1024})).send().await?;
            if response.status()!=StatusCode::CREATED { return Err(format!("durable setup: {}",response.text().await?).into()); }
            let init:MultipartInit=response.json().await?;
            self.upload_durable_part_range(&init.version_id,0,1,16*1024*1024).await?;unfinished_versions.push(init.version_id);
        }
        let wrong=self.client.post(format!("{}/noa/v1/maintenance/backups/operation",self.base_url)).bearer_auth(&self.token).json(&json!({"operation":"create-online","repository":"primary","confirm":true})).send().await?;
        if wrong.status()!=StatusCode::UNAUTHORIZED { return Err("fragment token granted online operator access".into()); }
        let accepted=self.online_control(&secret,json!({"operation":"create-online","repository":"primary","confirm":true})).await?;
        let id=accepted["operation_id"].as_str().ok_or("missing online job identity")?.to_owned();
        let start=Instant::now();
        let scratch=self.index_roots[0].join(".n1-online-backups");
        let end=Instant::now()+Duration::from_secs(40);
        loop {
            let ready=fs::read_dir(&scratch).ok().into_iter().flatten().filter_map(Result::ok).any(|e|e.path().join("ready").is_file());
            if ready { break; }
            if Instant::now()>end { return Err("did not observe pinned online view before bulk copy completed".into()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let pin_observed_ms=start.elapsed().as_millis();
        // These mutations happen AFTER selection but DURING backup payload work.
        // They must succeed and must not alter the selected recovery point.
        let mutation_start=Instant::now();
        self.posix_post("/noa/v1/posix/rename",json!({"source_path":large_key,"destination_path":"online-moved.bin"})).await?;
        self.upload_bytes("online-moved.bin",b"after frozen view".to_vec(),"application/octet-stream").await?;
        self.posix_post("/noa/v1/posix/recover",json!({"path":"online-trash"})).await?;
        self.upload_bytes("online-after.bin",b"new key after frozen view".to_vec(),"application/octet-stream").await?;
        let response=self.client.delete(format!("{}/noa/v1/fragments/{other}",self.base_url)).bearer_auth(&other_token).json(&json!({"secret":self.args.master_key,"confirm_fragment":other})).send().await?;
        if !response.status().is_success() { return Err(format!("post-pin fragment delete: {}",response.text().await?).into()); }
        let concurrent_mutation_ms=mutation_start.elapsed().as_millis();
        let first=self.online_complete(&secret,&id).await?;
        if first["capture_mode"]!="online_committed" { return Err("online capture scope missing".into()); }
        if self.server.as_ref().ok_or("engine missing")?.id()!=original_pid { return Err("online capture restarted N1".into()); }
        let snapshot=first["snapshot"].as_str().ok_or("missing snapshot")?;
        self.backup_cli(&repo,&key,"verify",&["--snapshot",snapshot],true)?;
        self.backup_cli(&repo,&key,"replicate",&["--snapshot",snapshot,"--target-repository",replica.to_str().ok_or("replica path")?,"--target-key-file",key.to_str().ok_or("key path")?],true)?;
        // Kill the actual engine during another captured view, not a Shell
        // controller. A partial repository point must remain unpublished.
        self.online_control(&secret,json!({"operation":"create-online","repository":"primary","confirm":true})).await?;
        let end=Instant::now()+Duration::from_secs(40);
        loop {
            if fs::read_dir(&scratch).ok().into_iter().flatten().filter_map(Result::ok).any(|e|e.path().join("ready").is_file()) { break; }
            if Instant::now()>end { return Err("could not interrupt a pinned online view".into()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.stop_server()?;
        if fs::read_dir(repo.join("snapshots"))?.count()!=1 { return Err("interrupted online capture published a recovery point before interruption".into()); }
        self.start_server("online-capture-restart").await?;self.auth().await?;
        for root in &self.index_roots {
            if fs::read_dir(root.join(".n1-online-backups"))?.filter_map(Result::ok).any(|e|e.file_name()!="owner") { return Err("startup did not remove abandoned online view".into()); }
        }
        self.backup_cli(&repo,&key,"verify",&["--snapshot",snapshot],true)?;
        // Exercise the same endpoint over N1 native control, not only HTTPS.
        let shell=self.args.binary.parent().ok_or("binary parent")?.join("litiaina-n1-shell");
        let native_url=format!("n1://127.0.0.1:{}",self.args.native_port);
        let native=Command::new(&shell).args(["backup","create-online","--source-url",&native_url,"--repository","primary","--token-file"]).arg(&token_file).arg("--allow-plaintext").output()?;
        if !native.status.success() { return Err(format!("native online capture: {}",String::from_utf8_lossy(&native.stderr)).into()); }
        let second:Value=serde_json::from_slice(&native.stdout)?;
        if second["capture_mode"]!="online_committed" || second["snapshot"].as_str().is_none() { return Err("native capture returned no completed recovery point".into()); }
        // Real scheduled workflow; the unit deliberately does NOT exist. An
        // online job must not invoke a service stop/start/status subprocess.
        let job_file=evidence.join("online-job.json");
        fs::write(&job_file,serde_json::to_vec(&json!({"enabled":true,"service":{"unit":"n1-online-proof-does-not-exist.service","user":true},"engine_config":config,"repository":repo,"key_file":key,"state_dir":evidence.join("online-job-state"),"online_source":{"url":self.base_url,"repository":"primary","token_file":token_file,"ca_file":evidence.join("cert.pem")},"capture_timeout_seconds":300,"operation_timeout_seconds":300,"keep_last":0,"replica":{"repository":replica,"key_file":key,"keep_last":0}}))?)?;
        fs::set_permissions(&job_file,fs::Permissions::from_mode(0o600))?;
        let active_pid=self.server.as_ref().ok_or("missing restarted engine")?.id();
        let scheduled=Command::new(&shell).args(["backup","run-job","--config"]).arg(&job_file).output()?;
        if !scheduled.status.success() { return Err(format!("scheduled online capture: {}",String::from_utf8_lossy(&scheduled.stderr)).into()); }
        let status=Command::new(&shell).args(["backup","job-status","--config"]).arg(&job_file).output()?;
        let protection:Value=serde_json::from_slice(&status.stdout)?;
        if !status.status.success()||protection["healthy"]!=true||self.server.as_ref().ok_or("engine missing")?.id()!=active_pid { return Err("online scheduling did not preserve engine/protection state".into()); }
        self.stop_server()?;
        // Revoke access to ALL original data. The restore must remap members,
        // not silently obtain a missing piece from an old source root.
        for root in &self.index_roots { fs::rename(root,root.with_file_name("online-source-offline"))?; }
        self.index_roots=self.index_roots.iter().rev().map(|r|r.with_file_name("online-restored")).collect();self.blob_roots=self.index_roots.clone();
        for root in &self.index_roots { fs::create_dir(root)?; }
        self.args.out_dir=evidence.join("restored-control");fs::create_dir_all(self.args.out_dir.join("fs"))?;fs::create_dir(self.args.out_dir.join("refresh"))?;self.write_config()?;
        let restore_config=self.args.out_dir.join("n1_config.ini");fs::write(&restore_config,fs::read_to_string(&restore_config)?.replace("file_ttl_seconds=30","file_ttl_seconds=86400"))?;
        self.backup_cli(&replica,&key,"restore",&["--snapshot",snapshot,"--target-config",restore_config.to_str().ok_or("target config")?],true)?;
        for root in &self.index_roots {
            for entry in walkdir::WalkDir::new(root) { let entry=entry?;
                if entry.file_name()==".uploads" || unfinished_versions.iter().any(|v|entry.file_name()==v.as_str()) { return Err("online restore included unfinished durable-upload state".into()); }
            }
        }
        self.start_server("online-restored").await?;self.auth().await?;
        for (path,hash) in &hashes { self.assert_list_contains(path).await?;self.assert_download_sha256(path,hash).await?; }
        self.assert_download_sha256(large_key,&large_hash).await?;
        let large_versions=self.list_version_ids(large_key).await?;
        if large_versions.iter().any(|v|unfinished_versions.contains(v)) { return Err("unfinalized version became visible after restore".into()); }
        let lineage_key=lineage["new_key"].as_str().ok_or("lineage key")?;
        let versions=self.list_version_ids(lineage_key).await?;
        for field in ["first_version","second_version"] { if !versions.iter().any(|v|Some(v.as_str())==lineage[field].as_str()) { return Err("restored committed version history incomplete".into()); } }
        let restored_other=self.auth_fragment(&other,"other-key").await?;
        self.assert_download_sha256_with_token(&restored_other,"other.bin",&sha256_bytes(b"snapshot survives source fragment deletion")).await?;
        self.posix_post("/noa/v1/posix/recover",json!({"path":"online-trash"})).await?;
        self.assert_download_sha256("online-trash/file.bin",&sha256_bytes(b"trash committed before view")).await?;
        let after=self.posix_stat("online-after.bin",false).await;
        if after.is_ok() { return Err("post-selection new key appeared in older snapshot".into()); }
        self.stop_server()?;
        self.summary=json!({"status":"PASS","scenario":"online_committed_backup","ec":[self.data_shards,self.parity_shards],"first":first,"second_native":second,"pin_observed_ms":pin_observed_ms,"concurrent_mutation_ms":concurrent_mutation_ms,"original_engine_pid":original_pid,"objects_verified":hashes.len()+2,"repository":repo,"replica":replica,"key_file":key,"checks":["dedicated_operator_auth","engine_not_stopped","copy_while_mutating","committed_version_set","new_and_existing_durable_parts_excluded","post_pin_rename_upload_recover","fragment_delete_retains_pinned_data","native_online_capture","replication_verify","relocated_reversed_members_restore","all_hashes_and_listing","old_version_history","trash_recovery","post_selection_new_key_excluded"]});
        self.summary["first_snapshot"]=first;self.summary["second_snapshot"]=second;
        self.summary["checks"].as_array_mut().unwrap().extend([json!("engine_sigkill_during_capture"),json!("interrupted_view_cleanup"),json!("last_completed_point_preserved")]);
        self.summary["checks"].as_array_mut().unwrap().extend([json!("https_source_ca_validation"),json!("scheduled_online_no_service_transition"),json!("scheduled_verify_replication_healthy")]);
        fs::write(evidence.join("summary.json"),serde_json::to_vec_pretty(&self.summary)?)?;
        Ok(())
    }
}
