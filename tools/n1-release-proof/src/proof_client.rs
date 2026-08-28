impl Proof {
    async fn create_named_fragment(&self, name: &str, key: &str) -> ProofResult<String> {
        let response = self
            .client
            .post(format!("{}/noa/v1/fragments", self.base_url))
            .json(&json!({
                "name": name,
                "secret": self.args.master_key,
                "key": key,
            }))
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "multi-fragment create failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json::<FragmentCreate>().await?.fragment_hash)
    }

    async fn auth_fragment(&self, fragment: &str, key: &str) -> ProofResult<String> {
        let response = self
            .client
            .post(format!("{}/noa/v1/auth/fragment", self.base_url))
            .json(&json!({
                "fragment": fragment,
                "secret": key,
                "read": true,
                "write": true,
                "modify": true,
                "delete": true,
                "expires_in": 21600,
            }))
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "multi-fragment auth failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json::<AuthResponse>().await?.access_token)
    }

    async fn upload_durable_part_range(
        &self,
        version_id: &str,
        start_part: u64,
        end_part: u64,
        part_size: u64,
    ) -> ProofResult<()> {
        let total_size = self.args.source_file.metadata()?.len();
        for part_idx in start_part..end_part {
            let offset = part_idx * part_size;
            let remaining = total_size.saturating_sub(offset);
            let len = remaining.min(part_size);
            let mut file = tokio::fs::File::open(&self.args.source_file).await?;
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let reader = file.take(len);
            let stream = ReaderStream::with_capacity(reader, 1024 * 1024);
            let response = self
                .client
                .put(format!(
                    "{}/noa/v1/upload/multipart/durable/{}/{}?durability=batched",
                    self.base_url, version_id, part_idx
                ))
                .bearer_auth(&self.token)
                .header("content-type", "application/octet-stream")
                .header(CONTENT_LENGTH, len)
                .body(reqwest::Body::wrap_stream(stream))
                .send()
                .await?;
            if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
                return Err(format!(
                    "multipart part {part_idx} failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
        }
        Ok(())
    }

    async fn assert_list_contains(&self, object_key: &str) -> ProofResult<()> {
        self.assert_list_contains_with_token(&self.token, object_key)
            .await
    }

    async fn assert_list_contains_with_token(
        &self,
        token: &str,
        object_key: &str,
    ) -> ProofResult<()> {
        let (parent, name) = split_parent_name(object_key);
        let mut continuation_token: Option<String> = None;
        let mut pages = 0usize;
        let mut last_names = Vec::new();

        loop {
            pages += 1;
            if pages > 10_000 {
                return Err(format!(
                    "list pagination exceeded 10000 pages while looking for {object_key} under {parent}"
                )
                .into());
            }

            let mut url = format!(
                "{}/noa/v1/posix/list?path={}&limit=1000",
                self.base_url,
                encode_query_value(parent)
            );
            if let Some(token_value) = continuation_token.as_deref() {
                url.push_str("&continuation_token=");
                url.push_str(&encode_query_value(token_value));
            }

            let response = self.client.get(url).bearer_auth(token).send().await?;
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "list failed for {object_key}: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }

            let body: Value = response.json().await?;
            let entries = body.get("entries").and_then(Value::as_array);
            if let Some(entries) = entries {
                if entries
                    .iter()
                    .any(|entry| entry.get("name").and_then(Value::as_str) == Some(name))
                {
                    return Ok(());
                }

                last_names = entries
                    .iter()
                    .filter_map(|entry| entry.get("name").and_then(Value::as_str))
                    .take(8)
                    .map(str::to_owned)
                    .collect();
            }

            continuation_token = body
                .get("continuation_token")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if continuation_token.is_none() {
                return Err(format!(
                    "list did not contain {object_key} after {pages} page(s) under {parent}; last_names={last_names:?}"
                )
                .into());
            }
        }
    }

    async fn download_sha256(&self, object_key: &str) -> ProofResult<String> {
        self.download_sha256_with_token(&self.token, object_key)
            .await
    }

    async fn download_sha256_with_token(
        &self,
        token: &str,
        object_key: &str,
    ) -> ProofResult<String> {
        let response = self
            .client
            .get(format!(
                "{}/noa/v1/objects/download?object_key={}",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "download failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let mut hasher = Sha256::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            hasher.update(&chunk?);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    async fn expect_download_status(
        &self,
        object_key: &str,
        expected: StatusCode,
    ) -> ProofResult<()> {
        let response = self
            .client
            .get(format!(
                "{}/noa/v1/objects/download?object_key={}",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != expected {
            return Err(format!(
                "download status mismatch for {object_key}: expected={} actual={} body={}",
                expected,
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(())
    }

    async fn range_probe(&self, object_key: &str) -> ProofResult<()> {
        let response = self
            .client
            .get(format!(
                "{}/noa/v1/objects/download?object_key={}",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .header("range", "bytes=0-1048575")
            .send()
            .await?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(format!(
                "range probe failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let bytes = response.bytes().await?;
        if bytes.is_empty() || bytes.len() > 1_048_576 {
            return Err(
                format!("range probe returned unexpected byte count {}", bytes.len()).into(),
            );
        }
        Ok(())
    }

    async fn assert_download_sha256(&self, object_key: &str, expected: &str) -> ProofResult<()> {
        self.assert_download_sha256_with_token(&self.token, object_key, expected)
            .await
    }

    async fn assert_download_sha256_with_token(
        &self,
        token: &str,
        object_key: &str,
        expected: &str,
    ) -> ProofResult<()> {
        let actual = self.download_sha256_with_token(token, object_key).await?;
        if actual != expected {
            return Err(format!(
                "download hash mismatch for {object_key}: expected={expected} actual={actual}"
            )
            .into());
        }
        Ok(())
    }

    async fn upload_bytes(
        &self,
        object_key: &str,
        payload: Vec<u8>,
        content_type: &str,
    ) -> ProofResult<String> {
        self.upload_bytes_with_token(&self.token, object_key, payload, content_type)
            .await
    }

    async fn upload_bytes_with_token(
        &self,
        token: &str,
        object_key: &str,
        payload: Vec<u8>,
        content_type: &str,
    ) -> ProofResult<String> {
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/one-shot/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(token)
            .header("content-type", content_type)
            .body(payload)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "one-shot upload failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let body: Value = response.json().await?;
        if body.get("object_key").and_then(Value::as_str) != Some(object_key) {
            return Err(format!("one-shot logical path mismatch for {object_key}: {body}").into());
        }
        Ok(body
            .get("version_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string())
    }

    async fn posix_post(&self, route: &str, body: Value) -> ProofResult<Value> {
        self.posix_post_with_token(&self.token, route, body).await
    }

    async fn posix_post_with_token(
        &self,
        token: &str,
        route: &str,
        body: Value,
    ) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, route))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await?;
        if !matches!(
            response.status(),
            StatusCode::OK | StatusCode::CREATED | StatusCode::ACCEPTED
        ) {
            return Err(format!(
                "{route} failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn posix_stat(&self, object_key: &str, trash: bool) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/noa/v1/posix/stat?path={}&trash={}",
                self.base_url,
                encode_query_value(object_key),
                trash
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "posix stat failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn assert_live_path_absent(&self, object_key: &str) -> ProofResult<()> {
        let response = self
            .client
            .get(format!(
                "{}/noa/v1/posix/stat?path={}&trash=false",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::NOT_FOUND {
            return Err(format!(
                "expected live path to be absent for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(())
    }

    async fn assert_expected_visible(&self, expected: &BTreeSet<String>) -> ProofResult<()> {
        for object_key in expected {
            self.posix_stat(object_key, false).await?;
        }
        Ok(())
    }
}
