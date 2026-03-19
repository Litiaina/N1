use std::error::Error;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use ini::Ini;
use reqwest::{Client, Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub type DynError = Box<dyn Error>;
const ACCESS_REFRESH_SKEW_SECS: u64 = 30;

#[derive(Serialize)]
struct AuthRequest {
    fragment: String,
    secret: String,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
}

#[derive(Deserialize)]
struct AuthResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    refresh_expires_in: Option<u64>,
}

#[derive(Serialize)]
struct RefreshRequest<'a> {
    refresh_token: &'a str,
}

#[derive(Serialize)]
pub struct CreateFragmentRequest {
    pub name: String,
    pub key: String,
    pub secret: String,
}

pub struct AuthSession {
    access_token: String,
    refresh_token: Option<String>,
    access_expires_at: Option<Instant>,
    secret: String,
    auth_fragment: String,
}

impl AuthSession {
    fn from_auth_response(auth: AuthResponse, secret: String, auth_fragment: String) -> Self {
        let mut session = Self {
            access_token: String::new(),
            refresh_token: None,
            access_expires_at: None,
            secret,
            auth_fragment,
        };
        session.apply_auth_response(auth);
        session
    }

    fn apply_auth_response(&mut self, auth: AuthResponse) {
        self.access_token = auth.access_token;
        self.refresh_token = auth.refresh_token;
        self.access_expires_at = auth
            .expires_in
            .map(|ttl| Instant::now() + Duration::from_secs(ttl));
        let _ = auth.refresh_expires_in;
    }

    fn access_token(&self) -> &str {
        &self.access_token
    }

    fn access_token_expires_soon(&self) -> bool {
        self.access_expires_at
            .map(|deadline| {
                let skew = Duration::from_secs(ACCESS_REFRESH_SKEW_SECS);
                Instant::now().checked_add(skew).unwrap_or(deadline) >= deadline
            })
            .unwrap_or(false)
    }

    async fn ensure_access_token(
        &mut self,
        client: &Client,
        base_url: &str,
    ) -> Result<(), DynError> {
        if self.access_token_expires_soon() {
            self.refresh_or_reauthenticate(client, base_url).await?;
        }
        Ok(())
    }

    async fn refresh_or_reauthenticate(
        &mut self,
        client: &Client,
        base_url: &str,
    ) -> Result<(), DynError> {
        if self.try_refresh(client, base_url).await.is_ok() {
            return Ok(());
        }
        self.reauthenticate(client, base_url).await
    }

    async fn try_refresh(&mut self, client: &Client, base_url: &str) -> Result<(), DynError> {
        let refresh_token = self
            .refresh_token
            .clone()
            .ok_or_else(|| io::Error::other("refresh token unavailable"))?;

        let response = client
            .post(api_url(base_url, "/authenticate/refresh"))
            .json(&RefreshRequest {
                refresh_token: &refresh_token,
            })
            .send()
            .await?
            .error_for_status()?;

        let auth: AuthResponse = response.json().await?;
        self.apply_auth_response(auth);
        Ok(())
    }

    async fn reauthenticate(&mut self, client: &Client, base_url: &str) -> Result<(), DynError> {
        let auth = request_auth(client, base_url, &self.secret, &self.auth_fragment).await?;
        self.apply_auth_response(auth);
        Ok(())
    }
}

pub fn build_client(insecure: bool) -> Result<Client, reqwest::Error> {
    Client::builder()
        .danger_accept_invalid_certs(insecure)
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(2)
        .tcp_nodelay(true)
        .build()
}

pub fn api_url(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

pub fn get_base_url(url_opt: Option<String>) -> String {
    if let Some(url) = url_opt {
        return url.trim_end_matches('/').to_string();
    }

    let config =
        Ini::load_from_file("n1_config.ini").or_else(|_| Ini::load_from_file("config.ini"));
    if let Ok(config) = config
        && let Some(section) = config.section(Some("server"))
    {
        let host = section.get("host").unwrap_or("127.0.0.1");
        let port = section
            .get("https_port")
            .or_else(|| section.get("port"))
            .unwrap_or("50001");
        let effective_host = if host == "0.0.0.0" { "127.0.0.1" } else { host };
        return format!("https://{}:{}", effective_host, port);
    }

    "https://127.0.0.1:50001".to_string()
}

pub fn get_secret(secret_opt: Option<String>) -> Result<String, DynError> {
    if let Some(secret) = secret_opt {
        return Ok(secret);
    }
    if let Ok(secret) = std::env::var("N1_MASTER_KEY") {
        return Ok(secret);
    }

    print!("Enter master secret: ");
    io::stdout().flush()?;
    Ok(rpassword::read_password()?)
}

pub fn resolve_auth_fragment(auth_fragment_opt: Option<String>) -> String {
    auth_fragment_opt
        .or_else(|| std::env::var("N1_AUTH_FRAGMENT").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "admin".to_string())
}

async fn request_auth(
    client: &Client,
    base_url: &str,
    secret: &str,
    auth_fragment: &str,
) -> Result<AuthResponse, DynError> {
    let response = client
        .post(api_url(base_url, "/authenticate/fragment"))
        .json(&AuthRequest {
            fragment: auth_fragment.to_string(),
            secret: secret.to_string(),
            read: true,
            write: true,
            modify: true,
            delete: true,
        })
        .send()
        .await?
        .error_for_status()?;

    Ok(response.json().await?)
}

pub async fn authenticate(
    client: &Client,
    base_url: &str,
    secret: &str,
    auth_fragment: &str,
) -> Result<AuthSession, DynError> {
    let auth = request_auth(client, base_url, secret, auth_fragment).await?;
    Ok(AuthSession::from_auth_response(
        auth,
        secret.to_string(),
        auth_fragment.to_string(),
    ))
}

pub async fn send_with_session<F>(
    client: &Client,
    base_url: &str,
    session: &mut AuthSession,
    build: F,
) -> Result<reqwest::Response, DynError>
where
    F: Fn(&str) -> reqwest::RequestBuilder,
{
    session.ensure_access_token(client, base_url).await?;

    let response = build(session.access_token()).send().await?;
    if response.status() == StatusCode::UNAUTHORIZED {
        session.refresh_or_reauthenticate(client, base_url).await?;
        return Ok(build(session.access_token())
            .send()
            .await?
            .error_for_status()?);
    }

    Ok(response.error_for_status()?)
}

pub async fn fetch_json<T: DeserializeOwned>(
    client: &Client,
    base_url: &str,
    session: &mut AuthSession,
    url: &str,
) -> Result<T, DynError> {
    Ok(send_with_session(client, base_url, session, |token| {
        client.get(url).bearer_auth(token)
    })
    .await?
    .json::<T>()
    .await?)
}

pub async fn fetch_json_query<T: DeserializeOwned>(
    client: &Client,
    base_url: &str,
    session: &mut AuthSession,
    url: &str,
    query: &[(impl AsRef<str>, impl AsRef<str>)],
) -> Result<T, DynError> {
    let query_pairs: Vec<(String, String)> = query
        .iter()
        .map(|(key, value)| (key.as_ref().to_string(), value.as_ref().to_string()))
        .collect();
    Ok(send_with_session(client, base_url, session, |token| {
        client.get(url).query(&query_pairs).bearer_auth(token)
    })
    .await?
    .json::<T>()
    .await?)
}

pub async fn send_without_body(
    client: &Client,
    base_url: &str,
    session: &mut AuthSession,
    method: Method,
    url: &str,
) -> Result<(), DynError> {
    let response = send_with_session(client, base_url, session, |token| {
        client.request(method.clone(), url).bearer_auth(token)
    })
    .await?;
    print_response_message(response).await
}

pub async fn send_query_only(
    client: &Client,
    base_url: &str,
    session: &mut AuthSession,
    method: Method,
    url: &str,
    query: &[(String, String)],
) -> Result<(), DynError> {
    let response = send_with_session(client, base_url, session, |token| {
        client
            .request(method.clone(), url)
            .query(query)
            .bearer_auth(token)
    })
    .await?;
    print_response_message(response).await
}

pub async fn print_response_message(response: reqwest::Response) -> Result<(), DynError> {
    let response = response.error_for_status()?;
    let status = response.status();
    let body = response.text().await?;
    if body.trim().is_empty() {
        println!("ok [{}]", status);
    } else {
        println!("{}", body.trim());
    }
    Ok(())
}

pub fn print_output<T: Serialize>(
    json_out: bool,
    value: &T,
    formatted: &str,
) -> Result<(), DynError> {
    if json_out {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{formatted}");
    }
    Ok(())
}

pub async fn watch_rendered<T, FRender, FStop>(
    client: &Client,
    base_url: &str,
    session: &mut AuthSession,
    url: &str,
    poll_ms: u64,
    json_out: bool,
    render: FRender,
    stop_when: FStop,
) -> Result<(), DynError>
where
    T: DeserializeOwned + Serialize,
    FRender: Fn(&T) -> String,
    FStop: Fn(&T) -> bool,
{
    let interval = Duration::from_millis(poll_ms.max(250));
    let mut last_output: Option<String> = None;

    loop {
        let status: T = fetch_json(client, base_url, session, url).await?;
        let output = if json_out {
            serde_json::to_string_pretty(&status)?
        } else {
            render(&status)
        };

        if last_output.as_deref() != Some(output.as_str()) {
            println!("{output}");
            if !json_out {
                println!();
            }
            last_output = Some(output);
        }

        if stop_when(&status) {
            break;
        }

        tokio::time::sleep(interval).await;
    }

    Ok(())
}
