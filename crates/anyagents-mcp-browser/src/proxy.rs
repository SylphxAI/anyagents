//! Optional browser-only per-profile / per-session egress proxy support.
//!
//! Strictly browser-scoped: never reads or mutates global `HTTP_PROXY`,
//! `HTTPS_PROXY`, or `ALL_PROXY`. Unauthenticated proxies are passed directly
//! to Chromium alongside `--proxy-bypass-list` so loopback CDP is never
//! proxied. Authenticated proxies (`user:pass@`) are fronted by an in-process
//! loopback HTTP CONNECT forwarder on `127.0.0.1:0` so Chromium receives only
//! `--proxy-server=http://127.0.0.1:<port>` with zero credentials in argv,
//! logs, screenshots, or target-site headers.

use std::fmt;
use std::io::{self, Read as _, Write as _};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;

use crate::secrets::{parse_secret_ref, resolve_secret_raw};
use crate::session::BrowserError;

/// Browser-only fallback proxy environment variable.
pub const BROWSER_PROXY_ENV: &str = "ANYAGENTS_BROWSER_PROXY";
/// Prefix for per-profile browser proxy environment variables.
pub const PROFILE_PROXY_ENV_PREFIX: &str = "ANYAGENTS_BROWSER_PROXY_";
/// Bypass list keeping loopback / CDP direct and untouched.
pub const PROXY_BYPASS_LIST: &str = "localhost;127.0.0.1;[::1]";

const MAX_HTTP_HEAD: usize = 64 * 1024;
const SOCKS_MAX_CRED: usize = 255;

/// Supported browser proxy schemes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyScheme {
    Http,
    Https,
    Socks5,
    Socks5h,
}

impl ProxyScheme {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Socks5 => "socks5",
            Self::Socks5h => "socks5h",
        }
    }

    const fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
            Self::Socks5 | Self::Socks5h => 1080,
        }
    }
}

/// Optional per-session / per-tool-call proxy override.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProxyOverride {
    /// Env var name (e.g. from Named Encrypted Secrets) holding a proxy URL or `op://` ref.
    pub proxy_env: Option<String>,
    /// Direct proxy URL (`http://`, `https://`, `socks5://`, `socks5h://`) or `op://` secret_ref.
    pub proxy: Option<String>,
}

impl ProxyOverride {
    pub fn from_opts(proxy: Option<String>, proxy_env: Option<String>) -> Option<Self> {
        let proxy = non_empty(proxy.as_deref()).map(str::to_string);
        let proxy_env = non_empty(proxy_env.as_deref()).map(str::to_string);
        (proxy.is_some() || proxy_env.is_some()).then_some(Self { proxy_env, proxy })
    }
}

/// Parsed browser-only proxy configuration. `Debug` never prints credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct BrowserProxy {
    scheme: ProxyScheme,
    host: String,
    port: u16,
    auth: Option<(String, String)>,
}

impl fmt::Debug for BrowserProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BrowserProxy({})", self.redacted_display())
    }
}

impl BrowserProxy {
    /// Parse an optional `http://`, `https://`, `socks5://`, or `socks5h://`
    /// proxy URL with optional `user:pass@`. Errors never echo credentials.
    pub fn parse(raw: &str) -> Result<Self, BrowserError> {
        let s = raw.trim();
        if s.is_empty() || raw.bytes().any(|b| matches!(b, 0 | b'\r' | b'\n')) {
            return Err(BrowserError::invalid_input("invalid browser proxy URL"));
        }
        let u = Url::parse(s)
            .map_err(|_| BrowserError::invalid_input("invalid browser proxy URL syntax"))?;
        let scheme = match u.scheme() {
            "http" => ProxyScheme::Http,
            "https" => ProxyScheme::Https,
            "socks5" => ProxyScheme::Socks5,
            "socks5h" => ProxyScheme::Socks5h,
            other => {
                return Err(BrowserError::invalid_input(format!(
                    "unsupported browser proxy scheme `{other}`; expected http, https, socks5, or socks5h"
                )));
            }
        };
        let host = u
            .host_str()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| BrowserError::invalid_input("browser proxy URL is missing host"))?
            .to_string();
        let port = u.port().unwrap_or_else(|| scheme.default_port());
        if port == 0 {
            return Err(BrowserError::invalid_input(
                "browser proxy URL port must be non-zero",
            ));
        }
        let auth = if !u.username().is_empty() || u.password().is_some() {
            let user = percent_decode(u.username());
            let pass = percent_decode(u.password().unwrap_or(""));
            if matches!(scheme, ProxyScheme::Socks5 | ProxyScheme::Socks5h)
                && (user.len() > SOCKS_MAX_CRED || pass.len() > SOCKS_MAX_CRED)
            {
                return Err(BrowserError::invalid_input(
                    "SOCKS5 proxy credentials exceed 255 bytes",
                ));
            }
            Some((user, pass))
        } else {
            None
        };
        Ok(Self {
            scheme,
            host,
            port,
            auth,
        })
    }

    pub fn scheme(&self) -> ProxyScheme {
        self.scheme
    }

    pub fn is_authenticated(&self) -> bool {
        self.auth.is_some()
    }

    /// Host and port formatted for TCP dials and URLs (brackets raw IPv6 literals).
    pub fn host_port(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// Credential-free display form safe for logs, errors, and `Debug`.
    pub fn redacted_display(&self) -> String {
        let prefix = if self.auth.is_some() {
            "<redacted>@"
        } else {
            ""
        };
        format!("{}://{}{}", self.scheme.as_str(), prefix, self.host_port())
    }

    /// Direct Chromium flags for an unauthenticated proxy (never emits credentials).
    pub fn direct_chromium_args(&self) -> [String; 2] {
        [
            format!(
                "--proxy-server={}://{}",
                self.scheme.as_str(),
                self.host_port()
            ),
            format!("--proxy-bypass-list={PROXY_BYPASS_LIST}"),
        ]
    }
}

/// Normalize a browser profile name to `ANYAGENTS_BROWSER_PROXY_<PROFILE_UPPER_SNAKE>`.
pub fn profile_proxy_env_key(profile: &str) -> String {
    let suffix: String = profile
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("{PROFILE_PROXY_ENV_PREFIX}{suffix}")
}

/// Redact `user:pass@` from any proxy URL string for safe logging and `Debug`.
pub fn redact_proxy_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(parsed) = BrowserProxy::parse(trimmed) {
        return parsed.redacted_display();
    }
    if let Ok(mut u) = Url::parse(trimmed) {
        if !u.username().is_empty() || u.password().is_some() {
            let _ = u.set_username("redacted");
            let _ = u.set_password(None);
            return u.to_string().replace("redacted@", "<redacted>@");
        }
        return u.to_string();
    }
    if trimmed.contains('@') {
        "<redacted>".into()
    } else {
        trimmed.into()
    }
}

/// Resolve the effective browser-only proxy configuration using strict precedence:
/// 1. `override_req.proxy_env` (env var name from Named Encrypted Secrets),
/// 2. `override_req.proxy` (URL or `op://` / `env:` secret_ref),
/// 3. `ANYAGENTS_BROWSER_PROXY_<PROFILE_UPPER_SNAKE>`,
/// 4. `config_proxy` (`--proxy` / `ANYAGENTS_BROWSER_PROXY`) or `ANYAGENTS_BROWSER_PROXY`.
///
/// Never reads global `HTTP_PROXY`, `HTTPS_PROXY`, or `ALL_PROXY`.
pub fn resolve_browser_proxy(
    config_proxy: Option<&str>,
    profile: Option<&str>,
    override_req: Option<&ProxyOverride>,
) -> Result<Option<BrowserProxy>, BrowserError> {
    resolve_browser_proxy_with(
        config_proxy,
        profile,
        override_req,
        |k| std::env::var(k).ok(),
        resolve_secret_raw,
    )
}

/// Pure, testable resolver decoupled from process environment and `op` CLI.
pub fn resolve_browser_proxy_with<E, S>(
    config_proxy: Option<&str>,
    profile: Option<&str>,
    override_req: Option<&ProxyOverride>,
    env: E,
    resolve_ref: S,
) -> Result<Option<BrowserProxy>, BrowserError>
where
    E: Fn(&str) -> Option<String>,
    S: Fn(&str) -> Result<String, BrowserError>,
{
    if let Some(ov) = override_req {
        if let Some(var_name) = non_empty(ov.proxy_env.as_deref()) {
            // Reuse secret_ref validation to verify identifier syntax and block reserved creds.
            parse_secret_ref(&format!("env:{var_name}"))?;
            let raw = env(var_name)
                .as_deref()
                .and_then(|v| non_empty(Some(v)))
                .map(str::to_string)
                .ok_or_else(|| {
                    BrowserError::new(
                        "PROXY_CONFIG_ERROR",
                        format!("proxy_env `{var_name}` is unset or empty"),
                    )
                })?;
            return resolve_value_or_ref(&raw, &resolve_ref).map(Some);
        }
        if let Some(raw) = non_empty(ov.proxy.as_deref()) {
            return resolve_value_or_ref(raw, &resolve_ref).map(Some);
        }
    }
    if let Some(prof) = non_empty(profile) {
        let key = profile_proxy_env_key(prof);
        if let Some(raw) = env(&key).as_deref().and_then(|v| non_empty(Some(v))) {
            return resolve_value_or_ref(raw, &resolve_ref).map(Some);
        }
    }
    let fallback = non_empty(config_proxy)
        .map(str::to_string)
        .or_else(|| env(BROWSER_PROXY_ENV).and_then(|v| non_empty(Some(&v)).map(str::to_string)));
    match fallback {
        Some(raw) => resolve_value_or_ref(&raw, &resolve_ref).map(Some),
        None => Ok(None),
    }
}

fn resolve_value_or_ref<S>(raw: &str, resolve_ref: &S) -> Result<BrowserProxy, BrowserError>
where
    S: Fn(&str) -> Result<String, BrowserError>,
{
    let trimmed = raw.trim();
    if trimmed.starts_with("op://") || trimmed.starts_with("env:") {
        let resolved = resolve_ref(trimmed)?;
        BrowserProxy::parse(&resolved)
    } else {
        BrowserProxy::parse(trimmed)
    }
}

/// Active browser proxy wiring. Dropping this aborts any loopback forwarder task.
pub struct ProxyHandle {
    chromium_proxy_url: String,
    redacted_upstream: String,
    relay: Option<JoinHandle<()>>,
}

impl fmt::Debug for ProxyHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyHandle")
            .field("chromium_proxy_url", &self.chromium_proxy_url)
            .field("redacted_upstream", &self.redacted_upstream)
            .field("loopback_forwarder", &self.relay.is_some())
            .finish()
    }
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        if let Some(task) = self.relay.take() {
            task.abort();
        }
    }
}

impl ProxyHandle {
    /// Start credential-safe Chromium proxy wiring for `proxy`.
    ///
    /// - Unauthenticated proxy: no relay spawned; Chromium connects directly.
    /// - Authenticated proxy: spawns a loopback HTTP CONNECT forwarder on
    ///   `127.0.0.1:0` and exposes only `http://127.0.0.1:<port>` to Chromium.
    pub async fn start(proxy: &BrowserProxy) -> Result<Self, BrowserError> {
        let redacted_upstream = proxy.redacted_display();
        let Some((user, pass)) = proxy.auth.clone() else {
            return Ok(Self {
                chromium_proxy_url: format!("{}://{}", proxy.scheme.as_str(), proxy.host_port()),
                redacted_upstream,
                relay: None,
            });
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
            BrowserError::new(
                "PROXY_ERROR",
                format!("failed binding loopback proxy forwarder: {e}"),
            )
        })?;
        let local: SocketAddr = listener.local_addr().map_err(|e| {
            BrowserError::new(
                "PROXY_ERROR",
                format!("failed inspecting loopback proxy address: {e}"),
            )
        })?;

        let scheme = proxy.scheme;
        let host = proxy.host.clone();
        let port = proxy.port;
        let upstream = proxy.host_port();
        let auth_hdr = format!(
            "Proxy-Authorization: Basic {}\r\n",
            base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
        );

        let relay = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let (host, upstream, auth_hdr, user, pass) = (
                    host.clone(),
                    upstream.clone(),
                    auth_hdr.clone(),
                    user.clone(),
                    pass.clone(),
                );
                tokio::spawn(async move {
                    let _ = match scheme {
                        ProxyScheme::Http => relay_http(client, &upstream, &auth_hdr).await,
                        ProxyScheme::Https => relay_https(client, &host, port, &auth_hdr).await,
                        ProxyScheme::Socks5 | ProxyScheme::Socks5h => {
                            relay_socks5(client, &upstream, &user, &pass).await
                        }
                    };
                });
            }
        });

        Ok(Self {
            chromium_proxy_url: format!("http://{local}"),
            redacted_upstream,
            relay: Some(relay),
        })
    }

    /// Credential-free `--proxy-server` and `--proxy-bypass-list` Chromium flags.
    pub fn chromium_args(&self) -> [String; 2] {
        [
            format!("--proxy-server={}", self.chromium_proxy_url),
            format!("--proxy-bypass-list={PROXY_BYPASS_LIST}"),
        ]
    }

    /// Credential-free proxy URL handed to Chromium (`--proxy-server=<url>`).
    pub fn proxy_server_url(&self) -> &str {
        &self.chromium_proxy_url
    }

    /// Redacted upstream proxy address safe for logs.
    pub fn redacted_upstream(&self) -> &str {
        &self.redacted_upstream
    }
}

fn non_empty(opt: Option<&str>) -> Option<&str> {
    opt.map(str::trim).filter(|s| !s.is_empty())
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---------------------------------------------------------------------------
// Loopback HTTP CONNECT forwarder (pure tokio::net — zero io-util dependency)
// ---------------------------------------------------------------------------

async fn tcp_read_some(s: &TcpStream, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        s.readable().await?;
        match s.try_read(buf) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            res => return res,
        }
    }
}

async fn tcp_read_exact(s: &TcpStream, mut buf: &mut [u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let n = tcp_read_some(s, buf).await?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
        }
        buf = &mut buf[n..];
    }
    Ok(())
}

async fn tcp_write_all(s: &TcpStream, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        s.writable().await?;
        match s.try_write(buf) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "write zero")),
            Ok(n) => buf = &buf[n..],
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

async fn copy_half(r: OwnedReadHalf, w: OwnedWriteHalf) -> io::Result<()> {
    let mut buf = [0u8; 16 * 1024];
    loop {
        r.readable().await?;
        let n = match r.try_read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        };
        let mut off = 0;
        while off < n {
            w.writable().await?;
            match w.try_write(&buf[off..n]) {
                Ok(0) => return Ok(()),
                Ok(k) => off += k,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

async fn splice_tcp(a: TcpStream, b: TcpStream) -> io::Result<()> {
    let (ar, aw) = a.into_split();
    let (br, bw) = b.into_split();
    let t1 = tokio::spawn(copy_half(ar, bw));
    let t2 = tokio::spawn(copy_half(br, aw));
    let _ = tokio::join!(t1, t2);
    Ok(())
}

async fn read_http_head(client: &TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if tcp_read_some(client, &mut byte).await? == 0 {
            return if head.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated"))
            };
        }
        head.push(byte[0]);
        if head.len() > MAX_HTTP_HEAD {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "head too large"));
        }
    }
    Ok(Some(head))
}

fn strip_header(rest: &[u8], name: &[u8]) -> Vec<u8> {
    rest.split_inclusive(|b| *b == b'\n')
        .filter(|line| {
            !(line.len() > name.len()
                && line[..name.len()].eq_ignore_ascii_case(name)
                && line[name.len()] == b':')
        })
        .flatten()
        .copied()
        .collect()
}

fn rewrite_http_proxy_head(head: &[u8], auth_hdr: &str) -> io::Result<Vec<u8>> {
    let line_end = head
        .windows(2)
        .position(|w| w == b"\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad head"))?
        + 2;
    let mut out = head[..line_end].to_vec();
    out.extend_from_slice(auth_hdr.as_bytes());
    let rest = strip_header(&head[line_end..], b"proxy-authorization");
    if head.starts_with(b"CONNECT ") {
        out.extend_from_slice(&rest);
    } else {
        let rest = strip_header(&rest, b"connection");
        let rest = strip_header(&rest, b"proxy-connection");
        out.extend_from_slice(b"Connection: close\r\n");
        out.extend_from_slice(&rest);
    }
    Ok(out)
}

async fn relay_http(client: TcpStream, upstream: &str, auth_hdr: &str) -> io::Result<()> {
    let Some(head) = read_http_head(&client).await? else {
        return Ok(());
    };
    let out = rewrite_http_proxy_head(&head, auth_hdr)?;
    let up = TcpStream::connect(upstream).await?;
    tcp_write_all(&up, &out).await?;
    splice_tcp(client, up).await
}

async fn relay_https(client: TcpStream, host: &str, port: u16, auth_hdr: &str) -> io::Result<()> {
    let Some(head) = read_http_head(&client).await? else {
        return Ok(());
    };
    let out = rewrite_http_proxy_head(&head, auth_hdr)?;
    let client_std = client.into_std()?;
    let host = host.to_string();
    tokio::task::spawn_blocking(move || relay_https_blocking(client_std, &host, port, &out))
        .await
        .map_err(|e| io::Error::other(format!("https proxy task: {e}")))?
}

fn is_poll_timeout(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    ) || err
        .get_ref()
        .and_then(|r| r.downcast_ref::<ureq::Error>())
        .is_some_and(|u| matches!(u, ureq::Error::Timeout(_)))
}

fn relay_https_blocking(
    mut client: std::net::TcpStream,
    host: &str,
    port: u16,
    out_head: &[u8],
) -> io::Result<()> {
    use ureq::unversioned::resolver::{DefaultResolver, Resolver as _};
    use ureq::unversioned::transport::time::{Duration as UreqDuration, Instant as UreqInstant};
    use ureq::unversioned::transport::{
        ConnectionDetails, Connector as _, NextTimeout, RustlsConnector, TcpConnector,
        TransportAdapter,
    };

    let authority = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let uri: ureq::http::Uri = format!("https://{authority}/")
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("uri: {e}")))?;
    // Explicitly disable env proxy lookup so global HTTP_PROXY/HTTPS_PROXY are never consulted.
    let cfg = ureq::config::Config::builder().proxy(None).build();
    let resolver = DefaultResolver::default();
    let connect_timeout = NextTimeout {
        after: UreqDuration::from_secs(15),
        reason: ureq::Timeout::Connect,
    };
    let addrs = resolver
        .resolve(&uri, &cfg, connect_timeout)
        .map_err(ureq::Error::into_io)?;
    let details = ConnectionDetails {
        uri: &uri,
        addrs,
        config: &cfg,
        request_level: false,
        resolver: &resolver,
        now: UreqInstant::now(),
        timeout: connect_timeout,
        current_time: Arc::new(UreqInstant::now),
        run_connector: Arc::new(|_| Err(ureq::Error::ConnectionFailed)),
    };
    let connector = ureq::unversioned::transport::Connector::<()>::chain(
        TcpConnector::default(),
        RustlsConnector::default(),
    );
    let transport = connector
        .connect(&details, None::<()>)
        .map_err(ureq::Error::into_io)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "tls proxy connect failed"))?;
    let mut up = TransportAdapter::new(transport);
    up.set_timeout(connect_timeout);
    up.write_all(out_head)?;

    let poll = Duration::from_millis(20);
    client.set_nonblocking(false)?;
    client.set_read_timeout(Some(poll))?;
    up.set_timeout(NextTimeout {
        after: UreqDuration::from_millis(20),
        reason: ureq::Timeout::RecvBody,
    });

    let mut buf = [0u8; 16 * 1024];
    loop {
        match client.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => up.write_all(&buf[..n])?,
            Err(e) if is_poll_timeout(&e) => {}
            Err(e) => return Err(e),
        }
        match up.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => client.write_all(&buf[..n])?,
            Err(e) if is_poll_timeout(&e) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn parse_http_target(head: &[u8]) -> io::Result<(String, u16, Option<Vec<u8>>)> {
    let line_end = head
        .windows(2)
        .position(|w| w == b"\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad head"))?;
    let req_line = std::str::from_utf8(&head[..line_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 request line"))?;
    let mut parts = req_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing method"))?;
    let target = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing target"))?;
    let version = parts.next().unwrap_or("HTTP/1.1");

    if method.eq_ignore_ascii_case("CONNECT") {
        let (h, p) = split_host_port(target, 443)?;
        return Ok((h, p, None));
    }

    let u = Url::parse(target)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "expected absolute proxy URI"))?;
    let host = u
        .host_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing target host"))?
        .to_string();
    let port = u.port_or_known_default().unwrap_or(80);
    let mut origin_path = u.path().to_string();
    if let Some(q) = u.query() {
        origin_path.push('?');
        origin_path.push_str(q);
    }
    let rest = strip_header(&head[line_end + 2..], b"proxy-authorization");
    let rest = strip_header(&rest, b"proxy-connection");
    let rest = strip_header(&rest, b"connection");
    let mut rewritten =
        format!("{method} {origin_path} {version}\r\nConnection: close\r\n").into_bytes();
    rewritten.extend_from_slice(&rest);
    Ok((host, port, Some(rewritten)))
}

fn split_host_port(target: &str, default_port: u16) -> io::Result<(String, u16)> {
    if let Some(rest) = target.strip_prefix('[') {
        let (h, tail) = rest
            .split_once(']')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad ipv6 target"))?;
        let port = match tail.strip_prefix(':') {
            Some(p) => p
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad port"))?,
            None => default_port,
        };
        return Ok((h.to_string(), port));
    }
    match target.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() => Ok((
            h.to_string(),
            p.parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad port"))?,
        )),
        _ if !target.is_empty() => Ok((target.to_string(), default_port)),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "empty target")),
    }
}

async fn relay_socks5(client: TcpStream, upstream: &str, user: &str, pass: &str) -> io::Result<()> {
    let Some(head) = read_http_head(&client).await? else {
        return Ok(());
    };
    let (dst_host, dst_port, plain_req) = parse_http_target(&head)?;
    let up = TcpStream::connect(upstream).await?;

    // RFC 1928 greeting: VER=5, NMETHODS=1, METHOD=0x02 (username/password).
    tcp_write_all(&up, &[5, 1, 2]).await?;
    let mut sel = [0u8; 2];
    tcp_read_exact(&up, &mut sel).await?;
    if sel != [5, 2] {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "upstream SOCKS5 proxy refused username/password auth",
        ));
    }

    // RFC 1929 username/password sub-negotiation.
    let mut auth = Vec::with_capacity(3 + user.len() + pass.len());
    auth.push(1);
    auth.push(user.len() as u8);
    auth.extend_from_slice(user.as_bytes());
    auth.push(pass.len() as u8);
    auth.extend_from_slice(pass.as_bytes());
    tcp_write_all(&up, &auth).await?;
    let mut auth_resp = [0u8; 2];
    tcp_read_exact(&up, &mut auth_resp).await?;
    if auth_resp[1] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "upstream SOCKS5 proxy rejected credentials",
        ));
    }

    // RFC 1928 CONNECT request (IPv4 / IPv6 / domain name).
    let mut req = vec![5, 1, 0];
    match dst_host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => {
            req.push(1);
            req.extend_from_slice(&ip.octets());
        }
        Ok(IpAddr::V6(ip)) => {
            req.push(4);
            req.extend_from_slice(&ip.octets());
        }
        Err(_) => {
            if dst_host.len() > SOCKS_MAX_CRED {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SOCKS5 target hostname exceeds 255 bytes",
                ));
            }
            req.push(3);
            req.push(dst_host.len() as u8);
            req.extend_from_slice(dst_host.as_bytes());
        }
    }
    req.extend_from_slice(&dst_port.to_be_bytes());
    tcp_write_all(&up, &req).await?;

    let mut rep = [0u8; 4];
    tcp_read_exact(&up, &mut rep).await?;
    if rep[0] != 5 || rep[1] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!("upstream SOCKS5 CONNECT failed (code {})", rep[1]),
        ));
    }
    match rep[3] {
        1 => {
            let mut skip = [0u8; 6];
            tcp_read_exact(&up, &mut skip).await?;
        }
        3 => {
            let mut len = [0u8; 1];
            tcp_read_exact(&up, &mut len).await?;
            let mut skip = vec![0u8; len[0] as usize + 2];
            tcp_read_exact(&up, &mut skip).await?;
        }
        4 => {
            let mut skip = [0u8; 18];
            tcp_read_exact(&up, &mut skip).await?;
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid SOCKS5 ATYP",
            ));
        }
    }

    if let Some(rewritten) = plain_req {
        tcp_write_all(&up, &rewritten).await?;
    } else {
        tcp_write_all(&client, b"HTTP/1.1 200 Connection Established\r\n\r\n").await?;
    }
    splice_tcp(client, up).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution_precedence_profile_env_normalization_and_global_proxy_isolation() {
        assert_eq!(
            profile_proxy_env_key("work-eu.1"),
            "ANYAGENTS_BROWSER_PROXY_WORK_EU_1"
        );

        let env = |k: &str| match k {
            "HTTP_PROXY" | "HTTPS_PROXY" | "ALL_PROXY" => {
                Some("http://global-must-never-be-used:9999".into())
            }
            "ANYAGENTS_BROWSER_PROXY" => Some("http://fallback.example:8080".into()),
            "ANYAGENTS_BROWSER_PROXY_WORK_EU" => Some("socks5h://u:pw@profile.example:1080".into()),
            "MY_NAMED_PROXY" => Some("https://secret-user:secret-pass@named.example:8443".into()),
            _ => None,
        };
        let resolve_op = |r: &str| -> Result<String, BrowserError> {
            assert_eq!(r, "op://vault/proxy/url");
            Ok("http://op-user:op-pass@op.example:3128".into())
        };

        // 1. proxy_env > proxy > ANYAGENTS_BROWSER_PROXY_<PROFILE> > ANYAGENTS_BROWSER_PROXY
        let ov_env = ProxyOverride {
            proxy_env: Some("MY_NAMED_PROXY".into()),
            proxy: Some("http://lower-priority.example:8080".into()),
        };
        let r1 = resolve_browser_proxy_with(
            Some("http://cfg.example:8080"),
            Some("work-eu"),
            Some(&ov_env),
            env,
            resolve_op,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            r1.redacted_display(),
            "https://<redacted>@named.example:8443"
        );

        // 2. per-call proxy (including op:// secret_ref) > per-profile env
        let ov_op = ProxyOverride {
            proxy_env: None,
            proxy: Some("op://vault/proxy/url".into()),
        };
        let r2 = resolve_browser_proxy_with(
            Some("http://cfg.example:8080"),
            Some("work-eu"),
            Some(&ov_op),
            env,
            resolve_op,
        )
        .unwrap()
        .unwrap();
        assert_eq!(r2.redacted_display(), "http://<redacted>@op.example:3128");

        // 3. per-profile env > browser-only fallback
        let r3 = resolve_browser_proxy_with(
            Some("http://cfg.example:8080"),
            Some("work-eu"),
            None,
            env,
            resolve_op,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            r3.redacted_display(),
            "socks5h://<redacted>@profile.example:1080"
        );

        // 4. browser-only fallback when profile has no dedicated env
        let r4 = resolve_browser_proxy_with(None, Some("other"), None, env, resolve_op)
            .unwrap()
            .unwrap();
        assert_eq!(r4.redacted_display(), "http://fallback.example:8080");

        // 5. Global HTTP_PROXY/HTTPS_PROXY/ALL_PROXY alone yields direct (None)
        let global_only = |k: &str| match k {
            "HTTP_PROXY" | "HTTPS_PROXY" | "ALL_PROXY" => Some("http://global.example:3128".into()),
            _ => None,
        };
        assert_eq!(
            resolve_browser_proxy_with(None, Some("work-eu"), None, global_only, resolve_op)
                .unwrap(),
            None
        );

        // 6. Unset proxy_env fails closed; reserved env var in proxy_env is refused.
        let unset = ProxyOverride {
            proxy_env: Some("MISSING_PROXY_VAR".into()),
            proxy: None,
        };
        assert_eq!(
            resolve_browser_proxy_with(None, None, Some(&unset), env, resolve_op)
                .unwrap_err()
                .code,
            "PROXY_CONFIG_ERROR"
        );
        let reserved = ProxyOverride {
            proxy_env: Some("ANYAGENTS_AGENT_TOKEN".into()),
            proxy: None,
        };
        assert_eq!(
            resolve_browser_proxy_with(None, None, Some(&reserved), env, resolve_op)
                .unwrap_err()
                .code,
            "REFUSED"
        );
    }

    #[test]
    fn parse_and_redaction_cover_all_schemes_and_never_leak_credentials() {
        for raw in [
            "http://alice:p%40ssword@proxy.example:3128",
            "https://alice:p%40ssword@proxy.example:8443",
            "socks5://alice:p%40ssword@proxy.example:1080",
            "socks5h://alice:p%40ssword@proxy.example:1080",
        ] {
            let p = BrowserProxy::parse(raw).unwrap();
            assert!(p.is_authenticated());
            let dbg = format!("{p:?}");
            let disp = p.redacted_display();
            let red = redact_proxy_url(raw);
            for rendered in [&dbg, &disp, &red] {
                assert!(!rendered.contains("alice"), "leaked user in {rendered}");
                assert!(!rendered.contains("ssword"), "leaked pass in {rendered}");
                assert!(
                    rendered.contains("proxy.example"),
                    "missing host in {rendered}"
                );
            }
        }
        let unauth = BrowserProxy::parse("socks5h://127.0.0.1:1080").unwrap();
        assert!(!unauth.is_authenticated());
        assert_eq!(unauth.redacted_display(), "socks5h://127.0.0.1:1080");
        assert_eq!(
            unauth.direct_chromium_args(),
            [
                "--proxy-server=socks5h://127.0.0.1:1080".to_string(),
                format!("--proxy-bypass-list={PROXY_BYPASS_LIST}"),
            ]
        );

        let bad_err = BrowserProxy::parse("ftp://secret-u:secret-p@bad.example:21").unwrap_err();
        assert!(!bad_err.message.contains("secret-u") && !bad_err.message.contains("secret-p"));
        let syn_err = BrowserProxy::parse("not a url secret-u:secret-p@x").unwrap_err();
        assert!(!syn_err.message.contains("secret-u") && !syn_err.message.contains("secret-p"));
    }

    #[tokio::test]
    async fn unauthenticated_proxy_passes_through_without_spawning_relay() {
        let p = BrowserProxy::parse("http://proxy.example:8080").unwrap();
        let h = ProxyHandle::start(&p).await.unwrap();
        assert_eq!(h.proxy_server_url(), "http://proxy.example:8080");
        assert!(h.relay.is_none());
        assert_eq!(
            h.chromium_args(),
            [
                "--proxy-server=http://proxy.example:8080".to_string(),
                format!("--proxy-bypass-list={PROXY_BYPASS_LIST}"),
            ]
        );
    }

    #[tokio::test]
    async fn authenticated_http_forwarder_injects_basic_auth_and_redacts_argv() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = upstream.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (s, _) = upstream.accept().await.unwrap();
            let head = read_http_head(&s).await.unwrap().unwrap();
            tcp_write_all(&s, b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let mut payload = [0u8; 4];
            tcp_read_exact(&s, &mut payload).await.unwrap();
            assert_eq!(&payload, b"ping");
            tcp_write_all(&s, b"pong").await.unwrap();
            String::from_utf8(head).unwrap()
        });

        let proxy = BrowserProxy::parse(&format!("http://myuser:s3cr3t@{up_addr}")).unwrap();
        let handle = ProxyHandle::start(&proxy).await.unwrap();
        let args = handle.chromium_args();
        assert!(args[0].starts_with("--proxy-server=http://127.0.0.1:"));
        assert_eq!(args[1], format!("--proxy-bypass-list={PROXY_BYPASS_LIST}"));
        let dbg = format!("{handle:?}");
        for text in [&args[0], &args[1], &dbg] {
            assert!(!text.contains("myuser") && !text.contains("s3cr3t"));
        }

        let local_addr = handle.proxy_server_url().trim_start_matches("http://");
        let client = TcpStream::connect(local_addr).await.unwrap();
        tcp_write_all(
            &client,
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: Basic ZmFrZTpmYWtl\r\n\r\n",
        )
        .await
        .unwrap();
        let resp = read_http_head(&client).await.unwrap().unwrap();
        assert!(resp.starts_with(b"HTTP/1.1 200"));
        tcp_write_all(&client, b"ping").await.unwrap();
        let mut reply = [0u8; 4];
        tcp_read_exact(&client, &mut reply).await.unwrap();
        assert_eq!(&reply, b"pong");

        let seen = server.await.unwrap();
        let expected_b64 = base64::engine::general_purpose::STANDARD.encode("myuser:s3cr3t");
        assert!(seen.contains(&format!("Proxy-Authorization: Basic {expected_b64}\r\n")));
        assert!(!seen.contains("ZmFrZTpmYWtl"));
    }

    #[tokio::test]
    async fn authenticated_socks5h_forwarder_completes_rfc1928_1929_handshake() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = upstream.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (s, _) = upstream.accept().await.unwrap();
            let mut greet = [0u8; 3];
            tcp_read_exact(&s, &mut greet).await.unwrap();
            assert_eq!(greet, [5, 1, 2]);
            tcp_write_all(&s, &[5, 2]).await.unwrap();

            // RFC 1929 auth: [1, ulen=2, "su", plen=4, "spwd"]
            let mut auth = [0u8; 9];
            tcp_read_exact(&s, &mut auth).await.unwrap();
            assert_eq!(&auth, b"\x01\x02su\x04spwd");
            tcp_write_all(&s, &[1, 0]).await.unwrap();

            // RFC 1928 CONNECT: [5, 1, 0, 3, len=11, "example.com", 0x01, 0xbb]
            let mut req = [0u8; 18];
            tcp_read_exact(&s, &mut req).await.unwrap();
            assert_eq!(&req[..5], &[5, 1, 0, 3, 11]);
            assert_eq!(&req[5..16], b"example.com");
            assert_eq!(&req[16..18], &443u16.to_be_bytes());
            tcp_write_all(&s, &[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                .await
                .unwrap();

            let mut msg = [0u8; 5];
            tcp_read_exact(&s, &mut msg).await.unwrap();
            assert_eq!(&msg, b"hello");
            tcp_write_all(&s, b"world").await.unwrap();
        });

        let proxy = BrowserProxy::parse(&format!("socks5h://su:spwd@{up_addr}")).unwrap();
        let handle = ProxyHandle::start(&proxy).await.unwrap();
        assert!(handle.proxy_server_url().starts_with("http://127.0.0.1:"));
        assert!(
            !handle.proxy_server_url().contains("su")
                && !handle.proxy_server_url().contains("spwd")
        );

        let local_addr = handle.proxy_server_url().trim_start_matches("http://");
        let client = TcpStream::connect(local_addr).await.unwrap();
        tcp_write_all(
            &client,
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n",
        )
        .await
        .unwrap();
        let resp = read_http_head(&client).await.unwrap().unwrap();
        assert!(resp.starts_with(b"HTTP/1.1 200"));
        tcp_write_all(&client, b"hello").await.unwrap();
        let mut back = [0u8; 5];
        tcp_read_exact(&client, &mut back).await.unwrap();
        assert_eq!(&back, b"world");
        server.await.unwrap();
    }
}
