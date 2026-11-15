//! Proxy selection belongs to the browser process, not its local control client.

use super::ToolError;
type Result<T> = std::result::Result<T, ToolError>;

fn invalid_proxy() -> ToolError {
    // Neither URL parser nor engine diagnostics may echo credential material.
    ToolError::invalid(
        "invalid browser proxy: expected a valid proxy endpoint without whitespace, paths, queries, fragments, or proxy maps",
    )
}

pub(super) fn resolve(
    requested: Option<&str>,
    running: Option<Option<&str>>,
    default: impl FnOnce() -> Result<Option<String>>,
) -> Result<Option<String>> {
    let selected = if let Some(value) = requested {
        Some(value.to_string())
    } else if let Some(value) = running {
        value.map(str::to_string)
    } else {
        default()?
    };
    configure(selected.as_deref())?;
    Ok(selected)
}

/// Credentialed proxies use the engine's authentication transport. Everything
/// else retains Chromium's endpoint syntax, including explicit direct fallback.
pub(super) fn configure(
    value: Option<&str>,
) -> Result<(Option<agent_workstation::Proxy>, Option<String>)> {
    let Some(value) = value else {
        return Ok((None, None));
    };
    if !value.contains('@') {
        validate(value)?;
        return Ok((None, launch_argument(Some(value))));
    }
    let (scheme, authority) = value.split_once("://").ok_or_else(invalid_proxy)?;
    if !matches!(scheme, "http" | "socks5")
        || !value.is_ascii()
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(invalid_proxy());
    }
    let (userinfo, endpoint) = authority.split_once('@').ok_or_else(invalid_proxy)?;
    if userinfo.is_empty()
        || userinfo.contains(['@', '/', '\\', '?', '#', ';', ',', '=', '\'', '"'])
        || endpoint.contains('@')
    {
        return Err(invalid_proxy());
    }
    // Validate the raw host/port before URL normalization can discard syntax.
    validate_endpoint(&format!("{scheme}://{endpoint}"))?;
    let url = url::Url::parse(value).map_err(|_| invalid_proxy())?;
    if url.username().is_empty()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid_proxy());
    }
    let proxy = agent_workstation::Proxy::parse(value).map_err(|_| invalid_proxy())?;
    Ok((Some(proxy), None))
}

/// The one fallback Chromium's proxy list may end with: connect directly when
/// every proxy before it is unreachable.
const DIRECT_FALLBACK: &str = ",direct://";

fn validate(value: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    validate_endpoint(value.strip_suffix(DIRECT_FALLBACK).unwrap_or(value))
}

fn validate_endpoint(value: &str) -> Result<()> {
    let invalid = invalid_proxy;
    if value.is_empty()
        || !value.is_ascii()
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
        || value.contains(['@', '/', '\\', '?', '#', ';', ',', '=', '\'', '"'])
            && !value.contains("://")
    {
        return Err(invalid());
    }
    let (scheme, endpoint) = value.split_once("://").unwrap_or(("http", value));
    if !matches!(scheme, "http" | "https" | "socks4" | "socks5") {
        return Err(invalid());
    }
    if endpoint.contains(['@', '/', '\\', '?', '#', ';', ',', '=', '\'', '"']) {
        return Err(invalid());
    }
    // Check the port explicitly: Authority normalizes some out-of-range ports.
    let port = if endpoint.starts_with('[') {
        endpoint.split_once("]:").map(|(_, port)| port)
    } else {
        endpoint.rsplit_once(':').map(|(_, port)| port)
    };
    if let Some(port) = port
        && (port.is_empty()
            || !port.bytes().all(|c| c.is_ascii_digit())
            || !matches!(port.parse::<u32>(), Ok(p) if (1..=65535).contains(&p)))
    {
        return Err(invalid());
    }
    let authority = endpoint
        .parse::<axum::http::uri::Authority>()
        .map_err(|_| invalid())?;
    let host = authority.host();
    if host.starts_with('[') {
        host.trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::Ipv6Addr>()
            .map_err(|_| invalid())?;
    } else if !host.trim_end_matches('.').split('.').all(|label| {
        !label.is_empty()
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
    }) {
        return Err(invalid());
    }
    if authority.port().is_some() && authority.port_u16().is_none_or(|port| port == 0)
        || endpoint.ends_with(':')
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn launch_argument(proxy: Option<&str>) -> Option<String> {
    proxy.map(|value| {
        if value.is_empty() {
            "--no-proxy-server".to_string()
        } else {
            format!("--proxy-server={value}")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_endpoints_and_launch_arguments() {
        for value in [
            "proxy.example.test",
            "http://proxy.example.test",
            "https://proxy.example.test",
            "socks4://proxy.example.test",
            "socks5://[::1]",
            "",
        ] {
            validate(value).unwrap();
        }
        assert!(configure(None).unwrap().0.is_none());
        assert_eq!(configure(None).unwrap().1, None);
        assert_eq!(
            configure(Some("")).unwrap().1.as_deref(),
            Some("--no-proxy-server")
        );
        for value in [
            "http://user:secret@proxy.example.test:8080",
            "socks5://user:secret@[::1]:1080",
        ] {
            let (transport, argument) = configure(Some(value)).unwrap();
            assert!(transport.is_some());
            assert!(argument.is_none());
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let value = format!("socks5://{}", listener.local_addr().unwrap());
        validate(&value).unwrap();
        assert_eq!(
            launch_argument(Some(&value)),
            Some(format!("--proxy-server={value}"))
        );
        let fallback = format!("{value},direct://");
        validate(&fallback).unwrap();
        validate("proxy.example.test:1080,direct://").unwrap();
        assert_eq!(
            launch_argument(Some(&fallback)),
            Some(format!("--proxy-server={value},direct://"))
        );
        assert_eq!(launch_argument(Some("")), Some("--no-proxy-server".into()));
        assert_eq!(launch_argument(None), None);
    }

    #[test]
    fn proxy_rejects_malformed_input_without_echoing_credentials() {
        for value in [
            "http://private:secret@extra@proxy.example.test",
            "https://private:secret@proxy.example.test",
            "socks4://private:secret@proxy.example.test",
            "http://private:secret@proxy.example.test/",
            "http://private:secret@proxy.example.test?query",
            "http://private:secret@proxy.example.test#fragment",
            "http://private:secret@proxy.example.test:65536",
            "http://private:secret@proxy.example.test,direct://",
            " ",
            " http://proxy.example.test",
            "proxy.example.test\n",
            "proxy.example.test\0",
            "proxy.example.test\u{2003}",
            "--no-sandbox",
            "http://user:private@proxy.example.test/path",
            "socks5://proxy.example.test/path",
            "http://proxy.example.test?query",
            "proxy.example.test#fragment",
            "proxy.example.test;direct://",
            "direct://",
            "ftp://proxy.example.test",
            "http://",
            "http://[invalid]",
            "http://a..test",
            "proxy.example.test:0",
            "proxy.example.test:65536",
            "proxy.example.test:word",
            "proxy.example.test:",
            "http://proxy.example.test/",
            "http=proxy.example.test",
            "http://proxy.example.test --no-sandbox",
            "http://proxy.example.test\\path",
            "\"proxy.example.test\"",
            "http://proxy.example.test,other.example.test",
            ",direct://",
            "direct://,direct://",
            "proxy.example.test,direct://,direct://",
            "proxy.example.test;direct://,direct://",
            "proxy.example.test,direct:// ",
            "proxy.example.test, direct://",
            "proxy.example.test,DIRECT",
            "http://user:private@proxy.example.test,direct://",
            "--no-sandbox,direct://",
        ] {
            let error = match configure(Some(value)) {
                Err(error) => error.message,
                Ok(_) => panic!("malformed proxy accepted"),
            };
            assert!(!error.contains("private"));
            assert!(!error.contains("secret"));
            assert_eq!(error, invalid_proxy().message);
        }
    }

    #[test]
    fn proxy_precedence_preserves_running_route_and_explicit_direct() {
        let default = || Ok(Some("socks5://default.example.test".into()));
        assert_eq!(resolve(None, None, default).unwrap(), default().unwrap());
        assert_eq!(resolve(Some(""), None, default).unwrap(), Some("".into()));
        assert_eq!(
            resolve(None, Some(None), || panic!("must not read environment")).unwrap(),
            None
        );
        let running = Some("http://running.example.test");
        assert_eq!(
            resolve(None, Some(running), default).unwrap(),
            running.map(str::to_string)
        );
        assert_eq!(
            resolve(Some(""), Some(running), || panic!(
                "must not read environment"
            ))
            .unwrap(),
            Some("".into())
        );
        assert!(resolve(None, None, || Ok(Some("bad input".into()))).is_err());
        assert!(resolve(None, None, || Err(ToolError::invalid("unreadable default"))).is_err());
    }

    /// Real Chromium honors the fallback: with the proxy port closed, a page
    /// still loads directly instead of failing.
    #[test]
    #[cfg(target_os = "linux")]
    fn direct_fallback_reaches_the_page_when_the_proxy_is_down() {
        use std::io::{Read, Write};
        let chromium = match super::super::session::tool("chromium", "browser") {
            Ok(path) => path,
            Err(error) => {
                assert!(
                    std::env::var_os("CI").is_none(),
                    "Chromium fixture missing in CI: {}",
                    error.message
                );
                eprintln!("skipping direct fallback fixture: {}", error.message);
                return;
            }
        };
        let closed = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let server = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();
        std::thread::spawn(move || {
            for mut stream in server.incoming().flatten() {
                std::thread::spawn(move || {
                    let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
                    let mut buf = [0u8; 4096];
                    if stream.read(&mut buf).unwrap_or(0) == 0 {
                        return;
                    }
                    let body = "<html><body>reached-directly</body></html>";
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                });
            }
        });
        let proxy = format!("socks5://{closed},direct://");
        validate(&proxy).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let out = std::process::Command::new(chromium)
            .arg("--headless=new")
            .arg("--no-sandbox")
            .arg("--disable-gpu")
            .arg("--no-first-run")
            .arg("--timeout=20000")
            .arg(launch_argument(Some(&proxy)).unwrap())
            // Loopback bypasses proxies by default; route it through the list.
            .arg("--proxy-bypass-list=<-loopback>")
            .arg(format!("--user-data-dir={}", dir.path().display()))
            .arg("--dump-dom")
            .arg(format!("http://{addr}/"))
            .stdin(std::process::Stdio::null())
            .output()
            .expect("run Chromium");
        let dom = String::from_utf8_lossy(&out.stdout);
        assert!(
            dom.contains("reached-directly"),
            "page did not load through the direct fallback: {dom}"
        );
    }
}
