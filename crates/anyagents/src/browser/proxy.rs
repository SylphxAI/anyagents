//! Proxy selection is scoped to a browser process, never to the local CDP client.

use super::BrowserRuntimeState;
use anyagents_core::Error;

pub(super) fn environment_default() -> Result<Option<String>, Error> {
    match std::env::var("ANYAGENTS_BROWSER_PROXY") {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(Error::Message(
            "ANYAGENTS_BROWSER_PROXY must be valid Unicode".into(),
        )),
    }
}

pub(super) fn resolve(
    requested: Option<&str>,
    running: Option<&BrowserRuntimeState>,
    default: impl FnOnce() -> Result<Option<String>, Error>,
) -> Result<Option<String>, Error> {
    let selected = if let Some(value) = requested {
        Some(value.to_string())
    } else if let Some(state) = running {
        state.proxy.clone()
    } else {
        default()?
    };
    if let Some(value) = selected.as_deref() {
        validate(value)?;
    }
    Ok(selected)
}

fn validate(value: &str) -> Result<(), Error> {
    // Never echo the input: accidentally supplied userinfo may contain secrets.
    let invalid = || {
        Error::Message(
        "invalid browser proxy: expected an HTTP/HTTPS/SOCKS4/SOCKS5 host with optional port, without credentials, whitespace, paths, or proxy maps; use an empty string for direct access".into(),
    )
    };
    if value.is_empty() {
        return Ok(());
    }
    if !value.is_ascii()
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
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let value = format!("socks5://{}", listener.local_addr().unwrap());
        validate(&value).unwrap();
        assert_eq!(
            launch_argument(Some(&value)),
            Some(format!("--proxy-server={value}"))
        );
        assert_eq!(launch_argument(Some("")), Some("--no-proxy-server".into()));
        assert_eq!(launch_argument(None), None);
    }

    #[test]
    fn proxy_rejects_malformed_input_without_echoing_credentials() {
        for value in [
            " ",
            " http://proxy.example.test",
            "proxy.example.test\n",
            "proxy.example.test\0",
            "proxy.example.test\u{2003}",
            "--no-sandbox",
            "http://user:private@proxy.example.test",
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
        ] {
            let error = match validate(value) {
                Err(error) => error.to_string(),
                Ok(()) => panic!("malformed proxy accepted: {value:?}"),
            };
            assert!(!error.contains("private"));
        }
    }

    #[test]
    fn proxy_precedence_preserves_running_route_and_explicit_direct() {
        let default = || Ok(Some("socks5://default.example.test".into()));
        assert_eq!(resolve(None, None, default).unwrap(), default().unwrap());
        assert_eq!(resolve(Some(""), None, default).unwrap(), Some("".into()));
        let mut running: BrowserRuntimeState = serde_json::from_value(serde_json::json!({
            "profile":"test", "pid":0, "cdp_port":0, "headless":true,
            "display_mode":"test", "started_at_ms":0, "last_used_at_ms":0
        }))
        .unwrap();
        assert_eq!(running.proxy, None); // older runtime metadata remains readable
        assert_eq!(
            resolve(None, Some(&running), || panic!("must not read environment")).unwrap(),
            None
        );
        running.proxy = Some("http://running.example.test".into());
        assert_eq!(
            resolve(None, Some(&running), default).unwrap(),
            running.proxy
        );
        assert_eq!(
            resolve(Some(""), Some(&running), default).unwrap(),
            Some("".into())
        );
        assert!(resolve(None, None, || Ok(Some("bad input".into()))).is_err());
        assert!(
            resolve(None, None, || Err(Error::Message(
                "unreadable default".into()
            )))
            .is_err()
        );
    }
}
