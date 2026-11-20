//! SSRF private/internal IP guard and domain allowlist/blocklist policy.

use std::net::{IpAddr, ToSocketAddrs};

use url::{Host, Url};

/// Returns `true` only if `ip` is a globally routable unicast address.
///
/// Rejects loopback, RFC1918 private, link-local (including cloud metadata
/// `169.254.169.254`), unspecified (`0.0.0.0/8`, `::`), broadcast, multicast,
/// CGNAT (`100.64.0.0/10`), benchmarking (`198.18.0.0/15`), documentation,
/// reserved (`240.0.0.0/4`), IPv6 ULA (`fc00::/7`), IPv6 link-local (`fe80::/10`),
/// and IPv4-mapped IPv6 addresses targeting any rejected IPv4 range.
pub fn is_global_unicast_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.is_multicast()
                || o[0] == 0 // "this network" 0.0.0.0/8
                || (o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000) // CGNAT 100.64.0.0/10
                || (o[0] == 198 && (o[1] & 0xfe) == 18) // Benchmarking 198.18.0.0/15
                || o[0] >= 240) // Reserved 240.0.0.0/4
        }
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_global_unicast_ip(IpAddr::V4(mapped));
            }
            let s = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00 // Unique local fc00::/7
                || (s[0] & 0xffc0) == 0xfe80 // Link-local fe80::/10
                || (s[0] == 0x2001 && s[1] == 0x0db8)) // Documentation 2001:db8::/32
        }
    }
}

/// Validate a URL for SSRF safety, including DNS resolution of hostnames.
pub fn validate_public_url(raw_url: &str) -> Result<Url, String> {
    validate_public_url_inner(raw_url, true)
}

/// Validate a URL's scheme, credentials, hostname, and (optionally) resolved IP addresses.
pub fn validate_public_url_inner(raw_url: &str, resolve_dns: bool) -> Result<Url, String> {
    let trimmed = raw_url.trim();
    let parsed = Url::parse(trimmed)
        .map_err(|e| format!("URL must be a valid `http://` or `https://` URL: {e}"))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(format!(
                "SSRF guard rejected unsupported URL scheme `{other}`"
            ));
        }
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("URL must not contain embedded user credentials".to_string());
    }
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "URL missing port".to_string())?;
    let host = parsed
        .host()
        .ok_or_else(|| "URL missing host".to_string())?;

    match host {
        Host::Ipv4(v4) => {
            let ip = IpAddr::V4(v4);
            if !is_global_unicast_ip(ip) {
                return Err(format!("SSRF guard rejected non-public IP `{ip}`"));
            }
            Ok(parsed)
        }
        Host::Ipv6(v6) => {
            let ip = IpAddr::V6(v6);
            if !is_global_unicast_ip(ip) {
                return Err(format!("SSRF guard rejected non-public IP `{ip}`"));
            }
            Ok(parsed)
        }
        Host::Domain(domain) => {
            let lower = domain.trim_matches('.').to_ascii_lowercase();
            if lower.is_empty()
                || lower == "localhost"
                || lower.ends_with(".localhost")
                || lower.ends_with(".local")
                || lower.ends_with(".internal")
                || lower == "metadata.google.internal"
                || lower == "metadata.goog"
                || lower == "instance-data"
            {
                return Err(format!("SSRF guard rejected local/internal host `{lower}`"));
            }
            if resolve_dns {
                let addrs: Vec<_> = (lower.as_str(), port)
                    .to_socket_addrs()
                    .map_err(|e| format!("failed to resolve host `{lower}`: {e}"))?
                    .collect();
                if addrs.is_empty() {
                    return Err(format!("host `{lower}` resolved to no addresses"));
                }
                for sock in addrs {
                    if !is_global_unicast_ip(sock.ip()) {
                        return Err(format!(
                            "SSRF guard rejected host `{lower}` resolving to non-public IP `{}`",
                            sock.ip()
                        ));
                    }
                }
            }
            Ok(parsed)
        }
    }
}

/// Normalize a domain filter entry (`https://Docs.RS/foo` -> `docs.rs`).
pub fn normalize_domain(raw: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    let without_scheme = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(&trimmed);
    without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .split(':')
        .next()
        .unwrap_or(without_scheme)
        .trim_matches('.')
        .to_string()
}

/// Whether `host` equals `domain` or is a subdomain of `domain`.
pub fn host_matches_domain(host: &str, domain: &str) -> bool {
    let h = host.trim_matches('.').to_ascii_lowercase();
    let d = normalize_domain(domain);
    if d.is_empty() {
        return false;
    }
    h == d || h.ends_with(&format!(".{d}"))
}

/// Check whether `url_str` is an HTTP(S) URL permitted by `allowed_domains` and `blocked_domains`.
pub fn url_allowed_by_domain_policy(
    url_str: &str,
    allowed_domains: &[String],
    blocked_domains: &[String],
) -> bool {
    let Ok(parsed) = Url::parse(url_str.trim()) else {
        return false;
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    if !blocked_domains.is_empty() && blocked_domains.iter().any(|d| host_matches_domain(host, d)) {
        return false;
    }
    if !allowed_domains.is_empty() && !allowed_domains.iter().any(|d| host_matches_domain(host, d))
    {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_private_loopback_metadata_and_non_http_urls() {
        for bad in [
            "file:///etc/passwd",
            "ftp://example.com/data",
            "http://localhost:8080/admin",
            "http://sub.localhost/secret",
            "http://service.internal/api",
            "http://metadata.google.internal/computeMetadata/v1/",
            "http://127.0.0.1:3000/",
            "http://0x7f000001/",
            "http://10.0.0.1/",
            "http://172.16.0.1/",
            "http://192.168.1.1/",
            "http://169.254.169.254/latest/meta-data/",
            "http://100.64.0.1/",
            "http://[::1]:8080/",
            "http://[::ffff:127.0.0.1]/",
            "http://[fe80::1]/",
            "http://[fc00::1]/",
            "https://user:pass@example.com/",
        ] {
            assert!(
                validate_public_url_inner(bad, false).is_err(),
                "expected SSRF guard to reject `{bad}`"
            );
        }
        assert!(validate_public_url_inner("https://93.184.216.34/index.html", false).is_ok());
        assert!(validate_public_url_inner("https://1.1.1.1/", false).is_ok());
        assert!(validate_public_url_inner("https://example.com/docs", false).is_ok());
    }

    #[test]
    fn enforces_allowed_and_blocked_domain_policies() {
        let allowed = vec!["rust-lang.org".to_string(), "docs.rs".to_string()];
        let blocked = vec!["internal.docs.rs".to_string()];
        assert!(url_allowed_by_domain_policy(
            "https://doc.rust-lang.org/std/",
            &allowed,
            &blocked
        ));
        assert!(url_allowed_by_domain_policy(
            "https://docs.rs/serde",
            &allowed,
            &blocked
        ));
        assert!(!url_allowed_by_domain_policy(
            "https://internal.docs.rs/secret",
            &allowed,
            &blocked
        ));
        assert!(!url_allowed_by_domain_policy(
            "https://example.com/",
            &allowed,
            &blocked
        ));
    }
}
