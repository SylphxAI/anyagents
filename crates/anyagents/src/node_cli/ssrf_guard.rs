//! Fail-closed SSRF / private-network deny-list for egress-proxy dialing.
//!
//! Before connecting to any `host:port`, the node resolves DNS and validates
//! every resolved `SocketAddr` against a configurable deny-list.  By default
//! ALL private/reserved ranges are denied, preventing DNS rebinding attacks.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use serde::{Deserialize, Serialize};

/// Policy configuration for the egress SSRF guard.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct EgressPolicy {
    /// Explicitly allowed private CIDRs (e.g. `["10.0.0.0/8"]`).
    #[serde(default)]
    pub(crate) allow_private_cidrs: Vec<String>,
    /// Explicitly allowed destination domains.
    #[serde(default)]
    pub(crate) allow_domains: Vec<String>,
    /// Explicitly denied destination domains (checked before allow).
    #[serde(default)]
    pub(crate) deny_domains: Vec<String>,
}

/// Parsed CIDR for fast prefix matching.
#[derive(Clone, Debug)]
struct Cidr {
    network: IpAddr,
    prefix_len: u8,
}

impl Cidr {
    fn parse(raw: &str) -> Option<Self> {
        let (addr_str, prefix_str) = raw.trim().rsplit_once('/')?;
        let prefix_len: u8 = prefix_str.parse().ok()?;
        let network: IpAddr = addr_str.parse().ok()?;
        match network {
            IpAddr::V4(_) if prefix_len > 32 => None,
            IpAddr::V6(_) if prefix_len > 128 => None,
            _ => Some(Self {
                network,
                prefix_len,
            }),
        }
    }

    fn contains(&self, addr: &IpAddr) -> bool {
        match (self.network, addr) {
            (IpAddr::V4(net), IpAddr::V4(test)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                let mask = u32::MAX
                    .checked_shl(32 - u32::from(self.prefix_len))
                    .unwrap_or(0);
                (u32::from(net) & mask) == (u32::from(*test) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(test)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                let mask = u128::MAX
                    .checked_shl(128 - u32::from(self.prefix_len))
                    .unwrap_or(0);
                (u128::from(net) & mask) == (u128::from(*test) & mask)
            }
            _ => false,
        }
    }
}

/// Compiled deny-list for fast runtime checks.
#[derive(Clone, Debug)]
pub(crate) struct SsrfGuard {
    allowed_cidrs: Vec<Cidr>,
    allow_domains: Vec<String>,
    deny_domains: Vec<String>,
}

/// Result of an SSRF guard check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SsrfDecision {
    Allow,
    Deny(String),
}

impl SsrfGuard {
    pub(crate) fn from_policy(policy: &EgressPolicy) -> Self {
        let allowed_cidrs = policy
            .allow_private_cidrs
            .iter()
            .filter_map(|raw| Cidr::parse(raw))
            .collect();
        let allow_domains = policy
            .allow_domains
            .iter()
            .map(|d| d.trim().to_ascii_lowercase())
            .filter(|d| !d.is_empty())
            .collect();
        let deny_domains = policy
            .deny_domains
            .iter()
            .map(|d| d.trim().to_ascii_lowercase())
            .filter(|d| !d.is_empty())
            .collect();
        Self {
            allowed_cidrs,
            allow_domains,
            deny_domains,
        }
    }

    /// Check whether connecting to `host:port` via the given resolved addresses
    /// is allowed.  Returns `SsrfDecision::Deny(reason)` if any resolved address
    /// falls into a denied range that is not explicitly allowed.
    pub(crate) fn check_host(
        &self,
        host: &str,
        _port: u16,
        resolved: &[SocketAddr],
    ) -> SsrfDecision {
        let host_lower = host.trim().to_ascii_lowercase();

        // Domain deny-list takes priority.
        for pattern in &self.deny_domains {
            if domain_matches(&host_lower, pattern) {
                return SsrfDecision::Deny(format!(
                    "destination domain `{host}` is in the deny-list"
                ));
            }
        }

        // If the host is in the allow-domains list, skip IP checks.
        let domain_allowed = self
            .allow_domains
            .iter()
            .any(|pattern| domain_matches(&host_lower, pattern));

        if domain_allowed {
            return SsrfDecision::Allow;
        }

        // Check every resolved address against the private/reserved deny-list.
        for addr in resolved {
            let ip = addr.ip();
            if is_private_or_reserved(&ip) && !self.is_cidr_allowed(&ip) {
                return SsrfDecision::Deny(format!(
                    "resolved address {ip} for `{host}` is in a private/reserved range"
                ));
            }
        }

        SsrfDecision::Allow
    }

    fn is_cidr_allowed(&self, ip: &IpAddr) -> bool {
        self.allowed_cidrs.iter().any(|cidr| cidr.contains(ip))
    }
}

fn domain_matches(host: &str, pattern: &str) -> bool {
    host == pattern || host.ends_with(&format!(".{pattern}"))
}

/// Returns `true` if the IP address belongs to any private, reserved,
/// loopback, link-local, multicast, unspecified, CGNAT, or other
/// non-globally-routable range.
fn is_private_or_reserved(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_v4_private_or_reserved(v4),
        IpAddr::V6(v6) => is_v6_private_or_reserved(v6),
    }
}

fn is_v4_private_or_reserved(ip: &Ipv4Addr) -> bool {
    let octets = ip.octets();
    // Loopback: 127.0.0.0/8
    if octets[0] == 127 {
        return true;
    }
    // RFC1918 10.0.0.0/8
    if octets[0] == 10 {
        return true;
    }
    // RFC1918 172.16.0.0/12
    if octets[0] == 172 && (16..=31).contains(&octets[1]) {
        return true;
    }
    // RFC1918 192.168.0.0/16
    if octets[0] == 192 && octets[1] == 168 {
        return true;
    }
    // Link-local / APIPA: 169.254.0.0/16 (includes cloud metadata 169.254.169.254)
    if octets[0] == 169 && octets[1] == 254 {
        return true;
    }
    // CGNAT: 100.64.0.0/10
    if octets[0] == 100 && (64..=127).contains(&octets[1]) {
        return true;
    }
    // Multicast: 224.0.0.0/4
    if (224..=239).contains(&octets[0]) {
        return true;
    }
    // Reserved: 240.0.0.0/4 (includes broadcast 255.255.255.255)
    if octets[0] >= 240 {
        return true;
    }
    // Unspecified: 0.0.0.0/8
    if octets[0] == 0 {
        return true;
    }
    // Documentation: 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
    if (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
    {
        return true;
    }
    // Benchmarking: 198.18.0.0/15
    if octets[0] == 198 && (18..=19).contains(&octets[1]) {
        return true;
    }
    false
}

fn is_v6_private_or_reserved(ip: &Ipv6Addr) -> bool {
    let segs = ip.segments();
    // Loopback: ::1
    if ip == &Ipv6Addr::LOCALHOST {
        return true;
    }
    // Unspecified: ::
    if ip == &Ipv6Addr::UNSPECIFIED {
        return true;
    }
    // IPv4-mapped: ::ffff:0:0/96 — check the embedded v4 address.
    if segs[0] == 0
        && segs[1] == 0
        && segs[2] == 0
        && segs[3] == 0
        && segs[4] == 0
        && segs[5] == 0xffff
    {
        let v4 = Ipv4Addr::new(
            (segs[6] >> 8) as u8,
            segs[6] as u8,
            (segs[7] >> 8) as u8,
            segs[7] as u8,
        );
        return is_v4_private_or_reserved(&v4);
    }
    // Link-local: fe80::/10
    if (segs[0] & 0xffc0) == 0xfe80 {
        return true;
    }
    // Unique local (ULA): fc00::/7
    if (segs[0] & 0xfe00) == 0xfc00 {
        return true;
    }
    // Multicast: ff00::/8
    if (segs[0] & 0xff00) == 0xff00 {
        return true;
    }
    // Discard: 100::/64
    if segs[0] == 0x0100 && segs[1] == 0 && segs[2] == 0 && segs[3] == 0 {
        return true;
    }
    // Documentation: 2001:db8::/32
    if segs[0] == 0x2001 && segs[1] == 0x0db8 {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(ip: &str, port: u16) -> SocketAddr {
        SocketAddr::new(ip.parse().unwrap(), port)
    }

    fn default_guard() -> SsrfGuard {
        SsrfGuard::from_policy(&EgressPolicy::default())
    }

    #[test]
    fn denies_loopback_v4_and_v6() {
        let guard = default_guard();
        assert_eq!(
            guard.check_host("localhost", 80, &[addr("127.0.0.1", 80)]),
            SsrfDecision::Deny(
                "resolved address 127.0.0.1 for `localhost` is in a private/reserved range".into()
            )
        );
        assert_eq!(
            guard.check_host("localhost", 80, &[addr("::1", 80)]),
            SsrfDecision::Deny(
                "resolved address ::1 for `localhost` is in a private/reserved range".into()
            )
        );
    }

    #[test]
    fn denies_rfc1918() {
        let guard = default_guard();
        for ip in ["10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.1"] {
            assert!(
                matches!(
                    guard.check_host("internal", 80, &[addr(ip, 80)]),
                    SsrfDecision::Deny(_)
                ),
                "should deny {ip}"
            );
        }
    }

    #[test]
    fn denies_metadata_link_local() {
        let guard = default_guard();
        assert!(matches!(
            guard.check_host("metadata.cloud", 80, &[addr("169.254.169.254", 80)]),
            SsrfDecision::Deny(_)
        ));
    }

    #[test]
    fn denies_cgnat() {
        let guard = default_guard();
        assert!(matches!(
            guard.check_host("cgnat", 80, &[addr("100.64.0.1", 80)]),
            SsrfDecision::Deny(_)
        ));
    }

    #[test]
    fn denies_ipv4_mapped_ipv6() {
        let guard = default_guard();
        assert!(matches!(
            guard.check_host("mapped", 80, &[addr("::ffff:127.0.0.1", 80)]),
            SsrfDecision::Deny(_)
        ));
        assert!(matches!(
            guard.check_host("mapped", 80, &[addr("::ffff:10.0.0.1", 80)]),
            SsrfDecision::Deny(_)
        ));
    }

    #[test]
    fn denies_ula_and_link_local_v6() {
        let guard = default_guard();
        assert!(matches!(
            guard.check_host("ula", 80, &[addr("fd12::1", 80)]),
            SsrfDecision::Deny(_)
        ));
        assert!(matches!(
            guard.check_host("ll", 80, &[addr("fe80::1", 80)]),
            SsrfDecision::Deny(_)
        ));
    }

    #[test]
    fn allows_public_ip() {
        let guard = default_guard();
        assert_eq!(
            guard.check_host("example.com", 443, &[addr("93.184.216.34", 443)]),
            SsrfDecision::Allow
        );
    }

    #[test]
    fn allow_private_cidr_overrides_deny() {
        let guard = SsrfGuard::from_policy(&EgressPolicy {
            allow_private_cidrs: vec!["127.0.0.0/8".into()],
            ..Default::default()
        });
        assert_eq!(
            guard.check_host("localhost", 80, &[addr("127.0.0.1", 80)]),
            SsrfDecision::Allow
        );
    }

    #[test]
    fn deny_domain_overrides_allow_domain() {
        let guard = SsrfGuard::from_policy(&EgressPolicy {
            allow_domains: vec!["example.com".into()],
            deny_domains: vec!["example.com".into()],
            ..Default::default()
        });
        assert!(matches!(
            guard.check_host("example.com", 80, &[addr("93.184.216.34", 80)]),
            SsrfDecision::Deny(_)
        ));
    }

    #[test]
    fn allow_domain_skips_ip_check() {
        let guard = SsrfGuard::from_policy(&EgressPolicy {
            allow_domains: vec!["internal.corp".into()],
            ..Default::default()
        });
        assert_eq!(
            guard.check_host("internal.corp", 80, &[addr("10.0.0.5", 80)]),
            SsrfDecision::Allow
        );
    }
}
