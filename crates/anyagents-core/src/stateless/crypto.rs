//! Authenticated symmetric encryption (Encrypt-then-MAC) for secret blobs,
//! gateway credentials, and sandbox transport tokens stored in PostgreSQL.
//!
//! Uses domain-separated SHA-256 CTR keystream + HMAC-SHA256 authentication
//! over `(magic || nonce || aad_len || aad || ciphertext)` with constant-time
//! tag verification. Fails closed on missing keys, wrong AAD, or tampered bytes.

use std::fmt;

use sha2::{Digest, Sha256};

use crate::error::Error;
use crate::storage::storage_env_enabled;

/// Primary deployment encryption key for PostgreSQL secret columns.
pub const MASTER_KEY_ENV: &str = "ANYAGENTS_MASTER_KEY";
/// Fallback shared deployment key when only the capability HMAC key is configured.
pub const CAPABILITY_KEY_ENV: &str = "ANYAGENTS_CAPABILITY_KEY";

const MAGIC: &[u8; 6] = b"AAENC1";
const NONCE_LEN: usize = 16;
const TAG_LEN: usize = 32;
const MIN_KEY_MATERIAL_LEN: usize = 16;

/// Derived symmetric encryption & MAC keys. Never prints or debugs key material.
#[derive(Clone)]
pub struct MasterKey {
    enc_key: [u8; 32],
    mac_key: [u8; 32],
}

impl fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MasterKey")
            .field("enc_key", &"[redacted]")
            .field("mac_key", &"[redacted]")
            .finish()
    }
}

impl Drop for MasterKey {
    fn drop(&mut self) {
        self.enc_key.fill(0);
        self.mac_key.fill(0);
    }
}

impl MasterKey {
    /// Derive a [`MasterKey`] from raw secret bytes (>= 16 bytes).
    pub fn from_bytes(secret: &[u8]) -> Result<Self, Error> {
        if secret.len() < MIN_KEY_MATERIAL_LEN {
            return Err(Error::Message(format!(
                "master key material must be at least {MIN_KEY_MATERIAL_LEN} bytes"
            )));
        }
        let enc_key = domain_hash(b"anyagents-aead-enc-v1", secret);
        let mac_key = domain_hash(b"anyagents-aead-mac-v1", secret);
        Ok(Self { enc_key, mac_key })
    }

    /// Derive a [`MasterKey`] from a UTF-8 passphrase or hex secret string.
    pub fn from_secret_str(secret: &str) -> Result<Self, Error> {
        let trimmed = secret.trim();
        if trimmed.len() < MIN_KEY_MATERIAL_LEN {
            return Err(Error::Message(format!(
                "master key must be at least {MIN_KEY_MATERIAL_LEN} characters"
            )));
        }
        Self::from_bytes(trimmed.as_bytes())
    }

    /// Load the deployment master key from `ANYAGENTS_MASTER_KEY` (or a
    /// domain-separated derivation of `ANYAGENTS_CAPABILITY_KEY` when present).
    /// Returns `Ok(None)` when neither variable is configured.
    pub fn from_env() -> Result<Option<Self>, Error> {
        if let Ok(raw) = std::env::var(MASTER_KEY_ENV) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Self::from_secret_str(trimmed).map(Some);
            }
        }
        if storage_env_enabled()
            && let Ok(cap) = std::env::var(CAPABILITY_KEY_ENV)
        {
            let trimmed = cap.trim();
            if trimmed.len() >= 32 {
                let derived =
                    domain_hash(b"anyagents-master-from-capability-v1", trimmed.as_bytes());
                return Self::from_bytes(&derived).map(Some);
            }
        }
        Ok(None)
    }

    /// Require a configured deployment master key, failing closed if absent.
    pub fn require_from_env() -> Result<Self, Error> {
        Self::from_env()?.ok_or_else(|| {
            Error::Message(format!(
                "encryption key required: set {MASTER_KEY_ENV} (at least {MIN_KEY_MATERIAL_LEN} chars)"
            ))
        })
    }

    /// Encrypt `plaintext` bound to associated data `aad`.
    pub fn encrypt(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let nonce = random_nonce()?;
        let mut ciphertext = Vec::with_capacity(plaintext.len());
        xor_keystream(&self.enc_key, &nonce, aad, plaintext, &mut ciphertext);
        let tag = compute_tag(&self.mac_key, &nonce, aad, &ciphertext);

        let mut out = Vec::with_capacity(MAGIC.len() + NONCE_LEN + ciphertext.len() + TAG_LEN);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        out.extend_from_slice(&tag);
        Ok(out)
    }

    /// Authenticate and decrypt `blob` bound to `aad`. Fails closed on any tamper or key mismatch.
    pub fn decrypt(&self, aad: &[u8], blob: &[u8]) -> Result<Vec<u8>, Error> {
        let min_len = MAGIC.len() + NONCE_LEN + TAG_LEN;
        if blob.len() < min_len || &blob[..MAGIC.len()] != MAGIC {
            return Err(Error::Message(
                "invalid encrypted blob envelope or unsupported version".into(),
            ));
        }
        let nonce: [u8; NONCE_LEN] = blob[MAGIC.len()..MAGIC.len() + NONCE_LEN]
            .try_into()
            .map_err(|_| Error::Message("invalid encrypted blob nonce".into()))?;
        let ct_start = MAGIC.len() + NONCE_LEN;
        let ct_end = blob.len() - TAG_LEN;
        let ciphertext = &blob[ct_start..ct_end];
        let tag = &blob[ct_end..];

        let expected = compute_tag(&self.mac_key, &nonce, aad, ciphertext);
        if !ct_eq(&expected, tag) {
            return Err(Error::Message(
                "encrypted blob authentication failed (wrong key, wrong AAD, or tampered bytes)"
                    .into(),
            ));
        }
        let mut plaintext = Vec::with_capacity(ciphertext.len());
        xor_keystream(&self.enc_key, &nonce, aad, ciphertext, &mut plaintext);
        Ok(plaintext)
    }
}

fn domain_hash(domain: &[u8], data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update((domain.len() as u64).to_be_bytes());
    h.update(domain);
    h.update((data.len() as u64).to_be_bytes());
    h.update(data);
    h.finalize().into()
}

fn random_nonce() -> Result<[u8; NONCE_LEN], Error> {
    // uuid::Uuid::new_v4 draws 122 bits from OS CSPRNG; combine two v4 UUIDs
    // via SHA-256 to obtain 128 uniform CSPRNG bits without platform-specific syscalls.
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let mut h = Sha256::new();
    h.update(b"anyagents-nonce-v1");
    h.update(a.as_bytes());
    h.update(b.as_bytes());
    let digest = h.finalize();
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&digest[..NONCE_LEN]);
    Ok(nonce)
}

fn xor_keystream(
    enc_key: &[u8; 32],
    nonce: &[u8; NONCE_LEN],
    aad: &[u8],
    input: &[u8],
    out: &mut Vec<u8>,
) {
    let aad_hash: [u8; 32] = Sha256::digest(aad).into();
    let mut counter: u64 = 0;
    for chunk in input.chunks(32) {
        let mut h = Sha256::new();
        h.update(b"anyagents-ctr-v1");
        h.update(enc_key);
        h.update(nonce);
        h.update(aad_hash);
        h.update(counter.to_be_bytes());
        let block = h.finalize();
        for (b, k) in chunk.iter().zip(block.iter()) {
            out.push(b ^ k);
        }
        counter = counter.wrapping_add(1);
    }
}

fn hmac_sha256(key: &[u8; 32], parts: &[&[u8]]) -> [u8; 32] {
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..32 {
        ipad[i] ^= key[i];
        opad[i] ^= key[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    for part in parts {
        inner.update(part);
    }
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn compute_tag(mac_key: &[u8; 32], nonce: &[u8; NONCE_LEN], aad: &[u8], ct: &[u8]) -> [u8; 32] {
    let aad_len = (aad.len() as u64).to_be_bytes();
    let ct_len = (ct.len() as u64).to_be_bytes();
    hmac_sha256(mac_key, &[MAGIC, nonce, &aad_len, aad, &ct_len, ct])
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip_and_tamper_resistance() {
        let key = MasterKey::from_secret_str("0123456789abcdef0123456789abcdef").unwrap();
        let other = MasterKey::from_secret_str("fedcba9876543210fedcba9876543210").unwrap();
        let aad = b"agent_files:agt_1:secrets:token";
        let pt = b"super-secret-value-12345";

        let ct1 = key.encrypt(aad, pt).unwrap();
        let ct2 = key.encrypt(aad, pt).unwrap();
        assert_ne!(
            ct1, ct2,
            "distinct nonces must produce distinct ciphertexts"
        );
        assert!(!ct1.windows(pt.len()).any(|w| w == pt));

        assert_eq!(key.decrypt(aad, &ct1).unwrap(), pt);
        assert_eq!(key.decrypt(aad, &ct2).unwrap(), pt);

        // Wrong key fails closed
        assert!(other.decrypt(aad, &ct1).is_err());
        // Wrong AAD fails closed
        assert!(
            key.decrypt(b"agent_files:agt_2:secrets:token", &ct1)
                .is_err()
        );
        // Bit flip fails closed
        let mut tampered = ct1.clone();
        let mid = MAGIC.len() + NONCE_LEN + 2;
        tampered[mid] ^= 0x01;
        assert!(key.decrypt(aad, &tampered).is_err());

        // Debug never leaks key bytes
        let dbg = format!("{key:?}");
        assert!(dbg.contains("[redacted]"));
    }
}
