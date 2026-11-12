use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub(crate) fn url_encode_query_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 8);
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Standard base64 encoding with padding (RFC 4648 section 4).
pub fn base64_encode(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

/// Base64 decoding with the permissive contract this crate has always accepted:
/// ASCII whitespace is ignored, the URL-safe alphabet (`-`/`_`) is accepted alongside
/// `+`/`/`, `=` contributes value 0, and the legacy per-chunk hole semantics are
/// preserved exactly: the first byte is emitted always, the byte under a mid-chunk
/// `=` is dropped, and any following non-`=` byte is emitted. Byte decoding itself is delegated to the standard `base64` crate via
/// per-chunk canonical re-encoding, so non-canonical inputs the standard crate alone
/// would reject (non-zero pad bits, padding in non-final chunks) decode exactly as
/// before.
pub fn base64_decode(encoded: &str) -> Result<Vec<u8>, String> {
    let clean: Vec<u8> = encoded
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    if clean.is_empty() {
        return Ok(Vec::new());
    }
    if !clean.len().is_multiple_of(4) {
        return Err("invalid base64 length".to_string());
    }

    fn value_of(b: u8) -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    }

    let mut out = Vec::with_capacity(clean.len() * 3 / 4);
    for chunk in clean.as_chunks::<4>().0 {
        let mut vals = [0u8; 4];
        for (i, &b) in chunk.iter().enumerate() {
            if b == b'=' {
                vals[i] = 0;
            } else {
                vals[i] = value_of(b).ok_or_else(|| format!("invalid base64 character: {b}"))?;
            }
        }
        let n = ((vals[0] as u32) << 18)
            | ((vals[1] as u32) << 12)
            | ((vals[2] as u32) << 6)
            | (vals[3] as u32);
        let bytes = [
            ((n >> 16) & 0xff) as u8,
            ((n >> 8) & 0xff) as u8,
            (n & 0xff) as u8,
        ];
        // Legacy hole semantics: the byte under a mid-chunk '=' is dropped, but a
        // following non-'=' byte is still emitted, e.g. "Zg=Z" -> [0x66, 0x19].
        let mut emit_buf = [0u8; 3];
        emit_buf[0] = bytes[0];
        let mut emit_len = 1;
        if chunk[2] != b'=' {
            emit_buf[emit_len] = bytes[1];
            emit_len += 1;
        }
        if chunk[3] != b'=' {
            emit_buf[emit_len] = bytes[2];
            emit_len += 1;
        }
        let canonical_chunk = STANDARD.encode(&emit_buf[..emit_len]);
        let decoded = STANDARD
            .decode(canonical_chunk)
            .expect("canonical chunk produced by STANDARD.encode must decode");
        out.extend_from_slice(&decoded);
    }
    Ok(out)
}
