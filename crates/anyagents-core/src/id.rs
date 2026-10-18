//! TypeIDs: a lowercase prefix, `_`, and a 26-character Crockford base32 UUIDv7.
//!
//! The suffix is the UUID in big-endian order with two zero bits on the left,
//! split into 26 groups of 5 bits. The first character is therefore at most `7`.

use uuid::Uuid;

use crate::error::Error;

const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Build the decode table once. 0xFF marks a byte that is not in the alphabet.
fn decode_table() -> [u8; 256] {
    let mut table = [0xFFu8; 256];
    for (index, byte) in ALPHABET.iter().enumerate() {
        table[*byte as usize] = index as u8;
    }
    table
}

fn encode(bytes: &[u8; 16]) -> String {
    let mut out = [0u8; 26];
    out[0] = ALPHABET[((bytes[0] & 224) >> 5) as usize];
    out[1] = ALPHABET[(bytes[0] & 31) as usize];
    out[2] = ALPHABET[((bytes[1] & 248) >> 3) as usize];
    out[3] = ALPHABET[(((bytes[1] & 7) << 2) | ((bytes[2] & 192) >> 6)) as usize];
    out[4] = ALPHABET[((bytes[2] & 62) >> 1) as usize];
    out[5] = ALPHABET[(((bytes[2] & 1) << 4) | ((bytes[3] & 240) >> 4)) as usize];
    out[6] = ALPHABET[(((bytes[3] & 15) << 1) | ((bytes[4] & 128) >> 7)) as usize];
    out[7] = ALPHABET[((bytes[4] & 124) >> 2) as usize];
    out[8] = ALPHABET[(((bytes[4] & 3) << 3) | ((bytes[5] & 224) >> 5)) as usize];
    out[9] = ALPHABET[(bytes[5] & 31) as usize];
    out[10] = ALPHABET[((bytes[6] & 248) >> 3) as usize];
    out[11] = ALPHABET[(((bytes[6] & 7) << 2) | ((bytes[7] & 192) >> 6)) as usize];
    out[12] = ALPHABET[((bytes[7] & 62) >> 1) as usize];
    out[13] = ALPHABET[(((bytes[7] & 1) << 4) | ((bytes[8] & 240) >> 4)) as usize];
    out[14] = ALPHABET[(((bytes[8] & 15) << 1) | ((bytes[9] & 128) >> 7)) as usize];
    out[15] = ALPHABET[((bytes[9] & 124) >> 2) as usize];
    out[16] = ALPHABET[(((bytes[9] & 3) << 3) | ((bytes[10] & 224) >> 5)) as usize];
    out[17] = ALPHABET[(bytes[10] & 31) as usize];
    out[18] = ALPHABET[((bytes[11] & 248) >> 3) as usize];
    out[19] = ALPHABET[(((bytes[11] & 7) << 2) | ((bytes[12] & 192) >> 6)) as usize];
    out[20] = ALPHABET[((bytes[12] & 62) >> 1) as usize];
    out[21] = ALPHABET[(((bytes[12] & 1) << 4) | ((bytes[13] & 240) >> 4)) as usize];
    out[22] = ALPHABET[(((bytes[13] & 15) << 1) | ((bytes[14] & 128) >> 7)) as usize];
    out[23] = ALPHABET[((bytes[14] & 124) >> 2) as usize];
    out[24] = ALPHABET[(((bytes[14] & 3) << 3) | ((bytes[15] & 224) >> 5)) as usize];
    out[25] = ALPHABET[(bytes[15] & 31) as usize];
    // Alphabet bytes are ASCII, so this cannot fail.
    String::from_utf8(out.to_vec()).expect("alphabet is ascii")
}

fn decode_suffix(suffix: &str) -> Result<[u8; 16], ()> {
    let bytes = suffix.as_bytes();
    if bytes.len() != 26 {
        return Err(());
    }
    let table = decode_table();
    let mut dec = [0u8; 26];
    for (index, byte) in bytes.iter().enumerate() {
        let value = table[*byte as usize];
        if value == 0xFF {
            return Err(());
        }
        dec[index] = value;
    }
    // Two padding bits sit at the top of the first symbol. Anything above 7
    // would not be a 128-bit value.
    if dec[0] > 7 {
        return Err(());
    }
    let mut out = [0u8; 16];
    out[0] = (dec[0] << 5) | dec[1];
    out[1] = (dec[2] << 3) | (dec[3] >> 2);
    out[2] = (dec[3] << 6) | (dec[4] << 1) | (dec[5] >> 4);
    out[3] = (dec[5] << 4) | (dec[6] >> 1);
    out[4] = (dec[6] << 7) | (dec[7] << 2) | (dec[8] >> 3);
    out[5] = (dec[8] << 5) | dec[9];
    out[6] = (dec[10] << 3) | (dec[11] >> 2);
    out[7] = (dec[11] << 6) | (dec[12] << 1) | (dec[13] >> 4);
    out[8] = (dec[13] << 4) | (dec[14] >> 1);
    out[9] = (dec[14] << 7) | (dec[15] << 2) | (dec[16] >> 3);
    out[10] = (dec[16] << 5) | dec[17];
    out[11] = (dec[18] << 3) | (dec[19] >> 2);
    out[12] = (dec[19] << 6) | (dec[20] << 1) | (dec[21] >> 4);
    out[13] = (dec[21] << 4) | (dec[22] >> 1);
    out[14] = (dec[22] << 7) | (dec[23] << 2) | (dec[24] >> 3);
    out[15] = (dec[24] << 5) | dec[25];
    Ok(out)
}

fn generate(prefix: &str) -> String {
    format!("{prefix}_{}", encode(Uuid::now_v7().as_bytes()))
}

fn parse_prefixed(prefix: &str, raw: &str) -> Result<String, Error> {
    let Some(suffix) = raw
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('_'))
    else {
        return Err(Error::InvalidId(raw.to_string()));
    };
    if decode_suffix(suffix).is_err() {
        return Err(Error::InvalidId(raw.to_string()));
    }
    Ok(raw.to_string())
}

macro_rules! identifier {
    ($name:ident, $prefix:literal) => {
        /// Owned TypeID. The string form is `prefix_` plus 26 Crockford characters.
        #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// New UUIDv7 TypeID with this prefix.
            pub fn generate() -> Self {
                Self(generate($prefix))
            }

            /// Parse and check the prefix and the suffix.
            pub fn parse(raw: &str) -> Result<Self, Error> {
                Ok(Self(parse_prefixed($prefix, raw)?))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

identifier!(AgentId, "agt");
identifier!(MessageId, "msg");
identifier!(ChannelId, "chn");
identifier!(UsageId, "usg");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vector_round_trips() {
        let uuid = Uuid::parse_str("01890a5d-ac96-774b-bcce-b302099a8057").unwrap();
        let suffix = encode(uuid.as_bytes());
        assert_eq!(suffix, "01h455vb4pex5vsknk084sn02q");
        assert_eq!(suffix.len(), 26);
        assert_eq!(decode_suffix(&suffix).unwrap(), *uuid.as_bytes());
    }

    #[test]
    fn extremes_round_trip() {
        for bytes in [[0u8; 16], [0xFF; 16]] {
            let suffix = encode(&bytes);
            assert_eq!(decode_suffix(&suffix).unwrap(), bytes);
            assert!(suffix.as_bytes()[0] <= b'7');
        }
    }

    #[test]
    fn generated_id_is_uuid_v7() {
        let id = AgentId::generate();
        let suffix = id.as_str().strip_prefix("agt_").unwrap();
        assert_eq!(suffix.len(), 26);
        let bytes = decode_suffix(suffix).unwrap();
        assert_eq!(bytes[6] >> 4, 0x7);
        assert_eq!(bytes[8] & 0b1100_0000, 0b1000_0000);
        assert_eq!(AgentId::parse(id.as_str()).unwrap(), id);
    }

    #[test]
    fn rejects_bad_suffixes() {
        assert!(AgentId::parse("msg_01h455vb4pex5vsknk084sn02q").is_err());
        assert!(AgentId::parse("agt_01h455vb4pex5vsknk084sn02").is_err());
        assert!(AgentId::parse("agt_01H455vb4pex5vsknk084sn02q").is_err());
        assert!(AgentId::parse("agt_8zzzzzzzzzzzzzzzzzzzzzzzzz").is_err());
        assert!(AgentId::parse("agt_01h455vb4pex5vsknk084sn02!").is_err());
        assert!(MessageId::parse("msg_01h455vb4pex5vsknk084sn02q").is_ok());
    }
}
