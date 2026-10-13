// SPDX-License-Identifier: Apache-2.0
//! Keyed identity mapping using HMAC-SHA256.
//!
//! ```text
//! pseudonym = HMAC-SHA256(key, canonical_input)[..16]
//! canonical_input = context || 0x00 || epoch:u32-be || 0x00 || raw_id
//! ```
//!
//! Engine IDs use eight input bytes; content IDs use sixteen. Versioned contexts
//! separate identity domains. Outputs are 128-bit values encoded as lowercase hex.
//! The constructions are checked against `conformance/pseudonym/`.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Domain separation for the engine's block id. The `:v1` in every context means a
/// construction change is a new context, never a silent reinterpretation of the same one.
pub const BLOCK_ID_CONTEXT: &str = "infertap:block-id:v1";

/// Domain separation for content identities and engine block identities.
///
/// Consumers compare these identity types in separate namespaces. Engine hashes
/// can depend on process-local seeds; content identities use a separate derivation.
/// This context separates their HMAC inputs even when their numeric values match.
pub const CONTENT_ID_CONTEXT: &str = "infertap:content-id:v2";

/// Domain separation for the fleet canary; see [`canary`].
pub const CANARY_CONTEXT: &str = "infertap:canary:v1";

/// Domain separation for the partition dimension: the engine's `extra_keys`, where
/// `cache_salt` arrives. Operator-controlled text that routinely names a tenant, so it
/// never crosses the host boundary in the clear.
pub const PARTITION_CONTEXT: &str = "infertap:partition:v1";

/// Domain separation for the `LoRA` adapter name. The same class of data as the partition
/// under a different context, so the same string in the two dimensions yields different
/// pseudonyms and a consumer can never conflate them.
pub const LORA_NAME_CONTEXT: &str = "infertap:lora-name:v1";

/// Domain separation for engine-emitted cache backend identifiers.
pub const BACKEND_ID_CONTEXT: &str = "infertap:backend-id:v1";

/// Retained pseudonym width in bytes (128 bits). The full 256-bit MAC may be stored, but 128
/// is the floor and the conformance vectors encode the 128-bit form, so it is the contract.
pub const PSEUDONYM_BYTES: usize = 16;

/// Required MAC key length in bytes.
pub const KEY_BYTES: usize = 32;

/// Bytes of canary nonce. MAC'ing a *secret* nonce, rather than a fixed public constant, is
/// what keeps the key-consistency canary from being a verification oracle: an attacker holding
/// records cannot test a candidate key without also holding the nonce, and anyone holding the
/// nonce holds the key file already.
pub const NONCE_BYTES: usize = 32;

/// Exact key-file length: [`KEY_BYTES`] of MAC key followed by [`NONCE_BYTES`] of canary nonce.
///
/// Reject truncated material so both the MAC key and canary nonce are present.
pub const KEY_FILE_BYTES: usize = KEY_BYTES + NONCE_BYTES;

/// A validated key file: MAC key plus canary nonce, each exactly its declared width.
#[derive(Clone)]
pub struct Key {
    mac: [u8; KEY_BYTES],
    nonce: [u8; NONCE_BYTES],
}

/// Overwrite key material with zeros.
///
/// Best effort without `unsafe`. The fill removes the ordinary case of key bytes lingering
/// in freed heap for the life of the process. The compiler fence orders the store against
/// later memory operations but does not by itself prevent dead-store elimination, so this
/// is not a guarantee against every optimizer. Public so a caller that decoded a key from
/// text can zero the text the same way.
pub fn zero(bytes: &mut [u8]) {
    bytes.fill(0);
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
}

impl Drop for Key {
    /// Both halves are zeroed on drop, every clone included: the nonce is as secret as the
    /// key, because the canary MAC'd over it must not become an oracle.
    fn drop(&mut self) {
        zero(&mut self.mac);
        zero(&mut self.nonce);
    }
}

/// Why key material was refused. Never carries the material itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyError {
    /// Not exactly [`KEY_FILE_BYTES`] of decoded material; `got` is what arrived.
    WrongLength {
        /// The decoded byte count that arrived.
        got: usize,
    },
    /// The MAC key and the canary nonce are byte-identical: what `openssl rand -hex 32`
    /// pasted twice produces, and the provisioning mistake the canary exists to catch.
    IdenticalHalves,
    /// The text decodes as neither hex nor standard base64.
    NotHexOrBase64,
}

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongLength { got } => write!(
                f,
                "key file is {got} bytes, need exactly {KEY_FILE_BYTES} \
                 ({KEY_BYTES}-byte HMAC key + {NONCE_BYTES}-byte canary nonce). \
                 Generate with: openssl rand -hex {KEY_FILE_BYTES}"
            ),
            Self::IdenticalHalves => write!(
                f,
                "key file's two halves are identical; the nonce must be independent key \
                 material, not a copy of the key"
            ),
            Self::NotHexOrBase64 => write!(f, "key is neither valid hex nor base64"),
        }
    }
}

impl std::error::Error for KeyError {}

impl Key {
    /// Split raw key-file bytes into MAC key and canary nonce.
    ///
    /// # Errors
    /// [`KeyError::WrongLength`] or [`KeyError::IdenticalHalves`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, KeyError> {
        if bytes.len() != KEY_FILE_BYTES {
            return Err(KeyError::WrongLength { got: bytes.len() });
        }
        let (mac, nonce) = bytes.split_at(KEY_BYTES);
        if mac == nonce {
            return Err(KeyError::IdenticalHalves);
        }
        let (Ok(mac), Ok(nonce)) = (
            <[u8; KEY_BYTES]>::try_from(mac),
            <[u8; NONCE_BYTES]>::try_from(nonce),
        ) else {
            return Err(KeyError::WrongLength { got: bytes.len() });
        };
        Ok(Self { mac, nonce })
    }

    /// The canary nonce. MAC'd under this key to produce a value every instance provisioned
    /// from the same key file emits identically, so a mis-provisioned node is named exactly
    /// rather than inferred from a metric being low.
    #[must_use]
    pub fn canary_nonce(&self) -> &[u8] {
        &self.nonce
    }

    /// Parse a key from hex or standard base64 (hex first, then base64).
    ///
    /// # Errors
    /// [`KeyError::NotHexOrBase64`], or whatever [`Key::from_bytes`] refuses.
    pub fn from_hex_or_base64(s: &str) -> Result<Self, KeyError> {
        use base64::Engine;
        let s = s.trim();
        let mut bytes = hex::decode(s)
            .or_else(|_| base64::engine::general_purpose::STANDARD.decode(s))
            .map_err(|_| KeyError::NotHexOrBase64)?;
        let key = Self::from_bytes(&bytes);
        // The decoded copy is zeroed whether or not it was accepted; the caller owns the
        // text it decoded from and zeroes that itself.
        zero(&mut bytes);
        key
    }
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print key or nonce bytes: the nonce's secrecy is what keeps the canary from
        // being an oracle, so it is as sensitive as the key.
        write!(
            f,
            "Key({KEY_BYTES} bytes key + {NONCE_BYTES} bytes nonce, redacted)"
        )
    }
}

/// The pseudonym for `raw_id` under `(key, context, epoch)`: the pinned
/// `HMAC-SHA256(key, context || 0x00 || epoch:u32-be || 0x00 || raw_id:u64-be)[..16]`, hex.
///
/// # Panics
/// Never: HMAC-SHA256 accepts a key of any length, so the internal `expect` cannot fire.
#[must_use]
pub fn pseudonym(key: &Key, context: &str, epoch: u32, raw_id: u64) -> String {
    tag(key, context, epoch, &raw_id.to_be_bytes())
}

/// The one construction both pseudonyms share:
/// `HMAC-SHA256(key, context || 0x00 || epoch:u32-be || 0x00 || last)[..16]`, hex. The
/// fixed-width fields come first and the variable-length one last, so there is nothing to
/// canonicalize and no delimiter that can collide.
fn tag(key: &Key, context: &str, epoch: u32, last: &[u8]) -> String {
    #[allow(clippy::expect_used)] // HMAC-SHA256 accepts a key of any length; this cannot fire
    let mut mac = HmacSha256::new_from_slice(&key.mac).expect("HMAC accepts any key length");
    mac.update(context.as_bytes());
    mac.update(&[0]);
    mac.update(&epoch.to_be_bytes());
    mac.update(&[0]);
    mac.update(last);
    let mac = mac.finalize().into_bytes();
    hex::encode(&mac[..PSEUDONYM_BYTES])
}

/// The pseudonym for a variable-length string dimension under `(key, context, epoch)`:
/// `HMAC-SHA256(key, context || 0x00 || epoch:u32-be || 0x00 || raw_utf8)[..16]`, hex.
///
/// The variable-length field is last, so no delimiter or canonicalization is needed.
/// Equal inputs within a key, context, and epoch produce equal pseudonyms.
///
/// # Panics
/// HMAC-SHA256 accepts any key length; the internal construction cannot fail.
#[must_use]
pub fn pseudonym_str(key: &Key, context: &str, epoch: u32, raw: &str) -> String {
    tag(key, context, epoch, raw.as_bytes())
}

/// Derive a key-file canary for comparing pseudonymization configuration.
///
/// The same key file and epoch produce the same canary. Consumers use it to detect
/// split key provisioning before comparing pseudonyms across instances. Equality
/// is subject to the collision probability of a 128-bit tag.
///
/// The secret nonce from the key file prevents precomputation from a guessed key
/// alone. Including the epoch changes the canary on rotation.
#[must_use]
pub fn canary(key: &Key, epoch: u32) -> String {
    pseudonym_str(key, CANARY_CONTEXT, epoch, &hex::encode(key.canary_nonce()))
}

/// Pseudonymize a full-width content identity under its dedicated construction context.
#[must_use]
pub fn pseudonym_content(key: &Key, epoch: u32, raw_id: u128) -> String {
    tag(key, CONTENT_ID_CONTEXT, epoch, &raw_id.to_be_bytes())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{
        pseudonym, pseudonym_str, Key, KeyError, BLOCK_ID_CONTEXT, LORA_NAME_CONTEXT,
        PARTITION_CONTEXT,
    };

    // The labeled test-only key from conformance/pseudonym: 64 ASCII bytes, key then nonce.
    fn test_key() -> Key {
        Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")
            .expect("32-byte key")
    }

    #[test]
    fn matches_a_pinned_vector() {
        // context block-id:v1, epoch 0, raw 1 -> the value in conformance/pseudonym/vectors.json
        assert_eq!(
            pseudonym(&test_key(), BLOCK_ID_CONTEXT, 0, 1),
            "db83955d1a485fe98364fd9e47c8bc08"
        );
    }

    #[test]
    fn rotation_changes_the_pseudonym() {
        let k = test_key();
        assert_ne!(
            pseudonym(&k, BLOCK_ID_CONTEXT, 0, 1),
            pseudonym(&k, BLOCK_ID_CONTEXT, 1, 1)
        );
    }

    #[test]
    fn context_separates_domains() {
        let k = test_key();
        assert_ne!(
            pseudonym(&k, BLOCK_ID_CONTEXT, 0, 1),
            pseudonym(&k, "infertap:other-purpose:v1", 0, 1)
        );
    }

    #[test]
    fn debug_output_never_contains_key_bytes() {
        let dbg = format!("{:?}", test_key());
        assert!(dbg.contains("redacted"));
        assert!(!dbg.contains("infertap-conformance")); // the ASCII bytes
        assert!(!dbg.contains("696e666572746170")); // their hex
    }

    #[test]
    fn a_key_file_that_is_not_exactly_64_bytes_is_rejected() {
        assert!(Key::from_bytes(b"too-short").is_err());
        assert!(Key::from_hex_or_base64("abcd").is_err());
        // A 32-byte file would otherwise yield a working key and no canary: a detector
        // silently absent, which is what the canary exists to prevent.
        assert!(Key::from_bytes(&[7u8; 32]).is_err());
        assert!(Key::from_bytes(&[7u8; 63]).is_err());
        assert!(Key::from_bytes(&[7u8; 65]).is_err());
    }

    #[test]
    fn identical_halves_are_rejected() {
        // `openssl rand -hex 32` pasted twice. It would work, it would look fine, and it is
        // exactly the provisioning mistake the canary is meant to catch.
        let half = vec![9u8; 32];
        let mut doubled = half.clone();
        doubled.extend_from_slice(&half);
        let err = Key::from_bytes(&doubled).expect_err("must refuse");
        assert_eq!(err, KeyError::IdenticalHalves);
        assert!(err.to_string().contains("identical"), "got {err}");
        assert_eq!(
            Key::from_bytes(&half).expect_err("short"),
            KeyError::WrongLength { got: 32 }
        );
        assert_eq!(
            Key::from_hex_or_base64("not hex, not base64!").expect_err("garbage"),
            KeyError::NotHexOrBase64
        );
    }

    #[test]
    fn the_nonce_is_carried_and_never_printed() {
        let k = test_key();
        assert_eq!(k.canary_nonce().len(), 32);
        assert_ne!(k.canary_nonce(), &b"infertap-conformance-test-key!!!"[..]);
        let dbg = format!("{k:?}");
        assert!(dbg.contains("redacted"));
        assert!(
            !dbg.contains("nonce!!!"),
            "the nonce is as sensitive as the key"
        );
    }

    #[test]
    fn string_dimensions_are_deterministic_and_domain_separated() {
        let k = test_key();
        let a = pseudonym_str(&k, PARTITION_CONTEXT, 0, "tenant-A");
        assert_eq!(a, pseudonym_str(&k, PARTITION_CONTEXT, 0, "tenant-A")); // stable
        assert_eq!(a.len(), 32);
        assert_ne!(a, pseudonym_str(&k, PARTITION_CONTEXT, 0, "tenant-B")); // distinguishes
        assert_ne!(a, pseudonym_str(&k, PARTITION_CONTEXT, 1, "tenant-A")); // epoch rotates
        assert_ne!(a, pseudonym_str(&k, LORA_NAME_CONTEXT, 0, "tenant-A")); // domains separate
        assert!(!a.contains("tenant")); // and the input never survives
    }

    #[test]
    fn output_is_128_bits() {
        assert_eq!(pseudonym(&test_key(), BLOCK_ID_CONTEXT, 0, 0).len(), 32); // 16 bytes hex
    }

    #[test]
    fn the_key_owns_its_bytes_and_a_clone_is_independent() {
        // Zeroing on drop is per instance, so a clone must not share a buffer with the
        // original: dropping one leaves the other usable, and both zero their own copy.
        let key = test_key();
        let twin = key.clone();
        let before = pseudonym(&key, BLOCK_ID_CONTEXT, 0, 42);
        drop(key);
        assert_eq!(pseudonym(&twin, BLOCK_ID_CONTEXT, 0, 42), before);
        let mut buf = vec![7u8; 16];
        super::zero(&mut buf);
        assert!(buf.iter().all(|b| *b == 0));
    }
}
