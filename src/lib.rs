// SPDX-License-Identifier: Apache-2.0
//! KV-cache event collection and pseudonymized record output.
//!
//! Adapters normalize engine telemetry and derive content identities before discarding
//! input token IDs. The compute stage converts events to records. Sinks apply output
//! controls and persist records for shipping.
//!
//! The inference-events revision in `SPEC_PIN` defines the emitted record contract.
//!
//! The binary, CLI/configuration, and record contract are the supported interfaces.
//! These Rust modules serve the binary and integration tests. They are not a supported
//! SDK and may change without compatibility guarantees; the crate is not published.

// Lint policy (unsafe forbidden, clippy pedantic) lives in Cargo.toml [lints]; every public
// item is documented, and CI denies a bare one.
#![warn(missing_docs)]

/// Declare a reference-counted string newtype with the conversions every label shares:
/// `as_str`, `From<&str>`, `From<String>`, `Display`, and `Serialize` as a plain string.
/// The caller adds the derives and any type-specific impls beside the invocation.
macro_rules! shared_text {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        pub struct $name(std::sync::Arc<str>);

        impl $name {
            /// The text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            fn from(text: &str) -> Self {
                Self(std::sync::Arc::from(text))
            }
        }

        impl From<String> for $name {
            fn from(text: String) -> Self {
                Self(std::sync::Arc::from(text))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&self.0)
            }
        }
    };
}

pub mod clock;
pub mod compute;
pub mod event_model;
pub mod lifecycle;
pub mod pseudonym;
pub mod record;
pub mod sink;
pub mod source;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
