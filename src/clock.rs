// SPDX-License-Identifier: Apache-2.0
//! Engine and producer clocks as distinct types.
//!
//! Cache events carry the engine's timestamp. Records the producer emits about itself
//! carry the producer's wall clock. Contract section 2.6 forbids comparing the two, so
//! [`EngineMs`] and [`ProducerMs`] are separate types that cannot meet in a comparison
//! or a subtraction. A record that carries both, such as `holder_reset` or
//! `identity_refused`, names each field by its domain.

use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch on the engine's clock: the `at_ms` of every cache
/// event and of every record derived from one, and the window bounds of an identity
/// refusal. The adapter converts the engine's own unit at the boundary, so this is the
/// only engine time unit in the crate.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EngineMs(pub f64);

impl EngineMs {
    /// The earlier of two engine instants.
    #[must_use]
    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }
}

/// Milliseconds since the Unix epoch on the producer's own wall clock: the `at_ms` of
/// every record the producer emits about itself, and the instant it last heard a source.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProducerMs(pub f64);

impl ProducerMs {
    /// The producer clock now. One definition for the whole crate.
    #[must_use]
    pub fn now() -> Self {
        Self(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0.0, |d| d.as_secs_f64() * 1000.0),
        )
    }
}
