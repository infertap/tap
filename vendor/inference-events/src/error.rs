// SPDX-License-Identifier: Apache-2.0
//! Machine-readable validation failures without input values.

/// The constraint that rejected a record or projected field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// A required field is absent.
    MissingField,
    /// A value has the wrong JSON type.
    IncorrectType,
    /// A value differs from the required constant.
    UnexpectedConstant,
    /// An integer exceeds the field's bounds.
    OutOfRange,
    /// Fields form a prohibited combination.
    ForbiddenCombination,
    /// A range ends before it starts.
    InvalidOrder,
    /// A producer attempted to emit an undefined record kind.
    UnknownKind,
    /// An extension duplicates a typed field or the record discriminator.
    ExtensionCollision,
    /// A structurally checked value cannot be represented by the generated type.
    InvalidRepresentation,
    /// Serialization failed.
    Serialization,
    /// The embedded schema contains an unresolved reference.
    InvalidSchema,
}

/// A validation failure with a field path and a stable category.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    /// Dotted field path with bracketed array indices.
    pub path: String,
    /// The violated constraint category.
    pub kind: ErrorKind,
}
impl ValidationError {
    pub(crate) fn new(path: &str, kind: ErrorKind) -> Self {
        Self {
            path: path.into(),
            kind,
        }
    }
}
impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self.kind {
            ErrorKind::MissingField => "required field absent",
            ErrorKind::IncorrectType => "incorrect type",
            ErrorKind::UnexpectedConstant => "unexpected constant",
            ErrorKind::OutOfRange => "integer outside allowed range",
            ErrorKind::ForbiddenCombination => "forbidden field combination",
            ErrorKind::InvalidOrder => "range end precedes its start",
            ErrorKind::UnknownKind => "unknown producer record kind",
            ErrorKind::ExtensionCollision => "extension duplicates a reserved field",
            ErrorKind::InvalidRepresentation => "invalid typed representation",
            ErrorKind::Serialization => "serialization failed",
            ErrorKind::InvalidSchema => "unresolved schema reference",
        };
        write!(f, "{}: {reason}", self.path)
    }
}
impl std::error::Error for ValidationError {}
