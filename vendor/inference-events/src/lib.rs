// SPDX-License-Identifier: Apache-2.0
//! Shared JSON wire records and structural validation for inference telemetry.
//!
//! Decode through [`Record::decode`] to enforce schema constraints. Generated structures
//! describe field layouts; cross-record lifecycle, provenance and identity obligations
//! remain the responsibility of producers and readers. Unknown fields and kinds survive
//! decoding. Projected columnar reads can validate individual fields with [`validate_field`].

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

mod error;
pub use error::{ErrorKind, ValidationError};
mod generated;
pub use generated::*;
use serde_json::{Map, Value};
use std::sync::OnceLock;

/// The authoritative JSON Schema bundled with this model version.
pub const SCHEMA_JSON: &str = include_str!("../schema/records.schema.json");
/// Physical column definitions generated from the same schema.
pub const COLUMN_SCHEMA_JSON: &str = include_str!("../schema/columns.json");

/// Preserve omission separately from explicit null in nullable optional fields.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Presence<T> {
    /// The field was omitted.
    #[default]
    Absent,
    /// The field was explicitly null.
    Null,
    /// The field carried a value.
    Value(T),
}
impl<T> Presence<T> {
    /// Whether serialization should omit this field.
    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }
}
impl<T: serde::Serialize> serde::Serialize for Presence<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Value(value) => value.serialize(serializer),
            _ => serializer.serialize_none(),
        }
    }
}
impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Presence<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<T>::deserialize(deserializer).map(|value| value.map_or(Self::Null, Self::Value))
    }
}

fn error(path: &str, kind: ErrorKind) -> ValidationError {
    ValidationError::new(path, kind)
}

fn check_extensions(
    extensions: &Map<String, Value>,
    reserved: &[&str],
    path: &str,
) -> Result<(), ValidationError> {
    for field in reserved {
        if extensions.contains_key(*field) {
            return Err(error(
                &format!("{path}.{field}"),
                ErrorKind::ExtensionCollision,
            ));
        }
    }
    Ok(())
}

fn schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA
        .get_or_init(|| serde_json::from_str(SCHEMA_JSON).expect("generated schema is valid JSON"))
}

/// A known typed record or an unchanged future record kind.
#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    /// A generated record defined by this schema.
    Known(Box<KnownRecord>),
    /// A future kind preserved with its common envelope.
    Unknown(Map<String, Value>),
}
impl Record {
    /// Validate one complete JSON record before constructing typed fields.
    pub fn decode(mut value: Value) -> Result<Self, ValidationError> {
        validate_record(&value)?;
        let object = value
            .as_object()
            .ok_or_else(|| error("record", ErrorKind::IncorrectType))?;
        let kind = object["kind"]
            .as_str()
            .ok_or_else(|| error("kind", ErrorKind::IncorrectType))?;
        if !RECORD_KINDS.contains(&kind) {
            return Ok(Self::Unknown(object.clone()));
        }
        let definition = &schema()["$defs"][kind];
        normalize_integers(&mut value, definition);
        serde_json::from_value(value)
            .map(|record| Self::Known(Box::new(record)))
            .map_err(|_| error("record", ErrorKind::InvalidRepresentation))
    }
    /// Serialize and validate the complete wire record, including caller modifications.
    pub fn to_value(&self) -> Result<Value, ValidationError> {
        let value = match self {
            Self::Known(record) => {
                record.check_extensions()?;
                serde_json::to_value(record)
                    .map_err(|_| error("record", ErrorKind::Serialization))?
            }
            Self::Unknown(record) => Value::Object(record.clone()),
        };
        validate_record(&value)?;
        Ok(value)
    }
}

/// Validate a reader record: common envelope, known structure, and record-local semantics.
/// Future kinds are accepted. This does not validate stream ordering or lifecycle history.
pub fn validate_record(value: &Value) -> Result<(), ValidationError> {
    let object = value
        .as_object()
        .ok_or_else(|| error("record", ErrorKind::IncorrectType))?;
    let kind = object
        .get("kind")
        .ok_or_else(|| error("kind", ErrorKind::MissingField))?
        .as_str()
        .ok_or_else(|| error("kind", ErrorKind::IncorrectType))?;
    let timestamp = object
        .get("at_ms")
        .ok_or_else(|| error("at_ms", ErrorKind::MissingField))?;
    if !timestamp.is_number() {
        return Err(error("at_ms", ErrorKind::IncorrectType));
    }
    if let Some(definition) = schema()["$defs"]
        .get(kind)
        .filter(|_| RECORD_KINDS.contains(&kind))
    {
        check(value, definition, "record")?;
    }
    if kind == "identity_refused" {
        let start = object.get("window_start_ms").and_then(Value::as_f64);
        let end = object.get("window_end_ms").and_then(Value::as_f64);
        if matches!((start,end), (Some(a),Some(b)) if a > b) {
            return Err(error("window_end_ms", ErrorKind::InvalidOrder));
        }
    }
    if kind == "segments_dropped" {
        let first = object.get("first_seq").and_then(integer_value);
        let last = object.get("last_seq").and_then(integer_value);
        if matches!((first,last), (Some(a),Some(b)) if a > b) {
            return Err(error("last_seq", ErrorKind::InvalidOrder));
        }
    }
    Ok(())
}

/// Validate a producer record, rejecting kinds not defined by this schema.
/// Includes the same record-local semantic checks as [`validate_record`].
pub fn validate_producer_record(value: &Value) -> Result<(), ValidationError> {
    validate_record(value)?;
    let kind = value["kind"].as_str().expect("validated discriminator");
    if !RECORD_KINDS.contains(&kind) {
        return Err(error("kind", ErrorKind::UnknownKind));
    }
    Ok(())
}

/// Validate a projected field without requiring columns the reader did not request.
/// Unknown fields remain available to the reader's schema-evolution policy.
pub fn validate_field(kind: &str, field: &str, value: &Value) -> Result<(), ValidationError> {
    if !RECORD_KINDS.contains(&kind) {
        return Ok(());
    }
    if let Some(definition) = schema()["$defs"][kind]["properties"].get(field) {
        check(value, definition, field)?;
    }
    Ok(())
}

fn check(value: &Value, rule: &Value, path: &str) -> Result<(), ValidationError> {
    if let Some(reference) = rule.get("$ref").and_then(Value::as_str) {
        let definition = schema()
            .pointer(reference.strip_prefix('#').unwrap_or(reference))
            .ok_or_else(|| error(path, ErrorKind::InvalidSchema))?;
        return check(value, definition, path);
    }
    if let Some(constant) = rule.get("const") {
        if constant != value {
            return Err(error(path, ErrorKind::UnexpectedConstant));
        }
    }
    if let Some(ty) = rule.get("type") {
        let accepts = |ty: &str| match ty {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "integer" => integer_value(value).is_some(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => false,
        };
        let valid = ty.as_str().map(&accepts).unwrap_or_else(|| {
            ty.as_array()
                .is_some_and(|types| types.iter().filter_map(Value::as_str).any(accepts))
        });
        if !valid {
            return Err(error(path, ErrorKind::IncorrectType));
        }
    }
    if let Some(n) = integer_value(value) {
        if rule
            .get("minimum")
            .and_then(Value::as_i64)
            .is_some_and(|min| n < i128::from(min))
            || rule
                .get("maximum")
                .and_then(Value::as_i64)
                .is_some_and(|max| n > i128::from(max))
        {
            return Err(error(path, ErrorKind::OutOfRange));
        }
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = rule.get("required").and_then(Value::as_array) {
            for field in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(field) {
                    return Err(error(&format!("{path}.{field}"), ErrorKind::MissingField));
                }
            }
        }
        if let Some(properties) = rule.get("properties").and_then(Value::as_object) {
            for (field, definition) in properties {
                if let Some(v) = object.get(field) {
                    check(v, definition, &format!("{path}.{field}"))?;
                }
            }
        }
    }
    if let (Some(items), Some(definition)) = (value.as_array(), rule.get("items")) {
        for (i, item) in items.iter().enumerate() {
            check(item, definition, &format!("{path}[{i}]"))?;
        }
    }
    if let Some(rules) = rule.get("allOf").and_then(Value::as_array) {
        for child in rules {
            check(value, child, path)?;
        }
    }
    if let Some(condition) = rule.get("if") {
        if check(value, condition, path).is_ok() {
            if let Some(then) = rule.get("then") {
                check(value, then, path)?;
            }
        }
    }
    if let Some(negative) = rule.get("not") {
        if check(value, negative, path).is_ok() {
            return Err(error(path, ErrorKind::ForbiddenCombination));
        }
    }
    Ok(())
}

// JSON Schema treats 1.0 as an integer. Normalize exact whole numbers before typed decode.
fn integer_value(value: &Value) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
        .or_else(|| {
            let value = value.as_f64()?;
            (value.fract() == 0.0 && value >= i64::MIN as f64 && value < 9223372036854775808.0)
                .then_some(value as i128)
        })
}
fn normalize_integers(value: &mut Value, rule: &Value) {
    if let Some(reference) = rule.get("$ref").and_then(Value::as_str) {
        if let Some(rule) = schema().pointer(reference.trim_start_matches('#')) {
            normalize_integers(value, rule);
        }
        return;
    }
    if rule["type"] == "integer"
        || rule["type"]
            .as_array()
            .is_some_and(|types| types.iter().any(|ty| ty == "integer"))
    {
        if let Some(number) = integer_value(value).and_then(|n| i64::try_from(n).ok()) {
            *value = Value::from(number);
        }
    }
    if let (Some(object), Some(properties)) = (
        value.as_object_mut(),
        rule.get("properties").and_then(Value::as_object),
    ) {
        for (field, definition) in properties {
            if let Some(value) = object.get_mut(field) {
                normalize_integers(value, definition);
            }
        }
    }
    if let (Some(items), Some(definition)) = (value.as_array_mut(), rule.get("items")) {
        for value in items {
            normalize_integers(value, definition);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn signed_maximum_rejects_values_above_a_negative_bound() {
        let rule = json!({"type":"integer", "minimum":-10, "maximum":-1});
        assert!(check(&json!(-5), &rule, "field").is_ok());
        assert_eq!(
            check(&json!(0), &rule, "field").unwrap_err().kind,
            ErrorKind::OutOfRange
        );
    }

    #[test]
    fn nullable_integers_normalize_whole_numbers_without_changing_null() {
        let rule = json!({"type":["integer", "null"]});
        let mut value = json!(3.0);
        normalize_integers(&mut value, &rule);
        assert_eq!(value.as_i64(), Some(3));
        let mut null = Value::Null;
        normalize_integers(&mut null, &rule);
        assert!(null.is_null());
    }
}
