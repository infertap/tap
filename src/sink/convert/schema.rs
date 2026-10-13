// SPDX-License-Identifier: Apache-2.0
//! Derive the Parquet column and leaf plan from the pinned record schema.

use super::ConvertError;
use parquet::basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType};
use parquet::schema::types::{Type, TypePtr};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const SCHEMA_JSON: &str = inference_events::COLUMN_SCHEMA_JSON;

/// A primitive column type from the contract schema, spelled as the schema spells it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prim {
    /// A UTF-8 string.
    Utf8,
    /// A signed 64-bit integer.
    Int64,
    /// A 64-bit float.
    Float64,
    /// A boolean.
    Bool,
}

impl Prim {
    /// The schema's spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Utf8 => "utf8",
            Self::Int64 => "int64",
            Self::Float64 => "float64",
            Self::Bool => "bool",
        }
    }
}

impl std::fmt::Display for Prim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Prim {
    /// The type the schema spells as `ty`, or `None` for a spelling the contract lacks.
    pub(super) fn parse(ty: &str) -> Option<Self> {
        match ty {
            "utf8" => Some(Self::Utf8),
            "int64" => Some(Self::Int64),
            "float64" => Some(Self::Float64),
            "bool" => Some(Self::Bool),
            _ => None,
        }
    }
}

/// The physical shape of one column.
#[derive(Debug, Clone, Copy)]
pub(super) enum Shape {
    Prim(Prim),
    ListUtf8,
    /// The `endpoints` column: a list of the closed struct the schema declares.
    Endpoints,
    /// The `extra` map: everything the schema does not type.
    ExtraMap,
}

/// One field of the closed `endpoints` element.
#[derive(Debug)]
pub(super) struct ElemField {
    pub(super) name: String,
    pub(super) prim: Prim,
    pub(super) optional: bool,
}

/// One output column, with the index of its first leaf in the leaf plan.
#[derive(Debug)]
pub(super) struct Column {
    pub(super) name: String,
    pub(super) shape: Shape,
    pub(super) leaf: usize,
}

/// One physical leaf, in schema order.
#[derive(Debug)]
pub(super) struct LeafPlan {
    pub(super) prim: Prim,
    pub(super) repeated: bool,
}

/// Build the file schema: every column optional, lists three-level, `extra` a string map.
fn build_schema(columns: &[Column], element: &[ElemField]) -> Result<TypePtr, ConvertError> {
    let mut fields: Vec<TypePtr> = Vec::new();
    for col in columns {
        let t = match col.shape {
            Shape::Prim(prim) => leaf_type(&col.name, prim, Repetition::OPTIONAL)?,
            Shape::ListUtf8 => {
                let elem = leaf_type("element", Prim::Utf8, Repetition::REQUIRED)?;
                list_type(&col.name, Arc::new(elem))?
            }
            Shape::Endpoints => {
                let mut leaves: Vec<TypePtr> = Vec::new();
                for f in element {
                    let rep = if f.optional {
                        Repetition::OPTIONAL
                    } else {
                        Repetition::REQUIRED
                    };
                    leaves.push(Arc::new(leaf_type(&f.name, f.prim, rep)?));
                }
                let elem = Type::group_type_builder("element")
                    .with_repetition(Repetition::OPTIONAL)
                    .with_fields(leaves)
                    .build()
                    .map_err(|e| {
                        ConvertError::Schema(format!("the endpoints element does not build: {e}"))
                    })?;
                list_type(&col.name, Arc::new(elem))?
            }
            Shape::ExtraMap => map_type(&col.name)?,
        };
        fields.push(Arc::new(t));
    }
    Type::group_type_builder("schema")
        .with_fields(fields)
        .build()
        .map(Arc::new)
        .map_err(|e| ConvertError::Schema(format!("the file schema does not build: {e}")))
}

fn leaf_type(name: &str, prim: Prim, rep: Repetition) -> Result<Type, ConvertError> {
    let builder = match prim {
        Prim::Utf8 => Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_logical_type(Some(LogicalType::String)),
        Prim::Int64 => Type::primitive_type_builder(name, PhysicalType::INT64),
        Prim::Float64 => Type::primitive_type_builder(name, PhysicalType::DOUBLE),
        Prim::Bool => Type::primitive_type_builder(name, PhysicalType::BOOLEAN),
    };
    builder
        .with_repetition(rep)
        .build()
        .map_err(|e| ConvertError::Schema(format!("column {name:?} does not build: {e}")))
}

fn list_type(name: &str, element: TypePtr) -> Result<Type, ConvertError> {
    let inner = Type::group_type_builder("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields(vec![element])
        .build()
        .map_err(|e| ConvertError::Schema(format!("list column {name:?} does not build: {e}")))?;
    Type::group_type_builder(name)
        .with_repetition(Repetition::OPTIONAL)
        .with_converted_type(ConvertedType::LIST)
        .with_logical_type(Some(LogicalType::List))
        .with_fields(vec![Arc::new(inner)])
        .build()
        .map_err(|e| ConvertError::Schema(format!("list column {name:?} does not build: {e}")))
}

fn map_type(name: &str) -> Result<Type, ConvertError> {
    let key = leaf_type("key", Prim::Utf8, Repetition::REQUIRED)?;
    let value = leaf_type("value", Prim::Utf8, Repetition::REQUIRED)?;
    let kv = Type::group_type_builder("key_value")
        .with_repetition(Repetition::REPEATED)
        .with_fields(vec![Arc::new(key), Arc::new(value)])
        .build()
        .map_err(|e| ConvertError::Schema(format!("map column {name:?} does not build: {e}")))?;
    Type::group_type_builder(name)
        .with_repetition(Repetition::OPTIONAL)
        .with_converted_type(ConvertedType::MAP)
        .with_logical_type(Some(LogicalType::Map))
        .with_fields(vec![Arc::new(kv)])
        .build()
        .map_err(|e| ConvertError::Schema(format!("map column {name:?} does not build: {e}")))
}

/// The closed `endpoints` element, sorted by field name.
fn parse_element(spec: &Value) -> Result<Vec<ElemField>, ConvertError> {
    let obj = spec["endpoints_element"].as_object().ok_or_else(|| {
        ConvertError::Schema("the vendored schema has no endpoints_element object".into())
    })?;
    let mut out = Vec::new();
    for (name, f) in obj {
        let ty = f["type"].as_str().ok_or_else(|| {
            ConvertError::Schema(format!("endpoints element field {name:?} has no type"))
        })?;
        let prim = Prim::parse(ty).ok_or_else(|| {
            ConvertError::Schema(format!(
                "endpoints element field {name:?} has unknown type {ty:?}"
            ))
        })?;
        out.push(ElemField {
            name: name.clone(),
            prim,
            optional: f["presence"].as_str() != Some("required"),
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// The union of every kind's typed fields. One name, one type: a field typed two ways
/// across kinds violates the contract, and building a column for either type would decode
/// the other kind's records wrongly.
fn parse_union(spec: &Value) -> Result<BTreeMap<String, String>, ConvertError> {
    let kinds = spec["kinds"]
        .as_object()
        .ok_or_else(|| ConvertError::Schema("the vendored schema has no kinds object".into()))?;
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    for (kind, body) in kinds {
        let fs = body["fields"]
            .as_object()
            .ok_or_else(|| ConvertError::Schema(format!("kind {kind:?} has no fields object")))?;
        for (name, f) in fs {
            let ty = f["type"].as_str().ok_or_else(|| {
                ConvertError::Schema(format!("field {name:?} of kind {kind:?} has no type"))
            })?;
            match fields.get(name) {
                Some(prev) if prev != ty => {
                    return Err(ConvertError::Schema(format!(
                        "field {name:?} is two types across kinds: {prev:?} and {ty:?}"
                    )));
                }
                Some(_) => {}
                None => {
                    fields.insert(name.clone(), ty.to_string());
                }
            }
        }
    }
    Ok(fields)
}

#[derive(Debug)]
pub(super) struct ColumnPlan {
    pub(super) columns: Vec<Column>,
    pub(super) element: Vec<ElemField>,
    pub(super) leaves: Vec<LeafPlan>,
    pub(super) typed: BTreeSet<String>,
    pub(super) schema: TypePtr,
}

impl ColumnPlan {
    pub(super) fn from_contract() -> Result<Self, ConvertError> {
        let spec: Value = serde_json::from_str(SCHEMA_JSON).map_err(|e| {
            ConvertError::Schema(format!("the vendored schema is not valid JSON: {e}"))
        })?;
        let element = parse_element(&spec)?;
        let union = parse_union(&spec)?;
        for head in ["kind", "at_ms"] {
            if !union.contains_key(head) {
                return Err(ConvertError::Schema(format!(
                    "the vendored schema has no {head:?} field"
                )));
            }
        }

        let mut names: Vec<&str> = vec!["kind", "at_ms"];
        names.extend(
            union
                .keys()
                .map(String::as_str)
                .filter(|k| *k != "kind" && *k != "at_ms"),
        );

        let mut columns: Vec<Column> = Vec::new();
        let mut leaves: Vec<LeafPlan> = Vec::new();
        for name in names {
            let ty = &union[name];
            let shape = match ty.as_str() {
                "list<utf8>" => Shape::ListUtf8,
                "list<struct>" => Shape::Endpoints,
                other => Shape::Prim(Prim::parse(other).ok_or_else(|| {
                    ConvertError::Schema(format!("field {name:?} has unknown type {other:?}"))
                })?),
            };
            let leaf = leaves.len();
            match shape {
                Shape::Prim(prim) => leaves.push(LeafPlan {
                    prim,
                    repeated: false,
                }),
                Shape::ListUtf8 => leaves.push(LeafPlan {
                    prim: Prim::Utf8,
                    repeated: true,
                }),
                Shape::Endpoints => {
                    for f in &element {
                        leaves.push(LeafPlan {
                            prim: f.prim,
                            repeated: true,
                        });
                    }
                }
                Shape::ExtraMap => {}
            }
            columns.push(Column {
                name: name.to_string(),
                shape,
                leaf,
            });
        }
        let leaf = leaves.len();
        for _ in 0..2 {
            leaves.push(LeafPlan {
                prim: Prim::Utf8,
                repeated: true,
            });
        }
        columns.push(Column {
            name: "extra".to_string(),
            shape: Shape::ExtraMap,
            leaf,
        });

        let schema = build_schema(&columns, &element)?;
        Ok(Self {
            columns,
            element,
            leaves,
            typed: union.into_keys().collect(),
            schema,
        })
    }
}
