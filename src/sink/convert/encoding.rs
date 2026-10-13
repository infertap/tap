// SPDX-License-Identifier: Apache-2.0
//! Encode records into bounded column buffers and write Parquet row groups.

use super::schema::{ColumnPlan, LeafPlan, Prim, Shape};
use super::{ConvertError, Malformed};
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::writer::SerializedFileWriter;
use serde_json::{Map, Value};
use std::fs::File;

/// Buffered values and levels for one leaf. The values are the leaf's type and no other:
/// a buffer is built for its plan, so a value of the wrong type has nowhere to go.
struct LeafBuf {
    values: LeafValues,
    defs: Vec<i16>,
    reps: Vec<i16>,
}

/// The value vector of one leaf, keyed by the column's primitive type.
enum LeafValues {
    Utf8(Vec<ByteArray>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Bool(Vec<bool>),
}

impl LeafBuf {
    fn for_prim(prim: Prim) -> Self {
        Self {
            values: match prim {
                Prim::Utf8 => LeafValues::Utf8(Vec::new()),
                Prim::Int64 => LeafValues::Int64(Vec::new()),
                Prim::Float64 => LeafValues::Float64(Vec::new()),
                Prim::Bool => LeafValues::Bool(Vec::new()),
            },
            defs: Vec::new(),
            reps: Vec::new(),
        }
    }

    /// The column type this buffer holds.
    const fn prim(&self) -> Prim {
        match self.values {
            LeafValues::Utf8(_) => Prim::Utf8,
            LeafValues::Int64(_) => Prim::Int64,
            LeafValues::Float64(_) => Prim::Float64,
            LeafValues::Bool(_) => Prim::Bool,
        }
    }

    fn clear(&mut self) {
        match &mut self.values {
            LeafValues::Utf8(v) => v.clear(),
            LeafValues::Int64(v) => v.clear(),
            LeafValues::Float64(v) => v.clear(),
            LeafValues::Bool(v) => v.clear(),
        }
        self.defs.clear();
        self.reps.clear();
    }

    /// Push one string into a string leaf; a leaf of another type refuses it as a wiring
    /// defect, never a property of the segment.
    fn push_str(&mut self, s: &str) -> Result<(), ConvertError> {
        let LeafValues::Utf8(v) = &mut self.values else {
            return Err(ConvertError::Schema(format!(
                "a string was pushed into a {} leaf",
                self.prim()
            )));
        };
        v.push(ByteArray::from(s));
        Ok(())
    }
}

/// The rows accumulated for the next row group, with an approximate byte size.
pub(super) struct Batch {
    leaves: Vec<LeafBuf>,
    pub(super) rows: usize,
    pub(super) bytes: usize,
}

impl Batch {
    /// One buffer per leaf, each of its plan's type.
    pub(super) fn for_plan(leaves: &[LeafPlan]) -> Self {
        Self {
            leaves: leaves.iter().map(|l| LeafBuf::for_prim(l.prim)).collect(),
            rows: 0,
            bytes: 0,
        }
    }

    pub(super) fn clear(&mut self) {
        for leaf in &mut self.leaves {
            leaf.clear();
        }
        self.rows = 0;
        self.bytes = 0;
    }
}

fn mismatch(field: &str, line: usize, column: Prim) -> ConvertError {
    malformed(
        line,
        Malformed::TypeMismatch {
            field: field.to_string(),
            column,
        },
    )
}

/// One line of the source that cannot be encoded: the segment is poison, not a retry.
pub(super) fn malformed(line: usize, why: Malformed) -> ConvertError {
    ConvertError::Malformed { line, why }
}

/// Push one coerced value into the leaf's value vector. The leaf's own type decides the
/// coercion; a value that does not fit it is the line's fault. Levels are pushed
/// separately.
fn push_value(
    buf: &mut LeafBuf,
    bytes: &mut usize,
    field: &str,
    line: usize,
    v: &Value,
) -> Result<(), ConvertError> {
    let prim = buf.prim();
    match &mut buf.values {
        LeafValues::Utf8(out) => {
            let Some(s) = v.as_str() else {
                return Err(mismatch(field, line, prim));
            };
            *bytes += s.len();
            out.push(ByteArray::from(s));
        }
        LeafValues::Int64(out) => {
            let Some(n) = v.as_i64() else {
                return Err(mismatch(field, line, prim));
            };
            *bytes += 8;
            out.push(n);
        }
        LeafValues::Float64(out) => {
            let Some(n) = v.as_f64() else {
                return Err(mismatch(field, line, prim));
            };
            *bytes += 8;
            out.push(n);
        }
        LeafValues::Bool(out) => {
            let Some(b) = v.as_bool() else {
                return Err(mismatch(field, line, prim));
            };
            *bytes += 8;
            out.push(b);
        }
    }
    Ok(())
}

/// Push definition and repetition levels, counting their bytes toward the buffer limit.
/// Even an all-null column consumes memory for these levels.
fn push_levels(buf: &mut LeafBuf, bytes: &mut usize, def: i16, rep: Option<i16>) {
    buf.defs.push(def);
    *bytes += 2;
    if let Some(r) = rep {
        buf.reps.push(r);
        *bytes += 2;
    }
}

/// Append one flat optional column cell. Definition levels: 0 absent, 1 present.
fn append_flat(
    buf: &mut LeafBuf,
    bytes: &mut usize,
    field: &str,
    line: usize,
    v: Option<&Value>,
) -> Result<(), ConvertError> {
    match v {
        None => push_levels(buf, bytes, 0, None),
        Some(v) => {
            push_value(buf, bytes, field, line, v)?;
            push_levels(buf, bytes, 1, None);
        }
    }
    Ok(())
}

/// Append one `list<utf8>` cell. Definition levels: 0 absent, 1 empty, 2 element.
fn append_list_utf8(
    buf: &mut LeafBuf,
    bytes: &mut usize,
    field: &str,
    line: usize,
    v: Option<&Value>,
) -> Result<(), ConvertError> {
    let Some(v) = v else {
        push_levels(buf, bytes, 0, Some(0));
        return Ok(());
    };
    let Some(items) = v.as_array() else {
        return Err(malformed(
            line,
            Malformed::NotStringArray {
                field: field.to_string(),
            },
        ));
    };
    if items.is_empty() {
        push_levels(buf, bytes, 1, Some(0));
        return Ok(());
    }
    for (i, item) in items.iter().enumerate() {
        push_value(buf, bytes, field, line, item)?;
        push_levels(buf, bytes, 2, Some(i16::from(i > 0)));
    }
    Ok(())
}

/// Record encoding onto the plan: each typed key to its column, the rest to `extra`.
impl ColumnPlan {
    /// Append one record: each typed key to its column, everything else to `extra`.
    pub(super) fn append(
        &self,
        rec: &Map<String, Value>,
        line: usize,
        batch: &mut Batch,
    ) -> Result<(), ConvertError> {
        // Require a non-empty kind so readers can dispatch every encoded record.
        match rec.get("kind") {
            Some(Value::String(k)) if !k.is_empty() => {}
            _ => {
                return Err(malformed(line, Malformed::KindMissing));
            }
        }
        let Batch {
            leaves,
            bytes,
            rows,
        } = batch;
        for col in &self.columns {
            let v = rec.get(&col.name).filter(|v| !v.is_null());
            match col.shape {
                Shape::Prim(_) => {
                    append_flat(&mut leaves[col.leaf], bytes, &col.name, line, v)?;
                }
                Shape::ListUtf8 => {
                    append_list_utf8(&mut leaves[col.leaf], bytes, &col.name, line, v)?;
                }
                Shape::Endpoints => {
                    let end = col.leaf + self.element.len();
                    self.append_endpoints(&mut leaves[col.leaf..end], bytes, line, v)?;
                }
                Shape::ExtraMap => {
                    self.append_extra(&mut leaves[col.leaf..col.leaf + 2], bytes, line, rec)?;
                }
            }
        }
        *rows += 1;
        Ok(())
    }

    /// Append one `endpoints` cell. The element is a closed struct: an unknown key is an
    /// error, and so is a missing required field, which the file schema cannot encode as
    /// null. Leaf definition levels: 0 absent list, 1 empty list, 3 struct present, 4 an
    /// optional leaf's own value.
    fn append_endpoints(
        &self,
        bufs: &mut [LeafBuf],
        bytes: &mut usize,
        line: usize,
        v: Option<&Value>,
    ) -> Result<(), ConvertError> {
        let Some(v) = v else {
            for buf in bufs {
                push_levels(buf, bytes, 0, Some(0));
            }
            return Ok(());
        };
        let Some(items) = v.as_array() else {
            return Err(malformed(line, Malformed::EndpointsNotArray));
        };
        if items.is_empty() {
            for buf in bufs {
                push_levels(buf, bytes, 1, Some(0));
            }
            return Ok(());
        }
        for (i, item) in items.iter().enumerate() {
            let Some(obj) = item.as_object() else {
                return Err(malformed(line, Malformed::EndpointNotObject));
            };
            for key in obj.keys() {
                if !self.element.iter().any(|f| f.name == *key) {
                    return Err(malformed(
                        line,
                        Malformed::EndpointUnknownField(key.clone()),
                    ));
                }
            }
            let rep = Some(i16::from(i > 0));
            for (f, buf) in self.element.iter().zip(bufs.iter_mut()) {
                match obj.get(&f.name).filter(|v| !v.is_null()) {
                    Some(val) => {
                        push_value(buf, bytes, &f.name, line, val)?;
                        push_levels(buf, bytes, if f.optional { 4 } else { 3 }, rep);
                    }
                    None if f.optional => push_levels(buf, bytes, 3, rep),
                    None => {
                        return Err(malformed(
                            line,
                            Malformed::EndpointMissingField(f.name.clone()),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Append one `extra` cell: every record key the schema does not type, in key order.
    /// A string value rides verbatim; any other value rides as its compact JSON text.
    /// Definition levels: 1 empty map, 2 entry. The map is never absent, matching the
    /// corpus generator.
    fn append_extra(
        &self,
        bufs: &mut [LeafBuf],
        bytes: &mut usize,
        line: usize,
        rec: &Map<String, Value>,
    ) -> Result<(), ConvertError> {
        let [keys, vals] = bufs else {
            return Err(ConvertError::Schema(format!(
                "line {line}: the extra column is miswired"
            )));
        };
        let mut wrote = false;
        for (k, v) in rec {
            if self.typed.contains(k) {
                continue;
            }
            let rendered = match v {
                Value::String(s) => s.clone(),
                other => serde_json::to_string(other).map_err(|e| {
                    malformed(
                        line,
                        Malformed::DoesNotRender {
                            field: k.clone(),
                            source: e,
                        },
                    )
                })?,
            };
            let rep = Some(i16::from(wrote));
            *bytes += k.len() + rendered.len();
            keys.push_str(k)?;
            vals.push_str(&rendered)?;
            push_levels(keys, bytes, 2, rep);
            push_levels(vals, bytes, 2, rep);
            wrote = true;
        }
        if !wrote {
            push_levels(keys, bytes, 1, Some(0));
            push_levels(vals, bytes, 1, Some(0));
        }
        Ok(())
    }

    /// Write the batch as one row group, leaf by leaf in schema order.
    pub(super) fn write_group(
        &self,
        writer: &mut SerializedFileWriter<File>,
        batch: &Batch,
    ) -> Result<(), ConvertError> {
        let mut group = writer.next_row_group().map_err(ConvertError::Parquet)?;
        for (plan, buf) in self.leaves.iter().zip(&batch.leaves) {
            let mut col = group
                .next_column()
                .map_err(ConvertError::Parquet)?
                .ok_or_else(|| {
                    ConvertError::Schema("the writer offers fewer columns than the schema".into())
                })?;
            let defs = Some(buf.defs.as_slice());
            let reps = plan.repeated.then_some(buf.reps.as_slice());
            let written = match &buf.values {
                LeafValues::Utf8(v) => col.typed::<ByteArrayType>().write_batch(v, defs, reps),
                LeafValues::Int64(v) => col.typed::<Int64Type>().write_batch(v, defs, reps),
                LeafValues::Float64(v) => col.typed::<DoubleType>().write_batch(v, defs, reps),
                LeafValues::Bool(v) => col.typed::<BoolType>().write_batch(v, defs, reps),
            };
            written.map_err(ConvertError::Parquet)?;
            col.close().map_err(ConvertError::Parquet)?;
        }
        group.close().map_err(ConvertError::Parquet)?;
        Ok(())
    }
}
