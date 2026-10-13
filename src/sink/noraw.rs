// SPDX-License-Identifier: Apache-2.0
//! Scan serialized records for unexpected raw values after pseudonymization.
//!
//! The scan compares each output value with the raw identities and operator strings
//! carried by that record. It recurses into arrays and maps. State is per-record.
//!
//! IDs below 2^32 are excluded because they can equal ordinary counters or scope values.
//! Under a uniform 64-bit hash assumption, an ID falls in this range with probability
//! 2^-32; for uniform 128-bit content IDs it is 2^-96. A raw ID in this range can
//! escape detection by this backstop.
//!
//! Operator strings may equal public vocabulary such as a record kind or tier. Matches
//! at paths where the same string already appeared before mapping are exempt, except
//! for fields that the pseudonymizer rewrites. Newly introduced matches are rejected.
//!
//! These exclusions prevent false-positive aborts. A successful scan establishes no
//! detected match within this scope; it does not establish that arbitrary metadata is
//! free of sensitive content. Typed identity mapping is the primary control.

use std::borrow::Cow;

use serde::ser::{self, Serialize};

/// Smallest ID checked by the raw-value scanner.
/// Smaller IDs can equal numeric counters and are excluded from this backstop.
pub(crate) const PLAUSIBLE_FINGERPRINT_MIN: u64 = 1 << 32;

/// What kind of raw value survived the mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeakKind {
    /// A raw block or content id the record carried before mapping.
    CatalogId,
    /// A raw operator-controlled string (a cache salt, a `LoRA` name) the record carried.
    OperatorString,
}

/// A raw value that survived pseudonymization, and the path it was found at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLeak {
    /// Dotted path from the record root, array indices in brackets: `record.extra_keys[0]`.
    pub path: String,
    /// Which kind of raw value it is.
    pub kind: LeakKind,
}

impl std::fmt::Display for RawLeak {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            LeakKind::CatalogId => write!(f, "{} carries a raw catalog id", self.path),
            LeakKind::OperatorString => write!(f, "{} carries a raw operator string", self.path),
        }
    }
}

impl std::error::Error for RawLeak {}

/// Scan serialized values for unexpected raw IDs and operator strings.
/// Return the number of values visited. Exclusions are described in the module docs.
/// The successful path walks serialization without constructing a JSON tree.
///
/// # Errors
/// Returns the first detected match with its path and value category.
///
/// # Panics
/// Panics if the supplied type refuses serialization. Record variants serialize normally.
pub fn assert_no_raw<T: Serialize + ?Sized>(
    record: &T,
    raw_ids: &[u128],
    raw_strs: &[String],
    preexisting: &[String],
) -> Result<usize, RawLeak> {
    // The carried sets are per record and a handful long: a scan beats a hash.
    let is_raw_id = |i: u128| i >= u128::from(PLAUSIBLE_FINGERPRINT_MIN) && raw_ids.contains(&i);
    let mut scan = Scan {
        is_raw_id: &is_raw_id,
        raw_strs,
        preexisting,
    };
    let mut walk = Walk::new(&mut scan);
    match record.serialize(&mut walk) {
        Ok(()) => Ok(walk.scanned),
        Err(Stop::Leak(leak)) => Err(leak),
        #[allow(clippy::panic)] // a record serializes; see the doc
        Err(Stop::Unserializable(why)) => panic!("a record serializes: {why}"),
    }
}

/// Collect existing raw-string paths before pseudonymization.
/// Skip the top-level fields in `exclude`: the pseudonymizer rewrites those fields,
/// so their original values must not become scanner exemptions.
///
/// # Panics
/// Panics if the supplied type refuses serialization, as in [`assert_no_raw`].
#[must_use]
pub(crate) fn preexisting_string_paths<T: Serialize + ?Sized>(
    record: &T,
    raw_strs: &[String],
    exclude: &[&str],
) -> Vec<String> {
    let mut collect = Collect {
        raw_strs,
        exclude,
        out: Vec::new(),
    };
    if raw_strs.is_empty() {
        return collect.out;
    }
    let mut walk = Walk::new(&mut collect);
    #[allow(clippy::expect_used)] // a record serializes; see assert_no_raw
    record.serialize(&mut walk).expect("a record serializes");
    collect.out
}

/// One step of the path from the record root, kept on a stack so the dotted path is
/// rendered only when it is needed: on a leak, or on the rare raw-string match that must
/// be checked against the preexisting occurrences. The success path allocates nothing
/// beyond the stack itself.
enum Seg {
    Key(Cow<'static, str>),
    Index(usize),
}

fn render(path: &[Seg]) -> String {
    use std::fmt::Write as _;
    let mut s = String::from("record");
    for seg in path {
        match seg {
            Seg::Key(k) => {
                s.push('.');
                s.push_str(k);
            }
            Seg::Index(i) => {
                let _ = write!(s, "[{i}]");
            }
        }
    }
    s
}

/// What a walk does at each scalar it reaches.
trait Visit {
    /// An unsigned integer, or a signed one that is not negative.
    fn number(&mut self, n: u64, path: &[Seg]) -> Result<(), RawLeak>;
    /// A string, including a unit variant's name.
    fn text(&mut self, s: &str, path: &[Seg]) -> Result<(), RawLeak>;
}

/// The guard's visit: a carried id or string anywhere is a leak.
struct Scan<'a> {
    is_raw_id: &'a dyn Fn(u128) -> bool,
    raw_strs: &'a [String],
    preexisting: &'a [String],
}

impl Visit for Scan<'_> {
    fn number(&mut self, n: u64, path: &[Seg]) -> Result<(), RawLeak> {
        if (self.is_raw_id)(u128::from(n)) {
            return Err(RawLeak {
                path: render(path),
                kind: LeakKind::CatalogId,
            });
        }
        Ok(())
    }

    fn text(&mut self, s: &str, path: &[Seg]) -> Result<(), RawLeak> {
        if s.parse::<u128>().is_ok_and(self.is_raw_id) {
            return Err(RawLeak {
                path: render(path),
                kind: LeakKind::CatalogId,
            });
        }
        // A match at a path the string already occupied before the decorator ran is a
        // coincidence that predates it (module doc); anywhere else it is a leak.
        if self.raw_strs.iter().any(|r| r == s) {
            let at = render(path);
            if !self.preexisting.contains(&at) {
                return Err(RawLeak {
                    path: at,
                    kind: LeakKind::OperatorString,
                });
            }
        }
        Ok(())
    }
}

/// The decorator's pre-mapping visit: where each carried string already sits.
struct Collect<'a> {
    raw_strs: &'a [String],
    exclude: &'a [&'a str],
    out: Vec<String>,
}

impl Visit for Collect<'_> {
    fn number(&mut self, _: u64, _: &[Seg]) -> Result<(), RawLeak> {
        Ok(())
    }

    fn text(&mut self, s: &str, path: &[Seg]) -> Result<(), RawLeak> {
        let excluded = matches!(path.first(), Some(Seg::Key(k)) if self.exclude.contains(&&**k));
        if !excluded && self.raw_strs.iter().any(|r| r == s) {
            let at = render(path);
            if !self.out.contains(&at) {
                self.out.push(at);
            }
        }
        Ok(())
    }
}

/// Why a walk stopped.
#[derive(Debug)]
enum Stop {
    Leak(RawLeak),
    Unserializable(String),
}

impl std::fmt::Display for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leak(l) => l.fmt(f),
            Self::Unserializable(why) => f.write_str(why),
        }
    }
}

impl std::error::Error for Stop {}

impl ser::Error for Stop {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self::Unserializable(msg.to_string())
    }
}

/// A serializer that writes nothing: it walks the record's serialized shape and hands
/// every scalar to a [`Visit`], with the path to it. Every value the JSON line would
/// carry is counted, containers included, so the count means the same as it would over
/// the tree.
struct Walk<'v, V> {
    visit: &'v mut V,
    path: Vec<Seg>,
    scanned: usize,
}

impl<'v, V: Visit> Walk<'v, V> {
    fn new(visit: &'v mut V) -> Self {
        Self {
            visit,
            path: Vec::new(),
            scanned: 0,
        }
    }

    fn number(&mut self, n: u64) -> Result<(), Stop> {
        self.scanned += 1;
        self.visit.number(n, &self.path).map_err(Stop::Leak)
    }

    fn text(&mut self, s: &str) -> Result<(), Stop> {
        self.scanned += 1;
        self.visit.text(s, &self.path).map_err(Stop::Leak)
    }

    /// A scalar the guard has nothing to say about: a null, a bool, a float, a negative.
    fn other(&mut self) {
        self.scanned += 1;
    }

    /// Enter a container, counting it, with a placeholder the first key or index fills.
    fn enter(&mut self, seg: Seg) -> &mut Self {
        self.scanned += 1;
        self.path.push(seg);
        self
    }

    fn set_key(&mut self, key: Cow<'static, str>) {
        if let Some(last) = self.path.last_mut() {
            *last = Seg::Key(key);
        }
    }

    fn advance_index(&mut self) {
        if let Some(Seg::Index(i)) = self.path.last_mut() {
            *i += 1;
        }
    }

    fn leave(&mut self) {
        self.path.pop();
    }
}

impl<V: Visit> ser::Serializer for &mut Walk<'_, V> {
    type Ok = ();
    type Error = Stop;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _: bool) -> Result<(), Stop> {
        self.other();
        Ok(())
    }
    fn serialize_i8(self, v: i8) -> Result<(), Stop> {
        self.serialize_i64(i64::from(v))
    }
    fn serialize_i16(self, v: i16) -> Result<(), Stop> {
        self.serialize_i64(i64::from(v))
    }
    fn serialize_i32(self, v: i32) -> Result<(), Stop> {
        self.serialize_i64(i64::from(v))
    }
    fn serialize_i64(self, v: i64) -> Result<(), Stop> {
        if let Ok(n) = u64::try_from(v) {
            return self.number(n);
        }
        self.other();
        Ok(())
    }
    fn serialize_u8(self, v: u8) -> Result<(), Stop> {
        self.number(u64::from(v))
    }
    fn serialize_u16(self, v: u16) -> Result<(), Stop> {
        self.number(u64::from(v))
    }
    fn serialize_u32(self, v: u32) -> Result<(), Stop> {
        self.number(u64::from(v))
    }
    fn serialize_u64(self, v: u64) -> Result<(), Stop> {
        self.number(v)
    }
    fn serialize_f32(self, _: f32) -> Result<(), Stop> {
        self.other();
        Ok(())
    }
    fn serialize_f64(self, _: f64) -> Result<(), Stop> {
        self.other();
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<(), Stop> {
        self.text(v.encode_utf8(&mut [0; 4]))
    }
    fn serialize_str(self, v: &str) -> Result<(), Stop> {
        self.text(v)
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Stop> {
        // As JSON carries them: an array of numbers, none a fingerprint.
        self.enter(Seg::Index(0));
        for _ in v {
            self.other();
        }
        self.leave();
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Stop> {
        self.other();
        Ok(())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Stop> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Stop> {
        self.other();
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Stop> {
        self.other();
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<(), Stop> {
        self.text(variant)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Stop> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Stop> {
        // As JSON carries it: `{variant: value}`.
        self.enter(Seg::Key(Cow::Borrowed(variant)));
        value.serialize(&mut *self)?;
        self.leave();
        Ok(())
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self, Stop> {
        Ok(self.enter(Seg::Index(0)))
    }
    fn serialize_tuple(self, _: usize) -> Result<Self, Stop> {
        Ok(self.enter(Seg::Index(0)))
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, Stop> {
        Ok(self.enter(Seg::Index(0)))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self, Stop> {
        self.enter(Seg::Key(Cow::Borrowed(variant)));
        Ok(self.enter(Seg::Index(0)))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self, Stop> {
        Ok(self.enter(Seg::Key(Cow::Borrowed(""))))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, Stop> {
        Ok(self.enter(Seg::Key(Cow::Borrowed(""))))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self, Stop> {
        self.enter(Seg::Key(Cow::Borrowed(variant)));
        Ok(self.enter(Seg::Key(Cow::Borrowed(""))))
    }
}

impl<V: Visit> ser::SerializeSeq for &mut Walk<'_, V> {
    type Ok = ();
    type Error = Stop;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Stop> {
        value.serialize(&mut **self)?;
        self.advance_index();
        Ok(())
    }
    fn end(self) -> Result<(), Stop> {
        self.leave();
        Ok(())
    }
}

impl<V: Visit> ser::SerializeTuple for &mut Walk<'_, V> {
    type Ok = ();
    type Error = Stop;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Stop> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Stop> {
        self.leave();
        Ok(())
    }
}

impl<V: Visit> ser::SerializeTupleStruct for &mut Walk<'_, V> {
    type Ok = ();
    type Error = Stop;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Stop> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Stop> {
        self.leave();
        Ok(())
    }
}

impl<V: Visit> ser::SerializeTupleVariant for &mut Walk<'_, V> {
    type Ok = ();
    type Error = Stop;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Stop> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Stop> {
        self.leave();
        self.leave();
        Ok(())
    }
}

impl<V: Visit> ser::SerializeMap for &mut Walk<'_, V> {
    type Ok = ();
    type Error = Stop;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Stop> {
        let key = key.serialize(KeyText)?;
        self.set_key(Cow::Owned(key));
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Stop> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Stop> {
        self.leave();
        Ok(())
    }
}

impl<V: Visit> ser::SerializeStruct for &mut Walk<'_, V> {
    type Ok = ();
    type Error = Stop;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Stop> {
        self.set_key(Cow::Borrowed(key));
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Stop> {
        self.leave();
        Ok(())
    }
}

impl<V: Visit> ser::SerializeStructVariant for &mut Walk<'_, V> {
    type Ok = ();
    type Error = Stop;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Stop> {
        ser::SerializeStruct::serialize_field(self, key, value)
    }
    fn end(self) -> Result<(), Stop> {
        self.leave();
        self.leave();
        Ok(())
    }
}

/// The serializer a map key goes through: JSON keys are strings, and a record's are.
struct KeyText;

impl ser::Serializer for KeyText {
    type Ok = String;
    type Error = Stop;
    type SerializeSeq = ser::Impossible<String, Stop>;
    type SerializeTuple = ser::Impossible<String, Stop>;
    type SerializeTupleStruct = ser::Impossible<String, Stop>;
    type SerializeTupleVariant = ser::Impossible<String, Stop>;
    type SerializeMap = ser::Impossible<String, Stop>;
    type SerializeStruct = ser::Impossible<String, Stop>;
    type SerializeStructVariant = ser::Impossible<String, Stop>;

    fn serialize_str(self, v: &str) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_char(self, v: char) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<String, Stop> {
        Ok(variant.to_string())
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<String, Stop> {
        value.serialize(self)
    }
    fn serialize_bool(self, v: bool) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_i8(self, v: i8) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_i16(self, v: i16) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_i32(self, v: i32) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_i64(self, v: i64) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_u8(self, v: u8) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_u16(self, v: u16) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_u32(self, v: u32) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_u64(self, v: u64) -> Result<String, Stop> {
        Ok(v.to_string())
    }
    fn serialize_f32(self, _: f32) -> Result<String, Stop> {
        Err(Stop::Unserializable("a float is not a map key".into()))
    }
    fn serialize_f64(self, _: f64) -> Result<String, Stop> {
        Err(Stop::Unserializable("a float is not a map key".into()))
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<String, Stop> {
        Err(Stop::Unserializable("bytes are not a map key".into()))
    }
    fn serialize_none(self) -> Result<String, Stop> {
        Err(Stop::Unserializable("null is not a map key".into()))
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<String, Stop> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<String, Stop> {
        Err(Stop::Unserializable("unit is not a map key".into()))
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<String, Stop> {
        Err(Stop::Unserializable(
            "a unit struct is not a map key".into(),
        ))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<String, Stop> {
        Err(Stop::Unserializable("a variant is not a map key".into()))
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Stop> {
        Err(Stop::Unserializable("a sequence is not a map key".into()))
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Stop> {
        Err(Stop::Unserializable("a tuple is not a map key".into()))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Stop> {
        Err(Stop::Unserializable("a tuple is not a map key".into()))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Stop> {
        Err(Stop::Unserializable("a variant is not a map key".into()))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Stop> {
        Err(Stop::Unserializable("a map is not a map key".into()))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Stop> {
        Err(Stop::Unserializable("a struct is not a map key".into()))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Stop> {
        Err(Stop::Unserializable("a variant is not a map key".into()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{assert_no_raw, preexisting_string_paths};
    use serde_json::json;

    fn ids(v: &[u64]) -> Vec<u128> {
        v.iter().copied().map(u128::from).collect()
    }

    fn none() -> Vec<String> {
        Vec::new()
    }

    #[test]
    fn a_pseudonymized_record_passes_and_reports_the_scan_count() {
        let r = json!({"block_id": "db83955d1a485fe98364fd9e47c8bc08", "epoch": 7, "n_tokens": 4});
        assert!(assert_no_raw(&r, &ids(&[1, 2]), &[], &none()).expect("clean") >= 3);
    }

    // A realistic fingerprint: uniform over u64, well above the tracking floor.
    const HASH: u64 = 12_955_354_968_414_679_641;

    #[test]
    fn a_raw_integer_id_is_caught() {
        let r = json!({"block_id": HASH, "epoch": 7});
        assert!(assert_no_raw(&r, &ids(&[HASH]), &[], &none()).is_err());
    }

    #[test]
    fn a_raw_id_as_a_decimal_string_is_caught() {
        let r = json!({"block_id": HASH.to_string()});
        assert!(assert_no_raw(&r, &ids(&[HASH]), &[], &none()).is_err());
    }

    #[test]
    fn a_hash_above_i64_max_is_still_caught() {
        // the upper half of the u64 range, which a signed comparison would miss
        let big = 12_955_354_968_414_679_641_u64;
        assert!(assert_no_raw(&json!({"block_id": big}), &ids(&[big]), &[], &none()).is_err());
    }

    #[test]
    fn a_raw_operator_string_nested_in_an_array_is_caught() {
        // `extra_keys` is an array, so the scan must descend into containers
        let r = json!({"kind": "store", "extra_keys": ["tenant-AAAA"]});
        assert!(assert_no_raw(&r, &ids(&[]), &["tenant-AAAA".to_string()], &none()).is_err());
    }

    #[test]
    fn a_pseudonymized_partition_passes() {
        let r = json!({"kind": "store", "extra_keys": ["9f2c1ab4de77c0315b8ee0a4c6d1f883"]});
        assert!(assert_no_raw(&r, &ids(&[]), &["tenant-AAAA".to_string()], &none()).is_ok());
    }

    #[test]
    fn counters_are_never_mistaken_for_fingerprints() {
        // The full shape of a real store record. A synthetic id of 0 collides with `epoch`,
        // `dp_rank`, and `group_idx`, so the exclusion is a rule on the value rather than
        // a list of field names.
        let r = json!({"kind": "store", "block_id": "db83955d1a485fe98364fd9e47c8bc08",
                       "epoch": 0, "dp_rank": 0, "group_idx": 0, "n_tokens": 16});
        for small in [0_u64, 16, 1, 7] {
            assert!(
                assert_no_raw(&r, &ids(&[small]), &[], &none()).is_ok(),
                "a carried id of {small} is a counter, not a fingerprint"
            );
        }
    }

    #[test]
    fn narrowing_the_tracked_range_does_not_weaken_the_real_check() {
        // Every field is still scanned, and a genuine fingerprint in an unmapped field is
        // still the leak this exists to catch.
        let r = json!({"kind": "store", "some_future_field": HASH, "dp_rank": 0});
        assert!(assert_no_raw(&r, &ids(&[HASH]), &[], &none()).is_err());
    }

    #[test]
    fn a_hex_pseudonym_that_looks_numeric_does_not_false_positive() {
        let r = json!({"block_id": "0000000000000000000000000000002a"});
        assert!(assert_no_raw(&r, &ids(&[42]), &[], &none()).is_ok());
    }

    // --- the string side of the plausibility question (module doc, last section) ---

    /// A salt that collides with record vocabulary is a coincidence predating the decorator:
    /// wire-controlled text (`cache_salt: "store"`) must not abort the tap on the record's
    /// own `kind` field.
    #[test]
    fn a_salt_colliding_with_record_vocabulary_is_not_the_decorators_leak() {
        let mapped = json!({"kind": "store", "tier": "GPU",
                            "extra_keys": ["9f2c1ab4de77c0315b8ee0a4c6d1f883"]});
        for salt in ["store", "GPU"] {
            let carried = vec![salt.to_string()];
            // what the decorator computes before mapping: the record with the raw salt
            // still in extra_keys
            let before = json!({"kind": "store", "tier": "GPU", "extra_keys": [salt]});
            let pre = preexisting_string_paths(&before, &carried, &["extra_keys", "lora_name"]);
            assert!(
                assert_no_raw(&mapped, &ids(&[]), &carried, &pre).is_ok(),
                "salt {salt:?} aborted on a vocabulary coincidence"
            );
        }
    }

    /// The mapped field's own path is never grandfathered: a raw salt still sitting in
    /// `extra_keys` after mapping is the mapping having failed, coincidence or not.
    #[test]
    fn the_mapped_fields_own_path_is_never_grandfathered() {
        let before = json!({"kind": "store", "extra_keys": ["store"]});
        let carried = vec!["store".to_string()];
        let pre = preexisting_string_paths(&before, &carried, &["extra_keys", "lora_name"]);
        assert_eq!(
            pre,
            vec!["record.kind".to_string()],
            "only the vocabulary coincidence is grandfathered, never the mapped field"
        );
        // mapping failed: the raw salt survived in extra_keys itself
        assert!(assert_no_raw(&before, &ids(&[]), &carried, &pre).is_err());
    }

    /// A raw string the decorator copied to a new path is still a leak: the path did not
    /// hold it before mapping, so no coincidence excuses it.
    #[test]
    fn a_raw_string_at_a_new_path_is_still_caught() {
        let carried = vec!["tenant-AAAA".to_string()];
        let before = json!({"kind": "store", "extra_keys": ["tenant-AAAA"]});
        let pre = preexisting_string_paths(&before, &carried, &["extra_keys", "lora_name"]);
        let leaked = json!({"kind": "store", "extra_keys": ["9f2c1ab4de77c0315b8ee0a4c6d1f883"],
                            "debug_note": "tenant-AAAA"});
        assert!(assert_no_raw(&leaked, &ids(&[]), &carried, &pre).is_err());
    }

    /// The scan counts serialized nodes, including containers, across plain and
    /// flattened structs and provenance maps. Leak paths must match the JSON shape.
    #[test]
    fn the_walk_counts_what_the_tree_holds_and_names_paths_the_same() {
        use crate::clock::ProducerMs;
        use crate::record::{Provenance, Record, SegmentOpen};
        use serde_json::Value;

        fn nodes(v: &Value) -> usize {
            1 + match v {
                Value::Array(items) => items.iter().map(nodes).sum(),
                Value::Object(map) => map.values().map(nodes).sum(),
                _ => 0,
            }
        }
        let mut store = crate::testing::store(7);
        store.extra_keys = Some(vec![crate::record::OperatorText("t".into())]);
        let mut prov = serde_json::Map::new();
        prov.insert("gpu".to_string(), json!("h100"));
        prov.insert("tags".to_string(), json!(["a", "tenant-AAAA"]));
        let header = Record::SegmentOpen(SegmentOpen {
            provenance: Provenance::try_from(prov).unwrap(),
            at_ms: ProducerMs(1.0),
            contract_version: "1.0",
            incarnation: "1-1".to_string(),
            segment_seq: 0,
            workload_class: None,
            heartbeat_secs: 60,
            max_segment_secs: 300,
            key_epoch: Some(0),
        });
        for record in [Record::Store(store), header.clone()] {
            let walked = assert_no_raw(&record, &ids(&[]), &[], &none()).expect("clean");
            assert_eq!(walked, nodes(&record.to_value()), "{record:?}");
        }
        let leak = assert_no_raw(&header, &ids(&[]), &["tenant-AAAA".to_string()], &none())
            .expect_err("the operator string inside the flattened map is found");
        assert_eq!(leak.path, "record.tags[1]");
    }

    #[test]
    fn a_leak_names_its_path_and_kind() {
        let r =
            json!({"kind": "store", "extra_keys": ["tenant-AAAA"], "block_id": HASH.to_string()});
        let leak = assert_no_raw(&r, &ids(&[HASH]), &[], &none()).expect_err("a leak");
        assert_eq!(leak.kind, super::LeakKind::CatalogId);
        assert_eq!(leak.path, "record.block_id");
        let r = json!({"kind": "store", "extra_keys": ["tenant-AAAA"]});
        let leak = assert_no_raw(&r, &ids(&[]), &["tenant-AAAA".to_string()], &none())
            .expect_err("a leak");
        assert_eq!(leak.kind, super::LeakKind::OperatorString);
        assert_eq!(leak.path, "record.extra_keys[0]");
        assert_eq!(
            leak.to_string(),
            "record.extra_keys[0] carries a raw operator string"
        );
    }
}
