// SPDX-License-Identifier: Apache-2.0
use inference_events::{validate_field, Record};
use serde_json::{json, Value};

fn store() -> Value {
    json!({"kind":"store","at_ms":1.25,"instance_id":"i0","block_id":"340282366920938463463374607431768211455","n_tokens":16,"tier":"GPU"})
}
#[test]
fn full_width_ids_fractional_clocks_and_extensions_round_trip() {
    let mut value = store();
    value["content_id"] = json!("0123456789abcdef0123456789abcdef");
    value["future_field"] = json!({"nested":[1,"x"]});
    assert_eq!(
        Record::decode(value.clone()).unwrap().to_value().unwrap(),
        value
    );
}
#[test]
fn malformed_records_fail_without_echoing_values() {
    let mut cases = Vec::new();
    let mut v = store();
    v.as_object_mut().unwrap().remove("block_id");
    cases.push(v);
    let mut v = store();
    v["block_id"] = json!(7);
    cases.push(v);
    let mut v = store();
    v["n_tokens"] = json!(-1);
    cases.push(v);
    let mut v = store();
    v["n_tokens"] = json!(u64::MAX);
    cases.push(v);
    let mut v = store();
    v["dp_rank"] = json!(4294967296u64);
    cases.push(v);
    let mut v = store();
    v["extra_keys"] = json!([true]);
    cases.push(v);
    let mut v = store();
    v["content_id"] = Value::Null;
    cases.push(v);
    let mut v = store();
    v["parent_unknown"] = json!(true);
    v["parent_id"] = json!("synthetic-secret");
    cases.push(v);
    let mut v = store();
    v["n_tokens"] = json!("synthetic-secret");
    cases.push(v);
    for case in cases {
        let error = Record::decode(case).unwrap_err().to_string();
        assert!(!error.contains("synthetic-secret"));
    }
}
#[test]
fn omitted_empty_and_nullable_values_preserve_meaning() {
    for (field, value) in [
        ("extra_keys", json!([])),
        ("tier", Value::Null),
        ("parent_unknown", json!(false)),
    ] {
        let mut v = store();
        v[field] = value;
        assert_eq!(Record::decode(v.clone()).unwrap().to_value().unwrap(), v);
    }
    let v = store();
    assert_eq!(Record::decode(v.clone()).unwrap().to_value().unwrap(), v);
}
#[test]
fn future_kinds_survive_with_their_envelope() {
    let value = json!({"kind":"future_event","at_ms":9.5,"future":{"arbitrary":true}});
    assert_eq!(
        Record::decode(value.clone()).unwrap().to_value().unwrap(),
        value
    );
    assert!(Record::decode(json!({"kind":"future_event"})).is_err());
}
#[test]
fn canary_and_nested_endpoint_rules_are_checked() {
    let mut v = json!({"kind":"agent_start","at_ms":0,"agent_version":"1.0.0","egress":"pseudonymized","endpoint_count":1,"max_payload_bytes":1024});
    assert!(Record::decode(v.clone()).is_err());
    v["canary"] = json!("canary");
    assert!(Record::decode(v).is_ok());
    assert!(validate_field("heartbeat", "endpoints", &json!([{"source":"x"}])).is_err());
    assert!(validate_field("store", "extra_keys", &json!(["salt"])).is_ok());
    assert!(validate_field("store", "n_tokens", &json!("bad")).is_err());
}
#[test]
fn ordering_constraints_are_checked() {
    let v = json!({"kind":"identity_refused","at_ms":2,"instance_id":"i0","dp_rank":0,"group_idx":0,"refused":1,"window_start_ms":3,"window_end_ms":1});
    assert!(Record::decode(v).is_err());
}

#[test]
fn optional_nullable_tier_keeps_explicit_null() {
    for value in [
        serde_json::json!({"kind":"evict","at_ms":1.0,"instance_id":"i0","block_id":"7","tier":null}),
        serde_json::json!({"kind":"evict","at_ms":1.0,"instance_id":"i0","block_id":"7"}),
    ] {
        assert_eq!(
            Record::decode(value.clone()).unwrap().to_value().unwrap(),
            value
        );
    }
}

#[test]
fn rust_and_standard_json_schema_share_acceptance_cases() {
    let cases: Value = serde_json::from_str(include_str!("structure.json")).unwrap();
    for case in cases.as_array().unwrap() {
        assert_eq!(
            Record::decode(case["record"].clone()).is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn sequence_ordering_accepts_equivalent_integer_representations() {
    for first in [json!(3), json!(3.0)] {
        for last in [json!(5), json!(5.0)] {
            let mut value = json!({"kind":"segments_dropped","at_ms":1.0,
                "incarnation":"1-1","count":3,"first":"a","last":"b",
                "first_seq":first,"last_seq":last});
            assert!(Record::decode(value.clone()).is_ok());
            value["first_seq"] = last;
            value["last_seq"] = first.clone();
            assert!(Record::decode(value)
                .unwrap_err()
                .to_string()
                .contains("precedes"));
        }
    }
}

#[test]
fn backend_identity_round_trip() {
    for kind in ["store", "evict"] {
        let mut value = store();
        value["kind"] = json!(kind);
        value["backend_id"] = json!("opaque-backend");
        assert_eq!(
            Record::decode(value.clone()).unwrap().to_value().unwrap(),
            value
        );
        value["backend_id"] = Value::Null;
        assert!(Record::decode(value.clone()).is_err());
        value["backend_id"] = json!(42);
        assert!(Record::decode(value).is_err());
    }
}

#[test]
fn extension_collisions_fail_before_serialization() {
    use inference_events::{ErrorKind, KnownRecord};
    for name in ["kind", "block_id", "n_tokens", "backend_id"] {
        let mut record = Record::decode(store()).unwrap();
        let Record::Known(known) = &mut record else {
            panic!("expected known record");
        };
        let KnownRecord::Store(value) = known.as_mut() else {
            panic!("expected store");
        };
        value.extensions.insert(name.into(), json!("replacement"));
        assert!(serde_json::to_value(&*value).is_err());
        let error = record.to_value().unwrap_err();
        assert_eq!(error.kind, ErrorKind::ExtensionCollision);
        assert!(error.path.ends_with(name));
        assert!(!error.to_string().contains("replacement"));
    }
}

#[test]
fn nested_extension_collisions_fail_before_serialization() {
    use inference_events::{ErrorKind, KnownRecord};
    let value = json!({
        "kind":"heartbeat", "at_ms":1, "msgs_seen":0, "dropped":0,
        "oversized":0, "unknown_types":0, "events_ingested":0,
        "content_unresolved":0, "content_bridge_entries":0,
        "content_bridge_evicted":0, "publisher_restarts":0,
        "endpoints":[{"source":"engine", "endpoint":"tcp://engine:5557",
                      "msgs_seen":0, "dropped":0}]
    });
    let mut record = Record::decode(value).unwrap();
    let Record::Known(known) = &mut record else {
        panic!("expected known record")
    };
    let KnownRecord::Heartbeat(heartbeat) = known.as_mut() else {
        panic!("expected heartbeat")
    };
    heartbeat.endpoints[0]
        .extensions
        .insert("source".into(), json!("replacement"));
    assert!(serde_json::to_value(&*heartbeat).is_err());
    let error = record.to_value().unwrap_err();
    assert_eq!(error.kind, ErrorKind::ExtensionCollision);
    assert_eq!(error.path, "record.endpoints[0].source");
}

#[test]
fn producer_validation_rejects_unknown_kinds_but_readers_preserve_them() {
    use inference_events::{validate_producer_record, ErrorKind};
    let future = json!({"kind":"future_event", "at_ms":1, "payload":{"kind":"nested"}});
    assert!(Record::decode(future.clone()).is_ok());
    assert_eq!(
        validate_producer_record(&future).unwrap_err().kind,
        ErrorKind::UnknownKind
    );
    assert!(validate_producer_record(&store()).is_ok());
}

#[test]
fn validation_errors_distinguish_missing_fields_and_incorrect_types() {
    use inference_events::ErrorKind;
    let missing = Record::decode(json!({"kind":"store"})).unwrap_err();
    let wrong = Record::decode(json!({"kind":"store", "at_ms":"private-input"})).unwrap_err();
    assert_eq!(missing.kind, ErrorKind::MissingField);
    assert_eq!(wrong.kind, ErrorKind::IncorrectType);
    assert_eq!(missing.path, wrong.path);
    assert!(!wrong.to_string().contains("private-input"));
}
