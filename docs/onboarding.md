# Engine configuration and verification

Use this guide after [installing the tap](install.md). The engine baselines are
vLLM 0.30.0 and SGLang 0.5.20; see [Compatibility and support](compatibility.md) for
supported features.

## Configure the publisher

Enable prefix caching and configure the ZMQ KV-event publisher:

```sh
vllm serve <model> --kv-events-config '{"enable_kv_cache_events": true, "publisher": "zmq", "endpoint": "tcp://*:5557", "replay_endpoint": "tcp://*:5657"}'
```

For SGLang, the publisher takes the same configuration without the enable key, and the
tap takes `--engine sglang`:

```sh
python3 -m sglang.launch_server --model-path <model> --kv-events-config '{"publisher": "zmq", "endpoint": "tcp://*:5557", "replay_endpoint": "tcp://*:5657"}'
```

Before starting the engine, isolate the event ports. They carry tokenized prompts without
authentication. On a host, apply [packaging/systemd/kv-events.nft](../packaging/systemd/kv-events.nft)
and persist the rule in your firewall configuration. In Kubernetes, follow the
[network policy instructions](../packaging/kubernetes/README.md#verify-network-isolation).
The tap connects over loopback in the engine's network namespace.

Configure matching tap endpoints:

```toml
engine = "vllm"
engine_endpoint = ["i0=tcp://127.0.0.1:5557"]
engine_replay_endpoint = ["i0=tcp://127.0.0.1:5657"]
```

Use a wildcard bind address on the engine and a dialable address on the tap. For data
parallelism, set the tap's rank parameters and isolate the full port range as described
in [Data parallelism](install.md#data-parallelism).

Replay is optional but recommended. It recovers available history on attachment and
deduplicates the overlap with live traffic. An absent, expired, or truncated replay
history can leave content identities unresolved until sufficient new events arrive.

## Verify collection

1. Validate the configuration with `infertap check-config`, using the same configuration
   and key as the running service.
2. Start the tap and send requests to the engine.
3. Check the tap's logs for incoming messages and reported errors.
4. Wait for a segment to seal: five minutes by default, or the configured `segment_secs`.
   Check `<out_dir>/kv/` for `seg-*.parquet` files. An active shipper may already have
   moved them to the destination.
5. Check the destination for uploaded segments and inspect shipper logs for failures.

`infertap probe --out-dir <out_dir>` checks local output freshness. Use the running
tap's heartbeat and segment settings when probing. A successful probe does not establish
engine connectivity or successful shipping; check those separately.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| No messages under load | Enable prefix caching and KV events. Check the publisher bind address, network namespace, and global-rank port offsets. |
| No sealed files | Check output permissions and disk space. Wait for `segment_secs` or stop the tap cleanly to seal. Check whether the shipper already moved the files. |
| `content_unresolved` increases | Check replay availability, missing parent or group metadata, and identity-state capacity. Some engine events cannot provide complete identities. |
| `identity_refused` records | Review [identity-state sizing](install.md#deriving---resident-blocks) and the RSS limit. |
| `dropped` or `oversized` increases | Inspect publisher sequence gaps and message sizes. These counters indicate incomplete collection and can overlap. |
| `unknown_types` increases | Check the engine version against the support baseline. |
| Old sealed segments are discarded | Restore shipping or increase retention and disk capacity. Retention discards are reported in the stream. |
| S3 shipping returns `AccessDenied` | Check destination credentials and prefix-scoped PutObject, GetObject, and ListBucket permissions. |
| Content identities differ across instances | Check model, tokenizer, block size, cache salts, LoRA names, pseudonymization key, and key epoch. |

A message can contain multiple events. Loss counters do not give an exact missing-record
count, and loss after the last observed sequence number may remain undetected.
See [Compatibility and support](compatibility.md#limitations) for identity limitations.

## Data handling

Keep prompt content and secrets out of labels and provenance. The adapter discards token
IDs after deriving identities; operator-supplied text remains operator-controlled data.
Use the same pseudonymization key and epoch only where cross-instance identity comparison
is intended. Engine `PYTHONHASHSEED` and prefix-cache hash settings do not affect the tap's
content identity calculation.

Ship sealed Parquet segments using the packaged shipper. Keep active JSONL, conversion
sources, and temporary files local. Allow disk headroom beyond the sealed-segment retention
limit for the active segment and conversion work. See [Delivery](install.md#delivery).
