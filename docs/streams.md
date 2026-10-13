# Storage layout

The tap writes KV-cache records under `kv/` within its configured output directory.
Lifecycle and loss records share the same stream as cache events. Diagnostics go to stderr.

## Local files

| Path | Purpose |
| --- | --- |
| `<out_dir>/kv/active.jsonl` | Active write-ahead log; may be incomplete. |
| `<out_dir>/kv/seg-*.jsonl` | Sealed WAL awaiting conversion. |
| `<out_dir>/kv/seg-*.parquet.tmp` | Conversion in progress. |
| `<out_dir>/kv/seg-*.parquet` | Complete sealed segment, ready for shipping. |
| `<out_dir>/kv/.writer.lock.jsonl` | Persistent file used for the exclusive writer lock. |
| `<out_dir>/incarnation` | Current tap incarnation, used by the Kubernetes shipper. |
| `<out_dir>/closed-<incarnation>` | Shutdown marker containing the number of unconverted segments. |

Do not ship JSONL, temporary files, or the writer lock. The packaged shippers apply these
exclusions. Do not delete the lock file while a writer is running.

The shutdown marker is written after conversion settles. A zero count means no segments
remain unconverted. The Kubernetes shipper waits for the current incarnation's marker
before its final upload, within the pod's termination budget.

## Object-store layout

```text
<prefix>/kv/YYYY-MM-DD/HH/<node>/seg-<incarnation>-<sequence>.parquet
```

Set `INFERTAP_DEST` to the prefix ending in `/kv/`, including the trailing slash.
The systemd shipper uses the hostname for `<node>`; the Kubernetes shipper uses the pod
name. Dates and hours are UTC shipment time, so delayed uploads appear in the current
shipment window. Record timestamps retain their original event and producer times.

Use distinct destination prefixes where node or pod names could collide across fleets.
Segment headers carry incarnation and sequence metadata; consumers must follow the
[record contract](conformance-notes.md) rather than infer identity from filenames alone.

For retention, shipping, and crash recovery, see [Delivery](install.md#delivery).
