#!/usr/bin/env bash
# End to end with no GPU and no engine: the stub publisher replays captured wire bytes over
# a real ZMQ PUB socket, the tap subscribes through its live transport, and what lands on
# disk is asserted: sealed Parquet segments, a clean closing tally and a fresh probe.
#
#   scripts/e2e-stub.sh [path/to/infertap]     # a build with --features zmq
#   PYTHON=/path/to/python (with pyzmq)        # default: python3
#
# Runs once per engine corpus: the vLLM capture under --engine vllm and the SGLang capture
# under --engine sglang, each in raw and keyed mode. Judged by exit code. Run by CI's e2e
# job and by preflight where pyzmq is available.
set -euo pipefail
cd "$(dirname "$0")/.."
tap="${1:-target/debug/infertap}"
py="${PYTHON:-python3}"
"$py" -c 'import zmq' 2>/dev/null || { echo "e2e-stub: $py has no pyzmq" >&2; exit 1; }
[ -x "$tap" ] || { echo "e2e-stub: $tap is not an executable; build with --features zmq" >&2; exit 1; }

port="$("$py" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
out="$(mktemp -d)"
log="$out/tap.log"
cleanup() {
  for pid in "${stub_pid:-}" "${tap_pid:-}"; do
    [ -z "$pid" ] || { kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; }
  done
  rm -rf "${root:-$out}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
root="$out"
for engine in vllm sglang; do
corpus="conformance/$engine-wire"
[ -d "$corpus" ] || { echo "e2e-stub: no corpus under $corpus" >&2; exit 1; }
for mode in raw keyed; do
out="$root/$engine-$mode"
mkdir -p "$out"
log="$out/tap.log"
key_args=(--raw-ids)
if [ "$mode" = keyed ]; then
  # A test-only key generated locally; never a deployment credential.
  "$py" -c 'import secrets; print(secrets.token_hex(64))' > "$out/key"
  chmod 600 "$out/key"
  key_args=(--key-file "$out/key")
fi

# The configuration is validated before anything runs, the way a deploy hook would.
"$tap" check-config --engine "$engine" --engine-endpoint "stub=tcp://127.0.0.1:$port" "${key_args[@]}" --out-dir "$out" >/dev/null

# The tap first (a SUB reconnects until the publisher binds), then the stub: 300 frames at
# 200/s, so the run's --duration ends after the stub is done and nothing is left unsent.
"$tap" --engine "$engine" --engine-endpoint "stub=tcp://127.0.0.1:$port" "${key_args[@]}" --out-dir "$out" \
  --duration 6 --segment-secs 2 --heartbeat-secs 1 --log-format json 2>"$log" &
tap_pid=$!
sleep 0.5
"$py" packaging/smoke/stub-publisher.py --corpus "$corpus" --bind "tcp://127.0.0.1:$port" --rate 200 --count 300 >/dev/null &
stub_pid=$!
tap_exit=0
wait "$tap_pid" || tap_exit=$?
tap_pid=
[ "$tap_exit" -eq 0 ] || { echo "e2e-stub: the tap exited $tap_exit" >&2; cat "$log" >&2; exit 1; }
wait "$stub_pid"
stub_pid=

# What landed: sealed Parquet under the kv stream, and the tally the tap logged.
parquet="$(ls "$out"/kv/seg-*.parquet 2>/dev/null | wc -l | tr -d ' ')"
[ "$parquet" -ge 1 ] || { echo "e2e-stub: no sealed parquet segment under $out/kv" >&2; ls -la "$out/kv" >&2; cat "$log" >&2; exit 1; }
[ -z "$(ls "$out"/kv/*.jsonl 2>/dev/null | grep -v active.jsonl)" ] || { echo "e2e-stub: an unconverted sealed JSONL remains" >&2; ls -la "$out/kv" >&2; exit 1; }
tally="$("$py" - "$log" "$mode" <<'PY'
import json, re, sys
msgs = [json.loads(l)["msg"] for l in open(sys.argv[1]) if l.startswith("{")]
done = [m for m in msgs if m.startswith("run complete:")]
if not done: sys.exit("no closing tally in the log")
kv = dict(re.findall(r"(\w+)=(\S+)", done[-1]))
if int(kv["events"]) < 1000: sys.exit(f"too few events: {kv}")
if int(kv["dropped"]) != 0: sys.exit(f"drops on a gapless publisher: {kv}")
if int(kv["msgs"]) < 250: sys.exit(f"too few messages seen: {kv}")
if sys.argv[2] == "keyed" and int(kv.get("noraw_scanned", "0")) <= 0:
    sys.exit(f"keyed run did not scan mapped output: {kv}")
print(f"events={kv['events']} msgs={kv['msgs']} dropped={kv['dropped']} records={kv['records']}")
PY
)"
# The liveness probe agrees the stream is fresh.
"$tap" probe "${key_args[@]}" --out-dir "$out" --heartbeat-secs 60 >/dev/null
echo "e2e-stub: $engine $mode ok: $parquet sealed parquet segment(s); $tally"
done
done
out="$root"
