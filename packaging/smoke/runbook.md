# Packaging smoke test runbook

Validate the systemd units and container on a disposable Linux host. The stub publisher
replays captured wire fixtures; no GPU or running inference engine is required.

Check service permissions, sandbox settings, shipping, malformed-message handling, and
container volume access. Record results outside the repository, including software
versions and any failed step.

## Prerequisites

- A Linux host or VM with systemd 247 or newer for `LoadCredential`. Version 254 or newer
  also enables restart backoff; 247–253 uses the fixed restart interval. Check with
  `systemd --version`.
- This repo checked out on that host (the stub replays `conformance/vllm-wire/`).
- `rclone`, `python3` + `pyzmq` + `pyarrow` (the Parquet spot-checks), `jq`. No `libzmq5`:
  the binary links its own statically, and `pyzmq` wheels bundle theirs.
- The release binary: `cargo build --release --features zmq`, or the release tarball
  (which is built with `zmq`; libzmq is statically linked, nothing to install).

## 1. Check sample output

```sh
target/release/infertap show-payload
```

**Expect:** the ingress/egress narration on stderr, 11 JSONL records on stdout,
pseudonymized `block_id`s, exit 0. This is the same sample-output check CI runs inside the
container image.

## 2. Start the test publisher

```sh
python3 packaging/smoke/stub-publisher.py --rate 20
```

**Expect:** `bound tcp://127.0.0.1:5557; N corpus frames; rate 20.0/s`. Leave it running
(its own terminal, or `systemd-run --user --unit stub-pub python3 .../stub-publisher.py`).

## 3. Install the service

Install the binary, service, configuration, and key using the
[systemd installation guide](../../docs/install.md#systemd) from this checkout.
Use a disposable host and a test key. Apply the following test overrides before starting
collection.

Use shorter test intervals through the service environment file. Production defaults
are 300 seconds for segment sealing and 60 seconds for heartbeats:

```sh
sudo tee /etc/default/infertap <<'EOF'
INFERTAP_SEGMENT_SECS=20
INFERTAP_HEARTBEAT_SECS=10
EOF
sudo systemctl daemon-reload && sudo systemctl enable --now infertap
```

Remove the test override file during cleanup.

**Checks:**

```sh
systemctl status infertap                 # active (running); no sandbox denials in journal
journalctl -u infertap -n 20              # "writing sealed segments to /var/lib/infertap/kv (incarnation ...)"
sleep 25 && sudo ls -l /var/lib/infertap/kv  # active.jsonl plus at least one seg-*.parquet;
                                          # a seg-*.jsonl is a conversion not yet finished
sudo head -1 /var/lib/infertap/kv/active.jsonl | jq .kind   # "segment_open": the WAL form, live
sudo python3 -c "import glob, pyarrow.parquet as q; [print(k) for f in sorted(glob.glob('/var/lib/infertap/kv/seg-*.parquet')) for k in q.read_table(f, columns=['kind']).column('kind').to_pylist()]" | sort | uniq -c
                                          # segment_open, agent_start, heartbeat, store/evict/clear
sudo python3 -c "import glob, pyarrow.parquet as q; [print(f, q.read_table(f, columns=['kind']).column('kind')[0].as_py()) for f in sorted(glob.glob('/var/lib/infertap/kv/seg-*.parquet'))]"
                                          # every line ends in segment_open: row 0 of each
                                          # twin is its own header (the sort|uniq line above
                                          # proves presence, not position)
sudo jq -r 'select(.kind=="store") | .block_id' /var/lib/infertap/kv/active.jsonl | head -3
                                          # 32-char hex pseudonyms, never decimal raw ids
```

Record these permissions while the tap runs. Repeat after the lifecycle stop below:

```sh
getent group infertap                   # the static service group persists after stop
stat -c '%a %U %G' /var/lib/infertap      # the mode and owner the shipper will face
```

The tap and shipper must use the same static account.

## 4. Check shipping

Production ships to an object store; the smoke test ships to a local directory through the
packaged unit and its filters. The landing directory needs a drop-in
because the ship unit's `ProtectSystem=strict` sandbox allows writes only
where listed:

```sh
# owned by the service user: the local dir stands in for the object store, so this chown
# grants permission to create the destination prefix
sudo mkdir -p /srv/infertap-landing && sudo chown infertap:infertap /srv/infertap-landing
sudo install -m 0644 packaging/systemd/infertap-ship.service packaging/systemd/infertap-ship.timer /etc/systemd/system/
sudo mkdir -p /etc/infertap
sudo tee /etc/infertap/ship.env <<'EOF'
INFERTAP_DEST=landing:/srv/infertap-landing/
RCLONE_CONFIG_LANDING_TYPE=local
EOF
sudo mkdir -p /etc/systemd/system/infertap-ship.service.d
sudo tee /etc/systemd/system/infertap-ship.service.d/smoke.conf <<'EOF'
[Service]
# Smoke only: the local landing dir stands in for the object store, so the sandbox must
# be allowed to write it. A production deployment ships over the network and does not
# carry this line.
ReadWritePaths=/var/lib/infertap /srv/infertap-landing
EOF
sudo systemctl daemon-reload
```

Run one shipping pass directly before enabling the timer:

```sh
sudo ls /var/lib/infertap/kv/            # note the sealed segments present
sudo systemctl start infertap-ship.service
systemctl status infertap-ship.service   # oneshot: exited, status=0?
journalctl -u infertap-ship -n 30        # rclone stats; any "permission denied"?
sudo ls -R /srv/infertap-landing/        # <date>/<hour>/<hostname>/seg-*.parquet present? No *.jsonl, no *.tmp
sudo ls /var/lib/infertap/kv/            # uploaded segments removed?
```

**Troubleshooting:**

| symptom | meaning |
|---|---|
| segments in landing dir AND removed from `/var/lib/infertap/kv`; `active.jsonl` untouched | Expected: both units run as the same account and can remove uploaded files |
| segments in landing dir but STILL in `/var/lib/infertap/kv` | upload-then-fail-to-delete: local files accumulate and may be uploaded again. Check both units run as the same user and the state dir is owned by it; inspect directory permissions with `stat` and account membership with `getent` |
| `infertap-ship.service` fails at spawn with `216/GROUP` | check for incompatible `DynamicUser` and `SupplementaryGroups` settings: another unit's dynamic group is unresolvable at exec time (`docs/design/design-log.md`, "One static user for both units") |
| `infertap-ship.service` fails to exec (`203/EXEC`) | rclone is not at `/usr/bin/rclone`, the path the unit names |
| rclone runs, `mkdir ... permission denied` | the landing stand-in is not owned by the service user; check ownership before retrying |
| rclone runs, nothing copied | filter or dest problem: check the `--exclude active.jsonl` denylist behavior and `INFERTAP_DEST` expansion (`%H` is the systemd host specifier) |
| unit fails before rclone with a namespace/sandbox error | a sandbox directive this systemd version rejects; record the exact directive from the journal |

Run a second shipping pass:

```sh
sudo systemctl start infertap-ship.service && journalctl -u infertap-ship -n 10
```

**Expect:** a no-op, since nothing new sealed. Re-uploading already-shipped segments would be
unexpected repeated upload.

**Timer check:** `sudo systemctl enable --now infertap-ship.timer`, wait through two ticks
(`OnUnitActiveSec=120`), and confirm segments keep flowing without intervention:
`systemctl list-timers infertap-ship.timer`.

## 5. Check lifecycle and recovery

Stop the shipping timer and wait for any running shipping pass to finish, so the
following checks can inspect the sealed files locally:

```sh
sudo systemctl stop infertap-ship.timer
while systemctl is-active --quiet infertap-ship.service; do sleep 1; done
```

**Orchestrated stop.** `sudo systemctl stop infertap`, then in the LAST sealed segment:

```sh
sudo python3 -c "import glob, pyarrow.parquet as q; [print(r['reason']) for f in sorted(glob.glob('/var/lib/infertap/kv/seg-*.parquet')) for r in q.read_table(f, columns=['kind','reason']).to_pylist() if r['kind']=='agent_stop']"   # "signal"
```

**Clean restart.** `sudo systemctl start infertap` opens a new incarnation. A successful
clean stop seals its active segment, so it should not require orphan recovery.

**Crash recovery.** After the tap opens `active.jsonl`, kill the tap unit on
this disposable host:

```sh
sudo systemctl kill --signal=SIGKILL infertap
journalctl -u infertap -f
```

Wait for the unit's automatic restart. Confirm the orphan was sealed under its original
incarnation with an attributed `segment_recovered` record, and that the new incarnation
resumes writing. The previously sealed segments must remain readable.

**Poison frame (optional but recommended).** Stop the stub with an anchored pattern,
`pkill -f "^python3 .*stub-publisher"`, because an unanchored one matches the SSH
session's own command line and kills the session. Then restart it with `--poison` and
watch the unit from outside:

```sh
python3 packaging/smoke/stub-publisher.py --poison --rate 20
journalctl -u infertap -f          # malformed message warning
systemctl status infertap          # active (running)
```

**Expect:** the malformed message is refused and counted. The tap stays running and
resumes processing valid messages when the stub returns to clean frames. A malformed
message does not require a process restart.

## 6. Check the container

```sh
docker build -f packaging/docker/Dockerfile -t infertap .
docker run --rm infertap show-payload         # sample output inside the image
docker volume create infertap-smoke-out
# Give the container a private copy readable by its runtime UID. Keep the host service's
# original key root-owned and mode 0600.
smoke_key_dir=$(mktemp -d)
sudo install -o 65532 -g 65532 -m 0400 /etc/infertap/pseudonym.key "$smoke_key_dir/key"
docker run --rm --read-only --cap-drop=ALL --security-opt no-new-privileges \
  --memory 1g --cpus 1.5 \
  --network host \
  -v infertap-smoke-out:/out \
  -v "$smoke_key_dir/key":/run/secrets/pseudonym.key:ro \
  infertap --endpoints i0=tcp://127.0.0.1:5557 \
    --key-file /run/secrets/pseudonym.key --out-dir /out --duration 60
sudo rm "$smoke_key_dir/key"
rmdir "$smoke_key_dir"
```

Host networking reaches the loopback publisher. For other network namespaces, follow
the [container guide](../../docs/install.md#container).

**Expect:** no EACCES opening `/out` (the image ships `/out` owned by the runtime uid
precisely so a fresh named volume works), 60 seconds of records and a clean exit. Inspect:
`docker run --rm -v infertap-smoke-out:/out debian:trixie-slim ls -l /out`.

## 7. Record results and clean up

Record the host, systemd and rclone versions, shipping results, malformed-message
handling, and journal warnings in the test report. Do not commit deployment details.

Cleanup:

```sh
sudo systemctl disable --now infertap infertap-ship.timer
sudo systemctl stop infertap-ship.service
sudo rm -f /etc/systemd/system/infertap.service /etc/systemd/system/infertap-ship.service /etc/systemd/system/infertap-ship.timer /etc/default/infertap
sudo rm -rf /etc/systemd/system/infertap-ship.service.d /etc/infertap /var/lib/infertap /srv/infertap-landing
sudo rm /usr/local/bin/infertap && sudo systemctl daemon-reload
sudo userdel infertap
docker volume rm infertap-smoke-out
```

## Scope

This runbook does not verify sustained production load, the live-engine socket inventory,
or uncaptured event shapes. See `docs/trust.md`, section 6, for verification limits.
