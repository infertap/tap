#!/usr/bin/env python3
"""Replay a wire corpus's valid fixtures for GPU-free packaging smoke tests.

Wrap payloads in a three-part envelope: topic, eight-byte big-endian sequence,
and payload. Sequences increase monotonically. With --poison, send a malformed
frame first to check that collection continues after a decode error.

Requires pyzmq. See runbook.md for invocation and expected results.
"""

import argparse
import json
import pathlib
import sys
import time

try:
    import zmq
except ImportError:
    sys.exit("pyzmq is required: pip install pyzmq")


def load_payloads(corpus: pathlib.Path):
    """Return valid, fully modeled fixture payloads in filename order.

    Exclude unknown-event fixtures so the normal smoke workload does not produce
    schema-drift warnings. Malformed frames are exercised separately by --poison.
    """
    payloads = []
    for path in sorted(corpus.glob("*.json")):
        fixture = json.loads(path.read_text())
        expect = fixture.get("expect", {})
        if "events" in expect and not expect.get("unknown_types"):
            payloads.append((path.name, bytes.fromhex(fixture["wire_hex"])))
    if not payloads:
        sys.exit(f"no cleanly-modelled wire fixtures under {corpus}")
    return payloads


def main():
    default_corpus = pathlib.Path(__file__).resolve().parents[2] / "conformance" / "vllm-wire"
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--bind", default="tcp://127.0.0.1:5557", help="PUB bind endpoint")
    ap.add_argument("--rate", type=float, default=20.0, help="frames per second")
    ap.add_argument(
        "--count", type=int, default=0, help="stop after N frames (0 = run until interrupted)"
    )
    ap.add_argument("--corpus", default=str(default_corpus), help="path to conformance/vllm-wire")
    ap.add_argument(
        "--poison",
        action="store_true",
        help="send one malformed single-part frame first; the tap must count and skip it "
        "and continue processing valid frames",
    )
    args = ap.parse_args()

    payloads = load_payloads(pathlib.Path(args.corpus))
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.bind(args.bind)
    print(f"bound {args.bind}; {len(payloads)} corpus frames; rate {args.rate}/s", flush=True)
    # PUB/SUB has no join barrier: a frame published before a subscription is live is
    # silently dropped. The tap reconnects and re-subscribes on its own, and the seq is
    # monotonic from here, so anything missed before the join shows as nothing at all,
    # not as a gap. A short pause lets the first frames land for a tap already up.
    time.sleep(0.5)

    if args.poison:
        pub.send(b"malformed-single-part-frame")
        print("malformed frame sent: expect a counted refusal and continued processing", flush=True)

    seq = 0
    sent = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    try:
        while not args.count or sent < args.count:
            _name, payload = payloads[sent % len(payloads)]
            pub.send_multipart([b"", seq.to_bytes(8, "big"), payload])
            seq += 1
            sent += 1
            if sent % 500 == 0:
                print(f"sent {sent}", flush=True)
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        pass
    print(f"done: {sent} frames sent", flush=True)


if __name__ == "__main__":
    main()
