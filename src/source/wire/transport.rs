// SPDX-License-Identifier: Apache-2.0
//! Connect to configured ZMQ publisher and replay endpoints.
//! This feature-gated module owns the sockets; framing, decoding, and normalization
//! operate above `FrameTransport`. Live and in-memory transports share that pipeline.

use std::io;
use std::time::Duration;

use super::{FrameTransport, Message, MAX_PAYLOAD_BYTES};
use crate::source::{Endpoint, Shed, SourceKey};

/// Budget used to size per-socket message queues.
/// Divide by estimated message bytes and socket count, then clamp to the configured
/// HWM range. This absorbs brief stalls; it is not a byte limit. RSS shedding and
/// OS memory limits provide separate controls.
const AGGREGATE_RECV_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Mean payload observed in the vLLM 0.26.0 capture (~6.5 KiB), the denominator of the
/// aggregate budget.
const OBSERVED_MEAN_PAYLOAD_BYTES: u64 = 6_656;

/// Maximum receive high-water mark per socket.
const RECV_HWM_CEILING: i32 = 10_000;

/// libzmq's own default, now the floor: the width past which the aggregate budget stops
/// shrinking the per-socket mark (~40 sockets) and the guard is the only bound left.
const RECV_HWM_FLOOR: i32 = 1_000;

/// The per-socket receive high-water mark for a tap subscribed to `sources` endpoints.
fn recv_hwm_for(sources: usize) -> i32 {
    let n = sources.max(1) as u64;
    let per = AGGREGATE_RECV_BUDGET_BYTES / (OBSERVED_MEAN_PAYLOAD_BYTES * n);
    i32::try_from(per)
        .unwrap_or(RECV_HWM_CEILING)
        .clamp(RECV_HWM_FLOOR, RECV_HWM_CEILING)
}

/// Discard queued messages without decoding them when the RSS policy requests it.
/// Leave sequence tracking unchanged so later accepted messages can expose gaps.
/// Loss after the last accepted message may remain undetected.
impl ZmqSubTransport {
    /// One sweep: at most one queue's worth per socket, so a publisher that refills
    /// faster than the sweep drains cannot hold the loop here. What a bounded sweep
    /// leaves, the next check sweeps; the loop checks on every wake and inside every
    /// drain.
    pub(crate) fn shed_ready(&mut self) -> Shed {
        let budget = recv_hwm_for(self.sockets.len());
        self.shed_up_to(budget)
    }

    /// The sweep with its per-socket budget explicit, for the test that pins the bound.
    pub(crate) fn shed_up_to(&mut self, per_socket: i32) -> Shed {
        let mut shed = Shed::default();
        for (_source, socket) in &self.sockets {
            let mut left = per_socket;
            // EAGAIN ends the drain of this socket. A real socket error is the poll
            // loop's to classify on its next wake, not the shed's.
            while left > 0 {
                let Ok(parts) = socket.recv_multipart(zmq::DONTWAIT) else {
                    break;
                };
                left -= 1;
                shed.messages += 1;
                shed.bytes += parts.iter().map(Vec::len).sum::<usize>() as u64;
            }
        }
        shed
    }
}

/// Per-receive timeout for replay. An unavailable endpoint ends the drain after
/// this timeout, allowing collection to proceed to live traffic.
const REPLAY_RECV_TIMEOUT_MS: i32 = 1_000;

/// Deadline for one replay endpoint, including a publisher that keeps responding.
const REPLAY_PHASE_DEADLINE: Duration = Duration::from_secs(5);

/// Aggregate replay-buffer budget across endpoints.
/// Stop replay when this budget is reached and report the drain as incomplete.
const REPLAY_BYTE_BUDGET: u64 = AGGREGATE_RECV_BUDGET_BYTES / 4;

/// A SUB socket per source, polled for the next ready message.
pub struct ZmqSubTransport {
    // The context outlives the sockets, and the replay drain creates its DEALERs from it.
    ctx: zmq::Context,
    sockets: Vec<(SourceKey, zmq::Socket)>,
    /// Fan-in cursor: the socket considered first on the next poll. Rotates past whichever
    /// socket was serviced, so every ready socket is reached within one cursor lap no
    /// matter how busy the others are (see `poll`).
    next: usize,
    /// Messages recovered from the publisher's replay ring at connect time, served by
    /// `poll` before live traffic so the stream a consumer sees is one ordered history.
    replayed: std::collections::VecDeque<Message>,
    /// Highest replayed sequence per source: the dedup boundary. The SUB connects before
    /// the replay drain, so a message published during the drain arrives on both paths.
    /// The live copy is dropped here, because a duplicate through the framing layer would
    /// read as a publisher restart and double-ingest every record it carries.
    replay_high: std::collections::HashMap<SourceKey, u64>,
    /// Last live sequence per source while replay deduplication is active.
    /// A regression clears the replay watermark and reaches the subscriber as reset
    /// evidence. A restart before the first live message is ambiguous: new messages
    /// at or below the replay watermark can be mistaken for overlap.
    live_seq: std::collections::HashMap<SourceKey, u64>,
    /// Drains that ended on a timeout or error rather than the END marker. The cost is
    /// less history, which the reader's own guards already declare (`NotComparable` and
    /// the left-open evict); nothing here fails the run.
    replay_incomplete: u64,
}

impl std::fmt::Debug for ZmqSubTransport {
    /// The sources and the attach state; the sockets themselves have no useful Debug.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqSubTransport")
            .field(
                "sources",
                &self.sockets.iter().map(|(s, _)| s).collect::<Vec<_>>(),
            )
            .field("replayed", &self.replayed.len())
            .field("replay_incomplete", &self.replay_incomplete)
            .finish_non_exhaustive()
    }
}

impl ZmqSubTransport {
    /// Connect a SUB socket to each `(source_id, endpoint)` and subscribe to all topics.
    /// `endpoint` is a ZMQ address such as `tcp://127.0.0.1:5557`.
    ///
    /// # Errors
    /// Returns the libzmq error if a socket cannot be created, subscribed, or connected.
    pub fn connect(endpoints: &[Endpoint]) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let mut sockets = Vec::with_capacity(endpoints.len());
        let hwm = recv_hwm_for(endpoints.len());
        for Endpoint { source, address } in endpoints {
            let socket = ctx.socket(zmq::SUB)?;
            socket.set_subscribe(b"")?;
            // ZMQ_MAXMSGSIZE: libzmq refuses an oversized message part before allocating
            // it (and disconnects the sending peer; SUB reconnects automatically). First
            // line of the payload bound; the Subscriber re-checks above the transport seam
            // so non-ZMQ transports are bounded too.
            socket.set_maxmsgsize(i64::try_from(MAX_PAYLOAD_BYTES).unwrap_or(i64::MAX))?;
            // Absorb the sub-second stalls, notably a segment seal's fsync, that would
            // otherwise drop messages libzmq had already received. Sized against the
            // aggregate budget, not per socket (see `recv_hwm_for`).
            socket.set_rcvhwm(hwm)?;
            socket.connect(address)?;
            sockets.push((source.clone(), socket));
        }
        Ok(Self {
            ctx,
            sockets,
            next: 0,
            replayed: std::collections::VecDeque::new(),
            replay_high: std::collections::HashMap::new(),
            live_seq: std::collections::HashMap::new(),
            replay_incomplete: 0,
        })
    }

    /// Fetch retained history from sequence zero and serve it before live messages.
    /// The DEALER request is `[empty, start_seq]`. An empty response payload marks
    /// completion. Timeouts, the endpoint deadline, and the aggregate byte budget
    /// stop incomplete drains without preventing live collection.
    /// Call after `connect`; `poll` deduplicates observed replay/live overlap.
    ///
    /// # Errors
    /// Returns socket creation or socket-option errors. Connect, send, and receive
    /// failures are counted as incomplete drains.
    pub fn drain_replay(&mut self, replay: &[Endpoint]) -> Result<(), zmq::Error> {
        let mut replayed_bytes: u64 = self
            .replayed
            .iter()
            .map(|m| m.parts.iter().map(Vec::len).sum::<usize>() as u64)
            .sum();
        for Endpoint { source, address } in replay {
            let started = std::time::Instant::now();
            let socket = self.ctx.socket(zmq::DEALER)?;
            // Disable linger so pending replay requests cannot block socket teardown
            // when the endpoint is unavailable.
            socket.set_linger(0)?;
            socket.set_rcvtimeo(REPLAY_RECV_TIMEOUT_MS)?;
            socket.set_sndtimeo(REPLAY_RECV_TIMEOUT_MS)?;
            socket.set_maxmsgsize(i64::try_from(MAX_PAYLOAD_BYTES).unwrap_or(i64::MAX))?;
            if socket.connect(address).is_err()
                || socket
                    .send_multipart([&b""[..], &0u64.to_be_bytes()[..]], 0)
                    .is_err()
            {
                self.replay_incomplete += 1;
                continue;
            }
            loop {
                if started.elapsed() >= REPLAY_PHASE_DEADLINE
                    || replayed_bytes >= REPLAY_BYTE_BUDGET
                {
                    // A ring that outlasts the deadline or the budget is history past what
                    // startup may spend on it; the rest is declared missing by the reader.
                    self.replay_incomplete += 1;
                    break;
                }
                let Ok(mut frames) = socket.recv_multipart(0) else {
                    // Timeout or transport error: the ring beyond this point is history
                    // the reader's guards will declare missing, not a failure.
                    self.replay_incomplete += 1;
                    break;
                };
                if frames.first().is_some_and(Vec::is_empty) {
                    frames.remove(0);
                }
                // The end marker is the frame set whose payload is empty: the publisher
                // terminates with END_SEQ and an empty payload frame.
                if frames.len() != 3 || frames[2].is_empty() {
                    break;
                }
                if let Ok(seq) = <[u8; 8]>::try_from(frames[1].as_slice()) {
                    let seq = u64::from_be_bytes(seq);
                    let high = self.replay_high.entry(source.clone()).or_insert(0);
                    *high = (*high).max(seq);
                }
                replayed_bytes += frames.iter().map(Vec::len).sum::<usize>() as u64;
                self.replayed.push_back(Message {
                    source: source.clone(),
                    parts: frames,
                });
            }
        }
        Ok(())
    }

    /// Drains that ended early (timeout or error) rather than on the END marker.
    #[must_use]
    pub fn replay_incomplete(&self) -> u64 {
        self.replay_incomplete
    }
}

fn to_io(e: zmq::Error) -> io::Error {
    io::Error::other(e)
}

impl FrameTransport for ZmqSubTransport {
    fn shed_queue(&mut self) -> Shed {
        self.shed_ready()
    }

    fn poll(&mut self, timeout: Duration) -> io::Result<Option<Message>> {
        // Recovered history first, in ring order: the consumer sees one ordered stream and
        // never learns which transport leg a message rode.
        if let Some(m) = self.replayed.pop_front() {
            return Ok(Some(m));
        }
        let millis = i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX);
        let mut items: Vec<zmq::PollItem> = self
            .sockets
            .iter()
            .map(|(_, s)| s.as_poll_item(zmq::POLLIN))
            .collect();
        if zmq::poll(&mut items, millis).map_err(to_io)? == 0 {
            return Ok(None); // idle: nothing ready within the timeout
        }
        // Start at the rotating cursor to prevent a continuously ready socket from
        // starving later sockets. Advance past the socket serviced on this poll.
        let n = items.len();
        let ready = (0..n)
            .map(|i| (self.next + i) % n)
            .find(|&i| items[i].is_readable());
        drop(items);
        match ready {
            Some(i) => {
                self.next = (i + 1) % n;
                let parts = self.sockets[i].1.recv_multipart(0).map_err(to_io)?;
                // Suppress live copies already returned by replay. A live sequence regression
                // clears the watermark so the subscriber can observe the reset. This peek
                // reads only the sequence; the framing layer still validates the envelope.
                let source = &self.sockets[i].0;
                if let Some(seq) = parts
                    .get(1)
                    .and_then(|f| <[u8; 8]>::try_from(f.as_slice()).ok())
                    .map(u64::from_be_bytes)
                {
                    let prev = self.live_seq.insert(source.clone(), seq);
                    if prev.is_some_and(|p| seq < p) {
                        self.replay_high.remove(source);
                    } else if self
                        .replay_high
                        .get(source)
                        .is_some_and(|high| seq <= *high)
                    {
                        return Ok(None); // deduplicated; the caller polls again
                    }
                }
                Ok(Some(Message {
                    source: self.sockets[i].0.clone(),
                    parts,
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::time::{Duration, Instant};

    use super::ZmqSubTransport;
    use crate::source::vllm::Vllm;
    use crate::source::wire::{FrameTransport, Message, Subscriber};
    use crate::source::{Endpoint, EventSource as _, Shed, SourceKey};

    fn ep(source: &str, address: impl Into<String>) -> Endpoint {
        Endpoint {
            source: SourceKey::from(source),
            address: address.into(),
        }
    }

    /// An address nothing listens on: bound by the kernel's choice and released, so a
    /// fixed port another process happens to hold cannot make the test see a peer.
    fn silent_address() -> String {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PUB).expect("socket");
        socket.bind("tcp://127.0.0.1:*").expect("bind");
        let endpoint = socket
            .get_last_endpoint()
            .expect("endpoint")
            .expect("utf8 endpoint");
        drop(socket);
        drop(ctx);
        endpoint
    }

    /// A sweep is bounded per socket: a queue deeper than the budget is drained over
    /// several sweeps, never in one that a refilling publisher could extend without end.
    /// Nothing is lost to the bound; what one sweep leaves, the next one takes.
    #[test]
    fn a_shed_sweep_takes_at_most_its_budget_and_the_rest_waits_for_the_next() {
        let ctx = zmq::Context::new();
        let publisher = ctx.socket(zmq::PUB).expect("pub");
        publisher.bind("tcp://127.0.0.1:*").expect("bind");
        let endpoint = publisher.get_last_endpoint().expect("ep").expect("utf8");
        let mut t = ZmqSubTransport::connect(&[ep("i0", endpoint)]).expect("connect");
        std::thread::sleep(Duration::from_millis(100));
        let total = 20u64;
        for seq in 0..total {
            publisher
                .send_multipart([&b"topic"[..], &seq.to_be_bytes(), b"payload"], 0)
                .expect("send");
        }
        // Everything is queued once the last message is readable: TCP orders the rest
        // behind it. Wait for it without consuming anything.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut items = [t.sockets[0].1.as_poll_item(zmq::POLLIN)];
            zmq::poll(&mut items, 50).expect("poll");
            if items[0].is_readable() {
                std::thread::sleep(Duration::from_millis(100));
                break;
            }
            assert!(Instant::now() < deadline, "nothing arrived");
        }
        let mut swept = 0u64;
        let mut sweeps = 0u32;
        while swept < total {
            let shed = t.shed_up_to(5);
            assert!(shed.messages <= 5, "a sweep exceeded its budget: {shed:?}");
            if shed.messages == 0 {
                std::thread::sleep(Duration::from_millis(10));
            } else {
                sweeps += 1;
            }
            swept += shed.messages;
            assert!(
                Instant::now() < deadline,
                "the queue never drained: {swept}/{total}"
            );
        }
        assert_eq!(swept, total, "nothing is lost to the bound");
        assert!(
            sweeps >= 4,
            "twenty messages take at least four sweeps of five"
        );
    }

    #[test]
    fn shed_drains_everything_queued_and_the_stream_survives() {
        // The memory shed's transport half: everything queued is discarded undecoded
        // and counted, and the next message flows normally. The stream is gapped, not
        // torn, and the gap is the declaration.
        let ctx = zmq::Context::new();
        let publisher = ctx.socket(zmq::PUB).expect("pub");
        publisher.bind("tcp://127.0.0.1:*").expect("bind");
        let endpoint = publisher.get_last_endpoint().expect("ep").expect("utf8");
        let mut t = ZmqSubTransport::connect(&[ep("i0", endpoint)]).expect("connect");
        // Let the SUB attach (PUB drops pre-subscription sends), then queue three.
        std::thread::sleep(Duration::from_millis(100));
        for seq in 0..3u64 {
            publisher
                .send_multipart([&b"topic"[..], &seq.to_be_bytes(), b"payload"], 0)
                .expect("send");
        }
        // Wait until at least one is queued so the sweep has something real to drain.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let shed = t.shed_ready();
            if shed.messages > 0 {
                assert!(shed.bytes > 0, "discarded messages carry their byte count");
                break;
            }
            assert!(Instant::now() < deadline, "nothing ever arrived to shed");
            std::thread::sleep(Duration::from_millis(10));
        }
        // Drained: an immediate second sweep finds nothing.
        // (Arrivals mid-sweep wait for the next; none are in flight here.)
        assert_eq!(
            t.shed_ready(),
            Shed::default(),
            "a drained queue sheds nothing"
        );
        // The stream survives: a fresh message is received normally.
        publisher
            .send_multipart([&b"topic"[..], &9u64.to_be_bytes(), b"after-shed"], 0)
            .expect("send");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(Message { source, parts }) =
                t.poll(Duration::from_millis(50)).expect("poll")
            {
                assert_eq!(source.as_str(), "i0");
                assert_eq!(parts[2], b"after-shed");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the post-shed message never arrived"
            );
        }
    }

    #[test]
    fn recv_hwm_scales_with_fleet_width_between_its_bounds() {
        // One socket reaches the ceiling; 27 sockets share the aggregate budget.
        // Larger endpoint counts reach the floor. Check the arithmetic at each scale.
        assert_eq!(super::recv_hwm_for(1), 10_000);
        assert_eq!(super::recv_hwm_for(27), 1_493);
        assert_eq!(super::recv_hwm_for(1_000), 1_000);
        assert_eq!(super::recv_hwm_for(0), 10_000); // degenerate, never constructed
    }

    #[test]
    fn connect_and_idle_poll_without_a_publisher() {
        // Connecting a SUB to an endpoint with no publisher is fine (connect is lazy); polling
        // then returns idle. Exercises the socket setup + poll + timeout path hermetically,
        // with no engine running.
        let mut t = ZmqSubTransport::connect(&[ep("i0", silent_address())]).expect("connect");
        assert!(t.poll(Duration::from_millis(20)).expect("poll").is_none());
    }

    #[test]
    fn subscriber_over_the_real_socket_is_idle_until_fed() {
        let t = ZmqSubTransport::connect(&[ep("i0", silent_address())]).expect("connect");
        let mut sub = Subscriber::new(t, &Vllm, Duration::from_millis(20));
        assert_eq!(sub.poll().expect("poll"), 0); // idle, stream still open
        assert_eq!(sub.stats().messages_seen, 0);
    }

    // --- the live data path: real frames through a real bound PUB socket ---
    //
    // Everything above the FrameTransport seam is tested against mocks. These are the only
    // tests in which libzmq carries bytes, so they pin the properties a mock cannot:
    // multipart integrity through recv_multipart, source attribution, and the fan-in
    // schedule. Ports are never hardcoded: the PUB binds a wildcard port and the SUB
    // connects to whatever the OS granted, so a stray local publisher cannot flip an
    // assertion.

    /// The publisher's replay half, speaking the engines' protocol: a ROUTER that answers one
    /// request with `msgs` as `(client, empty, topic, seq8, payload)` frames and terminates
    /// with the empty-payload end marker.
    fn bind_replay_router(ctx: &zmq::Context, msgs: Vec<(u64, &'static [u8])>) -> String {
        let router = ctx.socket(zmq::ROUTER).expect("router");
        router.bind("tcp://127.0.0.1:*").expect("bind");
        let endpoint = router
            .get_last_endpoint()
            .expect("endpoint")
            .expect("utf8 endpoint");
        std::thread::spawn(move || {
            let Ok(req) = router.recv_multipart(0) else {
                return;
            };
            let client = &req[0];
            for (seq, payload) in msgs {
                router
                    .send_multipart(
                        [
                            client.as_slice(),
                            b"",
                            b"topic",
                            &seq.to_be_bytes(),
                            payload,
                        ],
                        0,
                    )
                    .expect("replay send");
            }
            // The end marker: END_SEQ with an EMPTY payload frame.
            router
                .send_multipart(
                    [client.as_slice(), b"", b"", &(-1i64).to_be_bytes(), b""],
                    0,
                )
                .expect("end marker");
            // Hold the socket briefly so the frames flush before drop.
            std::thread::sleep(Duration::from_millis(200));
        });
        endpoint
    }

    /// The replay contract in one test: the drained ring is served before live traffic,
    /// the overlap a concurrent publish creates is deduplicated by sequence, and history
    /// plus live read as one ordered stream through the framing layer above.
    #[test]
    fn replay_drains_first_and_the_overlap_deduplicates() {
        let ctx = zmq::Context::new();
        let (publisher, live_endpoint) = bind_pub(&ctx);
        // The ring holds seqs 1 and 2; seq 2 also arrives live, which is the overlap.
        let replay_endpoint =
            bind_replay_router(&ctx, vec![(1, b"replayed-one"), (2, b"replayed-two")]);

        let mut t = ZmqSubTransport::connect(&[ep("i0", live_endpoint)]).expect("connect");
        t.drain_replay(&[ep("i0", replay_endpoint)]).expect("drain");
        assert_eq!(t.replay_incomplete(), 0, "the ring ended on its marker");

        // The drained history serves first, in ring order.
        let Message { source, parts } = t
            .poll(Duration::from_millis(50))
            .expect("poll")
            .expect("m1");
        assert_eq!(
            (source.as_str(), parts[2].as_slice()),
            ("i0", &b"replayed-one"[..])
        );
        let Message { parts, .. } = t
            .poll(Duration::from_millis(50))
            .expect("poll")
            .expect("m2");
        assert_eq!(parts[2].as_slice(), b"replayed-two");

        // Live seq 2 is the overlap and is dropped. Live seq 3 is new and is delivered.
        // PUB/SUB has no join barrier, so both are republished until seq 3 arrives; seq 2
        // must never surface.
        let deadline = Instant::now() + Duration::from_secs(10);
        let three = loop {
            publisher
                .send_multipart([&b"topic"[..], &2u64.to_be_bytes(), b"live-dup"], 0)
                .expect("send");
            publisher
                .send_multipart([&b"topic"[..], &3u64.to_be_bytes(), b"live-new"], 0)
                .expect("send");
            if let Some(Message { parts, .. }) = t.poll(Duration::from_millis(50)).expect("poll") {
                break parts;
            }
            assert!(Instant::now() < deadline, "no live message within deadline");
        };
        assert_eq!(
            three[2].as_slice(),
            b"live-new",
            "the overlapped duplicate must never surface: {three:?}"
        );
    }

    /// A regressed live sequence must clear the replay deduplication window,
    /// so a publisher restart reaches the adapter as a holder reset.
    #[test]
    fn a_publisher_restart_under_the_dedup_window_is_delivered() {
        let ctx = zmq::Context::new();
        let (publisher, live_endpoint) = bind_pub(&ctx);
        let replay_endpoint =
            bind_replay_router(&ctx, vec![(1, b"replayed-one"), (2, b"replayed-two")]);
        let mut t = ZmqSubTransport::connect(&[ep("i0", live_endpoint)]).expect("connect");
        t.drain_replay(&[ep("i0", replay_endpoint)]).expect("drain");
        while t.poll(Duration::from_millis(50)).expect("poll").is_some() {}

        // Live joins past the ring: seq 3 clears the window's high of 2.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            publisher
                .send_multipart([&b"topic"[..], &3u64.to_be_bytes(), b"live-three"], 0)
                .expect("send");
            if let Some(Message { parts, .. }) = t.poll(Duration::from_millis(50)).expect("poll") {
                if parts[2].as_slice() == b"live-three" {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "live leg never joined");
        }

        // The restart: the sequence returns to zero, below the ring's high. It must arrive.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            publisher
                .send_multipart([&b"topic"[..], &0u64.to_be_bytes(), b"reborn-zero"], 0)
                .expect("send");
            if let Some(Message { parts, .. }) = t.poll(Duration::from_millis(50)).expect("poll") {
                if parts[2].as_slice() == b"reborn-zero" {
                    break; // leftover re-publishes of seq 3 are tolerated on the way
                }
            }
            assert!(
                Instant::now() < deadline,
                "the restart was eaten by the dedup window"
            );
        }

        // The window is gone: seq 1, still under the old high, is delivered too.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            publisher
                .send_multipart([&b"topic"[..], &1u64.to_be_bytes(), b"reborn-one"], 0)
                .expect("send");
            if let Some(Message { parts, .. }) = t.poll(Duration::from_millis(50)).expect("poll") {
                if parts[2].as_slice() == b"reborn-one" {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "the dedup window survived the restart"
            );
        }
    }

    /// A configured-but-dead replay endpoint costs one bounded timeout and some history,
    /// never the run: the drain reports incomplete and live polling works untouched.
    #[test]
    fn a_dead_replay_endpoint_degrades_to_no_history() {
        let mut t = ZmqSubTransport::connect(&[ep("i0", silent_address())]).expect("connect");
        let started = Instant::now();
        t.drain_replay(&[ep("i0", silent_address())])
            .expect("drain");
        assert_eq!(t.replay_incomplete(), 1);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the drain is bounded, never a startup wedge"
        );
        assert!(t.poll(Duration::from_millis(20)).expect("poll").is_none());
    }

    fn bind_pub(ctx: &zmq::Context) -> (zmq::Socket, String) {
        let publisher = ctx.socket(zmq::PUB).expect("pub socket");
        publisher.bind("tcp://127.0.0.1:*").expect("bind");
        let endpoint = publisher
            .get_last_endpoint()
            .expect("endpoint")
            .expect("utf8 endpoint");
        (publisher, endpoint)
    }

    const FRAME: [&[u8]; 3] = [b"topic", b"\x00\x00\x00\x01", b"payload-bytes"];

    /// Publish `FRAME` on `publisher` until `t` yields a message or the deadline passes:
    /// PUB/SUB has no join barrier (a frame published before the subscription is live is
    /// silently dropped), so arrival is awaited, never assumed.
    fn publish_until_received(publisher: &zmq::Socket, t: &mut ZmqSubTransport) -> Message {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            publisher.send_multipart(FRAME, 0).expect("send");
            if let Some(msg) = t.poll(Duration::from_millis(50)).expect("poll") {
                return msg;
            }
            assert!(
                Instant::now() < deadline,
                "no frame arrived within 10s of publishing"
            );
        }
    }

    #[test]
    fn frames_flow_through_a_real_publisher_intact_and_attributed() {
        let ctx = zmq::Context::new();
        let (publisher, endpoint) = bind_pub(&ctx);
        let mut t = ZmqSubTransport::connect(&[ep("i0", endpoint)]).expect("connect");
        let Message { source, parts } = publish_until_received(&publisher, &mut t);
        assert_eq!(
            source.as_str(),
            "i0",
            "the frame must carry its subscription's label"
        );
        let expected: Vec<Vec<u8>> = FRAME.iter().map(|p| p.to_vec()).collect();
        assert_eq!(
            parts, expected,
            "multipart frame must arrive byte-identical"
        );
    }

    /// Round-robin from the cursor reaches every ready socket within one lap; first-readable
    /// scheduling starves every socket behind a busy one. The contest is timing-independent:
    /// both queues are deep, both arrivals are confirmed before counting begins, and depth
    /// exceeds the contest length so neither can drain mid-way, leaving only the schedule
    /// to observe (`docs/design/design-log.md`, "The fan-in schedule").
    #[test]
    fn a_flooded_first_socket_does_not_starve_the_second() {
        let ctx = zmq::Context::new();
        let (pub_a, ep_a) = bind_pub(&ctx);
        let (pub_b, ep_b) = bind_pub(&ctx);
        let mut t = ZmqSubTransport::connect(&[ep("iA", ep_a), ep("iB", ep_b)]).expect("connect");

        // Both subscriptions confirmed live before anything is counted.
        publish_until_received(&pub_a, &mut t);
        publish_until_received(&pub_b, &mut t);

        let flood = |n: u32| {
            for _ in 0..n {
                pub_a.send_multipart(FRAME, 0).expect("send");
                pub_b.send_multipart(FRAME, 0).expect("send");
            }
        };
        // First flood: settle until at least one frame from each source has been serviced,
        // which shows both floods are landing in the subscriber's queues. TCP orders the
        // rest of each stream behind the frame already seen.
        flood(200);
        let deadline = Instant::now() + Duration::from_secs(10);
        let (mut seen_a, mut seen_b) = (false, false);
        while !(seen_a && seen_b) {
            match t.poll(Duration::from_millis(100)).expect("poll") {
                Some(Message { source, .. }) if source.as_str() == "iA" => seen_a = true,
                Some(Message { source, .. }) if source.as_str() == "iB" => seen_b = true,
                _ => {}
            }
            assert!(
                Instant::now() < deadline,
                "the floods never arrived (seen_a={seen_a}, seen_b={seen_b})"
            );
        }
        // Second flood: whatever the settle phase consumed, both queues now hold more
        // frames than the whole contest can service, so socket 0 stays readable for every
        // poll. That is the condition under which first-readable scheduling gives socket 1
        // nothing at all.
        flood(200);
        let mut from_b = 0_u32;
        for _ in 0..100 {
            if let Some(Message { source, .. }) = t.poll(Duration::from_millis(100)).expect("poll")
            {
                if source.as_str() == "iB" {
                    from_b += 1;
                }
            }
        }
        // Round-robin over two always-ready sockets is ~50/100; first-readable is 0/100.
        // The floor sits far from both so scheduling noise cannot flip the verdict.
        assert!(
            from_b >= 20,
            "socket 1 was serviced {from_b}/100 times while socket 0 stayed busy: the \
             fan-in starves"
        );
    }
}
