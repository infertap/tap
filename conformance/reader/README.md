# Reader conformance corpus

Each fixture pairs sealed input segments with the expected reader verdict.
`tools/gen-reader-corpus.py` derives the expectations from the specification.

## Fixture shape

```json
{
  "name": "...",
  "note": "what this pins and why",
  "segments": [[ {segment_open...}, {record...}, ... ], ...],
  "expect": { ...one verdict block per family below... }
}
```

`segments` are complete sealed segments: header first, per the delivery contract. A
consumer ingests them through its real delivery path — this checks record decoding and stream interpretation together.

## Verdict families

- **`audit`** — the evict re-derivation audit (spec §2.3, a SHOULD): per instance, the
  count of audited evicts in each of five classes, which MUST partition:
  `agreed` (the evict carries what the latest strictly-prior same-holder store carries,
  including both carrying nothing), `mismatched` (both carry an identity and they differ —
  the silent-wrong direction), `underresolved` (the store, from the **same producer run**,
  carries an identity the evict lacks — a promise held and lost), `indeterminate` (the
  evidence cannot grade it: cross-run joins, unattributed provenance, or a value-less
  store beside a valued evict), `unjoined` (no strictly-prior store at all — including
  same-instant pairs, because one batch stamps one timestamp and order is unknowable).
- **`parent_conflicts`** — the uniqueness property's announced failure (spec §3.1
  property 5): the number of identities claiming more than one parent, surfaced rather
  than folded.
- **`residual_zero`** — the qualified zero (spec §5.7): what a conforming reader attaches
  to a cross-instance zero, one of `positive` (not zero — the finding stands),
  `single_holder` (nothing could have been observed), `not_comparable` (no portable
  identity anywhere; the figure says nothing), `partly_comparable`, `split_key_space`, or
  `measured` (two-plus comparable holders: a real absence). Incarnation opacity (spec
  §4.2) rides the audit family: a structureless token must change no verdict.
- **`staleness`** — the liveness bounds (spec §5.8) and the unexplained-departure rule
  (spec §2.4): per producer run, keyed by its incarnation token (an opaque key, per spec
  §4.2), one of `live`, `departed` (last record is an `agent_stop`: an announced
  departure, at any later horizon), `unexplained` (silence past the LARGER declared
  bound with no stop — the absence is the signal), or `no_basis` (no `agent_start`
  observed: the declared bounds are the entire basis for the staleness verdict, and a
  reader must refuse it rather than invent a threshold). Verdicts are judged at the
  stream's **knowledge horizon** — the latest producer-clock record anywhere in the
  stream, never wall-clock now: an archive read a year later must reach the same
  verdicts. The allowance beyond the bound is implementation-defined, so fixtures place
  every interval an order of magnitude inside or beyond the bound, where no conforming
  allowance can flip the verdict.

- **`coverage`** — coverage accompanies every figure (spec §5.7, the conformance
  floor): a producer-clock question span answered with the coverage a conforming
  reader attaches — `complete` (exactly 1.0 over a counted bracket: under-reporting a
  clean bracket manufactures doubt) or `reduced` (strictly below 1.0 under inbound
  loss, or under a tail no stop closed — the crash costs coverage even where batch
  staleness stays silent). Pins the obligation's direction and its two exact
  endpoints, never any model's interior arithmetic.
- **`holder_register`** — the per-holder figure register, as questions of the form
  (holder, ENGINE-clock `span_ms`) each answered with the register a figure over them
  carries. One form, two obligations. The `identity_refused` lower bound (spec §2.4):
  `lower_bound` for the named holder inside the declared window, `none` for the
  sibling worker and outside it, and the declaration's producer-clock `at_ms` must
  not confine it (spec §2.6) — one fixture places that stamp inside an innocent span
  so a domain-crossing reader fails. The multi-instance gap scope (spec §5.1): a
  missing `segment_seq` is a producer-level loss, so every instance the incarnation
  covered reads `indeterminate` — including the one with no surviving records, which
  is exactly the instance a record-driven reader forgets — while an instance covered
  by its own intact run reads `none`.
- **`absence`** — the lower-bound rule (spec §5.6): the fixture carries the question (a
  producer-clock `span_ms`), and the verdict is the register a conforming reader may
  attach to absence over it — `exact` only within an observed, counted sequence (an
  `agent_start` through an `agent_stop`, both observed, counters establishing nothing
  was lost), `lower_bound` everywhere else: before the producer started, during a gap,
  or outside any window it observed.
- **`uncovered`** — the incarnation boundary (spec §5.3): per instance, the exact
  uncovered intervals as `[from_ms, to_ms]` producer-clock pairs — the departing run's
  `agent_stop` (or its last observation where no stop was received) to the arriving
  run's `agent_start` (or its first observation where the start record was lost: a
  producer is provably running by its first record, so uncovered past it asserts a
  non-observation the records refute). Overlapping runs yield nothing, and a
  continuously covered instance expects the empty list — a reader that keys on "a
  boundary happened" rather than "nobody was watching" fabricates a window and fails.
- **`gap_degraded`** — detected gaps degrade the window (spec §5.4): the sorted list of
  detected-gap classes present in the stream, in the spec's own words —
  `missing_segment_seq`, `dropped_delta`, `segments_dropped` — and the empty list for
  an intact stream, which pins that a reader must not fabricate gap evidence. A window
  whose list is nonempty is indeterminate. A restart at seq zero (spec §5.2) and a
  sequence beginning above zero (absence outside observation, spec §5.6) are not gaps
  and must not appear.

The clock-domain fixtures (spec §2.6, `clock_*`) declare no family of their own,
deliberately: §2.6 adds no verdict — it constrains how every other verdict may be
computed — so its fixtures are adversarial streams graded by the families above. The
engine clock displaced a month from the producer clock flips no staleness or audit
verdict; a 2 ms cross-instance overlap is not simultaneity under any plausible
configured allowance; a ten-minute overlap survives the allowance, or the allowance is
not one.

**The spec's no-covering-fixture table is empty as of 2026-08-01.** Every reader
obligation is fixtured here — the §3.4 two-canary split
(`canary_split_key_material_is_not_a_measured_zero`, a residual_zero verdict) closed
the last one with reader-side substance — and the producer-side rows discharged onto
emission corpora and named producer tests, cited in the spec's footnotes. The standing
rule: a new requirement enters the spec with its fixture, or it reopens the table.
