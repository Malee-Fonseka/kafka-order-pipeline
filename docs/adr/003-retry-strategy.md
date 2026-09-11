# ADR 003 — Two-stage retry with pause/seek delay, never sleep

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phase:** 6
- **Implements:** design decision D5
- **Relates to:** ADR 010 (what counts as transient), ADR 005 (commit ordering), ADR 002 (why retry topics forward)

## Context

A transient failure — the schema registry restarting, a dropped connection —
should be retried after a delay. The naive implementation is one line:

```
catch (e) { await sleep(30_000); return handle(record); }
```

It is also the most common way to break a Kafka consumer. While the handler
sleeps, the client does not poll. Once the gap exceeds `max.poll.interval.ms`
(300 s by default), the broker decides the consumer is dead, revokes its
partitions, and rebalances the group. Every other member pauses while that
happens. The sleeping consumer wakes, discovers it owns nothing, rejoins, and
triggers a second rebalance. With a five-minute retry delay this is not a risk;
it is a certainty. §2.1 of the specification names it as a marks-losing defect.

Two other things go wrong with naive retry:

- **No ceiling.** A record that never succeeds is retried forever. A poison
  pill that is _misclassified_ as transient becomes a livelock.
- **No jitter.** N records that failed together retry together. A downstream
  that was just recovering is hit by a synchronised wave and falls over again.

## Decision

### Two stages

**Stage 1 — bounded, in place.** Up to 3 attempts inside the handler, with
exponential backoff and **full jitter** (a uniform draw from `[0, ceiling]`,
ceilings 100 → 200 → 400 ms, capped at 800 ms), under a total **elapsed budget
of 2 s**. Whichever limit is hit first ends the stage. The budget is the one
that matters for group membership: 2 s cannot approach 300 s. This stage
catches the majority of blips — a reconnect, a registry that was mid-restart —
at the cost of a few hundred milliseconds and no extra Kafka traffic.

**Stage 2 — tiered retry topics.** A record still failing after stage 1 is
republished, as **raw bytes with its original key and headers**, to a retry
topic chosen by its attempt count: `orders.retry.5s`, then `orders.retry.30s`,
then `orders.retry.5m`, then exhausted. Republishing is a terminal outcome for
the original record (ADR 005): its next attempt is durably recorded elsewhere,
so the offset commits.

### Retry topics are delay queues that forward, not processing queues

A retry-tier record that has waited out its delay is **forwarded back to
`orders`**, unchanged, rather than processed where it sits. This costs one
extra hop per retry and buys correctness: aggregation ownership follows the
`orders` partition (ADR 002, ADR 006). Processing on the retry topic would be
correct only if the instance holding `orders.retry.5s` partition 1 were always
the instance holding `orders` partition 1 — co-partitioning _and_
co-assignment, neither of which this system should rest on. Two instances
writing one product's changelog entry is a lost update.

The record's journey is visible in Kafbat UI: `orders` → `orders.retry.5s` →
`orders` → `orders.retry.30s` → … Each hop carries the same correlation id.

### Escalation is driven by headers, so the ceiling is global

`x-attempt-count` counts failed _deliveries_ (a delivery being one pass
through stage 1, so up to three handler calls). It rides the record across
every hop and every instance. After delivery _k_ fails, the record goes to
tier _k_; after the third tier it is exhausted. The ceiling is therefore
enforced no matter which instance handles which hop, and the total time a
transient failure is given before it is declared permanent is the sum of the
delays — a little under six minutes.

`x-retry-not-before` carries the epoch millisecond before which the record
must not be forwarded. `x-first-failed-at`, `x-last-failed-at` and the
`x-original-{topic,partition,offset}` triple are set on the first hop and
preserved afterwards, for the DLQ's forensics (ADR 004).

### The delay: pause + seek, on a timer, never in the handler

When a retry-tier record arrives before its `x-retry-not-before`:

1. **`pause()`** the partition. The client stops _fetching_ from it but keeps
   polling every other partition and keeps heartbeating. The broker sees a
   healthy member throughout.
2. **`seek()`** the partition back to this record's offset. The record is not
   lost; it is the next thing delivered when fetching resumes.
3. **Schedule a resume** with `setTimeout` for the remaining delay. The timer
   runs outside any handler.
4. **Return without committing.** Nothing terminal has happened.

The handler returns in microseconds. When the timer fires, the partition
resumes, the same record is redelivered, this time it is due, and it is
forwarded. Wall-clock time passes; the poll loop never stops.

Pausing is per partition, which is Kafka's granularity, so any record behind
the early one waits too. Retry topics are appended in failure order, so the
head record is always the earliest due, and nothing behind it could have been
forwarded sooner anyway.

The client's documented contract for this is "pause, then throw, and we seek
back for you". This implementation seeks explicitly and returns normally
instead, because a throw is logged by the client at `error` level on every
deferred record — a five-minute tier would produce one spurious error line per
record per resume — and because an explicit seek is a line a reviewer can
point at.

### Verified live

- **Tier 1 recovery.** A marker record failed three in-place attempts, was
  republished to `orders.retry.5s`, held by a paused partition for 4922 ms,
  forwarded, and processed on delivery 2 at +5.2 s. Same group member id and
  the same 12 assigned partitions before and after.
- **Five-minute tier.** A marker rode 5s → 30s → 5m, with the retry
  partition paused for the full 300.0 s, and was processed on its fourth
  delivery at +336.5 s. Group membership was sampled every 45 s throughout:
  the same member id and the same 12 assigned partitions at every sample,
  and zero revocations until shutdown.

## Consequences

**Positive**

- No rebalance, at any tier delay. The proof is a log with zero revocations.
- Most blips are absorbed in stage 1 without touching a retry topic.
- The ceiling is enforced globally and the total retry budget is a stated
  number.
- Retries are observable: three topics whose depth is on the dashboard, and a
  correlation id to follow.

**Negative**

- Two hops per tier (to the retry topic and back). Invisible at this rate.
- Head-of-line waiting on a paused partition. Correct, and bounded by the
  tier's own delay.
- A retried record re-enters `orders` at the tail, behind newer records for
  its product. Aggregation is order-independent, so this is harmless here; a
  handler that needed per-key ordering would need a different design.
- Delays are tied to topic names (`retry.5s` really is five seconds). Changing
  a delay means a new topic, which is a feature: the name never lies.

## Alternatives considered

| Alternative                                         | Rejected because                                                                                                                           |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `await sleep()` in the handler                      | Exceeds `max.poll.interval.ms` and rebalances the group. The defect §2.1 names.                                                            |
| Pause with a throw (the client's suggested pattern) | Works, but the client logs an `error` on every deferred delivery; explicit seek is quieter and more legible.                               |
| A single retry topic with a delay header            | Head-of-line blocking across _all_ delays: a 5-minute record ahead of a 5-second one holds it for five minutes. Tiers keep like with like. |
| Process on the retry topic                          | Correct only under co-partitioning and co-assignment; otherwise two instances own one product. See ADR 002.                                |
| Retry in memory only                                | Lost on restart, invisible to the operator, and unbounded in memory under a long outage.                                                   |
| No jitter                                           | Synchronised retry waves against a recovering downstream. D5 makes jitter mandatory for this reason.                                       |
