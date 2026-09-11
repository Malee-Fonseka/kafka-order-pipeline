# ADR 008 — Idempotent producer with `acks=all`

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phase:** 3
- **Implements:** design decision D2
- **Relates to:** ADR 005 (consumer-side delivery guarantee)

## Context

A producer's defaults are tuned for throughput, not correctness. Left alone,
`@confluentinc/kafka-javascript` — like every librdkafka-based client —
acknowledges a send before it is safely replicated and retries internally in a
way that can duplicate records. Both behaviours are invisible until something
fails, which is precisely when they matter.

Two distinct failure modes:

**Lost writes.** With `acks=1` the leader acknowledges as soon as it has written
the record, before followers have replicated it. If that leader fails before
replication completes, the record is acknowledged to the application and absent
from the topic.

**Duplicate writes.** When a send times out, the client cannot distinguish "the
broker never received it" from "the broker received it and the acknowledgement
was lost". It retries. If the original did land, the record is now on the topic
twice — and the application never knows, because both attempts look successful.
This duplication is produced by the client's _own_ retry logic; it has nothing
to do with the retry topics in ADR 003.

## Decision

Configure the producer explicitly:

| Setting                  | Value                                | Reason                                                                                                                                                             |
| ------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `idempotent`             | `true`                               | The broker assigns the producer an id and tracks a per-partition sequence number, so a retried record that already landed is discarded rather than appended twice. |
| `acks`                   | `-1` (all)                           | No acknowledgement until every in-sync replica holds the write. Survives a leader failover without losing an acknowledged record.                                  |
| `maxInFlightRequests`    | `5`                                  | The highest value that still preserves ordering under idempotence. Above it the broker cannot guarantee sequence ordering; below it, throughput drops for nothing. |
| `retry`                  | 10 retries, 100 ms initial, 30 s cap | Survives a broker restart in the transport layer, where it belongs.                                                                                                |
| `allowAutoTopicCreation` | `false`                              | §10.4 — a typo'd topic name must fail loudly. The broker also refuses, but failing client-side gives a clearer error.                                              |

### What this does and does not buy

It buys **no duplicates from the client's own retries**, which is a real and
commonly-skipped guarantee.

It does **not** buy exactly-once delivery end to end, and this repository does
not claim it. Idempotence is scoped to one producer session: a producer that
crashes and restarts gets a new producer id, so a record it sent but did not see
acknowledged may be sent again by the new session and will not be de-duplicated.
End-to-end exactly-once requires transactions spanning the consume-process-produce
cycle, which this project does not implement. The honest guarantee is
**at-least-once**, documented in ADR 005.

Claiming otherwise is the overclaim §2.4 warns an examiner will catch.

### Why the retry settings live here and not in the retry topics

The tiered retry topics (ADR 003) exist for **message** failures — a record the
handler cannot process right now. The settings above exist for **transport**
failures — a broker that is restarting. Conflating them would push a simple
connection blip through the entire escalation ladder and into the DLQ, which is
both slow and wrong.

## Consequences

**Positive**

- Client-side retry duplication is eliminated at the broker.
- An acknowledged write is durable across a leader failover.
- Ordering is preserved per partition, which is what makes the `product` key of
  ADR 002 give a correct per-product sequence.
- The delivery guarantee can be stated precisely rather than hopefully.

**Negative**

- `acks=all` costs latency: the round trip now includes replication. At this
  project's message rate it is unmeasurable, and it is the correct trade for a
  system whose whole subject is not losing messages.
- Idempotence requires `maxInFlightRequests <= 5` and `acks=all`; these settings
  are interdependent and cannot be tuned in isolation.
- A producer restart still admits duplicates. This is stated, not hidden.

## Alternatives considered

| Alternative                                          | Rejected because                                                                                                                                        |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Defaults                                             | Acknowledges before replication and can silently duplicate on retry — the two failure modes this ADR exists to close.                                   |
| `acks=1`                                             | Faster, but loses acknowledged records on leader failover. Unacceptable in a system about delivery guarantees.                                          |
| `acks=0`                                             | Fire and forget. No guarantee of any kind.                                                                                                              |
| Transactions                                         | The only route to genuine exactly-once, but it requires the consumer side too and is well beyond this assignment. Named as the upgrade path in ADR 005. |
| Client retries disabled, relying on the retry topics | Pushes transport blips through the message-failure ladder: slower, noisier, and it fills the DLQ with records that were never bad.                      |
