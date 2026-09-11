# ADR 005 — At-least-once delivery via manual offset commits

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phase:** 4
- **Implements:** design decision D7
- **Relates to:** ADR 008 (producer-side idempotence)

## Context

A Kafka consumer does not acknowledge each message. It periodically records a
**committed offset** per partition — "everything before this position is
done" — and on restart resumes from there. When that commit happens, relative
to when the message is actually processed, is the entire delivery guarantee.

The default, `enable.auto.commit=true`, commits on a timer (every five seconds
in this client) with no regard for whether the handler has finished. Combined
with any asynchronous processing, that produces two failure modes, both silent:

- **Loss.** The timer fires after a message is fetched but before its handler
  completes. The consumer crashes. On restart it resumes _after_ the message,
  which was never processed.
- **Duplication.** The handler completes, the consumer crashes before the next
  timer tick. On restart the message is reprocessed.

Auto-commit therefore delivers neither at-most-once nor at-least-once; it
delivers "usually once", which is not a guarantee anyone can build on.

## Decision

**`autoCommit: false`. An offset is committed only after the record it belongs
to has reached a terminal state, and it is committed explicitly by the
pipeline.**

Terminal means one of:

| Outcome                               | Why it is safe to move past the record                              |
| ------------------------------------- | ------------------------------------------------------------------- |
| Processed successfully                | The work is done.                                                   |
| Republished to a retry tier (Phase 6) | The record's next attempt is now durably recorded on another topic. |
| Written to the DLQ (Phase 7)          | The record and its diagnosis are durably recorded on another topic. |

Anything else — a failure the handler did not classify — is **not** terminal.
The pipeline does not commit, the error propagates, and the client seeks back
to the same offset and redelivers. Reprocess, never skip.

The committed position is `offset + 1`: the offset of the _next_ record to
read, which is the convention both Kafka and this client use. Offsets are
handled as `BigInt` because they are `int64` on the broker.

Commits are **per record**, not batched. At this system's message rate the
extra round trip is unmeasurable, and per-record commits make the guarantee
easy to state and easy to verify: after a clean shutdown, `CURRENT-OFFSET`
equals `LOG-END-OFFSET` on every partition (lag 0), and a restart resumes at
exactly that position with zero replay. Both were verified against the live
broker as this phase's gate.

### The guarantee, precisely

This is **at-least-once**. Every record is processed one or more times. None
is lost.

### The duplicate-on-crash window

There is a window between "the handler finished" and "the commit landed on the
broker". A crash inside it — process killed, machine lost, network partition
at the wrong instant — means the record is processed again on restart. At
per-record commit granularity the window is one record wide.

For this system's handler that is a real, if small, effect: the running
average would count one price twice. It is not hidden and it is not claimed
away. It is what at-least-once means.

### What is deliberately _not_ claimed

This is not exactly-once, and no combination of settings in this repository
makes it so. The producer's idempotence (ADR 008) removes duplicates from the
producer's own retries; it does nothing about a crashed _consumer_. Genuine
exactly-once needs one of two things, neither of which is built here:

1. **Idempotent processing.** Make the handler tolerate a repeat: keep the set
   of `orderId`s already folded into the aggregate and ignore a second sight of
   one. Simple and effective for this workload; the cost is bounded state and a
   snapshot of that state alongside the aggregate. This is the natural upgrade
   path for this project.
2. **Kafka transactions.** Bind the offset commit and any produced output into
   one atomic transaction (`read_committed` consumers, transactional producer,
   `sendOffsets`). The correct general answer; substantially more machinery and
   well beyond this assignment.

Claiming exactly-once without one of these is the overclaim §2.4 warns an
examiner will catch.

## Consequences

**Positive**

- No message is ever lost — the property this system is graded on.
- The guarantee is stated in one sentence and verified with one command.
- The commit discipline lives in one small module (`pipeline.ts`) with tests
  that assert: no commit before processing completes; no commit on failure;
  shutdown waits for the in-flight commit.
- Graceful shutdown is correct by construction: drain in-flight, then
  disconnect. The broker reassigns the partitions immediately instead of
  waiting out the session timeout.

**Negative**

- Duplicates are possible on crash. Documented above; not mitigated.
- One commit round trip per record. Acceptable at this scale; batching
  (commit every N records or T milliseconds) widens the duplicate window to N
  records in exchange for throughput, and would be the first tuning step.
- A permanent failure must resolve rather than throw, or the client redelivers
  the same bytes forever. Phase 4 resolves it as a counted, logged `skipped`
  outcome; Phase 7 replaces that with a DLQ write. Until then a poison pill is
  skipped, which is data loss of a record that was undecodable anyway — and it
  is logged at `warn` so it is impossible to miss.

## Alternatives considered

| Alternative                               | Rejected because                                                                                                        |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Auto-commit (the default)                 | Loses messages on crash. Not a guarantee.                                                                               |
| Commit _before_ processing (at-most-once) | Never duplicates, sometimes loses. The wrong trade for an order pipeline.                                               |
| Batched commits                           | Reasonable tuning, but it widens the duplicate window and makes the guarantee harder to state. Not needed at this rate. |
| Claim exactly-once                        | Requires idempotent processing or transactions, neither of which is implemented. See above.                             |
