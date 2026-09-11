# ADR 004 — The dead letter queue stores raw bytes and has a replay tool

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phase:** 7
- **Implements:** design decision D6
- **Relates to:** ADR 010 (what arrives here), ADR 003 (the other way in), ADR 005 (when the source offset commits)

## Context

A dead letter queue is where records go when the system has given up on
them: a permanent failure (ADR 010), or a transient one that ran out of retry
tiers (ADR 003). The obvious implementation is to deserialize the record,
attach the error, and write the result as a structured document.

It is broken by construction. **The single most common permanent failure is
that the record would not deserialize.** A DLQ that needs a decoded object has
nothing to write for exactly the records it exists for. Every submission that
"stores the failed order with its error" has quietly excluded the most
important class of failure from its own error handling — and the exclusion is
invisible until the first poison pill arrives and vanishes.

The second failure of the obvious design is that a DLQ topic is not a DLQ. A
topic full of records nobody can inspect or act on is a grave with a
misleading name. §2.2 of the specification makes the distinction explicit:
"I made a DLQ topic" versus "I built dead-letter handling".

## Decision

### The value is the original bytes. The key is the original key. Nothing is re-serialized.

The DLQ record's value is the `Buffer` exactly as consumed — opaque by
design. If it was undecodable on `orders`, it is equally undecodable here, and
that is fine: the point is to _keep_ it, faithfully, so it can be inspected
and replayed. A record whose value was `null` is written with a `null` value.

### Every diagnostic is a header

Because the value is opaque, all forensics live in headers. The full set:

| Header                                                       | Content                                                                                                                                                                                                     |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `x-original-topic` / `-partition` / `-offset` / `-timestamp` | Where and when the record was **first** consumed. For a record that rode the retry tiers, these come from the retry metadata written on the first hop — not from the retry topic it happened to be on last. |
| `x-original-key`                                             | The key as text, for readers that cannot see the record key                                                                                                                                                 |
| `x-error-type`                                               | `permanent` or `transient-exhausted` — the only two ways in                                                                                                                                                 |
| `x-error-class` / `-message` / `-stack`                      | The classified error. For a permanent one the message is prefixed with its reason code (`deserialization: …`). The stack is capped at 2 KB on a UTF-8 boundary.                                             |
| `x-attempt-count`                                            | Failed deliveries, total, across every tier                                                                                                                                                                 |
| `x-first-failed-at` / `x-last-failed-at`                     | ISO timestamps bracketing the record's whole history                                                                                                                                                        |
| `x-consumer-group`                                           | Which group gave up                                                                                                                                                                                         |
| `x-correlation-id`                                           | Carried from the producer, unchanged — the thread through every hop                                                                                                                                         |
| `x-app-version`                                              | The consumer version that gave up. "This message is bad" and "the deploy at 14:05 is bad" are different diagnoses.                                                                                          |

The set is defined in one module (`packages/shared/src/dlq.ts`); the writer
builds it, the inspector reads it, and the replay tool strips it, so the three
cannot disagree about a name.

### Writing is terminal; a failed write is not

Dead-lettering is one of the three terminal outcomes after which the source
offset may commit (ADR 005): the record's fate is now durably recorded on
another topic. If the DLQ write itself fails — the broker refused it — nothing
durable has happened, so the failure surfaces as transient, the pipeline does
not commit, and the record is redelivered. It is never lost in the gap between
two topics.

### The topic never expires

`orders.dlq` has `retention.ms = -1` and one partition. A dead letter is an
operator's problem to look at, and problems should not silently age out.

### `dlq-inspector`: list, decode, replay

A DLQ is only dead-letter _handling_ if someone can act on it.

- **`list`** — one row per record: when, why (type, class, reason), where from
  (original topic/partition/offset), how many attempts, the key, the
  correlation id, and how many times it has already been replayed.
- **`decode <offset>`** — everything knowable, in layers that do not depend on
  each other: the headers (always readable); the wire-format frame (is there a
  magic byte, what schema id); the raw bytes as hex and text (a human can
  recognise JSON or junk); and an Avro decode with **validation off**, because
  a record that failed validation is exactly one the operator wants to see
  decoded. A failure in one layer is reported in its slot and the next layer
  still runs.
- **`replay <offset>… | --from A --to B | --all`** — sends selected records back
  to `orders`: same key, same bytes. Headers that describe the _past_ failure
  are stripped — above all `x-attempt-count`, so the record earns a fresh set
  of retry tiers rather than arriving pre-exhausted. The correlation id is
  kept. Three headers are added: `x-replayed-from-dlq-offset`,
  `x-replayed-at`, and `x-replay-count`, which climbs each time the same record
  comes back and is how an operator tells "replayed once, fine now" from
  "keeps coming back". `--dry-run` shows the selection without sending.

The dead letter itself stays on the DLQ topic after replay: Kafka is
append-only, and the record is the permanent evidence that this happened.

The CLI writes its **output to stdout and its logs to stderr**, so
`dlq-inspector list --json | jq` sees data and only data. It is the one place
in the repository where `console` is used, and it is used for output, not
diagnostics.

### Verified live

Three poison pills of different flavours were dead-lettered on their first
delivery with all fifteen headers present — confirmed with the Kafka console
consumer, independently of this repository's own reader. A `__TRANSIENT_FAIL__`
marker rode 5s → 30s → 5m and was dead-lettered as `transient-exhausted` with
`x-attempt-count: 4`; with the "downstream" restored, `replay` sent it back to
`orders` and it was processed on the next delivery, carrying `x-replay-count:
1` and its original correlation id.

## Consequences

**Positive**

- The DLQ holds every failed record, including the ones that could not be
  decoded — which is to say, the ones it exists for.
- Every record is diagnosable from its headers alone, in Kafbat UI or the CLI,
  without any tooling that understands Avro.
- Replay closes the loop. A downstream outage or a fixed bug no longer means
  data loss; it means `dlq-inspector replay --all`.
- The replay count makes a record that keeps failing visible as such.

**Negative**

- Records are opaque in the DLQ; reading one means `decode`. This is the
  cost of storing what actually failed.
- Headers duplicate a little of what the retry metadata already carried.
  Deliberate: a dead letter must be self-describing without reference to a
  retry topic that may have expired.
- The DLQ grows without bound. That is a feature — nothing should age out
  silently — but it does mean someone has to look at it. The dashboard's DLQ
  depth tile is the prompt.
- Replaying a record that fails for the same reason produces a second dead
  letter. Correct, and the replay count reports it; a tool that suppressed
  the second one would be hiding a fact.

## Alternatives considered

| Alternative                                                  | Rejected because                                                                                                               |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| Store the deserialized order plus the error                  | Cannot represent the most common failure — a record that would not deserialize. Broken by construction; §2.2 names it.         |
| Store a JSON envelope `{ bytes: base64, error: … }`          | Works, but re-encodes the value, and the record key and headers already are the envelope. Kafka has headers for exactly this.  |
| Store the error in the value and the bytes in a header       | Backwards. Headers have size expectations; values do not.                                                                      |
| No replay tool; replay by hand with `kafka-console-producer` | Loses the key and headers, cannot strip the attempt count, and cannot be done for a binary value at all. "I made a DLQ topic." |
| Delete the dead letter after replay                          | Kafka is append-only; and the record is the evidence. The replay count on any future dead letter carries the history forward.  |
| A finite retention on the DLQ                                | Problems that age out silently are problems nobody solved.                                                                     |
