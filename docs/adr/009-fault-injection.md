# ADR 009 — Fault injection is built into the producer

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phase:** 3
- **Implements:** design decision D8

## Context

Three of this system's features — retry, the dead letter queue, and the error
taxonomy that routes between them — are only observable when something fails.
In normal operation they are invisible: every record decodes, the handler
succeeds, and the DLQ stays empty.

The assignment requires a **live demonstration**. That creates a problem the
implementation has to solve rather than hope around. The usual approaches are
all bad:

- **Wait for a real failure.** §12 lists "a demo that depends on something
  failing by luck" as marks-losing, and rightly: the failure either does not
  come, or comes while explaining something else.
- **Stop a container mid-demo.** Kills the broker connection for everything at
  once, so it demonstrates transport failure, not _message_ failure — the retry
  tiers and the DLQ are about records, not sockets.
- **Hand-craft a bad message with a CLI.** Possible, but it is a side channel
  the system does not otherwise use, and it cannot produce a controlled _rate_
  of failures to watch a dashboard respond to.

## Decision

The producer can emit failures on purpose, under `CHAOS_MODE`, at configured
rates, from an injectable random source.

Two kinds, chosen because they exercise opposite halves of the error taxonomy
(ADR 004 / D4):

| Kind          | Mechanism                                                                                                                                                                | Exercises                                                               |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| **Transient** | A structurally valid order whose product is the marker `__TRANSIENT_FAIL__`. The consumer's handler throws a _retryable_ error for it and succeeds after a few attempts. | The retry tiers, and the claim that a delayed retry causes no rebalance |
| **Poison**    | Bytes that are not decodable Avro, written **without passing through the serializer**.                                                                                   | Permanent classification and the path to the DLQ                        |

### The poison pill must bypass the serializer

This is the part that is easy to get wrong. Serializing a "bad" order produces
perfectly valid Avro containing an odd value — which decodes successfully and
then fails _validation_. That exercises a different code path entirely.

A genuine poison pill is bytes the deserializer cannot decode at all, so it is
written as a raw `Buffer` straight to the topic. It is still keyed by a real
product (ADR 002), so it lands on a partition a consumer actually owns and fails
where it can be seen.

### Three flavours, cycled rather than random

Corruption is not one thing, and the three ways a payload can be undecodable
fail at three different layers:

| Flavour             | Corruption                            | Fails at                                       |
| ------------------- | ------------------------------------- | ---------------------------------------------- |
| `json-not-avro`     | JSON bytes; byte 0 is `{`, not `0x00` | The magic-byte check, before any registry call |
| `unknown-schema-id` | Correctly framed, schema id `999999`  | The registry lookup                            |
| `truncated-payload` | A real record cut short after 8 bytes | The Avro decoder                               |

They are **cycled** rather than drawn at random so that a short demo run
exercises all three, and the DLQ ends up displaying three genuinely different
failure reasons instead of the same one three times. A random draw would
frequently show only one.

`truncated-payload` needs a real serialized record to cut short — the only
corruption that cannot be fabricated from nothing without risking bytes that
accidentally decode — so the producer keeps the most recent valid payload for
that purpose, falling back to an unregistered schema id before the first valid
record exists.

### The random source is injectable

`Math.random` is the default; tests pass a fixed sequence. Rate boundaries are
therefore asserted exactly rather than inferred from a distribution, and a
scripted demo can be made reproducible.

## Consequences

**Positive**

- The demo is controllable and repeatable: chaos is a flag, failure rates are
  dials, and the examiner watches retries climb and the DLQ fill on cue.
- Both halves of the error taxonomy are exercised by the real system, through
  the real topic, rather than by a test harness.
- The marker product is visible in Kafbat UI, so a reader can see which records
  are synthetic.

**Negative**

- Test-affecting code ships in the production producer. Mitigated by defaulting
  `CHAOS_MODE` to false and by the marker being deliberately unmistakable.
- The marker product pollutes the aggregate with a synthetic product while chaos
  is on. Acceptable — and arguably useful, since it makes the injected traffic
  visible on the dashboard as its own series.
- The producer knows about a value the _consumer_ interprets. That coupling is
  real; it is named in one exported constant rather than a string literal in two
  packages.

## Alternatives considered

| Alternative                              | Rejected because                                                                                               |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Stop a container during the demo         | Demonstrates transport failure, not message failure. The retry tiers and DLQ are about records.                |
| A separate chaos CLI                     | A second way to publish, used only in demos, that diverges from the real producer's configuration and headers. |
| A random poison flavour each time        | A short run would often show only one failure reason, understating the taxonomy.                               |
| Serializing a deliberately invalid order | Produces valid Avro. Exercises validation, not deserialization — not a poison pill.                            |
