# ADR 010 — Failures are classified by one typed function

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phases:** 2 (the types), 6 (the classifier)
- **Implements:** design decision D4
- **Relates to:** ADR 003 (what happens to each kind), ADR 004 (where permanent ones go)

## Context

Every failure a consumer meets is one of two things: the operation might
succeed if repeated, or it will not. Everything downstream — retry, dead
letter, commit — depends on which. Getting it wrong is not a matter of taste:

- Retrying a **permanent** failure is a livelock. A record whose bytes are not
  Avro will not be Avro next time. Cycled through the retry tiers it wastes six
  minutes and three topics and arrives at the DLQ anyway.
- Dead-lettering a **transient** failure is data loss dressed up as
  diagnostics. A perfectly good order parked because the registry blinked.

The usual implementation scatters this decision across `catch` blocks — a
string match here, an `instanceof` there — and nobody can say afterwards why a
given record went where it did.

## Decision

### Two typed variants, discriminated on `kind`

`TransientError` and `PermanentError` share a base class and carry the
underlying cause. `PermanentError` also carries a machine-readable `reason`:
`deserialization`, `schema-incompatible`, `unknown-schema-id`, `validation`,
or `unclassified`. A `switch` over the union narrows exhaustively; a handler
that forgets a case does not compile.

Code that _knows_ the answer raises the right type directly: the deserializer
raises permanent for bad bytes and transient for a registry outage
mid-decode; the chaos handler raises transient; the changelog writer raises
transient when the broker refuses a write.

### One classification function for everything else

`classifyError(error: unknown): ClassifiedError` is applied exactly once to
every failure that escapes a handler unclassified, in this order:

| Rule                                                                                                                              | Result                                               |
| --------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Already classified                                                                                                                | Returned unchanged — the raiser knew best            |
| Node system error code in the connection family (`ECONNRESET`, `ECONNREFUSED`, `ETIMEDOUT`, `EPIPE`, `EAI_AGAIN`, `ENOTFOUND`, …) | Transient                                            |
| HTTP status 408, 425, 429 or 5xx                                                                                                  | Transient                                            |
| Any other 4xx                                                                                                                     | Permanent, `unclassified` — a fact about the request |
| A client error that declares itself `retriable`                                                                                   | Transient                                            |
| A message that reads as a timeout                                                                                                 | Transient                                            |
| Anything else                                                                                                                     | Permanent, `unclassified`                            |

The rules are deliberately mechanical. The tests enumerate them, and it is
possible to point at the line that decided a record's fate.

### The default is permanent, not transient

This is the one judgement call in the table and it is worth stating why. An
error nothing recognises is far more often a bug — a `TypeError`, a null
dereference, an assumption that broke — than a network condition that no rule
anticipated. Bugs are deterministic; retrying one through six minutes of tiers
helps nobody and hides the problem behind a wall of retries. And the DLQ has
**replay** (ADR 004): a record parked there with full forensics is not lost.
It is waiting for the fix.

The other default — treat unknowns as transient — would be right if the DLQ
were a grave. It is not.

### Where the two halves live

The _permanent_ half was needed in Phase 2 (a corrupt payload must never
retry) and the _transient_ half in Phase 6 (the first retry). The types and
the deserializer's classifications landed with Phase 2; the general classifier
and its network rules landed with Phase 6. Both are in
`packages/shared/src/errors.ts`, which is the single most reviewable file in
the codebase and is tested as such.

## Consequences

**Positive**

- Every retry and every DLQ write can be traced to a rule.
- Poison pills never enter a retry tier; registry outages never enter the DLQ.
- Adding a new transient condition is one line in one table, with a test.
- The `reason` code travels into the DLQ headers, so the inspector can group
  failures by cause.

**Negative**

- An unrecognised transient condition is parked in the DLQ rather than
  retried. Visible, replayable, and it prompts the table to grow — but it is
  a delay for that record.
- Timeout detection by message text is a heuristic. It is the last rule
  before the default and it errs toward retrying, which is the safe direction
  for a timeout.

## Alternatives considered

| Alternative                             | Rejected because                                                                                    |
| --------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Classify in `catch` blocks as needed    | Undocumented, untestable, and inconsistent between handlers — the thing this ADR exists to prevent. |
| Default unknowns to transient           | Cycles bugs through the retry tiers and hides them. The DLQ with replay is the better parking spot. |
| Retry everything with a ceiling         | The livelock is bounded but still six minutes per poison pill, and the DLQ loses its meaning.       |
| A single error type with a boolean flag | Loses the reason code and the exhaustive `switch`.                                                  |
