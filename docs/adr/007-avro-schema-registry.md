# ADR 007 — Avro via Schema Registry, not a shared `.avsc` import

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 2
- **Relates to:** §2.5, §4 of `TODO.md` (project specification)

## Context

The assignment requires that "each message must use Avro serialization". That
sentence has two very different implementations, and only one of them is how
Avro is actually deployed on Kafka.

**The tutorial implementation.** Import `order.avsc` into the producer, import
the same file into the consumer, encode with a local Avro library, publish the
bytes. This technically produces Avro-encoded messages. It also means:

- The schema is duplicated into every service and pinned at _deploy_ time. A
  producer redeployed with a changed field silently emits records the consumer
  decodes into garbage — Avro binary is positional, so a reordered or retyped
  field does not fail loudly, it decodes to the wrong value.
- Nothing validates that a schema change is compatible. There is no gate; the
  first indication of breakage is bad data in the aggregate.
- Third-party tooling cannot decode the topic. Kafbat UI shows binary blobs,
  because nothing on the wire says which schema wrote them.

**The production implementation.** A Schema Registry holds the schemas, assigns
each a numeric ID, and enforces a compatibility policy on every registration.
Producers and consumers reference schemas by ID over the wire.

## Decision

Use **Confluent Schema Registry** with the official
`@confluentinc/schemaregistry` `AvroSerializer` / `AvroDeserializer`.

### Wire format

Values on `orders` are framed in Confluent's wire format, not bare Avro:

```
 byte 0      bytes 1..4              bytes 5..n
┌────────┬───────────────────────┬─────────────────────┐
│  0x00  │ schema ID, int32 BE   │ Avro binary payload │
└────────┴───────────────────────┴─────────────────────┘
 magic     registry lookup key     no embedded schema
```

- **Byte 0** is the magic byte `0x00`, the format version marker.
- **Bytes 1–4** are the registry's schema ID, big-endian `int32`.
- **Bytes 5+** are the Avro binary encoding of the record.

The payload carries no schema of its own. The consumer reads the ID, fetches the
writer schema from the registry once, caches it, and decodes every subsequent
message with the cached schema. The framing costs 5 bytes per message; embedding
the schema would cost roughly 400 bytes per message for this record.

`packages/shared/src/wire-format.ts` implements this framing explicitly rather
than leaving it inside the vendor library, because two later phases need it:
the DLQ writer records the schema ID of bytes it _could not_ decode (D6), and
the inspector CLI uses the magic byte to decide whether a record is worth
attempting to decode at all.

### Subject naming

`TopicNameStrategy`: subject = `<topic>-value`, so the Order schema lives under
**`orders-value`**. This is the registry default and what Kafbat UI assumes when
it decodes a topic; deviating from it means the UI stops decoding messages, which
would cost a visible demonstration step for no benefit.

### Compatibility mode: `BACKWARD`

Set explicitly on the subject at bootstrap rather than inherited from the global
default, so the guarantee is a property of this repository and not of whichever
registry it is pointed at.

`BACKWARD` means a **new consumer** can read data written by the **previous
producer**. That matches the deployment order this system actually uses:
consumers are upgraded first, producers second. Permitted changes are adding a
field _with a default_ and deleting a field; changing a field's type or adding a
required field is rejected by the registry at registration time — a build-time
failure instead of a runtime data corruption.

### Registration is a bootstrap step, not a manual one

`ensureOrderSchemaRegistered()` runs on service start: it sets the subject's
compatibility level, then registers `schemas/order.avsc`. Registration is
idempotent — re-registering identical schema text returns the existing ID rather
than allocating a new one — so every service can call it on every boot without
coordination, and a fresh `docker compose up` on a grader's machine needs no
out-of-band curl command.

The schema is **read from `schemas/order.avsc` on disk**, not duplicated into
TypeScript. The artefact registered with the registry is byte-for-byte the
artefact committed to the repository and read by the grader.

### Failure classification

Deserialization failure is the canonical **permanent** error (D4): a record
whose bytes are not valid framed Avro will fail identically on every retry, so
retrying it is a livelock. `createOrderDeserializer()` therefore converts every
decode failure into `PermanentError('deserialization')`, which routes the record
straight to the DLQ.

This is also why the DLQ stores **raw bytes** (D6): the most common permanent
failure _is_ deserialization, and you cannot deserialize a message in order to
record it.

## Consequences

**Positive**

- Schema evolution is gated by the registry rather than by review discipline.
- Kafbat UI decodes messages live, which demonstrates that the topic really
  carries registry-backed Avro rather than JSON with an Avro-shaped comment.
- One schema artefact, one source of truth, verifiable at `/subjects`.
- Schema IDs give the DLQ genuine forensics: "written with schema 3, this
  consumer reads schema 4" is a diagnosable statement.

**Negative**

- The registry is a runtime dependency and a new failure mode. A registry
  outage is classified **transient** (retryable), not permanent — it is exactly
  the case the retry tiers exist for.
- First decode after start pays one HTTP round trip per unseen schema ID.
  Subsequent decodes are served from the client's in-process cache.
- Local unit tests cannot reach a real registry. The round-trip test therefore
  drives the genuine `AvroSerializer`/`AvroDeserializer` against an in-memory
  fake client, so the wire format under test is real; end-to-end registry
  behaviour is covered by the testcontainers suite in Phase 8.

## Alternatives considered

| Alternative                                 | Rejected because                                                                                                                   |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Shared `.avsc` imported by both services    | No compatibility gate, no UI decoding, silent corruption on field reorder. §2.5 calls this out as a visibly weaker implementation. |
| JSON on the wire                            | Fails the assignment's explicit Avro requirement.                                                                                  |
| `RecordNameStrategy` subject naming         | Breaks Kafbat UI's default decoding for no gain at this scale.                                                                     |
| `FULL` / `FORWARD` compatibility            | `FULL` forbids legitimate additive evolution; `FORWARD` matches producer-first deployment, which is not this system's order.       |
| Registering schemas by hand before the demo | A demo step that can be forgotten is a demo step that will be forgotten. §12 lists "a demo that depends on luck" as marks-losing.  |
