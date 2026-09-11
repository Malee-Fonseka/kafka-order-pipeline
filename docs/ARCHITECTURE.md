# Architecture

A Kafka pipeline that produces and consumes **order** messages in Avro, keeps
a **running average of price** per product and globally, retries transient
failures **without blocking the consumer**, and parks permanent failures in a
**dead letter queue** that can be inspected and replayed.

Every non-obvious decision has an ADR in [`adr/`](adr/README.md). This
document is the map; the ADRs are the reasoning.

## 1. Components

```
                        ┌──────────────────────────────────────────────────┐
                        │            Schema Registry  :8081                │
                        │      subject orders-value · BACKWARD             │
                        └────────▲──────────────────────────▲──────────────┘
                                 │ register at boot         │ fetch schema by id
                                 │                          │
┌──────────┐      ┌──────────────┴────┐   orders    ┌───────┴─────────────────────────────┐
│ CHAOS    ├─────▶│  producer         │────────────▶│  consumer  (one or more instances)  │
│ MODE     │      │  idempotent,      │ 3 parts,    │                                     │
└──────────┘      │  acks=all,        │ key=product │  deserialize ─▶ stage 1 in-place    │
                  │  key = product    │             │     ─▶ aggregate (Welford)          │
                  └───────────────────┘             │     ─▶ changelog write ─▶ commit    │
                                                    └──┬────────┬──────────────┬──────────┘
                                                       │        │              │
                          still transient after stage 1│        │ permanent /  │ every processed order
                                                       ▼        │ exhausted    ▼
                                    ┌──────────────────────┐    │   ┌────────────────────────┐
                                    │ orders.retry.5s      │    │   │ orders.aggregate.state │
                                    │ orders.retry.30s     │    │   │ compacted, key=product │
                                    │ orders.retry.5m      │    │   │ (restore on assign)    │
                                    └──────────┬───────────┘    │   └────────────────────────┘
                                               │ due: forwarded ▼
                                               │ back to orders ┌────────────────────────┐
                                               └───────────────▶│ orders.dlq             │
                                                    (via orders)│ raw bytes + headers,   │
                                                                │ never expires          │
                                                                └───────────┬────────────┘
                                                                            │
                        ┌──────────────────┐                     ┌──────────▼────────────┐
                        │ Fastify :3000    │                     │ dlq-inspector CLI     │
                        │ REST · WebSocket │                     │ list · decode · replay│
                        │ dashboard        │                     │ (replay → orders)     │
                        └──────────────────┘                     └───────────────────────┘
```

| Package              | Role                                                                                                                                                                   |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `packages/shared`    | Config, logger, error taxonomy, Avro serde and registry bootstrap, topic registry, header names, retry and DLQ metadata, the Kafka client bootstrap, graceful shutdown |
| `packages/producer`  | Emits orders at a configurable rate; idempotent; keyed by product; fault injection under `CHAOS_MODE`                                                                  |
| `packages/consumer`  | Consumes `orders` and the retry tiers; aggregates; writes the changelog, retry tiers and DLQ; serves the API and dashboard                                             |
| `packages/dlq-tools` | `dlq-inspector` — list, decode and replay dead letters                                                                                                                 |

## 2. Topic topology

| Topic                    | Partitions | Config                   | Purpose                                |
| ------------------------ | ---------- | ------------------------ | -------------------------------------- |
| `orders`                 | 3          | `retention.ms=604800000` | Main stream, key = `product`           |
| `orders.retry.5s`        | 3          | 7-day retention          | Retry tier 1                           |
| `orders.retry.30s`       | 3          | 7-day retention          | Retry tier 2                           |
| `orders.retry.5m`        | 3          | 7-day retention          | Retry tier 3                           |
| `orders.dlq`             | 1          | `retention.ms=-1`        | Terminal failures — never expire       |
| `orders.aggregate.state` | 3          | `cleanup.policy=compact` | Aggregation changelog, key = `product` |

Three partitions, not one: it forces partition-aware state handling and makes
the rebalance demonstration real. `auto.create.topics.enable` is **off** on
the broker, so a typo'd topic name fails loudly instead of silently creating a
ghost topic; the topics are created by the one-shot `kafka-init` container.

Every topic name derives from one prefix (`TOPIC_PREFIX`, default `orders`),
so the compose bootstrap, the services and the tests cannot drift.

## 3. The message path

### Producer

1. Registers `schemas/order.avsc` with the registry at boot — idempotent, so
   every service does it and none needs to be first ([ADR 007](adr/007-avro-schema-registry.md)).
2. Serializes each order through `AvroSerializer` with **auto-registration
   off**: a producer that can invent schema versions at runtime defeats the
   compatibility gate.
3. Keys every record by **`product`** ([ADR 002](adr/002-partition-key-strategy.md)),
   attaches a correlation id and the package version as headers, and sends
   through an **idempotent, `acks=all`** producer ([ADR 008](adr/008-producer-delivery-configuration.md)).
4. Under `CHAOS_MODE`, replaces a configurable share of records with a
   **transient marker** (`product = __TRANSIENT_FAIL__`, still valid Avro) or a
   **poison pill** (raw undecodable bytes that bypass the serializer)
   ([ADR 009](adr/009-fault-injection.md)).

### Consumer, for a record on `orders`

```
deserialize ─┐
             ├─ stage 1: ≤3 in-place attempts, full jitter, ≤2 s ─┬─ ok ──▶ processed
handle ──────┘                                                    │
                                                    still transient
                                                                  │
                     stage 2: republish to the tier for this attempt ┼─ tier ─▶ retried
                                                                     └─ none ─▶ dead-lettered (transient-exhausted)
permanent at any point ────────────────────────────────────────────────────▶ dead-lettered (permanent)
```

- **handle** = fold the order into the per-product Welford state, write the
  updated state to the changelog, _then_ advance memory
  ([ADR 006](adr/006-aggregation-algorithm.md)).
- Every arrow on the right is a **terminal outcome**: the record's fate is
  durably recorded somewhere — the aggregate, a retry topic, the DLQ — and
  only then does the offset **commit** ([ADR 005](adr/005-delivery-guarantee.md)).
- Which errors are transient and which are permanent is one typed function
  ([ADR 010](adr/010-error-taxonomy.md)).

### Consumer, for a record on a retry tier

The same consumer group subscribes to `orders` and all three retry tiers.
A retry-tier record carries `x-retry-not-before`. If that time has not come:

1. **pause** the partition (the client stops fetching from it; heartbeats and
   polling continue for everything else);
2. **seek** the partition back to this record's offset;
3. **schedule a resume** on a timer;
4. return **without committing**.

When the timer fires, the record is redelivered, is now due, and is
**forwarded back to `orders`** unchanged — same key, same bytes, headers
intact — for the owning instance to process. Retry topics are delay queues,
not processing queues ([ADR 003](adr/003-retry-strategy.md)).

The attempt count rides in headers across every hop: after failed delivery
_k_ the record goes to tier _k_; after tier 3 it is dead-lettered. The ceiling
is therefore global, whichever instance handles which hop.

### Dead letters

The DLQ record's **value is the original bytes** and every diagnostic is a
header: origin topic/partition/offset/timestamp/key, error type, class,
message and stack, attempt count, first and last failure, consumer group,
correlation id, app version ([ADR 004](adr/004-dlq-raw-bytes.md)).
`dlq-inspector replay` sends a record back to `orders` with the failure
headers stripped — so it earns a fresh set of tiers — and the correlation id
kept.

## 4. Aggregation state and ownership

- **Welford's online algorithm**, in double precision, per product. `price`
  is a 32-bit float on the wire; a float32 running sum drifts, and the
  textbook variance collapses under cancellation. Welford does neither.
- **Global = merge** of the owned products (Chan et al.), computed on read.
- **Ownership follows partitions.** Because records are keyed by product,
  every record for a product arrives on one partition, and Kafka gives each
  partition to one member of the group. Each instance aggregates exactly the
  products on its partitions — no shared state, no coordination.
- **The changelog is the state store.** After every processed order, the
  product's updated state is written to `orders.aggregate.state` _before_ the
  offset commits. On **assign**, an instance restores its partitions'
  products from the changelog before any record is delivered; on **revoke**,
  it drops them. Restart, crash and rebalance all restore exactly. This is a
  hand-rolled Kafka Streams state store, built by hand so the mechanism is
  visible.

## 5. Delivery guarantee

**At-least-once.** Offsets are committed manually, after a terminal outcome,
and never before. The window between "handled" and "committed" is one record
wide; a crash inside it reprocesses that record on restart, and the running
average would count one price twice. This is stated, not hidden, in
[ADR 005](adr/005-delivery-guarantee.md), along with the two upgrade paths
(idempotent aggregation keyed on `orderId`, or Kafka transactions) that are
named without being claimed.

The producer's idempotence removes duplicates from _its own_ retries. It does
not make the system exactly-once.

## 6. Observability

| Surface                                   | What                                                                                                                                                     |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /`                                   | The dashboard: global average (hero), throughput, lag, DLQ depth, retry-tier depth, per-product table. One static page over a WebSocket; no build step   |
| `GET /aggregates`, `/aggregates/:product` | Owned products and the global merge                                                                                                                      |
| `GET /stats`                              | Throughput, consumer lag per partition, topic depths, paused retry partitions, counters                                                                  |
| `GET /health`                             | 200, or 503 if a state restore failed (the client swallows that error; the app records it)                                                               |
| `GET /metrics`                            | Prometheus: outcome counters by kind, tier and DLQ type; lag and depth gauges; histograms for processing, commit, changelog write and end-to-end latency |
| Structured logs                           | pino JSON, one shape for everything including librdkafka's own logger; every line about a record carries its correlation id                              |

## 7. Testing

- **Unit** (`npm test`, seconds): colocated `*.test.ts`. The serde round trip
  runs against the vendor's in-memory registry client; the Welford control
  set is worked by hand in the test comments; the delay gate is tested with a
  fake partition and fake timers; the classifier's rule table is enumerated.
- **Integration** (`npm run test:integration`, minutes, needs Docker): the
  real consumer in-process against Kafka and Schema Registry in containers.
  Happy path, transient recovery through the 5s tier, poison pill to DLQ with
  every header checked.

## Appendix — numbered principles referenced by the code and the ADRs

The source comments and ADRs cite these by number. They are the parts of the
project specification that the implementation is held to.

### §2 — What separates a working implementation from a correct one

- **§2.1 Blocking retry breaks consumer group membership.** Sleeping inside
  a message handler stops polling; past `max.poll.interval.ms` the broker
  declares the consumer dead, revokes its partitions and rebalances. Retry
  delays are implemented with **pause + seek**, never `await sleep()`.
- **§2.2 A DLQ that stores deserialized messages is broken by construction.**
  The most common permanent failure _is_ deserialization failure. The DLQ
  value is the **original raw bytes**; all diagnostics go in headers.
- **§2.3 Transient vs permanent must be an explicit classification.**
  Retrying a poison pill forever is a livelock; dead-lettering a network
  blip is data loss. The distinction is a typed, unit-tested function.
- **§2.4 The offset commit strategy determines the delivery guarantee.**
  Auto-commit plus async processing loses messages on crash. Manual commit
  after successful handling yields honest **at-least-once**. Claiming
  exactly-once without transactions is an overclaim.
- **§2.5 Avro on Kafka means Schema Registry.** The wire format is magic byte
  `0x00` + 4-byte schema id + Avro payload. Importing a shared `.avsc` into
  both services is a visibly weaker implementation.

### §4 — Technology stack

TypeScript 5 on Node 22, ESM, npm workspaces; `@confluentinc/kafka-javascript`
and `@confluentinc/schemaregistry`; `confluentinc/cp-kafka` and
`cp-schema-registry` 8.3.1 in KRaft mode; `ghcr.io/kafbat/kafka-ui`; Fastify;
zod; pino; prom-client; vitest and testcontainers; ESLint flat config with
type-aware rules; Prettier; GitHub Actions.

### §5.2 — Topic topology

As in section 2 above.

### §9 — Engineering standards

`strict` plus `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`,
`noImplicitOverride`, `noImplicitReturns`, `noFallthroughCasesInSwitch`,
`noPropertyAccessFromIndexSignature`, `verbatimModuleSyntax`. **Zero `any`.
Zero `@ts-ignore`.** `typescript-eslint` strict and stylistic type-checked
rule sets. **`no-console: error`** — every diagnostic goes through the
structured logger, which is what makes demo logs greppable; the one exception
is the CLI's product output. Conventional Commits, small and logical.

### §10 — Do-not-break rules

- **§10.1** The Kafka client is CommonJS; the project is ESM. Import the
  package's **default export** and destructure; never a named import. Do not
  "clean this up".
- **§10.2** Node 22, pinned by `.nvmrc` and `engines`, so CI and local agree
  and a prebuilt binary exists.
- **§10.3** Build `shared` before running a service in dev: `tsx` resolves
  `@order-pipeline/shared` to `dist/`. Run `npm run watch` alongside.
- **§10.4** Broker auto-topic-creation is off. Topics are created explicitly
  with correct partition counts and retention; a typo must fail loudly.
- **§10.5** Health-gated startup ordering in compose: the registry, the init
  container and the UI wait on `service_healthy`, not `service_started`.
- **§10.6** No `dotenv`; Node's `--env-file`. It resolves relative to the
  working directory, so services run from the repo root via the root
  scripts.
- **§10.7** Under `exactOptionalPropertyTypes`, build objects with a
  conditional spread so an optional key is absent rather than
  present-and-undefined. Never reach for `any`.
- **§10.8** Tests compile into `dist/`. Harmless.

### §11 — Live demonstration

The eight-step runbook is [`DEMO.md`](DEMO.md).

### §12 — Marks-losing checklist

`node_modules`, `dist` or `.env` committed · a messy history · a README that
does not get a grader running in five minutes · any `any` or `@ts-ignore` ·
blocking `sleep` for retry delay · a DLQ of deserialized objects · no graceful
shutdown · claiming exactly-once · **a demo that depends on something failing
by luck** · `auto.create.topics.enable` left on · a missing `package-lock.json`.
