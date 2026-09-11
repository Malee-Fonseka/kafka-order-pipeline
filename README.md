# Kafka order pipeline

A Kafka system that produces and consumes **order** messages in **Avro**,
keeps a **running average of price** per product and globally, retries
transient failures **without ever blocking the consumer**, and parks permanent
failures in a **dead letter queue** that can be inspected and replayed.

TypeScript on Node 22, Confluent's librdkafka client, Schema Registry, Fastify
with a live WebSocket dashboard. Every non-obvious decision has an
[architecture decision record](docs/adr/README.md).

```
producer ──orders──▶ consumer ──▶ running averages ──▶ dashboard :3000
   │                    │  ╲
 CHAOS_MODE       retry tiers ╲── DLQ ──▶ dlq-inspector list / decode / replay
                  5s · 30s · 5m
```

## Quickstart

You need **Docker** (Desktop or Engine, with Compose v2) and **Node 22**
(`nvm use` reads `.nvmrc`). Five commands; five minutes, most of it image
pulls.

```bash
git clone <this repository> && cd kafka-order-pipeline
cp .env.example .env
npm ci && npm run build
npm run infra:up                     # Kafka (KRaft), Schema Registry, Kafbat UI, six topics
npm run start:consumer               # in one terminal …
npm run start:producer               # … and in another
```

Then open:

| Where                            | What                                                                           |
| -------------------------------- | ------------------------------------------------------------------------------ |
| <http://localhost:3000>          | The dashboard — live running averages, throughput, lag, retry tiers, DLQ depth |
| <http://localhost:8080>          | Kafbat UI — topics, messages decoded via the registry, consumer groups         |
| <http://localhost:8081/subjects> | Schema Registry — `["orders-value"]`                                           |

Stop either service with **Ctrl-C** and watch it shut down gracefully: the
producer flushes, the consumer drains and commits. `npm run infra:down` stops
the stack; `npm run infra:reset` wipes it.

For the scripted, fault-injecting demonstration — a second consumer joining,
retries climbing tier by tier, the DLQ filling and being replayed — follow
[`docs/DEMO.md`](docs/DEMO.md).

## What it does

**Produces orders** — `{ orderId, product, price }` per the assignment schema,
Avro-serialized through Schema Registry, keyed by `product`, from an
idempotent `acks=all` producer. Under `CHAOS_MODE` it also emits, at
configurable rates, records that fail transiently and records that cannot be
decoded at all — so the demo never depends on luck.

**Consumes with manual commits** — an offset is committed only after the
record has reached a terminal state: processed, republished to a retry tier,
or written to the DLQ. Shutdown drains the in-flight record before
disconnecting.

**Aggregates with Welford's algorithm** — in double precision, per product
and globally, because `price` is a 32-bit float and a running sum drifts. The
state is written to a compacted changelog topic before each commit, so a
restart, crash or rebalance restores it exactly. Each consumer instance owns
the products on its partitions and nothing else; a second instance takes over
its share, restored from the changelog, and the averages stay right.

**Retries in two stages** — up to three jittered attempts inside the handler
under a 2-second budget; then republish to `orders.retry.5s`, `.30s`, `.5m` in
turn, and finally the DLQ. A retry-tier record that is not yet due is held by
**pausing its partition and seeking back to it**, with a timer to resume —
never by sleeping. The consumer sat with a partition paused for a full five
minutes with zero rebalances; the run is recorded in [ADR 003](docs/adr/003-retry-strategy.md).

**Dead-letters raw bytes** — the DLQ record's value is the original bytes,
untouched, because the record usually could not be deserialized; every
diagnostic (origin, error type, class, message, stack, attempt count,
timestamps, consumer group, correlation id, app version) is a header.
`dlq-inspector` lists, decodes best-effort, and **replays** selected records
back to `orders` with the failure headers stripped and the correlation id
kept.

**Classifies every failure** with one typed function: connection errors, 5xx
and timeouts are transient; bad bytes, unknown schema ids, validation and
anything unrecognised are permanent. Retrying a poison pill is a livelock;
dead-lettering a network blip is data loss. The distinction is unit-tested.

## Delivery guarantee — stated honestly

This system is **at-least-once**.

Offsets are committed manually, after a terminal outcome, never before, so
no record is lost. The cost is a window one record wide: if the consumer
crashes _after_ handling a record and _before_ its commit lands, that record
is processed again on restart, and the running average would count its price
twice. Graceful shutdown closes the window; a hard crash does not.

It is **not exactly-once**, and no setting here makes it so. The producer's
idempotence removes duplicates from _its own_ retries only. Two upgrade paths
exist and are not implemented: idempotent aggregation keyed on `orderId`, or
Kafka transactions. [ADR 005](docs/adr/005-delivery-guarantee.md) has the
full argument.

## Fault injection

Set in `.env`, or use the presets in [`demo/`](demo/README.md):

| Variable                                 | Effect                                                                                                                                      |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `CHAOS_MODE=true`                        | Master switch                                                                                                                               |
| `CHAOS_TRANSIENT_RATE`                   | Share of records carrying `product = __TRANSIENT_FAIL__`, which the consumer fails on until a configured delivery                           |
| `CHAOS_POISON_RATE`                      | Share of records emitted as undecodable bytes, cycling three flavours: JSON on an Avro topic, an unregistered schema id, a truncated record |
| `CONSUMER_CHAOS_TRANSIENT_SUCCEED_AFTER` | The delivery on which the marker succeeds: `2` recovers from the 5s tier; `5` rides every tier to the DLQ                                   |

## Scripts

| Script                                                           | Purpose                                                                                                                        |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `npm run infra:up` / `infra:down` / `infra:reset` / `infra:logs` | The Docker stack                                                                                                               |
| `npm run start:consumer` / `start:producer`                      | Built services, from the repo root                                                                                             |
| `npm run dev:consumer` / `dev:producer`                          | Same, with `tsx watch` — run `npm run watch` alongside so `shared` is built                                                    |
| `npm run demo:*`                                                 | Demo presets: `producer:chaos`, `producer:transient`, `producer:poison`, `consumer:b`, `consumer:exhaust`, `consumer:restored` |
| `npm run schema:register`                                        | Register `schemas/order.avsc` without starting a service                                                                       |
| `npm run schema:evolution-demo`                                  | Dry-run three schema changes against `BACKWARD` compatibility                                                                  |
| `npm run dlq -- list \| decode <offset> \| replay <offset>…`     | The dead letter inspector (`--json`, `--dry-run`, `--all`, `--from/--to`)                                                      |
| `npm run build` / `lint` / `format:check` / `test`               | The quality gate, as CI runs it                                                                                                |
| `npm run test:integration`                                       | The testcontainers suite — Kafka and Schema Registry in containers, the real consumer against them                             |

## Configuration

Every variable is documented in [`.env.example`](.env.example); copy it to
`.env`. The shared ones (`KAFKA_BROKERS`, `KAFKA_CLIENT_ID`,
`SCHEMA_REGISTRY_URL`, `TOPIC_PREFIX`, `LOG_LEVEL`) are validated at boot with
every problem reported at once. Services read `.env` from the repository root
via Node's `--env-file`, which is why the root scripts exist.

## API

The consumer serves, on `CONSUMER_API_PORT` (default 3000):

| Route                      |                                                                                      |
| -------------------------- | ------------------------------------------------------------------------------------ |
| `GET /`                    | Dashboard                                                                            |
| `GET /aggregates`          | Every product this instance owns, plus the global merge                              |
| `GET /aggregates/:product` | One product; 404 if another instance owns it                                         |
| `GET /stats`               | Throughput, lag per partition, retry-tier and DLQ depth, paused partitions, counters |
| `GET /health`              | 200, or 503 if a state restore failed                                                |
| `GET /metrics`             | Prometheus: counters by outcome, tier and DLQ type; lag gauges; latency histograms   |
| `GET /ws`                  | WebSocket: a snapshot on connect, then coalesced updates                             |

## Testing

```bash
npm test                    # 287 unit tests, seconds, no services
npm run test:integration    # 8 scenarios against real containers, ~1 minute, needs Docker
```

The unit suite drives the real Avro serializer against the registry vendor's
in-memory client, works the Welford control set by hand in the test comments,
and tests the pause/seek delay gate with a fake partition and fake timers.
The integration suite starts Kafka and Schema Registry with testcontainers
and runs the real consumer in-process: happy path against a hand-computed
set, transient recovery through the 5s tier, and poison pills to the DLQ with
every header verified. CI runs both.

## Repository layout

```
docker-compose.yml         Kafka (KRaft) · Schema Registry · Kafbat UI · one-shot topic init
schemas/order.avsc         The one authoritative schema; registered at boot
packages/
  shared/                  config · logger · errors · serde · registry · topics · headers · retry · dlq · kafka · shutdown
  producer/                emission loop · chaos injection
  consumer/                app · pipeline · processor · handler · aggregation/ · retry/ · dlq/ · api/ · integration/
  dlq-tools/               dlq-inspector CLI
demo/                      Environment presets for the runbook
docs/
  ARCHITECTURE.md          Components, message path, topology, guarantees — and the numbered principles the code cites
  DEMO.md                  The eight-step live demonstration
  adr/                     Ten decision records, with an index
```

## Engineering standards

TypeScript `strict` plus `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`
and friends; zero `any`, zero `@ts-ignore`. `typescript-eslint` strict and
stylistic type-checked rules; `no-console` is an error, so every diagnostic is
a structured pino line carrying the record's correlation id — including
librdkafka's own logs, bridged. Conventional Commits. CI builds, lints, checks
formatting, runs the unit suite, then the integration suite.
