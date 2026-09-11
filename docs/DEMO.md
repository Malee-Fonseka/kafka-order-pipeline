# Live demonstration runbook

Eight steps, about fifteen minutes, nothing left to chance. Every failure the
demo shows is injected on purpose ([ADR 009](adr/009-fault-injection.md)), and
every step says what you should see before you move on.

Every command is `npm run …` from the repository root and works the same in
PowerShell, bash and zsh. The demo presets under [`demo/`](../demo/README.md)
are applied by the scripts; you never set an environment variable by hand.

**Before you start:** the [README quickstart](../README.md#quickstart) — Docker
running, Node 22, `.env` copied, `npm ci && npm run build`. Have four
terminals ready: **infra**, **consumer A**, **consumer B / tools**, **producer**.

---

## Step 1 — The stack, and the six topics

```
npm run infra:up
docker compose ps
docker compose logs kafka-init
```

**You should see**

- `kop-kafka` and `kop-schema-registry` reported **healthy**; `kop-kafka-ui`
  up. The registry and the UI waited for the broker's health check, not just
  its start, so nothing crash-looped.
- The init container's log ends with all six topics:

  ```
  --- topics present ---
  orders
  orders.aggregate.state
  orders.dlq
  orders.retry.30s
  orders.retry.5m
  orders.retry.5s
  ```

- Kafbat UI at <http://localhost:8080>: cluster `local`, six topics, `orders`
  with **3 partitions**, `orders.dlq` with 1.

**Why it matters** — `auto.create.topics.enable` is off. The topics exist
because they were created deliberately with these partition counts and
retention settings; a typo'd topic name in code would fail, not silently
create a seventh topic.

---

## Step 2 — Real Avro, through the registry

```
npm run schema:register
curl http://localhost:8081/subjects
curl http://localhost:8081/config/orders-value
```

> PowerShell: use `curl.exe`; plain `curl` there is an alias with different output.

**You should see**

- `["orders-value"]` — the subject, named by `TopicNameStrategy`.
- `{"compatibilityLevel":"BACKWARD"}` — set explicitly by the bootstrap, not
  inherited.
- Run `npm run schema:register` again: the same `schemaId: 1` and
  `version: 1`. Registration is idempotent, so every service does it at boot.

Optional, and worth thirty seconds:

```
npm run schema:evolution-demo
```

Three proposed schema changes, dry-run against the registry: adding a field
_with_ a default is **accepted**; adding one _without_ is **rejected**;
retyping `price` to a string is **rejected**. The registry is unchanged
afterwards. That is the compatibility gate working before deployment rather
than after corruption.

**Why it matters** — [ADR 007](adr/007-avro-schema-registry.md). Messages
carry a 5-byte frame (magic byte + schema id) and no schema. Kafbat UI can
decode them because the registry can; a shared `.avsc` file could not offer
that, or the compatibility gate.

---

## Step 3 — Consumer, producer, dashboard

Terminal **consumer A**:

```
npm run start:consumer
```

Terminal **producer**:

```
npm run start:producer
```

Open <http://localhost:3000>.

**You should see**

- Consumer log: `order schema registered`, `kafka consumer connected` with
  `autoCommit: false`, `partitions assigned; aggregation state restored`, then
  `order received` lines each carrying a `correlationId`.
- Producer log: `kafka producer connected` with `idempotent: true`,
  `acks: "all"`, `maxInFlightRequests: 5`, then records at 5 per second.
- Dashboard: the **global running average** settling; five products in the
  table with counts climbing; **throughput ≈ 5 msg/s**; consumer lag **0**;
  DLQ **0**; every retry tier **0**; connection indicator **Live**.
- In Kafbat UI, open `orders` → Messages, choose the **SchemaRegistry** value
  serde: readable JSON orders, and prices like `291.3800048828125` — float32
  narrowing on the wire, visible in the wild.

**Why it matters** — those float32 artifacts are why the aggregate uses
Welford's algorithm in double precision ([ADR 006](adr/006-aggregation-algorithm.md)).
And `x-correlation-id` on every record is the thread the rest of the demo
follows.

Leave both running.

---

## Step 4 — A second consumer: rebalance, and the averages stay right

Terminal **consumer B**:

```
npm run demo:consumer:b
```

Open <http://localhost:3001> beside the first dashboard.

**You should see**

- Consumer A log: `Revoke partitions … 12 partition(s)`, then
  `Assign partitions … 8 partition(s)`; a `partitions revoked` line, then a
  `partitions assigned` line naming the products that came back.
- Consumer B log: `partitions assigned; aggregation state restored` naming the
  products it now owns, restored from the changelog in a few hundred
  milliseconds.
- Each dashboard shows **only the products its instance owns** — the two
  tables are disjoint. Counts continue from where the previous owner left
  off, not from zero.
- Kafbat UI → Consumers → `order-consumers`: two members, partitions split
  between them.

> With the five default products, all of them hash to partitions 0 and 2, so
> the second instance may own only the empty partition 1 and show no products.
> That is correct — nothing lives there. For a busier split, restart the
> producer with `PRODUCER_PRODUCTS=Item1,Item2,Item3,Item4,Item5,Item6,Item7,Item8`
> in `.env`.

**Why it matters** — [ADR 002](adr/002-partition-key-strategy.md): the key is
`product`, so a product's records all land on one partition and exactly one
instance owns its average. The changelog ([ADR 006](adr/006-aggregation-algorithm.md))
is what lets ownership move without losing the count.

Stop consumer B with **Ctrl-C** (watch it drain and disconnect cleanly).
Consumer A takes everything back and restores the products B had.

---

## Step 5 — Transient failures: retries climb, nothing rebalances

Stop the producer (Ctrl-C). Then, in the **producer** terminal:

```
npm run demo:producer:transient
```

Ten records, about half of them carrying the marker product
`__TRANSIENT_FAIL__`, which the consumer's handler fails on until the record's
second delivery.

**You should see, per marker, in the consumer A log — in this order**

```
in-place retries exhausted; escalating to retry tiers    attempts=3 delivery=1
record republished to retry tier                         tier=5s attempt=1
retry record not yet due; partition paused and seeked back   remainingMs≈5000
retry partition resumed
retry delay elapsed; record forwarded back to main topic
order received after retry                               delivery=2
```

- Dashboard: **Retry · 5s** rises by one per marker; the marker appears in
  the product table as `__TRANSIENT_FAIL__` _after_ its retry — labelled
  **chaos marker**; DLQ stays **0**.
- `/stats`: `pausedPartitions` shows `orders.retry.5s[…]` during the wait.
- Kafbat UI → Consumers → `order-consumers`: **the same member id, the same
  12 partitions**, before and after. No rebalance.
- Kafbat UI → `orders.retry.5s`: the records, each with `x-attempt-count: 1`,
  `x-retry-not-before`, and the same `x-correlation-id` as on `orders`.

**Why it matters** — [ADR 003](adr/003-retry-strategy.md). The five-second
wait is real wall-clock time, and the handler never slept for it: the
partition was paused and seeked, and the poll loop never stopped. The same
mechanism held a partition for a full **five minutes** (the `5m` tier) with
zero revocations — that run is recorded in the ADR. Sleeping instead would
have rebalanced the group.

---

## Step 6 — Poison pills: straight to the DLQ, with the evidence

```
npm run demo:producer:poison
```

Ten records, three or four of them undecodable — cycling three flavours:
JSON on an Avro topic, a schema id the registry has never seen, and a real
record cut short.

**You should see**

- Consumer A log, one per pill: `record dead-lettered` with
  `errorType: "permanent"`, `attempt: 1`, and a `dlqOffset`. **No** retry
  lines: a poison pill never touches a tier.
- Dashboard: **Dead letter queue** rises to 3 or 4; **Processed** keeps
  climbing for the good records around them — nothing stalled.
- Kafbat UI → `orders.dlq` → open a record: the value shown as bytes (it is
  not Avro; that is the point), and the headers:

  ```
  x-error-type            permanent
  x-error-class           PermanentError
  x-error-message         deserialization: expected magic byte 0x00, found 0x7b
  x-original-topic        orders
  x-original-partition    2
  x-original-offset       61
  x-attempt-count         1
  x-correlation-id        264c8eac-…
  x-consumer-group        order-consumers
  x-app-version           1.0.0
  ```

**Why it matters** — [ADR 004](adr/004-dlq-raw-bytes.md) and
[ADR 010](adr/010-error-taxonomy.md). The record could not be deserialized,
so a DLQ that stored deserialized records would have nothing to store. The
bytes are kept exactly as they arrived; everything knowable is in headers.

---

## Step 7 — Inspect and replay

Terminal **tools**:

```
npm run dlq -- list
npm run dlq -- decode 0
```

**You should see**

- `list`: one row per dead letter — offset, when, `permanent`, class, attempt,
  key, origin (`orders[2]@61`), correlation id, reason — and a summary line
  such as `4 of 4 dead letter(s) shown — 4 permanent`.
- `decode 0`: the headers; the wire-format verdict (`not Confluent-framed
(first byte 0x7b)` for the JSON pill, or `magic byte 0x00, schema id 999999`
  for the unknown-id pill); the bytes as hex and text; and the decode attempt's
  own error. Each layer reports independently.

Now replay. A poison pill will only be dead-lettered again — correctly, with
`x-replay-count: 1` — so the meaningful replay is a record that failed for a
reason that has since gone away. The fastest way to stage one in a demo:

1. In **consumer A**, Ctrl-C, then `npm run demo:consumer:exhaust` — the
   marker now fails every tier. Send one marker with
   `npm run demo:producer:transient`, and let it climb 5s → 30s → 5m to the
   DLQ (about six minutes; a good moment for questions). If time is short,
   skip to replaying a poison pill instead and show the replay count.
2. Ctrl-C consumer A again and start it as `npm run demo:consumer:restored`
   — "the downstream is back".
3. Replay:

```
npm run dlq -- list --limit 1
npm run dlq -- replay <offset> --dry-run
npm run dlq -- replay <offset>
```

**You should see**

- `list --limit 1`: the marker, `transient-exhausted`, attempt `4`, with
  `x-first-failed-at` and `x-last-failed-at` about 5m40s apart.
- `--dry-run`: `would replay`, nothing sent.
- `replay`: `replayed → orders[2]@…`. Then in consumer A:
  `order received` for the **same correlation id**, `delivery: 1` — a fresh
  start. In Kafbat UI the replayed record on `orders` carries
  `x-replayed-from-dlq-offset`, `x-replayed-at`, `x-replay-count: 1`, the
  correlation id — and **no** `x-attempt-count`. The dead letter itself is
  still on `orders.dlq`, as the record of what happened.

**Why it matters** — this is the difference between "I made a DLQ topic" and
dead-letter _handling_. A failure that was the downstream's fault is not data
loss; it is `dlq-inspector replay`.

---

## Step 8 — Graceful shutdown, and resuming exactly where it stopped

Start the producer again (`npm run start:producer`) so records are flowing,
then in **consumer A** press **Ctrl-C**.

**You should see**

```
graceful shutdown started            reason: SIGINT
draining in-flight records           inFlight: 0
kafka consumer disconnected; offsets committed
graceful shutdown complete
```

Check the group:

```
docker exec kop-kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group order-consumers
```

`CURRENT-OFFSET` equals `LOG-END-OFFSET` on every partition it had — **lag 0**
at the moment of shutdown. Start it again:

```
npm run start:consumer
```

The first `order received` is at the next offset after the last commit.
Nothing is replayed, nothing is skipped, and the dashboard's counts continue
from the restored changelog — not from zero.

**Why it matters** — [ADR 005](adr/005-delivery-guarantee.md). Offsets are
committed manually, after a terminal outcome, and shutdown drains the
in-flight record before disconnecting. What you have just watched is
at-least-once delivery, honestly stated: the one-record window between
"handled" and "committed" is documented, not hidden.

---

## Cleaning up

```
npm run infra:down          # keeps the data volume; next start resumes
npm run infra:reset         # wipes everything; registry empty, topics fresh
```

## If something looks wrong

| Symptom                                        | Likely cause                                                                                  |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Consumer starts but `/health` says `degraded`  | A changelog restore failed. Check the registry and broker are healthy; restart the consumer.  |
| Producer log says `Unknown topic or partition` | The init container did not run. `docker compose logs kafka-init`; `npm run infra:reset`.      |
| Second consumer fails with `EADDRINUSE`        | Port 3001 is taken; edit `demo/consumer-b.env`.                                               |
| `npm run dlq -- …` prints nothing              | Logs go to stderr and data to stdout; if you redirected stdout, that is where the table went. |
| Kafbat UI shows bytes for `orders` values      | Choose the **SchemaRegistry** serde in the Messages view.                                     |
