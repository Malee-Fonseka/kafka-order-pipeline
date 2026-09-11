# ADR 006 — Welford aggregation with a changelog-backed state store

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phase:** 5
- **Implements:** design decisions D3 and D9
- **Relates to:** ADR 002 (ownership follows the partition key), ADR 005 (commit ordering)

## Context

The assignment asks for a running average of `price`. Three things make the
obvious implementation — `sum / count` in memory — wrong for this system.

**`price` is a 32-bit float.** The Avro schema says `float`, and every decoded
value is float32. Accumulating a float32 sum rounds on every addition; after
tens of thousands of orders the low bits of the sum are noise, and the mean
inherits it. Variance from `E[x²] − E[x]²` is worse: it subtracts two large,
nearly equal numbers and can go negative.

**Memory is lost on restart.** A running average that resets to zero every
deploy is not a running average.

**Consumers scale by partition.** With three partitions and the `product` key
(ADR 002), a second consumer instance takes over some products. Its aggregate
for those products must start from where the previous owner left off, not from
nothing.

## Decision

### Welford's online algorithm, in double precision

Each new value moves the mean incrementally — `mean += (x − mean) / n` — and
accumulates `m2`, the sum of squared deviations from the _running_ mean.
Variance is `m2 / (n − 1)`. Both updates are numerically stable, O(1) in time
and space, and the inputs are widened from float32 to `number` (IEEE double) at
the aggregation boundary. State per product and globally: `count`, `mean`,
`m2`, `min`, `max`, `lastUpdated`.

The unit tests carry a hand-computed control set (the phase gate) and two
demonstrations: a float32 running sum of 100 000 × `0.1` drifts by more than
`1e-6` where Welford holds within `1e-12`; and the textbook variance of four
values near `10⁹` collapses to garbage where Welford returns `30` exactly.

### Global = merge of the owned products

The global aggregate is not maintained separately. It is the pairwise Welford
**merge** (Chan, Golub & LeVeque) of every product this instance owns, computed
on read. One source of truth per product; "global" means the same thing live as
it does after a restore. Merge is also the mechanism that would make ADR 002's
bucketed-key upgrade work. Verified live: the merge of six products matched an
independent two-pass computation over the topic, mean and variance, exactly.

### A changelog topic as the state store

`orders.aggregate.state` is log-compacted and keyed by product. After every
processed order, the product's updated state is written there. Compaction keeps
only the latest value per key, so the topic is at every moment a complete
snapshot of every product's aggregate — durable across restarts, crashes and
rebalances, with no database.

This is a hand-rolled equivalent of a **Kafka Streams state store with its
changelog topic**. Streams would give it for free; this project builds it by
hand so the mechanism is visible and explainable, which is the point of the
exercise.

Restore is reading the topic to its current end and keeping the last value per
key. Verified live: six products from 56 changelog records in 282 ms, figures
identical to 16 significant digits, zero orders replayed.

### Write-ahead, per order — not periodic

D3 says "periodic snapshots". This implementation does something stronger:
the changelog entry is written for **every** processed order, and it is written
**before** the order's offset is committed. Three steps, in this order:

1. `next` — compute the updated state without touching memory;
2. `write` — make it durable in the changelog;
3. `apply` — advance memory and notify the dashboard.

Then the pipeline commits. Two consequences follow:

- **Restore is exact.** After any crash the restored aggregate is never behind
  the committed offset. There is no "minus the last five seconds".
- **A failed changelog write is safe.** Memory has not moved, the pipeline
  does not commit, the order is redelivered and folded in once. Doing
  `apply` before `write` would count it twice.

The window that remains is the one ADR 005 already documents: a crash between
the changelog write and the offset commit redelivers an order that the
changelog has already absorbed, and it is counted twice. At-least-once, stated
plainly.

The cost is one produce per order on the message path. At this system's rate
it is unmeasurable. At a rate where it matters, the relaxation is a periodic
flush of dirty products — a small change to `state-store.ts` — which trades the
exact-restore guarantee for throughput. That is the knob; it is not turned.

### Ownership follows partitions

Every entry records the `orders` partition it arrived on. In the rebalance
callback, which this client awaits before applying an assignment:

- **assign** — restore that partition's products from the changelog. Records
  for them cannot arrive until the restore has finished.
- **revoke** — drop that partition's products. They live on, current, in the
  changelog; the instance that receives the partition restores them.

Each instance's API therefore reports exactly the products it owns. Verified
live: with two instances, A owned partitions 0 and 2 and all six products; B
owned partition 1 and none; stopping A moved all six to B with identical
figures. The changelog format is JSON rather than Avro — it is internal
state, never an order message, and being readable in Kafbat UI is worth more
than compactness.

### Restore failure is visible, not silent

The client swallows errors thrown from the rebalance callback and proceeds
with the assignment. A failed restore would therefore serve wrong figures with
no indication. The handler records the failure itself, `/health` returns 503
with the reason, and the dashboard shows it.

## Consequences

**Positive**

- The mean is exact to the precision of the inputs; the variance is
  meaningful. Both are verified against hand-computed figures.
- Restart, crash and rebalance all restore exactly, from Kafka alone.
- The global figure is consistent with the per-product ones by construction.
- The mechanism is small enough to read in one sitting, which a framework
  state store is not.

**Negative**

- One extra produce per order. Documented above with the relaxation.
- The changelog topic must have the same partition count as `orders` for
  Kafka to co-locate keys — but nothing here relies on that: entries carry
  their `orders` partition explicitly and restore filters by it, so the
  guarantee does not depend on the partitioner.
- Restore reads the whole changelog on every assignment and filters. Fine at
  this scale (a handful of products); with thousands of products the read
  could be narrowed to the co-located changelog partition.
- Between a partition being revoked from one instance and assigned to another,
  no instance reports that partition's products. This is correct — nobody owns
  them — and lasts one rebalance.

## Alternatives considered

| Alternative                         | Rejected because                                                                                                                     |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `sum / count`                       | Accumulates float32 rounding error; variance is unstable. The reason D3 exists.                                                      |
| Kafka Streams                       | Provides the state store and changelog out of the box, but hides exactly the mechanism this project is meant to demonstrate.         |
| An external database                | A second system to run, and the aggregate then lives outside Kafka's delivery semantics.                                             |
| Periodic snapshots (the D3 wording) | Loses up to one interval of aggregate on a crash. Per-order write-ahead is strictly stronger at a cost that is invisible here.       |
| Restore everything on startup only  | Wrong after a rebalance: a partition assigned later would carry a stale copy of its products. Restore-on-assign is the correct unit. |
| Avro for the changelog              | Internal state, never an order. JSON is readable in the UI and versioned by a field.                                                 |
