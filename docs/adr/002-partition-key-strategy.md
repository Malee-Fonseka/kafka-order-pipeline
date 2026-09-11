# ADR 002 — The message key is `product`, not `orderId`

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phase:** 3
- **Implements:** design decision D1

## Context

Every record published to `orders` needs a key. The obvious choice is `orderId`:
it is the record's natural identifier, it is unique, and it distributes
perfectly evenly across partitions. It is also wrong for this system.

Two Kafka facts drive the decision:

1. **Ordering is guaranteed only within a partition.** There is no global order
   across a topic.
2. **Partition selection is by key hash.** Records sharing a key always land on
   the same partition; records with different keys may not.

The `orders` topic has three partitions (§5.2), deliberately — one partition
would hide every parallelism bug this project is meant to demonstrate.

Now consider the aggregation requirement: a running average **per product**.
With `orderId` as the key, records for `Item1` scatter across all three
partitions. Start a second consumer instance and the group rebalances, giving
each instance a subset of partitions — and therefore a subset of `Item1`'s
records. Each instance computes an average over the fragment it happens to see.
Neither number is the average of `Item1`. Producing a correct figure would need
the instances to share state, which means a database, a distributed lock, or a
second aggregation stage: substantial machinery to recover a property that is
free if the key is chosen correctly.

## Decision

**Key every record by `product`.**

Every record for `Item1` hashes to the same partition. Kafka gives one partition
to exactly one consumer in a group, so exactly one instance owns `Item1`'s
running average at any moment. The aggregate is correct under parallelism with
no shared state, no coordination, and no locking.

This also makes the rebalance demonstration (§11 step 4) meaningful rather than
alarming: a second consumer starts, partitions are reassigned, and the averages
**stay correct** — because ownership of a product moves as a unit.

Poison pills are keyed by product too, even though their payload is
deliberately corrupt. A record keyed randomly might land on a partition no
consumer is actively demonstrating; keying it normally guarantees it reaches a
live consumer and fails visibly.

## Consequences

**Positive**

- Per-product aggregation is correct at any consumer count, by construction.
- No shared aggregation state, so no database and no distributed locking.
- Partition ownership is comprehensible: "who owns `Item1`?" has one answer.
- Rebalancing becomes a feature to demonstrate rather than a hazard to avoid.

**Negative — and this must be stated rather than glossed over**

- **Hot partitions.** Key distribution follows product popularity, not
  uniformity. One dominant product means one overloaded partition while others
  idle, and consumer throughput is then bounded by that single partition.
- **Parallelism is capped by product cardinality.** With five products and
  three partitions, at most three consumers do useful work; a fourth is idle.

Neither is a problem at this project's scale, and both are the standard
trade-off for key-based locality.

**The mitigation, deliberately not implemented:** give the key a bucket suffix —
`Item1-0`, `Item1-1`, … — to spread one product across several partitions, then
add a second aggregation stage that combines the per-bucket aggregates into a
per-product one. Welford's algorithm supports exactly this: two aggregates can
be merged pairwise without revisiting the underlying records (see ADR 006).
That is the correct fix at scale and it is a meaningful amount of extra
machinery, which is why it is documented here rather than built.

## Alternatives considered

| Alternative                | Rejected because                                                                                                                          |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Key by `orderId`           | Scatters a product across partitions; per-product aggregation becomes wrong the moment a second consumer starts.                          |
| No key (round-robin)       | Same defect as `orderId`, with no identifier benefit at all.                                                                              |
| One partition              | Makes the aggregate trivially correct by removing all parallelism — and removes the rebalance demonstration and any claim to scalability. |
| Composite bucketed key now | Correct at scale, but needs a second aggregation stage. Documented above as the known upgrade path.                                       |
