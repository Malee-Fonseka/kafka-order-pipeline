# Architecture decision records

One record per decision that a reviewer could reasonably ask "why?" about.
Each states the context, the decision, what it cost, and what was rejected.
Numbers are stable; phases refer to the build order.

| ADR                                           | Decision                                                                                                      | Phase |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----- |
| [001](001-kafka-client-choice.md)             | `@confluentinc/kafka-javascript`, not KafkaJS — a maintained librdkafka binding; the CommonJS import rule     | 1     |
| [002](002-partition-key-strategy.md)          | The message key is `product`, so one instance owns each product's average; the hot-partition trade-off        | 3     |
| [003](003-retry-strategy.md)                  | Two-stage retry: bounded jittered in-place attempts, then tiered topics with a pause/seek delay — never sleep | 6     |
| [004](004-dlq-raw-bytes.md)                   | The DLQ stores raw bytes with forensic headers, and has a replay tool                                         | 7     |
| [005](005-delivery-guarantee.md)              | At-least-once via manual commits after terminal outcomes; the duplicate-on-crash window stated                | 4     |
| [006](006-aggregation-algorithm.md)           | Welford in double precision; the changelog topic as a hand-rolled state store; ownership by partition         | 5     |
| [007](007-avro-schema-registry.md)            | Avro through Schema Registry, the wire format, `BACKWARD` compatibility, registration at boot                 | 2     |
| [008](008-producer-delivery-configuration.md) | Idempotent producer with `acks=all`, and what that does not buy                                               | 3     |
| [009](009-fault-injection.md)                 | Fault injection built into the producer, so the demo never depends on luck                                    | 3     |
| [010](010-error-taxonomy.md)                  | Failures are classified by one typed function; unknowns default to permanent                                  | 2, 6  |

Reading order for the whole story: 007 → 002 → 005 → 006 → 010 → 003 → 004.
