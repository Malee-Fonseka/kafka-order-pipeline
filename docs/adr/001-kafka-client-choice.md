# ADR 001 — `@confluentinc/kafka-javascript`, not KafkaJS

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 1
- **Relates to:** ADR 007 (the companion Schema Registry client)

## Context

Every Node.js Kafka tutorial recommends **KafkaJS**. It is pure JavaScript,
its API is pleasant, and it has more search results than everything else
combined. It also has had **no release since February 2023** and is not
maintained by anyone with a stake in it. A project whose whole subject is
delivery guarantees should not rest on a client that nobody is fixing.

The alternatives:

| Client                           | Nature             | State                                                                  |
| -------------------------------- | ------------------ | ---------------------------------------------------------------------- |
| `kafkajs`                        | Pure JS            | Unmaintained since 2023                                                |
| `node-rdkafka`                   | librdkafka binding | Maintained by Blizzard; low-level, callback-heavy API                  |
| `@confluentinc/kafka-javascript` | librdkafka binding | Confluent's official client; KafkaJS-compatible API; actively released |

## Decision

**`@confluentinc/kafka-javascript`**, and its companion
`@confluentinc/schemaregistry` for Avro.

It wraps **librdkafka** — the same C core under Confluent's Go, .NET and
Python clients, and the most battle-tested Kafka client in existence — and
deliberately exposes a **KafkaJS-compatible API** to ease migration. It ships
TypeScript definitions and is released on a regular cadence (1.10.x at the
time of writing). Idempotent producers, cooperative rebalancing, `pause`,
`seek` and manual commits are all first-class because librdkafka implements
them; the JavaScript layer does not have to.

### Practical consequences the rest of the repository lives with

- **It is a native module.** Prebuilt binaries are published for Node 18–24
  on Linux (glibc and musl), macOS arm64 and Windows x64. Outside those,
  `node-gyp` compiles librdkafka, which needs Python and a C++ toolchain.
  Hence `.nvmrc` and `engines` pin Node 22: an LTS with a confirmed prebuilt
  everywhere (§10.2).
- **npm is the tested package manager.** Confluent documents yarn and pnpm
  support as experimental. This independently settles the npm-workspaces
  choice.
- **It is CommonJS; the project is ESM.** Named imports from CommonJS depend
  on Node's static export detection, which is best-effort and varies by Node
  version and bundler. The API is therefore reached through the package's
  **default export** and destructured, in exactly one place
  (`packages/shared/src/kafka.ts`), and that is a do-not-break rule (§10.1).
  A named import happens to work under the current Node — it was verified —
  and the rule stands anyway, because the failure mode is a confusing
  `undefined` at runtime rather than a build error.
- **Its logger writes plain objects to stdout.** Bridged onto pino in the
  same module, so every line in a demo log has one shape (§9).

## Consequences

**Positive**

- The client is maintained, by the vendor of the broker.
- Everything the design needs — idempotence, manual commit, pause/seek,
  rebalance callbacks — is native and well-trodden.
- The KafkaJS-shaped API keeps the code readable to anyone who has seen the
  tutorials.

**Negative**

- A native dependency: install needs a matching prebuilt or a toolchain, and
  the Node version is pinned.
- The KafkaJS compatibility layer is not complete (`consumer.stop()` is
  unsupported; `autoCommit` is per consumer, not per `run()`), and the
  differences are documented in the package's `MIGRATION.md` rather than in
  the types. Each one this project relies on is noted at the point of use.
- CommonJS-into-ESM needs care, once, in one file.

## Alternatives considered

| Alternative                  | Rejected because                                                                                                                                                                               |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| KafkaJS                      | Unmaintained. Correctness features like idempotence and cooperative rebalancing exist but have nobody fixing them.                                                                             |
| node-rdkafka                 | Same core, maintained, but a low-level API that would leave this project reimplementing what the Confluent layer already provides.                                                             |
| A Java/Kotlin implementation | The assignment allows any language; TypeScript was chosen for the team's fluency and for the single-page dashboard, and the Confluent client removes the usual reason to avoid Node for Kafka. |
