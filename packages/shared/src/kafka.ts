import confluentKafka from '@confluentinc/kafka-javascript';

import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { Logger } from 'pino';

/**
 * Kafka client bootstrap, shared by every service.
 *
 * §10.1 — the package is CommonJS and the project is ESM, so the API is reached
 * through the **default export** and destructured. A named `import { KafkaJS }`
 * happens to resolve under the current Node and package version (Node's
 * `cjs-module-lexer` detects the re-export), but that detection is a
 * best-effort static analysis that varies by Node version and bundler, and the
 * failure mode is a confusing `undefined` at runtime rather than a build error.
 * This is a do-not-break rule; do not "clean it up".
 *
 * The `import type` above is a separate matter: it is erased entirely at
 * compile time, never becomes a runtime import, and so is unaffected.
 *
 * Centralised here so the producer, the consumer and the DLQ tools share one
 * copy of that pattern and one logger bridge, rather than three that drift.
 */
const { Kafka, logLevel } = confluentKafka.KafkaJS;

export type KafkaClient = KafkaJS.Kafka;
export type KafkaLogLevel = KafkaJS.logLevel;

/** librdkafka's version, for the startup banner. */
export const librdkafkaVersion: string = confluentKafka.librdkafkaVersion;

export interface KafkaClientOptions {
  readonly brokers: readonly string[];
  readonly clientId: string;
  readonly logger: Logger;
}

/**
 * Bridges the client's own logger onto pino.
 *
 * Left alone, librdkafka's binding writes plain objects straight to stdout —
 * `{ message: 'Producer disconnected', fac: 'BINDING' }` — which sits outside
 * the structured stream and defeats `no-console` (§9). During a live demo the
 * logs need to be greppable by correlation id, and a second, differently
 * shaped log format on the same terminal is exactly what makes that fail.
 */
export function createKafkaLogger(logger: Logger): KafkaJS.Logger {
  return {
    info: (message, extra) => {
      logger.info({ kafka: extra }, message);
    },
    error: (message, extra) => {
      logger.error({ kafka: extra }, message);
    },
    warn: (message, extra) => {
      logger.warn({ kafka: extra }, message);
    },
    debug: (message, extra) => {
      logger.debug({ kafka: extra }, message);
    },
    namespace: (namespace) => createKafkaLogger(logger.child({ namespace })),
    setLogLevel: () => {
      // pino owns the level, set once from LOG_LEVEL at construction. Letting
      // the client raise it at runtime would silently override the operator's
      // choice mid-demo.
    },
  };
}

/** Maps pino's level to the client's coarser enum. */
export function toKafkaLogLevel(pinoLevel: string): KafkaJS.logLevel {
  switch (pinoLevel) {
    case 'trace':
    case 'debug':
      return logLevel.DEBUG;
    case 'info':
      return logLevel.INFO;
    case 'warn':
      return logLevel.WARN;
    case 'error':
    case 'fatal':
      return logLevel.ERROR;
    default:
      return logLevel.INFO;
  }
}

/**
 * Builds the client from which producers and consumers are created.
 *
 * Deliberately carries no producer- or consumer-specific settings: those are
 * the substance of D2 and D7 respectively and belong beside the code that
 * depends on them, where a reviewer will actually look.
 */
export function createKafkaClient({ brokers, clientId, logger }: KafkaClientOptions): KafkaClient {
  return new Kafka({
    kafkaJS: {
      brokers: [...brokers],
      clientId,
      logger: createKafkaLogger(logger),
      logLevel: toKafkaLogLevel(logger.level),
    },
  });
}

export type Producer = KafkaJS.Producer;
export type RecordMetadata = KafkaJS.RecordMetadata;

export interface ProducerOptions {
  readonly kafka: KafkaClient;
  readonly logger: Logger;
  /** Appears in the connection log line; distinguishes the order producer from the changelog writer. */
  readonly purpose: string;
}

/**
 * Connects an idempotent producer (design decision D2).
 *
 * Every setting below is load-bearing, and the combination is what makes
 * "at-least-once with no client-side duplicates" an honest claim:
 *
 * | Setting | Why |
 * |---|---|
 * | `idempotent: true` | The client retries internally on transient broker errors. Without idempotence those retries can silently write the same record twice; with it, the broker de-duplicates by producer id and sequence number. |
 * | `acks: -1` (all) | No acknowledgement until every in-sync replica has the write. `acks: 1` would lose acknowledged records on a leader failover. |
 * | `maxInFlightRequests: 5` | The highest value that still preserves ordering under idempotence. Above 5 the broker cannot guarantee sequence ordering; below it, throughput drops for no benefit. |
 * | `retry` | Survives a broker restart without involving the retry topics, which exist for *message* failures, not transport failures. |
 * | `allowAutoTopicCreation: false` | §10.4 — a typo'd topic must fail loudly. The broker also refuses, but failing in the client is a clearer error. |
 *
 * Shared because every writer in the system — the order producer, the
 * aggregation changelog, the retry republisher, the DLQ writer, the replay
 * tool — needs exactly these guarantees. The idempotent producer is one line
 * of configuration that most submissions omit; it is worth being able to
 * explain in the viva.
 */
export async function createIdempotentProducer({
  kafka,
  logger,
  purpose,
}: ProducerOptions): Promise<Producer> {
  const producer = kafka.producer({
    kafkaJS: {
      idempotent: true,
      acks: -1,
      maxInFlightRequests: 5,
      allowAutoTopicCreation: false,
      retry: {
        retries: 10,
        initialRetryTime: 100,
        maxRetryTime: 30_000,
      },
      logger: createKafkaLogger(logger),
      logLevel: toKafkaLogLevel(logger.level),
    },
  });

  await producer.connect();
  logger.info(
    {
      purpose,
      idempotent: true,
      acks: 'all',
      maxInFlightRequests: 5,
      librdkafka: librdkafkaVersion,
    },
    'kafka producer connected',
  );

  return producer;
}

/**
 * librdkafka's rebalance event codes, for a `rebalance_cb`.
 *
 * The callback is a raw librdkafka option, not part of the KafkaJS-compatible
 * surface, so the codes it reports come from the native layer. Named here so
 * no service compares against a bare `-175`.
 */
export const REBALANCE_EVENT_CODES = {
  assign: confluentKafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS,
  revoke: confluentKafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS,
} as const;
