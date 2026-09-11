import confluentKafka from '@confluentinc/kafka-javascript';

import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { Logger } from '@order-pipeline/shared';

/**
 * Kafka producer construction, configured per design decision D2.
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
 */
const { Kafka, logLevel } = confluentKafka.KafkaJS;

export type Producer = KafkaJS.Producer;
export type RecordMetadata = KafkaJS.RecordMetadata;

/**
 * Bridges the client's own logger onto pino.
 *
 * Left alone, librdkafka's binding writes plain objects straight to stdout —
 * `{ message: 'Producer disconnected', fac: 'BINDING' }` — which sits outside
 * the structured stream and defeats `no-console` (§9). During a live demo the
 * logs need to be greppable by correlation id, and a second, differently
 * shaped log format on the same terminal is exactly what makes that fail.
 */
function pinoLoggerAdapter(logger: Logger, level: KafkaJS.logLevel): KafkaJS.Logger {
  const adapter: KafkaJS.Logger = {
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
    namespace: (namespace) => pinoLoggerAdapter(logger.child({ namespace }), level),
    setLogLevel: () => {
      // pino owns the level, set once from LOG_LEVEL at construction. Letting
      // the client raise it at runtime would silently override the operator's
      // choice mid-demo.
    },
  };

  return adapter;
}

/** Maps pino's level to the client's coarser enum. */
function toKafkaLogLevel(pinoLevel: string): KafkaJS.logLevel {
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

export interface ProducerOptions {
  readonly brokers: readonly string[];
  readonly clientId: string;
  readonly logger: Logger;
}

/**
 * Connects an idempotent producer.
 *
 * Every setting below is load-bearing (D2), and the combination is what makes
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
 * The idempotent producer is one line of configuration that most submissions
 * omit; it is worth being able to explain in the viva.
 */
export async function createOrderProducer({
  brokers,
  clientId,
  logger,
}: ProducerOptions): Promise<Producer> {
  const level = toKafkaLogLevel(logger.level);
  const kafkaLogger = pinoLoggerAdapter(logger, level);

  const kafka = new Kafka({
    kafkaJS: {
      brokers: [...brokers],
      clientId,
      logger: kafkaLogger,
      logLevel: level,
    },
  });

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
      logger: kafkaLogger,
      logLevel: level,
    },
  });

  await producer.connect();
  logger.info(
    {
      brokers,
      clientId,
      idempotent: true,
      acks: 'all',
      maxInFlightRequests: 5,
      librdkafka: confluentKafka.librdkafkaVersion,
    },
    'kafka producer connected',
  );

  return producer;
}
