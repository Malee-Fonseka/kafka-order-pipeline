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
