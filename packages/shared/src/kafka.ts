import { randomUUID } from 'node:crypto';

import confluentKafka from '@confluentinc/kafka-javascript';

import type { KafkaJS } from '@confluentinc/kafka-javascript';
import { describeError } from './errors.js';

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

/** A record as delivered by a scan. Mirrors the consumer's payload without the client types. */
export interface ScannedRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: string;
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  readonly headers: Readonly<Record<string, unknown>> | undefined;
}

export interface ScanTopicOptions {
  readonly kafka: KafkaClient;
  readonly topic: string;
  /** The ephemeral group id is derived from this and deleted afterwards. */
  readonly groupIdPrefix: string;
  readonly logger: Logger;
  /** Called for every record up to the watermark. Throwing stops the scan. */
  readonly onRecord: (record: ScannedRecord) => void;
  /** Upper bound on the whole scan. Exceeding it is a failure, not a partial result. */
  readonly timeoutMs?: number;
}

export interface ScanResult {
  /** Records delivered to `onRecord`. */
  readonly scanned: number;
  readonly partitions: number;
  readonly elapsedMs: number;
}

/**
 * Reads a topic from its beginning to the high watermarks it had when the
 * scan started, then stops.
 *
 * "Read everything that is there right now" is not a primitive Kafka offers —
 * a consumer is an open-ended subscription — so it is built from two: the
 * admin API says where each partition ends, and an ephemeral consumer group
 * reads until it gets there. The group id is unique per scan, commits
 * nothing, and is deleted afterwards so it does not linger in the group list.
 *
 * Used by the aggregation changelog restore and by the DLQ inspector. Anything
 * written to the topic after the scan begins is not part of the result.
 */
export async function scanTopic({
  kafka,
  topic,
  groupIdPrefix,
  logger,
  onRecord,
  timeoutMs = 30_000,
}: ScanTopicOptions): Promise<ScanResult> {
  const started = Date.now();

  const admin = kafka.admin();
  await admin.connect();
  let goals: Map<number, bigint>;
  try {
    const watermarks = await admin.fetchTopicOffsets(topic);
    goals = new Map(
      watermarks
        .filter((w) => BigInt(w.high) > BigInt(w.low))
        .map((w) => [w.partition, BigInt(w.high) - 1n]),
    );
  } finally {
    await admin.disconnect();
  }

  if (goals.size === 0) {
    return { scanned: 0, partitions: 0, elapsedMs: Date.now() - started };
  }

  const groupId = `${groupIdPrefix}-scan-${randomUUID()}`;
  const reader = kafka.consumer({
    kafkaJS: {
      groupId,
      fromBeginning: true,
      autoCommit: false,
      allowAutoTopicCreation: false,
      logger: createKafkaLogger(logger),
      logLevel: toKafkaLogLevel(logger.level),
    },
  });

  const reached = new Map<number, bigint>();
  let scanned = 0;
  let failure: unknown;

  const complete = (): boolean =>
    [...goals].every(([partition, goal]) => (reached.get(partition) ?? -1n) >= goal);

  let resolveDone: () => void = () => undefined;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });

  await reader.connect();
  try {
    await reader.subscribe({ topics: [topic] });
    await reader.run({
      eachMessage: async ({ partition, message }) => {
        await Promise.resolve();
        if (failure !== undefined) {
          return;
        }
        scanned += 1;
        reached.set(partition, BigInt(message.offset));
        try {
          onRecord({
            topic,
            partition,
            offset: message.offset,
            timestamp: message.timestamp,
            key: message.key,
            value: message.value,
            headers: message.headers,
          });
        } catch (error) {
          failure = error;
          resolveDone();
          return;
        }
        if (complete()) {
          resolveDone();
        }
      },
    });

    const timeout = new Promise<never>((_, reject) => {
      setTimeout(() => {
        reject(
          new Error(
            `scan of ${topic} timed out after ${String(timeoutMs)}ms; ` +
              `reached ${JSON.stringify([...reached].map(([p, o]) => [p, o.toString()]))} ` +
              `of ${JSON.stringify([...goals].map(([p, o]) => [p, o.toString()]))}`,
          ),
        );
      }, timeoutMs).unref();
    });

    await Promise.race([done, timeout]);
  } finally {
    await reader.disconnect();
    // Best effort: an orphaned scan group is harmless (no commits, expires on
    // its own) but clutters the group list in Kafbat UI.
    const cleanup = kafka.admin();
    try {
      await cleanup.connect();
      await cleanup.deleteGroups([groupId]);
    } catch (error) {
      logger.debug({ groupId, err: error }, 'could not delete scan group');
    } finally {
      await cleanup.disconnect();
    }
  }

  if (failure !== undefined) {
    throw failure instanceof Error ? failure : new Error(describeError(failure));
  }

  return { scanned, partitions: goals.size, elapsedMs: Date.now() - started };
}
