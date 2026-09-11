import { createKafkaLogger, librdkafkaVersion, toKafkaLogLevel } from '@order-pipeline/shared';

import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { KafkaClient, Logger } from '@order-pipeline/shared';

/**
 * Producer construction, configured per design decision D2.
 *
 * The client itself (and the §10.1 import discipline) lives in
 * `@order-pipeline/shared`; this module owns only what makes a producer
 * *this* producer.
 */

export type Producer = KafkaJS.Producer;
export type RecordMetadata = KafkaJS.RecordMetadata;

export interface ProducerOptions {
  readonly kafka: KafkaClient;
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
export async function createOrderProducer({ kafka, logger }: ProducerOptions): Promise<Producer> {
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
      idempotent: true,
      acks: 'all',
      maxInFlightRequests: 5,
      librdkafka: librdkafkaVersion,
    },
    'kafka producer connected',
  );

  return producer;
}
