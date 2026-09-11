import { createKafkaLogger, librdkafkaVersion, toKafkaLogLevel } from '@order-pipeline/shared';

import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { KafkaClient, Logger } from '@order-pipeline/shared';

/**
 * Consumer construction, configured per design decision D7.
 *
 * The client itself (and the §10.1 import discipline) lives in
 * `@order-pipeline/shared`; this module owns only what makes a consumer
 * *this* consumer.
 */

export type Consumer = KafkaJS.Consumer;
export type EachMessagePayload = KafkaJS.EachMessagePayload;

export interface ConsumerOptions {
  readonly kafka: KafkaClient;
  readonly groupId: string;
  readonly autoOffsetReset: 'earliest' | 'latest';
  readonly logger: Logger;
}

/**
 * How long the broker waits for a heartbeat before declaring the consumer
 * dead and rebalancing. librdkafka heartbeats on its own thread, so this is
 * about network partitions, not slow handlers.
 */
const SESSION_TIMEOUT_MS = 30_000;

/**
 * The client maps this onto `max.poll.interval.ms`: the longest a single
 * `eachMessage` may run before the broker considers the consumer stuck and
 * rebalances. It is the number §2.1 is about. Five minutes is librdkafka's
 * default; kept explicit so the Phase 6 argument — "our delayed retry never
 * blocks the handler this long" — refers to a value a reviewer can see.
 */
const MAX_POLL_INTERVAL_MS = 300_000;

/**
 * Connects a manually-committing consumer.
 *
 * | Setting | Why |
 * |---|---|
 * | `autoCommit: false` | The whole of D7. Auto-commit acknowledges on a timer regardless of whether the handler finished, which loses messages on a crash. Offsets are committed by the pipeline, after the record reaches a terminal state, and nowhere else. |
 * | `fromBeginning` | Consulted only when the group has no committed offset. Once one exists it wins — that is what makes restart-and-resume work. |
 * | `allowAutoTopicCreation: false` | §10.4 — subscribing to a typo'd topic must fail, not create a ghost. |
 * | `sessionTimeout` / `rebalanceTimeout` | Explicit rather than defaulted, because Phase 6 argues about them. |
 *
 * Not subscribed here: subscription and the run loop are the caller's, so
 * shutdown ordering stays visible in one place.
 */
export async function createOrderConsumer({
  kafka,
  groupId,
  autoOffsetReset,
  logger,
}: ConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer({
    kafkaJS: {
      groupId,
      autoCommit: false,
      fromBeginning: autoOffsetReset === 'earliest',
      allowAutoTopicCreation: false,
      sessionTimeout: SESSION_TIMEOUT_MS,
      rebalanceTimeout: MAX_POLL_INTERVAL_MS,
      logger: createKafkaLogger(logger),
      logLevel: toKafkaLogLevel(logger.level),
    },
  });

  await consumer.connect();
  logger.info(
    {
      groupId,
      autoCommit: false,
      autoOffsetReset,
      sessionTimeoutMs: SESSION_TIMEOUT_MS,
      maxPollIntervalMs: MAX_POLL_INTERVAL_MS,
      librdkafka: librdkafkaVersion,
    },
    'kafka consumer connected',
  );

  return consumer;
}
