import {
  buildTopicRegistry,
  createKafkaClient,
  createLogger,
  createOrderDeserializer,
  createRegistryClient,
  createShutdownManager,
  ensureOrderSchemaRegistered,
  isClassifiedError,
  loadConfig,
} from '@order-pipeline/shared';

import { consumerEnvSchema } from './config.js';
import { createOrderConsumer } from './kafka.js';
import { type IncomingRecord, createPipeline } from './pipeline.js';
import { type Outcome, createRecordProcessor } from './processor.js';

const config = loadConfig(consumerEnvSchema);

const logger = createLogger({
  service: 'consumer',
  level: config.LOG_LEVEL,
  pretty: config.NODE_ENV === 'development',
});

const topics = buildTopicRegistry(config.TOPIC_PREFIX);
const shutdown = createShutdownManager({ logger });

async function main(): Promise<void> {
  logger.info(
    {
      brokers: config.KAFKA_BROKERS,
      schemaRegistry: config.SCHEMA_REGISTRY_URL,
      groupId: config.CONSUMER_GROUP_ID,
      sourceTopic: topics.orders,
      autoOffsetReset: config.CONSUMER_AUTO_OFFSET_RESET,
    },
    'consumer starting',
  );

  // --- dependencies, registered for shutdown in dependency order ---
  // Hooks run in reverse, so the consumer (registered last) drains and
  // disconnects first, while the registry client it decodes through is still
  // open.

  const registry = createRegistryClient({ url: config.SCHEMA_REGISTRY_URL });
  shutdown.register('schema-registry-client', () => {
    registry.close();
  });

  const registration = await ensureOrderSchemaRegistered({
    client: registry,
    topic: topics.orders,
    logger,
  });

  const deserializer = createOrderDeserializer({ client: registry, topic: topics.orders });

  const kafka = createKafkaClient({
    brokers: config.KAFKA_BROKERS,
    clientId: config.KAFKA_CLIENT_ID,
    logger,
  });

  const consumer = await createOrderConsumer({
    kafka,
    groupId: config.CONSUMER_GROUP_ID,
    autoOffsetReset: config.CONSUMER_AUTO_OFFSET_RESET,
    logger,
  });

  const pipeline = createPipeline<Outcome>({
    process: createRecordProcessor({ deserializer, logger }),
    commit: async (position) => {
      await consumer.commitOffsets([position]);
      logger.debug(position, 'offset committed');
    },
  });

  const tally = { processed: 0, skipped: 0 };

  shutdown.register('kafka-consumer', async () => {
    // Order matters and each step protects the next:
    //  1. drain — let the record currently inside eachMessage finish and
    //     commit. Disconnecting first would drop that commit on the floor.
    //  2. disconnect — leaves the group cleanly, so the broker reassigns our
    //     partitions immediately instead of waiting out sessionTimeout.
    logger.info({ inFlight: pipeline.inFlight, ...tally }, 'draining in-flight records');
    await pipeline.drain();
    await consumer.disconnect();
    logger.info({ ...pipeline.stats, ...tally }, 'kafka consumer disconnected; offsets committed');
  });

  await consumer.subscribe({ topics: [topics.orders] });

  logger.info(
    { subject: registration.subject, schemaId: registration.schemaId, topic: topics.orders },
    'consuming orders',
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const record: IncomingRecord = {
        topic,
        partition,
        offset: message.offset,
        timestamp: message.timestamp,
        key: message.key,
        value: message.value,
        headers: message.headers,
      };

      // A rejection here is deliberate: the pipeline has already declined to
      // commit, and throwing makes the client seek back and redeliver — the
      // at-least-once behaviour for an unexpected failure.
      const outcome = await pipeline.handle(record);
      tally[outcome.kind] += 1;
    },
  });

  await shutdown.wait();
}

main().catch((error: unknown) => {
  logger.fatal(
    { err: error, classified: isClassifiedError(error) ? error.kind : 'unclassified' },
    'consumer failed',
  );
  process.exit(1);
});
