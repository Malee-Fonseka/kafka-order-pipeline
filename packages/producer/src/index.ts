import { randomUUID } from 'node:crypto';

import {
  APP_VERSION_HEADER,
  CORRELATION_ID_HEADER,
  buildTopicRegistry,
  createIdempotentProducer,
  createKafkaClient,
  createLogger,
  createOrderSerializer,
  createRegistryClient,
  createShutdownManager,
  encodeHeaders,
  ensureOrderSchemaRegistered,
  isClassifiedError,
  loadConfig,
  readPackageVersion,
} from '@order-pipeline/shared';

import { createChaosInjector } from './chaos.js';
import { producerEnvSchema } from './config.js';
import { createEmitter } from './emitter.js';
import { createOrderGenerator } from './orders.js';

const config = loadConfig(producerEnvSchema);

const logger = createLogger({
  service: 'producer',
  level: config.LOG_LEVEL,
  pretty: config.NODE_ENV === 'development',
});

const topics = buildTopicRegistry(config.TOPIC_PREFIX);
const shutdown = createShutdownManager({ logger });
const appVersion = readPackageVersion(import.meta.url);

async function main(): Promise<void> {
  logger.info(
    {
      brokers: config.KAFKA_BROKERS,
      schemaRegistry: config.SCHEMA_REGISTRY_URL,
      targetTopic: topics.orders,
      ratePerSecond: config.PRODUCER_RATE_PER_SEC,
      products: config.PRODUCER_PRODUCTS,
      chaosMode: config.CHAOS_MODE,
      ...(config.CHAOS_MODE
        ? {
            transientRate: config.CHAOS_TRANSIENT_RATE,
            poisonRate: config.CHAOS_POISON_RATE,
          }
        : {}),
      maxMessages: config.PRODUCER_MAX_MESSAGES ?? 'unbounded',
    },
    'producer starting',
  );

  const registry = createRegistryClient({ url: config.SCHEMA_REGISTRY_URL });
  shutdown.register('schema-registry-client', () => {
    registry.close();
  });

  const registration = await ensureOrderSchemaRegistered({
    client: registry,
    topic: topics.orders,
    logger,
  });

  const serializer = createOrderSerializer({ client: registry, topic: topics.orders });
  const kafka = createKafkaClient({
    brokers: config.KAFKA_BROKERS,
    clientId: config.KAFKA_CLIENT_ID,
    logger,
  });
  const producer = await createIdempotentProducer({ kafka, logger, purpose: 'orders' });

  const generator = createOrderGenerator({
    products: config.PRODUCER_PRODUCTS,
    minPrice: config.PRODUCER_MIN_PRICE,
    maxPrice: config.PRODUCER_MAX_PRICE,
  });

  const chaos = createChaosInjector({
    enabled: config.CHAOS_MODE,
    transientRate: config.CHAOS_TRANSIENT_RATE,
    poisonRate: config.CHAOS_POISON_RATE,
  });

  const tally = { valid: 0, transient: 0, poison: 0 };

  /**
   * A recent well-formed payload, kept so the truncation poison flavour has
   * something real to cut short. Fabricating a "nearly valid" record without
   * one risks producing bytes that accidentally decode.
   */
  let lastValidPayload: Buffer | undefined;

  const emitter = createEmitter({
    ratePerSecond: config.PRODUCER_RATE_PER_SEC,
    maxMessages: config.PRODUCER_MAX_MESSAGES,
    emit: async (): Promise<void> => {
      const emission = chaos.plan(generator.next(), lastValidPayload);
      const correlationId = randomUUID();

      // Every record carries a correlation id so one order can be followed from
      // this log line, through the retry tiers, into the DLQ and back out via
      // the inspector's replay.
      const headers = encodeHeaders({
        [CORRELATION_ID_HEADER]: correlationId,
        [APP_VERSION_HEADER]: appVersion,
      });

      // D1: the key is always the product, never the order id. Hashing on
      // product keeps every record for a product on one partition, so exactly
      // one consumer instance owns that product's running average.
      const key = emission.kind === 'poison' ? emission.product : emission.order.product;

      let value: Buffer;
      if (emission.kind === 'poison') {
        // Deliberately bypasses the Avro serializer (D8). Serializing these
        // bytes would defeat the point: the consumer must genuinely fail to
        // deserialize them.
        value = emission.bytes;
        tally.poison += 1;
      } else {
        value = await serializer.serialize(emission.order);
        lastValidPayload = value;
        if (emission.kind === 'transient') {
          tally.transient += 1;
        } else {
          tally.valid += 1;
        }
      }

      const [metadata] = await producer.send({
        topic: topics.orders,
        messages: [{ key, value, headers }],
      });

      logger.debug(
        {
          correlationId,
          kind: emission.kind,
          key,
          bytes: value.length,
          partition: metadata?.partition,
          offset: metadata?.offset,
          ...(emission.kind === 'poison' ? { flavour: emission.flavour } : {}),
          ...(emission.kind === 'poison' ? {} : { orderId: emission.order.orderId }),
        },
        'record produced',
      );
    },
  });

  // Registered after the registry client, so it runs *before* it: hooks run in
  // reverse registration order, and the producer must finish flushing while its
  // dependencies are still alive.
  shutdown.register('kafka-producer', async () => {
    await emitter.stop();
    logger.info({ ...tally, emitted: emitter.emitted }, 'emission stopped; flushing');

    // Flush before disconnect: anything still in librdkafka's buffer is a
    // message the caller believes was sent. §12 — unflushed buffers on Ctrl-C.
    await producer.flush({ timeout: 5_000 });
    await producer.disconnect();
    logger.info('kafka producer flushed and disconnected');
  });

  logger.info(
    {
      subject: registration.subject,
      schemaId: registration.schemaId,
      intervalMs: Math.round(1000 / config.PRODUCER_RATE_PER_SEC),
    },
    'producing orders',
  );

  await emitter.run();

  // Reached only on a bounded run — an unbounded producer leaves this loop via
  // a signal instead. Drive the same hook sequence a signal would, so a
  // finite run flushes exactly like an interrupted one rather than exiting
  // with records still in librdkafka's buffer.
  logger.info({ ...tally, emitted: emitter.emitted }, 'message budget reached');
  await shutdown.trigger('message-budget-reached');
}

main().catch((error: unknown) => {
  logger.fatal(
    { err: error, classified: isClassifiedError(error) ? error.kind : 'unclassified' },
    'producer failed',
  );
  process.exit(1);
});
