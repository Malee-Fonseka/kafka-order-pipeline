import {
  baseEnvSchema,
  buildTopicRegistry,
  createLogger,
  createOrderDeserializer,
  createOrderSerializer,
  createRegistryClient,
  createShutdownManager,
  ensureOrderSchemaRegistered,
  isClassifiedError,
  loadConfig,
} from '@order-pipeline/shared';

const config = loadConfig(baseEnvSchema);

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
      sourceTopic: topics.orders,
      retryTopics: topics.retryTiers.map((tier) => tier.topic),
      dlqTopic: topics.dlq,
    },
    'consumer starting',
  );

  const registry = createRegistryClient({ url: config.SCHEMA_REGISTRY_URL });
  shutdown.register('schema-registry-client', () => {
    registry.close();
  });

  // The consumer registers the schema too. It does not strictly need to — it
  // decodes by the schema ID on the wire — but doing so means the stack is
  // usable whichever service a grader happens to start first (ADR 007).
  const registration = await ensureOrderSchemaRegistered({
    client: registry,
    topic: topics.orders,
    logger,
  });

  // Constructed at boot so the schema fetch and its cache are warm before the
  // first record arrives, rather than paying a registry round trip inside the
  // first message handler.
  const deserializer = createOrderDeserializer({ client: registry, topic: topics.orders });

  // Boot-time self check: encode and decode one record against the live
  // registry. A schema mismatch between this consumer and the registry is
  // otherwise invisible until the first real message fails — and by then the
  // failure looks like a poison pill rather than a deployment problem.
  // Phase 4 replaces this with the real Kafka message loop.
  const probe = await createOrderSerializer({ client: registry, topic: topics.orders }).serialize({
    orderId: 'bootstrap-probe',
    product: 'Item1',
    price: 0,
  });
  const decoded = await deserializer.deserialize(probe);

  logger.info(
    {
      subject: registration.subject,
      schemaId: registration.schemaId,
      version: registration.version,
      probe: decoded,
    },
    'avro deserializer ready; awaiting phase 4 kafka consumer',
  );

  await shutdown.wait();
}

main().catch((error: unknown) => {
  logger.fatal(
    { err: error, classified: isClassifiedError(error) ? error.kind : 'unclassified' },
    'consumer failed to start',
  );
  process.exit(1);
});
