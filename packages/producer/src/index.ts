import {
  baseEnvSchema,
  buildTopicRegistry,
  createLogger,
  createOrderSerializer,
  createRegistryClient,
  createShutdownManager,
  ensureOrderSchemaRegistered,
  isClassifiedError,
  loadConfig,
} from '@order-pipeline/shared';

const config = loadConfig(baseEnvSchema);

const logger = createLogger({
  service: 'producer',
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
      targetTopic: topics.orders,
    },
    'producer starting',
  );

  const registry = createRegistryClient({ url: config.SCHEMA_REGISTRY_URL });
  shutdown.register('schema-registry-client', () => {
    registry.close();
  });

  // Registration is idempotent, so every service does this at boot and none of
  // them needs to know whether it is first. A registry that is unreachable now
  // fails the process here rather than surfacing as an unserialisable message
  // later (ADR 007).
  const registration = await ensureOrderSchemaRegistered({
    client: registry,
    topic: topics.orders,
    logger,
  });

  // Proves the wiring end to end: if the schema were absent or incompatible,
  // constructing this and encoding a record would fail rather than silently
  // registering a new version.
  const serializer = createOrderSerializer({ client: registry, topic: topics.orders });
  const probe = await serializer.serialize({
    orderId: 'bootstrap-probe',
    product: 'Item1',
    price: 0,
  });

  logger.info(
    {
      subject: registration.subject,
      schemaId: registration.schemaId,
      version: registration.version,
      probeBytes: probe.length,
    },
    'avro serializer ready; awaiting phase 3 kafka producer',
  );

  await shutdown.wait();
}

main().catch((error: unknown) => {
  logger.fatal(
    { err: error, classified: isClassifiedError(error) ? error.kind : 'unclassified' },
    'producer failed to start',
  );
  process.exit(1);
});
