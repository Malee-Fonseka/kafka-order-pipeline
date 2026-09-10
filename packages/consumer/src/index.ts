import {
  baseEnvSchema,
  buildTopicRegistry,
  createLogger,
  createShutdownManager,
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
      targetTopic: topics.orders,
    },
    'producer scaffold started',
  );

  shutdown.register('placeholder', () => {
    logger.debug('nothing to release yet');
  });

  await shutdown.wait();
}

void main();
