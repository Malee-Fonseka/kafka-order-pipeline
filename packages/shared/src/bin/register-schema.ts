import { baseEnvSchema, loadConfig } from '../config.js';
import { describeError } from '../errors.js';
import { createLogger } from '../logger.js';
import { createRegistryClient, ensureOrderSchemaRegistered } from '../registry.js';
import { ORDER_SCHEMA_PATH } from '../schema-file.js';
import { buildTopicRegistry } from '../topics.js';

/**
 * Registers `schemas/order.avsc` against a running registry, standalone.
 *
 * The services already do this at boot, so this is not required for the system
 * to work. It exists for the demo: it lets the registry be populated and
 * inspected at `/subjects` before any service is started, which is the order
 * the runbook walks through. It is also the quickest way to check that a
 * registry is reachable at all.
 */

const logger = createLogger({
  service: 'register-schema',
  level: 'info',
  pretty: process.env['NODE_ENV'] !== 'production',
});

async function main(): Promise<void> {
  const config = loadConfig(baseEnvSchema);
  const topics = buildTopicRegistry(config.TOPIC_PREFIX);
  const client = createRegistryClient({ url: config.SCHEMA_REGISTRY_URL });

  try {
    const registration = await ensureOrderSchemaRegistered({
      client,
      topic: topics.orders,
      logger,
    });

    logger.info(
      {
        registryUrl: config.SCHEMA_REGISTRY_URL,
        schemaFile: ORDER_SCHEMA_PATH,
        ...registration,
      },
      'schema registered — verify with: curl http://localhost:8081/subjects',
    );
  } finally {
    client.close();
  }
}

main().catch((error: unknown) => {
  logger.fatal({ err: error }, `schema registration failed: ${describeError(error)}`);
  process.exitCode = 1;
});
