import {
  GenericContainer,
  Network,
  type StartedNetwork,
  type StartedTestContainer,
  Wait,
} from 'testcontainers';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';

import { type Logger, buildTopicRegistry, createKafkaClient } from '@order-pipeline/shared';

/**
 * A disposable Kafka + Schema Registry stack for the integration suite.
 *
 * The same images as `docker-compose.yml`, in KRaft mode, on a private
 * network, with the same six topics — created by the admin client here
 * because the broker refuses to auto-create (§10.4) and there is no init
 * container in this setting. Ports are random, so the suite runs beside a
 * developer's stack without colliding.
 *
 * One stack per test file. Container start is the expensive part (~30 s);
 * the scenarios each use a distinct topic prefix and consumer group so they
 * share it without seeing each other's records.
 */

const KAFKA_IMAGE = 'confluentinc/cp-kafka:8.3.1';
const REGISTRY_IMAGE = 'confluentinc/cp-schema-registry:8.3.1';

export interface Stack {
  readonly bootstrap: string;
  readonly registryUrl: string;
  /** Creates the six topics for a prefix, as the compose init container would. */
  createTopics: (prefix: string) => Promise<void>;
  stop: () => Promise<void>;
}

export async function startStack(logger: Logger): Promise<Stack> {
  const network: StartedNetwork = await new Network().start();

  // `withHostname('kafka')` makes the broker advertise its in-network listener
  // as kafka:9092, which the alias resolves. Without it the advertised host
  // is the container id, and the registry's second connection fails.
  const kafka: StartedKafkaContainer = await new KafkaContainer(KAFKA_IMAGE)
    .withKraft()
    .withNetwork(network)
    .withNetworkAliases('kafka')
    .withHostname('kafka')
    .withEnvironment({ KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false' })
    .start();

  const bootstrap = `${kafka.getHost()}:${String(kafka.getMappedPort(9093))}`;

  const registry: StartedTestContainer = await new GenericContainer(REGISTRY_IMAGE)
    .withNetwork(network)
    .withEnvironment({
      SCHEMA_REGISTRY_HOST_NAME: 'schema-registry',
      SCHEMA_REGISTRY_LISTENERS: 'http://0.0.0.0:8081',
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'kafka:9092',
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: 'backward',
    })
    .withExposedPorts(8081)
    .withWaitStrategy(Wait.forHttp('/subjects', 8081).forStatusCode(200))
    .withStartupTimeout(120_000)
    .start();

  const registryUrl = `http://${registry.getHost()}:${String(registry.getMappedPort(8081))}`;

  logger.info({ bootstrap, registryUrl }, 'integration stack started');

  const client = createKafkaClient({ brokers: [bootstrap], clientId: 'integration-admin', logger });

  return {
    bootstrap,
    registryUrl,

    async createTopics(prefix) {
      const topics = buildTopicRegistry(prefix);
      const admin = client.admin();
      await admin.connect();
      try {
        await admin.createTopics({
          topics: [
            { topic: topics.orders, numPartitions: 3, replicationFactor: 1 },
            ...topics.retryTiers.map((tier) => ({
              topic: tier.topic,
              numPartitions: 3,
              replicationFactor: 1,
            })),
            {
              topic: topics.dlq,
              numPartitions: 1,
              replicationFactor: 1,
              configEntries: [{ name: 'retention.ms', value: '-1' }],
            },
            {
              topic: topics.aggregateState,
              numPartitions: 3,
              replicationFactor: 1,
              configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
            },
          ],
        });
      } finally {
        await admin.disconnect();
      }
    },

    async stop() {
      await registry.stop();
      await kafka.stop();
      await network.stop();
    },
  };
}
