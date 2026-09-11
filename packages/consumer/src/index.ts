import { hostname } from 'node:os';

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

import { createAggregator } from './aggregation/aggregator.js';
import { createStateStore } from './aggregation/state-store.js';
import { createMetrics } from './api/metrics.js';
import { type HealthReport, createApiServer } from './api/server.js';
import { createStatsSampler, createThroughput } from './api/stats.js';
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
const instanceId = `${hostname()}-${String(process.pid)}`;

async function main(): Promise<void> {
  logger.info(
    {
      instanceId,
      brokers: config.KAFKA_BROKERS,
      schemaRegistry: config.SCHEMA_REGISTRY_URL,
      groupId: config.CONSUMER_GROUP_ID,
      sourceTopic: topics.orders,
      stateTopic: topics.aggregateState,
      autoOffsetReset: config.CONSUMER_AUTO_OFFSET_RESET,
      api: `${config.CONSUMER_API_HOST}:${String(config.CONSUMER_API_PORT)}`,
    },
    'consumer starting',
  );

  // --- dependencies, registered for shutdown in dependency order ---
  // Hooks run in reverse, so the consumer (registered last) drains and
  // disconnects first, while everything it depends on is still open.

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

  // Aggregation state and its changelog (D3).
  const aggregator = createAggregator();
  const stateStore = await createStateStore({
    kafka,
    topic: topics.aggregateState,
    groupId: config.CONSUMER_GROUP_ID,
    logger,
  });
  shutdown.register('aggregation-changelog', async () => {
    await stateStore.close();
  });

  // Health: a failed restore means this instance would serve wrong figures for
  // the affected partitions. The client swallows errors thrown from the
  // rebalance callback, so the failure is recorded here and surfaced as 503.
  let degraded: string | undefined;
  const health = (): HealthReport =>
    degraded === undefined ? { ok: true } : { ok: false, reason: degraded };

  // Observability (D9): counters, throughput, lag, topic depths, /metrics.
  const metrics = createMetrics();
  const throughput = createThroughput();
  const tally = { processed: 0, skipped: 0 };

  // The message path: deserialize → aggregate → changelog → (pipeline) commit.
  const process = createRecordProcessor({ deserializer, logger });

  const pipeline = createPipeline<Outcome>({
    process: async (record) => {
      const outcome = await process(record);

      if (outcome.kind === 'processed') {
        // Write-ahead, in three steps that must stay in this order:
        //   next  — compute the updated state without touching memory;
        //   write — make it durable in the changelog;
        //   apply — only now advance memory and notify the dashboard.
        // The offset commits after all three. On restart the restored
        // aggregate is therefore never behind the committed position, and a
        // failed changelog write leaves memory untouched so the redelivery
        // counts the order once (ADR 006).
        const entry = aggregator.next(outcome.order, {
          partition: record.partition,
          offset: record.offset,
          timestamp: Number(record.timestamp),
        });
        await stateStore.write(entry);
        aggregator.apply(entry);
        throughput.mark();
      }

      metrics.recordOutcome(outcome.kind);
      tally[outcome.kind] += 1;
      return outcome;
    },
    commit: async (position) => {
      await consumer.commitOffsets([position]);
      metrics.recordCommit();
      logger.debug(position, 'offset committed');
    },
  });
  const stats = createStatsSampler({
    kafka,
    topics,
    groupId: config.CONSUMER_GROUP_ID,
    instanceId,
    logger,
    throughput,
    counters: () => ({
      processed: tally.processed,
      skipped: tally.skipped,
      committed: pipeline.stats.committed,
      failed: pipeline.stats.failed,
    }),
    ownedPartitions: () => aggregator.snapshot().ownedPartitions,
    intervalMs: config.CONSUMER_STATS_INTERVAL_MS,
  });
  await stats.start();
  shutdown.register('stats-sampler', async () => {
    await stats.stop();
  });

  const api = createApiServer({
    aggregator,
    stats,
    metrics,
    health,
    logger,
    host: config.CONSUMER_API_HOST,
    port: config.CONSUMER_API_PORT,
  });
  await api.start();
  shutdown.register('api-server', async () => {
    await api.stop();
  });

  aggregator.onChange((entry) => {
    metrics.recordAggregate(entry);
  });

  const consumer = await createOrderConsumer({
    kafka,
    groupId: config.CONSUMER_GROUP_ID,
    topic: topics.orders,
    autoOffsetReset: config.CONSUMER_AUTO_OFFSET_RESET,
    logger,
    onRebalance: async ({ kind, partitions, lost }) => {
      if (kind === 'revoke') {
        // The state lives on, current, in the changelog. Whoever receives these
        // partitions restores it from there; keeping a copy here would only
        // serve stale numbers.
        const before = aggregator.snapshot().products;
        const dropped = aggregator.drop(partitions);
        for (const p of before) {
          if (partitions.includes(p.partition)) {
            metrics.forgetProduct(p.product);
          }
        }
        logger.info(
          { partitions, dropped, lost },
          'partitions revoked; aggregation state released',
        );
        return;
      }

      try {
        const restored = await stateStore.restore(partitions);
        aggregator.restore(partitions, restored);
        for (const entry of restored) {
          metrics.recordAggregate(entry);
        }
        degraded = undefined;
        logger.info(
          { partitions, products: restored.map((e) => e.product).sort() },
          'partitions assigned; aggregation state restored',
        );
      } catch (error) {
        degraded = `state restore failed for partitions ${partitions.join(',')}`;
        logger.error(
          { partitions, err: error },
          'aggregation state restore failed; serving degraded',
        );
      }
    },
  });

  shutdown.register('kafka-consumer', async () => {
    // Order matters and each step protects the next:
    //  1. drain — let the record currently inside eachMessage finish, write
    //     its changelog entry and commit. Disconnecting first would drop that.
    //  2. disconnect — leaves the group cleanly, so the broker reassigns our
    //     partitions immediately instead of waiting out sessionTimeout.
    logger.info({ inFlight: pipeline.inFlight, ...tally }, 'draining in-flight records');
    await pipeline.drain();
    await consumer.disconnect();
    logger.info({ ...pipeline.stats, ...tally }, 'kafka consumer disconnected; offsets committed');
  });

  await consumer.subscribe({ topics: [topics.orders] });

  logger.info(
    {
      subject: registration.subject,
      schemaId: registration.schemaId,
      topic: topics.orders,
      dashboard: `http://${config.CONSUMER_API_HOST}:${String(config.CONSUMER_API_PORT)}/`,
    },
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
      await pipeline.handle(record);
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
