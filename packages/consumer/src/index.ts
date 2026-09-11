import { hostname } from 'node:os';

import {
  buildTopicRegistry,
  createIdempotentProducer,
  createKafkaClient,
  createLogger,
  createOrderDeserializer,
  createRegistryClient,
  createShutdownManager,
  ensureOrderSchemaRegistered,
  isClassifiedError,
  loadConfig,
  readPackageVersion,
} from '@order-pipeline/shared';

import { createAggregator } from './aggregation/aggregator.js';
import { createStateStore } from './aggregation/state-store.js';
import { createMetrics } from './api/metrics.js';
import { type HealthReport, createApiServer } from './api/server.js';
import { createStatsSampler, createThroughput } from './api/stats.js';
import { consumerEnvSchema } from './config.js';
import { createDlqWriter } from './dlq/writer.js';
import { createOrderHandler } from './handler.js';
import { createOrderConsumer } from './kafka.js';
import { type IncomingRecord, createPipeline } from './pipeline.js';
import { type Outcome, createForwardProcessor, createRecordProcessor } from './processor.js';
import { type BackoffOptions, DEFAULT_BACKOFF } from './retry/backoff.js';
import { createDelayGate } from './retry/delay-gate.js';
import { createRetryPublisher } from './retry/publisher.js';

const config = loadConfig(consumerEnvSchema);

const logger = createLogger({
  service: 'consumer',
  level: config.LOG_LEVEL,
  pretty: config.NODE_ENV === 'development',
});

const topics = buildTopicRegistry(config.TOPIC_PREFIX);
const shutdown = createShutdownManager({ logger });
const instanceId = `${hostname()}-${String(process.pid)}`;
const appVersion = readPackageVersion(import.meta.url);

async function main(): Promise<void> {
  logger.info(
    {
      instanceId,
      brokers: config.KAFKA_BROKERS,
      schemaRegistry: config.SCHEMA_REGISTRY_URL,
      groupId: config.CONSUMER_GROUP_ID,
      sourceTopic: topics.orders,
      retryTopics: topics.retryTiers.map((t) => t.topic),
      dlqTopic: topics.dlq,
      appVersion,
      stateTopic: topics.aggregateState,
      inPlaceRetry: {
        attempts: config.CONSUMER_RETRY_INPLACE_ATTEMPTS,
        budgetMs: config.CONSUMER_RETRY_INPLACE_BUDGET_MS,
      },
      chaosTransientSucceedAfter: config.CONSUMER_CHAOS_TRANSIENT_SUCCEED_AFTER,
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
  const tally: Record<Outcome['kind'], number> = {
    processed: 0,
    retried: 0,
    'dead-lettered': 0,
    forwarded: 0,
  };

  // Retry machinery (D5). One producer serves both the changelog and the
  // retry republisher; both need the D2 guarantees and neither is hot.
  const retryProducer = await createIdempotentProducer({
    kafka,
    logger,
    purpose: 'retry-tiers-and-dlq',
  });
  shutdown.register('retry-producer', async () => {
    await retryProducer.flush({ timeout: 5_000 });
    await retryProducer.disconnect();
  });
  const publisher = createRetryPublisher({ producer: retryProducer, topics, logger });

  // The dead letter writer (D6) shares the same producer: it writes raw
  // bytes to a topic that never expires, with the failure in headers.
  const dlq = createDlqWriter({
    producer: retryProducer,
    topic: topics.dlq,
    consumerGroup: config.CONSUMER_GROUP_ID,
    appVersion,
    logger,
  });
  const delayGate = createDelayGate({ logger });
  shutdown.register('retry-delay-gate', () => {
    delayGate.close();
  });

  const backoff: BackoffOptions = {
    ...DEFAULT_BACKOFF,
    maxAttempts: config.CONSUMER_RETRY_INPLACE_ATTEMPTS,
    budgetMs: config.CONSUMER_RETRY_INPLACE_BUDGET_MS,
  };

  // The message path for the main topic:
  //   deserialize → handle (aggregate + changelog) → [stage 1 → stage 2] → commit
  const handler = createOrderHandler({
    aggregator,
    stateStore,
    chaos: { transientSucceedAfterDelivery: config.CONSUMER_CHAOS_TRANSIENT_SUCCEED_AFTER },
    onProcessed: () => {
      throughput.mark();
    },
  });
  const processOrder = createRecordProcessor({
    deserializer,
    handler,
    publisher,
    dlq,
    backoff,
    logger,
  });

  // The message path for a retry tier that is due: forward back to orders.
  const processRetry = createForwardProcessor({ publisher });

  const retryTopics = new Set(topics.retryTiers.map((tier) => tier.topic));

  const pipeline = createPipeline<Outcome>({
    process: async (record) => {
      const outcome = retryTopics.has(record.topic)
        ? await processRetry(record)
        : await processOrder(record);

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
      deadLettered: tally['dead-lettered'],
      retried: tally.retried,
      forwarded: tally.forwarded,
      committed: pipeline.stats.committed,
      failed: pipeline.stats.failed,
    }),
    pausedPartitions: () => delayGate.paused,
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

  // One group, one subscription: the main topic and every retry tier. Pausing
  // a retry partition stops fetching from it alone; heartbeats and the poll
  // loop carry on for everything else, which is why a five-minute tier delay
  // causes no rebalance.
  await consumer.subscribe({ topics: [topics.orders, ...topics.retryTiers.map((t) => t.topic)] });

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
    eachMessage: async (payload) => {
      const { topic, partition, message } = payload;
      const record: IncomingRecord = {
        topic,
        partition,
        offset: message.offset,
        timestamp: message.timestamp,
        key: message.key,
        value: message.value,
        headers: message.headers,
      };

      // A retry-tier record that is not yet due is held back by pausing its
      // partition and seeking to it — never by sleeping here. Nothing is
      // committed; the record is redelivered when the partition resumes.
      if (retryTopics.has(topic)) {
        const decision = delayGate.check(record, {
          // The client binds pause to this partition already; wrapping it
          // keeps the call site free of an unbound method reference.
          pause: () => payload.pause(),
          seek: (offset) => {
            consumer.seek({ topic, partition, offset });
          },
        });
        if (decision.kind === 'deferred') {
          return;
        }
      }

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
