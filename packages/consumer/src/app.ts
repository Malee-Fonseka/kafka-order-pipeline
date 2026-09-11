import { hostname } from 'node:os';
import { performance } from 'node:perf_hooks';

import {
  type Logger,
  buildTopicRegistry,
  createIdempotentProducer,
  createKafkaClient,
  createOrderDeserializer,
  createRegistryClient,
  ensureOrderSchemaRegistered,
  readPackageVersion,
  readRetryMetadata,
} from '@order-pipeline/shared';

import { type Aggregator, createAggregator } from './aggregation/aggregator.js';
import { createStateStore } from './aggregation/state-store.js';
import { createMetrics } from './api/metrics.js';
import { type HealthReport, createApiServer } from './api/server.js';
import { type RuntimeStats, createStatsSampler, createThroughput } from './api/stats.js';
import type { ConsumerEnv } from './config.js';
import { createDlqWriter } from './dlq/writer.js';
import { createOrderHandler } from './handler.js';
import { createOrderConsumer } from './kafka.js';
import { type IncomingRecord, createPipeline } from './pipeline.js';
import { type Outcome, createForwardProcessor, createRecordProcessor } from './processor.js';
import { type BackoffOptions, DEFAULT_BACKOFF } from './retry/backoff.js';
import { createDelayGate } from './retry/delay-gate.js';
import { createRetryPublisher } from './retry/publisher.js';

/**
 * The consumer, composed.
 *
 * Everything the process does lives here, built from configuration and a
 * logger rather than from `process.env` and signal handlers. That split is
 * what lets the integration suite run the *real* consumer in-process against
 * a real broker: a test builds a config, calls `start()`, produces records,
 * awaits outcomes, and calls `stop()`. The entry point (`index.ts`) does the
 * same three things, with the shutdown manager's signal handling wrapped
 * around them.
 *
 * Teardown is ordered. Each component registers a closer as it starts, and
 * `stop()` runs them in reverse, so the consumer drains and disconnects
 * before the producers it wrote through are flushed, and before the registry
 * client it decoded through is closed.
 */

export interface ConsumerAppOptions {
  readonly config: ConsumerEnv;
  readonly logger: Logger;
  /** Overrides the package version reported in DLQ headers; tests pin it. */
  readonly appVersion?: string;
}

export type OutcomeListener = (outcome: Outcome, record: IncomingRecord) => void;

export interface ConsumerApp {
  start: () => Promise<void>;
  stop: () => Promise<void>;
  readonly instanceId: string;
  readonly aggregator: Aggregator;
  readonly tally: Readonly<Record<Outcome['kind'], number>>;
  readonly stats: () => RuntimeStats;
  readonly health: () => HealthReport;
  /** Where the API is listening, once started. */
  readonly apiAddress: string | undefined;
  /** Every terminal outcome, as it happens. Tests await these instead of polling. */
  onOutcome: (listener: OutcomeListener) => () => void;
}

interface Closer {
  readonly name: string;
  readonly close: () => Promise<void> | void;
}

export function createConsumerApp({ config, logger, appVersion }: ConsumerAppOptions): ConsumerApp {
  const topics = buildTopicRegistry(config.TOPIC_PREFIX);
  const instanceId = `${hostname()}-${String(process.pid)}`;
  const version = appVersion ?? readPackageVersion(import.meta.url);

  const aggregator = createAggregator();
  const metrics = createMetrics();
  const throughput = createThroughput();
  const tally: Record<Outcome['kind'], number> = {
    processed: 0,
    retried: 0,
    'dead-lettered': 0,
    forwarded: 0,
  };
  const outcomeListeners = new Set<OutcomeListener>();

  let degraded: string | undefined;
  const health = (): HealthReport =>
    degraded === undefined ? { ok: true } : { ok: false, reason: degraded };

  const closers: Closer[] = [];
  let apiAddress: string | undefined;
  let currentStats: (() => RuntimeStats) | undefined;
  let started = false;

  const seconds = (fromMs: number): number => (performance.now() - fromMs) / 1000;

  async function start(): Promise<void> {
    if (started) {
      throw new Error('consumer app already started');
    }
    started = true;

    logger.info(
      {
        instanceId,
        appVersion: version,
        brokers: config.KAFKA_BROKERS,
        schemaRegistry: config.SCHEMA_REGISTRY_URL,
        groupId: config.CONSUMER_GROUP_ID,
        sourceTopic: topics.orders,
        retryTopics: topics.retryTiers.map((t) => t.topic),
        dlqTopic: topics.dlq,
        stateTopic: topics.aggregateState,
        autoOffsetReset: config.CONSUMER_AUTO_OFFSET_RESET,
        inPlaceRetry: {
          attempts: config.CONSUMER_RETRY_INPLACE_ATTEMPTS,
          budgetMs: config.CONSUMER_RETRY_INPLACE_BUDGET_MS,
        },
        chaosTransientSucceedAfter: config.CONSUMER_CHAOS_TRANSIENT_SUCCEED_AFTER,
        api: `${config.CONSUMER_API_HOST}:${String(config.CONSUMER_API_PORT)}`,
      },
      'consumer starting',
    );

    // --- registry and serde ---
    const registry = createRegistryClient({ url: config.SCHEMA_REGISTRY_URL });
    closers.push({
      name: 'schema-registry-client',
      close: () => {
        registry.close();
      },
    });
    const registration = await ensureOrderSchemaRegistered({
      client: registry,
      topic: topics.orders,
      logger,
    });
    const deserializer = createOrderDeserializer({ client: registry, topic: topics.orders });

    // --- kafka ---
    const kafka = createKafkaClient({
      brokers: config.KAFKA_BROKERS,
      clientId: config.KAFKA_CLIENT_ID,
      logger,
    });

    // --- aggregation state and its changelog (D3) ---
    const stateStore = await createStateStore({
      kafka,
      topic: topics.aggregateState,
      groupId: config.CONSUMER_GROUP_ID,
      logger,
    });
    closers.push({ name: 'aggregation-changelog', close: () => stateStore.close() });
    const timedStateStore = {
      ...stateStore,
      write: async (entry: Parameters<typeof stateStore.write>[0]): Promise<void> => {
        const t0 = performance.now();
        await stateStore.write(entry);
        metrics.observeChangelogWrite(seconds(t0));
      },
    };

    // --- retry tiers and the DLQ (D5, D6) share one idempotent producer ---
    const writer = await createIdempotentProducer({
      kafka,
      logger,
      purpose: 'retry-tiers-and-dlq',
    });
    closers.push({
      name: 'retry-and-dlq-producer',
      close: async () => {
        await writer.flush({ timeout: 5_000 });
        await writer.disconnect();
      },
    });
    const publisher = createRetryPublisher({ producer: writer, topics, logger });
    const dlq = createDlqWriter({
      producer: writer,
      topic: topics.dlq,
      consumerGroup: config.CONSUMER_GROUP_ID,
      appVersion: version,
      logger,
    });
    const delayGate = createDelayGate({ logger });
    closers.push({
      name: 'retry-delay-gate',
      close: () => {
        delayGate.close();
      },
    });

    const backoff: BackoffOptions = {
      ...DEFAULT_BACKOFF,
      maxAttempts: config.CONSUMER_RETRY_INPLACE_ATTEMPTS,
      budgetMs: config.CONSUMER_RETRY_INPLACE_BUDGET_MS,
    };

    // --- the message paths ---
    const handler = createOrderHandler({
      aggregator,
      stateStore: timedStateStore,
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
    const processRetry = createForwardProcessor({ publisher });
    const retryTopics = new Set(topics.retryTiers.map((tier) => tier.topic));

    // --- the consumer itself ---
    const consumer = await createOrderConsumer({
      kafka,
      groupId: config.CONSUMER_GROUP_ID,
      topic: topics.orders,
      autoOffsetReset: config.CONSUMER_AUTO_OFFSET_RESET,
      logger,
      onRebalance: async ({ kind, partitions, lost }) => {
        if (kind === 'revoke') {
          // The state lives on, current, in the changelog. Whoever receives
          // these partitions restores it from there; keeping a copy here
          // would only serve stale numbers.
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
          // The client swallows errors thrown from this callback and proceeds
          // with the assignment. Record the failure here so /health can say
          // 503 rather than serving wrong figures silently.
          degraded = `state restore failed for partitions ${partitions.join(',')}`;
          logger.error(
            { partitions, err: error },
            'aggregation state restore failed; serving degraded',
          );
        }
      },
    });

    const pipeline = createPipeline<Outcome>({
      process: async (record) => {
        const t0 = performance.now();
        const outcome = retryTopics.has(record.topic)
          ? await processRetry(record)
          : await processOrder(record);

        metrics.observeProcessing(outcome.kind, seconds(t0));
        metrics.recordOutcome(outcome.kind);
        switch (outcome.kind) {
          case 'processed': {
            // From the *original* record's broker timestamp when this is a
            // retry: a forwarded record has a fresh timestamp, and end-to-end
            // means from first arrival, tiers included.
            const origin =
              readRetryMetadata(record.headers).originalTimestamp ?? Number(record.timestamp);
            metrics.observeEndToEnd(Math.max(0, (Date.now() - origin) / 1000));
            break;
          }
          case 'retried':
            metrics.recordRetried(outcome.tier.label);
            break;
          case 'forwarded': {
            const tier = topics.retryTiers.find((t) => t.topic === outcome.fromTopic);
            metrics.recordForwarded(tier?.label ?? outcome.fromTopic);
            break;
          }
          case 'dead-lettered':
            metrics.recordDeadLettered(outcome.errorType);
            break;
        }
        tally[outcome.kind] += 1;
        for (const listener of outcomeListeners) {
          listener(outcome, record);
        }
        return outcome;
      },
      commit: async (position) => {
        const t0 = performance.now();
        await consumer.commitOffsets([position]);
        metrics.observeCommit(seconds(t0));
        metrics.recordCommit();
        logger.debug(position, 'offset committed');
      },
    });

    // --- observability (D9) ---
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
      ownedPartitions: () => aggregator.snapshot().ownedPartitions,
      pausedPartitions: () => delayGate.paused,
      intervalMs: config.CONSUMER_STATS_INTERVAL_MS,
    });
    await stats.start();
    currentStats = stats.current;
    closers.push({ name: 'stats-sampler', close: () => stats.stop() });

    const api = createApiServer({
      aggregator,
      stats,
      metrics,
      health,
      logger,
      host: config.CONSUMER_API_HOST,
      port: config.CONSUMER_API_PORT,
    });
    apiAddress = await api.start();
    closers.push({ name: 'api-server', close: () => api.stop() });

    aggregator.onChange((entry) => {
      metrics.recordAggregate(entry);
    });

    closers.push({
      name: 'kafka-consumer',
      close: async () => {
        // Order matters and each step protects the next:
        //  1. drain — let the record currently inside eachMessage finish,
        //     write its changelog entry and commit.
        //  2. disconnect — leaves the group cleanly, so the broker reassigns
        //     our partitions immediately instead of waiting out sessionTimeout.
        logger.info({ inFlight: pipeline.inFlight, ...tally }, 'draining in-flight records');
        await pipeline.drain();
        await consumer.disconnect();
        logger.info(
          { ...pipeline.stats, ...tally },
          'kafka consumer disconnected; offsets committed',
        );
      },
    });

    // One group, one subscription: the main topic and every retry tier.
    // Pausing a retry partition stops fetching from it alone; heartbeats and
    // the poll loop carry on for everything else, which is why a five-minute
    // tier delay causes no rebalance.
    await consumer.subscribe({
      topics: [topics.orders, ...topics.retryTiers.map((t) => t.topic)],
    });

    logger.info(
      {
        subject: registration.subject,
        schemaId: registration.schemaId,
        topic: topics.orders,
        dashboard: `${apiAddress}/`,
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
            pause: () => payload.pause(),
            seek: (offset) => {
              consumer.seek({ topic, partition, offset });
            },
          });
          if (decision.kind === 'deferred') {
            metrics.recordPause();
            return;
          }
        }

        // A rejection here is deliberate: the pipeline has already declined
        // to commit, and throwing makes the client seek back and redeliver —
        // the at-least-once behaviour for an unexpected failure.
        await pipeline.handle(record);
      },
    });
  }

  async function stop(): Promise<void> {
    for (const closer of [...closers].reverse()) {
      try {
        await closer.close();
        logger.debug({ component: closer.name }, 'closed');
      } catch (error) {
        logger.error({ component: closer.name, err: error }, 'close failed');
      }
    }
    closers.length = 0;
  }

  return {
    start,
    stop,
    instanceId,
    aggregator,
    tally,
    stats: () => {
      if (currentStats === undefined) {
        throw new Error('stats are available after start()');
      }
      return currentStats();
    },
    health,
    get apiAddress() {
      return apiAddress;
    },
    onOutcome(listener) {
      outcomeListeners.add(listener);
      return () => {
        outcomeListeners.delete(listener);
      };
    },
  };
}
