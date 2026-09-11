import { randomUUID } from 'node:crypto';

import {
  APP_VERSION_HEADER,
  CORRELATION_ID_HEADER,
  type Order,
  type Producer,
  TRANSIENT_FAIL_PRODUCT,
  buildTopicRegistry,
  createIdempotentProducer,
  createKafkaClient,
  createLogger,
  createOrderSerializer,
  createPoisonPayload,
  createRegistryClient,
  encodeHeaders,
  ensureOrderSchemaRegistered,
  loadConfig,
  readDlqMetadata,
  scanTopic,
} from '@order-pipeline/shared';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { type ConsumerApp, createConsumerApp } from '../app.js';
import { type ConsumerEnv, consumerEnvSchema } from '../config.js';
import type { Outcome } from '../processor.js';
import { type Stack, startStack } from './stack.js';

/**
 * The integration suite (Phase 8): the real consumer, in-process, against a
 * real broker and a real schema registry in containers.
 *
 * Three scenarios, one per feature the assignment is graded on:
 *
 *   1. happy path — orders are aggregated and the figures match a
 *      hand-computed control set;
 *   2. transient recovery — a failing record rides the 5s tier, comes back,
 *      and is processed, with the group never rebalancing;
 *   3. poison pill — undecodable bytes reach the DLQ on the first delivery
 *      with the full forensic header set, and nothing else stops.
 *
 * Each scenario has its own topic prefix and consumer group on the shared
 * stack, so they cannot see each other's records.
 */

// Quiet by default — retries and dead letters log at warn by design, and the
// assertions cover them. LOG_LEVEL=info shows the whole journey when debugging.
const logger = createLogger({
  service: 'integration',
  level: process.env['LOG_LEVEL'] ?? 'error',
  pretty: false,
});

let stack: Stack;

beforeAll(async () => {
  stack = await startStack(logger);
}, 180_000);

afterAll(async () => {
  await stack.stop();
}, 60_000);

/** A consumer config for one scenario, isolated by prefix and group. */
function scenarioConfig(prefix: string, overrides: Record<string, string> = {}): ConsumerEnv {
  return loadConfig(consumerEnvSchema, {
    NODE_ENV: 'test',
    LOG_LEVEL: 'warn',
    KAFKA_BROKERS: stack.bootstrap,
    KAFKA_CLIENT_ID: `integration-${prefix}`,
    SCHEMA_REGISTRY_URL: stack.registryUrl,
    TOPIC_PREFIX: prefix,
    CONSUMER_GROUP_ID: `${prefix}-consumers`,
    CONSUMER_AUTO_OFFSET_RESET: 'earliest',
    CONSUMER_API_PORT: '0',
    CONSUMER_API_HOST: '127.0.0.1',
    CONSUMER_STATS_INTERVAL_MS: '1000',
    ...overrides,
  });
}

/** A producer plus serializer for a scenario's main topic. */
async function orderProducer(prefix: string): Promise<{
  send: (order: Order, headers?: Record<string, string>) => Promise<void>;
  sendRaw: (key: string, value: Buffer) => Promise<void>;
  serialize: (order: Order) => Promise<Buffer>;
  close: () => Promise<void>;
}> {
  const topics = buildTopicRegistry(prefix);
  const registry = createRegistryClient({ url: stack.registryUrl });
  await ensureOrderSchemaRegistered({ client: registry, topic: topics.orders, logger });
  const serializer = createOrderSerializer({ client: registry, topic: topics.orders });
  const kafka = createKafkaClient({
    brokers: [stack.bootstrap],
    clientId: `integration-producer-${prefix}`,
    logger,
  });
  const producer: Producer = await createIdempotentProducer({
    kafka,
    logger,
    purpose: `integration-${prefix}`,
  });

  return {
    serialize: (order) => serializer.serialize(order),
    async send(order, headers = {}) {
      await producer.send({
        topic: topics.orders,
        messages: [
          {
            key: order.product,
            value: await serializer.serialize(order),
            headers: encodeHeaders({ [CORRELATION_ID_HEADER]: randomUUID(), ...headers }),
          },
        ],
      });
    },
    async sendRaw(key, value) {
      await producer.send({
        topic: topics.orders,
        messages: [
          { key, value, headers: encodeHeaders({ [CORRELATION_ID_HEADER]: randomUUID() }) },
        ],
      });
    },
    async close() {
      await producer.flush({ timeout: 5_000 });
      await producer.disconnect();
      registry.close();
    },
  };
}

/** Resolves when `count` outcomes matching `predicate` have been observed. */
function outcomes(
  app: ConsumerApp,
  count: number,
  predicate: (outcome: Outcome) => boolean,
  timeoutMs = 60_000,
): Promise<Outcome[]> {
  return new Promise((resolve, reject) => {
    const seen: Outcome[] = [];
    const timer = setTimeout(() => {
      unsubscribe();
      reject(
        new Error(
          `timed out after ${String(timeoutMs)}ms waiting for ${String(count)} outcome(s); saw ${String(seen.length)}: ${seen.map((o) => o.kind).join(',')}`,
        ),
      );
    }, timeoutMs);
    const unsubscribe = app.onOutcome((outcome) => {
      if (!predicate(outcome)) {
        return;
      }
      seen.push(outcome);
      if (seen.length >= count) {
        clearTimeout(timer);
        unsubscribe();
        resolve(seen);
      }
    });
  });
}

describe('happy path', () => {
  const prefix = 'happy';
  let app: ConsumerApp;

  beforeAll(async () => {
    await stack.createTopics(prefix);
    app = createConsumerApp({ config: scenarioConfig(prefix), logger, appVersion: 'it' });
    await app.start();
  }, 60_000);

  afterAll(async () => {
    await app.stop();
  }, 30_000);

  it('aggregates orders into figures that match a hand-computed control set', async () => {
    // The Welford control set from welford.test.ts, split across two products:
    //   Item1: 10, 20          → n=2, mean 15
    //   Item2: 30, 40, 50      → n=3, mean 40
    //   global: 10..50         → n=5, mean 30, sample variance 250
    const producer = await orderProducer(prefix);
    const done = outcomes(app, 5, (o) => o.kind === 'processed');

    const orders: Order[] = [
      { orderId: '1', product: 'Item1', price: 10 },
      { orderId: '2', product: 'Item1', price: 20 },
      { orderId: '3', product: 'Item2', price: 30 },
      { orderId: '4', product: 'Item2', price: 40 },
      { orderId: '5', product: 'Item2', price: 50 },
    ];
    for (const order of orders) {
      await producer.send(order);
    }

    await done;
    await producer.close();

    const snapshot = app.aggregator.snapshot();
    expect(app.aggregator.product('Item1')).toMatchObject({ count: 2, mean: 15, min: 10, max: 20 });
    expect(app.aggregator.product('Item2')).toMatchObject({ count: 3, mean: 40, min: 30, max: 50 });
    expect(snapshot.global.count).toBe(5);
    expect(snapshot.global.mean).toBeCloseTo(30, 12);
    expect(snapshot.global.variance).toBeCloseTo(250, 9);
    expect(app.tally).toMatchObject({ processed: 5, 'dead-lettered': 0, retried: 0 });
    expect(app.health()).toEqual({ ok: true });
  }, 60_000);

  it('serves the same figures over the REST API', async () => {
    const response = await fetch(`${app.apiAddress ?? ''}/aggregates/Item2`);

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ product: 'Item2', count: 3, mean: 40 });
  });

  it('commits every processed record — lag returns to zero', async () => {
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    const stats = app.stats();

    expect(stats.counters.committed).toBeGreaterThanOrEqual(5);
    expect(stats.lag.total).toBe(0);
  });

  it('exposes counters and latency histograms with real samples', async () => {
    const response = await fetch(`${app.apiAddress ?? ''}/metrics`);
    const body = await response.text();

    expect(response.status).toBe(200);
    expect(body).toMatch(/^orders_consumed_total{outcome="processed"} 5$/m);
    expect(body).toMatch(/^orders_committed_total 5$/m);
    expect(body).toMatch(/^order_processing_duration_seconds_count{outcome="processed"} 5$/m);
    expect(body).toMatch(/^offset_commit_duration_seconds_count 5$/m);
    expect(body).toMatch(/^changelog_write_duration_seconds_count 5$/m);
    expect(body).toMatch(/^order_end_to_end_latency_seconds_count 5$/m);
    expect(body).toMatch(/^order_price_mean{product="Item2"} 40$/m);
  });
});

describe('transient recovery', () => {
  const prefix = 'transient';
  let app: ConsumerApp;

  beforeAll(async () => {
    await stack.createTopics(prefix);
    // Fails the first delivery; succeeds when the record returns from the 5s tier.
    app = createConsumerApp({
      config: scenarioConfig(prefix, { CONSUMER_CHAOS_TRANSIENT_SUCCEED_AFTER: '2' }),
      logger,
      appVersion: 'it',
    });
    await app.start();
  }, 60_000);

  afterAll(async () => {
    await app.stop();
  }, 30_000);

  it('retries in place, escalates to the 5s tier, forwards, and processes on the second delivery', async () => {
    const producer = await orderProducer(prefix);
    const retried = outcomes(app, 1, (o) => o.kind === 'retried');
    const forwarded = outcomes(app, 1, (o) => o.kind === 'forwarded');
    const processed = outcomes(app, 1, (o) => o.kind === 'processed');
    const started = Date.now();

    await producer.send({ orderId: '100', product: TRANSIENT_FAIL_PRODUCT, price: 99 });

    const [first] = await retried;
    expect(first).toMatchObject({ kind: 'retried', tier: { label: '5s' }, attempt: 1 });

    const [hop] = await forwarded;
    expect(hop).toMatchObject({ kind: 'forwarded', attempt: 1 });

    const [final] = await processed;
    expect(final).toMatchObject({ kind: 'processed', delivery: 2 });
    // The 5s tier really did take five seconds — the delay gate held it.
    expect(Date.now() - started).toBeGreaterThanOrEqual(5_000);

    await producer.close();

    expect(app.aggregator.product(TRANSIENT_FAIL_PRODUCT)?.count).toBe(1);
    expect(app.tally).toMatchObject({ retried: 1, forwarded: 1, processed: 1, 'dead-lettered': 0 });

    // The tier and the pause are counted, and the end-to-end latency
    // histogram saw the whole five-second journey.
    const body = await (await fetch(`${app.apiAddress ?? ''}/metrics`)).text();
    expect(body).toMatch(/^orders_retried_total{tier="5s"} 1$/m);
    expect(body).toMatch(/^retry_forwarded_total{tier="5s"} 1$/m);
    expect(body).toMatch(/^retry_partition_pauses_total [1-9]d*$/m);
    expect(body).toMatch(/^order_end_to_end_latency_seconds_bucket{le="5"} 0$/m);
  }, 60_000);

  it('never rebalanced: the same member owns every partition throughout', () => {
    // Twelve partitions (4 topics × 3) assigned once, never revoked. Ownership
    // of the orders partitions is what the aggregator reports.
    expect(app.aggregator.snapshot().ownedPartitions).toEqual([0, 1, 2]);
    expect(app.health()).toEqual({ ok: true });
  });
});

describe('poison pill', () => {
  const prefix = 'poison';
  let app: ConsumerApp;

  beforeAll(async () => {
    await stack.createTopics(prefix);
    app = createConsumerApp({ config: scenarioConfig(prefix), logger, appVersion: 'it' });
    await app.start();
  }, 60_000);

  afterAll(async () => {
    await app.stop();
  }, 30_000);

  it('dead-letters every flavour on the first delivery, with the full header set', async () => {
    const producer = await orderProducer(prefix);
    const reference = await producer.serialize({ orderId: '1', product: 'Item1', price: 1 });
    const deadLettered = outcomes(app, 3, (o) => o.kind === 'dead-lettered');

    await producer.sendRaw('Item1', createPoisonPayload('json-not-avro'));
    await producer.sendRaw('Item2', createPoisonPayload('unknown-schema-id'));
    await producer.sendRaw('Item3', createPoisonPayload('truncated-payload', reference));

    const results = await deadLettered;
    await producer.close();

    for (const result of results) {
      expect(result).toMatchObject({ kind: 'dead-lettered', errorType: 'permanent', attempt: 1 });
    }
    // No retry tier was ever involved: a poison pill is never transient.
    expect(app.tally).toMatchObject({ 'dead-lettered': 3, retried: 0, forwarded: 0 });
    expect(await (await fetch(`${app.apiAddress ?? ''}/metrics`)).text()).toMatch(
      /^orders_dead_lettered_total{error_type="permanent"} 3$/m,
    );

    // Now read the DLQ topic itself and check what actually landed.
    const topics = buildTopicRegistry(prefix);
    const kafka = createKafkaClient({
      brokers: [stack.bootstrap],
      clientId: 'integration-dlq-reader',
      logger,
    });
    const letters: {
      key: string;
      value: Buffer | null;
      headers: Record<string, unknown> | undefined;
    }[] = [];
    await scanTopic({
      kafka,
      topic: topics.dlq,
      groupIdPrefix: 'integration',
      logger,
      onRecord: (record) => {
        letters.push({
          key: record.key?.toString('utf8') ?? '',
          value: record.value,
          headers: record.headers === undefined ? undefined : { ...record.headers },
        });
      },
    });

    expect(letters).toHaveLength(3);
    const byKey = new Map(letters.map((l) => [l.key, l]));

    // Raw bytes, untouched: the JSON poison pill is still JSON in the DLQ.
    expect(byKey.get('Item1')?.value?.toString('utf8')).toContain('not avro');

    for (const letter of letters) {
      const meta = readDlqMetadata(letter.headers);
      expect(meta.errorType).toBe('permanent');
      expect(meta.errorClass).toBe('PermanentError');
      expect(meta.attempt).toBe(1);
      expect(meta.originalTopic).toBe(topics.orders);
      expect(meta.originalOffset).toBeDefined();
      expect(meta.originalTimestamp).toBeGreaterThan(0);
      expect(meta.originalKey).toBe(letter.key);
      expect(meta.consumerGroup).toBe(`${prefix}-consumers`);
      expect(meta.correlationId).toMatch(/^[0-9a-f-]{36}$/);
      expect(meta.firstFailedAt).toBeDefined();
      expect(meta.lastFailedAt).toBeDefined();
      expect(meta.errorStack).toContain('PermanentError');
      expect(letter.headers?.[APP_VERSION_HEADER]?.toString()).toBe('it');
    }
    expect(readDlqMetadata(byKey.get('Item1')?.headers).errorMessage).toMatch(/^deserialization:/);
    expect(readDlqMetadata(byKey.get('Item2')?.headers).errorMessage).toMatch(
      /^unknown-schema-id:/,
    );
    expect(readDlqMetadata(byKey.get('Item3')?.headers).errorMessage).toMatch(/^deserialization:/);
  }, 60_000);

  it('keeps processing good orders around the poison', async () => {
    const producer = await orderProducer(prefix);
    const processed = outcomes(app, 1, (o) => o.kind === 'processed');

    await producer.send({ orderId: '7', product: 'Item1', price: 12.5 });

    await processed;
    await producer.close();

    expect(app.aggregator.product('Item1')).toMatchObject({ count: 1, mean: 12.5 });
    expect(app.health()).toEqual({ ok: true });
  }, 60_000);
});
