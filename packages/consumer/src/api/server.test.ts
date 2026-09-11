import type { Order } from '@order-pipeline/shared';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { type Aggregator, createAggregator } from '../aggregation/aggregator.js';
import { createMetrics } from './metrics.js';
import { type ApiServer, type ServerFrame, createApiServer } from './server.js';
import type { RuntimeStats, StatsSampler } from './stats.js';

import type { Logger } from 'pino';
import { WebSocket } from 'ws';

const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
  fatal: () => undefined,
  trace: () => undefined,
  child: () => silentLogger,
  level: 'silent',
} as unknown as Logger;

const stats: RuntimeStats = {
  instanceId: 'test-1',
  groupId: 'order-consumers',
  uptimeSeconds: 12,
  throughputPerSecond: 4.5,
  counters: { processed: 10, deadLettered: 1, retried: 0, forwarded: 0, committed: 11, failed: 0 },
  ownedPartitions: [0, 2],
  pausedPartitions: [],
  lag: {
    total: 3,
    partitions: [
      { partition: 0, committed: 17, end: 20, lag: 3, owned: true },
      { partition: 2, committed: 40, end: 40, lag: 0, owned: true },
    ],
  },
  retryTiers: [
    { label: '5s', topic: 'orders.retry.5s', depth: 0 },
    { label: '30s', topic: 'orders.retry.30s', depth: 0 },
    { label: '5m', topic: 'orders.retry.5m', depth: 0 },
  ],
  dlqDepth: 0,
  sampledAt: '2026-01-01T00:00:00.000Z',
  sampleError: null,
};

function fakeSampler(): StatsSampler {
  return {
    current: () => stats,
    onSample: () => () => undefined,
    start: async () => Promise.resolve(),
    stop: async () => Promise.resolve(),
  };
}

function fold(aggregator: Aggregator, product: string, price: number, partition: number): void {
  const order: Order = { orderId: '1', product, price };
  aggregator.apply(aggregator.next(order, { partition, offset: '1', timestamp: 1 }));
}

describe('api server', () => {
  let aggregator: Aggregator;
  let server: ApiServer;
  let health: { ok: boolean; reason?: string };

  beforeEach(() => {
    aggregator = createAggregator();
    health = { ok: true };
    server = createApiServer({
      aggregator,
      stats: fakeSampler(),
      metrics: createMetrics(),
      health: () => health,
      logger: silentLogger,
      host: '127.0.0.1',
      port: 0,
    });
  });

  afterEach(async () => {
    await server.stop();
  });

  it('serves the dashboard at the root', async () => {
    const response = await server.app.inject({ method: 'GET', url: '/' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/html');
    expect(response.body).toContain('Order pipeline');
    // No build step, no framework: the page must not reference a bundle.
    expect(response.body).not.toMatch(/<script[^>]+src=/);
  });

  it('returns every owned product and the global merge', async () => {
    fold(aggregator, 'Item1', 10, 0);
    fold(aggregator, 'Item1', 20, 0);
    fold(aggregator, 'Item2', 30, 2);

    const response = await server.app.inject({ method: 'GET', url: '/aggregates' });
    const body = response.json<{
      global: { count: number; mean: number };
      products: { product: string; mean: number }[];
      ownedPartitions: number[];
    }>();

    expect(response.statusCode).toBe(200);
    expect(body.global).toMatchObject({ count: 3, mean: 20 });
    expect(body.products.map((p) => p.product)).toEqual(['Item1', 'Item2']);
    expect(body.ownedPartitions).toEqual([0, 2]);
  });

  it('returns one product by name', async () => {
    fold(aggregator, 'Item1', 10, 0);

    const response = await server.app.inject({ method: 'GET', url: '/aggregates/Item1' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({ product: 'Item1', count: 1, mean: 10, partition: 0 });
  });

  it('404s for a product this instance does not own, and says which partitions it does', async () => {
    fold(aggregator, 'Item1', 10, 0);

    const response = await server.app.inject({ method: 'GET', url: '/aggregates/Item7' });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toMatchObject({ error: 'not found', ownedPartitions: [0] });
  });

  it('reports healthy with 200', async () => {
    const response = await server.app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({ status: 'ok' });
  });

  it('reports degraded with 503 and the reason', async () => {
    health = { ok: false, reason: 'state restore failed for partitions 1' };

    const response = await server.app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({ status: 'degraded', reason: /restore failed/ });
  });

  it('exposes prometheus metrics', async () => {
    fold(aggregator, 'Item1', 12.5, 0);
    // The server wires aggregator changes to metrics only through the app's
    // onChange in index.ts; here, drive the metric directly via the registry.
    const response = await server.app.inject({ method: 'GET', url: '/metrics' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/plain');
    expect(response.body).toContain('# HELP orders_consumed_total');
    expect(response.body).toContain('consumer_process_cpu');
  });

  it('exposes operational stats', async () => {
    const response = await server.app.inject({ method: 'GET', url: '/stats' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      throughputPerSecond: 4.5,
      lag: { total: 3 },
      dlqDepth: 0,
      websocketClients: 0,
    });
  });
});

describe('api server websocket', () => {
  let aggregator: Aggregator;
  let server: ApiServer;
  let address: string;

  beforeEach(async () => {
    aggregator = createAggregator();
    server = createApiServer({
      aggregator,
      stats: fakeSampler(),
      metrics: createMetrics(),
      health: () => ({ ok: true }),
      logger: silentLogger,
      host: '127.0.0.1',
      port: 0,
      pushIntervalMs: 50,
    });
    address = await server.start();
  });

  afterEach(async () => {
    await server.stop();
  });

  async function connect(): Promise<{ frames: ServerFrame[]; close: () => void }> {
    const socket = new WebSocket(`${address.replace('http', 'ws')}/ws`);
    const frames: ServerFrame[] = [];
    socket.on('message', (data: Buffer | ArrayBuffer | Buffer[]) => {
      const text = Buffer.isBuffer(data)
        ? data.toString('utf8')
        : Array.isArray(data)
          ? Buffer.concat(data).toString('utf8')
          : Buffer.from(data).toString('utf8');
      frames.push(JSON.parse(text) as ServerFrame);
    });
    await new Promise<void>((resolve, reject) => {
      socket.once('open', resolve);
      socket.once('error', reject);
    });
    return {
      frames,
      close: () => {
        socket.close();
      },
    };
  }

  it('sends a full snapshot on connect', async () => {
    fold(aggregator, 'Item1', 10, 0);
    const client = await connect();

    await vi.waitFor(() => {
      expect(client.frames.length).toBeGreaterThanOrEqual(1);
    });

    expect(client.frames[0]).toMatchObject({
      type: 'snapshot',
      aggregates: { products: [{ product: 'Item1' }] },
      stats: { groupId: 'order-consumers' },
    });
    expect(server.clientCount).toBe(1);
    client.close();
  });

  it('coalesces a burst of changes into a single aggregates frame', async () => {
    const client = await connect();
    await vi.waitFor(() => {
      expect(client.frames.length).toBe(1);
    });

    for (let i = 1; i <= 25; i += 1) {
      fold(aggregator, 'Item1', i, 0);
    }

    // Wait past two push intervals: the burst must produce one frame, not 25.
    await new Promise((resolve) => setTimeout(resolve, 150));

    const pushes = client.frames.filter((f) => f.type === 'aggregates');
    expect(pushes).toHaveLength(1);
    // And that one frame carries the final state, not an intermediate one.
    expect(pushes[0]).toMatchObject({
      aggregates: { products: [{ product: 'Item1', count: 25 }] },
    });
    client.close();
  });

  it('forgets a client that disconnects', async () => {
    const client = await connect();
    await vi.waitFor(() => {
      expect(server.clientCount).toBe(1);
    });

    client.close();

    await vi.waitFor(() => {
      expect(server.clientCount).toBe(0);
    });
  });
});
