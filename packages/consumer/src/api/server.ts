import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import websocket from '@fastify/websocket';
import Fastify, { type FastifyInstance, LogController } from 'fastify';

import type { AggregateSnapshot, Aggregator } from '../aggregation/aggregator.js';
import type { Metrics } from './metrics.js';
import type { RuntimeStats, StatsSampler } from './stats.js';

import type { IncomingMessage, Server, ServerResponse } from 'node:http';

import type { Logger } from '@order-pipeline/shared';
import type { WebSocket } from 'ws';

/**
 * REST + WebSocket surface over the aggregation state (D9).
 *
 * | Route | Purpose |
 * |---|---|
 * | `GET /` | The dashboard — one static page, no build step, no framework |
 * | `GET /aggregates` | Every product this instance owns, plus the global merge |
 * | `GET /aggregates/:product` | One product, 404 if not owned here |
 * | `GET /stats` | Throughput, lag, retry-tier and DLQ depth |
 * | `GET /health` | 200 when healthy, 503 when a state restore has failed |
 * | `GET /metrics` | Prometheus exposition |
 * | `GET /ws` | Push channel: `snapshot` on connect, then `aggregates` and `stats` frames |
 *
 * The `aggregates` frame is coalesced: many records in a burst produce one
 * frame per `pushIntervalMs`, carrying the full snapshot. The client never
 * merges — it replaces — so a dropped frame costs nothing and there is no
 * per-product delta protocol to keep in sync with the server.
 */

export interface HealthReport {
  readonly ok: boolean;
  readonly reason?: string;
}

export interface ApiServerOptions {
  readonly aggregator: Aggregator;
  readonly stats: StatsSampler;
  readonly metrics: Metrics;
  readonly health: () => HealthReport;
  readonly logger: Logger;
  readonly host: string;
  readonly port: number;
  /** Minimum gap between `aggregates` pushes. */
  readonly pushIntervalMs?: number;
}

export type ServerFrame =
  | {
      readonly type: 'snapshot';
      readonly aggregates: AggregateSnapshot;
      readonly stats: RuntimeStats;
    }
  | { readonly type: 'aggregates'; readonly aggregates: AggregateSnapshot }
  | { readonly type: 'stats'; readonly stats: RuntimeStats };

/**
 * Spelled out rather than the bare `FastifyInstance` alias: an instance built
 * with a pino `loggerInstance` is parameterised by that logger's type, and the
 * default alias does not match it under `exactOptionalPropertyTypes`.
 */
export type App = FastifyInstance<Server, IncomingMessage, ServerResponse, Logger>;

function buildApp(logger: Logger): App {
  return Fastify({
    loggerInstance: logger.child({ component: 'api' }),
    // Per-request access lines would drown the order log during a demo. The
    // API's own warnings and errors still go through pino.
    logController: new LogController({ disableRequestLogging: true }),
  });
}

export interface ApiServer {
  readonly app: App;
  start: () => Promise<string>;
  stop: () => Promise<void>;
  /** Connected WebSocket clients; exposed for tests and the stats tile. */
  readonly clientCount: number;
}

/**
 * `src/api` and `dist/api` sit at the same depth, so one relative walk finds
 * `public/` from either. Read once: the page is static.
 */
const dashboardPath = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
  'public',
  'dashboard.html',
);

export function createApiServer({
  aggregator,
  stats,
  metrics,
  health,
  logger,
  host,
  port,
  pushIntervalMs = 250,
}: ApiServerOptions): ApiServer {
  const app = buildApp(logger);

  const clients = new Set<WebSocket>();
  const broadcast = (frame: ServerFrame): void => {
    if (clients.size === 0) {
      return;
    }
    const payload = JSON.stringify(frame);
    for (const client of clients) {
      if (client.readyState === client.OPEN) {
        client.send(payload);
      }
    }
  };

  // Coalesce aggregate pushes: mark dirty on every change, flush on a timer.
  let dirty = false;
  let flushTimer: NodeJS.Timeout | undefined;
  const scheduleFlush = (): void => {
    dirty = true;
    if (flushTimer !== undefined) {
      return;
    }
    flushTimer = setTimeout(() => {
      flushTimer = undefined;
      if (dirty) {
        dirty = false;
        broadcast({ type: 'aggregates', aggregates: aggregator.snapshot() });
      }
    }, pushIntervalMs);
    flushTimer.unref();
  };

  const unsubscribeChange = aggregator.onChange(() => {
    scheduleFlush();
  });
  const unsubscribeStats = stats.onSample((sample) => {
    metrics.recordStats(sample);
    broadcast({ type: 'stats', stats: sample });
  });

  let dashboardHtml: string | undefined;
  const dashboard = (): string => {
    dashboardHtml ??= readFileSync(dashboardPath, 'utf8');
    return dashboardHtml;
  };

  app.get('/', async (_request, reply) => {
    await reply.type('text/html; charset=utf-8').send(dashboard());
  });

  app.get('/aggregates', () => aggregator.snapshot());

  app.get<{ Params: { product: string } }>('/aggregates/:product', async (request, reply) => {
    const product = aggregator.product(request.params.product);
    if (product === undefined) {
      return reply.code(404).send({
        error: 'not found',
        message: `no aggregate for product "${request.params.product}" on this instance`,
        ownedPartitions: aggregator.snapshot().ownedPartitions,
      });
    }
    return product;
  });

  app.get('/stats', () => ({ ...stats.current(), websocketClients: clients.size }));

  app.get('/health', async (_request, reply) => {
    const report = health();
    return reply.code(report.ok ? 200 : 503).send({
      status: report.ok ? 'ok' : 'degraded',
      ...(report.reason === undefined ? {} : { reason: report.reason }),
      ownedPartitions: aggregator.snapshot().ownedPartitions,
      products: aggregator.productCount,
      uptimeSeconds: stats.current().uptimeSeconds,
    });
  });

  app.get('/metrics', async (_request, reply) => {
    const body = await metrics.registry.metrics();
    return reply.type(metrics.registry.contentType).send(body);
  });

  void app.register(websocket);
  void app.register((instance, _opts, done) => {
    instance.get('/ws', { websocket: true }, (socket) => {
      clients.add(socket);
      socket.send(
        JSON.stringify({
          type: 'snapshot',
          aggregates: aggregator.snapshot(),
          stats: stats.current(),
        } satisfies ServerFrame),
      );
      socket.on('close', () => {
        clients.delete(socket);
      });
      socket.on('error', (error) => {
        logger.warn({ err: error }, 'websocket client error');
        clients.delete(socket);
      });
    });
    done();
  });

  return {
    app,
    async start() {
      const address = await app.listen({ host, port });
      logger.info({ address, dashboard: `${address}/` }, 'api listening');
      return address;
    },
    async stop() {
      unsubscribeChange();
      unsubscribeStats();
      if (flushTimer !== undefined) {
        clearTimeout(flushTimer);
      }
      for (const client of clients) {
        client.close(1001, 'server shutting down');
      }
      clients.clear();
      await app.close();
      logger.info('api closed');
    },
    get clientCount() {
      return clients.size;
    },
  };
}
