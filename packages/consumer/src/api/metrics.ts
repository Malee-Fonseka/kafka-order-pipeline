import { Counter, Gauge, Registry, collectDefaultMetrics } from 'prom-client';

import type { ProductEntry } from '../aggregation/aggregator.js';
import type { RuntimeStats } from './stats.js';

/**
 * Prometheus metrics for `GET /metrics`.
 *
 * Phase 5 exposes what already exists as counters and gauges; Phase 8 adds
 * histograms (processing latency, commit latency) and the retry-tier
 * counters. A private registry rather than the global default keeps tests
 * isolated — two servers in one process must not share a registry.
 */

export interface Metrics {
  readonly registry: Registry;
  recordOutcome: (outcome: 'processed' | 'skipped' | 'retried' | 'exhausted' | 'forwarded') => void;
  recordCommit: () => void;
  recordFailure: () => void;
  recordAggregate: (entry: ProductEntry) => void;
  /** Called on every stats sample; pushes lag and depth gauges. */
  recordStats: (stats: RuntimeStats) => void;
  /** Called after a partition revoke so stale per-product series disappear. */
  forgetProduct: (product: string) => void;
}

export function createMetrics(): Metrics {
  const registry = new Registry();
  collectDefaultMetrics({ register: registry, prefix: 'consumer_' });

  const consumed = new Counter({
    name: 'orders_consumed_total',
    help: 'Records consumed, by terminal outcome (processed, skipped, retried, exhausted, forwarded).',
    labelNames: ['outcome'] as const,
    registers: [registry],
  });

  const committed = new Counter({
    name: 'orders_committed_total',
    help: 'Offsets committed after a terminal outcome.',
    registers: [registry],
  });

  const failed = new Counter({
    name: 'orders_failed_total',
    help: 'Records whose handler threw an unclassified error (not committed; redelivered).',
    registers: [registry],
  });

  const priceMean = new Gauge({
    name: 'order_price_mean',
    help: 'Running mean price per product (Welford, double precision).',
    labelNames: ['product'] as const,
    registers: [registry],
  });

  const priceCount = new Gauge({
    name: 'order_price_count',
    help: 'Orders folded into the running aggregate per product.',
    labelNames: ['product'] as const,
    registers: [registry],
  });

  const lag = new Gauge({
    name: 'consumer_lag_records',
    help: 'Records between the committed offset and the end of the partition.',
    labelNames: ['partition'] as const,
    registers: [registry],
  });

  const topicDepth = new Gauge({
    name: 'topic_depth_records',
    help: 'Records retained on the retry and DLQ topics.',
    labelNames: ['topic'] as const,
    registers: [registry],
  });

  const pausedPartitions = new Gauge({
    name: 'retry_partitions_paused',
    help: 'Retry-tier partitions currently paused by the delay gate.',
    registers: [registry],
  });

  const throughput = new Gauge({
    name: 'consumer_throughput_per_second',
    help: 'Records processed per second over a sliding window.',
    registers: [registry],
  });

  return {
    registry,
    recordOutcome(outcome) {
      consumed.inc({ outcome });
    },
    recordCommit() {
      committed.inc();
    },
    recordFailure() {
      failed.inc();
    },
    recordAggregate(entry) {
      priceMean.set({ product: entry.product }, entry.state.mean);
      priceCount.set({ product: entry.product }, entry.state.count);
    },
    recordStats(stats) {
      throughput.set(stats.throughputPerSecond);
      for (const p of stats.lag.partitions) {
        if (p.lag !== null) {
          lag.set({ partition: String(p.partition) }, p.lag);
        }
      }
      for (const tier of stats.retryTiers) {
        topicDepth.set({ topic: tier.topic }, tier.depth);
      }
      topicDepth.set({ topic: 'dlq' }, stats.dlqDepth);
      pausedPartitions.set(stats.pausedPartitions.length);
    },
    forgetProduct(product) {
      priceMean.remove({ product });
      priceCount.remove({ product });
    },
  };
}
