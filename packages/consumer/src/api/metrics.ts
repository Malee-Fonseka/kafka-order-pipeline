import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

import type { ProductEntry } from '../aggregation/aggregator.js';
import type { RuntimeStats } from './stats.js';

/**
 * Prometheus metrics for `GET /metrics`.
 *
 * Counters for every terminal outcome and every tier, gauges for the
 * aggregate and the group's position, histograms for the latencies an
 * operator would alert on. A private registry rather than the global default
 * keeps tests isolated — two servers in one process must not share a
 * registry.
 *
 * Bucket choices: processing and commit are single-digit milliseconds to low
 * seconds (stage 1 retries add up to 2 s); end-to-end latency includes retry
 * tiers, so it stretches to minutes.
 */

const FAST_BUCKETS = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5];
const END_TO_END_BUCKETS = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600];

export interface Metrics {
  readonly registry: Registry;
  recordOutcome: (outcome: 'processed' | 'retried' | 'dead-lettered' | 'forwarded') => void;
  /** Detail counters: which tier a retry went to, which kind of dead letter. */
  recordRetried: (tier: string) => void;
  recordForwarded: (tier: string) => void;
  recordDeadLettered: (errorType: string) => void;
  recordPause: () => void;
  /** Latencies, in seconds. */
  observeProcessing: (outcome: string, seconds: number) => void;
  observeCommit: (seconds: number) => void;
  observeChangelogWrite: (seconds: number) => void;
  /** Broker timestamp of the record to the moment it was processed. */
  observeEndToEnd: (seconds: number) => void;
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
    help: 'Records consumed, by terminal outcome (processed, retried, dead-lettered, forwarded).',
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

  const retried = new Counter({
    name: 'orders_retried_total',
    help: 'Records republished to a retry tier, by tier.',
    labelNames: ['tier'] as const,
    registers: [registry],
  });

  const forwarded = new Counter({
    name: 'retry_forwarded_total',
    help: 'Due retry-tier records forwarded back to the main topic, by tier.',
    labelNames: ['tier'] as const,
    registers: [registry],
  });

  const deadLettered = new Counter({
    name: 'orders_dead_lettered_total',
    help: 'Records written to the DLQ, by error type (permanent, transient-exhausted).',
    labelNames: ['error_type'] as const,
    registers: [registry],
  });

  const pauses = new Counter({
    name: 'retry_partition_pauses_total',
    help: 'Times the delay gate paused a retry-tier partition rather than sleeping.',
    registers: [registry],
  });

  const processing = new Histogram({
    name: 'order_processing_duration_seconds',
    help: 'Handler time per record, from delivery to terminal outcome, by outcome.',
    labelNames: ['outcome'] as const,
    buckets: FAST_BUCKETS,
    registers: [registry],
  });

  const commit = new Histogram({
    name: 'offset_commit_duration_seconds',
    help: 'Round trip for one manual offset commit.',
    buckets: FAST_BUCKETS,
    registers: [registry],
  });

  const changelog = new Histogram({
    name: 'changelog_write_duration_seconds',
    help: 'Round trip for one aggregation changelog write (acks=all).',
    buckets: FAST_BUCKETS,
    registers: [registry],
  });

  const endToEnd = new Histogram({
    name: 'order_end_to_end_latency_seconds',
    help: 'Broker timestamp of the record to the moment it was processed; includes retry tiers.',
    buckets: END_TO_END_BUCKETS,
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
    recordRetried(tier) {
      retried.inc({ tier });
    },
    recordForwarded(tier) {
      forwarded.inc({ tier });
    },
    recordDeadLettered(errorType) {
      deadLettered.inc({ error_type: errorType });
    },
    recordPause() {
      pauses.inc();
    },
    observeProcessing(outcome, seconds) {
      processing.observe({ outcome }, seconds);
    },
    observeCommit(seconds) {
      commit.observe(seconds);
    },
    observeChangelogWrite(seconds) {
      changelog.observe(seconds);
    },
    observeEndToEnd(seconds) {
      endToEnd.observe(seconds);
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
