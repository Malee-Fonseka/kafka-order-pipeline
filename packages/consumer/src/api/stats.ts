import type { KafkaClient, Logger, TopicRegistry } from '@order-pipeline/shared';

/**
 * Operational figures for the dashboard (D9): throughput, consumer lag, and
 * the depth of the retry and DLQ topics.
 *
 * None of this is on the message path. Lag and topic depths come from the
 * admin API on a timer, so a slow broker query delays a dashboard tile rather
 * than a commit.
 */

export interface PartitionLag {
  readonly partition: number;
  readonly committed: number | null;
  readonly end: number;
  readonly lag: number | null;
  readonly owned: boolean;
}

export interface TopicDepth {
  readonly label: string;
  readonly topic: string;
  /** Records currently retained on the topic (end − start, summed over partitions). */
  readonly depth: number;
}

export interface RuntimeStats {
  readonly instanceId: string;
  readonly groupId: string;
  readonly uptimeSeconds: number;
  readonly throughputPerSecond: number;
  readonly counters: {
    readonly processed: number;
    readonly deadLettered: number;
    readonly retried: number;
    readonly forwarded: number;
    readonly committed: number;
    readonly failed: number;
  };
  /** Retry-tier partitions currently held back by the delay gate. */
  readonly pausedPartitions: readonly string[];
  readonly ownedPartitions: readonly number[];
  readonly lag: {
    readonly total: number | null;
    readonly partitions: readonly PartitionLag[];
  };
  readonly retryTiers: readonly TopicDepth[];
  readonly dlqDepth: number;
  /** Set when the last admin query failed; the figures above may be stale. */
  readonly sampledAt: string;
  readonly sampleError: string | null;
}

/** Sliding-window event rate. */
export interface Throughput {
  mark: (at?: number) => void;
  perSecond: (now?: number) => number;
}

export function createThroughput(windowMs = 10_000): Throughput {
  const stamps: number[] = [];

  const prune = (now: number): void => {
    const cutoff = now - windowMs;
    let drop = 0;
    while (drop < stamps.length && (stamps[drop] ?? now) < cutoff) {
      drop += 1;
    }
    if (drop > 0) {
      stamps.splice(0, drop);
    }
  };

  return {
    mark(at = Date.now()) {
      stamps.push(at);
      prune(at);
    },
    perSecond(now = Date.now()) {
      prune(now);
      return stamps.length / (windowMs / 1000);
    },
  };
}

export interface StatsSamplerOptions {
  readonly kafka: KafkaClient;
  readonly topics: TopicRegistry;
  readonly groupId: string;
  readonly instanceId: string;
  readonly logger: Logger;
  readonly throughput: Throughput;
  readonly counters: () => RuntimeStats['counters'];
  readonly ownedPartitions: () => readonly number[];
  readonly pausedPartitions?: () => readonly string[];
  readonly intervalMs: number;
}

export interface StatsSampler {
  /** The most recent sample; available immediately, refreshed on the interval. */
  current: () => RuntimeStats;
  /** Registers for each refreshed sample. */
  onSample: (listener: (stats: RuntimeStats) => void) => () => void;
  start: () => Promise<void>;
  stop: () => Promise<void>;
}

export function createStatsSampler({
  kafka,
  topics,
  groupId,
  instanceId,
  logger,
  throughput,
  counters,
  ownedPartitions,
  pausedPartitions = () => [],
  intervalMs,
}: StatsSamplerOptions): StatsSampler {
  const startedAt = Date.now();
  const listeners = new Set<(stats: RuntimeStats) => void>();
  const admin = kafka.admin();

  let timer: NodeJS.Timeout | undefined;
  let sampling = false;
  let lastLag: RuntimeStats['lag'] = { total: null, partitions: [] };
  let lastTiers: TopicDepth[] = topics.retryTiers.map((tier) => ({
    label: tier.label,
    topic: tier.topic,
    depth: 0,
  }));
  let lastDlq = 0;
  let lastError: string | null = null;
  let lastSampledAt = new Date(startedAt).toISOString();

  const build = (): RuntimeStats => ({
    instanceId,
    groupId,
    uptimeSeconds: Math.round((Date.now() - startedAt) / 1000),
    throughputPerSecond: Number(throughput.perSecond().toFixed(2)),
    counters: counters(),
    ownedPartitions: ownedPartitions(),
    pausedPartitions: pausedPartitions(),
    lag: lastLag,
    retryTiers: lastTiers,
    dlqDepth: lastDlq,
    sampledAt: lastSampledAt,
    sampleError: lastError,
  });

  const depthOf = async (topic: string): Promise<number> => {
    const watermarks = await admin.fetchTopicOffsets(topic);
    return watermarks.reduce((sum, w) => sum + (Number(w.high) - Number(w.low)), 0);
  };

  const sample = async (): Promise<void> => {
    if (sampling) {
      return;
    }
    sampling = true;
    try {
      const owned = new Set(ownedPartitions());

      const [ends, committedByTopic, dlqDepth, ...tierDepths] = await Promise.all([
        admin.fetchTopicOffsets(topics.orders),
        admin.fetchOffsets({ groupId, topics: [topics.orders] }),
        depthOf(topics.dlq),
        ...topics.retryTiers.map((tier) => depthOf(tier.topic)),
      ]);

      const committed = new Map<number, number>();
      for (const topic of committedByTopic) {
        for (const p of topic.partitions) {
          // The broker reports -1 for "no committed offset yet".
          const value = Number(p.offset);
          if (value >= 0) {
            committed.set(p.partition, value);
          }
        }
      }

      const partitions: PartitionLag[] = ends
        .map((w) => {
          const end = Number(w.high);
          const c = committed.get(w.partition) ?? null;
          return {
            partition: w.partition,
            committed: c,
            end,
            lag: c === null ? null : Math.max(0, end - c),
            owned: owned.has(w.partition),
          };
        })
        .sort((a, b) => a.partition - b.partition);

      const known = partitions.filter((p) => p.lag !== null);
      lastLag = {
        total: known.length === 0 ? null : known.reduce((sum, p) => sum + (p.lag ?? 0), 0),
        partitions,
      };
      lastTiers = topics.retryTiers.map((tier, i) => ({
        label: tier.label,
        topic: tier.topic,
        depth: tierDepths[i] ?? 0,
      }));
      lastDlq = dlqDepth;
      lastError = null;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      logger.warn({ err: error }, 'stats sample failed; dashboard figures may be stale');
    } finally {
      lastSampledAt = new Date().toISOString();
      sampling = false;
    }

    const stats = build();
    for (const listener of listeners) {
      listener(stats);
    }
  };

  return {
    current: build,
    onSample(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    async start() {
      await admin.connect();
      await sample();
      timer = setInterval(() => {
        void sample();
      }, intervalMs);
      timer.unref();
    },
    async stop() {
      if (timer !== undefined) {
        clearInterval(timer);
        timer = undefined;
      }
      await admin.disconnect();
    },
  };
}
