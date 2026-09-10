/** A single delayed-retry stage. */
export interface RetryTier {
  readonly topic: string;
  readonly delayMs: number;
  /** Human-readable suffix used in topic names and metrics labels. */
  readonly label: string;
}

export interface TopicRegistry {
  readonly orders: string;
  readonly dlq: string;
  readonly aggregateState: string;
  readonly retryTiers: readonly RetryTier[];
  /** Every topic the system reads from or writes to. */
  readonly all: readonly string[];
}

const RETRY_TIER_DEFINITIONS = [
  { label: '5s', delayMs: 5_000 },
  { label: '30s', delayMs: 30_000 },
  { label: '5m', delayMs: 300_000 },
] as const;

/**
 * Derives every topic name from a single prefix so that the compose bootstrap,
 * the services and the tests can never drift apart.
 */
export function buildTopicRegistry(prefix: string): TopicRegistry {
  const orders = prefix;
  const dlq = `${prefix}.dlq`;
  const aggregateState = `${prefix}.aggregate.state`;

  const retryTiers: readonly RetryTier[] = RETRY_TIER_DEFINITIONS.map((tier) => ({
    label: tier.label,
    delayMs: tier.delayMs,
    topic: `${prefix}.retry.${tier.label}`,
  }));

  return {
    orders,
    dlq,
    aggregateState,
    retryTiers,
    all: [orders, ...retryTiers.map((tier) => tier.topic), dlq, aggregateState],
  };
}

/** Subject name under which the Order schema is registered (TopicNameStrategy). */
export function valueSubjectFor(topic: string): string {
  return `${topic}-value`;
}
