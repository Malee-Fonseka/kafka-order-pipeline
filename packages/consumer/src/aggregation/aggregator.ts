import type { Order } from '@order-pipeline/shared';

import {
  type AggregateView,
  EMPTY_STATE,
  type WelfordState,
  merge,
  toView,
  update,
} from './welford.js';

/**
 * In-memory aggregation state for the products this instance owns.
 *
 * Ownership follows partitions. Because records are keyed by product (D1),
 * every record for a product arrives on one partition, and Kafka gives each
 * partition to exactly one member of the group. So "the products on the
 * partitions assigned to me" is precisely "the products whose running average
 * I am responsible for", with no coordination between instances.
 *
 * Each entry remembers the partition it arrived on. When a partition is
 * revoked its products are dropped here — they live on, current, in the
 * changelog topic, and the instance that receives the partition restores them
 * from there. When a partition is assigned, the caller restores its products
 * into this aggregator before any new record for it is processed.
 *
 * The global aggregate is not maintained separately. It is the Welford
 * *merge* of everything owned, computed on read. That keeps exactly one source
 * of truth per product and makes "global" mean the same thing on restore as it
 * does live.
 */

export interface ProductEntry {
  readonly product: string;
  /** Partition of `orders` this product's records arrive on. */
  readonly partition: number;
  /** Offset of the most recent record folded in; informational. */
  readonly lastOffset: string;
  readonly state: WelfordState;
}

export interface RecordSource {
  readonly partition: number;
  readonly offset: string;
  /** Broker timestamp of the record, epoch milliseconds. */
  readonly timestamp: number;
}

export interface ProductAggregate extends AggregateView {
  readonly product: string;
  readonly partition: number;
}

export interface AggregateSnapshot {
  readonly global: AggregateView;
  readonly products: readonly ProductAggregate[];
  readonly ownedPartitions: readonly number[];
}

export type ChangeListener = (entry: ProductEntry) => void;

export interface Aggregator {
  /**
   * Computes the product's entry with this order folded in, without changing
   * anything. Pair with {@link Aggregator.apply} once the entry is durable.
   *
   * Split deliberately: the caller writes the entry to the changelog *between*
   * the two calls. If that write fails, memory has not moved, so the record's
   * redelivery folds the order in exactly once rather than twice.
   */
  next: (order: Order, source: RecordSource) => ProductEntry;
  /** Makes a prepared entry current and notifies listeners. */
  apply: (entry: ProductEntry) => void;
  /** Replaces the entries for the given partitions with restored ones. */
  restore: (partitions: readonly number[], entries: readonly ProductEntry[]) => void;
  /** Forgets every product on the given partitions. */
  drop: (partitions: readonly number[]) => number;
  product: (name: string) => ProductAggregate | undefined;
  snapshot: () => AggregateSnapshot;
  /** Called after every `apply`. Unsubscribe with the returned function. */
  onChange: (listener: ChangeListener) => () => void;
  readonly productCount: number;
}

export function createAggregator(): Aggregator {
  const entries = new Map<string, ProductEntry>();
  const owned = new Set<number>();
  const listeners = new Set<ChangeListener>();

  const toAggregate = (entry: ProductEntry): ProductAggregate => ({
    product: entry.product,
    partition: entry.partition,
    ...toView(entry.state),
  });

  return {
    next(order, source) {
      const previous = entries.get(order.product)?.state ?? EMPTY_STATE;
      return {
        product: order.product,
        partition: source.partition,
        lastOffset: source.offset,
        state: update(previous, order.price, source.timestamp),
      };
    },

    apply(entry) {
      entries.set(entry.product, entry);
      owned.add(entry.partition);
      for (const listener of listeners) {
        listener(entry);
      }
    },

    restore(partitions, restored) {
      const partitionSet = new Set(partitions);

      // Clear first: a product that was on this partition but is absent from
      // the restored set would otherwise survive as stale state.
      for (const [product, entry] of entries) {
        if (partitionSet.has(entry.partition)) {
          entries.delete(product);
        }
      }
      for (const entry of restored) {
        if (partitionSet.has(entry.partition)) {
          entries.set(entry.product, entry);
        }
      }
      for (const partition of partitions) {
        owned.add(partition);
      }
    },

    drop(partitions) {
      const partitionSet = new Set(partitions);
      let dropped = 0;

      for (const [product, entry] of entries) {
        if (partitionSet.has(entry.partition)) {
          entries.delete(product);
          dropped += 1;
        }
      }
      for (const partition of partitions) {
        owned.delete(partition);
      }
      return dropped;
    },

    product(name) {
      const entry = entries.get(name);
      return entry === undefined ? undefined : toAggregate(entry);
    },

    snapshot() {
      let global = EMPTY_STATE;
      const products: ProductAggregate[] = [];

      for (const entry of entries.values()) {
        global = merge(global, entry.state);
        products.push(toAggregate(entry));
      }
      products.sort((a, b) => a.product.localeCompare(b.product));

      return {
        global: toView(global),
        products,
        ownedPartitions: [...owned].sort((a, b) => a - b),
      };
    },

    onChange(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },

    get productCount() {
      return entries.size;
    },
  };
}
