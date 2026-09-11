import type { Order } from '@order-pipeline/shared';
import { describe, expect, it, vi } from 'vitest';

import { type ProductEntry, createAggregator } from './aggregator.js';
import { EMPTY_STATE, update } from './welford.js';

const source = (
  partition: number,
  offset: number,
): { partition: number; offset: string; timestamp: number } => ({
  partition,
  offset: String(offset),
  timestamp: 1_700_000_000_000 + offset,
});

const order = (product: string, price: number, id = '1'): Order => ({
  orderId: id,
  product,
  price,
});

/** next + apply in one step, for tests that are not about the split. */
function fold(
  aggregator: ReturnType<typeof createAggregator>,
  o: Order,
  partition: number,
  offset: number,
): ProductEntry {
  const entry = aggregator.next(o, source(partition, offset));
  aggregator.apply(entry);
  return entry;
}

describe('aggregator', () => {
  it('keeps one running aggregate per product', () => {
    const aggregator = createAggregator();

    fold(aggregator, order('Item1', 10), 0, 1);
    fold(aggregator, order('Item1', 20), 0, 2);
    fold(aggregator, order('Item2', 100), 2, 1);

    expect(aggregator.product('Item1')).toMatchObject({ count: 2, mean: 15, min: 10, max: 20 });
    expect(aggregator.product('Item2')).toMatchObject({ count: 1, mean: 100 });
    expect(aggregator.productCount).toBe(2);
  });

  it('derives the global aggregate as the merge of owned products', () => {
    // Hand check: 10, 20 (Item1) and 30, 40, 50 (Item2) → the 10..50 control
    // set from welford.test.ts: mean 30, variance 250.
    const aggregator = createAggregator();
    fold(aggregator, order('Item1', 10), 0, 1);
    fold(aggregator, order('Item1', 20), 0, 2);
    fold(aggregator, order('Item2', 30), 2, 1);
    fold(aggregator, order('Item2', 40), 2, 2);
    fold(aggregator, order('Item2', 50), 2, 3);

    const { global } = aggregator.snapshot();

    expect(global.count).toBe(5);
    expect(global.mean).toBeCloseTo(30, 12);
    expect(global.variance).toBeCloseTo(250, 9);
    expect(global.min).toBe(10);
    expect(global.max).toBe(50);
  });

  it('does not change memory until apply', () => {
    // The write-ahead split: a changelog write that fails between next and
    // apply must leave nothing behind, or the redelivery double-counts.
    const aggregator = createAggregator();
    const listener = vi.fn();
    aggregator.onChange(listener);

    const entry = aggregator.next(order('Item1', 10), source(0, 1));

    expect(aggregator.product('Item1')).toBeUndefined();
    expect(aggregator.snapshot().ownedPartitions).toEqual([]);
    expect(listener).not.toHaveBeenCalled();

    aggregator.apply(entry);

    expect(aggregator.product('Item1')?.count).toBe(1);
    expect(listener).toHaveBeenCalledWith(entry);
  });

  it('computes next from current memory, so an un-applied entry is not built upon', () => {
    const aggregator = createAggregator();
    fold(aggregator, order('Item1', 10), 0, 1);

    const abandoned = aggregator.next(order('Item1', 999), source(0, 2));
    const retried = aggregator.next(order('Item1', 20), source(0, 2));

    expect(abandoned.state.count).toBe(2);
    expect(retried.state.count).toBe(2);
    expect(retried.state.mean).toBe(15);
  });

  it('tags each product with the partition its records arrive on', () => {
    const aggregator = createAggregator();
    fold(aggregator, order('Item1', 1), 2, 1);

    expect(aggregator.product('Item1')?.partition).toBe(2);
    expect(aggregator.snapshot().ownedPartitions).toEqual([2]);
  });

  it('drops every product on revoked partitions and nothing else', () => {
    const aggregator = createAggregator();
    fold(aggregator, order('Item1', 1), 0, 1);
    fold(aggregator, order('Item2', 1), 0, 2);
    fold(aggregator, order('Item3', 1), 2, 1);

    const dropped = aggregator.drop([0]);

    expect(dropped).toBe(2);
    expect(aggregator.product('Item1')).toBeUndefined();
    expect(aggregator.product('Item2')).toBeUndefined();
    expect(aggregator.product('Item3')).toBeDefined();
    expect(aggregator.snapshot().ownedPartitions).toEqual([2]);
  });

  it('restores entries for assigned partitions, replacing stale ones', () => {
    const aggregator = createAggregator();
    fold(aggregator, order('Item1', 1), 0, 1); // stale copy, will be replaced
    fold(aggregator, order('Item9', 1), 2, 1); // other partition, untouched

    const restored: ProductEntry[] = [
      {
        product: 'Item1',
        partition: 0,
        lastOffset: '41',
        state: update(update(EMPTY_STATE, 10, 1), 20, 2),
      },
      {
        product: 'Item5',
        partition: 1,
        lastOffset: '7',
        state: update(EMPTY_STATE, 5, 1),
      },
    ];

    aggregator.restore([0, 1], restored);

    expect(aggregator.product('Item1')).toMatchObject({ count: 2, mean: 15 });
    expect(aggregator.product('Item5')).toMatchObject({ count: 1, mean: 5 });
    expect(aggregator.product('Item9')?.count).toBe(1);
    expect(aggregator.snapshot().ownedPartitions).toEqual([0, 1, 2]);
  });

  it('ignores restored entries for partitions not being assigned', () => {
    const aggregator = createAggregator();

    aggregator.restore(
      [0],
      [{ product: 'Elsewhere', partition: 2, lastOffset: '1', state: update(EMPTY_STATE, 1, 1) }],
    );

    expect(aggregator.product('Elsewhere')).toBeUndefined();
  });

  it('forgets a product that existed on the partition but is absent from the restore', () => {
    // A product that was owned here, then owned elsewhere and never written
    // to the changelog again, must not survive as stale state.
    const aggregator = createAggregator();
    fold(aggregator, order('Ghost', 1), 0, 1);

    aggregator.restore([0], []);

    expect(aggregator.product('Ghost')).toBeUndefined();
  });

  it('lists products sorted by name in the snapshot', () => {
    const aggregator = createAggregator();
    fold(aggregator, order('Item3', 1), 0, 1);
    fold(aggregator, order('Item1', 1), 0, 2);
    fold(aggregator, order('Item2', 1), 0, 3);

    expect(aggregator.snapshot().products.map((p) => p.product)).toEqual([
      'Item1',
      'Item2',
      'Item3',
    ]);
  });

  it('reports an empty global aggregate with null bounds when nothing is owned', () => {
    expect(createAggregator().snapshot().global).toMatchObject({ count: 0, min: null, max: null });
  });

  it('lets a listener unsubscribe', () => {
    const aggregator = createAggregator();
    const listener = vi.fn();
    const unsubscribe = aggregator.onChange(listener);

    fold(aggregator, order('Item1', 1), 0, 1);
    unsubscribe();
    fold(aggregator, order('Item1', 1), 0, 2);

    expect(listener).toHaveBeenCalledTimes(1);
  });
});
