import { type Order, TRANSIENT_FAIL_PRODUCT, TransientError } from '@order-pipeline/shared';
import { describe, expect, it, vi } from 'vitest';

import { createAggregator } from './aggregation/aggregator.js';
import type { StateStore } from './aggregation/state-store.js';
import { type HandlerContext, createOrderHandler } from './handler.js';

const ORDER: Order = { orderId: '1', product: 'Item1', price: 10 };
const MARKER: Order = { orderId: '2', product: TRANSIENT_FAIL_PRODUCT, price: 10 };

function context(delivery: number, attempt = 1): HandlerContext {
  return { delivery, attempt, source: { partition: 0, offset: '1', timestamp: 1 } };
}

function fakeStore(): StateStore & { write: ReturnType<typeof vi.fn> } {
  return {
    write: vi.fn(async () => Promise.resolve()),
    restore: async () => Promise.resolve([]),
    close: async () => Promise.resolve(),
  };
}

describe('order handler', () => {
  it('folds an order into the aggregate after the changelog write succeeds', async () => {
    const aggregator = createAggregator();
    const store = fakeStore();
    const handler = createOrderHandler({
      aggregator,
      stateStore: store,
      chaos: { transientSucceedAfterDelivery: 2 },
    });

    await handler(ORDER, context(1));

    expect(store.write).toHaveBeenCalledTimes(1);
    expect(aggregator.product('Item1')?.count).toBe(1);
  });

  it('leaves memory untouched when the changelog write fails', async () => {
    // The write-ahead property: a failed write must not advance memory, or
    // the in-place retry that follows counts the order twice.
    const aggregator = createAggregator();
    const store = fakeStore();
    store.write.mockRejectedValueOnce(new TransientError('broker away'));
    const handler = createOrderHandler({
      aggregator,
      stateStore: store,
      chaos: { transientSucceedAfterDelivery: 2 },
    });

    await expect(handler(ORDER, context(1))).rejects.toBeInstanceOf(TransientError);
    expect(aggregator.product('Item1')).toBeUndefined();

    await handler(ORDER, context(1, 2));
    expect(aggregator.product('Item1')?.count).toBe(1);
  });

  it('fails the chaos marker transiently on every in-place attempt of an early delivery', async () => {
    const handler = createOrderHandler({
      aggregator: createAggregator(),
      stateStore: fakeStore(),
      chaos: { transientSucceedAfterDelivery: 2 },
    });

    for (const attempt of [1, 2, 3]) {
      await expect(handler(MARKER, context(1, attempt))).rejects.toBeInstanceOf(TransientError);
    }
  });

  it('lets the chaos marker succeed once the configured delivery is reached', async () => {
    const aggregator = createAggregator();
    const handler = createOrderHandler({
      aggregator,
      stateStore: fakeStore(),
      chaos: { transientSucceedAfterDelivery: 2 },
    });

    await handler(MARKER, context(2));

    expect(aggregator.product(TRANSIENT_FAIL_PRODUCT)?.count).toBe(1);
  });

  it('never fails the marker when configured to succeed on delivery 1', async () => {
    const handler = createOrderHandler({
      aggregator: createAggregator(),
      stateStore: fakeStore(),
      chaos: { transientSucceedAfterDelivery: 1 },
    });

    await expect(handler(MARKER, context(1))).resolves.toBeUndefined();
  });

  it('does not apply the chaos rule to real products', async () => {
    const handler = createOrderHandler({
      aggregator: createAggregator(),
      stateStore: fakeStore(),
      chaos: { transientSucceedAfterDelivery: 99 },
    });

    await expect(handler(ORDER, context(1))).resolves.toBeUndefined();
  });

  it('notifies after a successful fold', async () => {
    const onProcessed = vi.fn();
    const handler = createOrderHandler({
      aggregator: createAggregator(),
      stateStore: fakeStore(),
      chaos: { transientSucceedAfterDelivery: 2 },
      onProcessed,
    });

    await handler(ORDER, context(1));

    expect(onProcessed).toHaveBeenCalledWith(ORDER);
  });
});
