import { isOrder } from '@order-pipeline/shared';
import { describe, expect, it } from 'vitest';

import { createOrderGenerator } from './orders.js';

/** A deterministic stand-in for Math.random that cycles a fixed sequence. */
function sequenceOf(values: readonly number[]): () => number {
  let index = 0;
  return () => {
    const value = values[index % values.length] ?? 0;
    index += 1;
    return value;
  };
}

describe('createOrderGenerator', () => {
  it('produces orders that pass the shared validator', () => {
    const generator = createOrderGenerator({
      products: ['Item1', 'Item2'],
      minPrice: 1,
      maxPrice: 500,
    });

    for (let i = 0; i < 200; i += 1) {
      expect(isOrder(generator.next())).toBe(true);
    }
  });

  it('numbers order ids sequentially from the assignment example', () => {
    const generator = createOrderGenerator({
      products: ['Item1'],
      minPrice: 1,
      maxPrice: 2,
    });

    expect([generator.next().orderId, generator.next().orderId, generator.next().orderId]).toEqual([
      '1001',
      '1002',
      '1003',
    ]);
  });

  it('never repeats an order id', () => {
    const generator = createOrderGenerator({ products: ['Item1'], minPrice: 0, maxPrice: 1 });
    const ids = new Set<string>();

    for (let i = 0; i < 1000; i += 1) {
      ids.add(generator.next().orderId);
    }

    expect(ids.size).toBe(1000);
  });

  it('keeps prices inside the configured range', () => {
    const generator = createOrderGenerator({
      products: ['Item1'],
      minPrice: 10,
      maxPrice: 20,
    });

    for (let i = 0; i < 500; i += 1) {
      const { price } = generator.next();
      expect(price).toBeGreaterThanOrEqual(10);
      expect(price).toBeLessThanOrEqual(20);
    }
  });

  it('rounds prices to two decimals', () => {
    const generator = createOrderGenerator({
      products: ['Item1'],
      minPrice: 0,
      maxPrice: 100,
      random: sequenceOf([0, 1 / 3]),
    });

    const { price } = generator.next();
    expect(price).toBe(33.33);
  });

  it('draws from every configured product', () => {
    const products = ['Item1', 'Item2', 'Item3'];
    const generator = createOrderGenerator({ products, minPrice: 1, maxPrice: 2 });
    const seen = new Set<string>();

    for (let i = 0; i < 300; i += 1) {
      seen.add(generator.next().product);
    }

    expect([...seen].sort()).toEqual(products);
  });

  it('stays in bounds when the random source returns exactly 1', () => {
    // Math.random() never returns 1, but an injected or seeded source might,
    // and the resulting index would be one past the end of the array.
    const generator = createOrderGenerator({
      products: ['Item1', 'Item2'],
      minPrice: 5,
      maxPrice: 5,
      random: () => 1,
    });

    const order = generator.next();
    expect(order.product).toBe('Item2');
    expect(order.price).toBe(5);
  });

  it('counts what it has produced', () => {
    const generator = createOrderGenerator({ products: ['Item1'], minPrice: 1, maxPrice: 1 });

    expect(generator.count).toBe(0);
    generator.next();
    generator.next();
    expect(generator.count).toBe(2);
  });

  it.each([
    { label: 'no products', options: { products: [], minPrice: 1, maxPrice: 2 } },
    {
      label: 'an inverted price range',
      options: { products: ['Item1'], minPrice: 9, maxPrice: 2 },
    },
  ])('rejects $label at construction', ({ options }) => {
    // Fail at startup, not on the thousandth message.
    expect(() => createOrderGenerator(options)).toThrow();
  });
});
