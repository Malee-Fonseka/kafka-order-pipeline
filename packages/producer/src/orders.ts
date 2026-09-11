import type { Order } from '@order-pipeline/shared';

/**
 * Synthetic order generation.
 *
 * The random source is injectable so the generator is deterministic under test
 * and under a scripted demo. "Run it and hope the distribution looks right" is
 * not a verification strategy, and §12 lists a demo that depends on luck as
 * marks-losing.
 */

export interface OrderGeneratorOptions {
  /** The key space: every product hashes to one partition (D1). */
  readonly products: readonly string[];
  readonly minPrice: number;
  readonly maxPrice: number;
  /** Defaults to `Math.random`. Injected in tests for determinism. */
  readonly random?: () => number;
  /** First order id. The assignment's examples start at 1001. */
  readonly startingOrderId?: number;
}

export interface OrderGenerator {
  next: () => Order;
  /** How many orders have been produced so far. */
  readonly count: number;
}

/** Prices are money: two decimals, never a full-precision float. */
function roundToCents(value: number): number {
  return Math.round(value * 100) / 100;
}

export function createOrderGenerator({
  products,
  minPrice,
  maxPrice,
  random = Math.random,
  startingOrderId = 1001,
}: OrderGeneratorOptions): OrderGenerator {
  // Narrowing the first element here gives every later lookup a typed fallback,
  // so `noUncheckedIndexedAccess` is satisfied without a cast or an assertion.
  const [firstProduct] = products;
  if (firstProduct === undefined) {
    throw new Error('at least one product is required');
  }
  if (minPrice > maxPrice) {
    throw new Error(`minPrice ${String(minPrice)} exceeds maxPrice ${String(maxPrice)}`);
  }

  let produced = 0;

  return {
    next(): Order {
      // A random source that returns exactly 1 would index past the end.
      const index = Math.min(Math.floor(random() * products.length), products.length - 1);

      const order: Order = {
        orderId: String(startingOrderId + produced),
        product: products[index] ?? firstProduct,
        price: roundToCents(minPrice + random() * (maxPrice - minPrice)),
      };

      produced += 1;
      return order;
    },
    get count(): number {
      return produced;
    },
  };
}
