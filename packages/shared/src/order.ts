import { z } from 'zod';

import { PermanentError } from './errors.js';

/**
 * The order record, mirroring `schemas/order.avsc` field for field.
 *
 * Hand-written rather than code-generated so the doc comments stay readable,
 * and pinned to the schema by a compile-time equality assertion in
 * `order.test.ts` — if the Avro record and this interface ever drift, the build
 * fails rather than the pipeline silently dropping a field.
 */
export interface Order {
  /** Unique identifier for the order, e.g. `"1001"`. */
  readonly orderId: string;
  /**
   * Name of the purchased item, e.g. `"Item1"`.
   *
   * Also the Kafka message key (D1): hashing on `product` keeps every message
   * for a product on one partition, so exactly one consumer instance owns that
   * product's running average.
   */
  readonly product: string;
  /**
   * Price of the product.
   *
   * `float` (32-bit) on the wire per the assignment schema; JavaScript has only
   * `number`, so a value that round-trips through Avro comes back as the
   * nearest float32. Aggregation widens to double internally (D3).
   */
  readonly price: number;
}

/**
 * Business validation, applied *after* a successful Avro decode.
 *
 * Avro guarantees shape, not sense: `price: -5` is a perfectly valid float. A
 * value that decodes but violates a rule is a permanent failure — replaying it
 * would fail identically — so it belongs in the DLQ, never in a retry tier.
 */
export const orderSchema = z.object({
  orderId: z.string().min(1, 'orderId must not be empty'),
  product: z.string().min(1, 'product must not be empty'),
  price: z
    .number()
    .finite('price must be a finite number')
    .nonnegative('price must not be negative'),
});

/**
 * Validates a decoded record, throwing a classified permanent error listing
 * every violation at once.
 */
export function parseOrder(value: unknown): Order {
  const result = orderSchema.safeParse(value);

  if (!result.success) {
    const issues = result.error.issues
      .map((issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`)
      .join('; ');
    throw new PermanentError('validation', `order failed validation: ${issues}`, {
      cause: result.error,
    });
  }

  return result.data;
}

/**
 * Narrows an already-decoded value to {@link Order} without throwing.
 * Useful where a caller wants to branch rather than catch.
 */
export function isOrder(value: unknown): value is Order {
  return orderSchema.safeParse(value).success;
}

/** Stable one-line rendering for logs and CLI output. */
export function formatOrder(order: Order): string {
  return `${order.orderId} ${order.product} ${order.price.toFixed(2)}`;
}
