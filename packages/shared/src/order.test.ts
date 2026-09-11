import { describe, expect, it } from 'vitest';
import type { z } from 'zod';

import { PermanentError } from './errors.js';
import { type Order, formatOrder, isOrder, orderSchema, parseOrder } from './order.js';
import { readOrderSchemaJson, readOrderSchemaString } from './schema-file.js';

/**
 * Exact structural equality at the type level.
 *
 * The conditional-on-a-generic-function trick is the standard way to get
 * *invariant* comparison. Mutual `extends` would be weaker: it cannot tell
 * `unknown` from `any`, and it quietly accepts a widened field. Here, two types
 * are equal only if the compiler resolves both deferred conditionals
 * identically.
 *
 * `T` is necessarily used once per signature — that is the mechanism, not an
 * oversight — so the lint rule that objects to single-use type parameters has
 * to be turned off for this declaration specifically.
 */
/* eslint-disable @typescript-eslint/no-unnecessary-type-parameters */
type AssertEqual<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
/* eslint-enable @typescript-eslint/no-unnecessary-type-parameters */

describe('Order type', () => {
  it('matches the validator exactly, so the two cannot drift', () => {
    // Enforced by `tsc -b`, which compiles this file (§10.8): if the interface
    // and the validator disagree on any field, `AssertEqual` resolves to
    // `false` and the assignment below stops the build. The runtime assertion
    // is only here so the guarantee shows up as a named test.
    //
    // `Readonly` wraps the inferred side because that is the one deliberate
    // difference: `Order` marks its fields readonly (a decoded message is not
    // ours to mutate) while zod infers them mutable. Field names and value
    // types are still compared exactly.
    const typesAgree: AssertEqual<Readonly<z.infer<typeof orderSchema>>, Order> = true;
    expect(typesAgree).toBe(true);
  });

  it('declares exactly the fields in schemas/order.avsc', () => {
    const names = readOrderSchemaJson().fields.map((field) => field.name);

    // The interface is hand-written; this is what keeps it honest against the
    // one artefact that is actually registered with the registry.
    expect(names).toEqual(['orderId', 'product', 'price']);
    expect(Object.keys(orderSchema.shape)).toEqual(names);
  });
});

describe('schema file', () => {
  it('resolves from the package location, not the working directory', () => {
    // The CLI tools and vitest run from different cwds; a cwd-relative path
    // would work in tests and fail in the demo.
    expect(readOrderSchemaString()).toContain('"name": "Order"');
  });

  it('returns identical text on repeat reads', () => {
    expect(readOrderSchemaString()).toBe(readOrderSchemaString());
  });
});

describe('parseOrder', () => {
  it('accepts a well-formed order', () => {
    const order = parseOrder({ orderId: '1001', product: 'Item1', price: 19.99 });
    expect(order).toEqual({ orderId: '1001', product: 'Item1', price: 19.99 });
  });

  it('strips unknown fields so an evolved writer schema stays readable', () => {
    // BACKWARD compatibility permits adding a field with a default. A consumer
    // that has not been updated must ignore it, not reject the record.
    const order = parseOrder({
      orderId: '1001',
      product: 'Item1',
      price: 1,
      currency: 'EUR',
    });

    expect(order).not.toHaveProperty('currency');
  });

  it('normalises an avsc class instance into a plain object', () => {
    class DecodedOrder {
      public constructor(
        public orderId: string,
        public product: string,
        public price: number,
      ) {}
    }
    const parsed = parseOrder(new DecodedOrder('1', 'Item1', 5));

    expect(Object.getPrototypeOf(parsed)).toBe(Object.prototype);
  });

  it.each([
    { label: 'a negative price', value: { orderId: '1', product: 'Item1', price: -0.01 } },
    { label: 'an empty orderId', value: { orderId: '', product: 'Item1', price: 1 } },
    { label: 'an empty product', value: { orderId: '1', product: '', price: 1 } },
    { label: 'a NaN price', value: { orderId: '1', product: 'Item1', price: Number.NaN } },
    { label: 'a missing field', value: { orderId: '1', product: 'Item1' } },
    { label: 'a non-object', value: 'not an order' },
    { label: 'null', value: null },
  ])('rejects $label as a permanent failure', ({ value }) => {
    // Permanent, not transient: replaying a negative price produces a negative
    // price. Retrying it would be an infinite loop, so it belongs in the DLQ.
    try {
      parseOrder(value);
      expect.unreachable('should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(PermanentError);
      expect((error as PermanentError).reason).toBe('validation');
    }
  });

  it('reports every violation at once rather than only the first', () => {
    try {
      parseOrder({ orderId: '', product: '', price: -1 });
      expect.unreachable('should have thrown');
    } catch (error) {
      const message = (error as PermanentError).message;
      expect(message).toContain('orderId');
      expect(message).toContain('product');
      expect(message).toContain('price');
    }
  });

  it('accepts a zero price — free is valid, negative is not', () => {
    expect(parseOrder({ orderId: '1', product: 'Item1', price: 0 }).price).toBe(0);
  });
});

describe('isOrder', () => {
  it('narrows without throwing', () => {
    expect(isOrder({ orderId: '1', product: 'Item1', price: 1 })).toBe(true);
    expect(isOrder({ orderId: '1', product: 'Item1', price: -1 })).toBe(false);
    expect(isOrder(undefined)).toBe(false);
  });
});

describe('formatOrder', () => {
  it('renders a stable single line for logs', () => {
    expect(formatOrder({ orderId: '1001', product: 'Item1', price: 19.5 })).toBe(
      '1001 Item1 19.50',
    );
  });
});
