import {
  type Order,
  POISON_FLAVOURS,
  type PoisonFlavour,
  TRANSIENT_FAIL_PRODUCT,
  createPoisonPayload,
} from '@order-pipeline/shared';

/**
 * Fault injection built into the producer (design decision D8).
 *
 * The three features this assignment is graded on — retry, DLQ, aggregation —
 * are only observable when something fails. Waiting for a real failure during a
 * live demonstration is not a plan; §12 lists "a demo that depends on something
 * failing by luck" as marks-losing. So the producer can be told to emit
 * failures on purpose, at a configured rate, from a seedable random source.
 *
 * Two distinct kinds, because they exercise opposite halves of the error
 * taxonomy (D4):
 *
 * - **Transient** — a structurally valid order carrying a marker product. The
 *   consumer's handler throws a *retryable* error for it and succeeds after a
 *   few attempts, demonstrating the retry tiers.
 * - **Poison** — bytes that are not decodable Avro at all. These fail
 *   *permanently* and must reach the DLQ without ever entering a retry tier.
 */

/** Re-exported for the producer's own tests and callers; defined in shared. */
export { POISON_FLAVOURS, TRANSIENT_FAIL_PRODUCT, createPoisonPayload, type PoisonFlavour };

export type Emission =
  | { readonly kind: 'valid'; readonly order: Order }
  | { readonly kind: 'transient'; readonly order: Order }
  | {
      readonly kind: 'poison';
      /** Still keyed by a real product, so the record lands on a live partition. */
      readonly product: string;
      readonly flavour: PoisonFlavour;
      readonly bytes: Buffer;
    };

export interface ChaosOptions {
  /** When false, every emission is valid and the rates are ignored. */
  readonly enabled: boolean;
  readonly transientRate: number;
  readonly poisonRate: number;
  readonly random?: () => number;
}

export interface ChaosInjector {
  /** Decides what to do with the next generated order. */
  plan: (order: Order, referencePayload?: Buffer) => Emission;
}

export function createChaosInjector({
  enabled,
  transientRate,
  poisonRate,
  random = Math.random,
}: ChaosOptions): ChaosInjector {
  let flavourIndex = 0;

  return {
    plan(order: Order, referencePayload?: Buffer): Emission {
      if (!enabled) {
        return { kind: 'valid', order };
      }

      const roll = random();

      if (roll < poisonRate) {
        const flavour = POISON_FLAVOURS[flavourIndex % POISON_FLAVOURS.length] ?? 'json-not-avro';
        flavourIndex += 1;

        return {
          kind: 'poison',
          product: order.product,
          flavour,
          bytes: createPoisonPayload(flavour, referencePayload),
        };
      }

      if (roll < poisonRate + transientRate) {
        // The marker replaces the product, which also means the record keys to
        // its own partition — convenient when watching retries in Kafbat UI.
        return { kind: 'transient', order: { ...order, product: TRANSIENT_FAIL_PRODUCT } };
      }

      return { kind: 'valid', order };
    },
  };
}
