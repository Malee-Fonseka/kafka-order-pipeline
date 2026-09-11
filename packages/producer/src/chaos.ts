import { type Order, TRANSIENT_FAIL_PRODUCT, encodeWireFormatHeader } from '@order-pipeline/shared';

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
export { TRANSIENT_FAIL_PRODUCT };

/**
 * The ways a payload can be undecodable.
 *
 * Cycled rather than picked at random so a demo run exercises all three, and so
 * the DLQ ends up showing genuinely different failure reasons instead of the
 * same one three times.
 */
export type PoisonFlavour =
  /** Someone published JSON to an Avro topic: the magic byte is wrong. */
  | 'json-not-avro'
  /** Correctly framed, but the schema id resolves to nothing in the registry. */
  | 'unknown-schema-id'
  /** Correctly framed and a real schema id, but the payload is cut short. */
  | 'truncated-payload';

export const POISON_FLAVOURS: readonly PoisonFlavour[] = [
  'json-not-avro',
  'unknown-schema-id',
  'truncated-payload',
];

/** A schema id no registry in this project will ever allocate. */
const UNREGISTERED_SCHEMA_ID = 999_999;

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

/**
 * Builds bytes that the consumer genuinely cannot decode.
 *
 * `referencePayload` is a real serialized order, used only by the truncation
 * flavour — cutting a valid record short is the one corruption that cannot be
 * fabricated without a valid record to start from. Without it, that flavour
 * falls back to an unknown schema id rather than emitting something that would
 * accidentally decode.
 */
export function createPoisonPayload(flavour: PoisonFlavour, referencePayload?: Buffer): Buffer {
  switch (flavour) {
    case 'json-not-avro':
      // Byte 0 is '{' (0x7b), not the 0x00 magic byte.
      return Buffer.from(
        JSON.stringify({ orderId: '9999', product: 'Item1', price: 42, note: 'not avro' }),
        'utf8',
      );

    case 'unknown-schema-id':
      return encodeWireFormatHeader(UNREGISTERED_SCHEMA_ID, Buffer.from([0x02, 0x41, 0x00]));

    case 'truncated-payload':
      if (referencePayload !== undefined && referencePayload.length > 8) {
        return Buffer.from(referencePayload.subarray(0, 8));
      }
      return encodeWireFormatHeader(UNREGISTERED_SCHEMA_ID, Buffer.from([0x02]));
  }
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
