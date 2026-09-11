import { baseEnvSchema } from '@order-pipeline/shared';
import { z } from 'zod';

/**
 * Producer environment contract.
 *
 * Extends the shared base schema rather than adding producer concerns to it
 * (Appendix B), so a consumer-only deployment is never asked for an emission
 * rate it has no use for.
 */

/**
 * A number from the environment.
 *
 * Not `z.coerce.number()`: coercion turns an empty string into `0`, so
 * `CHAOS_POISON_RATE=` in a half-edited `.env` would silently read as a valid
 * rate of zero rather than falling back to the documented default.
 */
function numberEnv(fallback: number): z.ZodType<number, z.ZodTypeDef, unknown> {
  return z
    .unknown()
    .transform((raw) => {
      if (raw === undefined || raw === null || (typeof raw === 'string' && raw.trim() === '')) {
        return fallback;
      }
      return Number(raw);
    })
    .pipe(z.number({ invalid_type_error: 'must be a number' }).finite('must be a finite number'));
}

/** A boolean from the environment, accepting the usual spellings. */
function booleanEnv(fallback: boolean): z.ZodType<boolean, z.ZodTypeDef, unknown> {
  const truthy = new Set(['1', 'true', 'yes', 'on']);
  const falsy = new Set(['0', 'false', 'no', 'off']);

  return z.unknown().transform((raw, ctx) => {
    if (raw === undefined || raw === null || (typeof raw === 'string' && raw.trim() === '')) {
      return fallback;
    }
    if (typeof raw !== 'string') {
      // Everything in `process.env` is a string; anything else means this
      // schema was handed a non-environment object, which is a caller bug.
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'must be a string' });
      return z.NEVER;
    }

    const normalised = raw.trim().toLowerCase();
    if (truthy.has(normalised)) {
      return true;
    }
    if (falsy.has(normalised)) {
      return false;
    }

    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `must be one of ${[...truthy, ...falsy].join(', ')}`,
    });
    return z.NEVER;
  });
}

/** A probability. Out-of-range values are a configuration bug, not a clamp. */
function rateEnv(fallback: number): z.ZodType<number, z.ZodTypeDef, unknown> {
  return numberEnv(fallback).pipe(z.number().min(0, 'must be >= 0').max(1, 'must be <= 1'));
}

export const producerEnvSchema = baseEnvSchema
  .extend({
    /** Messages per second. Fractional values are allowed for slow demos. */
    PRODUCER_RATE_PER_SEC: numberEnv(5).pipe(z.number().positive('must be greater than zero')),

    /** Products to emit. Each becomes a partition key, so this is the key space (D1). */
    PRODUCER_PRODUCTS: z
      .string()
      .default('Item1,Item2,Item3,Item4,Item5')
      .transform((raw) =>
        raw
          .split(',')
          .map((entry) => entry.trim())
          .filter((entry) => entry.length > 0),
      )
      .refine((products) => products.length > 0, 'at least one product is required'),

    PRODUCER_MIN_PRICE: numberEnv(1).pipe(z.number().nonnegative('must not be negative')),
    PRODUCER_MAX_PRICE: numberEnv(500).pipe(z.number().positive('must be greater than zero')),

    /** Stop after this many messages. Unset means run until interrupted. */
    PRODUCER_MAX_MESSAGES: z
      .string()
      .optional()
      .transform((raw) => (raw === undefined || raw.trim() === '' ? undefined : Number(raw)))
      .pipe(z.number().int().positive().optional()),

    /** Master switch for fault injection (D8). Off means every record is valid. */
    CHAOS_MODE: booleanEnv(false),

    /** Share of records emitted with the transient-failure marker product. */
    CHAOS_TRANSIENT_RATE: rateEnv(0.15),

    /** Share of records emitted as undecodable bytes. */
    CHAOS_POISON_RATE: rateEnv(0.05),
  })
  .superRefine((config, ctx) => {
    if (config.PRODUCER_MIN_PRICE > config.PRODUCER_MAX_PRICE) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['PRODUCER_MIN_PRICE'],
        message: `must not exceed PRODUCER_MAX_PRICE (${String(config.PRODUCER_MAX_PRICE)})`,
      });
    }

    const injected = config.CHAOS_TRANSIENT_RATE + config.CHAOS_POISON_RATE;
    if (injected > 1) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['CHAOS_TRANSIENT_RATE'],
        message: `CHAOS_TRANSIENT_RATE + CHAOS_POISON_RATE must not exceed 1 (got ${String(injected)})`,
      });
    }
  });

export type ProducerEnv = z.infer<typeof producerEnvSchema>;
