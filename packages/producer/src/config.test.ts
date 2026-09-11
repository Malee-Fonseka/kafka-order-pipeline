import { ConfigurationError, loadConfig } from '@order-pipeline/shared';
import { describe, expect, it } from 'vitest';

import { producerEnvSchema } from './config.js';

const baseEnv = {
  KAFKA_BROKERS: 'localhost:9092',
  KAFKA_CLIENT_ID: 'producer-test',
  SCHEMA_REGISTRY_URL: 'http://localhost:8081',
};

function load(
  overrides: Record<string, string | undefined> = {},
): ReturnType<typeof loadConfig<typeof producerEnvSchema>> {
  return loadConfig(producerEnvSchema, { ...baseEnv, ...overrides });
}

describe('producer config', () => {
  it('applies documented defaults', () => {
    const config = load();

    expect(config.PRODUCER_RATE_PER_SEC).toBe(5);
    expect(config.PRODUCER_PRODUCTS).toEqual(['Item1', 'Item2', 'Item3', 'Item4', 'Item5']);
    expect(config.PRODUCER_MIN_PRICE).toBe(1);
    expect(config.PRODUCER_MAX_PRICE).toBe(500);
    expect(config.CHAOS_MODE).toBe(false);
    expect(config.PRODUCER_MAX_MESSAGES).toBeUndefined();
  });

  it('still inherits the shared base schema', () => {
    expect(load().TOPIC_PREFIX).toBe('orders');
  });

  it('parses the product list, trimming entries', () => {
    expect(load({ PRODUCER_PRODUCTS: ' Widget , Gadget ,, Doohickey ' }).PRODUCER_PRODUCTS).toEqual(
      ['Widget', 'Gadget', 'Doohickey'],
    );
  });

  it.each([
    { label: 'true', value: 'true', expected: true },
    { label: '1', value: '1', expected: true },
    { label: 'yes', value: 'YES', expected: true },
    { label: 'false', value: 'false', expected: false },
    { label: 'off', value: 'off', expected: false },
  ])('reads CHAOS_MODE=$label as $expected', ({ value, expected }) => {
    expect(load({ CHAOS_MODE: value }).CHAOS_MODE).toBe(expected);
  });

  it('rejects an unrecognised CHAOS_MODE spelling rather than guessing', () => {
    // Silently treating "maybe" as false would make a demo look broken with no
    // indication why chaos never fired.
    expect(() => load({ CHAOS_MODE: 'maybe' })).toThrow(ConfigurationError);
  });

  it('falls back to the default when a variable is present but blank', () => {
    // The half-edited `.env` case. `z.coerce.number()` would read this as 0,
    // which for a rate means a producer that silently emits nothing.
    const config = load({ PRODUCER_RATE_PER_SEC: '', CHAOS_POISON_RATE: '  ' });

    expect(config.PRODUCER_RATE_PER_SEC).toBe(5);
    expect(config.CHAOS_POISON_RATE).toBe(0.05);
  });

  it.each([
    { label: 'a zero rate', env: { PRODUCER_RATE_PER_SEC: '0' } },
    { label: 'a negative rate', env: { PRODUCER_RATE_PER_SEC: '-1' } },
    { label: 'a non-numeric rate', env: { PRODUCER_RATE_PER_SEC: 'fast' } },
    { label: 'a negative price', env: { PRODUCER_MIN_PRICE: '-5' } },
    { label: 'a chaos rate above 1', env: { CHAOS_POISON_RATE: '1.5' } },
    { label: 'a negative chaos rate', env: { CHAOS_TRANSIENT_RATE: '-0.1' } },
    { label: 'a fractional message budget', env: { PRODUCER_MAX_MESSAGES: '2.5' } },
  ])('rejects $label', ({ env }) => {
    expect(() => load(env)).toThrow(ConfigurationError);
  });

  it('rejects an inverted price range', () => {
    expect(() => load({ PRODUCER_MIN_PRICE: '100', PRODUCER_MAX_PRICE: '10' })).toThrow(
      /PRODUCER_MAX_PRICE/,
    );
  });

  it('rejects chaos rates that together exceed certainty', () => {
    expect(() => load({ CHAOS_TRANSIENT_RATE: '0.7', CHAOS_POISON_RATE: '0.5' })).toThrow(
      /must not exceed 1/,
    );
  });

  it('accepts chaos rates summing to exactly 1', () => {
    const config = load({ CHAOS_TRANSIENT_RATE: '0.6', CHAOS_POISON_RATE: '0.4' });

    expect(config.CHAOS_TRANSIENT_RATE + config.CHAOS_POISON_RATE).toBe(1);
  });

  it('accepts a fractional rate for a slow demo', () => {
    expect(load({ PRODUCER_RATE_PER_SEC: '0.5' }).PRODUCER_RATE_PER_SEC).toBe(0.5);
  });

  it('reports every problem at once', () => {
    // The whole reason loadConfig exists: fixing one variable per restart is a
    // miserable way to bring up a stack.
    try {
      load({ PRODUCER_RATE_PER_SEC: '-1', KAFKA_CLIENT_ID: '' });
      expect.unreachable('should have thrown');
    } catch (error) {
      const message = (error as ConfigurationError).message;
      expect(message).toContain('PRODUCER_RATE_PER_SEC');
      expect(message).toContain('KAFKA_CLIENT_ID');
    }
  });
});
