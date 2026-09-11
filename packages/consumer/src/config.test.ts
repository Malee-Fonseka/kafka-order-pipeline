import { ConfigurationError, loadConfig } from '@order-pipeline/shared';
import { describe, expect, it } from 'vitest';

import { consumerEnvSchema } from './config.js';

const baseEnv = {
  KAFKA_BROKERS: 'localhost:9092',
  KAFKA_CLIENT_ID: 'consumer-test',
  SCHEMA_REGISTRY_URL: 'http://localhost:8081',
};

function load(
  overrides: Record<string, string | undefined> = {},
): ReturnType<typeof loadConfig<typeof consumerEnvSchema>> {
  return loadConfig(consumerEnvSchema, { ...baseEnv, ...overrides });
}

describe('consumer config', () => {
  it('applies documented defaults', () => {
    const config = load();

    expect(config.CONSUMER_GROUP_ID).toBe('order-consumers');
    expect(config.CONSUMER_AUTO_OFFSET_RESET).toBe('earliest');
  });

  it('still inherits the shared base schema', () => {
    expect(load().TOPIC_PREFIX).toBe('orders');
  });

  it('accepts latest as the offset reset policy', () => {
    expect(load({ CONSUMER_AUTO_OFFSET_RESET: 'latest' }).CONSUMER_AUTO_OFFSET_RESET).toBe(
      'latest',
    );
  });

  it.each([
    { label: 'an empty group id', env: { CONSUMER_GROUP_ID: '' } },
    { label: 'an unknown reset policy', env: { CONSUMER_AUTO_OFFSET_RESET: 'newest' } },
  ])('rejects $label', ({ env }) => {
    expect(() => load(env)).toThrow(ConfigurationError);
  });
});
