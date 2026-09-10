import { describe, expect, it } from 'vitest';
import { baseEnvSchema, ConfigurationError, loadConfig } from './config.js';

const validEnv = {
  KAFKA_BROKERS: 'localhost:9092',
  KAFKA_CLIENT_ID: 'test-client',
  SCHEMA_REGISTRY_URL: 'http://localhost:8081',
};

describe('loadConfig', () => {
  it('applies defaults for optional variables', () => {
    const config = loadConfig(baseEnvSchema, validEnv);

    expect(config.NODE_ENV).toBe('development');
    expect(config.LOG_LEVEL).toBe('info');
    expect(config.TOPIC_PREFIX).toBe('orders');
  });

  it('parses comma-separated brokers into a trimmed list', () => {
    const config = loadConfig(baseEnvSchema, {
      ...validEnv,
      KAFKA_BROKERS: 'a:9092, b:9092 ,c:9092',
    });

    expect(config.KAFKA_BROKERS).toEqual(['a:9092', 'b:9092', 'c:9092']);
  });

  it('reports every invalid variable at once', () => {
    expect(() =>
      loadConfig(baseEnvSchema, { KAFKA_BROKERS: '', SCHEMA_REGISTRY_URL: 'not-a-url' }),
    ).toThrow(ConfigurationError);
  });
});
