import { z } from 'zod';

/**
 * Environment contract shared by every service.
 *
 * Individual services extend this with their own fields rather than reading
 * `process.env` directly, so that a missing or malformed variable fails at
 * startup with a readable message instead of surfacing as `undefined` deep
 * inside a Kafka callback.
 */
export const baseEnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),

  /** Comma-separated bootstrap servers, parsed into a list. */
  KAFKA_BROKERS: z
    .string()
    .min(1, 'at least one broker is required')
    .transform((raw) =>
      raw
        .split(',')
        .map((entry) => entry.trim())
        .filter((entry) => entry.length > 0),
    )
    .refine((brokers) => brokers.length > 0, 'at least one broker is required'),

  KAFKA_CLIENT_ID: z.string().min(1),

  SCHEMA_REGISTRY_URL: z.string().refine((value) => {
    try {
      new URL(value);
      return true;
    } catch {
      return false;
    }
  }, 'must be a valid absolute URL'),

  TOPIC_PREFIX: z.string().min(1).default('orders'),
});

export type BaseEnv = z.infer<typeof baseEnvSchema>;

/** Raised when environment validation fails. Carries every problem at once. */
export class ConfigurationError extends Error {
  public constructor(issues: readonly string[]) {
    super(`Invalid environment configuration:\n  - ${issues.join('\n  - ')}`);
    this.name = 'ConfigurationError';
  }
}

/**
 * Validates `source` against `schema`, throwing a ConfigurationError listing
 * every problem rather than only the first.
 */
export function loadConfig<TSchema extends z.ZodTypeAny>(
  schema: TSchema,
  source: Record<string, string | undefined> = process.env,
): z.infer<TSchema> {
  const result = schema.safeParse(source);

  if (!result.success) {
    const issues = result.error.issues.map((issue) => {
      const path = issue.path.length > 0 ? issue.path.join('.') : '(root)';
      return `${path}: ${issue.message}`;
    });
    throw new ConfigurationError(issues);
  }

  return result.data as z.infer<TSchema>;
}
