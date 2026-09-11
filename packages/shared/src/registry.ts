import {
  Compatibility,
  RestError,
  SchemaRegistryClient,
  type Client,
  type ClientConfig,
  type SchemaInfo,
} from '@confluentinc/schemaregistry';

import { type ClassifiedError, PermanentError, TransientError, describeError } from './errors.js';
import { ORDER_SCHEMA_TYPE, readOrderSchemaString } from './schema-file.js';
import { valueSubjectFor } from './topics.js';

import type { Logger } from 'pino';

/**
 * Schema Registry access and the bootstrap that registers `order.avsc`.
 *
 * Note the plain named imports: unlike `@confluentinc/kafka-javascript` (which
 * must be default-imported, see §10.1), this package's CommonJS exports are
 * statically detectable by Node's ESM loader, so named imports resolve
 * correctly. Verified, not assumed — do not "align" it with the Kafka client.
 */

export { Compatibility };

/** Compatibility mode enforced on the Order subject. See ADR 007. */
export const ORDER_COMPATIBILITY = Compatibility.BACKWARD;

export interface RegistryClientOptions {
  readonly url: string;
  /** Registry HTTP retries. Covers a restarting registry without involving the retry tiers. */
  readonly maxRetries?: number;
  readonly retriesWaitMs?: number;
  readonly retriesMaxWaitMs?: number;
}

/**
 * Builds a registry client.
 *
 * Returns the `Client` interface rather than the concrete class so that tests
 * can substitute `MockClient` — which implements the same interface — without
 * a cast.
 */
export function createRegistryClient({
  url,
  maxRetries = 3,
  retriesWaitMs = 500,
  retriesMaxWaitMs = 5_000,
}: RegistryClientOptions): Client {
  const config: ClientConfig = {
    baseURLs: [url],
    maxRetries,
    retriesWaitMs,
    retriesMaxWaitMs,
  };

  return new SchemaRegistryClient(config);
}

/** The schema as it should be sent to the registry. */
export function orderSchemaInfo(): SchemaInfo {
  return { schema: readOrderSchemaString(), schemaType: ORDER_SCHEMA_TYPE };
}

export interface SchemaRegistration {
  readonly subject: string;
  readonly schemaId: number;
  readonly version: number;
  readonly compatibility: Compatibility;
}

export interface EnsureSchemaOptions {
  readonly client: Client;
  /** Main topic; the subject is derived as `<topic>-value` (TopicNameStrategy). */
  readonly topic: string;
  readonly logger: Logger;
  readonly compatibility?: Compatibility;
}

/**
 * Registers `schemas/order.avsc` under `<topic>-value` and pins the subject's
 * compatibility mode. Safe to call on every service start.
 *
 * Idempotent by the registry's own semantics: registering identical schema text
 * returns the existing ID instead of allocating a new version, so every service
 * can call this at boot without coordinating with the others.
 *
 * Registration happens *before* the compatibility update because a subject's
 * config cannot be set reliably until the subject exists. The ordering is not
 * a compromise — there is nothing for the very first schema to be backward
 * compatible *with*.
 */
export async function ensureOrderSchemaRegistered({
  client,
  topic,
  logger,
  compatibility = ORDER_COMPATIBILITY,
}: EnsureSchemaOptions): Promise<SchemaRegistration> {
  const subject = valueSubjectFor(topic);
  const info = orderSchemaInfo();

  // normalize: true — the registry compares canonicalised schema text, so
  // reformatting order.avsc does not spawn a redundant version.
  const schemaId = await callRegistry(
    () => client.register(subject, info, true),
    'register schema',
  );

  const appliedCompatibility = await callRegistry(
    () => client.updateCompatibility(subject, compatibility),
    'set subject compatibility',
  );

  const version = await callRegistry(
    () => client.getVersion(subject, info, true, false),
    'resolve schema version',
  );

  logger.info(
    { subject, schemaId, version, compatibility: appliedCompatibility },
    'order schema registered with schema registry',
  );

  return { subject, schemaId, version, compatibility: appliedCompatibility };
}

/**
 * Classifies a registry failure.
 *
 * Default is **transient**: a registry call that fails for an unrecognised
 * reason is far more likely to be a restarting container or a dropped socket
 * than a fact about the message. Only responses that state a durable problem —
 * an incompatible or malformed schema, a missing schema ID — are permanent.
 *
 * The inverse default applies when decoding a payload; see `serde.ts`.
 */
export function classifyRegistryError(error: unknown, action: string): ClassifiedError {
  const detail = `${action} failed: ${describeError(error)}`;

  if (error instanceof RestError) {
    // 409 Conflict — the schema violates the subject's compatibility policy.
    // 422 Unprocessable — the schema itself is malformed.
    if (error.status === 409 || error.status === 422) {
      return new PermanentError('schema-incompatible', detail, { cause: error });
    }

    // 404 — schema or subject genuinely absent from the registry.
    if (error.status === 404) {
      return new PermanentError('unknown-schema-id', detail, { cause: error });
    }

    // 401/403 — misconfiguration rather than a transient fault, but retrying is
    // harmless and a rotating credential does resolve itself. Fall through.
  }

  return new TransientError(detail, { cause: error });
}

/** Runs a registry call, converting any failure into a classified error. */
async function callRegistry<T>(operation: () => Promise<T>, action: string): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    throw classifyRegistryError(error, action);
  }
}
