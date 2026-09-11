/**
 * The error taxonomy (design decision D4).
 *
 * Every failure in the pipeline is classified into exactly one of two shapes
 * before any retry or DLQ decision is taken:
 *
 * - **transient** — the operation could plausibly succeed if repeated, so it is
 *   retried through the tiered retry topics.
 * - **permanent** — repeating the operation cannot change the outcome, so it
 *   goes straight to the dead letter queue.
 *
 * Getting this wrong in either direction is a correctness bug, not a style
 * choice: retrying a poison pill is a livelock, and dead-lettering a dropped
 * TCP connection is silent data loss. The classification is therefore an
 * explicit, typed, unit-tested decision rather than scattered `try`/`catch`.
 *
 * The two variants are discriminated on `kind`, so a `switch` over a
 * {@link ClassifiedError} narrows exhaustively and the compiler rejects any
 * handler that forgets a case.
 */

/** Why a failure can never succeed on a retry. Travels into DLQ headers. */
export type PermanentReason =
  /** Bytes on the topic are not a valid Confluent-framed Avro payload. */
  | 'deserialization'
  /** Writer schema cannot be read with the reader schema. */
  | 'schema-incompatible'
  /** Wire-format schema ID is not present in the registry. */
  | 'unknown-schema-id'
  /** Decoded fine, but the value violates a business rule. */
  | 'validation';

export type ErrorKind = 'transient' | 'permanent';

export interface PipelineErrorOptions {
  readonly cause?: unknown;
}

/**
 * Common base so a single `instanceof` distinguishes "we classified this" from
 * "this escaped unclassified", which is itself a bug worth logging loudly.
 */
export abstract class PipelineError extends Error {
  public abstract readonly kind: ErrorKind;

  protected constructor(message: string, options: PipelineErrorOptions = {}) {
    // Conditional spread, not `{ cause: options.cause }`: under
    // exactOptionalPropertyTypes an explicit `undefined` is not the same as an
    // absent key, and `new Error(m, { cause: undefined })` would set a `cause`
    // own-property of undefined that later serialises into DLQ headers.
    super(message, ...('cause' in options ? [{ cause: options.cause }] : []));
    this.name = new.target.name;
  }
}

/** Retryable. Carries the underlying cause for logging and DLQ forensics. */
export class TransientError extends PipelineError {
  public override readonly kind = 'transient';

  public constructor(message: string, options: PipelineErrorOptions = {}) {
    super(message, options);
  }
}

/** Not retryable. `reason` is the machine-readable code recorded in the DLQ. */
export class PermanentError extends PipelineError {
  public override readonly kind = 'permanent';
  public readonly reason: PermanentReason;

  public constructor(reason: PermanentReason, message: string, options: PipelineErrorOptions = {}) {
    super(message, options);
    this.reason = reason;
  }
}

/** The discriminated union callers switch on. */
export type ClassifiedError = TransientError | PermanentError;

export function isClassifiedError(error: unknown): error is ClassifiedError {
  return error instanceof PipelineError;
}

export function isTransient(error: unknown): error is TransientError {
  return error instanceof TransientError;
}

export function isPermanent(error: unknown): error is PermanentError {
  return error instanceof PermanentError;
}

/**
 * Best-effort human description of any thrown value.
 *
 * Handlers receive `unknown`, and a thrown string or plain object must still
 * produce a usable log line and DLQ header rather than "[object Object]".
 */
export function describeError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  try {
    return toJson(error) ?? String(error);
  } catch {
    return String(error);
  }
}

/**
 * `JSON.stringify` is declared as returning `string`, but genuinely returns
 * `undefined` for a top-level `undefined`, symbol or function — precisely the
 * values that reach {@link describeError}. Narrowing the return type here keeps
 * the fallback at the call site honest.
 */
function toJson(value: unknown): string | undefined {
  return JSON.stringify(value);
}
