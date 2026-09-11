import { pino, type Logger, type LoggerOptions, destination as pinoDestination } from 'pino';

export type { Logger };

export interface LoggerParams {
  readonly service: string;
  readonly level: string;
  readonly pretty: boolean;
  /**
   * Where log lines go. Services use stdout. A CLI whose stdout *is its
   * output* — a table, a JSON document meant for a pipe — logs to stderr so
   * that `dlq-inspector list --json | jq` sees data and only data.
   */
  readonly destination?: 'stdout' | 'stderr';
}

/**
 * Structured JSON logger. Pretty-printed in development only — production and
 * CI emit raw JSON so logs stay machine-parseable.
 *
 * Note the conditional spread: `exactOptionalPropertyTypes` forbids assigning
 * `undefined` to an optional property, so the key must be absent, not undefined.
 */
export function createLogger({
  service,
  level,
  pretty,
  destination = 'stdout',
}: LoggerParams): Logger {
  const fd = destination === 'stderr' ? 2 : 1;
  const options: LoggerOptions = {
    level,
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
      level: (label) => ({ level: label }),
    },
    ...(pretty
      ? {
          transport: {
            target: 'pino-pretty',
            options: {
              colorize: true,
              translateTime: 'SYS:HH:MM:ss.l',
              ignore: 'pid,hostname,service',
              messageFormat: '[{service}] {msg}',
              destination: fd,
            },
          },
        }
      : {}),
  };

  return pretty ? pino(options) : pino(options, pinoDestination(fd));
}
