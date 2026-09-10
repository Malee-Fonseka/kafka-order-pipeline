import { pino, type Logger, type LoggerOptions } from 'pino';

export type { Logger };

export interface LoggerParams {
  readonly service: string;
  readonly level: string;
  readonly pretty: boolean;
}

/**
 * Structured JSON logger. Pretty-printed in development only — production and
 * CI emit raw JSON so logs stay machine-parseable.
 *
 * Note the conditional spread: `exactOptionalPropertyTypes` forbids assigning
 * `undefined` to an optional property, so the key must be absent, not undefined.
 */
export function createLogger({ service, level, pretty }: LoggerParams): Logger {
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
            },
          },
        }
      : {}),
  };

  return pino(options);
}
