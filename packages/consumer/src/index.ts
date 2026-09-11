import {
  createLogger,
  createShutdownManager,
  isClassifiedError,
  loadConfig,
} from '@order-pipeline/shared';

import { createConsumerApp } from './app.js';
import { consumerEnvSchema } from './config.js';

/**
 * Process entry point. Everything of substance is in `app.ts`; this file
 * exists to read the environment, own the process-level concerns — signals,
 * the exit code — and get out of the way.
 */

const config = loadConfig(consumerEnvSchema);

const logger = createLogger({
  service: 'consumer',
  level: config.LOG_LEVEL,
  pretty: config.NODE_ENV === 'development',
});

const shutdown = createShutdownManager({ logger });
const app = createConsumerApp({ config, logger });

// The app tears its components down in the right order itself; the shutdown
// manager only needs to know when to ask.
shutdown.register('consumer-app', () => app.stop());

app
  .start()
  .then(() => shutdown.wait())
  .catch((error: unknown) => {
    logger.fatal(
      { err: error, classified: isClassifiedError(error) ? error.kind : 'unclassified' },
      'consumer failed',
    );
    process.exit(1);
  });
