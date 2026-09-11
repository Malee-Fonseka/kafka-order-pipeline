import { defineConfig } from 'vitest/config';

/**
 * Integration suite: `*.integration.test.ts` files start Kafka and Schema
 * Registry in containers (testcontainers) and run the real consumer against
 * them. Minutes to run; needs Docker. `npm run test:integration`.
 */
export default defineConfig({
  test: {
    environment: 'node',
    include: ['packages/*/src/**/*.integration.test.ts'],
    // Container start dominates; scenarios set their own timeouts on top.
    testTimeout: 90_000,
    hookTimeout: 180_000,
    // Files share nothing but Docker; one at a time so two stacks are never
    // starting simultaneously on a small CI runner.
    fileParallelism: false,
  },
});
