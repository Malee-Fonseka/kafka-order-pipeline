import { defineConfig } from 'vitest/config';

/**
 * Unit suite: tests colocated as `src/**\/*.test.ts`, no external services,
 * seconds to run. The integration suite has its own config —
 * `vitest.integration.config.ts` — and its own script.
 */
export default defineConfig({
  test: {
    environment: 'node',
    include: ['packages/*/src/**/*.test.ts'],
    exclude: ['**/node_modules/**', 'packages/*/src/**/*.integration.test.ts'],
    coverage: {
      provider: 'v8',
      include: ['packages/*/src/**/*.ts'],
      exclude: [
        'packages/*/src/**/*.test.ts',
        'packages/*/src/index.ts',
        'packages/*/src/integration/**',
      ],
    },
  },
});
