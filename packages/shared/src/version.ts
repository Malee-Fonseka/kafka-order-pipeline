import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Reads the version from a package's own `package.json`.
 *
 * Used for the `x-app-version` DLQ header (D6): knowing which build produced a
 * record that later failed is the difference between "this message is bad" and
 * "the deploy at 14:05 is bad".
 *
 * Callers pass their own `import.meta.url`. Because `src/` and `dist/` sit at
 * the same depth inside a package, one relative walk resolves the manifest
 * whether the caller was loaded from source or from compiled output.
 */
export function readPackageVersion(moduleUrl: string): string {
  const manifestPath = resolve(dirname(fileURLToPath(moduleUrl)), '..', 'package.json');

  try {
    const parsed: unknown = JSON.parse(readFileSync(manifestPath, 'utf8'));

    if (typeof parsed === 'object' && parsed !== null && 'version' in parsed) {
      const { version } = parsed;
      if (typeof version === 'string') {
        return version;
      }
    }
  } catch {
    // Fall through — a missing or unreadable manifest must never stop a
    // service from starting over a diagnostic header.
  }

  return 'unknown';
}
