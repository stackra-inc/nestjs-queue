/**
 * NestJS-queue internal DI tokens.
 *
 * Cross-package tokens (`QUEUE_SERVICE`, `QUEUE_MANAGER`,
 * `QUEUE_CONFIG`, `DEFAULT_QUEUE_CONNECTION_TOKEN`, ...) live in
 * `@stackra/contracts` — re-exported here for ergonomics. Tokens that
 * are only used inside this package stay local.
 *
 * @module @stackra/nestjs-queue/constants/tokens
 */

// ── Public tokens re-exported from contracts ────────────────────────────
export { QUEUE_CONFIG, QUEUE_MANAGER, DEFAULT_QUEUE_CONNECTION_TOKEN } from '@stackra/contracts';

// ── Module-internal tokens ──────────────────────────────────────────────

/**
 * Token for the raw `BullRootModuleOptions` payload supplied to
 * `BullModule.forRoot()`. Module-internal — other packages must not
 * import this token.
 */
export const NESTJS_QUEUE_MODULE_OPTIONS = Symbol.for('NESTJS_QUEUE_MODULE_OPTIONS');
