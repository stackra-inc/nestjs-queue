/**
 * NestJS queue module options.
 *
 * Wraps `BullRootModuleOptions` from `@nestjs/bullmq` to provide a
 * stable API surface. If BullMQ's interface changes in a future
 * major version, only this file needs updating — consumer code stays
 * intact.
 *
 * Internal — only consumed inside `@stackra/nestjs-queue`.
 *
 * @module @stackra/nestjs-queue/interfaces/nestjs-queue-module-options
 */

import type { BullRootModuleOptions } from '@nestjs/bullmq';

/**
 * Configuration options for `QueueModule.forRoot()`.
 *
 * @see {@link https://docs.bullmq.io/guide/connections BullMQ Connection Docs}
 */
export type INestjsQueueModuleOptions = BullRootModuleOptions;
