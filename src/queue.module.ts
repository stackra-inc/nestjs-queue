/**
 * NestJS queue module.
 *
 * Wraps `@nestjs/bullmq` with a stable API surface and exposes:
 *
 * - `QueueService` — BullMQ-flavored convenience facade (`dispatch`,
 *   `schedule`, `flow`, ...).
 * - `BullMQConnector` — adapter implementing the cross-stack
 *   `IQueueConnector` contract from `@stackra/contracts`. Lets server
 *   code share dispatching code with the browser (`@stackra/ts-queue`)
 *   when both are wired into the same DI container.
 *
 * Three registration entry points:
 *
 * - `forRoot(options)` — global Redis connection + shared services.
 * - `forRootAsync(options)` — DI-driven async configuration.
 * - `forFeature(queues)` — per-feature queue registration. Mirrors
 *   `BullModule.registerQueue(...)`.
 *
 * @module @stackra/nestjs-queue/queue.module
 */

import { type DynamicModule, Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';

import { BullMQConnector } from '@/connectors/bullmq.connector';
import { NESTJS_QUEUE_MODULE_OPTIONS } from '@/constants';
import type { INestjsQueueModuleOptions } from '@/interfaces';
import { QueueService } from '@/services/queue.service';

/**
 * Async-configuration shape — exposed as `forRootAsync` argument.
 */
export interface IQueueModuleAsyncOptions {
  /** Modules to import so the factory can inject their providers. */
  imports?: unknown[];

  /** Factory returning a resolved `INestjsQueueModuleOptions`. */
  useFactory: (
    ...deps: unknown[]
  ) => Promise<INestjsQueueModuleOptions> | INestjsQueueModuleOptions;

  /** DI tokens whose resolved providers are passed to `useFactory`. */
  inject?: unknown[];
}

/**
 * NestJS queue module.
 */
@Module({})
export class QueueModule {
  /**
   * Register the BullMQ connection globally.
   *
   * Call once in the root `AppModule`. Sets up:
   *
   * 1. The shared Redis connection used by every queue.
   * 2. `QueueService` for dispatching/scheduling/flows.
   * 3. `BullMQConnector` for cross-stack `IQueueConnector` consumers.
   *
   * @param options - BullMQ root-module options (connection, prefix, ...).
   * @returns A global NestJS dynamic module.
   *
   * @example
   * ```typescript
   * @Module({
   *   imports: [
   *     QueueModule.forRoot({
   *       connection: { host: 'redis.internal', port: 6379, db: 1 },
   *     }),
   *   ],
   * })
   * export class AppModule {}
   * ```
   */
  public static forRoot(options: INestjsQueueModuleOptions): DynamicModule {
    return {
      module: QueueModule,
      global: true,
      imports: [BullModule.forRoot(options)],
      providers: [
        { provide: NESTJS_QUEUE_MODULE_OPTIONS, useValue: options },
        QueueService,
        BullMQConnector,
      ],
      exports: [BullModule, QueueService, BullMQConnector, NESTJS_QUEUE_MODULE_OPTIONS],
    };
  }

  /**
   * Async variant of `forRoot()` for DI-driven configuration.
   *
   * @param options - Async options carrying `useFactory` / `inject`.
   * @returns A global NestJS dynamic module.
   */
  public static forRootAsync(options: IQueueModuleAsyncOptions): DynamicModule {
    return {
      module: QueueModule,
      global: true,
      imports: [
        ...((options.imports ?? []) as DynamicModule[]),
        BullModule.forRootAsync({
          imports: (options.imports ?? []) as never,
          useFactory: options.useFactory as never,
          inject: (options.inject ?? []) as never,
        }),
      ],
      providers: [
        {
          provide: NESTJS_QUEUE_MODULE_OPTIONS,
          useFactory: options.useFactory as never,
          inject: (options.inject ?? []) as never,
        },
        QueueService,
        BullMQConnector,
      ],
      exports: [BullModule, QueueService, BullMQConnector, NESTJS_QUEUE_MODULE_OPTIONS],
    };
  }

  /**
   * Register per-feature queues.
   *
   * Mirrors `BullModule.registerQueue(...)` — each queue becomes
   * injectable via `@InjectQueue(name)`. NOT global; only the feature
   * module that imports this declaration sees the queues.
   *
   * @param queues - Queue names to register.
   * @returns A NestJS dynamic module exposing each queue.
   *
   * @example
   * ```typescript
   * @Module({
   *   imports: [QueueModule.forFeature(['transfer', 'transfer:dlq'])],
   *   providers: [TransferProcessor, TransferService],
   * })
   * export class TransferModule {}
   * ```
   */
  public static forFeature(queues: string[]): DynamicModule {
    const queueConfigs = queues.map((name) => ({ name }));

    return {
      module: QueueModule,
      imports: [BullModule.registerQueue(...queueConfigs)],
      exports: [BullModule],
    };
  }
}
