import { DynamicModule } from '@nestjs/common';
import { BullRootModuleOptions } from '@nestjs/bullmq';
export { InjectQueue, OnWorkerEvent, Processor, WorkerHost } from '@nestjs/bullmq';
import { ModuleRef } from '@nestjs/core';
import { JobsOptions, FlowJob, ConnectionOptions, RedisClient, Queue, QueueEvents } from 'bullmq';
export { FlowJob, FlowProducer, Job, JobsOptions, Queue } from 'bullmq';
import { IQueueConnector, QueueConnectionConfig, IQueueConnection, IJobOptions, IQueuedJob, JobStatus } from '@stackra/contracts';
export { DEFAULT_QUEUE_CONNECTION_TOKEN, QUEUE_CONFIG, QUEUE_MANAGER } from '@stackra/contracts';

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

/**
 * Configuration options for `QueueModule.forRoot()`.
 *
 * @see {@link https://docs.bullmq.io/guide/connections BullMQ Connection Docs}
 */
type INestjsQueueModuleOptions = BullRootModuleOptions;

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

/**
 * Async-configuration shape — exposed as `forRootAsync` argument.
 */
interface IQueueModuleAsyncOptions {
    /** Modules to import so the factory can inject their providers. */
    imports?: unknown[];
    /** Factory returning a resolved `INestjsQueueModuleOptions`. */
    useFactory: (...deps: unknown[]) => Promise<INestjsQueueModuleOptions> | INestjsQueueModuleOptions;
    /** DI tokens whose resolved providers are passed to `useFactory`. */
    inject?: unknown[];
}
/**
 * NestJS queue module.
 */
declare class QueueModule {
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
    static forRoot(options: INestjsQueueModuleOptions): DynamicModule;
    /**
     * Async variant of `forRoot()` for DI-driven configuration.
     *
     * @param options - Async options carrying `useFactory` / `inject`.
     * @returns A global NestJS dynamic module.
     */
    static forRootAsync(options: IQueueModuleAsyncOptions): DynamicModule;
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
    static forFeature(queues: string[]): DynamicModule;
}

/**
 * BullMQ-flavored convenience service.
 *
 * High-level dispatcher for NestJS apps that want a single injectable
 * facade over their BullMQ queues without going through the portable
 * `IQueueService` abstraction. Resolves queue handles dynamically via
 * `ModuleRef`, caching them on first access.
 *
 * For cross-stack code that should also work on the browser, prefer
 * the `IQueueService` (`QUEUE_MANAGER`) surface from `@stackra/contracts`.
 *
 * @module @stackra/nestjs-queue/services/queue.service
 */

/**
 * BullMQ-flavored convenience service.
 *
 * Resolves named BullMQ `Queue` instances from the DI container at
 * runtime, supports dispatching, scheduling, and flow execution.
 */
declare class QueueService {
    private readonly moduleRef;
    /** Scoped logger. */
    private readonly logger;
    /** Cached `Queue` instances keyed by name. */
    private readonly queues;
    /** Lazy `FlowProducer` reusing the first resolved queue's connection. */
    private flowProducer;
    /**
     * @param moduleRef - NestJS `ModuleRef` for token-based lookup.
     */
    constructor(moduleRef: ModuleRef);
    /**
     * Dispatch a job onto a named queue.
     *
     * @typeParam T - Payload type.
     * @param queueName - BullMQ queue name (registered with
     *   `BullModule.registerQueue(...)`).
     * @param jobName   - Application-level job name.
     * @param data      - Job payload.
     * @param options   - BullMQ job options. Merged with sensible
     *   defaults — pass overrides in here.
     * @returns The dispatched job id.
     *
     * @throws When the named queue is not registered.
     */
    dispatch<T = unknown>(queueName: string, jobName: string, data: T, options?: JobsOptions): Promise<string>;
    /**
     * Schedule a repeatable job.
     *
     * @param queueName - BullMQ queue name.
     * @param jobName   - Application-level job name.
     * @param data      - Job payload.
     * @param pattern   - Cron pattern (e.g. `'0 * * * *'`).
     * @param options   - Additional BullMQ job options.
     * @returns The repeatable job id.
     */
    schedule<T = unknown>(queueName: string, jobName: string, data: T, pattern: string, options?: JobsOptions): Promise<string>;
    /**
     * Remove a previously scheduled repeatable job.
     *
     * @param queueName - BullMQ queue name.
     * @param jobName   - Application-level job name.
     * @param pattern   - Cron pattern used at schedule time.
     */
    removeSchedule(queueName: string, jobName: string, pattern: string): Promise<void>;
    /**
     * List repeatable jobs configured on a queue.
     *
     * @param queueName - BullMQ queue name.
     * @returns Array of repeatable job descriptors.
     */
    getRepeatableJobs(queueName: string): Promise<unknown[]>;
    /**
     * Execute a flow (job dependency graph).
     *
     * @param flow - The flow definition (parent + children).
     * @returns The root job id.
     */
    flow(flow: FlowJob): Promise<string>;
    /**
     * Read the current state of a job.
     *
     * @param queueName - BullMQ queue name.
     * @param jobId     - Job id returned by `dispatch()` / `schedule()`.
     * @returns Status object, or `null` if the job is unknown.
     */
    getStatus(queueName: string, jobId: string): Promise<{
        id: string;
        name: string;
        state: string;
        progress: number | string | object | boolean;
        result: unknown;
        failedReason?: string;
        timestamp: number;
        processedOn?: number;
        finishedOn?: number;
        attemptsMade: number;
    } | null>;
    /**
     * Resolve a `Queue` instance by name. Cached for subsequent calls.
     *
     * @param name - Queue name.
     * @returns The resolved BullMQ `Queue`.
     * @throws When the queue is not registered with `BullModule`.
     */
    private resolveQueue;
}

/**
 * BullMQ connector.
 *
 * Resolves an `IBullMQQueueConnectionConfig` into a live
 * `BullMQConnection`. Registered with `QueueModule.forRoot()` (via the
 * NestJS variant of the queue module) so the cross-stack
 * `IQueueService` surface — `manager.connection("bullmq")` —
 * transparently returns a BullMQ-backed connection on the server side.
 *
 * @module @stackra/nestjs-queue/connectors/bullmq.connector
 */

/**
 * BullMQ connector — wraps `BullMQConnection`.
 */
declare class BullMQConnector implements IQueueConnector {
    /**
     * Build a `BullMQConnection` from the supplied configuration.
     *
     * @param config - Driver-specific connection configuration.
     * @returns A ready-to-use BullMQ-backed connection.
     */
    connect(config: QueueConnectionConfig): Promise<IQueueConnection>;
}

/**
 * BullMQ-backed queue connection.
 *
 * Adapts `@nestjs/bullmq` to the `IQueueConnection` contract from
 * `@stackra/contracts`. Lets server-side code reuse the same
 * cross-stack queue surface that the browser package
 * (`@stackra/ts-queue`) exposes — consumers can write code against
 * `IQueueConnection` and choose the driver per environment.
 *
 * This connection is producer-focused: dispatching, scheduling,
 * inspecting, pausing, and clearing queues. Worker-side processing
 * is handled by NestJS Bull's own `@Processor` decorators on classes
 * that extend `WorkerHost` from `@nestjs/bullmq` — those are not
 * managed through this adapter.
 *
 * @module @stackra/nestjs-queue/connections/bullmq.connection
 */

/**
 * BullMQ-backed `IQueueConnection`.
 *
 * Wraps a single BullMQ `Queue` per logical queue name. Multiple
 * queues are managed by the connection's internal `Map<name, Queue>`.
 */
declare class BullMQConnection implements IQueueConnection {
    readonly name: string;
    private readonly connectionOptions;
    private readonly defaultQueueName;
    private readonly sharedClient?;
    /** Scoped logger. */
    private readonly logger;
    /** Open `Queue` instances keyed by queue tube name. */
    private readonly queues;
    /** Open `QueueEvents` instances for state introspection. */
    private readonly queueEvents;
    /**
     * @param name              - Connection name from queue config.
     * @param connectionOptions - Underlying ioredis connection options
     *   forwarded to every BullMQ `Queue` instance.
     * @param defaultQueueName  - Queue created eagerly on first access
     *   when no name is supplied.
     * @param sharedClient      - Optional shared ioredis client. When
     *   provided, BullMQ reuses it instead of opening new connections.
     */
    constructor(name: string, connectionOptions: ConnectionOptions | undefined, defaultQueueName?: string, sharedClient?: RedisClient | undefined);
    /** @inheritdoc */
    push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string>;
    /** @inheritdoc */
    later<T = unknown>(delayMs: number, name: string, data: T, options?: IJobOptions): Promise<string>;
    /** @inheritdoc */
    bulk<T = unknown>(jobs: Array<{
        name: string;
        data: T;
        options?: IJobOptions;
    }>): Promise<string[]>;
    /**
     * BullMQ pushes work to its own workers — there is no external
     * `pop()`. Returns `null` so consumer-side code that only needs to
     * dispatch jobs continues to work, while worker code goes through
     * NestJS Bull's `@Processor` mechanism.
     *
     * @inheritdoc
     */
    pop(_queue?: string): Promise<IQueuedJob | null>;
    /** @inheritdoc */
    size(queue?: string): Promise<number>;
    /** @inheritdoc */
    pendingSize(queue?: string): Promise<number>;
    /** @inheritdoc */
    delayedSize(queue?: string): Promise<number>;
    /** @inheritdoc */
    reservedSize(queue?: string): Promise<number>;
    /** @inheritdoc */
    remove(jobId: string): Promise<void>;
    /** @inheritdoc */
    release(jobId: string, delayMs?: number): Promise<void>;
    /** @inheritdoc */
    fail(jobId: string, reason: string): Promise<void>;
    /** @inheritdoc */
    clear(queue?: string): Promise<void>;
    /** @inheritdoc */
    pause(queue?: string): Promise<void>;
    /** @inheritdoc */
    resume(queue?: string): Promise<void>;
    /** @inheritdoc */
    isPaused(queue?: string): Promise<boolean>;
    /**
     * Close every cached `Queue` and `QueueEvents` handle.
     *
     * @inheritdoc
     */
    close(): Promise<void>;
    /**
     * Get (or eagerly create) the underlying BullMQ `Queue` for a tube.
     *
     * @param name - Queue tube name.
     * @returns Active BullMQ `Queue`.
     */
    getQueue(name?: string): Queue;
    /**
     * Get (or eagerly create) the BullMQ `QueueEvents` listener for
     * advanced introspection (job state, progress, completion).
     *
     * @param name - Queue tube name.
     * @returns Active BullMQ `QueueEvents`.
     */
    getQueueEvents(name?: string): QueueEvents;
    /**
     * BullMQ statuses → our `JobStatus` enum, narrowed to the fields we
     * expose. Useful when adapting a BullMQ Job into an `IQueuedJob`.
     *
     * @param state - Native BullMQ job state.
     * @returns Mapped `JobStatus`.
     */
    static mapStatus(state: 'completed' | 'failed' | 'delayed' | 'active' | 'waiting' | 'paused' | 'unknown'): JobStatus;
    /**
     * Translate our portable `IJobOptions` into BullMQ's `JobsOptions`.
     *
     * Unsupported fields (`uniqueFor`, `tags`, `failOnTimeout`, ...) are
     * ignored — consumers that need them should fall back to BullMQ
     * primitives directly via {@link getQueue}.
     */
    private toJobsOptions;
    /**
     * Read job counts from BullMQ. Defaults to the configured default
     * queue when no name is supplied.
     */
    private getCounts;
}

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

/**
 * Token for the raw `BullRootModuleOptions` payload supplied to
 * `BullModule.forRoot()`. Module-internal — other packages must not
 * import this token.
 */
declare const NESTJS_QUEUE_MODULE_OPTIONS: unique symbol;

export { BullMQConnection, BullMQConnector, type INestjsQueueModuleOptions, type IQueueModuleAsyncOptions, NESTJS_QUEUE_MODULE_OPTIONS, QueueModule, QueueService };
