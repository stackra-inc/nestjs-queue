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

import { Logger } from '@nestjs/common';
import {
  Queue,
  QueueEvents,
  type ConnectionOptions,
  type JobsOptions,
  type RedisClient,
} from 'bullmq';
import type { IJobOptions, IQueueConnection, IQueuedJob } from '@stackra/contracts';
import { JobStatus } from '@stackra/contracts';

/**
 * BullMQ-backed `IQueueConnection`.
 *
 * Wraps a single BullMQ `Queue` per logical queue name. Multiple
 * queues are managed by the connection's internal `Map<name, Queue>`.
 */
export class BullMQConnection implements IQueueConnection {
  /** Scoped logger. */
  private readonly logger = new Logger(BullMQConnection.name);

  /** Open `Queue` instances keyed by queue tube name. */
  private readonly queues: Map<string, Queue> = new Map();

  /** Open `QueueEvents` instances for state introspection. */
  private readonly queueEvents: Map<string, QueueEvents> = new Map();

  /**
   * @param name              - Connection name from queue config.
   * @param connectionOptions - Underlying ioredis connection options
   *   forwarded to every BullMQ `Queue` instance.
   * @param defaultQueueName  - Queue created eagerly on first access
   *   when no name is supplied.
   * @param sharedClient      - Optional shared ioredis client. When
   *   provided, BullMQ reuses it instead of opening new connections.
   */
  public constructor(
    public readonly name: string,
    private readonly connectionOptions: ConnectionOptions | undefined,
    private readonly defaultQueueName: string = 'default',
    private readonly sharedClient?: RedisClient
  ) {}

  // ────────────────────────────────────────────────────────────────────
  // Producer API
  // ────────────────────────────────────────────────────────────────────

  /** @inheritdoc */
  public async push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string> {
    const queueName = options?.queue ?? this.defaultQueueName;
    const queue = this.getQueue(queueName);
    const job = await queue.add(name, data, this.toJobsOptions(options));
    return job.id ?? '';
  }

  /** @inheritdoc */
  public async later<T = unknown>(
    delayMs: number,
    name: string,
    data: T,
    options?: IJobOptions
  ): Promise<string> {
    return this.push(name, data, { ...options, delayMs });
  }

  /** @inheritdoc */
  public async bulk<T = unknown>(
    jobs: Array<{ name: string; data: T; options?: IJobOptions }>
  ): Promise<string[]> {
    const groups = new Map<string, Array<{ name: string; data: T; opts: JobsOptions }>>();
    for (const job of jobs) {
      const queueName = job.options?.queue ?? this.defaultQueueName;
      const arr = groups.get(queueName) ?? [];
      arr.push({ name: job.name, data: job.data, opts: this.toJobsOptions(job.options) });
      groups.set(queueName, arr);
    }

    const ids: string[] = [];
    for (const [queueName, group] of groups) {
      const queue = this.getQueue(queueName);
      const added = await queue.addBulk(group);
      for (const job of added) {
        ids.push(job.id ?? '');
      }
    }
    return ids;
  }

  /**
   * BullMQ pushes work to its own workers — there is no external
   * `pop()`. Returns `null` so consumer-side code that only needs to
   * dispatch jobs continues to work, while worker code goes through
   * NestJS Bull's `@Processor` mechanism.
   *
   * @inheritdoc
   */
  public async pop(_queue?: string): Promise<IQueuedJob | null> {
    return null;
  }

  // ────────────────────────────────────────────────────────────────────
  // Counting / introspection
  // ────────────────────────────────────────────────────────────────────

  /** @inheritdoc */
  public async size(queue?: string): Promise<number> {
    const counts = await this.getCounts(queue);
    return (
      (counts.waiting ?? 0) + (counts.delayed ?? 0) + (counts.active ?? 0) + (counts.paused ?? 0)
    );
  }

  /** @inheritdoc */
  public async pendingSize(queue?: string): Promise<number> {
    return (await this.getCounts(queue)).waiting ?? 0;
  }

  /** @inheritdoc */
  public async delayedSize(queue?: string): Promise<number> {
    return (await this.getCounts(queue)).delayed ?? 0;
  }

  /** @inheritdoc */
  public async reservedSize(queue?: string): Promise<number> {
    return (await this.getCounts(queue)).active ?? 0;
  }

  // ────────────────────────────────────────────────────────────────────
  // Mutation
  // ────────────────────────────────────────────────────────────────────

  /** @inheritdoc */
  public async remove(jobId: string): Promise<void> {
    for (const queue of this.queues.values()) {
      const job = await queue.getJob(jobId);
      if (job) {
        await job.remove();
        return;
      }
    }
  }

  /** @inheritdoc */
  public async release(jobId: string, delayMs: number = 0): Promise<void> {
    for (const queue of this.queues.values()) {
      const job = await queue.getJob(jobId);
      if (!job) continue;

      // Promote to available state. BullMQ doesn't expose an explicit
      // "release reserved job" — equivalent is reschedule with delay.
      try {
        if (delayMs > 0) {
          await job.changeDelay(delayMs);
        } else {
          await job.promote();
        }
      } catch (err: Error | any) {
        this.logger.warn(
          `[BullMQConnection:${this.name}] Failed to release job ${jobId}: ${(err as Error).message}`
        );
      }
      return;
    }
  }

  /** @inheritdoc */
  public async fail(jobId: string, reason: string): Promise<void> {
    for (const queue of this.queues.values()) {
      const job = await queue.getJob(jobId);
      if (!job) continue;

      try {
        await job.moveToFailed(new Error(reason), 'manual', false);
      } catch (err: Error | any) {
        this.logger.warn(
          `[BullMQConnection:${this.name}] Failed to mark job ${jobId} as failed: ${(err as Error).message}`
        );
      }
      return;
    }
  }

  /** @inheritdoc */
  public async clear(queue: string = this.defaultQueueName): Promise<void> {
    const q = this.getQueue(queue);
    await q.drain(true);
    await q.clean(0, Number.MAX_SAFE_INTEGER, 'completed');
    await q.clean(0, Number.MAX_SAFE_INTEGER, 'failed');
    await q.clean(0, Number.MAX_SAFE_INTEGER, 'delayed');
    await q.clean(0, Number.MAX_SAFE_INTEGER, 'wait');
    await q.clean(0, Number.MAX_SAFE_INTEGER, 'active');
  }

  /** @inheritdoc */
  public async pause(queue: string = this.defaultQueueName): Promise<void> {
    await this.getQueue(queue).pause();
  }

  /** @inheritdoc */
  public async resume(queue: string = this.defaultQueueName): Promise<void> {
    await this.getQueue(queue).resume();
  }

  /** @inheritdoc */
  public async isPaused(queue: string = this.defaultQueueName): Promise<boolean> {
    return this.getQueue(queue).isPaused();
  }

  /**
   * Close every cached `Queue` and `QueueEvents` handle.
   *
   * @inheritdoc
   */
  public async close(): Promise<void> {
    await Promise.all(Array.from(this.queues.values()).map((q) => q.close()));
    await Promise.all(Array.from(this.queueEvents.values()).map((q) => q.close()));
    this.queues.clear();
    this.queueEvents.clear();
  }

  // ────────────────────────────────────────────────────────────────────
  // BullMQ-specific helpers
  // ────────────────────────────────────────────────────────────────────

  /**
   * Get (or eagerly create) the underlying BullMQ `Queue` for a tube.
   *
   * @param name - Queue tube name.
   * @returns Active BullMQ `Queue`.
   */
  public getQueue(name: string = this.defaultQueueName): Queue {
    let queue = this.queues.get(name);
    if (!queue) {
      const connection = (this.sharedClient ?? this.connectionOptions) as ConnectionOptions;
      queue = new Queue(name, { connection });
      this.queues.set(name, queue);
    }
    return queue;
  }

  /**
   * Get (or eagerly create) the BullMQ `QueueEvents` listener for
   * advanced introspection (job state, progress, completion).
   *
   * @param name - Queue tube name.
   * @returns Active BullMQ `QueueEvents`.
   */
  public getQueueEvents(name: string = this.defaultQueueName): QueueEvents {
    let events = this.queueEvents.get(name);
    if (!events) {
      const connection = (this.sharedClient ?? this.connectionOptions) as ConnectionOptions;
      events = new QueueEvents(name, { connection });
      this.queueEvents.set(name, events);
    }
    return events;
  }

  /**
   * BullMQ statuses → our `JobStatus` enum, narrowed to the fields we
   * expose. Useful when adapting a BullMQ Job into an `IQueuedJob`.
   *
   * @param state - Native BullMQ job state.
   * @returns Mapped `JobStatus`.
   */
  public static mapStatus(
    state: 'completed' | 'failed' | 'delayed' | 'active' | 'waiting' | 'paused' | 'unknown'
  ): JobStatus {
    switch (state) {
      case 'completed':
        return JobStatus.Completed;
      case 'failed':
        return JobStatus.Failed;
      case 'delayed':
        return JobStatus.Delayed;
      case 'active':
        return JobStatus.Reserved;
      case 'paused':
      case 'waiting':
      case 'unknown':
      default:
        return JobStatus.Pending;
    }
  }

  // ────────────────────────────────────────────────────────────────────
  // Private
  // ────────────────────────────────────────────────────────────────────

  /**
   * Translate our portable `IJobOptions` into BullMQ's `JobsOptions`.
   *
   * Unsupported fields (`uniqueFor`, `tags`, `failOnTimeout`, ...) are
   * ignored — consumers that need them should fall back to BullMQ
   * primitives directly via {@link getQueue}.
   */
  private toJobsOptions(options?: IJobOptions): JobsOptions {
    if (!options) return {};

    const opts: JobsOptions = {};
    if (options.delayMs !== undefined) opts.delay = options.delayMs;
    if (options.tries !== undefined) opts.attempts = options.tries;
    if (options.backoffMs !== undefined) {
      opts.backoff = { type: 'exponential', delay: options.backoffMs };
    }
    if (options.uniqueId !== undefined) opts.jobId = options.uniqueId;

    // Pass-through driver knobs (priority, lifo, ...).
    if (options.driverOptions) {
      Object.assign(opts, options.driverOptions);
    }

    return opts;
  }

  /**
   * Read job counts from BullMQ. Defaults to the configured default
   * queue when no name is supplied.
   */
  private async getCounts(queue?: string): Promise<Record<string, number>> {
    const q = this.getQueue(queue ?? this.defaultQueueName);
    return q.getJobCounts('waiting', 'active', 'delayed', 'completed', 'failed', 'paused');
  }
}
