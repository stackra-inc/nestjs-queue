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

import { Inject, Injectable, Logger } from '@nestjs/common';
import { ModuleRef, ModulesContainer } from '@nestjs/core';
import { getQueueToken } from '@nestjs/bullmq';
import { FlowProducer, type FlowJob, type JobsOptions, type Queue } from 'bullmq';

/**
 * Default job options applied when consumers do not pass overrides.
 */
const DEFAULT_JOB_OPTIONS: JobsOptions = {
  attempts: 3,
  backoff: { type: 'exponential', delay: 5000 },
  removeOnComplete: { age: 86_400, count: 100 },
  removeOnFail: { age: 604_800, count: 500 },
};

/**
 * BullMQ-flavored convenience service.
 *
 * Resolves named BullMQ `Queue` instances from the DI container at
 * runtime, supports dispatching, scheduling, and flow execution.
 */
@Injectable()
export class QueueService {
  /** Scoped logger. */
  private readonly logger = new Logger(QueueService.name);

  /** Cached `Queue` instances keyed by name. */
  private readonly queues: Map<string, Queue> = new Map();

  /** Lazy `FlowProducer` reusing the first resolved queue's connection. */
  private flowProducer: FlowProducer | null = null;

  /**
   * @param moduleRef - NestJS `ModuleRef` for token-based lookup.
   * @param modulesContainer - NestJS `ModulesContainer` for cross-module lookup.
   */
  public constructor(
    private readonly moduleRef: ModuleRef,
    private readonly modulesContainer: ModulesContainer
  ) {}

  // ────────────────────────────────────────────────────────────────────
  // Producer API
  // ────────────────────────────────────────────────────────────────────

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
  public async dispatch<T = unknown>(
    queueName: string,
    jobName: string,
    data: T,
    options?: JobsOptions
  ): Promise<string> {
    const queue = this.resolveQueue(queueName);
    const job = await queue.add(jobName, data, { ...DEFAULT_JOB_OPTIONS, ...options });

    this.logger.log(`Job dispatched: ${job.id} → ${queueName}:${jobName}`);
    return job.id ?? '';
  }

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
  public async schedule<T = unknown>(
    queueName: string,
    jobName: string,
    data: T,
    pattern: string,
    options?: JobsOptions
  ): Promise<string> {
    const queue = this.resolveQueue(queueName);
    const job = await queue.add(jobName, data, { ...options, repeat: { pattern } });

    this.logger.log(`Repeatable job scheduled: ${job.id} → ${queueName}:${jobName} (${pattern})`);
    return job.id ?? '';
  }

  /**
   * Remove a previously scheduled repeatable job.
   *
   * @param queueName - BullMQ queue name.
   * @param jobName   - Application-level job name.
   * @param pattern   - Cron pattern used at schedule time.
   */
  public async removeSchedule(queueName: string, jobName: string, pattern: string): Promise<void> {
    const queue = this.resolveQueue(queueName);
    await queue.removeRepeatable(jobName, { pattern });
    this.logger.log(`Repeatable job removed: ${queueName}:${jobName} (${pattern})`);
  }

  /**
   * List repeatable jobs configured on a queue.
   *
   * @param queueName - BullMQ queue name.
   * @returns Array of repeatable job descriptors.
   */
  public async getRepeatableJobs(queueName: string): Promise<unknown[]> {
    const queue = this.resolveQueue(queueName);
    return queue.getRepeatableJobs();
  }

  /**
   * Execute a flow (job dependency graph).
   *
   * @param flow - The flow definition (parent + children).
   * @returns The root job id.
   */
  public async flow(flow: FlowJob): Promise<string> {
    if (!this.flowProducer) {
      const queue = this.resolveQueue(flow.queueName);
      const opts = (queue as unknown as { opts: { connection: unknown } }).opts;
      this.flowProducer = new FlowProducer({
        connection: opts?.connection as never,
      });
    }

    const result = await this.flowProducer.add(flow);
    this.logger.log(`Flow dispatched: ${result.job.id} → ${flow.queueName}:${flow.name}`);
    return result.job.id ?? '';
  }

  /**
   * Read the current state of a job.
   *
   * @param queueName - BullMQ queue name.
   * @param jobId     - Job id returned by `dispatch()` / `schedule()`.
   * @returns Status object, or `null` if the job is unknown.
   */
  public async getStatus(
    queueName: string,
    jobId: string
  ): Promise<{
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
  } | null> {
    const queue = this.resolveQueue(queueName);
    const job = await queue.getJob(jobId);
    if (!job) return null;

    const state = await job.getState();

    return {
      id: job.id ?? '',
      name: job.name,
      state,
      progress: job.progress,
      result: job.returnvalue,
      ...(job.failedReason !== undefined ? { failedReason: job.failedReason } : {}),
      timestamp: job.timestamp,
      ...(job.processedOn !== undefined ? { processedOn: job.processedOn } : {}),
      ...(job.finishedOn !== undefined ? { finishedOn: job.finishedOn } : {}),
      attemptsMade: job.attemptsMade,
    };
  }

  // ────────────────────────────────────────────────────────────────────
  // Internal
  // ────────────────────────────────────────────────────────────────────

  /**
   * Resolve a `Queue` instance by name. Cached for subsequent calls.
   *
   * @param name - Queue name.
   * @returns The resolved BullMQ `Queue`.
   * @throws When the queue is not registered with `BullModule`.
   */
  private resolveQueue(name: string): Queue {
    let queue = this.queues.get(name);
    if (queue) return queue;

    const token = getQueueToken(name);

    // First try moduleRef (works if queue is in the same module scope)
    try {
      queue = this.moduleRef.get<Queue>(token, { strict: false });
      this.queues.set(name, queue);
      return queue;
    } catch {
      // Fall through to cross-module search
    }

    // Walk all modules to find the queue token
    for (const moduleRef of this.modulesContainer.values()) {
      try {
        const provider = moduleRef.providers.get(token);
        if (provider?.instance) {
          queue = provider.instance as Queue;
          this.queues.set(name, queue);
          return queue;
        }
      } catch {
        continue;
      }
    }

    throw new Error(
      `Queue "${name}" not found. Register it with QueueModule.forFeature(['${name}']).`
    );
  }
}
