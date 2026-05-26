'use strict';

var common = require('@nestjs/common');
var bullmq = require('@nestjs/bullmq');
var bullmq$1 = require('bullmq');
var contracts = require('@stackra/contracts');

var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __decorateClass = (decorators, target, key, kind) => {
  var result = kind > 1 ? void 0 : kind ? __getOwnPropDesc(target, key) : target;
  for (var i = decorators.length - 1, decorator; i >= 0; i--)
    if (decorator = decorators[i])
      result = (decorator(result)) || result;
  return result;
};
var BullMQConnection = class _BullMQConnection {
  /**
   * @param name              - Connection name from queue config.
   * @param connectionOptions - Underlying ioredis connection options
   *   forwarded to every BullMQ `Queue` instance.
   * @param defaultQueueName  - Queue created eagerly on first access
   *   when no name is supplied.
   * @param sharedClient      - Optional shared ioredis client. When
   *   provided, BullMQ reuses it instead of opening new connections.
   */
  constructor(name, connectionOptions, defaultQueueName = "default", sharedClient) {
    this.name = name;
    this.connectionOptions = connectionOptions;
    this.defaultQueueName = defaultQueueName;
    this.sharedClient = sharedClient;
  }
  name;
  connectionOptions;
  defaultQueueName;
  sharedClient;
  /** Scoped logger. */
  logger = new common.Logger(_BullMQConnection.name);
  /** Open `Queue` instances keyed by queue tube name. */
  queues = /* @__PURE__ */ new Map();
  /** Open `QueueEvents` instances for state introspection. */
  queueEvents = /* @__PURE__ */ new Map();
  // ────────────────────────────────────────────────────────────────────
  // Producer API
  // ────────────────────────────────────────────────────────────────────
  /** @inheritdoc */
  async push(name, data, options) {
    const queueName = options?.queue ?? this.defaultQueueName;
    const queue = this.getQueue(queueName);
    const job = await queue.add(name, data, this.toJobsOptions(options));
    return job.id ?? "";
  }
  /** @inheritdoc */
  async later(delayMs, name, data, options) {
    return this.push(name, data, { ...options, delayMs });
  }
  /** @inheritdoc */
  async bulk(jobs) {
    const groups = /* @__PURE__ */ new Map();
    for (const job of jobs) {
      const queueName = job.options?.queue ?? this.defaultQueueName;
      const arr = groups.get(queueName) ?? [];
      arr.push({ name: job.name, data: job.data, opts: this.toJobsOptions(job.options) });
      groups.set(queueName, arr);
    }
    const ids = [];
    for (const [queueName, group] of groups) {
      const queue = this.getQueue(queueName);
      const added = await queue.addBulk(group);
      for (const job of added) {
        ids.push(job.id ?? "");
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
  async pop(_queue) {
    return null;
  }
  // ────────────────────────────────────────────────────────────────────
  // Counting / introspection
  // ────────────────────────────────────────────────────────────────────
  /** @inheritdoc */
  async size(queue) {
    const counts = await this.getCounts(queue);
    return (counts.waiting ?? 0) + (counts.delayed ?? 0) + (counts.active ?? 0) + (counts.paused ?? 0);
  }
  /** @inheritdoc */
  async pendingSize(queue) {
    return (await this.getCounts(queue)).waiting ?? 0;
  }
  /** @inheritdoc */
  async delayedSize(queue) {
    return (await this.getCounts(queue)).delayed ?? 0;
  }
  /** @inheritdoc */
  async reservedSize(queue) {
    return (await this.getCounts(queue)).active ?? 0;
  }
  // ────────────────────────────────────────────────────────────────────
  // Mutation
  // ────────────────────────────────────────────────────────────────────
  /** @inheritdoc */
  async remove(jobId) {
    for (const queue of this.queues.values()) {
      const job = await queue.getJob(jobId);
      if (job) {
        await job.remove();
        return;
      }
    }
  }
  /** @inheritdoc */
  async release(jobId, delayMs = 0) {
    for (const queue of this.queues.values()) {
      const job = await queue.getJob(jobId);
      if (!job) continue;
      try {
        if (delayMs > 0) {
          await job.changeDelay(delayMs);
        } else {
          await job.promote();
        }
      } catch (err) {
        this.logger.warn(
          `[BullMQConnection:${this.name}] Failed to release job ${jobId}: ${err.message}`
        );
      }
      return;
    }
  }
  /** @inheritdoc */
  async fail(jobId, reason) {
    for (const queue of this.queues.values()) {
      const job = await queue.getJob(jobId);
      if (!job) continue;
      try {
        await job.moveToFailed(new Error(reason), "manual", false);
      } catch (err) {
        this.logger.warn(
          `[BullMQConnection:${this.name}] Failed to mark job ${jobId} as failed: ${err.message}`
        );
      }
      return;
    }
  }
  /** @inheritdoc */
  async clear(queue = this.defaultQueueName) {
    const q = this.getQueue(queue);
    await q.drain(true);
    await q.clean(0, Number.MAX_SAFE_INTEGER, "completed");
    await q.clean(0, Number.MAX_SAFE_INTEGER, "failed");
    await q.clean(0, Number.MAX_SAFE_INTEGER, "delayed");
    await q.clean(0, Number.MAX_SAFE_INTEGER, "wait");
    await q.clean(0, Number.MAX_SAFE_INTEGER, "active");
  }
  /** @inheritdoc */
  async pause(queue = this.defaultQueueName) {
    await this.getQueue(queue).pause();
  }
  /** @inheritdoc */
  async resume(queue = this.defaultQueueName) {
    await this.getQueue(queue).resume();
  }
  /** @inheritdoc */
  async isPaused(queue = this.defaultQueueName) {
    return this.getQueue(queue).isPaused();
  }
  /**
   * Close every cached `Queue` and `QueueEvents` handle.
   *
   * @inheritdoc
   */
  async close() {
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
  getQueue(name = this.defaultQueueName) {
    let queue = this.queues.get(name);
    if (!queue) {
      const connection = this.sharedClient ?? this.connectionOptions;
      queue = new bullmq$1.Queue(name, { connection });
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
  getQueueEvents(name = this.defaultQueueName) {
    let events = this.queueEvents.get(name);
    if (!events) {
      const connection = this.sharedClient ?? this.connectionOptions;
      events = new bullmq$1.QueueEvents(name, { connection });
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
  static mapStatus(state) {
    switch (state) {
      case "completed":
        return contracts.JobStatus.Completed;
      case "failed":
        return contracts.JobStatus.Failed;
      case "delayed":
        return contracts.JobStatus.Delayed;
      case "active":
        return contracts.JobStatus.Reserved;
      case "paused":
      case "waiting":
      case "unknown":
      default:
        return contracts.JobStatus.Pending;
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
  toJobsOptions(options) {
    if (!options) return {};
    const opts = {};
    if (options.delayMs !== void 0) opts.delay = options.delayMs;
    if (options.tries !== void 0) opts.attempts = options.tries;
    if (options.backoffMs !== void 0) {
      opts.backoff = { type: "exponential", delay: options.backoffMs };
    }
    if (options.uniqueId !== void 0) opts.jobId = options.uniqueId;
    if (options.driverOptions) {
      Object.assign(opts, options.driverOptions);
    }
    return opts;
  }
  /**
   * Read job counts from BullMQ. Defaults to the configured default
   * queue when no name is supplied.
   */
  async getCounts(queue) {
    const q = this.getQueue(queue ?? this.defaultQueueName);
    return q.getJobCounts("waiting", "active", "delayed", "completed", "failed", "paused");
  }
};

// src/connectors/bullmq.connector.ts
exports.BullMQConnector = class BullMQConnector {
  /**
   * Build a `BullMQConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use BullMQ-backed connection.
   */
  async connect(config) {
    if (config.driver !== "bullmq") {
      throw new Error(`BullMQConnector received non-bullmq driver: ${config.driver}`);
    }
    const name = config.name ?? "bullmq";
    const opts = config.options ?? {};
    const connection = opts.connection ?? opts.url;
    return new BullMQConnection(name, connection, config.defaultQueueName ?? "default");
  }
};
exports.BullMQConnector = __decorateClass([
  common.Injectable()
], exports.BullMQConnector);
var NESTJS_QUEUE_MODULE_OPTIONS = /* @__PURE__ */ Symbol.for("NESTJS_QUEUE_MODULE_OPTIONS");
var DEFAULT_JOB_OPTIONS = {
  attempts: 3,
  backoff: { type: "exponential", delay: 5e3 },
  removeOnComplete: { age: 86400, count: 100 },
  removeOnFail: { age: 604800, count: 500 }
};
exports.QueueService = class QueueService {
  /**
   * @param moduleRef - NestJS `ModuleRef` for token-based lookup.
   */
  constructor(moduleRef) {
    this.moduleRef = moduleRef;
  }
  moduleRef;
  /** Scoped logger. */
  logger = new common.Logger(exports.QueueService.name);
  /** Cached `Queue` instances keyed by name. */
  queues = /* @__PURE__ */ new Map();
  /** Lazy `FlowProducer` reusing the first resolved queue's connection. */
  flowProducer = null;
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
  async dispatch(queueName, jobName, data, options) {
    const queue = this.resolveQueue(queueName);
    const job = await queue.add(jobName, data, { ...DEFAULT_JOB_OPTIONS, ...options });
    this.logger.log(`Job dispatched: ${job.id} \u2192 ${queueName}:${jobName}`);
    return job.id ?? "";
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
  async schedule(queueName, jobName, data, pattern, options) {
    const queue = this.resolveQueue(queueName);
    const job = await queue.add(jobName, data, { ...options, repeat: { pattern } });
    this.logger.log(`Repeatable job scheduled: ${job.id} \u2192 ${queueName}:${jobName} (${pattern})`);
    return job.id ?? "";
  }
  /**
   * Remove a previously scheduled repeatable job.
   *
   * @param queueName - BullMQ queue name.
   * @param jobName   - Application-level job name.
   * @param pattern   - Cron pattern used at schedule time.
   */
  async removeSchedule(queueName, jobName, pattern) {
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
  async getRepeatableJobs(queueName) {
    const queue = this.resolveQueue(queueName);
    return queue.getRepeatableJobs();
  }
  /**
   * Execute a flow (job dependency graph).
   *
   * @param flow - The flow definition (parent + children).
   * @returns The root job id.
   */
  async flow(flow) {
    if (!this.flowProducer) {
      const queue = this.resolveQueue(flow.queueName);
      const opts = queue.opts;
      this.flowProducer = new bullmq$1.FlowProducer({
        connection: opts?.connection
      });
    }
    const result = await this.flowProducer.add(flow);
    this.logger.log(`Flow dispatched: ${result.job.id} \u2192 ${flow.queueName}:${flow.name}`);
    return result.job.id ?? "";
  }
  /**
   * Read the current state of a job.
   *
   * @param queueName - BullMQ queue name.
   * @param jobId     - Job id returned by `dispatch()` / `schedule()`.
   * @returns Status object, or `null` if the job is unknown.
   */
  async getStatus(queueName, jobId) {
    const queue = this.resolveQueue(queueName);
    const job = await queue.getJob(jobId);
    if (!job) return null;
    const state = await job.getState();
    return {
      id: job.id ?? "",
      name: job.name,
      state,
      progress: job.progress,
      result: job.returnvalue,
      ...job.failedReason !== void 0 ? { failedReason: job.failedReason } : {},
      timestamp: job.timestamp,
      ...job.processedOn !== void 0 ? { processedOn: job.processedOn } : {},
      ...job.finishedOn !== void 0 ? { finishedOn: job.finishedOn } : {},
      attemptsMade: job.attemptsMade
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
  resolveQueue(name) {
    let queue = this.queues.get(name);
    if (queue) return queue;
    try {
      const token = bullmq.getQueueToken(name);
      queue = this.moduleRef.get(token, { strict: false });
    } catch {
      throw new Error(
        `Queue "${name}" not found. Register it with QueueModule.forFeature(['${name}']).`
      );
    }
    this.queues.set(name, queue);
    return queue;
  }
};
exports.QueueService = __decorateClass([
  common.Injectable()
], exports.QueueService);

// src/queue.module.ts
exports.QueueModule = class QueueModule {
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
  static forRoot(options) {
    return {
      module: exports.QueueModule,
      global: true,
      imports: [bullmq.BullModule.forRoot(options)],
      providers: [
        { provide: NESTJS_QUEUE_MODULE_OPTIONS, useValue: options },
        exports.QueueService,
        exports.BullMQConnector
      ],
      exports: [bullmq.BullModule, exports.QueueService, exports.BullMQConnector, NESTJS_QUEUE_MODULE_OPTIONS]
    };
  }
  /**
   * Async variant of `forRoot()` for DI-driven configuration.
   *
   * @param options - Async options carrying `useFactory` / `inject`.
   * @returns A global NestJS dynamic module.
   */
  static forRootAsync(options) {
    return {
      module: exports.QueueModule,
      global: true,
      imports: [
        ...options.imports ?? [],
        bullmq.BullModule.forRootAsync({
          imports: options.imports ?? [],
          useFactory: options.useFactory,
          inject: options.inject ?? []
        })
      ],
      providers: [
        {
          provide: NESTJS_QUEUE_MODULE_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject ?? []
        },
        exports.QueueService,
        exports.BullMQConnector
      ],
      exports: [bullmq.BullModule, exports.QueueService, exports.BullMQConnector, NESTJS_QUEUE_MODULE_OPTIONS]
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
  static forFeature(queues) {
    const queueConfigs = queues.map((name) => ({ name }));
    return {
      module: exports.QueueModule,
      imports: [bullmq.BullModule.registerQueue(...queueConfigs)],
      exports: [bullmq.BullModule]
    };
  }
};
exports.QueueModule = __decorateClass([
  common.Module({})
], exports.QueueModule);

Object.defineProperty(exports, "InjectQueue", {
  enumerable: true,
  get: function () { return bullmq.InjectQueue; }
});
Object.defineProperty(exports, "OnWorkerEvent", {
  enumerable: true,
  get: function () { return bullmq.OnWorkerEvent; }
});
Object.defineProperty(exports, "Processor", {
  enumerable: true,
  get: function () { return bullmq.Processor; }
});
Object.defineProperty(exports, "WorkerHost", {
  enumerable: true,
  get: function () { return bullmq.WorkerHost; }
});
Object.defineProperty(exports, "DEFAULT_QUEUE_CONNECTION_TOKEN", {
  enumerable: true,
  get: function () { return contracts.DEFAULT_QUEUE_CONNECTION_TOKEN; }
});
Object.defineProperty(exports, "QUEUE_CONFIG", {
  enumerable: true,
  get: function () { return contracts.QUEUE_CONFIG; }
});
Object.defineProperty(exports, "QUEUE_MANAGER", {
  enumerable: true,
  get: function () { return contracts.QUEUE_MANAGER; }
});
exports.BullMQConnection = BullMQConnection;
exports.NESTJS_QUEUE_MODULE_OPTIONS = NESTJS_QUEUE_MODULE_OPTIONS;
//# sourceMappingURL=index.cjs.map
//# sourceMappingURL=index.cjs.map