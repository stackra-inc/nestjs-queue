/**
 * `@stackra/nestjs-queue` — NestJS queue module.
 *
 * Wraps `@nestjs/bullmq` with a stable surface and the cross-stack
 * `IQueueConnector` contract from `@stackra/contracts`. Server-side
 * code can choose between two surfaces:
 *
 * - **`QueueService`** — BullMQ-flavored convenience facade. Use this
 *   for NestJS-only code that wants `dispatch()` / `schedule()` /
 *   `flow()` ergonomics with `JobsOptions` directly.
 * - **`BullMQConnection`** (via `IQueueConnection`) — portable surface
 *   shared with `@stackra/ts-queue`. Use this for cross-stack code
 *   that should also work in the browser.
 *
 * @example Quick start
 * ```typescript
 * @Module({
 *   imports: [
 *     QueueModule.forRoot({
 *       connection: { host: 'localhost', port: 6379, db: 1 },
 *     }),
 *   ],
 * })
 * export class AppModule {}
 *
 * @Module({
 *   imports: [QueueModule.forFeature(['transfer'])],
 *   providers: [TransferService, TransferProcessor],
 * })
 * export class TransferModule {}
 * ```
 *
 * @example Dispatching
 * ```typescript
 * @Injectable()
 * export class TransferService {
 *   public constructor(private readonly queue: QueueService) {}
 *
 *   public async startImport(payload: IImportPayload): Promise<string> {
 *     return this.queue.dispatch('transfer', 'import', payload);
 *   }
 * }
 * ```
 *
 * @example Processing (BullMQ workers)
 * ```typescript
 * @Processor('transfer')
 * export class TransferProcessor extends WorkerHost {
 *   public async process(job: Job<IImportPayload>): Promise<void> {
 *     // handle job
 *   }
 * }
 * ```
 *
 * @module @stackra/nestjs-queue
 */

// ============================================================================
// Module
// ============================================================================
export { QueueModule } from './queue.module';
export type { IQueueModuleAsyncOptions } from './queue.module';

// ============================================================================
// Services
// ============================================================================
export { QueueService } from './services/queue.service';

// ============================================================================
// Connectors (IQueueConnector adapters)
// ============================================================================
export { BullMQConnector } from './connectors/bullmq.connector';

// ============================================================================
// Connections
// ============================================================================
export { BullMQConnection } from './connections/bullmq.connection';

// ============================================================================
// Constants (DI tokens)
// ============================================================================
export {
  QUEUE_CONFIG,
  QUEUE_MANAGER,
  DEFAULT_QUEUE_CONNECTION_TOKEN,
  NESTJS_QUEUE_MODULE_OPTIONS,
} from './constants';

// ============================================================================
// Internal interfaces
// ============================================================================
export type { INestjsQueueModuleOptions } from './interfaces';

// ============================================================================
// Re-exports from @nestjs/bullmq (consumers should never import
// directly so we keep version pinning in one place)
// ============================================================================
export { InjectQueue, Processor, WorkerHost, OnWorkerEvent } from '@nestjs/bullmq';

// ============================================================================
// Re-exports from bullmq (commonly needed types)
// ============================================================================
export type { Job, JobsOptions, Queue, FlowJob, FlowProducer } from 'bullmq';
