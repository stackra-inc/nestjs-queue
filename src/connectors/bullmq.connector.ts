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

import { Injectable } from '@nestjs/common';
import type { IQueueConnection, IQueueConnector, QueueConnectionConfig } from '@stackra/contracts';

import { BullMQConnection } from '@/connections/bullmq.connection';

/**
 * BullMQ connector — wraps `BullMQConnection`.
 */
@Injectable()
export class BullMQConnector implements IQueueConnector {
  /**
   * Build a `BullMQConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use BullMQ-backed connection.
   */
  public async connect(config: QueueConnectionConfig): Promise<IQueueConnection> {
    if (config.driver !== 'bullmq') {
      throw new Error(`BullMQConnector received non-bullmq driver: ${config.driver}`);
    }

    const name = config.name ?? 'bullmq';
    const opts = (config.options ?? {}) as { connection?: unknown; url?: string };
    const connection = (opts.connection ?? opts.url) as never;

    return new BullMQConnection(name, connection, config.defaultQueueName ?? 'default');
  }
}
