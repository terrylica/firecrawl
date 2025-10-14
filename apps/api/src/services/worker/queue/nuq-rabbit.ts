import {
  Channel,
  ConfirmChannel,
  ChannelModel,
  connect,
  ConsumeMessage,
} from "amqplib";
import { ScrapeJobData } from "../../../types";
import { Document } from "../../../controllers/v1/types";
import { NuQJob } from "../nuq";
import { redisCacheConnection } from "../../redis";
import { getJobFromGCS, removeJobFromGCS } from "../../../lib/gcs-jobs";
import { logger as _logger } from "../../../lib/logger";
import { Logger } from "winston";

const NUQ_RABBIT_MAX_SIZE = 1024 * 1024;
const NUQ_REDIS_MAX_SIZE = 1024 * 1024 * 2;

export const NUQ_GCS_WAIT_SIZE = Math.max(
  NUQ_RABBIT_MAX_SIZE,
  NUQ_REDIS_MAX_SIZE,
);

const NUQ_RABBIT_MAX_RETRIES = 10;
const NUQ_RABBIT_RETRY_BACKOFF = 1000;

const FAILED_JOB_JSON = JSON.stringify({ status: "failed" });

const clientId = process.env.NUQ_POD_NAME ?? "main";

export type NuQRabbitJobResult = {
  status: "completed" | "failed";
  result?: Document | null;
  failedReason?: any;
};

interface NuQRabbitJob extends NuQJob<ScrapeJobData, Document | null> {
  run: (
    fn: (
      job: NuQJob<ScrapeJobData, Document | null>,
    ) => Promise<NuQRabbitJobResult>,
  ) => Promise<void>;
}

// NOTE: only supports 'scrape' jobs right now
export class NuQRabbit {
  private connection!: ChannelModel;
  private publisher!: ConfirmChannel;
  private consumer!: Channel;

  private statusConsumer!: ConfirmChannel;
  private statusConsumerPromise: Promise<void> | null = null;
  private statusConsumerRunning = false;

  private reconnectTimer: NodeJS.Timeout | null = null;
  private pendingMessages: Array<() => void> = [];
  private reconnecting = false;
  private stopping = false;

  private listeners = new Map<
    string,
    ((result: NuQRabbitJobResult) => Promise<void>)[]
  >();

  public constructor(
    private readonly queue: string = "nuq.v2.scrape_queue",
    private readonly retryQueue: string = "nuq.v2.scrape_queue.retry.30s",
    private readonly deadLetterQueue: string = "nuq.v2.scrape_queue.dead",
    private readonly replyQueue: string = `nuq.v2.scrape_queue.reply.${clientId}`,
  ) {}

  async start() {
    this.stopping = false;
    await this.connect();
  }

  async stop() {
    this.stopping = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    await Promise.all([
      this.publisher?.close(),
      this.consumer?.close(),
      this.statusConsumer?.close(),
    ]);
    await this.connection?.close();
  }

  private reconnect() {
    if (this.stopping) return;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);

    this.connection = undefined as any;
    this.publisher = undefined as any;
    this.consumer = undefined as any;
    this.statusConsumer = undefined as any;
    this.reconnectTimer = setTimeout(() => this.start(), 500);
  }

  // TODO: handle reconnection on close
  private async connect() {
    if (!process.env.NUQ_RABBITMQ_URL) {
      throw new Error("NuQ RabbitMQ URL not set");
    }

    if (this.reconnecting) return;
    this.reconnecting = true;

    const logger = _logger.child({
      module: "nuq-rabbit",
    });

    let backoff = 500;

    while (!this.stopping) {
      try {
        this.connection = await connect(process.env.NUQ_RABBITMQ_URL!);
        this.connection.on("close", () => {
          logger.info("NuQ RabbitMQ connection closed");
          this.reconnect();
        });
        this.connection.on("error", error =>
          logger.error("NuQ RabbitMQ connection error", { error }),
        );
        this.connection.on("blocked", () =>
          logger.warn("NuQ RabbitMQ connection blocked"),
        );
        this.connection.on("unblocked", () =>
          logger.info("NuQ RabbitMQ connection unblocked"),
        );

        this.publisher = await this.connection.createConfirmChannel();
        this.publisher.on("close", () => this.reconnect());
        this.publisher.on("return", msg =>
          logger.warn("NuQ RabbitMQ message returned", { msg }),
        );

        this.consumer = await this.connection.createChannel();
        this.consumer.on("close", () => this.reconnect());

        await this.consumer.assertQueue(this.queue, {
          durable: true,
          arguments: {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": this.retryQueue,
          },
        });

        await this.consumer.assertQueue(this.retryQueue, {
          durable: true,
          arguments: {
            "x-queue-type": "quorum",
            "x-message-ttl": NUQ_RABBIT_RETRY_BACKOFF, // backoff queue to resubmit to primary queue after a period
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": this.queue,
          },
        });

        // jobs that fail all retries are temporarily stored in the DLQ for debugging
        await this.consumer.assertQueue(this.deadLetterQueue, {
          durable: true,
          arguments: {
            "x-queue-type": "classic",
            "x-queue-mode": "lazy",
            "x-message-ttl": 48 * 60 * 60 * 1000,
            "x-max-length-bytes": 1_000_000_000,
            "x-overflow": "drop-head",
          },
        });

        if (this.statusConsumerRunning) await this.startReplyConsumer();

        const pendingOps = this.pendingMessages.splice(0);
        pendingOps.forEach(fn => fn());
        await this.publisher.waitForConfirms().catch(() => {
          /* no-op */
        });

        this.reconnecting = false;
        logger.info("Connected to NuQ RabbitMQ");
        break;
      } catch (error) {
        logger.error("Error connecting to RabbitMQ", { error });
        await new Promise(resolve => setTimeout(resolve, backoff));
        backoff = Math.min(backoff * 2, 10000);
      }
    }
  }

  async startReplyConsumer() {
    const logger = _logger.child({
      module: "nuq-rabbit",
    });

    if (this.statusConsumerPromise) {
      return this.statusConsumerPromise;
    }

    this.statusConsumerPromise = new Promise(async resolve => {
      this.statusConsumer = await this.connection.createConfirmChannel();

      await this.statusConsumer.assertQueue(this.replyQueue, {
        exclusive: true,
        autoDelete: true,
        durable: false,
        arguments: {
          "x-queue-type": "classic",
          "x-message-ttl": 60000,
        },
      });

      await this.statusConsumer.consume(
        this.replyQueue,
        (msg: ConsumeMessage | null) => {
          if (msg === null) return;

          try {
            const id = msg.properties.correlationId as string;
            const result: NuQRabbitJobResult = JSON.parse(
              msg.content.toString(),
            );

            try {
              const listeners = this.listeners.get(id);
              if (listeners) listeners.forEach(listener => listener(result));
              this.listeners.delete(id);
            } catch (error) {
              logger.error("Error in rabbit queue generator", error);
            }
          } catch (error) {
            logger.error("Error in rabbit queue generator", error);
          }

          this.statusConsumer.ack(msg);
        },
        { noAck: false, consumerTag: `${this.replyQueue}.client` },
      );

      this.statusConsumerRunning = true;
      resolve();
      logger.info("Started reply consumer");
    });
  }

  // TODO: async jobs
  async queueJob(
    job: NuQJob<ScrapeJobData, Document | null>,
    timeoutMs: number | null,
    zeroDataRetention: boolean,
    logger: Logger = _logger,
  ): Promise<NuQRabbitJobResult> {
    if (!this.connection) {
      throw new Error("NuQ RabbitMQ connection not initialized");
    }

    if (!this.statusConsumerRunning) {
      await this.startReplyConsumer();
    }

    const jobId = job.id;

    const p = new Promise<NuQRabbitJobResult>((resolve, reject) => {
      let t: NodeJS.Timeout | null = null;

      if (timeoutMs !== null) {
        t = setTimeout(() => {
          reject(new Error("Timed out waiting for job"));
        }, timeoutMs);
      }

      const cb = async ({
        status,
        result,
        failedReason,
      }: NuQRabbitJobResult) => {
        if (t) clearTimeout(t);

        let document: Document | null = null;
        if (result !== undefined) {
          document = result;
        } else {
          const cached = await redisCacheConnection.getdel(
            "cache:job:" + job.id,
          );

          if (!cached) {
            const docs = await getJobFromGCS(jobId);
            logger.debug("Got job from GCS");
            if (!docs || docs.length === 0) {
              throw new Error("Job not found in GCS");
            }
            document = docs[0]!;

            // TODO: if it's a small document we shouldn't even need to upload to GCS as it won't reach here (with zdr enabled)
            if (zeroDataRetention) {
              await removeJobFromGCS(jobId);
            }
          } else {
            document = JSON.parse(cached);
          }
        }

        if (["completed", "failed"].includes(status)) {
          resolve({ status, result: document, failedReason });
        } else {
          resolve({ status, result: null, failedReason });
        }
      };

      if (this.listeners.has(job.id)) {
        this.listeners.get(job.id)!.push(cb);
      } else {
        this.listeners.set(job.id, [cb]);
      }
    });

    await this.publish(job, true);
    return p;
  }

  private async publish(job: NuQJob<ScrapeJobData, any>, rpc = false) {
    if (!this.connection) {
      throw new Error("NuQ RabbitMQ connection not initialized");
    }

    const sendMessage = () => {
      this.publisher.sendToQueue(this.queue, Buffer.from(JSON.stringify(job)), {
        contentType: "application/json",
        correlationId: job.id,
        replyTo: rpc ? this.replyQueue : undefined,
        persistent: true,
        mandatory: true,
        timestamp: this.timeSeconds(),
      });
    };

    if (this.publisher) {
      sendMessage();
      await this.publisher.waitForConfirms();
    } else {
      this.pendingMessages.push(sendMessage);
    }
  }

  async *subscribe(options: { prefetchCount?: number; signal?: AbortSignal }) {
    if (!this.connection || !this.consumer) {
      throw new Error("NuQ RabbitMQ connection not initialized");
    }

    const logger = _logger.child({
      module: "nuq-rabbit",
      method: "job-queue",
    });

    const { signal, prefetchCount = 1 } = options;

    const consumerTag = `${this.queue}.worker`;

    const buffer: NuQRabbitJob[] = [];
    let isClosed = false;

    const cleanup = () => {
      isClosed = true;
      this.consumer.cancel(consumerTag);
    };

    if (signal) {
      signal.addEventListener("abort", cleanup);
    }

    let pullNext: ((res: IteratorResult<NuQRabbitJob>) => void) | null = null;

    const push = (msg: NuQRabbitJob) => {
      if (isClosed) {
        return;
      }

      if (pullNext) {
        pullNext({ done: false, value: msg });
        pullNext = null;
        return;
      }

      buffer.push(msg);
    };

    await this.consumer.prefetch(prefetchCount);

    await this.consumer.consume(
      this.queue,
      async (msg: ConsumeMessage | null) => {
        if (msg === null) return;

        const jobId = msg.properties.correlationId as string;
        const data = JSON.parse(msg.content.toString("utf8"));

        const deaths = msg.properties.headers?.["x-death"] as
          | Array<any>
          | undefined;
        const attempts = deaths?.find(d => d.queue == this.queue)?.count || 0;

        const job = {
          ...data,
          run: async fn => {
            try {
              const result = await fn(job);

              if (msg.properties.replyTo) {
                let blob = JSON.stringify(result);
                const blobSize = Buffer.byteLength(blob, "utf8");

                // max 1MB to send via rabbit
                if (blobSize > NUQ_RABBIT_MAX_SIZE) {
                  // max 2MB to cache in redis for 30s
                  if (blobSize < NUQ_REDIS_MAX_SIZE) {
                    logger.debug("Caching job in Redis", { jobId });
                    await redisCacheConnection.set(
                      "cache:job:" + job.id,
                      blob,
                      "EX",
                      300,
                    );
                  } else {
                    logger.debug("Job too large to cache, will wait for GCS", {
                      jobId,
                    });
                  }

                  result.result = undefined;
                  blob = JSON.stringify(result);
                }

                this.publisher.sendToQueue(
                  msg.properties.replyTo,
                  Buffer.from(blob),
                  {
                    contentType: "application/json",
                    correlationId: msg.properties.correlationId,
                    persistent: false,
                    mandatory: true,
                    timestamp: this.timeSeconds(),
                  },
                );
                await this.publisher.waitForConfirms();
              }

              this.consumer.ack(msg);
            } catch (error) {
              logger.error("Error in job", { error, jobId });

              if (attempts < NUQ_RABBIT_MAX_RETRIES) {
                logger.warn(
                  `Retrying job ${attempts}/${NUQ_RABBIT_MAX_RETRIES}`,
                  { jobId },
                );
                this.consumer.nack(msg, false, false);
              } else {
                logger.error("Max retries reached, moving job to DLQ", {
                  jobId,
                });

                if (msg.properties.replyTo) {
                  this.publisher.sendToQueue(
                    msg.properties.replyTo,
                    Buffer.from(FAILED_JOB_JSON),
                    {
                      contentType: "application/json",
                      correlationId: msg.properties.correlationId,
                      persistent: false,
                      mandatory: true,
                      timestamp: this.timeSeconds(),
                    },
                  );
                  await this.publisher.waitForConfirms();
                }

                this.publisher.sendToQueue(this.deadLetterQueue, msg.content, {
                  correlationId: msg.properties.correlationId,
                  contentType: msg.properties.contentType,
                  headers: {
                    ...(msg.properties.headers || {}),
                    "x-error": "max-attempts",
                    "x-original-queue": this.queue,
                    "x-attempts": attempts,
                  },
                  persistent: false,
                  mandatory: true,
                  timestamp: this.timeSeconds(),
                });
                await this.publisher.waitForConfirms();

                this.consumer.ack(msg);
              }
            }
          },
        } as NuQRabbitJob;

        push(job);
      },
      {
        noAck: false,
        consumerTag,
      },
    );

    logger.info(
      `Started consuming NuQ RabbitMQ jobs with concurrency: ${prefetchCount}`,
    );

    try {
      while (true) {
        if (buffer.length > 0) {
          yield buffer.shift()!;
        } else if (isClosed || signal?.aborted) {
          break;
        } else {
          const next = await new Promise<IteratorResult<NuQRabbitJob>>(
            resolve => {
              pullNext = resolve;
            },
          );

          if (next.done) {
            break;
          } else {
            yield next.value;
          }
        }
      }
    } finally {
      cleanup();
    }
  }

  private timeSeconds() {
    return Math.floor(Date.now() / 1000);
  }
}

// TOOD: rename to rabbitQueue
export const rabbit = new NuQRabbit();
