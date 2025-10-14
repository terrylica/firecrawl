import "dotenv/config";
import { logger as _logger } from "../../lib/logger";
import { processJobInternal } from "./scrape-worker";
import { scrapeQueue, nuqGetLocalMetrics, nuqHealthCheck, NuQJob } from "./nuq";
import Express from "express";
import { _ } from "ajv";
import { initializeBlocklist } from "../../scraper/WebScraper/utils/blocklist";
import { ScrapeJobData } from "../../types";

import { Document } from "../../controllers/v2/types";
import { NuQRabbitJobResult, rabbit } from "./queue/nuq-rabbit";

async function runRabbitScrapeJob(
  job: NuQJob<ScrapeJobData, Document | null>,
): Promise<NuQRabbitJobResult> {
  const logger = _logger.child({
    module: "nuq-worker",
    scrapeId: job.id,
    zeroDataRetention: job.data?.zeroDataRetention ?? false,
  });

  logger.info("Acquired NuQ Rabbit job", { jobId: job.id });

  let processResult:
    | { ok: true; data: Awaited<ReturnType<typeof processJobInternal>> }
    | { ok: false; error: any };

  try {
    processResult = { ok: true, data: await processJobInternal(job) };
  } catch (error) {
    processResult = { ok: false, error };
  }

  return {
    status: processResult.ok ? "completed" : "failed",
    result: "data" in processResult ? processResult.data : undefined,
    failedReason: "error" in processResult ? processResult.error : undefined,
  } satisfies NuQRabbitJobResult;
}

async function startRabbitWorker(signal: AbortSignal) {
  await rabbit.start();

  const prefetchCount = Number(process.env.NUQ_RABBITMQ_PREFETCH_COUNT) || 10;
  while (!signal.aborted) {
    const queue = rabbit.subscribe({
      prefetchCount,
      signal,
    });

    const inflightJobs = new Set<Promise<void>>();
    const trackJob = (p: Promise<void>) => {
      inflightJobs.add(p);
      p.finally(() => inflightJobs.delete(p)).catch(error => {
        _logger.error("Uncaught error in job", { error });
      });
    };

    try {
      for await (const job of queue) {
        const p = job.run(async job => {
          return await runRabbitScrapeJob(job);
        });
        trackJob(p);

        if (inflightJobs.size >= prefetchCount) {
          await Promise.race(inflightJobs);
        }
      }
    } catch (error) {
      _logger.error("Error in NuQ RabbitMQ worker", error);
    } finally {
      await queue.return().catch(() => {});
      await Promise.allSettled(inflightJobs);

      if (!signal.aborted) {
        _logger.info("Resubscribing to NuQ RabbitMQ queue");
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
  }

  await rabbit.stop();
}

(async () => {
  try {
    await initializeBlocklist();
  } catch (error) {
    _logger.error("Failed to initialize blocklist", { error });
    process.exit(1);
  }

  let isShuttingDown = false;
  const abortController = new AbortController();

  const app = Express();

  app.get("/metrics", (_, res) =>
    res.contentType("text/plain").send(nuqGetLocalMetrics()),
  );
  app.get("/health", async (_, res) => {
    if (await nuqHealthCheck()) {
      res.status(200).send("OK");
    } else {
      res.status(500).send("Not OK");
    }
  });

  const server = app.listen(
    Number(process.env.NUQ_WORKER_PORT ?? process.env.PORT ?? 3000),
    () => {
      _logger.info("NuQ worker metrics server started");
    },
  );

  function shutdown() {
    isShuttingDown = true;
    abortController.abort();
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  if (process.env.NUQ_RABBITMQ_URL) {
    _logger.info("Starting NuQ RabbitMQ worker");
    startRabbitWorker(abortController.signal).catch(error => {
      _logger.error("Unhandled error in NuQ RabbitMQ worker", { error });
    });
  }

  // keeping legacy for now until everything is migrated to purely rabbit-based queues
  _logger.info("Starting NuQ polling worker");

  let noJobTimeout = 1500;

  while (!isShuttingDown) {
    const job = await scrapeQueue.getJobToProcess();

    if (job === null) {
      _logger.info("No jobs to process", { module: "nuq/metrics" });
      await new Promise(resolve => setTimeout(resolve, noJobTimeout));
      if (!process.env.NUQ_RABBITMQ_URL) {
        noJobTimeout = Math.min(noJobTimeout * 2, 10000);
      }
      continue;
    }

    noJobTimeout = 500;

    const logger = _logger.child({
      module: "nuq-worker",
      scrapeId: job.id,
      zeroDataRetention: job.data?.zeroDataRetention ?? false,
    });

    logger.info("Acquired job");

    const lockRenewInterval = setInterval(async () => {
      logger.info("Renewing lock");
      if (!(await scrapeQueue.renewLock(job.id, job.lock!, logger))) {
        logger.warn("Failed to renew lock");
        clearInterval(lockRenewInterval);
        return;
      }
      logger.info("Renewed lock");
    }, 15000);

    let processResult:
      | { ok: true; data: Awaited<ReturnType<typeof processJobInternal>> }
      | { ok: false; error: any };

    try {
      processResult = { ok: true, data: await processJobInternal(job) };
    } catch (error) {
      processResult = { ok: false, error };
    }

    clearInterval(lockRenewInterval);

    if (processResult.ok) {
      if (
        !(await scrapeQueue.jobFinish(
          job.id,
          job.lock!,
          processResult.data,
          logger,
        ))
      ) {
        logger.warn("Could not update job status");
      }
    } else {
      if (
        !(await scrapeQueue.jobFail(
          job.id,
          job.lock!,
          processResult.error instanceof Error
            ? processResult.error.message
            : typeof processResult.error === "string"
              ? processResult.error
              : JSON.stringify(processResult.error),
          logger,
        ))
      ) {
        logger.warn("Could not update job status");
      }
    }
  }

  _logger.info("NuQ worker shutting down");

  server.close(async () => {
    await scrapeQueue.shutdown();
    _logger.info("NuQ worker shut down");
    process.exit(0);
  });
})();
