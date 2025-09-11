import { Response } from "express";
import { v4 as uuidv4 } from "uuid";
import {
  mapRequestSchema,
  RequestWithAuth,
  scrapeOptions,
  ScrapeOptions,
  TeamFlags,
  MapRequest,
  MapDocument,
  MapResponse,
  MAX_MAP_LIMIT,
} from "./types";
import { crawlToCrawler, StoredCrawl } from "../../lib/crawl-redis";
import { configDotenv } from "dotenv";
import {
  checkAndUpdateURLForMap,
  isSameDomain,
  isSameSubdomain,
} from "../../lib/validateUrl";
import { fireEngineMap } from "../../search/fireEngine";
import { billTeam } from "../../services/billing/credit_billing";
import { logJob } from "../../services/logging/log_job";
import { logger } from "../../lib/logger";
import {
  generateURLSplits,
  queryIndexAtDomainSplitLevelWithMeta,
  queryIndexAtSplitLevelWithMeta,
} from "../../services/index";
import { redisEvictConnection } from "../../services/redis";
import { performCosineSimilarityV2 } from "../../lib/map-cosine";
import { MapTimeoutError } from "../../lib/error";
import { checkPermissions } from "../../lib/permissions";

configDotenv();

// Max Links that "Smart /map" can return
const MAX_FIRE_ENGINE_RESULTS = 500;

interface MapResult {
  success: boolean;
  job_id: string;
  time_taken: number;
  mapResults: MapDocument[];
}

function dedupeMapDocumentArray(documents: MapDocument[]): MapDocument[] {
  const urlMap = new Map<string, MapDocument>();

  for (const doc of documents) {
    const existing = urlMap.get(doc.url);

    if (!existing) {
      urlMap.set(doc.url, doc);
    } else if (doc.title !== undefined && existing.title === undefined) {
      urlMap.set(doc.url, doc);
    }
  }

  return Array.from(urlMap.values());
}

async function queryIndex(
  url: string,
  limit: number,
  useIndex: boolean,
  includeSubdomains: boolean,
): Promise<MapDocument[]> {
  if (!useIndex) {
    return [];
  }

  const urlSplits = generateURLSplits(url);
  if (urlSplits.length === 1) {
    const urlObj = new URL(url);
    const hostname = urlObj.hostname;

    // TEMP: this should be altered on June 15th 2025 7AM PT - mogery
    const [domainLinks, splitLinks] = await Promise.all([
      includeSubdomains
        ? queryIndexAtDomainSplitLevelWithMeta(hostname, limit)
        : [],
      queryIndexAtSplitLevelWithMeta(url, limit),
    ]);

    return dedupeMapDocumentArray([...domainLinks, ...splitLinks]);
  } else {
    return await queryIndexAtSplitLevelWithMeta(url, limit);
  }
}

async function getMapResults({
  url,
  search,
  limit = MAX_MAP_LIMIT,
  includeSubdomains = true,
  crawlerOptions = {},
  teamId,
  allowExternalLinks,
  abort = new AbortController().signal, // noop
  filterByPath = true,
  flags,
  useIndex = true,
  location,
}: {
  url: string;
  search?: string;
  limit?: number;
  includeSubdomains?: boolean;
  crawlerOptions?: any;
  teamId: string;
  origin?: string;
  includeMetadata?: boolean;
  allowExternalLinks?: boolean;
  abort?: AbortSignal;
  mock?: string;
  filterByPath?: boolean;
  flags: TeamFlags;
  useIndex?: boolean;
  location?: ScrapeOptions["location"];
}): Promise<MapResult> {
  const functionStartTime = process.hrtime();
  console.log("Starting getMapResults function");

  const id = uuidv4();
  let mapResults: MapDocument[] = [];
  const zeroDataRetention = flags?.forceZDR ?? false;

  // Setup phase timing
  const setupStartTime = process.hrtime();
  const sc: StoredCrawl = {
    originUrl: url,
    crawlerOptions: {
      ...crawlerOptions,
      limit: crawlerOptions.sitemapOnly ? 10000000 : limit,
      scrapeOptions: undefined,
    },
    scrapeOptions: scrapeOptions.parse({
      ...(location ? { location } : {}),
    }),
    internalOptions: { teamId },
    team_id: teamId,
    createdAt: Date.now(),
    zeroDataRetention,
  };

  const crawler = crawlToCrawler(id, sc, flags);
  const setupTime = process.hrtime(setupStartTime);
  console.log(
    "Setup phase completed:",
    (setupTime[0] * 1000 + setupTime[1] / 1000000).toFixed(2),
    "ms",
  );

  // Robots.txt phase timing
  const robotsStartTime = process.hrtime();
  try {
    sc.robots = await crawler.getRobotsTxt(false, abort);
    crawler.importRobotsTxt(sc.robots);
    const robotsTime = process.hrtime(robotsStartTime);
    console.log(
      "Robots.txt fetch completed:",
      (robotsTime[0] * 1000 + robotsTime[1] / 1000000).toFixed(2),
      "ms",
    );
  } catch (_) {
    const robotsTime = process.hrtime(robotsStartTime);
    console.log(
      "Robots.txt fetch failed:",
      (robotsTime[0] * 1000 + robotsTime[1] / 1000000).toFixed(2),
      "ms",
    );
  }

  // If sitemapOnly is true, only get links from sitemap
  if (crawlerOptions.sitemap === "only") {
    const sitemapOnlyStartTime = process.hrtime();
    console.log("Starting sitemap-only processing");

    const sitemap = await crawler.tryGetSitemap(
      urls => {
        urls.forEach(x => {
          mapResults.push({
            url: x,
          });
        });
      },
      true,
      true,
      crawlerOptions.timeout ?? 30000,
      abort,
      crawlerOptions.useMock,
    );

    if (sitemap > 0) {
      const urlProcessingStartTime = process.hrtime();
      mapResults = mapResults
        .slice(1)
        .map(x => {
          try {
            return {
              ...x,
              url: checkAndUpdateURLForMap(x.url).url.trim(),
            };
          } catch (_) {
            return null;
          }
        })
        .filter(x => x !== null) as MapDocument[];
      const urlProcessingTime = process.hrtime(urlProcessingStartTime);
      console.log(
        "URL processing completed:",
        (urlProcessingTime[0] * 1000 + urlProcessingTime[1] / 1000000).toFixed(
          2,
        ),
        "ms",
      );
    }

    const sitemapOnlyTime = process.hrtime(sitemapOnlyStartTime);
    console.log(
      "Sitemap-only processing completed:",
      (sitemapOnlyTime[0] * 1000 + sitemapOnlyTime[1] / 1000000).toFixed(2),
      "ms",
    );
  } else {
    const searchProcessingStartTime = process.hrtime();
    console.log("Starting search and sitemap processing");

    let urlWithoutWww = url.replace("www.", "");
    let mapUrl =
      search && allowExternalLinks
        ? `${search} ${urlWithoutWww}`
        : search
          ? `${search} site:${urlWithoutWww}`
          : `site:${url}`;

    const resultsPerPage = 100;
    const maxPages = Math.ceil(
      Math.min(MAX_FIRE_ENGINE_RESULTS, limit) / resultsPerPage,
    );

    // Cache check timing
    const cacheCheckStartTime = process.hrtime();
    const cacheKey = `fireEngineMap:${mapUrl}`;
    const cachedResult = await redisEvictConnection.get(cacheKey);
    const cacheCheckTime = process.hrtime(cacheCheckStartTime);
    console.log(
      "Cache check completed:",
      (cacheCheckTime[0] * 1000 + cacheCheckTime[1] / 1000000).toFixed(2),
      "ms",
    );

    let pagePromises: (Promise<any> | any)[];

    if (cachedResult) {
      const cacheParseStartTime = process.hrtime();
      pagePromises = JSON.parse(cachedResult);
      const cacheParseTime = process.hrtime(cacheParseStartTime);
      console.log(
        "Cache parsing completed:",
        (cacheParseTime[0] * 1000 + cacheParseTime[1] / 1000000).toFixed(2),
        "ms",
      );
    } else {
      const fetchPagesSetupStartTime = process.hrtime();
      const fetchPage = async (page: number) => {
        const pageStartTime = process.hrtime();
        const result = await fireEngineMap(
          mapUrl,
          {
            numResults: resultsPerPage,
            page: page,
          },
          abort,
        );
        const pageTime = process.hrtime(pageStartTime);
        console.log(
          `Page ${page} fetch completed:`,
          (pageTime[0] * 1000 + pageTime[1] / 1000000).toFixed(2),
          "ms",
        );
        return result;
      };

      pagePromises = Array.from({ length: maxPages }, (_, i) =>
        fetchPage(i + 1),
      );
      const fetchPagesSetupTime = process.hrtime(fetchPagesSetupStartTime);
      console.log(
        "Fire engine setup completed:",
        (
          fetchPagesSetupTime[0] * 1000 +
          fetchPagesSetupTime[1] / 1000000
        ).toFixed(2),
        "ms",
      );
    }

    // Parallel operations timing
    const parallelStartTime = process.hrtime();
    console.log("Starting parallel operations (index query + search results)");

    const [indexResults, searchResults] = await Promise.all([
      queryIndex(url, limit, useIndex, includeSubdomains),
      Promise.all(pagePromises),
    ]);
    const parallelTime = process.hrtime(parallelStartTime);
    console.log(
      "Parallel operations completed:",
      (parallelTime[0] * 1000 + parallelTime[1] / 1000000).toFixed(2),
      "ms",
    );

    // Cache storage timing
    if (!zeroDataRetention) {
      const cacheStoreStartTime = process.hrtime();
      await redisEvictConnection.set(
        cacheKey,
        JSON.stringify(searchResults),
        "EX",
        48 * 60 * 60,
      ); // Cache for 48 hours
      const cacheStoreTime = process.hrtime(cacheStoreStartTime);
      console.log(
        "Cache storage completed:",
        (cacheStoreTime[0] * 1000 + cacheStoreTime[1] / 1000000).toFixed(2),
        "ms",
      );
    }

    // Index results processing timing
    if (indexResults.length > 0) {
      const indexProcessingStartTime = process.hrtime();
      mapResults.push(...indexResults);
      const indexProcessingTime = process.hrtime(indexProcessingStartTime);
      console.log(
        "Index results processing completed:",
        (
          indexProcessingTime[0] * 1000 +
          indexProcessingTime[1] / 1000000
        ).toFixed(2),
        "ms",
        `(${indexResults.length} results)`,
      );
    }

    // Sitemap processing timing
    if (crawlerOptions.sitemap === "include") {
      const sitemapStartTime = process.hrtime();
      console.log("Starting sitemap processing");

      try {
        await crawler.tryGetSitemap(
          urls => {
            mapResults.push(
              ...urls.map(x => ({
                url: x,
              })),
            );
          },
          true,
          false,
          crawlerOptions.timeout ?? 30000,
          abort,
        );
        const sitemapTime = process.hrtime(sitemapStartTime);
        console.log(
          "Sitemap processing completed:",
          (sitemapTime[0] * 1000 + sitemapTime[1] / 1000000).toFixed(2),
          "ms",
        );
      } catch (e) {
        const sitemapTime = process.hrtime(sitemapStartTime);
        console.log(
          "Sitemap processing failed:",
          (sitemapTime[0] * 1000 + sitemapTime[1] / 1000000).toFixed(2),
          "ms",
        );
        logger.warn("tryGetSitemap threw an error", { error: e });
      }
    }

    // Results merging timing
    const mergingStartTime = process.hrtime();
    if (search) {
      console.log("Merging search results");
      mapResults = searchResults
        .flat()
        .map<MapDocument>(
          x =>
            ({
              url: x.url,
              title: x.title,
              description: x.description,
            }) satisfies MapDocument,
        )
        .concat(mapResults);
    } else {
      console.log("Concatenating map results");
      mapResults = mapResults.concat(
        searchResults.flat().map(x => ({
          url: x.url,
          title: x.title,
          description: x.description,
        })),
      );
    }
    const mergingTime = process.hrtime(mergingStartTime);
    console.log(
      "Results merging completed:",
      (mergingTime[0] * 1000 + mergingTime[1] / 1000000).toFixed(2),
      "ms",
      `(${mapResults.length} total results)`,
    );

    // Limit cutoff timing
    const cutoffStartTime = process.hrtime();
    const minumumCutoff = Math.min(MAX_MAP_LIMIT, limit);
    if (mapResults.length > minumumCutoff) {
      mapResults = mapResults.slice(0, minumumCutoff);
      const cutoffTime = process.hrtime(cutoffStartTime);
      console.log(
        "Results cutoff applied:",
        (cutoffTime[0] * 1000 + cutoffTime[1] / 1000000).toFixed(2),
        "ms",
        `(limited to ${minumumCutoff})`,
      );
    }

    // Cosine similarity timing
    if (search) {
      const similarityStartTime = process.hrtime();
      const searchQuery = search.toLowerCase();
      mapResults = performCosineSimilarityV2(mapResults, searchQuery);
      const similarityTime = process.hrtime(similarityStartTime);
      console.log(
        "Cosine similarity processing completed:",
        (similarityTime[0] * 1000 + similarityTime[1] / 1000000).toFixed(2),
        "ms",
      );
    }

    const searchProcessingTime = process.hrtime(searchProcessingStartTime);
    console.log(
      "Search and sitemap processing completed:",
      (
        searchProcessingTime[0] * 1000 +
        searchProcessingTime[1] / 1000000
      ).toFixed(2),
      "ms",
    );
  }

  // Filtering phase timing
  const filteringStartTime = process.hrtime();
  console.log("Starting filtering phase");

  // URL validation timing
  const urlValidationStartTime = process.hrtime();
  mapResults = mapResults
    .map(x => {
      try {
        return {
          ...x,
          url: checkAndUpdateURLForMap(
            x.url,
            crawlerOptions.ignoreQueryParameters ?? true,
          ).url.trim(),
        };
      } catch (_) {
        return null;
      }
    })
    .filter(x => x !== null) as MapDocument[];
  const urlValidationTime = process.hrtime(urlValidationStartTime);
  console.log(
    "URL validation completed:",
    (urlValidationTime[0] * 1000 + urlValidationTime[1] / 1000000).toFixed(2),
    "ms",
  );

  // Domain filtering timing
  const domainFilteringStartTime = process.hrtime();
  mapResults = mapResults.filter(x => isSameDomain(x.url, url));
  const domainFilteringTime = process.hrtime(domainFilteringStartTime);
  console.log(
    "Domain filtering completed:",
    (domainFilteringTime[0] * 1000 + domainFilteringTime[1] / 1000000).toFixed(
      2,
    ),
    "ms",
    `(${mapResults.length} results remaining)`,
  );

  // Subdomain filtering timing
  if (!includeSubdomains) {
    const subdomainFilteringStartTime = process.hrtime();
    mapResults = mapResults.filter(x => isSameSubdomain(x.url, url));
    const subdomainFilteringTime = process.hrtime(subdomainFilteringStartTime);
    console.log(
      "Subdomain filtering completed:",
      (
        subdomainFilteringTime[0] * 1000 +
        subdomainFilteringTime[1] / 1000000
      ).toFixed(2),
      "ms",
      `(${mapResults.length} results remaining)`,
    );
  }

  // Path filtering timing
  if (filterByPath && !allowExternalLinks) {
    const pathFilteringStartTime = process.hrtime();
    try {
      const urlObj = new URL(url);
      const urlPath = urlObj.pathname;
      // Only apply path filtering if the URL has a significant path (not just '/' or empty)
      // This means we only filter by path if the user has not selected a root domain
      if (urlPath && urlPath !== "/" && urlPath.length > 1) {
        const beforeCount = mapResults.length;
        mapResults = mapResults.filter(x => {
          try {
            const linkObj = new URL(x.url);
            return linkObj.pathname.startsWith(urlPath);
          } catch (e) {
            return false;
          }
        });
        const pathFilteringTime = process.hrtime(pathFilteringStartTime);
        console.log(
          "Path filtering completed:",
          (
            pathFilteringTime[0] * 1000 +
            pathFilteringTime[1] / 1000000
          ).toFixed(2),
          "ms",
          `(${beforeCount} -> ${mapResults.length} results)`,
        );
      } else {
        const pathFilteringTime = process.hrtime(pathFilteringStartTime);
        console.log(
          "Path filtering skipped (root domain):",
          (
            pathFilteringTime[0] * 1000 +
            pathFilteringTime[1] / 1000000
          ).toFixed(2),
          "ms",
        );
      }
    } catch (e) {
      // If URL parsing fails, continue without path filtering
      const pathFilteringTime = process.hrtime(pathFilteringStartTime);
      console.log(
        "Path filtering failed:",
        (pathFilteringTime[0] * 1000 + pathFilteringTime[1] / 1000000).toFixed(
          2,
        ),
        "ms",
      );
      logger.warn(`Failed to parse URL for path filtering: ${url}`, {
        error: e,
      });
    }
  }

  // Deduplication timing
  const dedupeStartTime = process.hrtime();
  mapResults = dedupeMapDocumentArray(mapResults);
  const dedupeTime = process.hrtime(dedupeStartTime);
  console.log(
    "Deduplication completed:",
    (dedupeTime[0] * 1000 + dedupeTime[1] / 1000000).toFixed(2),
    "ms",
    `(${mapResults.length} unique results)`,
  );

  const filteringTime = process.hrtime(filteringStartTime);
  console.log(
    "Filtering phase completed:",
    (filteringTime[0] * 1000 + filteringTime[1] / 1000000).toFixed(2),
    "ms",
  );

  // Final limit application timing
  const finalLimitStartTime = process.hrtime();
  mapResults = mapResults.slice(0, limit);
  const finalLimitTime = process.hrtime(finalLimitStartTime);
  console.log(
    "Final limit applied:",
    (finalLimitTime[0] * 1000 + finalLimitTime[1] / 1000000).toFixed(2),
    "ms",
    `(${mapResults.length} final results)`,
  );

  const totalFunctionTime = process.hrtime(functionStartTime);
  const totalTimeMs =
    totalFunctionTime[0] * 1000 + totalFunctionTime[1] / 1000000;
  console.log(
    "getMapResults function completed:",
    totalTimeMs.toFixed(2),
    "ms",
  );

  return {
    success: true,
    mapResults,
    job_id: id,
    time_taken: totalTimeMs / 1000, // Convert to seconds
  };
}

export async function mapController(
  req: RequestWithAuth<{}, MapResponse, MapRequest>,
  res: Response<MapResponse>,
) {
  const originalRequest = req.body;
  req.body = mapRequestSchema.parse(req.body);

  const permissions = checkPermissions(req.body, req.acuc?.flags);
  if (permissions.error) {
    return res.status(403).json({
      success: false,
      error: permissions.error,
    });
  }

  logger.info("Map request", {
    request: req.body,
    originalRequest,
    teamId: req.auth.team_id,
  });

  let result: Awaited<ReturnType<typeof getMapResults>>;
  const abort = new AbortController();
  try {
    result = (await Promise.race([
      getMapResults({
        url: req.body.url,
        search: req.body.search,
        limit: req.body.limit,
        includeSubdomains: req.body.includeSubdomains,
        crawlerOptions: {
          ...req.body,
          sitemap: req.body.sitemap,
        },
        origin: req.body.origin,
        teamId: req.auth.team_id,
        abort: abort.signal,
        mock: req.body.useMock,
        filterByPath: req.body.filterByPath !== false,
        flags: req.acuc?.flags ?? null,
        useIndex: req.body.useIndex,
        location: req.body.location,
      }),
      ...(req.body.timeout !== undefined
        ? [
            new Promise((resolve, reject) =>
              setTimeout(() => {
                abort.abort(new MapTimeoutError());
                reject(new MapTimeoutError());
              }, req.body.timeout),
            ),
          ]
        : []),
    ])) as any;
  } catch (error) {
    if (error instanceof MapTimeoutError) {
      return res.status(408).json({
        success: false,
        code: error.code,
        error: error.message,
      });
    } else {
      throw error;
    }
  }

  // Bill the team
  billTeam(
    req.auth.team_id,
    req.acuc?.sub_id ?? undefined,
    1,
    req.acuc?.api_key_id ?? null,
  ).catch(error => {
    logger.error(
      `Failed to bill team ${req.auth.team_id} for 1 credit: ${error}`,
    );
  });

  // Log the job
  logJob({
    job_id: result.job_id,
    success: result.mapResults.length > 0,
    message: "Map completed",
    num_docs: result.mapResults.length,
    docs: result.mapResults,
    time_taken: result.time_taken,
    team_id: req.auth.team_id,
    mode: "map",
    url: req.body.url,
    crawlerOptions: {},
    scrapeOptions: {},
    origin: req.body.origin ?? "api",
    integration: req.body.integration,
    num_tokens: 0,
    credits_billed: 1,
    zeroDataRetention: false, // not supported
  });

  const response = {
    success: true as const,
    links: result.mapResults,
  };

  return res.status(200).json(response);
}
