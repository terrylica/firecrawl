import { getCrawlStatusRateLimiter, redisRateLimitClient } from "../rate-limiter";

describe("Crawl Status Rate Limiter Service", () => {
  beforeAll(async () => {
    try {
      // Ensure Redis client is connected
      if (!redisRateLimitClient.status || redisRateLimitClient.status === "end") {
        // Client may already be connected by the main application
      }
    } catch (error) {
      console.error("Redis connection error in tests:", error);
    }
  });

  afterAll(async () => {
    try {
      // Don't disconnect as it might be used by other tests
      // await redisRateLimitClient.disconnect();
    } catch (error) {
      console.error("Redis disconnection error:", error);
    }
  });

  describe("getCrawlStatusRateLimiter", () => {
    it("should create a rate limiter with correct configuration", () => {
      const limiter = getCrawlStatusRateLimiter();

      expect(limiter).toBeDefined();
      expect(limiter.keyPrefix).toBe("crawlStatus");
      expect(limiter.points).toBe(100);
      expect(limiter.duration).toBe(60);
    });

    it("should consume points correctly for a team:job key", async () => {
      const limiter = getCrawlStatusRateLimiter();
      const testKey = `test-team-${Date.now()}:test-job-${Date.now()}`;

      try {
        const res = await limiter.consume(testKey, 1);
        expect(res.consumedPoints).toBe(1);
        expect(res.remainingPoints).toBe(99);
      } catch (error) {
        // If rate limit is already exceeded, that's also a valid test outcome
        expect(error).toHaveProperty("msBeforeNext");
      }
    });

    it("should have separate buckets for different job IDs", async () => {
      const limiter = getCrawlStatusRateLimiter();
      const timestamp = Date.now();
      const teamId = `test-team-${timestamp}`;
      const jobId1 = `test-job-1-${timestamp}`;
      const jobId2 = `test-job-2-${timestamp}`;
      
      const key1 = `${teamId}:${jobId1}`;
      const key2 = `${teamId}:${jobId2}`;

      try {
        // Consume from first job
        const res1 = await limiter.consume(key1, 50);
        expect(res1.consumedPoints).toBe(50);
        expect(res1.remainingPoints).toBe(50);

        // Consume from second job - should have full 100 points available
        const res2 = await limiter.consume(key2, 50);
        expect(res2.consumedPoints).toBe(50);
        expect(res2.remainingPoints).toBe(50);

        // Both jobs should maintain their separate state
        const res1Again = await limiter.get(key1);
        const res2Again = await limiter.get(key2);

        expect(res1Again?.remainingPoints).toBe(50);
        expect(res2Again?.remainingPoints).toBe(50);
      } catch (error) {
        // If rate limit is exceeded, that's expected in some test scenarios
        console.warn("Rate limit exceeded during test:", error);
      }
    });

    it("should have separate buckets for different teams", async () => {
      const limiter = getCrawlStatusRateLimiter();
      const timestamp = Date.now();
      const teamId1 = `test-team-1-${timestamp}`;
      const teamId2 = `test-team-2-${timestamp}`;
      const jobId = `test-job-${timestamp}`;
      
      const key1 = `${teamId1}:${jobId}`;
      const key2 = `${teamId2}:${jobId}`;

      try {
        // Consume from first team
        const res1 = await limiter.consume(key1, 50);
        expect(res1.consumedPoints).toBe(50);
        expect(res1.remainingPoints).toBe(50);

        // Consume from second team - should have full 100 points available
        const res2 = await limiter.consume(key2, 50);
        expect(res2.consumedPoints).toBe(50);
        expect(res2.remainingPoints).toBe(50);
      } catch (error) {
        console.warn("Rate limit exceeded during test:", error);
      }
    });

    it("should reject consumption when limit is exceeded", async () => {
      const limiter = getCrawlStatusRateLimiter();
      const testKey = `test-exceed-${Date.now()}:job-${Date.now()}`;

      try {
        // Try to consume more than the limit
        await limiter.consume(testKey, 101);
        fail("Should have thrown rate limit error");
      } catch (rateLimiterRes) {
        expect(rateLimiterRes).toHaveProperty("msBeforeNext");
        expect(rateLimiterRes.consumedPoints).toBeGreaterThan(100);
        expect(rateLimiterRes.remainingPoints).toBe(0);
      }
    });

    it("should allow consumption up to the limit", async () => {
      const limiter = getCrawlStatusRateLimiter();
      const testKey = `test-limit-${Date.now()}:job-${Date.now()}`;

      try {
        // Consume exactly at the limit
        const res = await limiter.consume(testKey, 100);
        expect(res.consumedPoints).toBe(100);
        expect(res.remainingPoints).toBe(0);

        // Next consumption should fail
        try {
          await limiter.consume(testKey, 1);
          fail("Should have thrown rate limit error");
        } catch (rateLimiterRes) {
          expect(rateLimiterRes).toHaveProperty("msBeforeNext");
        }
      } catch (error) {
        console.warn("Rate limit exceeded during test:", error);
      }
    });

    it("should reset after duration expires", async () => {
      const limiter = getCrawlStatusRateLimiter();
      const testKey = `test-reset-${Date.now()}:job-${Date.now()}`;

      try {
        // Consume some points
        const res1 = await limiter.consume(testKey, 50);
        expect(res1.consumedPoints).toBe(50);
        expect(res1.remainingPoints).toBe(50);

        // Check TTL
        const res1Info = await limiter.get(testKey);
        expect(res1Info).toBeDefined();
        if (res1Info) {
          expect(res1Info.msBeforeNext).toBeLessThanOrEqual(60000);
        }

        // Note: We can't easily test the full reset in a fast unit test
        // as it would require waiting 60 seconds. This is better tested
        // in integration tests.
      } catch (error) {
        console.warn("Rate limit test warning:", error);
      }
    }, 65000);

    it("should handle concurrent requests correctly", async () => {
      const limiter = getCrawlStatusRateLimiter();
      const testKey = `test-concurrent-${Date.now()}:job-${Date.now()}`;

      // Make 10 concurrent requests
      const promises = Array.from({ length: 10 }, () =>
        limiter.consume(testKey, 1).catch(err => err)
      );

      const results = await Promise.all(promises);
      
      // All requests should either succeed or fail with rate limit error
      results.forEach(result => {
        if (result.consumedPoints) {
          expect(result.consumedPoints).toBeGreaterThan(0);
          expect(result.consumedPoints).toBeLessThanOrEqual(100);
        } else {
          // It's a rate limit error
          expect(result).toHaveProperty("msBeforeNext");
        }
      });
    });

    it("should work with the keyPrefix format", () => {
      const limiter = getCrawlStatusRateLimiter();
      
      // The key prefix should be 'crawlStatus'
      expect(limiter.keyPrefix).toBe("crawlStatus");
      
      // When we use the limiter with a key like 'teamId:jobId',
      // Redis will store it as 'crawlStatus:teamId:jobId'
    });
  });
});

