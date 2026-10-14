// middleware/rateLimiter.js

const createRateLimiter = (redisClient, options = { windowInSeconds: 10, maxRequests: 5 }) => {
    return async (req, res, next) => {
        // Rate limit by userId if present, otherwise fall back to client IP
        const identifier = req.body.userId ? `user:${req.body.userId}` : `ip:${req.ip}`;
        const key = `rate_limit:${identifier}`;
        const now = Date.now();
        const windowStart = now - (options.windowInSeconds * 1000);

        try {
            // Step 1: Execute atomic check in Redis
            const multi = redisClient.multi();
            
            // Remove request timestamps older than our sliding window
            multi.zremrangebyscore(key, 0, windowStart);
            
            // Count active requests in the current sliding window
            multi.zcard(key);
            
            const results = await multi.exec();
            // results format: [[null, removedCount], [null, currentCount]]
            const requestCount = results[1][1];

            // Standard System Design Rate Limit Headers
            res.setHeader('X-RateLimit-Limit', options.maxRequests);

            if (requestCount >= options.maxRequests) {
                const retryAfter = options.windowInSeconds;
                res.setHeader('Retry-After', retryAfter);
                res.setHeader('X-RateLimit-Remaining', 0);

                console.warn(`🛑 [Rate Limiter] Blocked ${identifier}. Exceeded ${options.maxRequests} req / ${options.windowInSeconds}s`);

                return res.status(429).json({
                    error: "Too Many Requests",
                    message: `Rate limit exceeded. Maximum ${options.maxRequests} requests per ${options.windowInSeconds} seconds.`,
                    retryAfterSeconds: retryAfter
                });
            }

            // Step 2: Under limit -> Record this request and set expiration
            await redisClient.multi()
                .zadd(key, now, now)
                .expire(key, options.windowInSeconds)
                .exec();

            res.setHeader('X-RateLimit-Remaining', options.maxRequests - requestCount - 1);
            next();

        } catch (err) {
            console.error("Rate Limiter Error (Failing Open):", err);
            // System Design Best Practice: "Fail Open"
            // If Redis is experiencing hiccups, allow requests rather than blocking all customers
            next();
        }
    };
};

module.exports = createRateLimiter;