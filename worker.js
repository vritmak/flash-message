require('dotenv').config();
const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const pool = require('./db');

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: null,
};

const connection = new IORedis(redisConfig);

// Dedicated Redis Publisher instance for Pub/Sub
const redisPublisher = new IORedis(redisConfig);

// Mock Provider (10% random failure)
const mockSendProvider = async (notificationId) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            if (Math.random() < 0.1) {
                reject(new Error("Provider Network Timeout"));
            } else {
                resolve();
            }
        }, 1500); // 1.5s delay
    });
};

const worker = new Worker('notifications', async (job) => {
    const { notificationId, userId, message, type } = job.data;

    console.log(`📦 [Job ${job.id}] Processing Notification ${notificationId} for User ${userId}`);

    try {
        await mockSendProvider(notificationId);

        // 1. Update Database
        await pool.query('UPDATE notifications SET status = $1 WHERE id = $2', ['SENT', notificationId]);
        console.log(`✅ [Job ${job.id}] Sent successfully!`);

        // 2. Publish Real-Time Event to Redis Pub/Sub
        const payload = JSON.stringify({
            notificationId,
            userId,
            type,
            message,
            status: 'SENT',
            timestamp: new Date().toISOString()
        });

        await redisPublisher.publish('notifications_channel', payload);

    } catch (error) {
        console.error(`❌ [Job ${job.id}] Failed: ${error.message}`);
        await pool.query('UPDATE notifications SET status = $1 WHERE id = $2', ['RETRYING', notificationId]);
        throw error;
    }
}, { connection });

worker.on('failed', async (job, err) => {
    if (job.attemptsMade >= 5) {
        const { notificationId } = job.data;
        await pool.query('UPDATE notifications SET status = $1 WHERE id = $2', ['FAILED_PERMANENTLY', notificationId]);
    }
});

console.log("🛠️ Worker started with Redis Pub/Sub publishing enabled.");