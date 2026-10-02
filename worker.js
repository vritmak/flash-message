require('dotenv').config();
const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const pool = require('./db'); // <--- Importing your existing pool

// worker.js
const connection = new IORedis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: null,
});

// --- MOCK PROVIDER (10% Failure Simulation) ---
const mockSendProvider = async (notificationId) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            if (Math.random() < 0.1) { // 10% chance of failure
                reject(new Error("Provider Network Timeout"));
            } else {
                resolve();
            }
        }, 2000); // 2s processing time
    });
};

// --- WORKER LOGIC ---
const worker = new Worker('notifications', async (job) => {
    const { notificationId } = job.data;

    console.log(`📦 [Job ${job.id}] Notification ${notificationId} | Attempt: ${job.attemptsMade + 1}`);

    try {
        await mockSendProvider(notificationId);

        // Update DB to SENT using the imported pool
        await pool.query('UPDATE notifications SET status = $1 WHERE id = $2', ['SENT', notificationId]);
        console.log(`✅ [Job ${job.id}] Success!`);

    } catch (error) {
        console.error(`❌ [Job ${job.id}] Failed: ${error.message}`);

        // Update DB to RETRYING
        await pool.query('UPDATE notifications SET status = $1 WHERE id = $2', ['RETRYING', notificationId]);
        
        // Re-throw so BullMQ triggers the next retry
        throw error; 
    }
}, { connection });

// --- DEAD LETTER LOGIC (Final Failure) ---
worker.on('failed', async (job, err) => {
    // If all 5 attempts failed
    if (job.attemptsMade >= 5) {
        const { notificationId } = job.data;
        console.log(`💀 [Job ${job.id}] PERMANENT FAILURE for Notification ${notificationId}`);
        await pool.query('UPDATE notifications SET status = $1 WHERE id = $2', ['FAILED_PERMANENTLY', notificationId]);
    }
});

console.log("🛠️ Worker started. Waiting for messages...");