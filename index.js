require('dotenv').config();
const express = require('express');
const { Queue } = require('bullmq');
const IORedis = require('ioredis');
const pool = require('./db'); // <--- Importing your existing pool
const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter } = require('@bull-board/express');

// --- REDIS & QUEUE SETUP ---
// CORRECT:
const connection = new IORedis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: null,
});
const notificationQueue = new Queue('notifications', { connection });

// --- BULLBOARD SETUP (Visualization) ---
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');
createBullBoard({
    queues: [new BullMQAdapter(notificationQueue)],
    serverAdapter: serverAdapter,
});

const app = express();
app.use(express.json());

// Dashboard Route
app.use('/admin/queues', serverAdapter.getRouter());

// POST /send - The Producer
app.post('/send', async (req, res) => {
    const { userId, type, message } = req.body;

    try {
        // 1. Check if user exists using the imported pool
        const userRes = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
        if (userRes.rows.length === 0) return res.status(404).json({ error: "User not found" });

        // 2. Log in DB as QUEUED
        const logRes = await pool.query(
            'INSERT INTO notifications (user_id, type, content, status) VALUES ($1, $2, $3, $4) RETURNING id',
            [userId, type, message, 'QUEUED']
        );
        const notificationId = logRes.rows[0].id;

        // 3. Add to Queue with Retries & Exponential Backoff
        await notificationQueue.add('send-notification', 
            { notificationId, userId, type, message },
            {
                attempts: 5, 
                backoff: {
                    type: 'exponential',
                    delay: 5000, // Starts with 5s delay, then 10s, 20s...
                }
            }
        );

        res.status(202).json({ 
            success: true, 
            message: "Enqueued", 
            notificationId,
            dashboard: "http://localhost:3000/admin/queues"
        });

    } catch (err) {
        console.error("API Error:", err);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 API: http://localhost:${PORT}`);
    console.log(`📊 Dashboard: http://localhost:${PORT}/admin/queues`);
});