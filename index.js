require('dotenv').config();
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const { Queue } = require('bullmq');
const IORedis = require('ioredis');
const pool = require('./db');
const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter } = require('@bull-board/express');
const createRateLimiter = require('./middleware/rateLimiter');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// Body parser & Static file serving (for public/index.html UI)
app.use(express.json());
app.use(express.static('public'));

// --- REDIS CONFIGURATION ---
const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: null,
};

// Main Redis connection for BullMQ & Rate Limiter
const connection = new IORedis(redisConfig);

// Dedicated Redis connection for Pub/Sub listening
const redisSubscriber = new IORedis(redisConfig);

// --- BULLMQ QUEUE SETUP ---
const notificationQueue = new Queue('notifications', { connection });

// --- BULLBOARD DASHBOARD SETUP (Visualization) ---
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');
createBullBoard({
    queues: [new BullMQAdapter(notificationQueue)],
    serverAdapter: serverAdapter,
});
app.use('/admin/queues', serverAdapter.getRouter());

// --- DISTRIBUTED RATE LIMITER ---
// Max 5 notifications per 10-second sliding window per user/IP
const sendRateLimiter = createRateLimiter(connection, {
    windowInSeconds: 10,
    maxRequests: 5,
});

// --- WEBSOCKET CONNECTION MANAGEMENT ---
io.on('connection', (socket) => {
    console.log(`🔌 Client connected to WebSocket: ${socket.id}`);

    // Client registers its user ID to join a dedicated private room
    socket.on('register_user', (userId) => {
        socket.join(`user_${userId}`);
        console.log(`👤 Socket ${socket.id} joined room user_${userId}`);
    });

    socket.on('disconnect', () => {
        console.log(`🔌 Client disconnected: ${socket.id}`);
    });
});

// --- REDIS PUB/SUB LISTENER ---
redisSubscriber.subscribe('notifications_channel', (err) => {
    if (err) console.error("Failed to subscribe to Redis channel:", err);
    else console.log("📡 Subscribed to Redis 'notifications_channel'");
});

redisSubscriber.on('message', (channel, message) => {
    if (channel === 'notifications_channel') {
        const data = JSON.parse(message);
        console.log(`⚡ Received Pub/Sub event for User ${data.userId}. Pushing via WebSocket...`);

        // Emit only to that specific user's room!
        io.to(`user_${data.userId}`).emit('notification_received', data);
    }
});

// --- POST /send (PRODUCER WITH RATE LIMITING) ---
app.post('/send', sendRateLimiter, async (req, res) => {
    const { userId, type, message } = req.body;

    try {
        const userRes = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
        if (userRes.rows.length === 0) return res.status(404).json({ error: "User not found" });

        const logRes = await pool.query(
            'INSERT INTO notifications (user_id, type, content, status) VALUES ($1, $2, $3, $4) RETURNING id',
            [userId, type, message, 'QUEUED']
        );
        const notificationId = logRes.rows[0].id;

        await notificationQueue.add('send-notification', 
            { notificationId, userId, type, message },
            {
                attempts: 5,
                backoff: { type: 'exponential', delay: 3000 }
            }
        );

        res.status(202).json({
            success: true,
            message: "Enqueued",
            notificationId,
            dashboard: "/admin/queues"
        });

    } catch (err) {
        console.error("API Error:", err);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Server with WebSockets running on port ${PORT}`);
    console.log(`📊 Dashboard running on http://localhost:${PORT}/admin/queues`);
});