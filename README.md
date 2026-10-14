# ⚡ FlashMessage — Distributed Real-Time Notification Engine

[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Node.js](https://img.shields.io/badge/Node.js-18.x-339933?style=for-the-badge&logo=nodedotjs&logoColor=white)](https://nodejs.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![BullMQ](https://img.shields.io/badge/Queue-BullMQ-orange?style=for-the-badge)](https://bullmq.io/)
[![Socket.io](https://img.shields.io/badge/WebSockets-Socket.io-010101?style=for-the-badge&logo=socketdotio&logoColor=white)](https://socket.io/)

A high-throughput, fault-tolerant, distributed notification and messaging engine built to demonstrate core **System Design and Distributed Systems** concepts: asynchronous queueing, horizontal worker scaling, inter-process communication via Redis Pub/Sub, real-time WebSocket pushing, and distributed sliding-window rate limiting.

---

## 📐 System Architecture

```text
                              ┌─────────────────────────────────────────────────────────┐
                              │                    CLIENT LAYER                         │
                              └───────────────────────────┬─────────────────────────────┘
                                                          │
                                     HTTP POST /send      │  Persistent WebSocket
                                      (API Requests)      │  (Room: user_1)
                                                          ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                    API GATEWAY (Node.js)                                  │
│  ┌─────────────────────────┐    ┌─────────────────────────┐    ┌───────────────────────┐  │
│  │ Distributed Rate Limiter│───►│ Database Logger (PG)    │───►│ BullMQ Job Producer   │  │
│  │ (Redis ZSET Sliding Win)│    │ (State: QUEUED)         │    │ (Pushes job to Redis) │  │
│  └─────────────────────────┘    └─────────────────────────┘    └───────────┬───────────┘  │
│                                                                            │              │
│  ┌────────────────────────────────────────────────────────┐                │              │
│  │ Redis Pub/Sub Subscriber ──► Socket.io Push to Client  │                │              │
│  └────────────────────────────────────────────────────────┘                │              │
└──────────────────────────────────────────────▲─────────────────────────────┼──────────────┘
                                               │ Event                       │
                                               │ (notifications_channel)     │ Enqueue
                                               │                             ▼
                        ┌──────────────────────┴──────────────────────────────────┐
                        │                 REDIS MESSAGE BROKER                    │
                        │   - BullMQ Queue ('notifications')                      │
                        │   - Pub/Sub Channel ('notifications_channel')           │
                        │   - Rate Limit Logs (ZSET: 'rate_limit:user:1')         │
                        └──────────────────────▲──────────────────────────────────┘
                                               │ Pull Jobs (Competing Consumers)
                                               │
                        ┌──────────────────────┴──────────────────────────────────┐
                        │                 WORKER POOL (Horizontal Scale)          │
                        │   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐│
                        │   │   Worker 1   │   │   Worker 2   │   │   Worker 3   ││
                        │   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘│
                        └──────────┼──────────────────┼──────────────────┼────────┘
                                   │                  │                  │
                                   └──────────────────┼──────────────────┘
                                                      │
                                                      ▼
                                       ┌─────────────────────────────┐
                                       │   DOWNSTREAM INTEGRATION    │
                                       │   - Updates PostgreSQL state│
                                       │   - Simulates Network Flake │
                                       │   - Publishes completion    │
                                       └─────────────────────────────┘
```

## 🔬 Architectural Evolution (Phases 1 — 5)

This system was not built as a static application; it was iteratively engineered across 5 distinct phases to solve real-world distributed systems bottlenecks.

---

### Phase 1: The Synchronous Monolith (Baseline Prototype)

```text
[ Client ] ─── HTTP POST /send ───► [ Express API ] ─── INSERT (PENDING) ───► [ PostgreSQL ]
                                           │
                                           ▼
                                [ Mock Provider (1s-3s) ]
                                           │
                                           ▼
[ Client ] ◄─── HTTP 200 OK ──────── [ UPDATE (SENT) ] ─────────────────────► [ PostgreSQL ]
```

### Phase 2: Decoupled Asynchronicity & Fault Tolerance

```text
[ Client ] ─── POST /send ───► [ Express API ] ─── INSERT (QUEUED) ─────────► [ PostgreSQL ]
      │                               │
      │                               ▼
      │                     [ BullMQ Producer ] ──── Enqueue Job ───────────► [ Redis ]
      │                                                                           ▲
      ▼                                                                           │ Poll
[ Client ] ◄─── 202 Accepted ───────────────────────────────────────              │
                                                                                  ▼
[ PostgreSQL ] ◄─── UPDATE (SENT / RETRYING) ─── [ Mock Provider ] ◄────── [ Background Worker ]
```

### Phase 3: Containerization & Horizontal Worker Scaling

```text
┌──────────────────────────────┐
                                  │      Docker Virtual Network  │
                                  └──────────────┬───────────────┘
                                                 │
                          ┌──────────────────────┴──────────────────────┐
                          ▼                                             ▼
                 [ flash_postgres ]                              [ flash_redis ]
                  (Port: 5432)                                    (Port: 6379)
                          ▲                                             ▲
                          │                                             │
      ┌───────────────────┼─────────────────────────────────────────────┤
      │                   │                                             │
┌─────┴──────────┐ ┌──────┴─────────┐ ┌─────────────────┐ ┌─────────────┴───┐
│     api-1      │ │    worker-1    │ │    worker-2     │ │    worker-3     │
│  (Port: 3000)  │ │ (Replica Node) │ │ (Replica Node)  │ │ (Replica Node)  │
└────────────────┘ └────────────────┘ └─────────────────┘ └─────────────────┘
```

### Phase 4: Inter-Process Event Bus & Real-Time Push Delivery

```text
[ Browser (User 1) ] ◄── Persistent WebSocket ──► [ api-1 Gateway ]
                                                         ▲
                                                         │ Subscribes to
                                                         │ 'notifications_channel'
                                                         ▼
                                                [ Redis Pub/Sub Bus ]
                                                         ▲
                                                         │ PUBLISH payload
                                                         │ (When job succeeds)
                                                         │
                                  ┌──────────────────────┴──────────────────────┐
                                  │                                             │
                           [ worker-1 ]                   [ worker-2 ]   [ worker-3 ]
```

### Phase 5: Distributed Edge Protection (Sliding Window Log Rate Limiter)

```text
[ Client Ingress ]
       │
       ▼
[ POST /send ] ──► [ Rate Limiter Middleware ]
                          │
                          ▼
            [ Redis ZSET: 'rate_limit:user:1' ]
            1. ZREMRANGEBYSCORE key 0 (now - 10s)  ──► Remove expired entries
            2. ZCARD key                           ──► Count active requests
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
     Count >= 5                  Count < 5
   ┌───────────────────────┐   ┌───────────────────────┐
   │ HTTP 429 Too Many Req │   │ ZADD key now now      │
   │ Retry-After: 10       │   │ EXPIRE key 10         │
   │ X-RateLimit-Remain: 0 │   │ Continue to next()    │
   └───────────────────────┘   └───────────────────────┘
```

## 🏛️ System Design Concepts Implemented

FlashMessage was intentionally architected to demonstrate core distributed systems patterns and solve the classic bottlenecks of high-concurrency backend services.

---

### 1. Asynchronous Decoupling (Producer-Consumer Pattern)

* **Code Reference:** `index.js` (Producer) ➔ `Redis` ➔ `worker.js` (Consumer)
* **The Concept:** Ingestion is cleanly separated from execution. Client requests never block waiting for slow downstream tasks (such as network calls to SMS/Email gateways).

### 2. Horizontal Scalability & The Competing Consumers Pattern

* **Code Reference:** `docker-compose.yml` (deploy: replicas: 3 or --scale worker=N)
* **The Concept:** Background processing capacity scales linearly without changing a single line of business logic.

### 3. Fault Tolerance, Chaos Engineering & Exponential Backoff

* **Code Reference:** `worker.js` (Failure simulation) & `index.js` (Backoff options)
* **The Concept:** In distributed architectures, hardware and networks will fail. Systems must be self-healing.

### 4. Inter-Process Communication (IPC) via Pub/Sub

* **Code Reference:** `worker.js` (redisPublisher.publish) ➔ `index.js` (redisSubscriber.subscribe)
* **The Concept:** Decoupled event propagation across container boundaries.

### 5. Push vs. Pull (Zero-Polling Real-Time Architecture)

* **Code Reference:** `index.js` (Socket.io) & `public/index.html` (Client)
* **The Concept:** Moving from expensive client polling to reactive server-pushed events over persistent TCP connections.

### 6. Distributed Sliding Window Log Rate Limiting

* **Code Reference:** `middleware/rateLimiter.js`
* **The Concept:** Centralized, atomic rate limiting across horizontally scaled API Gateways using Redis Sorted Sets (ZSET).


## 🛠️ Technology Stack & Architectural Justifications

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                               TECH STACK MATRIX                          │
├───────────────────┬──────────────────────────┬───────────────────────────┤
│ Layer             │ Technology               │ Architectural Purpose     │
├───────────────────┼──────────────────────────┼───────────────────────────┤
│ API Gateway       │ Node.js 18 & Express.js  │ Non-blocking I/O Ingress  │
│ Message Broker    │ Redis 7 & BullMQ         │ Asynchronous Job Queue    │
│ Event Bus (IPC)   │ Redis Pub/Sub            │ Cross-Container Messaging │
│ State & Caching   │ Redis Sorted Sets (ZSET) │ Sliding Window Limiter    │
│ Primary Database  │ PostgreSQL 15 (Alpine)   │ ACID Relational Storage   │
│ Real-Time Layer   │ WebSockets (Socket.io 4) │ Zero-Polling Push Delivery│
│ Observability UI  │ BullBoard                │ Real-Time Queue Telemetry │
│ Containerization  │ Docker & Docker Compose  │ Microservice Replication  │
└───────────────────┴──────────────────────────┴───────────────────────────┘
```


## ⚡ Quick Start Guide

Get FlashMessage running with PostgreSQL, Redis, the API Gateway, and 3 scaled background workers in **under 2 minutes**.

---

### 1. Prerequisites
Ensure you have the following installed:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (must be running)
* [Git](https://git-scm.com/)

---

### 2. Clone the Repository
```bash
git clone https://github.com/<YOUR_GITHUB_USERNAME>/flash-message-backend.git
cd flash-message-backend
``` 

### 3. Create Environment File
Create a .env file in the project root:
```bash
PORT=3000
DB_HOST=postgres
DB_USER=postgres
DB_PASSWORD=secretpassword
DB_NAME=flash_message
DB_PORT=5432
REDIS_HOST=redis
REDIS_PORT=6379
```

### 4. Boot the Distributed Stack (One Command)
Run the stack with 3 horizontally scaled background workers:
```bash
docker compose up -d --build --scale worker=3
```

### 5. Access the Web Interfaces

### 6. Send Your First Notification
Open http://localhost:3000 in your browser so you can watch notifications arrive in real time.
In Postman send a post request to http://localhost:3000/send