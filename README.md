# PulseTrade Analytics

Real-time crypto telemetry pipeline and anomaly detection system built with FastAPI, Next.js, Upstash Redis, Neon PostgreSQL, Render, and Vercel.

---

## Overview

PulseTrade Analytics ingests live `BTC-USD` trade ticks from Coinbase WebSockets, queues them via Upstash Redis Streams, calculates real-time price volatility anomalies using a rolling Z-score calculation in Python, persists trade telemetry into Neon PostgreSQL, and renders a live-updating dashboard with Next.js and Recharts.

---

## Architecture & Data Flow

```text
[ Coinbase WebSocket Stream ]
          │
          ▼ (Live Trade Ticks)
[ FastAPI Producer Task ]
          │
          ▼ (XADD to Stream)
[ Upstash Redis Stream ]
          │
          ▼ (XREAD Stream & Compute Z-Score)
[ FastAPI Consumer Task ] ──► (Flag |Z| > 2.5 Anomalies)
          │
          ▼ (Persist Trade Records)
[ Neon PostgreSQL DB ]
          │
          ▼ (REST API: /api/metrics & /api/anomalies)
[ Next.js Dashboard on Vercel ]
```

---

## Key Features

- **Live Telemetry Ingestion**: Continuous trade streaming via public WebSockets.
- **Statistical Anomaly Detection**: In-memory sliding-window Z-score calculation to detect abnormal price spikes and flash dips (|Z| > 2.5).
- **High-Performance Buffering**: Decouples producer ingestion from database writes using Redis Streams.
- **Persistent Storage**: Stores trade records, Z-scores, and anomaly flags in serverless Neon Postgres.
- **Interactive Dashboard**: Modern dark-mode UI featuring live price trend charts, glowing anomaly markers, metric stat cards, and an auto-updating anomaly log table.

---

## Tech Stack

- **Frontend**: Next.js, TypeScript, Tailwind CSS, Recharts
- **Backend**: FastAPI, Python, WebSockets, Asyncpg, NumPy, Redis-py
- **Database & Cache**: Upstash Redis (Streams), Neon PostgreSQL
- **Deployment**: Render (Web Service Worker), Vercel (Frontend)

---

## API Endpoints

- `GET /` : Health check endpoint (supports GET and HEAD methods for uptime monitoring).
- `GET /api/metrics?limit=30` : Fetch recent trade telemetry for chart rendering.
- `GET /api/anomalies?limit=20` : Fetch recent flagged anomaly events.

---

## Environment Variables

```text
Backend (.env / Render Environment):
  UPSTASH_REDIS_URL = rediss://default:...@...upstash.io:6379
  NEON_DB_URL       = postgresql://user:...@...neon.tech/neondb?sslmode=require
```

---

## Local Development Setup

1. Clone the repository:
   ```text
   git clone https://github.com/Rishabh-Upadhyay-CSDA/realtime-crypto-pipeline.git
   cd realtime-crypto-pipeline
   ```

2. Setup Backend:
   ```text
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install fastapi uvicorn websockets redis asyncpg numpy python-dotenv
   uvicorn main:app --reload
   ```

3. Setup Frontend:
   ```text
   npm install
   npm run dev
   ```

4. Access Dashboard:
   Open http://localhost:3000 in your browser.

---

## Database Schema

```text
CREATE TABLE IF NOT EXISTS crypto_metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC NOT NULL,
    quantity NUMERIC NOT NULL,
    z_score DOUBLE PRECISION DEFAULT 0.0,
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## License

This project is open-source and available under the MIT License.
