import os
import json
import asyncio
import asyncpg
import numpy as np
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GzipMiddleware
from redis.asyncio import Redis
import websockets
from urllib.parse import urlparse, quote_plus, urlunparse

load_dotenv()

def sanitize_url(url_str: str) -> str:
    if not url_str: return url_str
    parsed = urlparse(url_str)
    if parsed.password:
        encoded_password = quote_plus(parsed.password)
        netloc = f"{parsed.username}:{encoded_password}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return url_str

# Clean the environment URLs
REDIS_URL = sanitize_url(os.getenv("UPSTASH_REDIS_URL"))
NEON_DB_URL = sanitize_url(os.getenv("NEON_DB_URL"))

# Z-score state
price_window = []
WINDOW_SIZE = 50

db_pool: asyncpg.Pool = None

def calculate_zscore(price: float) -> float:
    price_window.append(price)
    if len(price_window) > WINDOW_SIZE:
        price_window.pop(0)
    if len(price_window) < 10:
        return 0.0
    mean = np.mean(price_window)
    std = np.std(price_window)
    return float((price - mean) / std) if std > 0 else 0.0

async def producer_task():
    print("[Producer] Connecting to Coinbase WS...")

    url = "wss://ws-feed.exchange.coinbase.com"
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker"]
    }
    
    while True:
        try:
            redis = Redis.from_url(REDIS_URL, decode_responses=True)
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps(subscribe_message))
                print("[Producer] Connected to Coinbase WS!")

                async for message in ws:
                    data = json.loads(message)
                    if data.get("type") == "ticker" and "price" in data:
                        payload = {
                            "symbol": data.get("product_id", "BTC-USD"),
                            "price": str(data.get("price")),
                            "quantity": str(data.get("last_size", 0)),
                            "timestamp": str(data.get("time"))
                        }
                        await redis.xadd("crypto-trades", payload, maxlen=500, approximate=True)
                        await asyncio.sleep(11)
        except Exception as e:
            print(f"[Producer Error]: {e}")
            await asyncio.sleep(5)
        finally:
            if redis:
                await redis.aclose()

async def consumer_task():
    print("[Consumer] Connecting to Neon DB & Upstash Redis...")
    last_id = "$"
    batch_buffer = []

    while True:
        redis = None
        try:
            redis = Redis.from_url(REDIS_URL, decode_responses=True)
            while True:
                response = await redis.xread(streams={"crypto-trades": last_id}, count=10, block=11000)
                if response:
                    for _, messages in response:
                        for message_id, fields in messages:
                            last_id = message_id
                            symbol = fields.get("symbol", "BTC-USD")
                            price = float(fields.get("price", 0))
                            quantity = float(fields.get("quantity", 0))

                            z_score = calculate_zscore(price)
                            is_anomaly = abs(z_score) > 2.5

                            batch_buffer.append((symbol, price, quantity, z_score, is_anomaly))

                if len(batch_buffer) >= 30:
                    if db_pool:
                        async with db_pool.acquire() as conn:
                            await conn.executemany(
                                """
                                INSERT INTO crypto_metrics (symbol, price, quantity, z_score, is_anomaly)
                                VALUES ($1, $2, $3, $4, $5)
                                """,
                                batch_buffer
                            )
                    batch_buffer.clear()

                await asyncio.sleep(1)
        except Exception as e:
            print(f"[Consumer Error]: {e}. Retrying in 3s...")
            await asyncio.sleep(3)
        finally:
            if redis:
                await redis.aclose()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    if NEON_DB_URL:
        db_pool = await asyncpg.create_pool(dsn=NEON_DB_URL, min_size=0, max_size=3)

    producer_job = asyncio.create_task(producer_task())
    consumer_job = asyncio.create_task(consumer_task())
    yield
    # Properly call cancel with parentheses ()
    producer_job.cancel()
    consumer_job.cancel()
    if db_pool:
        await db_pool.close()

app = FastAPI(title="Crypto Telemetry Pipeline Worker", lifespan=lifespan)

app.add_middleware(GzipMiddleware, minimum_size=250)

# Add CORS Middleware for Vercel Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.api_route("/", methods=["GET", "HEAD"])
def health_check():
    """Health check endpoint for Render and UptimeRobot."""
    return {"status": "online", "service": "crypto-telemetry-pipeline"}

@app.get("/api/metrics")
async def get_metrics(response: Response, limit: int = Query(30, ge=1, le=500)):
    if not db_pool:
        return {"error": "Database pool unavaillable"}

    response.headers["Cache-Control"] = "public, max-age=4"

    async with db_pool.acquire() as conn:
        records = await conn.fetch(
            """
            SELECT id, symbol, price, quantity, z_score, is_anomaly, created_at
            FROM crypto_metrics
            ORDER BY id DESC
            LIMIT $1
            """,
            limit
        )
        return [dict(r) for r in records]

@app.get("/api/anomalies")
async def get_anomalies(response: Response, limit: int = Query(20, ge=1, le=100)):
    if not db_pool:
        return {"error": "Database pool unavailable"}

    response.headers["Cache-Control"] = "public, max-age=4"

    async with db_pool.acquire() as conn:
        records = await conn.fetch(
            """
            SELECT id, symbol, price, quantity, z_score, created_at
            FROM crypto_metrics
            WHERE is_anomaly = true
            ORDER BY id DESC
            LIMIT $1
            """,
            limit
        )
        return [dict(r) for r in records]