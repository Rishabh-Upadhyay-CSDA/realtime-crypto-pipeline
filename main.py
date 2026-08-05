import os
import json
import asyncio
import asyncpg
import numpy as np
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from redis.asyncio import Redis
import websockets

load_dotenv()

# Environment Variables
REDIS_URL = os.getenv("UPSTASH_REDIS_URL")
NEON_DB_URL = os.getenv("NEON_DB_URL")
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Z-score state
price_window = []
WINDOW_SIZE = 50

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
    """Reads from Binance WebSocket and publishes to Upstash Redis Stream."""
    print("[Producer] Connecting to Upstash Redis & Binance WS...")
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as ws:
                print("[Producer] Ingestion loop active...")
                async for message in ws:
                    trade = json.loads(message)
                    payload = {
                        "symbol": str(trade.get("s")),
                        "price": str(trade.get("p", 0)),
                        "quantity": str(trade.get("q", 0)),
                        "timestamp": str(trade.get("T"))
                    }
                    await redis.xadd("crypto-trades", payload, maxlen=1000, approximate=True)
        except Exception as e:
            print(f"[Producer] Connection error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

async def consumer_task():
    """Reads from Upstash Redis Stream, runs anomaly detection, writes to Neon DB."""
    print("[Consumer] Connecting to Neon DB & Upstash Redis...")
    pool = await asyncpg.create_pool(dsn=NEON_DB_URL)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    last_id = "$"
    print("[Consumer] Processing loop active...")

    while True:
        try:
            response = await redis.xread(streams={"crypto-trades": last_id}, count=10, block=1000)
            if not response:
                await asyncio.sleep(0.01)
                continue

            for _, messages in response:
                for message_id, fields in messages:
                    last_id = message_id
                    symbol = fields["symbol"]
                    price = float(fields["price"])
                    quantity = float(fields["quantity"])

                    z_score = calculate_zscore(price)
                    is_anomaly = abs(z_score) > 2.5

                    async with pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO crypto_metrics (symbol, price, quantity, z_score, is_anomaly)
                            VALUES ($1, $2, $3, $4, $5)
                            """,
                            symbol, price, quantity, z_score, is_anomaly
                        )
        except Exception as e:
            print(f"[Consumer] Processing error: {e}. Retrying in 3s...")
            await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app:FastAPI):
    producer_job = asyncio.create_task(producer_task())
    consumer_job = asyncio.create_task(consumer_task())
    yield
    producer_job.cancel
    consumer_job.cancel

app = FastAPI(title="Crypto Telemetry Pipeline Worker", lifespan=lifespan)

@app.get("/")
def health_check():
    """Health check endpoint for Render and UptimeRobot."""
    return {"status": "online", "service": "crypto-telemetry-pipeline"}