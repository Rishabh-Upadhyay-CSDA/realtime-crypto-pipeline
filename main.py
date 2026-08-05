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
from urllib.parse import urlparse, quote_plus, urlunparse

load_dotenv()

def sanitize_url(url_str: str) -> str:
    if not url_str: return url_str
    parsed = urlparse(url_str)
    if parsed.password:
        encoded_password = quote_plus(parsed.password)
        # Reconstruct userinfo safely
        netloc = f"{parsed.username}:{encoded_password}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return url_str

# Clean the environment URLs inside main.py
REDIS_URL = sanitize_url(os.getenv("UPSTASH_REDIS_URL"))
NEON_DB_URL = sanitize_url(os.getenv("NEON_DB_URL"))

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
                            "symbol": data.get("product_id"),
                            "price": str(data.get("price")),
                            "quantity": str(data.get("last_size", 0)),
                            "timestamp": str(data.get("time"))
                        }
                        await redis.xadd("crypto-trades", payload, maxlen=1000, approximate=True)
        except Exception as e:
            print(f"[Producer Error]: {e}")
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