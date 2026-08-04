import os
import json
import asyncio
import asyncpg
import numpy as np
from redis.asyncio import Redis
from dotenv import load_dotenv

REDIS_URL = os.getenv("UPSTASH_REDIS_URL")

NEON_DB_URL = os.getenv("NEON_DB_URL")

# Rolling window buffer for Z-score calculation
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

async def main():
    print("Connecting to Neon DB & Upstash Redis...")
    pool = await asyncpg.create_pool(dsn=NEON_DB_URL)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    
    # '$' reads only NEW messages arriving after the consumer starts
    last_id = "$"
    print("Consumer running. Polling 'crypto-trades' stream from Upstash Redis...")

    try:
        while True:
            # Poll Redis Stream with 1 second block timeout
            response = await redis.xread(
                streams={"crypto-trades": last_id}, 
                count=10, 
                block=1000
            )

            if not response:
                await asyncio.sleep(0.01)
                continue

            for stream_name, messages in response:
                for message_id, fields in messages:
                    # Update pointer to the latest processed message ID
                    last_id = message_id

                    symbol = fields["symbol"]
                    price = float(fields["price"])
                    quantity = float(fields["quantity"])

                    # Run Z-score calculation
                    z_score = calculate_zscore(price)
                    is_anomaly = abs(z_score) > 2.5

                    # Write enriched telemetry to Neon DB
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO crypto_metrics (symbol, price, quantity, z_score, is_anomaly)
                            VALUES ($1, $2, $3, $4, $5)
                            """,
                            symbol, price, quantity, z_score, is_anomaly
                        )

                    print(f"Processed: {symbol} @ ${price:.2f} | Z-Score: {z_score:.2f} | Anomaly: {is_anomaly}")

    except KeyboardInterrupt:
        print("\nConsumer stopping...")
    finally:
        await redis.close()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())