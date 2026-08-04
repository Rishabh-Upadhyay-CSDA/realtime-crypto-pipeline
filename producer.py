import os
import json
import asyncio
import websockets
from redis.asyncio import Redis
from dotenv import load_dotenv

REDIS_URL = os.getenv("UPSTASH_REDIS_URL")

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

async def stream_to_redis():
    print("Connecting to Upstash Redis...")
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    
    print("Connecting to Binance WebSocket...")
    async with websockets.connect(BINANCE_WS_URL) as ws:
        print("Connected! Publishing trade events to Upstash Redis Stream ('crypto-trades')...")
        
        async for message in ws:
            trade = json.loads(message)
            
            # Format trade payload
            payload = {
                "symbol": str(trade.get("s")),
                "price": str(trade.get("p", 0)),
                "quantity": str(trade.get("q", 0)),
                "timestamp": str(trade.get("T"))
            }
            
            # Append entry to Redis Stream via XADD
            # MAXLEN~ 1000 keeps the stream size capped to prevent memory bloat
            await redis.xadd(
                name="crypto-trades", 
                fields=payload, 
                maxlen=1000, 
                approximate=True
            )
            print(f"Published: {payload['symbol']} @ ${float(payload['price']):.2f}")

if __name__ == "__main__":
    try:
        asyncio.run(stream_to_redis())
    except KeyboardInterrupt:
        print("\nProducer stopped.")