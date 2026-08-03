import asyncio
import json
import os
import time
import asyncpg
import websockets
from dotenv import load_dotenv

load_dotenv()

NEON_DB_URL = os.getenv("NEON_DB_URL")

if not NEON_DB_URL:
    print("Error: NEON_DB_URL is not set in .env file!")
    exit(1)

# 1. DATABASE INITIALIZATION
async def init_db():
    conn = await asyncpg.connect(NEON_DB_URL)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS crypto_metrics (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            price FLOAT NOT NULL,
            quantity FLOAT NOT NULL,
            is_anomaly BOOLEAN NOT NULL,
            timestamp TIMESTAMPTZ DEFAULT NOW()
        );
    ''')
    await conn.close()

# 2. DATABASE SINK
async def save_to_neon(symbol, price, quantity, is_anomaly):
    try:
        conn = await asyncpg.connect(NEON_DB_URL)
        await conn.execute('''
            INSERT INTO crypto_metrics (symbol, price, quantity, is_anomaly)
            VALUES ($1, $2, $3, $4);
        ''', symbol, price, quantity, is_anomaly)
        await conn.close()
    except Exception as e:
        print(f"Database Write Error: {e}")

# 3. WEBSOCKET CONSUMER & ANOMALY ENGINE
async def stream_binance():
    # Public Binance WebSocket endpoint for BTC/USDT trades
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    price_window = []
    window_size = 20  # Rolling window size for anomaly detection
    
    print("Connecting to Binance WebSocket...")
    async with websockets.connect(url) as ws:
        print("Connected to Live Binance Stream! Ingesting trade data...\n")
        
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            
            price = float(data['p'])
            quantity = float(data['q'])
            symbol = data['s']
            
            # Maintain rolling window
            price_window.append(price)
            if len(price_window) > window_size:
                price_window.pop(0)
            
            # Anomaly Detection: Z-Score / Deviation check
            is_anomaly = False
            if len(price_window) >= 5:
                mean = sum(price_window) / len(price_window)
                variance = sum((x - mean) ** 2 for x in price_window) / len(price_window)
                std_dev = variance ** 0.5
                
                # Flag as anomaly if current price deviates by more than 2 std devs or unusually high volume
                if std_dev > 0:
                    z_score = abs(price - mean) / std_dev
                    is_anomaly = z_score > 2.0 or quantity > 1.0  # Spike threshold
            
            # Save to Neon
            await save_to_neon(symbol, price, quantity, is_anomaly)
            
            status = "ANOMALY SPIKE" if is_anomaly else "NORMAL"
            print(f"[{symbol}] Price: ${price:,.2f} | Qty: {quantity:.4f} | Status: {status}")
            
            # Throttle slightly to avoid DB rate limits on free tier
            await asyncio.sleep(0.3)

async def main():
    await init_db()
    await stream_binance()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPipeline stopped.")