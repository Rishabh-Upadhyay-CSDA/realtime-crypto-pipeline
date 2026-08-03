import { NextResponse } from 'next/server';
import { neon } from '@neondatabase/serverless';

export async function GET() {
  try {
    const sql = neon(process.env.NEON_DB_URL!);
    // Fetch latest 30 trade metrics
    const data = await sql`
      SELECT id, symbol, price, quantity, is_anomaly, timestamp 
      FROM crypto_metrics 
      ORDER BY id DESC 
      LIMIT 30
    `;
    return NextResponse.json(data.reverse());
  } catch (error) {
    return NextResponse.json({ error: 'Failed to fetch metrics' }, { status: 500 });
  }
}