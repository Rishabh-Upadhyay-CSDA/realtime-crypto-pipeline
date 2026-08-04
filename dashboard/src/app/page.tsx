'use client';

import { useEffect, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

interface Metric {
  id: number;
  price: number;
  quantity: number;
  is_anomaly: boolean;
  timestamp: string;
}

export default function Dashboard() {
  const [data, setData] = useState<Metric[]>([]);

  const fetchMetrics = async () => {
    try {
      const res = await fetch('/api/metrics');
      const json = await res.json();
      if (Array.isArray(json)) setData(json);
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 2000); // Auto-refresh every 2s
    return () => clearInterval(interval);
  }, []);

  return (
    <main className="min-h-screen bg-slate-900 text-white p-8 font-sans">
      <div className="max-w-5xl mx-auto space-y-6">
        <header className="flex justify-between items-center border-b border-slate-800 pb-4">
          <div>
            <h1 className="text-2xl font-bold text-emerald-400">Real-Time Crypto Telemetry Pipeline</h1>
            <p className="text-slate-400 text-sm">Live BTC/USDT trades ingested via WebSockets & Neon Postgres</p>
          </div>
          <span className="bg-emerald-500/10 text-emerald-400 px-3 py-1 rounded-full text-xs font-semibold animate-pulse">
            LIVE STREAMING
          </span>
        </header>

        <div className="bg-slate-800/50 border border-slate-700/50 p-6 rounded-xl shadow-xl">
          <h2 className="text-lg font-semibold mb-4 text-slate-200">Price Trend & Anomaly Monitor</h2>
          <div className="h-72 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data}>
                <XAxis dataKey="id" stroke="#64748b" />
                <YAxis domain={['auto', 'auto']} stroke="#64748b" />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155' }}
                  labelStyle={{ color: '#94a3b8' }}
                />
                <Line type="monotone" dataKey="price" stroke="#10b981" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </main>
  );
}