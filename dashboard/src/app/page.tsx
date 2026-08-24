'use client';

import { useState, useEffect } from 'react';
import { 
  ResponsiveContainer, 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  Tooltip, 
  CartesianGrid 
} from 'recharts';

interface Metric {
  id: number;
  symbol: string;
  price: number;
  quantity: number;
  z_score: number;
  is_anomaly?: boolean;
  created_at?: string;
}

export default function Dashboard() {
  const [data, setData] = useState<Metric[]>([]);
  const [anomalies, setAnomalies] = useState<Metric[]>([]);
  const [anomalyCount, setAnomalyCount] = useState<number>(0);
  const [latestPrice, setLatestPrice] = useState<number | null>(null);
  const [latestZScore, setLatestZScore] = useState<number>(0);

  const RENDER_BACKEND_URL = 'https://crypto-pipeline-worker.onrender.com';

  async function updateData() {
    try {
      // 1. Fetch live metrics stream
      const res = await fetch(`${RENDER_BACKEND_URL}/api/metrics?limit=30`);
      if (res.ok) {
        const result: Metric[] = await res.json();
        const formatted = result.reverse();
        setData(formatted);

        if (formatted.length > 0) {
          const latest = formatted[formatted.length - 1];
          setLatestPrice(Number(latest.price));
          setLatestZScore(Number(latest.z_score || 0));
          setAnomalyCount(formatted.filter((m) => m.is_anomaly).length);
        }
      }

      // 2. Fetch recent detected anomalies log
      const anomalyRes = await fetch(`${RENDER_BACKEND_URL}/api/anomalies?limit=10`);
      if (anomalyRes.ok) {
        const anomalyData: Metric[] = await anomalyRes.json();
        setAnomalies(anomalyData);
      }
    } catch (e) {
      console.error("Fetch error:", e);
    }
  }

  useEffect(() => {
    let interval: NodeJS.Timeout;

    const handleFetch = () => {
      if (!document.hidden) {
        updateData();
      }
    };

    handleFetch();

    interval = setInterval(handleFetch, 11000);

    const handleVisibilityChange = () => {
      if (document.hidden) {
        clearInterval(interval);
      } else {
        handleFetch();
        interval = setInterval(handleFetch, 11000);
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      clearInterval(interval);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, []);

  const renderCustomDot = (props: any) => {
    const { cx, cy, payload } = props;
    if (payload.is_anomaly) {
      return (
        <circle
          key={`dot-${payload.id}`}
          cx={cx}
          cy={cy}
          r={6}
          fill="#ef4444"
          stroke="#ffffff"
          strokeWidth={2}
          className="animate-ping"
        />
      );
    }
    return null;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-indigo-950 text-white font-sans flex flex-col justify-between">
      
      {/* Top Header */}
      <header className="border-b border-slate-800/80 backdrop-blur-md bg-slate-950/40 sticky top-0 z-50">
        <div className="max-w-6xl mx-auto px-6 py-4 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
          <div>
            <h1 className="text-2xl font-extrabold tracking-tight bg-gradient-to-r from-emerald-400 via-cyan-400 to-blue-500 bg-clip-text text-transparent">
              PulseTrade Analytics
            </h1>
            <p className="text-slate-400 text-xs mt-0.5">
              Real-time WebSocket Ingestion & Statistical Anomaly Stream
            </p>
          </div>

          <div className="flex items-center gap-3">
            <span className="flex items-center gap-2 bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 px-3 py-1 rounded-full text-xs font-semibold">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
              LIVE STREAMING
            </span>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-6xl mx-auto w-full px-6 py-8 space-y-6 flex-1">
        
        {/* Explanatory Banner */}
        <div className="p-4 rounded-xl bg-gradient-to-r from-blue-900/30 via-slate-800/40 to-purple-900/30 border border-slate-700/50 backdrop-blur-sm text-sm text-slate-300">
          <p className="font-semibold text-slate-200 mb-1">💡 What am I looking at?</p>
          <p className="text-slate-400 text-xs leading-relaxed">
            This dashboard ingests live <strong className="text-slate-200">BTC-USD</strong> trades from Coinbase WebSockets, processes them through Upstash Redis & Neon PostgreSQL, and calculates standard deviations (Z-score) over a 50-trade sliding window. Points where <strong className="text-rose-400">|Z| &gt; 2.5</strong> are flagged as price anomalies.
          </p>
        </div>

        {/* Live Stat Cards */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <div className="bg-slate-900/60 border border-slate-800 p-5 rounded-2xl backdrop-blur-md">
            <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Current Price</p>
            <p className="text-2xl font-bold text-slate-100 mt-1">
              {latestPrice ? `$${latestPrice.toLocaleString(undefined, { minimumFractionDigits: 2 })}` : 'Loading...'}
            </p>
          </div>

          <div className="bg-slate-900/60 border border-slate-800 p-5 rounded-2xl backdrop-blur-md">
            <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Latest Z-Score</p>
            <p className={`text-2xl font-bold mt-1 ${Math.abs(latestZScore) > 2.5 ? 'text-rose-400' : 'text-emerald-400'}`}>
              {latestZScore ? latestZScore.toFixed(3) : '0.000'}
            </p>
          </div>

          <div className="bg-slate-900/60 border border-slate-800 p-5 rounded-2xl backdrop-blur-md">
            <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Anomalies in Window</p>
            <p className="text-2xl font-bold text-rose-400 mt-1">{anomalyCount}</p>
          </div>
        </div>

        {/* Chart Container */}
        <div className="bg-slate-900/60 border border-slate-800 p-6 rounded-2xl shadow-2xl backdrop-blur-md space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
            <div>
              <h2 className="text-lg font-bold text-slate-100">Live Price & Anomaly Monitor</h2>
              <p className="text-xs text-slate-400">Red dots highlight anomalous trade events (|Z| &gt; 2.5)</p>
            </div>
            
            <div className="flex items-center gap-4 text-xs">
              <div className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 bg-emerald-400 rounded"></span>
                <span className="text-slate-400">BTC Price</span>
              </div>
              <div className="flex items-center gap-1.5">
                <span className="w-2.5 h-2.5 rounded-full bg-rose-500"></span>
                <span className="text-slate-400">Anomaly Event</span>
              </div>
            </div>
          </div>

          <div className="h-80 w-full pt-2">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.5} />
                <XAxis dataKey="id" stroke="#64748b" tick={{ fill: '#64748b', fontSize: 12 }} />
                <YAxis domain={['auto', 'auto']} stroke="#64748b" tick={{ fill: '#64748b', fontSize: 12 }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '0.75rem', color: '#f8fafc' }}
                  formatter={(value: any, name: any) => [
                    name === 'price' ? `$${Number(value).toLocaleString()}` : value,
                    name === 'price' ? 'Price' : String(name || '')
                  ]}
                />
                <Line
                  type="monotone"
                  dataKey="price"
                  stroke="#10b981"
                  strokeWidth={2}
                  dot={renderCustomDot}
                  activeDot={{ r: 6, fill: '#34d399' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Live Detected Anomalies Table */}
        <div className="bg-slate-900/60 border border-slate-800 p-6 rounded-2xl shadow-2xl backdrop-blur-md space-y-4">
          <div className="flex justify-between items-center">
            <div>
              <h2 className="text-lg font-bold text-slate-100 flex items-center gap-2">
                <span className="w-2 h-2 rounded-full bg-rose-500 animate-pulse"></span>
                Real-Time Anomaly Log
              </h2>
              <p className="text-xs text-slate-400">Recent trades triggered by statistical volatility filter (|Z| &gt; 2.5)</p>
            </div>
            <span className="text-xs font-semibold bg-rose-500/10 text-rose-400 border border-rose-500/20 px-2.5 py-1 rounded-md">
              {anomalies.length} Captured
            </span>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm text-slate-300">
              <thead className="text-xs uppercase bg-slate-950/50 text-slate-400 border-b border-slate-800">
                <tr>
                  <th className="py-3 px-4">Event ID</th>
                  <th className="py-3 px-4">Symbol</th>
                  <th className="py-3 px-4">Price</th>
                  <th className="py-3 px-4">Quantity</th>
                  <th className="py-3 px-4">Z-Score</th>
                  <th className="py-3 px-4">Timestamp</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {anomalies.length > 0 ? (
                  anomalies.map((item) => (
                    <tr key={item.id} className="hover:bg-slate-800/30 transition-colors">
                      <td className="py-3 px-4 font-mono text-xs text-slate-400">#{item.id}</td>
                      <td className="py-3 px-4 font-semibold text-slate-200">{item.symbol}</td>
                      <td className="py-3 px-4 text-emerald-400 font-mono">
                        ${Number(item.price).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                      </td>
                      <td className="py-3 px-4 font-mono text-slate-300">{Number(item.quantity).toFixed(4)}</td>
                      <td className="py-3 px-4">
                        <span className="inline-flex items-center gap-1 font-mono text-xs font-semibold text-rose-400 bg-rose-500/10 border border-rose-500/20 px-2 py-0.5 rounded">
                          {Number(item.z_score) > 0 ? `+${Number(item.z_score).toFixed(2)}` : Number(item.z_score).toFixed(2)}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-xs text-slate-400 font-mono">
                        {item.created_at ? new Date(item.created_at).toLocaleTimeString() : 'Just now'}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={6} className="py-8 text-center text-slate-500 text-xs">
                      No high-volatility anomaly events captured in current buffer window.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

      </main>

      {/* Footer */}
      <footer className="border-t border-slate-800/80 bg-slate-950/60 backdrop-blur-md py-6 mt-12">
        <div className="max-w-6xl mx-auto px-6 flex flex-col sm:flex-row items-center justify-between gap-4 text-xs text-slate-500">
          <div>
            <p className="text-slate-400 font-medium">PulseTrade Analytics © {new Date().getFullYear()}</p>
            <p className="mt-0.5">Real-Time Data Pipeline Portfolio Project</p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <span className="bg-slate-800/80 text-slate-300 px-2.5 py-1 rounded-md border border-slate-700/50">Next.js</span>
            <span className="bg-slate-800/80 text-slate-300 px-2.5 py-1 rounded-md border border-slate-700/50">FastAPI</span>
            <span className="bg-slate-800/80 text-slate-300 px-2.5 py-1 rounded-md border border-slate-700/50">Upstash Redis</span>
            <span className="bg-slate-800/80 text-slate-300 px-2.5 py-1 rounded-md border border-slate-700/50">Neon Postgres</span>
            <span className="bg-slate-800/80 text-slate-300 px-2.5 py-1 rounded-md border border-slate-700/50">Render & Vercel</span>
          </div>
        </div>
      </footer>

    </div>
  );
}