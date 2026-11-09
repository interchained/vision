'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { api } from '@/lib/api';
import { useSse } from '@/lib/sse';
import { shortHash, formatNumber, humanBytes, timeAgo } from '@/lib/utils';
import { Sk, SKELETON_ROWS } from './skeleton';
import { NodeSyncingInline } from './node-syncing';
import { useSyncState } from '@/hooks/use-sync';

export function RecentTxs({ limit = 12 }: { limit?: number }) {
  const [txs, setTxs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const syncing = useSyncState();

  const load = async () => {
    try {
      const r = await api.mempoolTxs(limit);
      setTxs(r.items);
    } catch (_e) {
      /* ignore */
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    const i = setInterval(load, 5000);
    return () => clearInterval(i);
  }, [limit]);

  useSse((ev) => {
    if (ev.type === 'mempool') load();
  });

  return (
    <div className="card overflow-hidden">
      <div className="flex items-center justify-between px-5 py-3 border-b border-[var(--color-border)]">
        <h3 className="text-sm font-semibold text-white">Live Mempool</h3>
        <Link href="/mempool" className="text-xs text-[var(--color-accent)] hover:underline">View all →</Link>
      </div>
      <div className="divide-y divide-[var(--color-border-soft)]">
        {loading && SKELETON_ROWS.slice(0, limit).map((i) => (
          <Sk.TxRow key={i} />
        ))}
        {!loading && txs.length === 0 && (
          syncing ? <NodeSyncingInline /> : <div className="px-5 py-12 text-center text-sm text-[var(--color-text-dim)]">Mempool is empty.</div>
        )}
        {!loading && txs.map((t) => (
          <Link
            key={t.txid}
            href={`/tx/${t.txid}`}
            className="flex items-center justify-between px-5 py-2.5 hover:bg-white/5 transition"
          >
            <span className="mono text-xs text-[var(--color-text-dim)]">{shortHash(t.txid, 8, 6)}</span>
            <div className="flex items-center gap-4 text-xs mono">
              <span className="text-[var(--color-gold)]">{Number(t.fee_rate_sat_vbyte).toFixed(1)} sat/vB</span>
              <span className="text-[var(--color-text-dim)]">{humanBytes(t.vsize)}</span>
              <span className="text-[var(--color-text-faint)] hidden sm:inline">{timeAgo(t.time)}</span>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
