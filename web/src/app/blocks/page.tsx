'use client';

import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import { api } from '@/lib/api';
import { formatNumber, humanBytes, shortHash, timeAgo } from '@/lib/utils';
import { Sk, SKELETON_ROWS } from '@/components/skeleton';
import { NodeSyncingInline } from '@/components/node-syncing';
import { useSyncState } from '@/hooks/use-sync';

export default function BlocksPage() {
  const [items, setItems] = useState<any[]>([]);
  const [tip, setTip] = useState<number | null>(null);
  const [nextBefore, setNextBefore] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const syncing = useSyncState();

  const load = async (before?: number) => {
    if (before) setLoadingMore(true);
    try {
      const r = await api.blocks({ limit: 30, before_height: before });
      setItems((prev) => (before ? [...prev, ...r.items] : r.items));
      setTip(r.tip_height);
      setNextBefore(r.next_before_height);
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  };

  // Poll the head of the chain every 5s so the page stays live; once
  // the user pages into history ("Load older") the poll pauses so
  // their view doesn't shift under them.
  const paginatedRef = useRef(false);
  useEffect(() => {
    load();
    const i = setInterval(() => {
      if (!paginatedRef.current) load();
    }, 5000);
    return () => clearInterval(i);
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl lg:text-3xl font-bold">Blocks</h1>
        {loading ? (
          <Sk.Line w="w-40" h="h-3" className="mt-2" />
        ) : (
          <p className="text-sm text-[var(--color-text-dim)] mt-1">Tip is at #{formatNumber(tip ?? 0)}.</p>
        )}
      </div>
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
        <div className="min-w-[600px]">
        <div className="hidden md:grid grid-cols-[110px_1fr_60px_90px_90px_130px_110px] gap-3 px-5 py-3 text-[11px] uppercase tracking-wider text-[var(--color-text-faint)] border-b border-[var(--color-border)]">
          <div>Height</div>
          <div>Hash</div>
          <div className="text-right">TX</div>
          <div className="text-right">Size</div>
          <div className="text-right">Weight</div>
          <div>Miner</div>
          <div className="text-right">Time</div>
        </div>
        <div className="divide-y divide-[var(--color-border-soft)]">
          {loading && SKELETON_ROWS.slice(0, 15).map((i) => (
            <Sk.BlockRow key={i} />
          ))}
          {!loading && items.length === 0 && (
            syncing ? <NodeSyncingInline /> : (
              <div className="px-5 py-12 text-center text-sm text-[var(--color-text-dim)]">No blocks indexed yet.</div>
            )
          )}
          {!loading && items.map((b) => (
            <Link
              key={b.hash}
              href={`/block/${b.height}`}
              className="grid grid-cols-2 md:grid-cols-[110px_1fr_60px_90px_90px_130px_110px] gap-3 px-5 py-3 hover:bg-white/5 transition mono text-sm items-center"
            >
              <div className="text-[var(--color-gold)] font-semibold">#{formatNumber(b.height)}</div>
              <div className="hidden md:block text-xs text-[var(--color-text-dim)] truncate">{shortHash(b.hash, 16, 12)}</div>
              <div className="text-right">{b.tx_count}</div>
              <div className="text-right text-[var(--color-text-dim)]">{humanBytes(b.size)}</div>
              <div className="text-right hidden md:block text-[var(--color-text-dim)]">{formatNumber(b.weight)}</div>
              <div className="hidden md:block">
                {b.miner ? (
                  <span className="chip" style={{ color: b.miner.color, borderColor: b.miner.color }}>{b.miner.name}</span>
                ) : b.miner_address ? (
                  <span className="mono text-xs text-[var(--color-text-dim)]" title={b.miner_address}>{shortHash(b.miner_address, 6, 4)}</span>
                ) : (
                  <span className="text-xs text-[var(--color-text-faint)]">—</span>
                )}
              </div>
              <div className="text-right text-xs text-[var(--color-text-dim)]">{timeAgo(b.time)}</div>
            </Link>
          ))}
          {loadingMore && SKELETON_ROWS.slice(0, 10).map((i) => (
            <Sk.BlockRow key={`more-${i}`} />
          ))}
        </div>
        </div>
        </div>
      </div>
      {nextBefore !== null && nextBefore > 0 && !loading && (
        <div className="text-center">
          <button
            onClick={() => { paginatedRef.current = true; load(nextBefore); }}
            disabled={loadingMore}
            className="px-5 py-2.5 bg-[var(--color-surface)] border border-[var(--color-border)] hover:border-[var(--color-accent)] disabled:opacity-50 rounded-lg text-sm transition"
          >
            {loadingMore ? 'Loading…' : 'Load older'}
          </button>
        </div>
      )}
    </div>
  );
}
