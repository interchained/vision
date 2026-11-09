'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { api } from '@/lib/api';
import { useSse } from '@/lib/sse';
import { formatNumber, humanBytes, shortHash, timeAgo } from '@/lib/utils';
import { Sk, SKELETON_ROWS } from './skeleton';
import { AlgoBadge } from './algo-badge';
import { NodeSyncingInline } from './node-syncing';
import { useSyncState } from '@/hooks/use-sync';

export function RecentBlocks({ limit = 12 }: { limit?: number }) {
  const [items, setItems] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [newestId, setNewestId] = useState<string | null>(null);
  const syncing = useSyncState();

  const load = async () => {
    try {
      const r = await api.blocks({ limit });
      setItems(r.items);
    } catch (_e) {
      /* ignore */
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    // SSE block events are the primary trigger; this poll is the floor
    // that keeps the list live even if the event stream is interrupted.
    const i = setInterval(load, 5000);
    return () => clearInterval(i);
  }, [limit]);

  useSse((ev) => {
    if (ev.type === 'block') {
      setNewestId(ev.data.hash);
      load();
      setTimeout(() => setNewestId(null), 1500);
    }
  });

  return (
    <div className="card overflow-hidden">
      <div className="flex items-center justify-between px-5 py-3 border-b border-[var(--color-border)]">
        <h3 className="text-sm font-semibold text-white">Recent Blocks</h3>
        <Link href="/blocks" className="text-xs text-[var(--color-accent)] hover:underline">View all →</Link>
      </div>
      <div className="divide-y divide-[var(--color-border-soft)]">
        {loading && SKELETON_ROWS.slice(0, limit).map((i) => (
          <div key={i} className="flex items-center justify-between px-5 py-3">
            <div className="flex items-center gap-4">
              <Sk.Line w="w-20" h="h-3.5" />
              <Sk.Line w="w-24" h="h-2.5" className="hidden sm:block" />
            </div>
            <div className="flex items-center gap-4">
              <Sk.Line w="w-8" h="h-2.5" />
              <Sk.Line w="w-12" h="h-2.5" />
              <Sk.Line w="w-12" h="h-2.5" />
            </div>
          </div>
        ))}
        {!loading && items.length === 0 && (
          syncing ? <NodeSyncingInline /> : <div className="px-5 py-12 text-center text-sm text-[var(--color-text-dim)]">Awaiting blocks…</div>
        )}
        {!loading && items.map((b) => (
          <Link
            key={b.hash}
            href={`/block/${b.height}`}
            className={`flex items-center justify-between px-5 py-3 hover:bg-white/5 transition ${
              newestId === b.hash ? 'slide-in' : ''
            }`}
          >
            <div className="flex items-center gap-4">
              <span className="text-base mono font-semibold text-[var(--color-gold)] min-w-[80px]">#{formatNumber(b.height)}</span>
              <AlgoBadge algo={b.algo} className="hidden sm:inline-flex" />
              <span className="hidden sm:inline text-xs text-[var(--color-text-faint)] mono">{shortHash(b.hash)}</span>
            </div>
            <div className="flex items-center gap-4 text-xs text-[var(--color-text-dim)] mono">
              <span>{b.tx_count} tx</span>
              <span>{humanBytes(b.size)}</span>
              <span>{timeAgo(b.time)}</span>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
