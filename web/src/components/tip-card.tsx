'use client';

import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import { useSse } from '@/lib/sse';
import { api } from '@/lib/api';
import { formatNumber, humanBytes, shortHash, timeAgo } from '@/lib/utils';
import { Sk } from './skeleton';

// Normalize the lightweight list-endpoint item to the shape this component renders.
function normalizeItem(item: any) {
  return {
    height: item.height,
    hash:   item.hash,
    time:   item.time,
    n_tx:   item.tx_count,
    size:   item.size,
    weight: item.weight,
    // List API returns flat miner / miner_address; wrap in coinbase so JSX is unchanged.
    coinbase: {
      miner:   item.miner   ?? null,
      address: item.miner_address ?? null,
    },
  };
}

async function fetchTip() {
  const resp = await api.blocks({ limit: 1 });
  const item = resp?.items?.[0];
  if (!item?.height) return null;
  return normalizeItem(item);
}

export function TipCard() {
  const [block, setBlock] = useState<any>(null);
  const [pulse, setPulse] = useState(false);
  const cardRef  = useRef<HTMLDivElement>(null);
  const retryRef = useRef<ReturnType<typeof setTimeout>>();

  useEffect(() => {
    const load = async () => {
      try {
        const b = await fetchTip();
        if (!b) {
          // Backend healthy but no blocks yet; retry shortly.
          retryRef.current = setTimeout(load, 3000);
          return;
        }
        setBlock(b);
      } catch {
        // Backend unreachable; retry sooner than the poll interval.
        retryRef.current = setTimeout(load, 3000);
      }
    };

    load();
    const i = setInterval(load, 5_000);
    return () => {
      clearInterval(i);
      clearTimeout(retryRef.current);
    };
  }, []);

  useSse(async (ev) => {
    if (ev.type === 'block') {
      try {
        // Use the same cached list endpoint — avoids the RPC-dependent /block/{id}.
        const b = await fetchTip();
        if (!b) return;
        setBlock(b);
        setPulse(true);
        setTimeout(() => setPulse(false), 1700);
      } catch {
        /* next poll will catch up */
      }
    }
  });

  if (!block) return <Sk.TipCard />;

  const cb = block.coinbase;
  return (
    <div
      ref={cardRef}
      className={`card p-6 lg:p-7 relative overflow-hidden ${pulse ? 'ripple' : ''}`}
    >
      <div className="absolute inset-0 grid-bg opacity-50 pointer-events-none" />
      <div className="relative">
        <div className="flex items-center justify-between mb-4">
          <span className="chip chip-gold">TIP</span>
          <span className="text-xs text-[var(--color-text-dim)] mono">{timeAgo(block.time)}</span>
        </div>
        <div className="flex items-baseline gap-3">
          <Link href={`/block/${block.height}`} className="text-3xl lg:text-4xl mono font-bold text-[var(--color-gold)] hover:text-[var(--color-gold-soft)] transition">
            #{formatNumber(block.height)}
          </Link>
          <span className="text-xs text-[var(--color-text-faint)] mono">{shortHash(block.hash)}</span>
        </div>
        <div className="mt-5 grid grid-cols-2 sm:grid-cols-4 gap-4 text-sm">
          <div>
            <div className="text-[10px] uppercase tracking-wider text-[var(--color-text-faint)]">TX</div>
            <div className="mono mt-1">{formatNumber(block.n_tx)}</div>
          </div>
          <div>
            <div className="text-[10px] uppercase tracking-wider text-[var(--color-text-faint)]">SIZE</div>
            <div className="mono mt-1">{humanBytes(block.size)}</div>
          </div>
          <div>
            <div className="text-[10px] uppercase tracking-wider text-[var(--color-text-faint)]">WEIGHT</div>
            <div className="mono mt-1">{formatNumber(block.weight)} WU</div>
          </div>
          <div>
            <div className="text-[10px] uppercase tracking-wider text-[var(--color-text-faint)]">MINER</div>
            <div className="mt-1 truncate">
              {cb?.miner ? (
                <span className="chip" style={{ color: cb.miner.color, borderColor: cb.miner.color }}>{cb.miner.name}</span>
              ) : cb?.address ? (
                <Link href={`/address/${cb.address}`} className="mono text-xs text-[var(--color-text-dim)] hover:text-[var(--color-accent)] transition" title={cb.address}>
                  {shortHash(cb.address, 8, 6)}
                </Link>
              ) : (
                <span className="text-[var(--color-text-dim)] text-xs">—</span>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
