'use client';

import Link from 'next/link';
import { useNetwork } from '@/hooks/use-network';
import { cn, formatNumber, humanHashrate, timeAgo } from '@/lib/utils';
import { AlgoBadge } from './algo-badge';
import { Sk } from './skeleton';

/** Per-lane stats as served by GET /stats/network's `algos` payload. */
export interface AlgoLaneStats {
  difficulty: number | null;
  networkhashps: number | null;
  last_block: { height: number; hash: string; time: number } | null;
  blocks_24h: number;
  share_pct_24h: number;
}

export interface NetworkAlgos {
  sha256: AlgoLaneStats;
  yespower: AlgoLaneStats;
  ethash: AlgoLaneStats;
}

type LaneKey = keyof NetworkAlgos;

const LANES: {
  key: LaneKey;
  name: string;
  hardware: string;
  accent: 'gold' | 'blue' | 'green';
  bar: string;
}[] = [
  { key: 'sha256', name: 'SHA-256', hardware: 'ASIC lane', accent: 'gold', bar: 'var(--color-gold)' },
  { key: 'yespower', name: 'Yespower', hardware: 'CPU lane', accent: 'blue', bar: 'var(--color-accent)' },
  { key: 'ethash', name: 'Ethash', hardware: 'GPU lane', accent: 'green', bar: 'var(--color-success)' },
];

const ACCENT_TEXT = {
  gold: 'text-[var(--color-gold)]',
  blue: 'text-[var(--color-accent)]',
  green: 'text-[var(--color-success)]',
} as const;

function LaneCard({
  lane,
  stats,
  loading,
}: {
  lane: (typeof LANES)[number];
  stats?: AlgoLaneStats;
  loading: boolean;
}) {
  const last = stats?.last_block;
  return (
    <div className="card p-4 lg:p-5">
      <div className="flex items-center justify-between gap-2">
        <div className="text-[10px] uppercase tracking-[0.18em] text-[var(--color-text-faint)] font-semibold">
          {lane.name} · {lane.hardware}
        </div>
        <AlgoBadge algo={lane.key} />
      </div>
      {loading ? (
        <>
          <Sk.Line w="w-28" h="h-7" className="mt-2 mb-2" />
          <Sk.Line w="w-20" h="h-2.5" />
        </>
      ) : (
        <>
          <div className={cn('mt-2 text-xl lg:text-2xl mono font-semibold', ACCENT_TEXT[lane.accent])}>
            {humanHashrate(stats?.networkhashps || 0)}
          </div>
          <div className="mt-1 text-xs text-[var(--color-text-dim)]">
            Difficulty {stats?.difficulty != null ? formatNumber(stats.difficulty, 8) : '—'}
          </div>

          <div className="mt-4 space-y-1.5 text-xs mono">
            <div className="flex items-center justify-between">
              <span className="text-[var(--color-text-faint)]">Last block</span>
              {last ? (
                <Link href={`/block/${last.height}`} className="text-[var(--color-accent)] hover:underline">
                  #{formatNumber(last.height)} · {timeAgo(last.time)}
                </Link>
              ) : (
                <span className="text-[var(--color-text-dim)]">—</span>
              )}
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[var(--color-text-faint)]">Blocks 24h</span>
              <span className="text-[var(--color-text-dim)]">
                {formatNumber(stats?.blocks_24h ?? 0)} · {(stats?.share_pct_24h ?? 0).toFixed(1)}%
              </span>
            </div>
          </div>

          <div className="mt-3 h-1 rounded-full bg-white/5 overflow-hidden" aria-hidden>
            <div
              className="h-full rounded-full transition-all"
              style={{ width: `${Math.min(100, Math.max(0, stats?.share_pct_24h ?? 0))}%`, background: lane.bar }}
            />
          </div>
        </>
      )}
    </div>
  );
}

export function PowAlgos() {
  const { stats, loading } = useNetwork();
  const algos = (stats?.algos ?? undefined) as NetworkAlgos | undefined;

  return (
    <section>
      <div className="mb-5">
        <h2 className="text-xl lg:text-2xl font-bold tracking-tight">Proof-of-Work — three algorithms, one chain</h2>
        <p className="text-sm text-[var(--color-text-dim)] mt-1">
          Tri-PoW: every lane mines the same Interchained chain, each with its own difficulty and hashrate.
        </p>
      </div>
      <div className="grid md:grid-cols-3 gap-3 lg:gap-4">
        {LANES.map((lane) => (
          <LaneCard key={lane.key} lane={lane} stats={algos?.[lane.key]} loading={loading} />
        ))}
      </div>
    </section>
  );
}
