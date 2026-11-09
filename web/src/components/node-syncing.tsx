'use client';

import { useEffect, useState } from 'react';
import { api } from '@/lib/api';
import { formatNumber } from '@/lib/utils';

/* ── Types ─────────────────────────────────────────────────────────── */
interface HistorySync {
  active: boolean;
  store?: string;
  checkpoint_height?: number;
  lowest_synced?: number;
  pct?: number;
  complete?: boolean;
  running?: boolean;
  blocks_written?: number;
}

interface HealthInfo {
  status: string;
  rpc: boolean;
  electrumx: boolean;
  db: boolean;
  sync?: HistorySync;
}

/* ── Sync status strip ───────────────────────────────────────────────
   The explorer serves from the chain tip immediately on the embedded
   store; history fills in backwards in the background. This strip is
   how users SEE that progress — it never blocks the UI. It shows:
     • history sync progress while the backfill is running
     • a degraded-connection note when a backend dependency is down
     • an unreachable note when the API itself can't be reached
   …and renders nothing when everything is caught up and healthy. */
export function NodeSyncingBanner() {
  const [health, setHealth] = useState<HealthInfo | null>(null);
  const [unreachable, setUnreachable] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const check = async () => {
      try {
        const h = await api.health();
        if (cancelled) return;
        setHealth(h);
        setUnreachable(false);
      } catch {
        if (!cancelled) setUnreachable(true);
      }
    };
    check();
    const t = setInterval(check, 2_000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, []);

  if (unreachable) {
    return (
      <div className="sync-strip sync-strip-down" role="status">
        <span className="sync-strip-dot" style={{ background: 'var(--color-danger)' }} />
        <span>
          API unreachable — retrying automatically. Anything already on
          screen may be stale.
        </span>
      </div>
    );
  }

  if (!health) return null;

  const sync = health.sync;
  const syncing = !!sync?.active && !sync.complete;
  const degraded = !health.rpc || !health.electrumx || !health.db;
  if (!syncing && !degraded) return null;

  const pct = sync?.pct ?? 0;
  const lowest = sync?.lowest_synced ?? 0;
  const checkpoint = sync?.checkpoint_height ?? 0;

  return (
    <div
      className={`sync-strip ${syncing ? 'sync-strip-syncing' : 'sync-strip-degraded'}`}
      role="status"
    >
      <div className="sync-strip-row">
        {syncing ? (
          <span>
            <strong>Syncing history — {pct.toFixed(1)}%</strong>
            {' · '}live at the tip, history filled back to{' '}
            <span className="mono">#{formatNumber(lowest)}</span>
            {checkpoint > 0 && (
              <>
                {' '}of <span className="mono">#{formatNumber(checkpoint)}</span>
              </>
            )}
            {' · '}search covers everything synced so far
          </span>
        ) : (
          <span>
            <strong>Node connection degraded</strong>
            {' · '}some data may be stale while the backend reconnects
          </span>
        )}
        <span className="sync-strip-chips">
          <MiniChip label="RPC" ok={health.rpc} />
          <MiniChip label="ElectrumX" ok={health.electrumx} />
          <MiniChip label="DB" ok={health.db} />
        </span>
      </div>
      {syncing && (
        <div className="sync-strip-track">
          <div className="sync-strip-fill" style={{ width: `${pct}%` }} />
        </div>
      )}
    </div>
  );
}

function MiniChip({ label, ok }: { label: string; ok: boolean }) {
  return (
    <span className="sync-strip-chip">
      <span
        className="sync-strip-dot"
        style={{
          background: ok ? 'var(--color-success)' : 'var(--color-danger)',
          boxShadow: ok
            ? '0 0 6px var(--color-success)'
            : '0 0 6px var(--color-danger)',
        }}
      />
      <span style={{ color: ok ? 'var(--color-success)' : 'var(--color-danger)' }}>
        {label}
      </span>
    </span>
  );
}

/* ── Compatibility exports (used by other pages) ───────────────────── */
export function NodeSyncingPage() {
  return (
    <div className="flex flex-col items-center justify-center py-20 gap-4">
      <div className="w-16 h-16 rounded-full border-2 border-[var(--color-accent)] border-t-transparent animate-spin" />
      <p className="text-sm text-[var(--color-text-dim)]">Loading data…</p>
    </div>
  );
}

export function NodeSyncingInline() {
  return (
    <div className="flex items-center justify-center py-8 gap-3">
      <div className="w-5 h-5 rounded-full border-2 border-[var(--color-accent)] border-t-transparent animate-spin" />
      <span className="text-sm text-[var(--color-text-dim)]">Syncing…</span>
    </div>
  );
}
