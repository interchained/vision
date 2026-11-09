'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { api } from '@/lib/api';
import { useSse } from '@/lib/sse';
import { Empty } from '@/components/empty';
import { formatItc, formatFeeRate } from '@/lib/format';
import { formatNumber, humanBytes, shortHash, timeAgo } from '@/lib/utils';
import { Sk, SKELETON_ROWS } from '@/components/skeleton';
import { NodeSyncingInline } from '@/components/node-syncing';
import { useSyncState } from '@/hooks/use-sync';
import { FeeCalculator } from '@/components/fee-calculator';

type Tab = 'summary' | 'txs' | 'projected' | 'estimate';

export default function MempoolPage() {
  const [tab, setTab] = useState<Tab>('summary');
  const [summary, setSummary] = useState<any>(null);
  const [txs, setTxs] = useState<any[]>([]);
  const [projected, setProjected] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const syncing = useSyncState();

  const load = async () => {
    try {
      const [s, t, p] = await Promise.all([
        api.mempoolSummary(),
        api.mempoolTxs(200),
        api.mempoolProjected(8),
      ]);
      setSummary(s);
      setTxs(t.items);
      setProjected(p.items);
    } catch (_e) {
      // ignore fetch errors
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    const i = setInterval(load, 5000);
    return () => clearInterval(i);
  }, []);

  useSse((ev) => { if (ev.type === 'mempool') load(); });

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl lg:text-3xl font-bold">Mempool</h1>
        <p className="text-sm text-[var(--color-text-dim)] mt-1">
          Live view of unconfirmed transactions waiting to enter the next blocks.
        </p>
      </div>

      <div className="card overflow-hidden">
        <div className="flex border-b border-[var(--color-border)]">
          {([
            ['summary', 'Summary'],
            ['txs', `Transactions${summary ? ` (${formatNumber(summary.tx_count)})` : ''}`],
            ['projected', 'Projected Blocks'],
            ['estimate', 'Fee Calculator'],
          ] as [Tab, string][]).map(([id, label]) => (
            <button
              key={id}
              onClick={() => setTab(id)}
              className={`px-4 py-3 text-sm border-b-2 transition ${
                tab === id
                  ? 'border-[var(--color-accent)] text-white'
                  : 'border-transparent text-[var(--color-text-dim)] hover:text-white'
              }`}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="p-5 lg:p-6">
          {tab === 'summary' && (
            loading ? (
              <div className="space-y-6">
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                  {[0,1,2,3].map((i) => <Sk.Card key={i} />)}
                </div>
                <div className="grid grid-cols-3 gap-3">
                  {[0,1,2].map((i) => <Sk.Card key={i} />)}
                </div>
              </div>
            ) : !summary ? (
              syncing ? <NodeSyncingInline /> : <div className="py-8 text-center text-sm text-[var(--color-text-dim)]">No mempool data available.</div>
            ) : (
              <div className="space-y-6">
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                  <Stat label="Pending TX" value={formatNumber(summary.tx_count)} />
                  <Stat label="Total vSize" value={humanBytes(summary.vsize_total)} />
                  <Stat label="Total Fees" value={formatItc(summary.fee_total_sats)} accent />
                  <Stat label="Median Fee Rate" value={formatFeeRate(summary.fee_rate_median)} accent />
                </div>
                <div>
                  <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-faint)] mb-3">
                    Fee Categories
                  </h3>
                  <div className="grid grid-cols-3 gap-3">
                    {(['low', 'medium', 'high'] as const).map((k, idx) => (
                      <div key={k} className="card p-4">
                        <div className="text-xs text-[var(--color-text-faint)] uppercase">{k}</div>
                        <div className="mt-2 text-lg mono font-semibold" style={{ color: idx === 0 ? '#888' : idx === 1 ? '#4dabff' : '#f0b32b' }}>
                          {formatFeeRate(summary.fee_categories[k])}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
                {summary.fee_histogram?.length > 0 && (
                  <div>
                    <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-faint)] mb-3">
                      Fee Histogram (sat/vB → vsize)
                    </h3>
                    <div className="card p-4">
                      <div className="grid gap-1" style={{ gridTemplateColumns: `repeat(${Math.min(30, summary.fee_histogram.length)}, 1fr)` }}>
                        {summary.fee_histogram.slice(0, 30).map(([rate, vsize]: [number, number], i: number) => {
                          const max = Math.max(...summary.fee_histogram.map((h: any) => h[1]));
                          const h = max ? (vsize / max) * 80 : 0;
                          return (
                            <div key={i} className="flex flex-col items-center gap-1" title={`${rate} sat/vB · ${humanBytes(vsize)}`}>
                              <div className="w-full bg-[var(--color-accent)]/30 rounded-t" style={{ height: `${h + 4}px` }} />
                              <div className="text-[9px] text-[var(--color-text-faint)] mono">{rate.toFixed(0)}</div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )
          )}

          {tab === 'txs' && (
            loading ? (
              <div className="divide-y divide-[var(--color-border-soft)] -mx-5">
                {SKELETON_ROWS.slice(0, 12).map((i) => <Sk.TxRow key={i} />)}
              </div>
            ) : txs.length === 0 ? (
              syncing ? <NodeSyncingInline /> : <Empty title="Mempool is empty" />
            ) : (
              <div className="divide-y divide-[var(--color-border-soft)] -mx-5">
                {txs.map((t) => (
                  <Link key={t.txid} href={`/tx/${t.txid}`} className="flex items-center justify-between px-5 py-2.5 hover:bg-white/5 transition mono text-xs">
                    <span className="text-[var(--color-text-dim)]">{shortHash(t.txid, 12, 8)}</span>
                    <div className="flex items-center gap-4">
                      <span className="text-[var(--color-text-dim)]">{humanBytes(t.vsize)}</span>
                      <span className="text-[var(--color-gold)]">{formatFeeRate(t.fee_rate_sat_vbyte)}</span>
                      <span className="text-[var(--color-text-faint)]">{timeAgo(t.time)}</span>
                    </div>
                  </Link>
                ))}
              </div>
            )
          )}

          {tab === 'projected' && (
            loading ? (
              <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-3">
                {[0,1,2,3].map((i) => <Sk.Card key={i} />)}
              </div>
            ) : projected.length === 0 ? (
              syncing ? <NodeSyncingInline /> : <Empty title="No projected blocks (mempool is empty)" />
            ) : (
              <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-3">
                {projected.map((p) => (
                  <div key={p.index} className="card p-4">
                    <div className="text-xs text-[var(--color-text-faint)] uppercase">Block +{p.index}</div>
                    <div className="mt-2 text-lg mono font-semibold text-[var(--color-gold)]">
                      {formatFeeRate(p.fee_rate_median)}
                    </div>
                    <div className="mt-2 text-[11px] text-[var(--color-text-dim)] mono">
                      {p.tx_count} tx · {humanBytes(p.vsize)}
                    </div>
                    <div className="text-[11px] text-[var(--color-text-dim)] mono">
                      Fees: {formatItc(p.fees_sats)}
                    </div>
                  </div>
                ))}
              </div>
            )
          )}
          {tab === 'estimate' && (
            <FeeCalculator projected={projected} />
          )}
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <div className="card p-4">
      <div className="text-[10px] uppercase tracking-wider text-[var(--color-text-faint)]">{label}</div>
      <div className={`mt-2 text-lg mono font-semibold ${accent ? 'text-[var(--color-gold)]' : ''}`}>{value}</div>
    </div>
  );
}
