import { TipCard } from '@/components/tip-card';
import { RecentBlocks } from '@/components/recent-blocks';
import { NetworkHealth } from '@/components/network-health';
import { PowAlgos } from '@/components/pow-algos';
import { RecentTxs } from '@/components/recent-txs';
import { AddressWatchlist } from '@/components/address-watchlist';

export default function HomePage() {
  return (
    <div className="space-y-8 lg:space-y-10">
      <section>
        <div className="flex flex-col sm:flex-row sm:items-end justify-between mb-5 gap-2">
          <div>
            <h1 className="text-2xl lg:text-3xl font-bold tracking-tight">Network Health</h1>
            <p className="text-sm text-[var(--color-text-dim)] mt-1">
              Real-time view of the Interchained mainnet — hashrate, difficulty, mempool, and tip.
            </p>
          </div>
        </div>
        <NetworkHealth />
      </section>

      <PowAlgos />

      <section className="grid lg:grid-cols-3 gap-6">
        <div className="lg:col-span-1 flex flex-col gap-6">
          <TipCard />
          <AddressWatchlist />
        </div>
        <div className="lg:col-span-2 grid md:grid-cols-2 gap-6">
          <RecentBlocks limit={8} />
          <RecentTxs limit={8} />
        </div>
      </section>
    </div>
  );
}
