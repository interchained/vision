'use client';

import { useEffect, useState } from 'react';
import { vision } from '@/lib/sdk-client';
import { api } from '@/lib/api';
import { useSse } from '@/lib/sse';

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error('timeout')), ms),
    ),
  ]);
}

export function useNetwork() {
  const [stats, setStats] = useState<any>(null);
  const [price, setPrice] = useState<any>(null);
  const [supply, setSupply] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [tipUpdated, setTipUpdated] = useState(0);

  useEffect(() => {
    // Fast lane: network stats + price every 5s so the dashboard and
    // footer feel live. Supply stays on its own slow cadence — it can
    // ride on an uncached gettxoutsetinfo and must not gate the rest.
    const loadFast = async () => {
      const [statsResult, priceResult] = await Promise.allSettled([
        withTimeout(vision.getNetworkStats(), 8000),
        withTimeout(vision.getPrice(), 8000),
      ]);
      if (statsResult.status === 'fulfilled') setStats(statsResult.value);
      if (priceResult.status === 'fulfilled') setPrice(priceResult.value);
      setLoading(false);
    };
    const loadSupply = async () => {
      const [supplyResult] = await Promise.allSettled([
        withTimeout(api.supply(), 65000),
      ]);
      if (supplyResult.status === 'fulfilled') setSupply(supplyResult.value);
    };
    loadFast();
    loadSupply();
    const fast = setInterval(loadFast, 5000);
    const slow = setInterval(loadSupply, 60000);
    return () => {
      clearInterval(fast);
      clearInterval(slow);
    };
  }, []);

  useSse((ev) => {
    if (ev.type === 'block') setTipUpdated((v) => v + 1);
    if (ev.type === 'snapshot' && ev.data?.price) setPrice(ev.data.price);
  });

  return { stats, price, supply, loading, tipUpdated };
}
