'use client';

import { cn } from '@/lib/utils';

export const ALGO_LABELS: Record<string, string> = {
  sha256: 'SHA-256',
  yespower: 'Yespower',
  ethash: 'Ethash',
};

const ALGO_COLORS: Record<string, string> = {
  sha256: 'var(--color-gold)',
  yespower: 'var(--color-accent)',
  ethash: 'var(--color-success)',
};

export function AlgoBadge({ algo, className }: { algo?: string | null; className?: string }) {
  if (!algo) return null;
  const color = ALGO_COLORS[algo] ?? 'var(--color-text-dim)';
  return (
    <span
      className={cn(
        'inline-flex items-center rounded-full border px-1.5 py-px text-[10px] font-semibold uppercase tracking-wide mono whitespace-nowrap',
        className,
      )}
      style={{ color, borderColor: color }}
    >
      {ALGO_LABELS[algo] ?? algo}
    </span>
  );
}
