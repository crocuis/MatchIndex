import { cn } from '@/lib/utils';

interface Stat {
  label: string;
  value: string | number;
  highlight?: boolean;
}

interface StatPanelProps {
  stats: Stat[];
  columns?: 2 | 3 | 4;
  className?: string;
}

export function StatPanel({ stats, columns = 3, className }: StatPanelProps) {
  const gridClass = {
    2: 'grid-cols-2',
    3: 'grid-cols-3',
    4: 'grid-cols-4',
  }[columns];

  return (
    <div className={cn('grid gap-px bg-border rounded-lg overflow-hidden', gridClass, className)}>
      {stats.map((stat) => (
        <div key={stat.label} className="bg-surface-1 px-3 py-2.5">
          <div className="text-[10px] uppercase tracking-wider text-text-muted mb-0.5">
            {stat.label}
          </div>
          <div
            className={cn(
              'text-[15px] font-semibold tabular-nums',
              stat.highlight ? 'text-accent-emerald' : 'text-text-primary'
            )}
          >
            {stat.value}
          </div>
        </div>
      ))}
    </div>
  );
}
