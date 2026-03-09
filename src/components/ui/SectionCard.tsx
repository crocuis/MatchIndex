import { cn } from '@/lib/utils';

interface SectionCardProps {
  title: React.ReactNode;
  children: React.ReactNode;
  action?: React.ReactNode;
  className?: string;
  noPadding?: boolean;
}

export function SectionCard({ title, children, action, className, noPadding }: SectionCardProps) {
  return (
    <div className={cn('rounded border border-border bg-surface-1 shadow-sm overflow-hidden flex flex-col', className)}>
      <div className="flex items-center justify-between border-b border-border bg-surface-2/50 px-3 py-2">
        <h3 className="text-[11px] font-bold uppercase tracking-wider text-text-primary flex items-center gap-2">
          <div className="w-1 h-3 bg-accent-magenta rounded-full" />
          {title}
        </h3>
        {action && <div className="flex items-center">{action}</div>}
      </div>
      <div className={cn('flex-1', !noPadding && 'p-3')}>{children}</div>
    </div>
  );
}
