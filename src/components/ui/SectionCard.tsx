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
    <div className={cn('rounded-lg border border-border bg-surface-1', className)}>
      <div className="flex items-center justify-between border-b border-border-subtle px-4 py-2.5">
        <h3 className="text-[13px] font-semibold text-text-primary">{title}</h3>
        {action}
      </div>
      <div className={cn(!noPadding && 'p-4')}>{children}</div>
    </div>
  );
}
