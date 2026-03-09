import { cn } from '@/lib/utils';

interface PageHeaderProps {
  title: React.ReactNode;
  subtitle?: React.ReactNode;
  meta?: string;
  children?: React.ReactNode;
  className?: string;
}

export function PageHeader({ title, subtitle, meta, children, className }: PageHeaderProps) {
  return (
    <div className={cn('mb-6 flex items-end justify-between border-b border-border pb-4', className)}>
      <div>
        <h1 className="text-2xl font-bold tracking-tight text-text-primary">{title}</h1>
        {subtitle && <p className="text-[13px] text-text-secondary mt-1">{subtitle}</p>}
        {meta && <p className="text-[11px] text-text-muted mt-1 font-mono uppercase tracking-wider">{meta}</p>}
      </div>
      {children && <div className="flex items-center gap-3">{children}</div>}
    </div>
  );
}
