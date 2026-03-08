import { cn } from '@/lib/utils';

interface PageHeaderProps {
  title: React.ReactNode;
  subtitle?: string;
  meta?: string;
  children?: React.ReactNode;
  className?: string;
}

export function PageHeader({ title, subtitle, meta, children, className }: PageHeaderProps) {
  return (
    <div className={cn('mb-4 flex items-start justify-between', className)}>
      <div>
        <h1 className="text-lg font-semibold text-text-primary">{title}</h1>
        {subtitle && <p className="text-[13px] text-text-secondary mt-0.5">{subtitle}</p>}
        {meta && <p className="text-[11px] text-text-muted mt-0.5">{meta}</p>}
      </div>
      {children && <div className="flex items-center gap-2">{children}</div>}
    </div>
  );
}
