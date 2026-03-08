import { cn } from '@/lib/utils';

interface EmptyStateProps {
  message: string;
  className?: string;
}

export function EmptyState({ message, className }: EmptyStateProps) {
  return (
    <div className={cn('flex items-center justify-center py-8 text-[13px] text-text-muted', className)}>
      {message}
    </div>
  );
}
