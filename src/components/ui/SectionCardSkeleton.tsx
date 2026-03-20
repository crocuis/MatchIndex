import { SectionCard } from '@/components/ui/SectionCard';
import { cn } from '@/lib/utils';

interface SectionCardSkeletonProps {
  title: string;
  className?: string;
  rows?: number;
  blocks?: number;
  compact?: boolean;
}

function SkeletonLine({ className }: { className?: string }) {
  return <div className={cn('h-3 rounded bg-surface-3/80 animate-pulse', className)} />;
}

export function SectionCardSkeleton({
  title,
  className,
  rows = 4,
  blocks = 0,
  compact = false,
}: SectionCardSkeletonProps) {
  return (
    <SectionCard title={title} className={className}>
      <div className={cn('space-y-3', compact && 'space-y-2')}>
        {Array.from({ length: rows }, (_, index) => (
          <div key={`row-${index}`} className="rounded border border-border-subtle bg-surface-2/40 p-3">
            <SkeletonLine className={cn(index % 2 === 0 ? 'w-1/3' : 'w-1/4')} />
            <SkeletonLine className="mt-2 w-full" />
          </div>
        ))}
        {Array.from({ length: blocks }, (_, index) => (
          <div key={`block-${index}`} className="rounded border border-border-subtle bg-surface-2/40 p-3">
            <SkeletonLine className="mb-2 h-4 w-28" />
            <div className="space-y-2">
              <SkeletonLine className="w-full" />
              <SkeletonLine className="w-5/6" />
              <SkeletonLine className="w-2/3" />
            </div>
          </div>
        ))}
      </div>
    </SectionCard>
  );
}
