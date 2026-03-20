import { SectionCard } from '@/components/ui/SectionCard';
import { cn } from '@/lib/utils';

interface DetailTabSkeletonProps {
  title: string;
  className?: string;
  primaryCount?: number;
  secondaryCount?: number;
  sidebarCount?: number;
  sidebarOnMatchesOnly?: boolean;
}

function SkeletonLine({ className }: { className?: string }) {
  return <div className={cn('h-3 rounded bg-surface-3/80 animate-pulse', className)} />;
}

function SkeletonSection({ lines = 5 }: { lines?: number }) {
  return (
    <div className="space-y-3">
      <SkeletonLine className="h-4 w-32" />
      <div className="space-y-2">
        {Array.from({ length: lines }, (_, index) => (
          <SkeletonLine key={index} className={cn(index % 3 === 0 ? 'w-full' : index % 3 === 1 ? 'w-5/6' : 'w-2/3')} />
        ))}
      </div>
    </div>
  );
}

export function DetailTabSkeleton({
  title,
  className,
  primaryCount = 1,
  secondaryCount = 1,
  sidebarCount = 1,
}: DetailTabSkeletonProps) {
  return (
    <div className={cn('grid grid-cols-12 gap-4', className)}>
      <div className="col-span-8 space-y-4">
        <SectionCard title={title}>
          <SkeletonSection lines={6} />
        </SectionCard>
        {Array.from({ length: Math.max(0, primaryCount - 1) }, (_, index) => (
          <SectionCard key={`primary-${index}`} title={<div className="h-4 w-32 animate-pulse rounded bg-surface-3/80" />}>
            <SkeletonSection lines={5} />
          </SectionCard>
        ))}
        {Array.from({ length: secondaryCount }, (_, index) => (
          <SectionCard key={`secondary-${index}`} title={<div className="h-4 w-40 animate-pulse rounded bg-surface-3/80" />}>
            <div className="space-y-2">
              {Array.from({ length: 4 }, (_, itemIndex) => (
                <div key={itemIndex} className="rounded border border-border-subtle bg-surface-2/50 p-3">
                  <SkeletonLine className="mb-2 w-24" />
                  <SkeletonLine className="w-full" />
                </div>
              ))}
            </div>
          </SectionCard>
        ))}
      </div>

      <div className="col-span-4 space-y-4">
        {Array.from({ length: sidebarCount }, (_, index) => (
          <SectionCard key={`sidebar-${index}`} title={<div className="h-4 w-24 animate-pulse rounded bg-surface-3/80" />}>
            <SkeletonSection lines={index === 0 ? 4 : 3} />
          </SectionCard>
        ))}
      </div>
    </div>
  );
}
