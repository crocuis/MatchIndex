import { DetailTabSkeleton } from '@/components/ui/DetailTabSkeleton';

export default function Loading() {
  return (
    <div>
      <DetailTabSkeleton title="Overview" primaryCount={1} secondaryCount={1} sidebarCount={1} />
    </div>
  );
}
