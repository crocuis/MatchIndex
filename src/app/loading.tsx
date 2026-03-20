function DashboardLoadingBlock({ className }: { className?: string }) {
  return <div className={className} />;
}

export default function Loading() {
  return (
    <div className="flex flex-col gap-6 animate-pulse">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div className="space-y-2">
          <DashboardLoadingBlock className="h-3 w-24 rounded bg-surface-2" />
          <DashboardLoadingBlock className="h-9 w-56 rounded bg-surface-2" />
          <DashboardLoadingBlock className="h-4 w-72 rounded bg-surface-2" />
        </div>
        <DashboardLoadingBlock className="h-24 w-full rounded-lg bg-surface-2 lg:w-64" />
      </div>

      <section className="space-y-4">
        <DashboardLoadingBlock className="h-3 w-36 rounded bg-surface-2" />
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
          <DashboardLoadingBlock className="h-72 rounded-lg bg-surface-2" />
          <DashboardLoadingBlock className="h-72 rounded-lg bg-surface-2" />
        </div>
      </section>

      <section className="space-y-4">
        <DashboardLoadingBlock className="h-3 w-40 rounded bg-surface-2" />
        <DashboardLoadingBlock className="h-28 rounded-lg bg-surface-2" />
        <div className="grid grid-cols-1 gap-4 md:gap-6 lg:grid-cols-12">
          <DashboardLoadingBlock className="h-[26rem] rounded-lg bg-surface-2 lg:col-span-8" />
          <div className="space-y-4 lg:col-span-4">
            <DashboardLoadingBlock className="h-44 rounded-lg bg-surface-2" />
            <DashboardLoadingBlock className="h-64 rounded-lg bg-surface-2" />
          </div>
        </div>
      </section>
    </div>
  );
}
