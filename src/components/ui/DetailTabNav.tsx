import Link from 'next/link';
import { cn } from '@/lib/utils';

interface DetailTabNavItem {
  key: string;
  label: string;
}

interface DetailTabNavProps {
  activeTab: string;
  basePath: string;
  className?: string;
  defaultTab?: string;
  query?: Record<string, string | undefined>;
  tabs: DetailTabNavItem[];
}

function buildDetailTabHref(
  basePath: string,
  tabKey: string,
  defaultTab: string,
  query?: Record<string, string | undefined>,
) {
  const params = new URLSearchParams();

  for (const [key, value] of Object.entries(query ?? {})) {
    if (!value) {
      continue;
    }

    params.set(key, value);
  }

  if (tabKey !== defaultTab) {
    params.set('tab', tabKey);
  }

  const queryString = params.toString();
  return queryString ? `${basePath}?${queryString}` : basePath;
}

export function DetailTabNav({
  activeTab,
  basePath,
  className,
  defaultTab = 'overview',
  query,
  tabs,
}: DetailTabNavProps) {
  return (
    <div className={cn('-mx-1 overflow-x-auto border-b border-border px-1', className)}>
      <div className="flex min-w-max gap-1">
        {tabs.map((tab) => {
          const isActive = tab.key === activeTab;

          return (
            <Link
              key={tab.key}
              href={buildDetailTabHref(basePath, tab.key, defaultTab, query)}
              prefetch
              scroll={false}
              className={cn(
                'whitespace-nowrap rounded-t-md border-b-2 -mb-px px-3 py-2 text-[13px] font-medium transition-colors',
                isActive
                  ? 'border-accent-emerald bg-surface-2/40 text-text-primary'
                  : 'border-transparent text-text-muted hover:bg-surface-2/20 hover:text-text-secondary'
              )}
            >
              {tab.label}
            </Link>
          );
        })}
      </div>
    </div>
  );
}
