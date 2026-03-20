'use client';

import { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { cn } from '@/lib/utils';

const StaticDetailTabsContext = createContext<{ activeTab: string }>({ activeTab: 'overview' });

interface StaticDetailTabItem {
  key: string;
  label: string;
  content: React.ReactNode;
}

interface StaticDetailTabsProps {
  initialTab: string;
  basePath: string;
  className?: string;
  defaultTab?: string;
  query?: Record<string, string | undefined>;
  tabs: StaticDetailTabItem[];
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

export function StaticDetailTabs({
  initialTab,
  basePath,
  className,
  defaultTab = 'overview',
  query,
  tabs,
}: StaticDetailTabsProps) {
  const availableKeys = useMemo(() => new Set(tabs.map((tab) => tab.key)), [tabs]);
  const normalizedInitialTab = availableKeys.has(initialTab) ? initialTab : defaultTab;
  const [activeTab, setActiveTab] = useState(normalizedInitialTab);

  useEffect(() => {
    setActiveTab(normalizedInitialTab);
  }, [normalizedInitialTab]);

  return (
    <StaticDetailTabsContext.Provider value={{ activeTab }}>
      <div className={className}>
        <div className="-mx-1 overflow-x-auto border-b border-border px-1">
          <div className="flex min-w-max gap-1" role="tablist" aria-label="Detail sections">
            {tabs.map((tab) => {
              const isActive = tab.key === activeTab;

              return (
                <button
                  key={tab.key}
                  type="button"
                  role="tab"
                  aria-selected={isActive}
                  aria-controls={`detail-panel-${tab.key}`}
                  onClick={() => {
                    setActiveTab(tab.key);
                    window.history.replaceState(
                      window.history.state,
                      '',
                      buildDetailTabHref(basePath, tab.key, defaultTab, query),
                    );
                  }}
                  className={cn(
                    'whitespace-nowrap rounded-t-md border-b-2 -mb-px px-3 py-2 text-[13px] font-medium transition-colors',
                    isActive
                      ? 'border-accent-emerald bg-surface-2/40 text-text-primary'
                      : 'border-transparent text-text-muted hover:bg-surface-2/20 hover:text-text-secondary'
                  )}
                >
                  {tab.label}
                </button>
              );
            })}
          </div>
        </div>

        <div className="mt-4">
          {tabs.map((tab) => {
            const isActive = tab.key === activeTab;

            if (!isActive) {
              return null;
            }

            return (
              <div
                key={tab.key}
                id={`detail-panel-${tab.key}`}
                role="tabpanel"
                aria-hidden={false}
              >
                {tab.content}
              </div>
            );
          })}
        </div>
      </div>
    </StaticDetailTabsContext.Provider>
  );
}

export function useStaticDetailTabActive(tabKey: string) {
  const { activeTab } = useContext(StaticDetailTabsContext);
  return activeTab === tabKey;
}
