'use client';

import { useState } from 'react';
import { cn } from '@/lib/utils';

export interface TabGroupTab {
  key: string;
  label: React.ReactNode;
  content?: React.ReactNode;
  render?: () => React.ReactNode;
}

interface TabGroupProps {
  tabs: readonly TabGroupTab[];
  defaultTab?: string;
  className?: string;
}

export function TabGroup({ tabs, defaultTab, className }: TabGroupProps) {
  const [activeTab, setActiveTab] = useState(defaultTab ?? tabs[0]?.key ?? '');
  const activeTabEntry = tabs.find((tab) => tab.key === activeTab);
  const activeContent = activeTabEntry?.render ? activeTabEntry.render() : activeTabEntry?.content;

  return (
    <div className={className}>
      <div className="-mx-1 mb-4 overflow-x-auto border-b border-border px-1">
        <div className="flex min-w-max gap-1">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={cn(
                'whitespace-nowrap rounded-t-md px-3 py-2 text-[13px] font-medium transition-colors border-b-2 -mb-px',
                activeTab === tab.key
                  ? 'text-text-primary border-accent-emerald bg-surface-2/40'
                  : 'text-text-muted border-transparent hover:text-text-secondary hover:bg-surface-2/20'
              )}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>
      <div>{activeContent}</div>
    </div>
  );
}
