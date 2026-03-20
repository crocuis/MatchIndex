'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useTranslations } from 'next-intl';
import { NAV_GROUPS } from '@/config/nav';
import { cn } from '@/lib/utils';

// Map nav config keys to translation keys
const navLabelKeys: Record<string, string> = {
  Dashboard: 'dashboard',
  'World Cup': 'worldCup',
  Results: 'results',
  Search: 'search',
  Competitions: 'competitions',
  Clubs: 'clubs',
  Players: 'players',
  Nations: 'nations',
};

const groupTitleKeys: Record<string, string> = {
  Overview: 'overview',
  Data: 'data',
};

interface SidebarProps {
  appVersion: string;
  hasDatabase: boolean;
}

export function Sidebar({ appVersion, hasDatabase }: SidebarProps) {
  const pathname = usePathname();
  const t = useTranslations('nav');
  const dataSourceLabel = hasDatabase ? t('sourceDatabase') : t('sourceMock');

  return (
    <aside className="flex w-48 flex-col border-r border-border bg-surface-1 shrink-0 relative z-0">
      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto py-4">
        {NAV_GROUPS.map((group) => (
          <div key={group.title} className="mb-5">
            <div className="px-4 py-1 text-[10px] font-bold uppercase tracking-widest text-text-muted mb-1">
              {t(groupTitleKeys[group.title] ?? group.title)}
            </div>
            {group.items.map((item) => {
              const isActive =
                item.href === '/'
                  ? pathname === '/'
                  : pathname === item.href || pathname.startsWith(item.href + '/');

              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={cn(
                    'flex items-center gap-3 px-4 py-1.5 text-[13px] transition-all relative',
                    isActive
                      ? 'text-text-primary font-medium bg-surface-2/50'
                      : 'text-text-secondary hover:text-text-primary hover:bg-surface-2/30'
                  )}
                >
                  {isActive && (
                    <div className="absolute left-0 top-0 bottom-0 w-1 bg-gradient-to-b from-accent-magenta to-accent-violet" />
                  )}
                  <item.icon className={cn('h-4 w-4 shrink-0', isActive ? 'text-accent-magenta' : 'text-text-muted')} />
                  {t(navLabelKeys[item.label] ?? item.label)}
                </Link>
              );
            })}
          </div>
        ))}
      </nav>

      {/* Footer */}
      <div className="border-t border-border px-4 py-3 bg-surface-1/50">
        <div className="text-[10px] text-text-muted font-mono">
          {t('version', { version: appVersion, source: dataSourceLabel, fallback: `v${appVersion}` })}
        </div>
      </div>
    </aside>
  );
}
