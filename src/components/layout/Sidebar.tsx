'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useTranslations } from 'next-intl';
import { NAV_GROUPS } from '@/config/nav';
import { cn } from '@/lib/utils';

// Map nav config keys to translation keys
const navLabelKeys: Record<string, string> = {
  Dashboard: 'dashboard',
  Results: 'results',
  Search: 'search',
  Leagues: 'leagues',
  Clubs: 'clubs',
  Players: 'players',
  Nations: 'nations',
};

const groupTitleKeys: Record<string, string> = {
  Overview: 'overview',
  Data: 'data',
};

export function Sidebar() {
  const pathname = usePathname();
  const t = useTranslations('nav');

  return (
    <aside className="flex w-52 flex-col border-r border-border bg-surface-1 shrink-0">
      {/* Logo */}
      <div className="flex h-11 items-center border-b border-border px-4">
        <Link href="/" className="flex items-center gap-2">
          <div className="h-5 w-5 rounded bg-accent-emerald" />
          <span className="font-mono text-xs font-bold tracking-widest text-text-primary uppercase">
            {t('brand')}
          </span>
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto py-2">
        {NAV_GROUPS.map((group) => (
          <div key={group.title} className="mb-2">
            <div className="px-4 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-text-muted">
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
                    'flex items-center gap-2.5 px-4 py-1.5 text-[13px] transition-colors',
                    isActive
                      ? 'bg-surface-3 text-text-primary border-r-2 border-accent-emerald'
                      : 'text-text-secondary hover:bg-surface-2 hover:text-text-primary'
                  )}
                >
                  <item.icon className="h-3.5 w-3.5 shrink-0" />
                  {t(navLabelKeys[item.label] ?? item.label)}
                </Link>
              );
            })}
          </div>
        ))}
      </nav>

      {/* Footer */}
      <div className="border-t border-border px-4 py-2">
        <div className="text-[10px] text-text-muted">{t('version')}</div>
      </div>
    </aside>
  );
}
