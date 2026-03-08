'use client';

import { useRouter } from 'next/navigation';
import { useState, useTransition } from 'react';
import { useTranslations, useLocale } from 'next-intl';
import { Search, Globe } from 'lucide-react';

const LOCALE_COOKIE = 'MATCHINDEX_LOCALE';

export function TopBar() {
  const router = useRouter();
  const t = useTranslations('topBar');
  const locale = useLocale();
  const [query, setQuery] = useState('');
  const [isPending, startTransition] = useTransition();

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      router.push(`/search?q=${encodeURIComponent(query.trim())}`);
    }
  };

  const switchLocale = () => {
    const newLocale = locale === 'en' ? 'ko' : 'en';
    document.cookie = `${LOCALE_COOKIE}=${newLocale}; path=/; max-age=31536000; SameSite=Lax`;
    startTransition(() => {
      router.refresh();
    });
  };

  return (
    <header className="flex h-11 items-center justify-between border-b border-border bg-surface-1 px-6 shrink-0">
      <form onSubmit={handleSearch} className="flex items-center gap-2">
        <Search className="h-3.5 w-3.5 text-text-muted" />
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t('searchPlaceholder')}
          className="w-72 bg-transparent text-[13px] text-text-primary placeholder:text-text-muted outline-none"
        />
      </form>

      <button
        onClick={switchLocale}
        disabled={isPending}
        className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[12px] font-medium
                   bg-surface-2 hover:bg-surface-3 text-text-secondary hover:text-text-primary
                   transition-colors disabled:opacity-50 disabled:cursor-wait"
        aria-label="Switch language"
      >
        <Globe className="h-3 w-3" />
        {isPending ? '...' : t('langToggle')}
      </button>
    </header>
  );
}
