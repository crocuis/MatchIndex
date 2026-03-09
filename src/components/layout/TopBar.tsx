'use client';

import Link from 'next/link';
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
    <header className="flex h-14 items-center justify-between border-b border-border bg-surface-1 px-4 shrink-0 shadow-sm relative z-10">
      <div className="flex items-center gap-6">
        <Link href="/" className="flex items-center gap-2.5">
          <div className="h-6 w-6 rounded bg-gradient-to-br from-accent-magenta to-accent-violet shadow-inner flex items-center justify-center">
            <div className="h-2 w-2 rounded-full bg-white/90" />
          </div>
          <span className="font-mono text-sm font-bold tracking-widest text-text-primary uppercase">
            {t('brand', { fallback: 'MATCHINDEX' })}
          </span>
        </Link>
        
        <form onSubmit={handleSearch} className="flex items-center gap-2 bg-surface-0 border border-border rounded px-3 py-1.5 focus-within:border-accent-magenta transition-colors w-72">
          <Search className="h-3.5 w-3.5 text-text-muted" />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder={t('searchPlaceholder')}
            className="w-full bg-transparent text-[13px] text-text-primary placeholder:text-text-muted outline-none"
          />
        </form>
      </div>

      <div className="flex items-center gap-4">
        <button
          onClick={switchLocale}
          disabled={isPending}
          className="flex items-center gap-1.5 px-2.5 py-1.5 rounded text-[12px] font-medium
                     bg-surface-2 hover:bg-surface-3 text-text-secondary hover:text-text-primary
                     transition-colors disabled:opacity-50 disabled:cursor-wait border border-border-subtle"
          aria-label="Switch language"
        >
          <Globe className="h-3.5 w-3.5" />
          {isPending ? '...' : t('langToggle')}
        </button>
      </div>
    </header>
  );
}
