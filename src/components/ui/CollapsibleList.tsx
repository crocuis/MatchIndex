'use client';

import { Children, useState, type ReactNode } from 'react';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { useTranslations } from 'next-intl';
import { cn } from '@/lib/utils';

interface CollapsibleListProps {
  children: ReactNode;
  limit?: number;
  gap?: string;
  className?: string;
}

export function CollapsibleList({
  children,
  limit = 10,
  gap = 'gap-1.5',
  className,
}: CollapsibleListProps) {
  const t = useTranslations('common');
  const items = Children.toArray(children);
  const [visibleCount, setVisibleCount] = useState(limit);

  const hasMore = items.length > limit;
  const visibleItems = items.slice(0, visibleCount);
  const hiddenCount = Math.max(items.length - visibleCount, 0);
  const isFullyExpanded = hiddenCount === 0;

  function handleToggle() {
    if (isFullyExpanded) {
      setVisibleCount(limit);
      return;
    }

    setVisibleCount((current) => Math.min(current + limit, items.length));
  }

  return (
    <div className={cn('flex flex-col', gap, className)}>
      {visibleItems}
      {hasMore ? (
        <button
          type="button"
          onClick={handleToggle}
          className={cn(
            'flex items-center justify-center gap-1.5 rounded border px-3 py-1.5',
            'text-[11px] font-medium transition-colors',
            'border-border-subtle bg-surface-2 text-text-secondary',
            'hover:border-border hover:bg-surface-3 hover:text-text-primary',
          )}
        >
          {isFullyExpanded ? (
            <>
              <ChevronUp className="size-3.5" />
              {t('showLess')}
            </>
          ) : (
            <>
              <ChevronDown className="size-3.5" />
              {t('showMore', { count: hiddenCount })}
            </>
          )}
        </button>
      ) : null}
    </div>
  );
}
