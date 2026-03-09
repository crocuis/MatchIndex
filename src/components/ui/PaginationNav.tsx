import Link from 'next/link';
import { cn } from '@/lib/utils';

interface PaginationNavProps {
  currentPage: number;
  totalPages: number;
  hrefForPage: (page: number) => string;
  previousLabel: string;
  nextLabel: string;
  pageLabel: string;
  className?: string;
}

export function PaginationNav({
  currentPage,
  totalPages,
  hrefForPage,
  previousLabel,
  nextLabel,
  pageLabel,
  className,
}: PaginationNavProps) {
  if (totalPages <= 1) {
    return null;
  }

  const previousPage = Math.max(1, currentPage - 1);
  const nextPage = Math.min(totalPages, currentPage + 1);
  const visiblePages = new Set<number>([1, totalPages]);

  for (let page = currentPage - 2; page <= currentPage + 2; page += 1) {
    if (page >= 1 && page <= totalPages) {
      visiblePages.add(page);
    }
  }

  const orderedPages = Array.from(visiblePages).sort((left, right) => left - right);
  const pageItems: Array<number | 'ellipsis'> = [];

  for (const page of orderedPages) {
    const previousItem = pageItems[pageItems.length - 1];

    if (typeof previousItem === 'number' && page - previousItem > 1) {
      pageItems.push('ellipsis');
    }

    pageItems.push(page);
  }

  return (
    <div className={cn('flex items-center gap-2 text-[11px]', className)}>
      <Link
        href={hrefForPage(previousPage)}
        aria-disabled={currentPage <= 1}
        className={cn(
          'rounded border border-border px-2 py-1 transition-colors',
          currentPage <= 1
            ? 'pointer-events-none opacity-40'
            : 'text-text-secondary hover:bg-surface-2 hover:text-text-primary'
        )}
      >
        {previousLabel}
      </Link>
      <span className="min-w-20 text-center font-medium text-text-secondary">
        {pageLabel}
      </span>
      <div className="flex items-center gap-1">
        {pageItems.map((item, index) => {
          if (item === 'ellipsis') {
            return (
              <span key={`ellipsis-${index}`} className="px-1 text-text-muted">
                ...
              </span>
            );
          }

          const isActive = item === currentPage;

          return (
            <Link
              key={item}
              href={hrefForPage(item)}
              aria-current={isActive ? 'page' : undefined}
              className={cn(
                'rounded border px-2 py-1 transition-colors min-w-7 text-center',
                isActive
                  ? 'border-accent-emerald bg-accent-emerald/10 text-accent-emerald'
                  : 'border-border text-text-secondary hover:bg-surface-2 hover:text-text-primary'
              )}
            >
              {item}
            </Link>
          );
        })}
      </div>
      <Link
        href={hrefForPage(nextPage)}
        aria-disabled={currentPage >= totalPages}
        className={cn(
          'rounded border border-border px-2 py-1 transition-colors',
          currentPage >= totalPages
            ? 'pointer-events-none opacity-40'
            : 'text-text-secondary hover:bg-surface-2 hover:text-text-primary'
        )}
      >
        {nextLabel}
      </Link>
    </div>
  );
}
