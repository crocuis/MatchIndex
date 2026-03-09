import Link from 'next/link';
import { Search } from 'lucide-react';
import { cn } from '@/lib/utils';

interface ListSearchFormProps {
  action: string;
  query?: string;
  placeholder: string;
  searchLabel: string;
  clearLabel: string;
  className?: string;
  hiddenValues?: Record<string, string | undefined>;
}

export function ListSearchForm({
  action,
  query,
  placeholder,
  searchLabel,
  clearLabel,
  className,
  hiddenValues,
}: ListSearchFormProps) {
  return (
    <form action={action} className={cn('mb-4 flex flex-wrap items-center gap-2', className)}>
      {Object.entries(hiddenValues ?? {}).map(([key, value]) => (
        value ? <input key={key} type="hidden" name={key} value={value} /> : null
      ))}
      <label className="flex min-w-72 flex-1 items-center gap-2 rounded border border-border bg-surface-1 px-3 py-2 focus-within:border-accent-emerald transition-colors">
        <Search className="h-3.5 w-3.5 text-text-muted" />
        <input
          type="search"
          name="q"
          defaultValue={query}
          placeholder={placeholder}
          className="w-full bg-transparent text-[13px] text-text-primary placeholder:text-text-muted outline-none"
        />
      </label>
      <button
        type="submit"
        className="rounded border border-border px-3 py-2 text-[12px] font-medium text-text-secondary transition-colors hover:bg-surface-2 hover:text-text-primary"
      >
        {searchLabel}
      </button>
      {query ? (
        <Link
          href={action + (hiddenValues && Object.keys(hiddenValues).length > 0 ? `?${new URLSearchParams(Object.entries(hiddenValues).filter(([, value]) => value) as Array<[string, string]>).toString()}` : '')}
          className="rounded border border-border px-3 py-2 text-[12px] font-medium text-text-muted transition-colors hover:bg-surface-2 hover:text-text-primary"
        >
          {clearLabel}
        </Link>
      ) : null}
    </form>
  );
}
