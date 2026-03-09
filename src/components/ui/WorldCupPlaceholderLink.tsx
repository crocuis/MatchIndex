'use client';

import { useMemo, useState } from 'react';
import { useTranslations } from 'next-intl';
import { cn } from '@/lib/utils';
import type { WorldCupPlaceholder } from '@/data/types';

interface WorldCupPlaceholderLinkProps {
  placeholder: WorldCupPlaceholder;
  label?: string;
  className?: string;
}

export function WorldCupPlaceholderLink({ placeholder, label, className }: WorldCupPlaceholderLinkProps) {
  const tWorldCup = useTranslations('worldCup');
  const [open, setOpen] = useState(false);
  const candidateSummary = useMemo(
    () => placeholder.candidates.map((candidate) => candidate.name).join(', '),
    [placeholder.candidates]
  );

  return (
    <span
      className={cn('relative inline-flex min-w-0 items-center gap-1', className)}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
    >
      <button
        type="button"
        title={candidateSummary}
        className="truncate text-left text-text-primary underline decoration-dotted underline-offset-2 hover:text-accent-emerald"
        onClick={(event) => {
          event.preventDefault();
          event.stopPropagation();
          setOpen((current) => !current);
        }}
        onBlur={() => {
          setTimeout(() => setOpen(false), 120);
        }}
        onFocus={() => setOpen(true)}
      >
        {label ?? placeholder.label}
      </button>

      <button
        type="button"
        aria-label={tWorldCup('placeholderInfoLabel')}
        className="inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full border border-border-subtle bg-surface-2 text-[10px] font-bold text-text-muted hover:border-border hover:text-text-primary"
        onClick={(event) => {
          event.preventDefault();
          event.stopPropagation();
          setOpen((current) => !current);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          setTimeout(() => setOpen(false), 120);
        }}
      >
        i
      </button>

      {open && (
        <div className="absolute left-0 top-full z-30 mt-2 w-72 rounded-lg border border-border bg-surface-1 p-3 shadow-2xl">
          <div className="space-y-1">
            <div className="text-[11px] font-semibold uppercase tracking-wider text-text-muted">
              {placeholder.confederation ?? tWorldCup('placeholderUnknownPath')}
            </div>
            <div className="text-[13px] font-semibold text-text-primary">{placeholder.label}</div>
            {placeholder.description && (
              <p className="text-[12px] leading-5 text-text-secondary">{placeholder.description}</p>
            )}
            {placeholder.resolvedOn && (
              <p className="text-[11px] text-text-muted">
                {tWorldCup('placeholderResolvedOn', { date: placeholder.resolvedOn })}
              </p>
            )}
          </div>

          <div className="mt-3 border-t border-border-subtle pt-3">
            <div className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-text-muted">
              {tWorldCup('placeholderCandidates')}
            </div>
            <div className="space-y-1.5">
              {placeholder.candidates.map((candidate) => (
                <div key={`${placeholder.id}-${candidate.name}`} className="rounded-md bg-surface-2/70 px-2.5 py-2">
                  <div className="text-[12px] font-medium text-text-primary">{candidate.name}</div>
                  {candidate.note && (
                    <div className="mt-0.5 text-[11px] leading-4 text-text-muted">{candidate.note}</div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </span>
  );
}
