'use client';

import { useEffect, useMemo, useState } from 'react';
import { useTranslations } from 'next-intl';
import { cn } from '@/lib/utils';

export interface AnalysisEventPickerOption {
  eventId: string;
  minute: number;
  stoppageMinute?: number | null;
  matchSecond?: number | null;
  playerName: string;
  eventType: string;
  detail?: string;
  metaLabel?: string;
}

type AnalysisEventFilter = 'all' | string;

interface AnalysisEventPickerProps {
  label: string;
  options: AnalysisEventPickerOption[];
  selectedEventId: string;
  onSelect: (eventId: string) => void;
}

function formatEventType(eventType: string) {
  return eventType.replace(/_/g, ' ');
}

function formatMatchClock(minute: number, stoppageMinute?: number | null) {
  return stoppageMinute && stoppageMinute > 0 ? `${minute}+${stoppageMinute}'` : `${minute}'`;
}

export function AnalysisEventPicker({
  label,
  options,
  selectedEventId,
  onSelect,
}: AnalysisEventPickerProps) {
  const tCommon = useTranslations('common');
  const [activeFilter, setActiveFilter] = useState<AnalysisEventFilter>('all');
  const filterOptions = useMemo(() => {
    const eventTypes = Array.from(new Set(options.map((option) => option.eventType)));

    return [
      { key: 'all', label: 'All', count: options.length },
      ...eventTypes.map((eventType) => ({
        key: eventType,
        label: formatEventType(eventType),
        count: options.filter((option) => option.eventType === eventType).length,
      })),
    ];
  }, [options]);
  const filteredOptions = useMemo(() => {
    if (activeFilter === 'all') {
      return options;
    }

    return options.filter((option) => option.eventType === activeFilter);
  }, [activeFilter, options]);
  const selectedIndex = filteredOptions.findIndex((option) => option.eventId === selectedEventId);
  const selectedOption = selectedIndex >= 0 ? filteredOptions[selectedIndex] : filteredOptions[0];

  useEffect(() => {
    if (selectedOption && selectedOption.eventId !== selectedEventId) {
      onSelect(selectedOption.eventId);
    }
  }, [onSelect, selectedEventId, selectedOption]);

  if (!selectedOption) {
    return null;
  }

  const hasPrevious = selectedIndex > 0;
  const hasNext = selectedIndex >= 0 && selectedIndex < options.length - 1;

  return (
    <div className="space-y-3 rounded-lg border border-border bg-surface-2/40 p-3">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-text-muted">{label}</div>
          <div className="flex flex-wrap items-center gap-2 text-[13px] text-text-primary">
            <span className="font-semibold tabular-nums">{formatMatchClock(selectedOption.minute, selectedOption.stoppageMinute)}</span>
            <span className="font-medium">{selectedOption.playerName}</span>
            <span className="rounded border border-border-subtle bg-surface-3 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-text-secondary">
              {formatEventType(selectedOption.eventType)}
            </span>
            {selectedOption.metaLabel ? (
              <span className="text-[11px] text-text-muted">{selectedOption.metaLabel}</span>
            ) : null}
            {selectedOption.matchSecond !== undefined && selectedOption.matchSecond !== null ? (
              <span className="text-[11px] text-text-muted">T+{selectedOption.matchSecond}s</span>
            ) : null}
          </div>
          {selectedOption.detail ? (
            <div className="text-[11px] text-text-muted">{selectedOption.detail}</div>
          ) : null}
        </div>

        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={() => hasPrevious && onSelect(filteredOptions[selectedIndex - 1]!.eventId)}
            disabled={!hasPrevious}
            className={cn(
              'rounded border px-2.5 py-1.5 text-[11px] font-medium transition-colors',
              hasPrevious
                ? 'border-border bg-surface-3 text-text-secondary hover:bg-surface-4 hover:text-text-primary'
                : 'border-border-subtle bg-surface-2 text-text-muted/60'
            )}
          >
            {tCommon('previous')}
          </button>
          <button
            type="button"
            onClick={() => hasNext && onSelect(filteredOptions[selectedIndex + 1]!.eventId)}
            disabled={!hasNext}
            className={cn(
              'rounded border px-2.5 py-1.5 text-[11px] font-medium transition-colors',
              hasNext
                ? 'border-border bg-surface-3 text-text-secondary hover:bg-surface-4 hover:text-text-primary'
                : 'border-border-subtle bg-surface-2 text-text-muted/60'
            )}
          >
            {tCommon('next')}
          </button>
        </div>
      </div>

      <div className="-mx-1 overflow-x-auto px-1">
        <div className="flex min-w-max gap-1.5">
          {filterOptions.map((filterOption) => {
            const isActive = filterOption.key === activeFilter;

            return (
              <button
                key={filterOption.key}
                type="button"
                onClick={() => setActiveFilter(filterOption.key)}
                className={cn(
                  'rounded-full border px-2.5 py-1 text-[10px] font-medium uppercase tracking-wide transition-colors',
                  isActive
                    ? 'border-accent-emerald bg-accent-emerald/10 text-text-primary'
                    : 'border-border bg-surface-1/70 text-text-muted hover:bg-surface-3 hover:text-text-secondary'
                )}
              >
                {filterOption.label} <span className="text-text-muted">{filterOption.count}</span>
              </button>
            );
          })}
        </div>
      </div>

      <div className="-mx-1 overflow-x-auto px-1">
        <div className="flex min-w-max gap-2">
          {filteredOptions.map((option) => {
            const isActive = option.eventId === selectedOption.eventId;

            return (
              <button
                key={option.eventId}
                type="button"
                onClick={() => onSelect(option.eventId)}
                className={cn(
                  'w-40 rounded-md border px-3 py-2 text-left transition-colors',
                  isActive
                    ? 'border-accent-emerald bg-accent-emerald/10 text-text-primary'
                    : 'border-border bg-surface-1/80 text-text-secondary hover:bg-surface-3 hover:text-text-primary'
                )}
              >
                <div className="flex items-center justify-between gap-2 text-[11px]">
                  <span className="font-semibold tabular-nums">{formatMatchClock(option.minute, option.stoppageMinute)}</span>
                  <span className="truncate uppercase tracking-wide text-text-muted">{formatEventType(option.eventType)}</span>
                </div>
                <div className="mt-1 truncate text-[12px] font-medium">{option.playerName}</div>
                {option.metaLabel ? (
                  <div className="mt-1 truncate text-[10px] text-text-muted">{option.metaLabel}</div>
                ) : null}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
