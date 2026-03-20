import type { ReactNode } from 'react';
import { LocalizedMatchText } from '@/components/ui/LocalizedMatchText';
import type { Match } from '@/data/types';
import { cn } from '@/lib/utils';

type MatchSectionTitleVariant = 'results' | 'fixtures' | 'stage';

interface MatchSectionTitleProps {
  title: string;
  count: number;
  dateLabel?: ReactNode;
  variant?: MatchSectionTitleVariant;
}

export function formatMatchSectionDate(date: string | undefined, locale: string) {
  return renderMatchSectionDateLabel(date ? { date, time: '00:00', id: '', venue: '' } : undefined, locale);
}

export function renderMatchSectionDateLabel(match: Pick<Match, 'date' | 'time' | 'id' | 'venue'> | undefined, locale: string) {
  void locale;

  if (!match) {
    return null;
  }

  return (
    <LocalizedMatchText
      matchId={match.id}
      venue={match.venue}
      date={match.date}
      time={match.time}
      variant="dateShort"
    />
  );
}

const TITLE_VARIANT_STYLES: Record<MatchSectionTitleVariant, { badge: string; date: string }> = {
  results: {
    badge: 'border-emerald-500/25 bg-emerald-500/10 text-emerald-300',
    date: 'text-emerald-200/70',
  },
  fixtures: {
    badge: 'border-sky-500/25 bg-sky-500/10 text-sky-300',
    date: 'text-sky-200/70',
  },
  stage: {
    badge: 'border-fuchsia-500/25 bg-fuchsia-500/10 text-fuchsia-300',
    date: 'text-fuchsia-200/70',
  },
};

export function MatchSectionTitle({ title, count, dateLabel, variant = 'results' }: MatchSectionTitleProps) {
  const styles = TITLE_VARIANT_STYLES[variant];

  return (
    <span className="flex items-center gap-2">
      <span>{title}</span>
      <span className={cn('rounded border px-1.5 py-0.5 text-[10px] font-semibold tracking-normal', styles.badge)}>
        {count}
      </span>
      {dateLabel ? (
        <span className={cn('text-[10px] font-medium normal-case tracking-normal', styles.date)}>
          {dateLabel}
        </span>
      ) : null}
    </span>
  );
}
