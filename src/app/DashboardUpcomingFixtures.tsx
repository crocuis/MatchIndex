'use client';

import { useMemo, useState } from 'react';
import { useLocale } from 'next-intl';
import { useBrowserTimeZone } from '@/components/providers/BrowserTimeZoneProvider';
import { FixtureCard } from '@/components/data/FixtureCard';
import { CollapsibleList } from '@/components/ui/CollapsibleList';
import { LocalizedMatchText } from '@/components/ui/LocalizedMatchText';
import { MatchSectionTitle } from '@/components/ui/MatchSectionTitle';
import { SectionCard } from '@/components/ui/SectionCard';
import type { Match } from '@/data/types';
import { cn, formatMatchDateLabelForTimeZone, getMatchDateKeyForTimeZone, getMatchSourceOffsetMinutes } from '@/lib/utils';

const DATE_TAB_LIMIT = 8;

interface DashboardUpcomingFixturesProps {
  matches: Match[];
  title: string;
  emptyLabel: string;
}

export function DashboardUpcomingFixtures({ matches, title, emptyLabel }: DashboardUpcomingFixturesProps) {
  const locale = useLocale();
  const timeZone = useBrowserTimeZone();
  const [selectedDateOverride, setSelectedDateOverride] = useState<string>();

  const dateGroups = useMemo(() => {
    const grouped = new Map<string, Match[]>();

    for (const match of matches) {
      const dateKey = getMatchDateKeyForTimeZone(match.date, match.time, timeZone, getMatchSourceOffsetMinutes(match));
      const existing = grouped.get(dateKey) ?? [];
      existing.push(match);
      grouped.set(dateKey, existing);
    }

    return Array.from(grouped.entries())
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([date, dateMatches]) => ({
        date,
        label: formatMatchDateLabelForTimeZone(
          dateMatches[0].date,
          dateMatches[0].time,
          locale,
          timeZone,
          getMatchSourceOffsetMinutes(dateMatches[0])
        ),
        matches: dateMatches,
      }))
      .slice(0, DATE_TAB_LIMIT);
  }, [locale, matches, timeZone]);

  const selectedDate = dateGroups.some((group) => group.date === selectedDateOverride)
    ? selectedDateOverride
    : dateGroups[0]?.date;
  const selectedGroup = dateGroups.find((group) => group.date === selectedDate) ?? dateGroups[0];

  return (
    <SectionCard
      title={(
        <MatchSectionTitle
          title={title}
          count={selectedGroup?.matches.length ?? 0}
          dateLabel={selectedGroup ? (
            <LocalizedMatchText
              matchId={selectedGroup.matches[0].id}
              venue={selectedGroup.matches[0].venue}
              date={selectedGroup.matches[0].date}
              time={selectedGroup.matches[0].time}
              variant="dateShort"
            />
          ) : null}
          variant="fixtures"
        />
      )}
    >
      {selectedGroup ? (
        <div className="space-y-3">
          {dateGroups.length > 1 ? (
            <div className="flex flex-wrap gap-2">
              {dateGroups.map((group) => {
                const isActive = group.date === selectedGroup.date;

                return (
                  <button
                    key={group.date}
                    type="button"
                    onClick={() => setSelectedDateOverride(group.date)}
                    className={cn(
                      'rounded border px-2.5 py-1 text-[11px] font-medium transition-colors',
                      isActive
                        ? 'border-accent-emerald bg-accent-emerald/10 text-accent-emerald'
                        : 'border-border-subtle bg-surface-2 text-text-secondary hover:border-border hover:text-text-primary'
                    )}
                  >
                    {group.label}
                  </button>
                );
              })}
            </div>
          ) : null}

          <CollapsibleList>
            {selectedGroup.matches.map((match) => (
              <FixtureCard
                key={match.id}
                match={match}
                showDate={false}
                showLeague
                leagueName={match.competitionName}
              />
            ))}
          </CollapsibleList>
        </div>
      ) : (
        <div className="text-[13px] text-text-muted">{emptyLabel}</div>
      )}
    </SectionCard>
  );
}
