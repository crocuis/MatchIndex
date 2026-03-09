'use client';

import { useMemo, useSyncExternalStore } from 'react';
import { useLocale, useTranslations } from 'next-intl';
import { TabGroup } from '@/components/ui/TabGroup';
import { MatchCard } from '@/components/data/MatchCard';
import { FixtureCard } from '@/components/data/FixtureCard';
import type { Match, WorldCupGroup, WorldCupPlaceholder, WorldCupStage } from '@/data/types';
import { formatMatchDateLabelForTimeZone, getMatchDateKeyForTimeZone, getMatchSourceOffsetMinutes } from '@/lib/utils';

interface WorldCupScheduleTabsProps {
  matches: Match[];
  stages: WorldCupStage[];
  groups: WorldCupGroup[];
  placeholders?: WorldCupPlaceholder[];
}

interface ScheduleSection {
  title: string;
  matches: Match[];
}

function sortMatches(left: Match, right: Match) {
  return `${left.date} ${left.time}`.localeCompare(`${right.date} ${right.time}`);
}

export function WorldCupScheduleTabs({ matches, stages, groups, placeholders = [] }: WorldCupScheduleTabsProps) {
  const locale = useLocale();
  const tWorldCup = useTranslations('worldCup');
  const timeZone = useSyncExternalStore(
    () => () => {},
    () => Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC',
    () => 'UTC'
  );

  const knockoutMatchIds = new Set(stages.flatMap((stage) => stage.matchIds));
  const groupStageMatches = matches
    .filter((match) => !knockoutMatchIds.has(match.id))
    .sort(sortMatches);

  const matchesByGroup = new Map<string, Match[]>();
  groups.forEach((group) => {
    const groupNationIds = new Set(group.standings.map((s) => s.nationId));
    const groupMatches = groupStageMatches.filter(
      (match) => groupNationIds.has(match.homeTeamId) && groupNationIds.has(match.awayTeamId)
    );
    if (groupMatches.length > 0) {
      matchesByGroup.set(group.name, groupMatches);
    }
  });

  const matchesByDate = useMemo(() => {
    const grouped = new Map<string, Match[]>();

    matches
      .slice()
      .sort(sortMatches)
      .forEach((match) => {
        const dateKey = getMatchDateKeyForTimeZone(match.date, match.time, timeZone, getMatchSourceOffsetMinutes(match));
        if (!grouped.has(dateKey)) {
          grouped.set(dateKey, []);
        }
        grouped.get(dateKey)!.push(match);
      });

    return grouped;
  }, [matches, timeZone]);

  const sortedDates = useMemo(() => Array.from(matchesByDate.keys()).sort(), [matchesByDate]);

  const stageMatchMap = new Map<string, Match[]>();
  stages.forEach((stage) => {
    const stageMatches = stage.matchIds
      .map((id) => matches.find((m) => m.id === id))
      .filter((m): m is Match => m !== undefined)
      .sort(sortMatches);
    if (stageMatches.length > 0) {
      stageMatchMap.set(stage.name, stageMatches);
    }
  });

  const roundSections: ScheduleSection[] = [];
  const groupRoundMap = new Map<number, Match[]>();

  groups.forEach((group) => {
    const groupMatches = matchesByGroup.get(group.name) ?? [];
    if (groupMatches.length === 0) return;

    const matchesPerRound = Math.max(1, Math.floor(group.standings.length / 2));

    for (let index = 0; index < groupMatches.length; index += matchesPerRound) {
      const roundMatches = groupMatches.slice(index, index + matchesPerRound);
      const roundNumber = Math.floor(index / matchesPerRound) + 1;

      groupRoundMap.set(roundNumber, [...(groupRoundMap.get(roundNumber) ?? []), ...roundMatches]);
    }
  });

  Array.from(groupRoundMap.entries())
    .sort(([left], [right]) => left - right)
    .forEach(([roundNumber, roundMatches]) => {
      roundSections.push({
        title: tWorldCup('groupRound', { round: roundNumber }),
        matches: roundMatches.sort(sortMatches),
      });
    });

  stages.forEach((stage) => {
    const stageMatches = stageMatchMap.get(stage.name);
    if (!stageMatches || stageMatches.length === 0) return;

    roundSections.push({
      title: stage.name,
      matches: stageMatches,
    });
  });

  const renderMatchList = (matchList: Match[]) => (
    <div className="space-y-1.5">
      {matchList.map((match) =>
        match.status === 'finished' ? (
          <MatchCard key={match.id} match={match} placeholders={placeholders} />
        ) : (
          <FixtureCard key={match.id} match={match} placeholders={placeholders} />
        )
      )}
    </div>
  );

  const tabs = [
    {
      key: 'group',
      label: tWorldCup('tabGroup'),
      content: (
        <div className="space-y-4">
          {Array.from(matchesByGroup.entries()).map(([groupName, groupMatches]) => (
            <div key={groupName} className="space-y-2">
              <div className="text-[11px] font-bold uppercase tracking-wider text-text-muted">{groupName}</div>
              {renderMatchList(groupMatches)}
            </div>
          ))}
          {matchesByGroup.size === 0 && (
            <div className="text-[13px] text-text-muted py-4 text-center">
              {tWorldCup('noGroupMatches')}
            </div>
          )}
        </div>
      ),
    },
    {
      key: 'date',
      label: tWorldCup('tabDate'),
      content: (
        <div className="space-y-4">
          {sortedDates.map((date) => (
            <div key={date} className="space-y-2">
              <div className="flex items-center justify-between gap-3 rounded-md border border-border-subtle bg-surface-2/30 px-3 py-2">
                <div className="text-[11px] font-bold uppercase tracking-wider text-text-secondary">
                  {formatMatchDateLabelForTimeZone(
                    matchesByDate.get(date)?.[0]?.date ?? date,
                    matchesByDate.get(date)?.[0]?.time ?? '00:00',
                    locale,
                    timeZone,
                    matchesByDate.get(date)?.[0] ? getMatchSourceOffsetMinutes(matchesByDate.get(date)![0]) : 0
                  )}
                </div>
                <div className="rounded-full border border-border-subtle bg-surface-1 px-2 py-0.5 text-[10px] font-medium text-text-muted">
                  {matchesByDate.get(date)?.length ?? 0}
                </div>
              </div>
              {renderMatchList(matchesByDate.get(date)!)}
            </div>
          ))}
        </div>
      ),
    },
    {
      key: 'round',
      label: tWorldCup('tabRound'),
      content: (
        <div className="space-y-4">
          {roundSections.map((section) => (
            <div key={section.title} className="space-y-2">
              <div className="text-[11px] font-bold uppercase tracking-wider text-text-muted">{section.title}</div>
              {renderMatchList(section.matches)}
            </div>
          ))}
        </div>
      ),
    },
  ];

  return <TabGroup tabs={tabs} defaultTab="date" />;
}
