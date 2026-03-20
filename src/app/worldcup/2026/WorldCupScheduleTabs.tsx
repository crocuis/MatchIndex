'use client';

import { useMemo, useSyncExternalStore } from 'react';
import { useLocale, useTranslations } from 'next-intl';
import { MatchCard } from '@/components/data/MatchCard';
import { FixtureCard } from '@/components/data/FixtureCard';
import { CollapsibleList } from '@/components/ui/CollapsibleList';
import { TabGroup } from '@/components/ui/TabGroup';
import type { Match, WorldCupGroup, WorldCupPlaceholder, WorldCupStage } from '@/data/types';
import { isFinishedMatchStatus } from '@/lib/matchStatus';
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

  const sortedMatches = useMemo(() => matches.slice().sort(sortMatches), [matches]);

  const scheduleData = useMemo(() => {
    const knockoutMatchIds = new Set(stages.flatMap((stage) => stage.matchIds));
    const matchMap = new Map(sortedMatches.map((match) => [match.id, match]));
    const matchesByGroup = new Map<string, Match[]>();
    const groupNationSets = new Map(groups.map((group) => [group.name, new Set(group.standings.map((standing) => standing.nationId))]));

    for (const group of groups) {
      matchesByGroup.set(group.name, []);
    }

    for (const match of sortedMatches) {
      if (knockoutMatchIds.has(match.id)) {
        continue;
      }

      for (const group of groups) {
        const groupNationIds = groupNationSets.get(group.name);
        if (!groupNationIds) {
          continue;
        }
        if (groupNationIds.has(match.homeTeamId) && groupNationIds.has(match.awayTeamId)) {
          matchesByGroup.get(group.name)?.push(match);
          break;
        }
      }
    }

    const compactMatchesByGroup = new Map(
      Array.from(matchesByGroup.entries()).filter(([, groupMatches]) => groupMatches.length > 0)
    );

    const stageMatchMap = new Map<string, Match[]>();
    for (const stage of stages) {
      const stageMatches = stage.matchIds
        .map((id) => matchMap.get(id))
        .filter((match): match is Match => match !== undefined);
      if (stageMatches.length > 0) {
        stageMatchMap.set(stage.name, stageMatches);
      }
    }

    const groupRoundMap = new Map<number, Match[]>();
    for (const group of groups) {
      const groupMatches = compactMatchesByGroup.get(group.name) ?? [];
      if (groupMatches.length === 0) {
        continue;
      }

      const matchesPerRound = Math.max(1, Math.floor(group.standings.length / 2));
      for (let index = 0; index < groupMatches.length; index += matchesPerRound) {
        const roundMatches = groupMatches.slice(index, index + matchesPerRound);
        const roundNumber = Math.floor(index / matchesPerRound) + 1;
        groupRoundMap.set(roundNumber, [...(groupRoundMap.get(roundNumber) ?? []), ...roundMatches]);
      }
    }

    const roundSections: ScheduleSection[] = [];
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
      if (!stageMatches || stageMatches.length === 0) {
        return;
      }

      roundSections.push({ title: stage.name, matches: stageMatches });
    });

    return {
      matchesByGroup: compactMatchesByGroup,
      roundSections,
    };
  }, [groups, sortedMatches, stages, tWorldCup]);

  const matchesByDate = useMemo(() => {
    const grouped = new Map<string, Match[]>();

    sortedMatches.forEach((match) => {
        const dateKey = getMatchDateKeyForTimeZone(match.date, match.time, timeZone, getMatchSourceOffsetMinutes(match));
        if (!grouped.has(dateKey)) {
          grouped.set(dateKey, []);
        }
        grouped.get(dateKey)!.push(match);
      });

    return grouped;
  }, [sortedMatches, timeZone]);

  const sortedDates = useMemo(() => Array.from(matchesByDate.keys()).sort(), [matchesByDate]);

  const renderMatchList = (matchList: Match[]) => (
    <CollapsibleList limit={6} gap="gap-1.5">
      {matchList.map((match) =>
        isFinishedMatchStatus(match.status) ? (
          <MatchCard key={match.id} match={match} placeholders={placeholders} />
        ) : (
          <FixtureCard key={match.id} match={match} placeholders={placeholders} />
        )
      )}
    </CollapsibleList>
  );

  const tabs = [
    {
      key: 'group',
      label: tWorldCup('tabGroup'),
      render: () => (
        <CollapsibleList limit={4} gap="gap-4">
          {Array.from(scheduleData.matchesByGroup.entries()).map(([groupName, groupMatches]) => (
            <div key={groupName} className="space-y-2">
              <div className="text-[11px] font-bold uppercase tracking-wider text-text-muted">{groupName}</div>
              {renderMatchList(groupMatches)}
            </div>
          ))}
          {scheduleData.matchesByGroup.size === 0 ? (
            <div className="py-4 text-center text-[13px] text-text-muted">{tWorldCup('noGroupMatches')}</div>
          ) : null}
        </CollapsibleList>
      ),
    },
    {
      key: 'date',
      label: tWorldCup('tabDate'),
      render: () => (
        <CollapsibleList limit={4} gap="gap-4">
          {sortedDates.map((date) => (
            <div key={date} className="space-y-2">
              <div className="flex items-center justify-between gap-3 rounded-md border border-border-subtle bg-surface-2/30 px-3 py-2">
                <div className="text-[11px] font-bold uppercase tracking-wider text-text-secondary">
                  {formatMatchDateLabelForTimeZone(
                    matchesByDate.get(date)?.[0]?.date ?? date,
                    matchesByDate.get(date)?.[0]?.time ?? '00:00',
                    locale,
                    timeZone,
                    matchesByDate.get(date)?.[0] ? getMatchSourceOffsetMinutes(matchesByDate.get(date)![0]) : 0,
                  )}
                </div>
                <div className="rounded-full border border-border-subtle bg-surface-1 px-2 py-0.5 text-[10px] font-medium text-text-muted">
                  {matchesByDate.get(date)?.length ?? 0}
                </div>
              </div>
              {renderMatchList(matchesByDate.get(date) ?? [])}
            </div>
          ))}
        </CollapsibleList>
      ),
    },
    {
      key: 'round',
      label: tWorldCup('tabRound'),
      render: () => (
        <CollapsibleList limit={4} gap="gap-4">
          {scheduleData.roundSections.map((section) => (
            <div key={section.title} className="space-y-2">
              <div className="text-[11px] font-bold uppercase tracking-wider text-text-muted">{section.title}</div>
              {renderMatchList(section.matches)}
            </div>
          ))}
        </CollapsibleList>
      ),
    },
  ] as const;

  return <TabGroup tabs={tabs} defaultTab="date" />;
}
