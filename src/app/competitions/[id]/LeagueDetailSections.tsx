import Link from 'next/link';
import { Suspense } from 'react';
import { getTranslations } from 'next-intl/server';
import { MatchArchiveSplitList } from '@/components/data/MatchArchiveSplitList';
import { StandingsTable } from '@/components/data/StandingsTable';
import { MatchSectionTitle, formatMatchSectionDate, renderMatchSectionDateLabel } from '@/components/ui/MatchSectionTitle';
import { SectionCard } from '@/components/ui/SectionCard';
import { PaginationNav } from '@/components/ui/PaginationNav';
import { StaticDetailTabs } from '@/components/ui/StaticDetailTabs';
import { TabGroup } from '@/components/ui/TabGroup';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { CompetitionMatchesPanel } from '@/app/competitions/[id]/CompetitionMatchesPanel';
import { CompetitionStatsPanel } from '@/app/competitions/[id]/CompetitionStatsPanel';
import type { League, Match } from '@/data/types';
import {
  analyzeTournamentMatches,
  buildGroupStageMatches,
  buildTournamentGroups,
  buildLeaguePhaseStandings,
  getTournamentChampion,
  isLegacyGroupStageMatch,
} from '@/app/competitions/[id]/tournamentView';
import type { KnockoutStageView } from '@/app/competitions/[id]/tournamentView';
import {
  getClubsByLeagueAndSeasonDb,
  getClubsByLeagueDb,
  getMatchesByLeagueAndSeasonDb,
  getMatchesByLeagueDb,
  getStandingsByLeagueAndSeasonDb,
  getStandingsByLeagueDb,
  getTopScorerRowsBySeasonDb,
  getTopScorerRowsDb,
} from '@/data/server';
import { getClubDisplayName } from '@/lib/utils';

interface SelectedSeasonValue {
  seasonId: string;
  seasonLabel: string;
  competitionFormat?: League['competitionFormat'];
}

interface LeagueDetailSectionsProps {
  league: League;
  locale: string;
  selectedSeason?: SelectedSeasonValue;
  isNonDefaultSeason: boolean;
  isTournament: boolean;
  seasonQueryParam?: string;
  resultsPage: number;
  fixturesPage: number;
  initialTab: 'overview' | 'matches' | 'stats';
}

const MATCHES_PAGE_SIZE = 20;

function normalizePage(value: number, totalPages: number) {
  if (totalPages <= 0) {
    return 1;
  }

  return Math.min(Math.max(1, value), totalPages);
}

function paginateMatches(matches: Match[], page: number) {
  const totalPages = Math.max(1, Math.ceil(matches.length / MATCHES_PAGE_SIZE));
  const currentPage = normalizePage(page, totalPages);
  const start = (currentPage - 1) * MATCHES_PAGE_SIZE;

  return {
    currentPage,
    totalPages,
    items: matches.slice(start, start + MATCHES_PAGE_SIZE),
  };
}

function buildClubLeagueContextHref(clubId: string, leagueId: string, seasonId?: string) {
  const searchParams = new URLSearchParams({ competition: leagueId });

  if (seasonId) {
    searchParams.set('season', seasonId);
  }

  return `/clubs/${clubId}?${searchParams.toString()}`;
}

interface KnockoutTieCardView {
  id: string;
  participantIds: [string, string];
  sourceTieIds?: [string, string];
  legs: KnockoutTieLegView[];
}

interface KnockoutTieLegView {
  id: string;
  href?: string;
  dateLabel: string;
  homeTeamId: string;
  homeTeamName: string;
  homeTeamLogo?: string;
  awayTeamId: string;
  awayTeamName: string;
  awayTeamLogo?: string;
  homeScore: number | null;
  awayScore: number | null;
}

interface KnockoutBracketRoundView {
  name: string;
  ties: KnockoutTieCardView[];
}

function renderBracketTeamMarker(teamId: string, teamName: string, logo?: string, href?: string) {
  if (logo || href) {
    return <ClubBadge shortName={teamName} clubId={teamId} logo={logo} size="sm" showText={false} />;
  }

  return <span className="inline-flex h-6 w-6 shrink-0 rounded-full border border-border-subtle bg-surface-0/70" />;
}

async function TournamentChampionValue({
  knockoutStages,
  locale,
  label,
}: {
  knockoutStages: KnockoutStageView[];
  locale: string;
  label: string;
}) {
  const champion = await getTournamentChampion(knockoutStages, locale);

  if (!champion) {
    return null;
  }

  return (
    <div>
      <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{label}</div>
      <div className="text-[13px] font-medium text-text-primary">{champion}</div>
    </div>
  );
}

async function LeagueClubListSection({
  leagueId,
  locale,
  title,
  seasonId,
}: {
  leagueId: string;
  locale: string;
  title: string;
  seasonId?: string;
}) {
  const clubs = seasonId
    ? await getClubsByLeagueAndSeasonDb(leagueId, seasonId, locale)
    : await getClubsByLeagueDb(leagueId, locale);

  return (
    <SectionCard title={title}>
      <div className="grid grid-cols-2 gap-2">
        {clubs.map((club) => (
          <Link key={club.id} href={`/clubs/${club.id}`} className="flex items-center gap-3 rounded border border-border-subtle bg-surface-2 px-3 py-2 transition-colors hover:bg-surface-3">
            <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="lg" />
            <div>
              <div className="text-[13px] font-medium text-text-primary">{getClubDisplayName(club, locale)}</div>
              <div className="text-[11px] text-text-muted">{club.stadium}</div>
            </div>
          </Link>
        ))}
      </div>
    </SectionCard>
  );
}

function toKnockoutLegView(match: Match): KnockoutTieLegView {
  return {
    id: match.id,
    href: `/matches/${match.id}`,
    dateLabel: `${match.date}${match.time ? ` · ${match.time}` : ''}`,
    homeTeamId: match.homeTeamId,
    homeTeamName: match.homeTeamName ?? match.homeTeamId,
    homeTeamLogo: match.homeTeamLogo,
    awayTeamId: match.awayTeamId,
    awayTeamName: match.awayTeamName ?? match.awayTeamId,
    awayTeamLogo: match.awayTeamLogo,
    homeScore: match.homeScore,
    awayScore: match.awayScore,
  };
}

function aggregateKnockoutRound(stage: KnockoutStageView): KnockoutBracketRoundView {
  const ties = new Map<string, KnockoutTieCardView>();

  for (const match of stage.matches) {
    const key = [match.homeTeamId, match.awayTeamId].sort().join('::');
    const existing = ties.get(key);

    if (!existing) {
      ties.set(key, {
        id: key,
        participantIds: [match.homeTeamId, match.awayTeamId],
        legs: [toKnockoutLegView(match)],
      });
      continue;
    }

    existing.legs.push(toKnockoutLegView(match));
  }

  return {
    name: stage.name,
    ties: Array.from(ties.values())
      .map((tie) => ({
        ...tie,
        legs: tie.legs.sort((left, right) => {
          const leftValue = left.dateLabel;
          const rightValue = right.dateLabel;
          return leftValue.localeCompare(rightValue);
        }),
      }))
      .sort((left, right) => left.id.localeCompare(right.id)),
  };
}

function renderBracketScore(value: number | null) {
  return value === null ? '-' : String(value);
}

function overlapsWithTie(left: KnockoutTieCardView, right: KnockoutTieCardView) {
  if (right.sourceTieIds?.includes(left.id)) {
    return true;
  }

  return left.participantIds.some((id) => right.participantIds.includes(id));
}

function createPlaceholderTie(roundName: string, index: number, leftSource: KnockoutTieCardView, rightSource: KnockoutTieCardView): KnockoutTieCardView {
  const roundShort = roundName === 'Quarter-finals' ? 'QF' : roundName === 'Semi-finals' ? 'SF' : 'F';
  return {
    id: `placeholder-${roundName.toLowerCase().replace(/\s+/g, '-')}-${index + 1}`,
    participantIds: [`placeholder-${roundShort}-${index * 2 + 1}`, `placeholder-${roundShort}-${index * 2 + 2}`],
    sourceTieIds: [leftSource.id, rightSource.id],
    legs: [{
      id: `placeholder-leg-${roundShort}-${index + 1}`,
      dateLabel: 'TBD',
      homeTeamId: `placeholder-home-${roundShort}-${index + 1}`,
      homeTeamName: `Winner ${roundShort}${index * 2 + 1}`,
      awayTeamId: `placeholder-away-${roundShort}-${index + 1}`,
      awayTeamName: `Winner ${roundShort}${index * 2 + 2}`,
      homeScore: null,
      awayScore: null,
    }],
  };
}

function createOpenPlaceholderTie(roundName: string, index: number): KnockoutTieCardView {
  const roundShort = roundName === 'Round of 16'
    ? 'R16'
    : roundName === 'Quarter-finals'
      ? 'QF'
      : roundName === 'Semi-finals'
        ? 'SF'
        : 'F';
  return {
    id: `open-placeholder-${roundName.toLowerCase().replace(/\s+/g, '-')}-${index + 1}`,
    participantIds: [`placeholder-${roundShort}-${index * 2 + 1}`, `placeholder-${roundShort}-${index * 2 + 2}`],
    legs: [{
      id: `open-placeholder-leg-${roundShort}-${index + 1}`,
      dateLabel: 'TBD',
      homeTeamId: `placeholder-home-${roundShort}-${index + 1}`,
      homeTeamName: `TBD ${roundShort}${index * 2 + 1}`,
      awayTeamId: `placeholder-away-${roundShort}-${index + 1}`,
      awayTeamName: `TBD ${roundShort}${index * 2 + 2}`,
      homeScore: null,
      awayScore: null,
    }],
  };
}

function ensureBracketStages(stages: KnockoutStageView[], stageTargetCounts?: Map<string, number>) {
  const stageMap = new Map(stages.map((stage) => [stage.name, aggregateKnockoutRound(stage)]));
  const orderedNames = ['Round of 16', 'Quarter-finals', 'Semi-finals', 'Final'];
  const rounds: KnockoutBracketRoundView[] = [];

  for (const stageName of orderedNames) {
    const existing = stageMap.get(stageName);
    const targetCount = stageTargetCounts?.get(stageName);
    if (existing && existing.ties.length > 0) {
      const ties = [...existing.ties];
      if (targetCount && ties.length < targetCount && rounds.length === 0) {
        while (ties.length < targetCount) {
          ties.push(createOpenPlaceholderTie(stageName, ties.length));
        }
      }
      rounds.push({ ...existing, ties });
      continue;
    }

    const previousRound = rounds.at(-1);
    if (!previousRound) {
      continue;
    }

    if (rounds.length === 0) {
      const openCount = targetCount ?? 0;
      if (openCount === 0) {
        continue;
      }

      rounds.push({
        name: stageName,
        ties: Array.from({ length: openCount }, (_, index) => createOpenPlaceholderTie(stageName, index)),
      });
      continue;
    }

    const placeholderTies: KnockoutTieCardView[] = [];
    for (let index = 0; index < previousRound.ties.length; index += 2) {
      const left = previousRound.ties[index];
      const right = previousRound.ties[index + 1];
      if (!left || !right) {
        continue;
      }

      placeholderTies.push(createPlaceholderTie(stageName, placeholderTies.length, left, right));
    }

    if (targetCount && placeholderTies.length < targetCount) {
      while (placeholderTies.length < targetCount) {
        placeholderTies.push(createOpenPlaceholderTie(stageName, placeholderTies.length));
      }
    }

    if (placeholderTies.length > 0) {
      rounds.push({ name: stageName, ties: placeholderTies });
    }
  }

  return rounds;
}

function buildBracketSideRounds(rounds: KnockoutBracketRoundView[], finalTie: KnockoutTieCardView, sideIndex: 0 | 1) {
  const semiRound = rounds.at(-2);
  if (!semiRound) {
    return [];
  }

  const semifinalTie = finalTie.sourceTieIds?.length
    ? semiRound.ties.find((tie) => tie.id === finalTie.sourceTieIds?.[sideIndex])
    : semiRound.ties.find((tie) => tie.participantIds.includes(finalTie.participantIds[sideIndex]));
  if (!semifinalTie) {
    return [];
  }

  const sideRounds: KnockoutBracketRoundView[] = [{ name: semiRound.name, ties: [semifinalTie] }];
  let parentTies = [semifinalTie];

  for (let roundIndex = rounds.length - 3; roundIndex >= 0; roundIndex -= 1) {
    const round = rounds[roundIndex];
    const ties: KnockoutTieCardView[] = [];

    for (const parentTie of parentTies) {
        const childTies = round.ties
          .filter((tie) => overlapsWithTie(tie, parentTie))
          .sort((left, right) => {
            const leftName = left.legs[0]?.homeTeamName ?? left.id;
            const rightName = right.legs[0]?.homeTeamName ?? right.id;
            return leftName.localeCompare(rightName);
          });
      ties.push(...childTies);
    }

    if (ties.length === 0) {
      continue;
    }

    sideRounds.unshift({ name: round.name, ties });
    parentTies = ties;
  }

  return sideRounds;
}

function getCanvasY(position: number, heightPx: number) {
  return (position / 100) * heightPx;
}

function renderRoundConnectors(
  round: KnockoutBracketRoundView,
  side: 'left' | 'right',
  heightPx: number,
  roundPositions: Map<string, number>,
  nextRound?: KnockoutBracketRoundView,
  nextRoundPositions?: Map<string, number>,
) {
  if (!nextRound || !nextRoundPositions) {
    return null;
  }

  const edgeX = side === 'left' ? 220 : 0;
  const spineX = side === 'left' ? 244 : -24;
  const parentX = side === 'left' ? 268 : -48;

  return (
    <svg className="pointer-events-none absolute inset-0 z-0 overflow-visible" width="220" height={heightPx}>
      {nextRound.ties.map((parentTie) => {
        const childPositions = round.ties
          .filter((childTie) => overlapsWithTie(childTie, parentTie))
          .map((childTie) => roundPositions.get(childTie.id))
          .filter((value): value is number => value !== undefined)
          .map((value) => getCanvasY(value, heightPx));

        const parentPosition = nextRoundPositions.get(parentTie.id);
        if (childPositions.length === 0 || parentPosition === undefined) {
          return null;
        }

        const top = Math.min(...childPositions);
        const bottom = Math.max(...childPositions);
        const parentY = getCanvasY(parentPosition, heightPx);

        return (
          <g key={parentTie.id}>
            {childPositions.map((childY, index) => (
              <line
                key={`${parentTie.id}-child-${index}`}
                x1={edgeX}
                y1={childY}
                x2={spineX}
                y2={childY}
                stroke="rgba(120, 136, 153, 0.35)"
                strokeWidth="1.5"
                strokeLinecap="round"
              />
            ))}
            <line
              x1={spineX}
              y1={top}
              x2={spineX}
              y2={bottom}
              stroke="rgba(120, 136, 153, 0.35)"
              strokeWidth="1.5"
              strokeLinecap="round"
            />
            <line
              x1={spineX}
              y1={parentY}
              x2={parentX}
              y2={parentY}
              stroke="rgba(120, 136, 153, 0.35)"
              strokeWidth="1.5"
              strokeLinecap="round"
            />
          </g>
        );
      })}
    </svg>
  );
}

function getBaseBracketPositions(ties: KnockoutTieCardView[]) {
  const topPadding = 10;
  const bottomPadding = 90;
  const step = ties.length === 1 ? 0 : (bottomPadding - topPadding) / (ties.length - 1);

  return new Map(
    ties.map((tie, index) => [tie.id, ties.length === 1 ? 50 : topPadding + (step * index)]),
  );
}

function buildBracketRoundPositions(rounds: KnockoutBracketRoundView[]) {
  const positions = new Map<string, Map<string, number>>();
  const firstRound = rounds[0];
  if (!firstRound) {
    return positions;
  }

  positions.set(firstRound.name, getBaseBracketPositions(firstRound.ties));

  for (let index = 1; index < rounds.length; index += 1) {
    const round = rounds[index];
    const previousRound = rounds[index - 1];
    const previousPositions = positions.get(previousRound.name);
    if (!previousPositions) {
      continue;
    }

    const roundPositions = new Map<string, number>();
    for (const tie of round.ties) {
      const childPositions = previousRound.ties
        .filter((childTie) => overlapsWithTie(childTie, tie))
        .map((childTie) => previousPositions.get(childTie.id))
        .filter((value): value is number => value !== undefined);

      if (childPositions.length === 0) {
        continue;
      }

      const average = childPositions.reduce((sum, value) => sum + value, 0) / childPositions.length;
      roundPositions.set(tie.id, average);
    }

    positions.set(round.name, roundPositions);
  }

  return positions;
}

function renderBracketColumn(
  round: KnockoutBracketRoundView,
  side: 'left' | 'right',
  isEdge: boolean,
  heightPx: number,
  roundPositions: Map<string, number>,
  nextRound?: KnockoutBracketRoundView,
  nextRoundPositions?: Map<string, number>,
  key?: string,
) {
  const connectorClass = side === 'left' ? 'right-[-17px]' : 'left-[-17px]';
  const ties = side === 'right' ? round.ties.slice().reverse() : round.ties;

  return (
    <div key={key} className="w-[220px]">
      <div className="mb-3 text-[11px] font-semibold uppercase tracking-wider text-text-muted">{round.name}</div>
      <div className="relative" style={{ height: `${heightPx}px` }}>
        {!isEdge ? renderRoundConnectors(round, side, heightPx, roundPositions, nextRound, nextRoundPositions) : null}
        {ties.map((tie, index) => (
          <div
            key={tie.id}
            className="absolute left-0 right-0 z-10 -translate-y-1/2"
            style={{ top: `${roundPositions.get(tie.id) ?? ((index + 0.5) / ties.length) * 100}%` }}
          >
            <div className="relative rounded-lg border border-border-subtle bg-surface-2 px-3 py-3 shadow-sm">
              {!isEdge ? <div className={`absolute top-1/2 h-px w-4 -translate-y-1/2 bg-border ${connectorClass}`} /> : null}
              <div className="space-y-2.5">
                {tie.legs.map((match, legIndex) => (
                  <Link
                    key={match.id}
                    href={`/matches/${match.id}`}
                    className="block rounded px-1 py-1 transition-colors hover:bg-surface-3/60"
                  >
                  <div className="mb-1 text-[10px] uppercase tracking-wider text-text-muted">
                    {tie.legs.length > 1 ? `${legIndex + 1}${legIndex === 0 ? 'st' : 'nd'} Leg · ` : ''}
                    {match.dateLabel}
                  </div>
                    <div className="space-y-1.5">
                      <div className="flex items-center justify-between gap-3">
                        <div className="flex min-w-0 items-center gap-2">
                          {renderBracketTeamMarker(match.homeTeamId, match.homeTeamName, match.homeTeamLogo, match.href)}
                          <span className="truncate text-[12px] font-medium text-text-primary">{match.homeTeamName}</span>
                        </div>
                        <span className="text-[12px] font-semibold tabular-nums text-text-primary">{renderBracketScore(match.homeScore)}</span>
                      </div>
                      <div className="flex items-center justify-between gap-3">
                        <div className="flex min-w-0 items-center gap-2">
                          {renderBracketTeamMarker(match.awayTeamId, match.awayTeamName, match.awayTeamLogo, match.href)}
                          <span className="truncate text-[12px] font-medium text-text-primary">{match.awayTeamName}</span>
                        </div>
                        <span className="text-[12px] font-semibold tabular-nums text-text-primary">{renderBracketScore(match.awayScore)}</span>
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function renderKnockoutBracket(stages: KnockoutStageView[]) {
  const stageTargetCounts = new Map<string, number>([
    ['Round of 16', 8],
    ['Quarter-finals', 4],
    ['Semi-finals', 2],
    ['Final', 1],
  ]);
  const rounds = ensureBracketStages(stages, stageTargetCounts);
  const finalRound = rounds.at(-1);
  if (!finalRound || finalRound.ties.length === 0) {
    return null;
  }

  const finalTie = finalRound.ties[0];
  if (!finalTie) {
    return null;
  }

  const leftRounds = buildBracketSideRounds(rounds, finalTie, 0);
  const rightRounds = buildBracketSideRounds(rounds, finalTie, 1);
  const orderedRightRounds = rightRounds.slice().reverse();
  const leftRoundPositions = buildBracketRoundPositions(leftRounds);
  const rightRoundPositions = buildBracketRoundPositions(rightRounds);
  const maxTiesPerSide = Math.max(
    1,
    ...leftRounds.map((round) => round.ties.length),
    ...orderedRightRounds.map((round) => round.ties.length),
  );
  const maxLegCount = Math.max(
    1,
    ...leftRounds.flatMap((round) => round.ties.map((tie) => tie.legs.length)),
    ...orderedRightRounds.flatMap((round) => round.ties.map((tie) => tie.legs.length)),
    ...finalRound.ties.map((tie) => tie.legs.length),
  );
  const heightPx = Math.max(520, maxTiesPerSide * (maxLegCount > 1 ? 230 : 170));

  return (
    <div className="-mx-1 overflow-x-auto px-1">
      <div className="grid min-w-max auto-cols-[220px] grid-flow-col gap-4">
        {leftRounds.map((round, index) => renderBracketColumn(
          round,
          'left',
          false,
          heightPx,
          leftRoundPositions.get(round.name) ?? new Map(),
          index === leftRounds.length - 1 ? finalRound : leftRounds[index + 1],
          index === leftRounds.length - 1 ? new Map([[finalTie.id, 50]]) : leftRoundPositions.get(leftRounds[index + 1].name),
          `left-${round.name}`,
        ))}
        {renderBracketColumn(finalRound, 'left', true, heightPx, new Map([[finalTie.id, 50]]))}
        {orderedRightRounds.map((round, index) => renderBracketColumn(
          round,
          'right',
          false,
          heightPx,
          rightRoundPositions.get(round.name) ?? new Map(),
          index === 0 ? finalRound : orderedRightRounds[index - 1],
          index === 0 ? new Map([[finalTie.id, 50]]) : rightRoundPositions.get(orderedRightRounds[index - 1].name),
          `right-${round.name}`,
        ))}
      </div>
    </div>
  );
}

export async function LeagueDetailSections({
  league,
  locale,
  selectedSeason,
  isNonDefaultSeason,
  isTournament,
  seasonQueryParam,
  resultsPage,
  fixturesPage,
  initialTab,
}: LeagueDetailSectionsProps) {
  const clubDataLocale = locale;
  const clubHref = (clubId: string) => buildClubLeagueContextHref(clubId, league.id, selectedSeason?.seasonId);

  const needsFullMatches = isTournament;
  const needsStandingsForm = initialTab === 'overview';
  const [tLeague, tTable, tCommon, standings, seasonClubs, allMatches, topScorerRows] = await Promise.all([
    getTranslations('league'),
    getTranslations('table'),
    getTranslations('common'),
    selectedSeason
      ? getStandingsByLeagueAndSeasonDb(league.id, selectedSeason.seasonId, clubDataLocale, needsStandingsForm)
      : getStandingsByLeagueDb(league.id, clubDataLocale, needsStandingsForm),
    isTournament
      ? selectedSeason
      ? getClubsByLeagueAndSeasonDb(league.id, selectedSeason.seasonId, clubDataLocale)
      : getClubsByLeagueDb(league.id, clubDataLocale)
      : Promise.resolve([]),
    needsFullMatches
      ? selectedSeason
        ? getMatchesByLeagueAndSeasonDb(league.id, selectedSeason.seasonId, clubDataLocale)
        : getMatchesByLeagueDb(league.id, clubDataLocale)
      : Promise.resolve([]),
    initialTab === 'stats'
      ? selectedSeason
        ? getTopScorerRowsBySeasonDb(league.id, selectedSeason.seasonId, clubDataLocale, 10)
        : getTopScorerRowsDb(league.id, clubDataLocale, 10)
      : Promise.resolve([]),
  ]);
  const displayClubs = seasonClubs;
  const clubById = new Map(displayClubs.map((club) => [club.id, club]));
  const localizedStandings = isTournament
    ? standings.map((row) => {
        const club = clubById.get(row.clubId);

        if (!club) {
          return row;
        }

        return {
          ...row,
          clubName: getClubDisplayName(club, locale),
          clubShortName: club.shortName,
          clubLogo: club.logo ?? row.clubLogo,
        };
      })
    : standings;
  const finishedMatches = allMatches.filter((match) => match.status === 'finished');
  const scheduledMatches = allMatches.filter((match) => match.status === 'scheduled');
  const recentResults = finishedMatches;
  const upcomingFixtures = scheduledMatches;
  const paginatedRecentResults = paginateMatches(recentResults, resultsPage);
  const paginatedUpcomingFixtures = paginateMatches(upcomingFixtures, fixturesPage);
  const recentResultsDateLabel = renderMatchSectionDateLabel(recentResults[0], locale);
  const upcomingFixturesDateLabel = renderMatchSectionDateLabel(upcomingFixtures[0], locale);
  const buildMatchesTabHref = (params: { resultsPage?: number; fixturesPage?: number }) => {
    const query = new URLSearchParams({ tab: 'matches' });

    if (seasonQueryParam) {
      query.set('season', seasonQueryParam);
    }

    if (params.resultsPage && params.resultsPage > 1) {
      query.set('resultsPage', String(params.resultsPage));
    }

    if (params.fixturesPage && params.fixturesPage > 1) {
      query.set('fixturesPage', String(params.fixturesPage));
    }

    return `/competitions/${league.id}?${query.toString()}`;
  };
  const hasLeaguePhaseMatches = allMatches.some((match) => {
    const normalizedStage = match.stage?.replace(/_/g, ' ').trim().toLowerCase() ?? '';
    return normalizedStage === 'league phase' || /^league (stage|phase)\s*-?\s*\d+$/i.test(match.stage ?? '');
  });
  const hasLegacyGroupMatches = allMatches.some((match) => isLegacyGroupStageMatch(match));
  const competitionFormat = selectedSeason?.competitionFormat ?? league.competitionFormat;
  const tournamentFormat = competitionFormat === 'league_phase'
    ? 'league-phase'
    : competitionFormat === 'group_knockout'
      ? 'legacy'
      : hasLeaguePhaseMatches
        ? 'league-phase'
        : hasLegacyGroupMatches
          ? 'legacy'
          : undefined;
  const tournamentAnalysis = isTournament ? analyzeTournamentMatches(allMatches, displayClubs) : undefined;
  const tournamentGroups = isTournament && tournamentFormat !== 'league-phase'
    ? buildTournamentGroups(allMatches, displayClubs)
    : [];
  const qualifyingStages = isTournament ? (tournamentAnalysis?.qualifyingStages ?? []) : [];
  const leaguePhaseStages = isTournament && tournamentFormat === 'league-phase' ? (tournamentAnalysis?.leaguePhaseStages ?? []) : [];
  const legacyGroupStageMatches = isTournament && tournamentFormat === 'legacy'
    ? buildGroupStageMatches(allMatches)
    : undefined;
  const hasGroupStyleStage = tournamentGroups.length > 0 || Boolean(legacyGroupStageMatches);
  const hasLeaguePhaseStage = tournamentFormat === 'league-phase' || leaguePhaseStages.length > 0;
  const isKnockoutOnlyTournament = isTournament
    && (competitionFormat === 'knockout' || (!competitionFormat && !hasGroupStyleStage && !hasLeaguePhaseStage && !tournamentFormat));
  const standingsTitle = isTournament
    ? tournamentFormat === 'legacy'
      ? tLeague('groupStageSnapshotLegacy')
      : isKnockoutOnlyTournament
        ? tLeague('participants')
        : tLeague('groupStageSnapshot')
    : tLeague('standings');
  const clubsTitle = isTournament ? tLeague('participants') : tLeague('clubsList');
  const resultsTitle = isTournament ? tLeague('tournamentResults') : tLeague('recentResults');
  const fixturesTitle = isTournament ? tLeague('tournamentFixtures') : tLeague('upcomingFixtures');
  const topScorersTitle = isTournament ? tLeague('topPerformers') : tLeague('topScorers');
  const participantRows = displayClubs.slice(0, 8);
  const leaguePhaseStandings = isTournament && tournamentFormat === 'league-phase'
    ? buildLeaguePhaseStandings(allMatches, displayClubs)
    : localizedStandings;
  const knockoutStages = isTournament ? (tournamentAnalysis?.knockoutStages ?? []) : [];
  const bracketStageNames = new Set(['round of 16', 'quarter-finals', 'semi-finals', 'final']);
  const bracketStages = knockoutStages.filter((stage) => bracketStageNames.has(stage.name.toLowerCase()));
  const formatDetail = tournamentFormat === 'legacy'
    ? tLeague('formatTournamentGroupStageDetail')
    : tournamentFormat === 'league-phase'
      ? tLeague('formatChampionsLeagueLeaguePhaseDetail')
      : isKnockoutOnlyTournament
        ? tLeague('formatTournamentKnockoutOnlyDetail')
        : tLeague('formatTournamentDetail');
  const trackingMode = tournamentFormat === 'league-phase'
      ? tLeague('trackingModeChampionsLeagueLeaguePhase')
      : isKnockoutOnlyTournament
        ? tLeague('trackingModeKnockoutOnly')
        : tLeague('trackingModeTournament');
  const advancingRule = tournamentFormat === 'league-phase'
      ? tLeague('advancingRuleLeaguePhase')
      : isKnockoutOnlyTournament
        ? tLeague('advancingRuleKnockoutOnly')
        : tLeague('advancingRule');
  const stageTabs = [
      ...knockoutStages.slice().reverse().map((stage) => ({
        key: `knockout-${stage.name}`,
        label: stage.name,
        content: (
          <MatchArchiveSplitList
            matches={stage.matches}
            locale={locale}
            recentResultsLabel={tLeague('recentResults')}
            upcomingFixturesLabel={tLeague('upcomingFixtures')}
            emptyLabel={tLeague('noMatches')}
          />
        ),
      })),
      ...(tournamentFormat === 'league-phase'
        ? leaguePhaseStages.slice().reverse().map((stage) => ({
            key: stage.id,
            label: stage.name,
            content: (
              <MatchArchiveSplitList
                matches={stage.matches}
                locale={locale}
                recentResultsLabel={tLeague('recentResults')}
                upcomingFixturesLabel={tLeague('upcomingFixtures')}
                emptyLabel={tLeague('noMatches')}
              />
            ),
          }))
        : legacyGroupStageMatches
          ? [{
              key: legacyGroupStageMatches.id,
              label: tLeague('groupStage'),
              content: (
                <MatchArchiveSplitList
                  matches={legacyGroupStageMatches.matches}
                  locale={locale}
                  recentResultsLabel={tLeague('recentResults')}
                  upcomingFixturesLabel={tLeague('upcomingFixtures')}
                  emptyLabel={tLeague('noMatches')}
                />
              ),
            }]
          : []),
      ...qualifyingStages.map((stage) => ({
        key: stage.id,
        label: stage.name,
        content: (
          <MatchArchiveSplitList
            matches={stage.matches}
            locale={locale}
            recentResultsLabel={tLeague('recentResults')}
            upcomingFixturesLabel={tLeague('upcomingFixtures')}
            emptyLabel={tLeague('noMatches')}
          />
        ),
      })),
    ];
  const stageMatchesCount = stageTabs.length;
  const stageMatchesDateLabel = knockoutStages[0]?.matches[0]?.date
    ?? leaguePhaseStages.at(-1)?.matches[0]?.date
    ?? legacyGroupStageMatches?.matches[0]?.date
    ?? qualifyingStages.at(-1)?.matches[0]?.date;
  const stageMatchesHeaderDateLabel = formatMatchSectionDate(stageMatchesDateLabel, locale);

  const overviewContent = isTournament ? (
    <div className="grid grid-cols-12 gap-4">
      <div className="col-span-8 space-y-4">
        <SectionCard title={tLeague('competitionOverview')}>
          <div className="grid grid-cols-3 gap-3 text-[12px] text-text-secondary">
            <div>
              <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{tLeague('format')}</div>
              <div className="text-[13px] font-medium text-text-primary">{formatDetail}</div>
            </div>
            <div>
              <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{tLeague('participants')}</div>
              <div className="text-[13px] font-medium text-text-primary">{league.numberOfClubs}</div>
            </div>
            <div>
              <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{tLeague('trackingMode')}</div>
              <div className="text-[13px] font-medium text-text-primary">{trackingMode}</div>
            </div>
            <Suspense fallback={<div className="h-[42px] rounded bg-surface-2/60 animate-pulse" />}>
              <TournamentChampionValue knockoutStages={knockoutStages} locale={locale} label={tLeague('champion')} />
            </Suspense>
          </div>
        </SectionCard>

        {tournamentGroups.length > 0 ? (
          <SectionCard title={tLeague('groupStage')}>
            <div className="mb-3 text-[12px] text-text-secondary">{advancingRule}</div>
            <div className="grid grid-cols-2 gap-4">
              {tournamentGroups.map((group) => (
                <div key={group.id} className="overflow-hidden rounded border border-border-subtle bg-surface-2">
                  <div className="border-b border-border-subtle px-3 py-2 text-[11px] font-semibold uppercase tracking-wider text-text-primary">
                    {group.name}
                  </div>
                  <StandingsTable standings={group.standings} compact getClubHref={clubHref} />
                </div>
              ))}
            </div>
          </SectionCard>
        ) : null}
      </div>

      <div className="col-span-4 space-y-4">
        {tournamentGroups.length === 0 && !isKnockoutOnlyTournament ? (
          <SectionCard title={standingsTitle} noPadding>
            <StandingsTable standings={leaguePhaseStandings} compact getClubHref={clubHref} />
          </SectionCard>
        ) : null}

        <SectionCard title={clubsTitle}>
          <div className="space-y-2">
            {participantRows.map((club) => (
              <Link key={club.id} href={clubHref(club.id)} className="flex items-center gap-3 rounded border border-border-subtle bg-surface-2 px-3 py-2 transition-colors hover:bg-surface-3">
                <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="sm" />
                <div className="min-w-0">
                  <div className="truncate text-[13px] font-medium text-text-primary">{getClubDisplayName(club, locale)}</div>
                  <div className="truncate text-[11px] text-text-muted">{club.stadium}</div>
                </div>
              </Link>
            ))}
          </div>
        </SectionCard>
      </div>
    </div>
  ) : (
    <div className="grid grid-cols-12 gap-4">
      <div className="col-span-8 space-y-4">
        <SectionCard title={standingsTitle} noPadding>
          <StandingsTable standings={localizedStandings} getClubHref={clubHref} />
        </SectionCard>

        <Suspense
          fallback={
            <SectionCard title={clubsTitle}>
              <div className="space-y-2">
                {Array.from({ length: 6 }, (_, index) => (
                  <div key={index} className="rounded border border-border-subtle bg-surface-2/50 p-3">
                    <div className="h-3 w-1/3 animate-pulse rounded bg-surface-3/80" />
                    <div className="mt-2 h-3 w-full animate-pulse rounded bg-surface-3/80" />
                  </div>
                ))}
              </div>
            </SectionCard>
          }
        >
          <LeagueClubListSection
            leagueId={league.id}
            locale={locale}
            title={clubsTitle}
            seasonId={isNonDefaultSeason && selectedSeason ? selectedSeason.seasonId : undefined}
          />
        </Suspense>
      </div>
    </div>
  );

  const matchesContent = (
    <div className="grid grid-cols-12 gap-4">
      <div className="col-span-12 space-y-4">
        {isTournament ? (
          <>
            {stageTabs.length > 0 ? (
          <SectionCard title={<MatchSectionTitle title={tLeague('stageMatches')} count={stageMatchesCount} dateLabel={stageMatchesHeaderDateLabel} variant="stage" />}>
            <div className="mb-3 text-[12px] text-text-secondary">{advancingRule}</div>
            <TabGroup tabs={stageTabs} defaultTab={stageTabs[0]?.key} />
          </SectionCard>
            ) : null}

            {bracketStages.length > 0 ? (
          <SectionCard title={tLeague('knockoutBracket')}>
            {renderKnockoutBracket(bracketStages)}
          </SectionCard>
            ) : null}

            <SectionCard
              title={<MatchSectionTitle title={resultsTitle} count={recentResults.length} dateLabel={recentResultsDateLabel} variant="results" />}
              action={(
                <PaginationNav
                  currentPage={paginatedRecentResults.currentPage}
                  totalPages={paginatedRecentResults.totalPages}
                  hrefForPage={(page) => buildMatchesTabHref({ resultsPage: page, fixturesPage: paginatedUpcomingFixtures.currentPage })}
                  previousLabel={tCommon('previous')}
                  nextLabel={tCommon('next')}
                  pageLabel={tCommon('pageOf', { page: paginatedRecentResults.currentPage, totalPages: paginatedRecentResults.totalPages })}
                />
              )}
            >
              <MatchArchiveSplitList
                matches={paginatedRecentResults.items}
                locale={locale}
                recentResultsLabel={tLeague('recentResults')}
                upcomingFixturesLabel={tLeague('upcomingFixtures')}
                emptyLabel={tLeague('noMatches')}
              />
            </SectionCard>

            <SectionCard
              title={<MatchSectionTitle title={fixturesTitle} count={upcomingFixtures.length} dateLabel={upcomingFixturesDateLabel} variant="fixtures" />}
              action={(
                <PaginationNav
                  currentPage={paginatedUpcomingFixtures.currentPage}
                  totalPages={paginatedUpcomingFixtures.totalPages}
                  hrefForPage={(page) => buildMatchesTabHref({ resultsPage: paginatedRecentResults.currentPage, fixturesPage: page })}
                  previousLabel={tCommon('previous')}
                  nextLabel={tCommon('next')}
                  pageLabel={tCommon('pageOf', { page: paginatedUpcomingFixtures.currentPage, totalPages: paginatedUpcomingFixtures.totalPages })}
                />
              )}
            >
              <MatchArchiveSplitList
                matches={paginatedUpcomingFixtures.items}
                locale={locale}
                recentResultsLabel={tLeague('recentResults')}
                upcomingFixturesLabel={tLeague('upcomingFixtures')}
                emptyLabel={tLeague('noMatches')}
              />
            </SectionCard>
          </>
        ) : (
          <CompetitionMatchesPanel
            key={`matches-${league.id}-${selectedSeason?.seasonId ?? 'current'}`}
            competitionId={league.id}
            seasonId={isNonDefaultSeason && selectedSeason ? selectedSeason.seasonId : undefined}
            initialMatches={initialTab === 'matches' ? allMatches : undefined}
            initialResultsPage={resultsPage}
            initialFixturesPage={fixturesPage}
            locale={locale}
            labels={{
              resultsTitle,
              fixturesTitle,
              recentResults: tLeague('recentResults'),
              upcomingFixtures: tLeague('upcomingFixtures'),
              noMatches: tLeague('noMatches'),
              previous: tCommon('previous'),
              next: tCommon('next'),
              pageOf: tCommon('pageOf', { page: '{page}', totalPages: '{totalPages}' }),
            }}
          />
        )}
      </div>
    </div>
  );

  const statsContent = (
    <div className="grid grid-cols-12 gap-4">
      <div className="col-span-12 space-y-4">
        <CompetitionStatsPanel
          key={`stats-${league.id}-${selectedSeason?.seasonId ?? 'current'}`}
          competitionId={league.id}
          seasonId={isNonDefaultSeason && selectedSeason ? selectedSeason.seasonId : undefined}
          initialRows={initialTab === 'stats' ? topScorerRows : undefined}
          isTournament={isTournament}
          title={topScorersTitle}
          labels={{
            rank: tTable('rank'),
            player: tTable('player'),
            club: tTable('club'),
            goals: tTable('goals'),
            assists: tTable('assists'),
          }}
        />
      </div>
    </div>
  );

  return (
    <StaticDetailTabs
      initialTab={initialTab}
      basePath={`/competitions/${league.id}`}
      query={seasonQueryParam ? { season: seasonQueryParam } : undefined}
      tabs={[
        { key: 'overview', label: tCommon('tabOverview'), content: overviewContent },
        { key: 'matches', label: tCommon('tabMatches'), content: matchesContent },
        { key: 'stats', label: tCommon('tabStats'), content: statsContent },
      ]}
    />
  );
}
