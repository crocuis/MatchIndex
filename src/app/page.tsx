import Link from 'next/link';
import { Suspense, type ReactNode } from 'react';
import { getLocale, getTranslations } from 'next-intl/server';
import { DashboardCompetitionFocus } from '@/app/DashboardCompetitionFocus';
import { DashboardUpcomingFixtures } from '@/app/DashboardUpcomingFixtures';
import { analyzeTournamentMatches } from '@/app/competitions/[id]/tournamentView';
import { FixtureCard } from '@/components/data/FixtureCard';
import { PageHeader } from '@/components/layout/PageHeader';
import { MatchSectionTitle, renderMatchSectionDateLabel } from '@/components/ui/MatchSectionTitle';
import { SectionCard } from '@/components/ui/SectionCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { MatchCard } from '@/components/data/MatchCard';
import { StatPanel } from '@/components/data/StatPanel';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { CollapsibleList } from '@/components/ui/CollapsibleList';
import { cn } from '@/lib/utils';
import { isTournamentCompetition } from '@/data/competitionTypes';
import type { Club, League, Match, StandingRow } from '@/data/types';
import {
  getClubsByLeagueDb,
  getLeagueCountDb,
  getLeaguesByIdsDb,
  getMatchesByLeagueDb,
  getRecentFinishedMatchesByLeagueIdsDb,
  getStandingsByLeagueDb,
  getTopScorerRowsDb,
  getUpcomingScheduledMatchesByLeagueIdsDb,
} from '@/data/server';

const DASHBOARD_LEAGUE_IDS = [
  'premier-league',
  'la-liga',
  '1-bundesliga',
  'serie-a',
  'ligue-1',
  'champions-league',
  'europa-league',
] as const;

const DASHBOARD_PRIMARY_LEAGUE_IDS = [
  'premier-league',
  'la-liga',
  '1-bundesliga',
  'serie-a',
  'ligue-1',
] as const;

const DASHBOARD_SECONDARY_LEAGUE_IDS = [
  'champions-league',
  'europa-league',
] as const;

const DASHBOARD_DEFAULT_LEAGUE_ID = 'premier-league';

const DASHBOARD_UPCOMING_FIXTURE_LIMIT = 72;
const DASHBOARD_LEAGUE_SNAPSHOT_LIMIT = 10;
const DASHBOARD_TOURNAMENT_SNAPSHOT_LIMIT = 8;

interface DashboardGlobalOverviewData {
  recentResults: Match[];
  upcomingFixtures: Match[];
}

interface DashboardCompetitionFocusData {
  standings: StandingRow[];
  topScorers: DashboardTopScorersSectionProps['rows'];
  tournamentMatches: Match[];
  tournamentClubs: Club[];
}

type DashboardTournamentFocusMode = 'league-phase' | 'group-stage' | 'knockout' | 'empty';

interface DashboardFocusSummaryCardProps {
  title: string;
  seasonLabel: string;
  seasonValue: string;
  formatLabel: string;
  formatValue: string;
  participantsLabel: string;
  participantsValue: number;
  currentStageLabel?: string;
  currentStageValue?: string;
  stageTrail?: string[];
  highlightedStage?: string;
  note?: string;
  children?: ReactNode;
}

interface DashboardTopScorersSectionProps {
  title: string;
  rows: Array<{
    playerId: string;
    playerName: string;
    clubShortName: string;
    goals: number;
  }>;
  rankLabel: string;
  playerLabel: string;
  goalsLabel: string;
}

interface DashboardLeagueFocusPanelProps {
  league: League;
  standings: StandingRow[];
  topScorers: DashboardTopScorersSectionProps['rows'];
  viewFullTableLabel: string;
  seasonLabel: string;
  formatLabel: string;
  participantsLabel: string;
  standingsTitle: string;
  overviewTitle: string;
  topScorersTitle: string;
  pointsLabel: string;
  rankLabel: string;
  playerLabel: string;
  goalsLabel: string;
  formatValue: string;
}

interface DashboardTournamentFocusPanelProps {
  league: League;
  standings: StandingRow[];
  topScorers: DashboardTopScorersSectionProps['rows'];
  allMatches: Match[];
  clubs: Club[];
  participantCount: number;
  viewFullTableLabel: string;
  seasonLabel: string;
  formatLabel: string;
  participantsLabel: string;
  currentStageLabel: string;
  standingsSnapshotLabel: string;
  groupStageSnapshotLabel: string;
  knockoutBracketLabel: string;
  competitionOverviewTitle: string;
  topPerformersTitle: string;
  tournamentSnapshotPendingLabel: string;
  tournamentSnapshotPendingDetail: string;
  rankLabel: string;
  playerLabel: string;
  goalsLabel: string;
  formatValue: string;
  currentStageNote?: string;
  locale: string;
}

function getTournamentFocusMode(allMatches: Match[], hasGroupTables: boolean, hasKnockoutStages: boolean) {
  const hasActiveLeaguePhaseMatches = allMatches.some((match) => {
    const stageName = match.stage?.toLowerCase() ?? '';
    return (stageName.includes('league phase') || stageName.includes('league stage')) && match.status !== 'finished';
  });

  if (hasActiveLeaguePhaseMatches) {
    return 'league-phase' satisfies DashboardTournamentFocusMode;
  }

  const hasAnyGroupMatches = allMatches.some((match) => Boolean(match.groupName) || match.stage?.toLowerCase() === 'group stage');
  const hasActiveGroupMatches = hasGroupTables && allMatches.some((match) => (Boolean(match.groupName) || match.stage?.toLowerCase() === 'group stage') && match.status !== 'finished');
  if (hasActiveGroupMatches) {
    return 'group-stage' satisfies DashboardTournamentFocusMode;
  }

  if (hasKnockoutStages) {
    return 'knockout' satisfies DashboardTournamentFocusMode;
  }

  if (hasGroupTables && hasAnyGroupMatches) {
    return 'group-stage' satisfies DashboardTournamentFocusMode;
  }

  const hasLeaguePhaseMatches = allMatches.some((match) => {
    const stageName = match.stage?.toLowerCase() ?? '';
    return stageName.includes('league phase') || stageName.includes('league stage');
  });
  if (hasLeaguePhaseMatches) {
    return 'league-phase' satisfies DashboardTournamentFocusMode;
  }

  return 'empty' satisfies DashboardTournamentFocusMode;
}

function getCurrentKnockoutStage(stages: Array<{ name: string; matches: Match[] }>) {
  return stages.find((stage) => stage.matches.some((match) => match.status === 'live'))
    ?? stages.find((stage) => stage.matches.some((match) => match.status !== 'finished'))
    ?? stages.at(-1);
}

function buildTournamentStageTrail(allMatches: Match[], knockoutStages: Array<{ name: string; matches: Match[] }>) {
  const trail: string[] = [];

  if (allMatches.some((match) => Boolean(match.groupName) || match.stage?.toLowerCase() === 'group stage')) {
    trail.push('Group Stage');
  }

  if (allMatches.some((match) => (match.stage?.toLowerCase() ?? '').includes('league phase') || (match.stage?.toLowerCase() ?? '').includes('league stage'))) {
    trail.push('League Phase');
  }

  for (const stage of knockoutStages) {
    if (!trail.includes(stage.name)) {
      trail.push(stage.name);
    }
  }

  return trail;
}

function DashboardFocusSummaryCard({
  title,
  seasonLabel,
  seasonValue,
  formatLabel,
  formatValue,
  participantsLabel,
  participantsValue,
  currentStageLabel,
  currentStageValue,
  stageTrail,
  highlightedStage,
  note,
  children,
}: DashboardFocusSummaryCardProps) {
  return (
    <SectionCard title={title}>
      <div className="grid grid-cols-2 gap-3 text-[11px]">
        <div>
          <div className="mb-1 font-semibold uppercase tracking-wider text-text-muted">{seasonLabel}</div>
          <div className="text-[13px] font-medium text-text-primary">{seasonValue}</div>
        </div>
        <div>
          <div className="mb-1 font-semibold uppercase tracking-wider text-text-muted">{formatLabel}</div>
          <div className="text-[13px] font-medium text-text-primary">{formatValue}</div>
        </div>
        <div>
          <div className="mb-1 font-semibold uppercase tracking-wider text-text-muted">{participantsLabel}</div>
          <div className="text-[13px] font-medium tabular-nums text-text-primary">{participantsValue}</div>
        </div>
        {currentStageLabel && currentStageValue ? (
          <div>
            <div className="mb-1 font-semibold uppercase tracking-wider text-text-muted">{currentStageLabel}</div>
            <div className="text-[13px] font-medium text-text-primary">{currentStageValue}</div>
          </div>
        ) : null}
      </div>

      {note ? (
        <div className="mt-4 rounded-md border border-border-subtle bg-surface-2/70 px-3 py-2 text-[11px] leading-5 text-text-secondary">
          {note}
        </div>
      ) : null}

      {stageTrail && stageTrail.length > 0 ? (
        <div className="mt-4 flex flex-wrap gap-1.5">
          {stageTrail.map((stage) => (
            <span
              key={stage}
              className={cn(
                'rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide',
                stage === highlightedStage
                  ? 'border-accent-emerald/40 bg-accent-emerald/10 text-accent-emerald'
                  : 'border-border-subtle bg-surface-1 text-text-secondary'
              )}
            >
              {stage}
            </span>
          ))}
        </div>
      ) : null}

      {children}
    </SectionCard>
  );
}

function DashboardTopScorersSection({ title, rows, rankLabel, playerLabel, goalsLabel }: DashboardTopScorersSectionProps) {
  if (rows.length === 0) {
    return null;
  }

  return (
    <SectionCard title={title} noPadding>
      <table className="w-full">
        <thead>
          <tr className="border-b border-border bg-surface-2/30">
            <th className="w-8 px-3 py-2 text-left">{rankLabel}</th>
            <th className="px-3 py-2 text-left">{playerLabel}</th>
            <th className="w-12 px-3 py-2 text-center">{goalsLabel}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border-subtle">
          {rows.map((row, index) => (
            <tr key={row.playerId} className="group transition-colors hover:bg-surface-2">
              <td className="px-3 py-2 text-[12px] font-mono tabular-nums text-text-muted">{index + 1}</td>
              <td className="px-3 py-2 text-[13px]">
                <div className="flex flex-col">
                  <EntityLink type="player" id={row.playerId} className="font-medium transition-colors group-hover:text-accent-magenta">
                    {row.playerName}
                  </EntityLink>
                  <span className="mt-0.5 text-[10px] uppercase tracking-wider text-text-secondary">{row.clubShortName}</span>
                </div>
              </td>
              <td className="px-3 py-2 text-center text-[13px] font-bold tabular-nums text-accent-magenta">{row.goals}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </SectionCard>
  );
}

function DashboardLeagueFocusPanel({
  league,
  standings,
  topScorers,
  viewFullTableLabel,
  seasonLabel,
  formatLabel,
  participantsLabel,
  standingsTitle,
  overviewTitle,
  topScorersTitle,
  pointsLabel,
  rankLabel,
  playerLabel,
  goalsLabel,
  formatValue,
}: DashboardLeagueFocusPanelProps) {
  const leader = standings[0];
  const runnerUp = standings[1];
  const leaderGap = leader && runnerUp ? leader.points - runnerUp.points : undefined;
  const focusRows = standings.slice(0, DASHBOARD_LEAGUE_SNAPSHOT_LIMIT);

  return (
    <div className="grid grid-cols-1 gap-4 md:gap-6 lg:grid-cols-12">
      <div className="lg:col-span-8">
        <SectionCard
          title={standingsTitle}
          action={(
            <Link
              href={`/competitions/${league.id}`}
              prefetch
              className="text-[10px] font-bold uppercase tracking-wider text-accent-magenta transition-colors hover:text-accent-violet"
            >
              {viewFullTableLabel}
            </Link>
          )}
          noPadding
        >
          <StandingsTable standings={focusRows} compact />
        </SectionCard>
      </div>

      <div className="space-y-4 lg:col-span-4">
        <DashboardFocusSummaryCard
          title={overviewTitle}
          seasonLabel={seasonLabel}
          seasonValue={league.season}
          formatLabel={formatLabel}
          formatValue={formatValue}
          participantsLabel={participantsLabel}
          participantsValue={standings.length || league.numberOfClubs}
        >
          {leader ? (
            <div className="mt-4 rounded-md border border-border-subtle bg-surface-2/70 p-3">
              <div className="flex items-center justify-between text-[10px] font-semibold uppercase tracking-wider text-text-muted">
                <span>#1</span>
                {leaderGap && leaderGap > 0 ? <span>+{leaderGap}</span> : null}
              </div>
              <div className="mt-2 flex items-center gap-2">
                <ClubBadge shortName={leader.clubShortName ?? leader.clubName ?? '??'} clubId={leader.clubId} logo={leader.clubLogo} size="sm" />
                <div className="min-w-0">
                  <div className="truncate text-[13px] font-medium text-text-primary">{leader.clubName}</div>
                  <div className="text-[11px] text-text-secondary">{leader.points} {pointsLabel}</div>
                </div>
              </div>
            </div>
          ) : null}
        </DashboardFocusSummaryCard>

        <DashboardTopScorersSection
          title={topScorersTitle}
          rows={topScorers}
          rankLabel={rankLabel}
          playerLabel={playerLabel}
          goalsLabel={goalsLabel}
        />
      </div>
    </div>
  );
}

function DashboardTournamentFocusPanel({
  league,
  standings,
  topScorers,
  allMatches,
  clubs,
  participantCount,
  viewFullTableLabel,
  seasonLabel,
  formatLabel,
  participantsLabel,
  currentStageLabel,
  standingsSnapshotLabel,
  groupStageSnapshotLabel,
  knockoutBracketLabel,
  competitionOverviewTitle,
  topPerformersTitle,
  tournamentSnapshotPendingLabel,
  tournamentSnapshotPendingDetail,
  rankLabel,
  playerLabel,
  goalsLabel,
  formatValue,
  currentStageNote,
  locale,
}: DashboardTournamentFocusPanelProps) {
  const tournamentAnalysis = analyzeTournamentMatches(allMatches, clubs);
  const focusMode = getTournamentFocusMode(allMatches, tournamentAnalysis.groups.length > 0, tournamentAnalysis.knockoutStages.length > 0);
  const currentKnockoutStage = getCurrentKnockoutStage(tournamentAnalysis.knockoutStages);
  const stageTrail = buildTournamentStageTrail(allMatches, tournamentAnalysis.knockoutStages);
  const stageMatchesDateLabel = renderMatchSectionDateLabel(currentKnockoutStage?.matches[0], locale);
  const standingsSnapshotRows = standings.slice(0, DASHBOARD_TOURNAMENT_SNAPSHOT_LIMIT);

  return (
    <div className="grid grid-cols-1 gap-4 md:gap-6 lg:grid-cols-12">
      <div className="lg:col-span-8">
        {focusMode === 'knockout' && currentKnockoutStage ? (
          <SectionCard
            title={<MatchSectionTitle title={currentKnockoutStage.name} count={currentKnockoutStage.matches.length} dateLabel={stageMatchesDateLabel} variant="stage" />}
            action={(
              <Link
                href={`/competitions/${league.id}`}
                prefetch
                className="text-[10px] font-bold uppercase tracking-wider text-accent-magenta transition-colors hover:text-accent-violet"
              >
                {viewFullTableLabel}
              </Link>
            )}
          >
            <div className="space-y-4">
              <div className="flex flex-wrap gap-1.5">
                {stageTrail.map((stage) => (
                  <span
                    key={stage}
                    className={cn(
                      'rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide',
                      stage === currentKnockoutStage.name
                        ? 'border-accent-emerald/40 bg-accent-emerald/10 text-accent-emerald'
                        : 'border-border-subtle bg-surface-1 text-text-secondary'
                    )}
                  >
                    {stage}
                  </span>
                ))}
              </div>

              <div className="grid gap-3 xl:grid-cols-2">
                {currentKnockoutStage.matches.map((match) => (
                  match.status === 'finished'
                    ? <MatchCard key={match.id} match={match} />
                    : <FixtureCard key={match.id} match={match} />
                ))}
              </div>
            </div>
          </SectionCard>
        ) : null}

        {focusMode === 'group-stage' && tournamentAnalysis.groups.length > 0 ? (
          <SectionCard
            title={groupStageSnapshotLabel}
            action={(
              <Link
                href={`/competitions/${league.id}`}
                prefetch
                className="text-[10px] font-bold uppercase tracking-wider text-accent-magenta transition-colors hover:text-accent-violet"
              >
                {viewFullTableLabel}
              </Link>
            )}
          >
            <div className="grid gap-4 xl:grid-cols-2">
              {tournamentAnalysis.groups.map((group) => (
                <div key={group.id} className="overflow-hidden rounded-md border border-border-subtle bg-surface-2">
                  <div className="border-b border-border-subtle px-3 py-2 text-[11px] font-semibold uppercase tracking-wider text-text-primary">
                    {group.name}
                  </div>
                  <StandingsTable standings={group.standings} compact />
                </div>
              ))}
            </div>
          </SectionCard>
        ) : null}

        {focusMode === 'league-phase' ? (
          <SectionCard
            title={standingsSnapshotLabel}
            action={(
              <Link
                href={`/competitions/${league.id}`}
                prefetch
                className="text-[10px] font-bold uppercase tracking-wider text-accent-magenta transition-colors hover:text-accent-violet"
              >
                {viewFullTableLabel}
              </Link>
            )}
            noPadding
          >
            <StandingsTable standings={standingsSnapshotRows} compact />
          </SectionCard>
        ) : null}

        {focusMode === 'group-stage' && tournamentAnalysis.groups.length === 0 ? (
          <SectionCard title={standingsSnapshotLabel} noPadding>
            <StandingsTable standings={standingsSnapshotRows} compact />
          </SectionCard>
        ) : null}

        {focusMode === 'empty' ? (
          <SectionCard title={tournamentSnapshotPendingLabel}>
            <div className="text-[13px] leading-6 text-text-secondary">{tournamentSnapshotPendingDetail}</div>
          </SectionCard>
        ) : null}
      </div>

      <div className="space-y-4 lg:col-span-4">
        <DashboardFocusSummaryCard
          title={competitionOverviewTitle}
          seasonLabel={seasonLabel}
          seasonValue={league.season}
          formatLabel={formatLabel}
          formatValue={formatValue}
          participantsLabel={participantsLabel}
          participantsValue={participantCount || league.numberOfClubs}
          currentStageLabel={focusMode === 'empty' ? undefined : currentStageLabel}
          currentStageValue={focusMode === 'knockout' ? currentKnockoutStage?.name ?? knockoutBracketLabel : focusMode === 'group-stage' ? groupStageSnapshotLabel : focusMode === 'league-phase' ? standingsSnapshotLabel : undefined}
          stageTrail={stageTrail}
          highlightedStage={currentKnockoutStage?.name}
          note={focusMode === 'knockout' ? undefined : currentStageNote}
        />

        <DashboardTopScorersSection
          title={topPerformersTitle}
          rows={topScorers}
          rankLabel={rankLabel}
          playerLabel={playerLabel}
          goalsLabel={goalsLabel}
        />
      </div>
    </div>
  );
}

function DashboardHeaderStatsFallback({
  leagueCount,
  leaguesLabel,
  resultsLabel,
  upcomingLabel,
}: {
  leagueCount: number;
  leaguesLabel: string;
  resultsLabel: string;
  upcomingLabel: string;
}) {
  return (
    <StatPanel
      stats={[
        { label: leaguesLabel, value: leagueCount },
        { label: resultsLabel, value: '--' },
        { label: upcomingLabel, value: '--' },
      ]}
      columns={3}
      className="w-64"
    />
  );
}

function DashboardSectionSkeleton({ className }: { className: string }) {
  return <div className={cn('rounded-lg bg-surface-2 animate-pulse', className)} />;
}

function GlobalOverviewSectionFallback({ title }: { title: string }) {
  return (
    <section className="space-y-4">
      <div className="text-[10px] font-semibold uppercase tracking-[0.24em] text-text-muted">{title}</div>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
        <DashboardSectionSkeleton className="h-72" />
        <DashboardSectionSkeleton className="h-72" />
      </div>
    </section>
  );
}

function CompetitionFocusSectionFallback({ title }: { title: string }) {
  return (
    <section className="space-y-4">
      <div className="text-[10px] font-semibold uppercase tracking-[0.24em] text-text-muted">{title}</div>
      <DashboardSectionSkeleton className="h-28" />
      <div className="grid grid-cols-1 gap-4 md:gap-6 lg:grid-cols-12">
        <DashboardSectionSkeleton className="h-[26rem] lg:col-span-8" />
        <div className="space-y-4 lg:col-span-4">
          <DashboardSectionSkeleton className="h-44" />
          <DashboardSectionSkeleton className="h-64" />
        </div>
      </div>
    </section>
  );
}

async function getDashboardGlobalOverviewData(locale: string, featuredLeagueIds: string[]): Promise<DashboardGlobalOverviewData> {
  const [recentResults, upcomingFixtures] = await Promise.all([
    getRecentFinishedMatchesByLeagueIdsDb(featuredLeagueIds, locale, 6),
    getUpcomingScheduledMatchesByLeagueIdsDb(featuredLeagueIds, locale, DASHBOARD_UPCOMING_FIXTURE_LIMIT),
  ]);

  return {
    recentResults,
    upcomingFixtures,
  };
}

async function getDashboardCompetitionFocusData(league: League, locale: string): Promise<DashboardCompetitionFocusData> {
  const selectedLeagueIsTournament = isTournamentCompetition(league);
  const [standings, topScorers, tournamentMatches, tournamentClubs] = await Promise.all([
    getStandingsByLeagueDb(league.id, locale),
    getTopScorerRowsDb(league.id, locale, 5),
    selectedLeagueIsTournament
      ? getMatchesByLeagueDb(league.id, locale)
      : Promise.resolve([]),
    selectedLeagueIsTournament
      ? getClubsByLeagueDb(league.id, locale)
      : Promise.resolve([]),
  ]);

  return {
    standings,
    topScorers,
    tournamentMatches,
    tournamentClubs,
  };
}

async function DashboardHeaderStats({
  leagueCount,
  overviewPromise,
  leaguesLabel,
  resultsLabel,
  upcomingLabel,
}: {
  leagueCount: number;
  overviewPromise: Promise<DashboardGlobalOverviewData>;
  leaguesLabel: string;
  resultsLabel: string;
  upcomingLabel: string;
}) {
  const { recentResults, upcomingFixtures } = await overviewPromise;

  return (
    <StatPanel
      stats={[
        { label: leaguesLabel, value: leagueCount },
        { label: resultsLabel, value: recentResults.length },
        { label: upcomingLabel, value: upcomingFixtures.length },
      ]}
      columns={3}
      className="w-64"
    />
  );
}

async function GlobalOverviewSection({
  locale,
  overviewPromise,
}: {
  locale: string;
  overviewPromise: Promise<DashboardGlobalOverviewData>;
}) {
  const tDashboard = await getTranslations('dashboard');
  const { recentResults, upcomingFixtures } = await overviewPromise;
  const recentResultsDateLabel = renderMatchSectionDateLabel(recentResults[0], locale);

  return (
    <section className="space-y-4">
      <div>
        <div className="text-[10px] font-semibold uppercase tracking-[0.24em] text-text-muted">{tDashboard('globalMatchRadar')}</div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
        <SectionCard title={<MatchSectionTitle title={tDashboard('recentResults')} count={recentResults.length} dateLabel={recentResultsDateLabel} variant="results" />}>
          <CollapsibleList>
            {recentResults.map((match) => (
              <MatchCard key={match.id} match={match} />
            ))}
          </CollapsibleList>
        </SectionCard>

        <DashboardUpcomingFixtures
          matches={upcomingFixtures}
          title={tDashboard('upcomingFixtures')}
          emptyLabel={tDashboard('noUpcomingFixtures')}
        />
      </div>
    </section>
  );
}

async function CompetitionFocusSection({
  locale,
  featuredLeagues,
  initialLeagueId,
  allFocusPromise,
}: {
  locale: string;
  featuredLeagues: League[];
  initialLeagueId: string;
  allFocusPromise: Promise<DashboardCompetitionFocusData[]>;
}) {
  const tDashboard = await getTranslations('dashboard');
  const tCommon = await getTranslations('common');
  const tLeague = await getTranslations('league');
  const tStandings = await getTranslations('standings');
  const tTable = await getTranslations('table');
  const allFocusData = await allFocusPromise;

  const featuredLeagueById = new Map(featuredLeagues.map((league) => [league.id, league]));
  const primaryLeagues = DASHBOARD_PRIMARY_LEAGUE_IDS
    .map((leagueId) => featuredLeagueById.get(leagueId))
    .filter((league): league is League => Boolean(league));
  const secondaryLeagues = DASHBOARD_SECONDARY_LEAGUE_IDS
    .map((leagueId) => featuredLeagueById.get(leagueId))
    .filter((league): league is League => Boolean(league));
  const defaultLeagueId = featuredLeagues.some((league) => league.id === DASHBOARD_DEFAULT_LEAGUE_ID)
    ? DASHBOARD_DEFAULT_LEAGUE_ID
    : featuredLeagues[0]?.id ?? initialLeagueId;

  const panels: Record<string, ReactNode> = {};

  for (let i = 0; i < featuredLeagues.length; i++) {
    const league = featuredLeagues[i];
    const data = allFocusData[i];
    const leagueIsTournament = isTournamentCompetition(league);
    const formatValue = leagueIsTournament ? tLeague('formatTournament') : tLeague('formatLeague');

    if (leagueIsTournament) {
      const advancingRule = league.competitionFormat === 'league_phase'
        ? tLeague('advancingRuleLeaguePhase')
        : league.competitionFormat === 'group_knockout'
          ? tLeague('advancingRuleLegacy')
          : league.competitionFormat === 'knockout'
            ? tLeague('advancingRuleKnockoutOnly')
            : tLeague('advancingRule');

      panels[league.id] = (
        <DashboardTournamentFocusPanel
          league={league}
          standings={data.standings}
          topScorers={data.topScorers}
          allMatches={data.tournamentMatches}
          clubs={data.tournamentClubs}
          participantCount={data.tournamentClubs.length}
          viewFullTableLabel={tCommon('viewFullTable')}
          seasonLabel={tLeague('season')}
          formatLabel={tLeague('format')}
          participantsLabel={tLeague('participants')}
          currentStageLabel={tLeague('currentStage')}
          standingsSnapshotLabel={tLeague('standingsSnapshot')}
          groupStageSnapshotLabel={tLeague('groupStageSnapshotLegacy')}
          knockoutBracketLabel={tLeague('knockoutBracket')}
          competitionOverviewTitle={tLeague('competitionOverview')}
          topPerformersTitle={tLeague('topPerformers')}
          tournamentSnapshotPendingLabel={tDashboard('tournamentSnapshotPending')}
          tournamentSnapshotPendingDetail={tDashboard('tournamentSnapshotPendingDetail')}
          rankLabel={tTable('rank')}
          playerLabel={tTable('player')}
          goalsLabel={tTable('goals')}
          formatValue={formatValue}
          currentStageNote={advancingRule}
          locale={locale}
        />
      );
    } else {
      panels[league.id] = (
        <DashboardLeagueFocusPanel
          league={league}
          standings={data.standings}
          topScorers={data.topScorers}
          viewFullTableLabel={tCommon('viewFullTable')}
          seasonLabel={tLeague('season')}
          formatLabel={tLeague('format')}
          participantsLabel={tLeague('participants')}
          standingsTitle={tLeague('standings')}
          overviewTitle={tLeague('competitionOverview')}
          topScorersTitle={tLeague('topScorers')}
          pointsLabel={tStandings('points')}
          rankLabel={tTable('rank')}
          playerLabel={tTable('player')}
          goalsLabel={tTable('goals')}
          formatValue={formatValue}
        />
      );
    }
  }

  return (
    <DashboardCompetitionFocus
      primaryLeagues={primaryLeagues}
      secondaryLeagues={secondaryLeagues}
      initialLeagueId={initialLeagueId}
      defaultLeagueId={defaultLeagueId}
      panels={panels}
    />
  );
}

export default async function DashboardPage({
  searchParams,
}: {
  searchParams: Promise<{ league?: string }>;
}) {
  const locale = await getLocale();
  const { league: requestedLeagueId } = await searchParams;
  const tDashboard = await getTranslations('dashboard');
  const tStats = await getTranslations('stats');

  const [leagueCount, featuredLeagues] = await Promise.all([
    getLeagueCountDb(),
    getLeaguesByIdsDb([...DASHBOARD_LEAGUE_IDS], locale),
  ]);
  const featuredLeagueIds = featuredLeagues.map((league) => league.id);
  const selectedLeague = featuredLeagues.find((league) => league.id === requestedLeagueId)
    ?? featuredLeagues.find((league) => league.id === DASHBOARD_DEFAULT_LEAGUE_ID)
    ?? featuredLeagues[0];

  if (!selectedLeague) {
    return null;
  }

  const globalOverviewPromise = getDashboardGlobalOverviewData(locale, featuredLeagueIds);
  const allCompetitionFocusPromise = Promise.all(
    featuredLeagues.map((league) => getDashboardCompetitionFocusData(league, locale))
  );

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={tDashboard('title')}
        subtitle={tDashboard('subtitle')}
        meta="OVERVIEW"
      >
        <Suspense
          fallback={(
            <DashboardHeaderStatsFallback
              leagueCount={leagueCount}
              leaguesLabel={tStats('leagues')}
              resultsLabel={tStats('results')}
              upcomingLabel={tStats('upcoming')}
            />
          )}
        >
          <DashboardHeaderStats
            leagueCount={leagueCount}
            overviewPromise={globalOverviewPromise}
            leaguesLabel={tStats('leagues')}
            resultsLabel={tStats('results')}
            upcomingLabel={tStats('upcoming')}
          />
        </Suspense>
      </PageHeader>

      <Suspense fallback={<GlobalOverviewSectionFallback title={tDashboard('globalMatchRadar')} />}>
        <GlobalOverviewSection locale={locale} overviewPromise={globalOverviewPromise} />
      </Suspense>

      <Suspense fallback={<CompetitionFocusSectionFallback title={tDashboard('competitionFocus')} />}>
        <CompetitionFocusSection
          locale={locale}
          featuredLeagues={featuredLeagues}
          initialLeagueId={selectedLeague.id}
          allFocusPromise={allCompetitionFocusPromise}
        />
      </Suspense>
    </div>
  );
}
