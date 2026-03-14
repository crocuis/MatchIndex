import Link from 'next/link';
import { getLocale, getTranslations } from 'next-intl/server';
import { FixtureCard } from '@/components/data/FixtureCard';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { MatchCard } from '@/components/data/MatchCard';
import { StatPanel } from '@/components/data/StatPanel';
import { EntityLink } from '@/components/ui/EntityLink';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { cn } from '@/lib/utils';
import { isTournamentCompetition } from '@/data/competitionTypes';
import type { League, Match, StandingRow } from '@/data/types';
import {
  getDashboardTournamentSummaryDb,
  getLeagueCountDb,
  getLeaguesByIdsDb,
  getRecentFinishedMatchesByLeagueIdsDb,
  getStandingsByLeagueDb,
  getTopScorerRowsDb,
  getUpcomingScheduledMatchesByLeagueIdsDb,
} from '@/data/server';

const DASHBOARD_LEAGUE_IDS = [
  'la-liga',
  'premier-league',
  '1-bundesliga',
  'serie-a',
  'ligue-1',
  'champions-league',
  'europa-league',
] as const;

const DASHBOARD_UPCOMING_FIXTURE_LIMIT = 72;
const DASHBOARD_UPCOMING_DATE_TAB_LIMIT = 8;

function formatDashboardFixtureDateLabel(date: string, locale: string) {
  return new Intl.DateTimeFormat(locale === 'ko' ? 'ko-KR' : 'en-US', {
    month: 'short',
    day: 'numeric',
    weekday: 'short',
  }).format(new Date(`${date}T00:00:00Z`));
}

function buildDashboardFixtureDateGroups(matches: Match[], locale: string) {
  const grouped = new Map<string, Match[]>();

  for (const match of matches) {
    const existing = grouped.get(match.date) ?? [];
    existing.push(match);
    grouped.set(match.date, existing);
  }

  return Array.from(grouped.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([date, dateMatches]) => ({
      date,
      label: formatDashboardFixtureDateLabel(date, locale),
      matches: dateMatches,
    }));
}

function buildDashboardHref({
  fixtureDate,
  leagueId,
}: {
  fixtureDate?: string;
  leagueId?: string;
}, defaults: {
  fixtureDate?: string;
  leagueId: string;
}) {
  const params = new URLSearchParams();

  if (leagueId && leagueId !== defaults.leagueId) {
    params.set('league', leagueId);
  }

  if (fixtureDate && fixtureDate !== defaults.fixtureDate) {
    params.set('fixtureDate', fixtureDate);
  }

  const query = params.toString();
  return query ? `/?${query}` : '/';
}

interface DashboardTournamentPanelProps {
  league: League;
  standings: StandingRow[];
  recentResults: Match[];
  upcomingFixtures: Match[];
  stageTrail: string[];
  viewFullTableLabel: string;
  seasonLabel: string;
  formatLabel: string;
  participantsLabel: string;
  stageMatchesLabel: string;
  recentResultsLabel: string;
  upcomingFixturesLabel: string;
}

function DashboardTournamentPanel({
  league,
  standings,
  recentResults,
  upcomingFixtures,
  stageTrail,
  viewFullTableLabel,
  seasonLabel,
  formatLabel,
  participantsLabel,
  stageMatchesLabel,
  recentResultsLabel,
  upcomingFixturesLabel,
}: DashboardTournamentPanelProps) {
  const participantCount = standings.length || league.numberOfClubs;
  const snapshotRows = standings.slice(0, 8);
  const snapshotTitle = standings.length > 0 ? 'League Phase Snapshot' : stageMatchesLabel;

  return (
    <div className="grid grid-cols-1 gap-4 md:gap-6 lg:grid-cols-12">
      <div className="lg:col-span-8">
        <SectionCard
          title={snapshotTitle}
          action={
            <Link
              href={`/leagues/${league.id}`}
              className="text-[10px] font-bold uppercase tracking-wider text-accent-magenta transition-colors hover:text-accent-violet"
            >
              {viewFullTableLabel}
            </Link>
          }
          noPadding
        >
          <div className="border-b border-border-subtle bg-surface-0/60 px-4 py-3">
            <div className="grid grid-cols-3 gap-3 text-[11px]">
              <div>
                <div className="mb-1 font-semibold uppercase tracking-wider text-text-muted">{seasonLabel}</div>
                <div className="text-[13px] font-medium text-text-primary">{league.season}</div>
              </div>
              <div>
                <div className="mb-1 font-semibold uppercase tracking-wider text-text-muted">{formatLabel}</div>
                <div className="text-[13px] font-medium text-text-primary">{snapshotTitle}</div>
              </div>
              <div>
                <div className="mb-1 font-semibold uppercase tracking-wider text-text-muted">{participantsLabel}</div>
                <div className="text-[13px] font-medium tabular-nums text-text-primary">{participantCount}</div>
              </div>
            </div>
            {stageTrail.length > 0 ? (
              <div className="mt-3 flex flex-wrap gap-1.5">
                {stageTrail.map((stage) => (
                  <span
                    key={stage}
                    className="rounded-full border border-border-subtle bg-surface-1 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-text-secondary"
                  >
                    {stage}
                  </span>
                ))}
              </div>
            ) : null}
          </div>
          <StandingsTable standings={snapshotRows} compact />
        </SectionCard>
      </div>

      <div className="space-y-4 lg:col-span-4">
        <SectionCard title={stageMatchesLabel}>
          <div className="space-y-3">
            {recentResults.length > 0 ? (
              <div>
                <div className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{recentResultsLabel}</div>
                <div className="space-y-1.5">
                  {recentResults.map((match) => <MatchCard key={match.id} match={match} />)}
                </div>
              </div>
            ) : null}
            {upcomingFixtures.length > 0 ? (
              <div>
                <div className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{upcomingFixturesLabel}</div>
                <div className="space-y-1.5">
                  {upcomingFixtures.map((match) => <FixtureCard key={match.id} match={match} />)}
                </div>
              </div>
            ) : null}
          </div>
        </SectionCard>
      </div>
    </div>
  );
}

export default async function DashboardPage({
  searchParams,
}: {
  searchParams: Promise<{ fixtureDate?: string; league?: string }>;
}) {
  const locale = await getLocale();
  const { fixtureDate, league: requestedLeagueId } = await searchParams;
  const tDashboard = await getTranslations('dashboard');
  const tCommon = await getTranslations('common');
  const tLeague = await getTranslations('league');
  const tTable = await getTranslations('table');
  const tStats = await getTranslations('stats');

  const [leagueCount, featuredLeagues] = await Promise.all([
    getLeagueCountDb(),
    getLeaguesByIdsDb([...DASHBOARD_LEAGUE_IDS], locale),
  ]);
  const featuredLeagueIds = featuredLeagues.map((league) => league.id);
  const selectedLeague = featuredLeagues.find((league) => league.id === requestedLeagueId) ?? featuredLeagues[0];

  if (!selectedLeague) {
    return null;
  }

  const selectedLeagueIsTournament = isTournamentCompetition(selectedLeague);

  const [recentResults, upcomingFixtures, standings, topScorers, tournamentSummary] = await Promise.all([
    getRecentFinishedMatchesByLeagueIdsDb(featuredLeagueIds, locale, 6),
    getUpcomingScheduledMatchesByLeagueIdsDb(featuredLeagueIds, locale, DASHBOARD_UPCOMING_FIXTURE_LIMIT),
    getStandingsByLeagueDb(selectedLeague.id, locale),
    getTopScorerRowsDb(selectedLeague.id, locale, 5),
    selectedLeagueIsTournament
      ? getDashboardTournamentSummaryDb(selectedLeague.id, locale)
      : Promise.resolve({ recentResults: [], upcomingFixtures: [], stageTrail: [] }),
  ]);
  const upcomingFixtureDateGroups = buildDashboardFixtureDateGroups(upcomingFixtures, locale);
  const availableFixtureDateGroups = upcomingFixtureDateGroups.slice(0, DASHBOARD_UPCOMING_DATE_TAB_LIMIT);
  const selectedFixtureDate = availableFixtureDateGroups.some((group) => group.date === fixtureDate)
    ? fixtureDate
    : availableFixtureDateGroups[0]?.date;
  const selectedFixtureGroup = availableFixtureDateGroups.find((group) => group.date === selectedFixtureDate);
  const defaultLeagueId = featuredLeagues[0]?.id ?? selectedLeague.id;
  const defaultFixtureDate = availableFixtureDateGroups[0]?.date;

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={tDashboard('title')}
        subtitle={tDashboard('subtitle')}
        meta="OVERVIEW"
      >
        <StatPanel
          stats={[
            { label: tStats('leagues'), value: leagueCount },
            { label: tStats('results'), value: recentResults.length },
            { label: tStats('upcoming'), value: upcomingFixtures.length },
          ]}
          columns={3}
          className="w-64"
        />
      </PageHeader>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
        <SectionCard title={tDashboard('recentResults')}>
          <div className="flex flex-col gap-1.5">
            {recentResults.map((match) => (
              <MatchCard key={match.id} match={match} />
            ))}
          </div>
        </SectionCard>

        <SectionCard title={tDashboard('upcomingFixtures')}>
          {selectedFixtureGroup ? (
            <div className="space-y-3">
              {availableFixtureDateGroups.length > 1 ? (
                <div className="flex flex-wrap gap-2">
                  {availableFixtureDateGroups.map((group) => {
                    const isActive = group.date === selectedFixtureDate;

                    return (
                      <Link
                        key={group.date}
                        href={buildDashboardHref({ fixtureDate: group.date, leagueId: selectedLeague.id }, {
                          fixtureDate: defaultFixtureDate,
                          leagueId: defaultLeagueId,
                        })}
                        className={cn(
                          'rounded border px-2.5 py-1 text-[11px] font-medium transition-colors',
                          isActive
                            ? 'border-accent-emerald bg-accent-emerald/10 text-accent-emerald'
                            : 'border-border-subtle bg-surface-2 text-text-secondary hover:border-border hover:text-text-primary'
                        )}
                      >
                        {group.label}
                      </Link>
                    );
                  })}
                </div>
              ) : null}

              <div className="flex flex-col gap-1.5">
                {selectedFixtureGroup.matches.map((match) => (
                  <MatchCard
                    key={match.id}
                    match={match}
                    showDate={false}
                    showLeague
                    leagueName={match.competitionName}
                  />
                ))}
              </div>
            </div>
          ) : (
            <div className="text-[13px] text-text-muted">{tDashboard('noUpcomingFixtures')}</div>
          )}
        </SectionCard>
      </div>

      <div>
        <div className="-mx-1 mb-4 overflow-x-auto border-b border-border px-1">
          <div className="flex min-w-max gap-1">
            {featuredLeagues.map((league) => {
              const isActive = league.id === selectedLeague.id;

              return (
                <Link
                  key={league.id}
                  href={buildDashboardHref({ fixtureDate: selectedFixtureDate, leagueId: league.id }, {
                    fixtureDate: defaultFixtureDate,
                    leagueId: defaultLeagueId,
                  })}
                  className={cn(
                    'whitespace-nowrap rounded-t-md px-3 py-2 text-[13px] font-medium transition-colors border-b-2 -mb-px flex items-center gap-2',
                    isActive
                      ? 'text-text-primary border-accent-emerald bg-surface-2/40'
                      : 'text-text-muted border-transparent hover:text-text-secondary hover:bg-surface-2/20'
                  )}
                >
                          <LeagueLogo leagueId={league.id} name={league.name} competitionType={league.competitionType} logo={league.logo} size="sm" />
                  <span>{league.name}</span>
                </Link>
              );
            })}
          </div>
        </div>

        {selectedLeagueIsTournament ? (
          <DashboardTournamentPanel
            league={selectedLeague}
            standings={standings}
            recentResults={tournamentSummary.recentResults}
            upcomingFixtures={tournamentSummary.upcomingFixtures}
            stageTrail={tournamentSummary.stageTrail}
            viewFullTableLabel={tCommon('viewFullTable')}
            seasonLabel={tLeague('season')}
            formatLabel={tLeague('format')}
            participantsLabel={tLeague('participants')}
            stageMatchesLabel={tLeague('stageMatches')}
            recentResultsLabel={tLeague('recentResults')}
            upcomingFixturesLabel={tLeague('upcomingFixtures')}
          />
        ) : (
          <div className="grid grid-cols-1 gap-4 md:gap-6 lg:grid-cols-12">
            <div className="lg:col-span-8">
              <SectionCard
                title={selectedLeague.name}
                action={
                  <Link
                    href={`/leagues/${selectedLeague.id}`}
                    className="text-[10px] uppercase tracking-wider font-bold text-accent-magenta hover:text-accent-violet transition-colors"
                  >
                    {tCommon('viewFullTable')}
                  </Link>
                }
                noPadding
              >
                <StandingsTable standings={standings} compact />
              </SectionCard>
            </div>
            <div className="lg:col-span-4">
              <SectionCard title={selectedLeague.name} noPadding>
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-border bg-surface-2/30">
                      <th className="px-3 py-2 text-left w-8">{tTable('rank')}</th>
                      <th className="px-3 py-2 text-left">{tTable('player')}</th>
                      <th className="px-3 py-2 text-center w-12">{tTable('goals')}</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border-subtle">
                    {topScorers.map((s, i) => (
                      <tr key={s.playerId} className="hover:bg-surface-2 transition-colors group">
                        <td className="px-3 py-2 text-[12px] text-text-muted tabular-nums font-mono">{i + 1}</td>
                        <td className="px-3 py-2 text-[13px]">
                          <div className="flex flex-col">
                            <EntityLink type="player" id={s.playerId} className="font-medium group-hover:text-accent-magenta transition-colors">
                              {s.playerName}
                            </EntityLink>
                            <span className="mt-0.5 text-[10px] text-text-secondary uppercase tracking-wider">{s.clubShortName}</span>
                          </div>
                        </td>
                        <td className="px-3 py-2 text-[13px] text-center tabular-nums font-bold text-accent-magenta">
                          {s.goals}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </SectionCard>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
