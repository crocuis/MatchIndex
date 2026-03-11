import Link from 'next/link';
import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { MatchCard } from '@/components/data/MatchCard';
import { StatPanel } from '@/components/data/StatPanel';
import { EntityLink } from '@/components/ui/EntityLink';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { TabGroup } from '@/components/ui/TabGroup';
import {
  getLeaguesDb,
  getRecentFinishedMatchesByLeagueIdsDb,
  getStandingsByLeagueIdsDb,
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
  'uefa-europa-league',
] as const;

export default async function DashboardPage() {
  const locale = await getLocale();
  const tDashboard = await getTranslations('dashboard');
  const tCommon = await getTranslations('common');
  const tTable = await getTranslations('table');
  const tStats = await getTranslations('stats');

  const leagues = await getLeaguesDb(locale);
  const featuredLeagues = DASHBOARD_LEAGUE_IDS
    .map((leagueId) => leagues.find((league) => league.id === leagueId))
    .filter((league): league is NonNullable<typeof league> => Boolean(league));
  const featuredLeagueIds = featuredLeagues.map((league) => league.id);

  const [recentResults, upcomingFixtures, ...topScorersByLeague] = await Promise.all([
    getRecentFinishedMatchesByLeagueIdsDb(featuredLeagueIds, locale, 6),
    getUpcomingScheduledMatchesByLeagueIdsDb(featuredLeagueIds, locale, 6),
    ...featuredLeagues.map((league) => getTopScorerRowsDb(league.id, locale, 5)),
  ]);

  const standingsByLeague = await getStandingsByLeagueIdsDb(featuredLeagueIds, locale);

  const tabs = featuredLeagues.map((league, index) => {
    const topScorers = topScorersByLeague[index];
    const standings = standingsByLeague[league.id] ?? [];

    return {
      key: league.id,
      label: (
        <div className="flex items-center gap-2">
          <LeagueLogo leagueId={league.id} name={league.name} logo={league.logo} size="sm" />
          <span>{league.name}</span>
        </div>
      ),
      content: (
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 md:gap-6">
          <div className="lg:col-span-8">
            <SectionCard
              title={league.name}
              action={
                <Link
                  href={`/leagues/${league.id}`}
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
            <SectionCard title={league.name} noPadding>
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
                          <span className="text-[10px] text-text-secondary uppercase tracking-wider mt-0.5">{s.clubShortName}</span>
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
      ),
    };
  });

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={tDashboard('title')}
        subtitle={tDashboard('subtitle')}
        meta="OVERVIEW"
      >
        <StatPanel
          stats={[
            { label: tStats('leagues'), value: leagues.length },
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
          <div className="flex flex-col gap-1.5">
            {upcomingFixtures.map((match) => (
              <MatchCard key={match.id} match={match} />
            ))}
          </div>
        </SectionCard>
      </div>

      <TabGroup tabs={tabs} />
    </div>
  );
}
