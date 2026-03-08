import Link from 'next/link';
import { getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { MatchCard } from '@/components/data/MatchCard';
import { StatPanel } from '@/components/data/StatPanel';
import { EntityLink } from '@/components/ui/EntityLink';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import {
  getLeagues,
  getStandingsByLeague,
  getFinishedMatches,
  getScheduledMatches,
  getTopScorers,
  getPlayerName,
  getClubShortName,
} from '@/data';

export default async function DashboardPage() {
  const tDashboard = await getTranslations('dashboard');
  const tCommon = await getTranslations('common');
  const tTable = await getTranslations('table');
  const tStats = await getTranslations('stats');

  const leagues = getLeagues();
  const recentResults = getFinishedMatches().slice(0, 6);
  const upcomingFixtures = getScheduledMatches().slice(0, 4);
  const plScorers = getTopScorers('pl', 5);
  const laligaScorers = getTopScorers('laliga', 5);

  return (
    <div>
      <PageHeader
        title={tDashboard('title')}
        subtitle={tDashboard('subtitle')}
      />

      <div className="grid grid-cols-12 gap-4">
        {/* Left column — Standings */}
        <div className="col-span-8 space-y-4">
          {leagues.map((league) => (
            <SectionCard
              key={league.id}
              title={(
                <div className="flex items-center gap-2">
                  <LeagueLogo leagueId={league.id} name={league.name} size="sm" />
                  <span>{league.name}</span>
                </div>
              )}
              action={
                <Link
                  href={`/leagues/${league.id}`}
                  className="text-[11px] text-accent-emerald hover:underline"
                >
                  {tCommon('viewFullTable')}
                </Link>
              }
              noPadding
            >
              <StandingsTable standings={getStandingsByLeague(league.id)} compact />
            </SectionCard>
          ))}
        </div>

        {/* Right column — Results, Fixtures, Top Scorers */}
        <div className="col-span-4 space-y-4">
          <SectionCard title={tDashboard('recentResults')}>
            <div className="space-y-1.5">
              {recentResults.map((match) => (
                <MatchCard key={match.id} match={match} />
              ))}
            </div>
          </SectionCard>

          <SectionCard title={tDashboard('upcomingFixtures')}>
            <div className="space-y-1.5">
              {upcomingFixtures.map((match) => (
                <MatchCard key={match.id} match={match} />
              ))}
            </div>
          </SectionCard>

          <SectionCard title={tDashboard('topScorersPL')} noPadding>
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="px-3 py-1.5 text-left">{tTable('rank')}</th>
                  <th className="px-3 py-1.5 text-left">{tTable('player')}</th>
                  <th className="px-3 py-1.5 text-center">{tTable('club')}</th>
                  <th className="px-3 py-1.5 text-center">{tTable('goals')}</th>
                  <th className="px-3 py-1.5 text-center">{tTable('assists')}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-subtle">
                {plScorers.map((s, i) => (
                  <tr key={s.playerId} className="hover:bg-surface-2">
                    <td className="px-3 py-1.5 text-[13px] text-text-muted tabular-nums">{i + 1}</td>
                    <td className="px-3 py-1.5 text-[13px]">
                      <EntityLink type="player" id={s.playerId}>
                        {getPlayerName(s.playerId)}
                      </EntityLink>
                    </td>
                    <td className="px-3 py-1.5 text-[13px] text-center text-text-secondary">
                      <div className="flex items-center justify-center gap-2">
                        <ClubBadge shortName={getClubShortName(s.clubId)} clubId={s.clubId} size="sm" />
                        <span>{getClubShortName(s.clubId)}</span>
                      </div>
                    </td>
                    <td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">
                      {s.goals}
                    </td>
                    <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                      {s.assists}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </SectionCard>

          <SectionCard title={tDashboard('topScorersLL')} noPadding>
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="px-3 py-1.5 text-left">{tTable('rank')}</th>
                  <th className="px-3 py-1.5 text-left">{tTable('player')}</th>
                  <th className="px-3 py-1.5 text-center">{tTable('club')}</th>
                  <th className="px-3 py-1.5 text-center">{tTable('goals')}</th>
                  <th className="px-3 py-1.5 text-center">{tTable('assists')}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-subtle">
                {laligaScorers.map((s, i) => (
                  <tr key={s.playerId} className="hover:bg-surface-2">
                    <td className="px-3 py-1.5 text-[13px] text-text-muted tabular-nums">{i + 1}</td>
                    <td className="px-3 py-1.5 text-[13px]">
                      <EntityLink type="player" id={s.playerId}>
                        {getPlayerName(s.playerId)}
                      </EntityLink>
                    </td>
                    <td className="px-3 py-1.5 text-[13px] text-center text-text-secondary">
                      <div className="flex items-center justify-center gap-2">
                        <ClubBadge shortName={getClubShortName(s.clubId)} clubId={s.clubId} size="sm" />
                        <span>{getClubShortName(s.clubId)}</span>
                      </div>
                    </td>
                    <td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">
                      {s.goals}
                    </td>
                    <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                      {s.assists}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </SectionCard>

          {/* Quick Stats */}
          <StatPanel
              stats={[
              { label: tStats('leagues'), value: leagues.length },
              { label: tStats('results'), value: recentResults.length },
              { label: tStats('upcoming'), value: upcomingFixtures.length },
            ]}
          />
        </div>
      </div>
    </div>
  );
}
