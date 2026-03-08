import { notFound } from 'next/navigation';
import Link from 'next/link';
import type { Metadata } from 'next';
import { getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { MatchCard } from '@/components/data/MatchCard';
import { FixtureCard } from '@/components/data/FixtureCard';
import { StatPanel } from '@/components/data/StatPanel';
import { EntityLink } from '@/components/ui/EntityLink';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import {
  getLeagueById,
  getStandingsByLeague,
  getClubsByLeague,
  getFinishedMatchesByLeague,
  getScheduledMatchesByLeague,
  getTopScorers,
  getPlayerName,
  getClubShortName,
  getLeagues,
} from '@/data';

export async function generateStaticParams() {
  return getLeagues().map((l) => ({ id: l.id }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ id: string }>;
}): Promise<Metadata> {
  const { id } = await params;
  const league = getLeagueById(id);
  return { title: league?.name ?? 'League' };
}

export default async function LeaguePage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const league = getLeagueById(id);
  if (!league) notFound();

  const standings = getStandingsByLeague(id);
  const clubs = getClubsByLeague(id);
  const recentResults = getFinishedMatchesByLeague(id).slice(0, 10);
  const upcomingFixtures = getScheduledMatchesByLeague(id).slice(0, 10);
  const topScorers = getTopScorers(id, 10);
  const tLeague = await getTranslations('league');
  const tTable = await getTranslations('table');

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <LeagueLogo leagueId={league.id} name={league.name} size="lg" />
            <span>{league.name}</span>
          </div>
        )}
        subtitle={`${league.country} · ${tLeague('season')} ${league.season}`}
        meta={`${league.numberOfClubs} ${tLeague('clubs')}`}
      />

      <StatPanel
        stats={[
          { label: tLeague('country'), value: league.country },
          { label: tLeague('season'), value: league.season },
          { label: tLeague('clubs'), value: league.numberOfClubs },
          { label: tLeague('matchesPlayed'), value: recentResults.length },
        ]}
        columns={4}
        className="mb-4"
      />

      <div className="grid grid-cols-12 gap-4">
        {/* Main — Standings */}
        <div className="col-span-8 space-y-4">
          <SectionCard title={tLeague('standings')} noPadding>
            <StandingsTable standings={standings} />
          </SectionCard>

          {/* Clubs grid */}
          <SectionCard title={tLeague('clubsList')}>
            <div className="grid grid-cols-2 gap-2">
              {clubs.map((club) => (
                <Link
                  key={club.id}
                  href={`/clubs/${club.id}`}
                  className="flex items-center gap-3 px-3 py-2 rounded border border-border-subtle bg-surface-2 hover:bg-surface-3 transition-colors"
                >
                  <ClubBadge shortName={club.shortName} clubId={club.id} size="lg" />
                  <div>
                    <div className="text-[13px] font-medium text-text-primary">{club.name}</div>
                    <div className="text-[11px] text-text-muted">{club.stadium}</div>
                  </div>
                </Link>
              ))}
            </div>
          </SectionCard>
        </div>

        {/* Sidebar — Results, Fixtures, Scorers */}
        <div className="col-span-4 space-y-4">
          <SectionCard title={tLeague('recentResults')}>
            <div className="space-y-1.5">
              {recentResults.map((m) => (
                <MatchCard key={m.id} match={m} />
              ))}
            </div>
          </SectionCard>

          <SectionCard title={tLeague('upcomingFixtures')}>
            <div className="space-y-1.5">
              {upcomingFixtures.map((m) => (
                <FixtureCard key={m.id} match={m} />
              ))}
            </div>
          </SectionCard>

          <SectionCard title={tLeague('topScorers')} noPadding>
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
                {topScorers.map((s, i) => (
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
                    <td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">{s.goals}</td>
                    <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">{s.assists}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </SectionCard>
        </div>
      </div>
    </div>
  );
}
