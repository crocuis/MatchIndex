import { notFound } from 'next/navigation';
import type { Metadata } from 'next';
import { getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { StatPanel } from '@/components/data/StatPanel';
import { MatchCard } from '@/components/data/MatchCard';
import { FixtureCard } from '@/components/data/FixtureCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { formatNumber, getPositionColor, cn } from '@/lib/utils';
import {
  getClubById,
  getClubs,
  getLeagueById,
  getPlayersByClub,
  getMatchesByClub,
  getStandingsByLeague,
} from '@/data';

export async function generateStaticParams() {
  return getClubs().map((c) => ({ id: c.id }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ id: string }>;
}): Promise<Metadata> {
  const { id } = await params;
  const club = getClubById(id);
  return { title: club?.name ?? 'Club' };
}

export default async function ClubPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const club = getClubById(id);
  if (!club) notFound();

  const league = getLeagueById(club.leagueId);
  const squad = getPlayersByClub(id);
  const allMatches = getMatchesByClub(id);
  const recentMatches = allMatches
    .filter((m) => m.status === 'finished')
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())
    .slice(0, 10);
  const upcomingFixtures = allMatches
    .filter((m) => m.status === 'scheduled')
    .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime())
    .slice(0, 5);
  const standings = getStandingsByLeague(club.leagueId);
  const clubStanding = standings.find((s) => s.clubId === id);
  const tClub = await getTranslations('club');
  const tTable = await getTranslations('table');

  const totalGoals = squad.reduce((sum, p) => sum + p.seasonStats.goals, 0);
  const totalAssists = squad.reduce((sum, p) => sum + p.seasonStats.assists, 0);

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <ClubBadge shortName={club.shortName} clubId={club.id} size="lg" />
            <span>{club.name}</span>
          </div>
        )}
        subtitle={`${league?.name ?? ''} · ${club.country}`}
        meta={`Founded ${club.founded} · ${club.stadium} (${formatNumber(club.stadiumCapacity)})`}
      />

      <StatPanel
        stats={[
          { label: tClub('leaguePos'), value: clubStanding ? `#${clubStanding.position}` : '-', highlight: clubStanding?.position === 1 },
          { label: tClub('points'), value: clubStanding?.points ?? '-' },
          { label: tClub('goalsScored'), value: totalGoals },
          { label: tClub('assists'), value: totalAssists },
        ]}
        columns={4}
        className="mb-4"
      />

      <div className="grid grid-cols-12 gap-4">
        {/* Main content */}
        <div className="col-span-8 space-y-4">
          {/* Squad */}
          <SectionCard title={`${tClub('squad')} (${squad.length})`} noPadding>
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="px-3 py-2 text-center w-8">{tTable('rank')}</th>
                  <th className="px-3 py-2 text-left">{tTable('player')}</th>
                  <th className="px-3 py-2 text-center w-16">{tTable('pos')}</th>
                  <th className="px-3 py-2 text-center w-10">{tTable('age')}</th>
                  <th className="px-3 py-2 text-left">{tTable('nationality')}</th>
                  <th className="px-3 py-2 text-center w-10">{tTable('app')}</th>
                  <th className="px-3 py-2 text-center w-10">{tTable('goals')}</th>
                  <th className="px-3 py-2 text-center w-10">{tTable('assists')}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-subtle">
                {squad
                  .sort((a, b) => {
                    const order = { GK: 0, DEF: 1, MID: 2, FWD: 3 };
                    return order[a.position] - order[b.position];
                  })
                  .map((player) => (
                    <tr key={player.id} className="hover:bg-surface-2">
                      <td className="px-3 py-1.5 text-[13px] text-center text-text-muted tabular-nums">
                        {player.shirtNumber}
                      </td>
                      <td className="px-3 py-1.5 text-[13px]">
                        <EntityLink type="player" id={player.id} className="flex items-center gap-2">
                          <PlayerAvatar name={player.name} position={player.position} size="sm" />
                          <span>{player.name}</span>
                        </EntityLink>
                      </td>
                      <td className="px-3 py-1.5 text-center">
                        <span className={cn('px-1.5 py-0.5 rounded text-[10px] font-bold', getPositionColor(player.position))}>
                          {player.position}
                        </span>
                      </td>
                      <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                        {player.age}
                      </td>
                      <td className="px-3 py-1.5 text-[13px] text-text-secondary">
                        {player.nationality}
                      </td>
                      <td className="px-3 py-1.5 text-[13px] text-center tabular-nums">
                        {player.seasonStats.appearances}
                      </td>
                      <td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">
                        {player.seasonStats.goals}
                      </td>
                      <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                        {player.seasonStats.assists}
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </SectionCard>

          {/* League Position Context */}
          <SectionCard title={tClub('leaguePosition')} noPadding>
            <StandingsTable standings={standings} />
          </SectionCard>
        </div>

        {/* Sidebar */}
        <div className="col-span-4 space-y-4">
          {/* Club Info */}
          <SectionCard title={tClub('clubInfo')}>
            <div className="mb-3 flex items-center justify-center">
              <ClubBadge shortName={club.shortName} clubId={club.id} size="lg" />
            </div>
            <dl className="space-y-2">
              {[
                [tClub('fullName'), club.name],
                [tClub('shortName'), club.shortName],
                [tClub('founded'), String(club.founded)],
                [tClub('stadium'), club.stadium],
                [tClub('capacity'), formatNumber(club.stadiumCapacity)],
                [tClub('country'), club.country],
                [tClub('league'), league?.name ?? '-'],
              ].map(([label, value]) => (
                <div key={label} className="flex justify-between">
                  <dt className="text-[12px] text-text-muted">{label}</dt>
                  <dd className="text-[13px] text-text-primary font-medium">{value}</dd>
                </div>
              ))}
            </dl>
          </SectionCard>

          <SectionCard title={tClub('recentMatches')}>
            <div className="space-y-1.5">
              {recentMatches.map((m) => (
                <MatchCard key={m.id} match={m} />
              ))}
            </div>
          </SectionCard>

          <SectionCard title={tClub('upcomingFixtures')}>
            <div className="space-y-1.5">
              {upcomingFixtures.map((m) => (
                <FixtureCard key={m.id} match={m} />
              ))}
            </div>
          </SectionCard>
        </div>
      </div>
    </div>
  );
}
