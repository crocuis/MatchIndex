import { notFound } from 'next/navigation';
import { Suspense } from 'react';
import { getLocale, getTranslations } from 'next-intl/server';
import { ClubSeasonArchiveSections } from '@/app/clubs/[id]/ClubSeasonArchiveSections';
import { ClubSeasonSelect } from '@/app/clubs/[id]/ClubSeasonSelect';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { DetailTabNav } from '@/components/ui/DetailTabNav';
import { StatPanel } from '@/components/data/StatPanel';
import { MatchCard } from '@/components/data/MatchCard';
import { FixtureCard } from '@/components/data/FixtureCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { cn, formatNumber, getClubDisplayName, getPositionColor } from '@/lib/utils';
import {
  getClubByIdDb,
  getClubSeasonHistoryDb,
  getClubSeasonMetaDb,
  getLeagueByIdDb,
  getPlayersByClubAndSeasonDb,
  getPlayersByClubDb,
  getRecentFinishedMatchesByClubAndSeasonDb,
  getUpcomingScheduledMatchesByClubAndSeasonDb,
} from '@/data/server';

export default async function ClubPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ season?: string; competition?: string; tab?: string }>;
}) {
  const { id } = await params;
  const { season, competition, tab } = await searchParams;
  const locale = await getLocale();
  const club = await getClubByIdDb(id, locale);
  if (!club) notFound();

  const [league, currentSquad, seasonHistory] = await Promise.all([
    getLeagueByIdDb(club.leagueId, locale),
    getPlayersByClubDb(id, locale),
    getClubSeasonHistoryDb(id, locale),
  ]);

  const defaultSeason = seasonHistory.find((entry) => entry.played > 0 || entry.position !== undefined) ?? seasonHistory[0];
  const selectedSeason = seasonHistory.find((entry) => {
    if (entry.seasonId !== season) {
      return false;
    }

    return competition ? entry.leagueId === competition : true;
  }) ?? defaultSeason;
  const seasonGroups = Array.from(
    seasonHistory.reduce((map, entry) => {
      const group = map.get(entry.seasonLabel) ?? [];
      group.push(entry);
      map.set(entry.seasonLabel, group);
      return map;
    }, new Map<string, typeof seasonHistory>())
  );
  const visibleSeasonGroups = selectedSeason
    ? seasonGroups.filter(([seasonLabel]) => seasonLabel === selectedSeason.seasonLabel)
    : seasonGroups;

  const [seasonMeta, squad, recentMatches, upcomingFixtures] = selectedSeason
    ? await Promise.all([
        getClubSeasonMetaDb(id, selectedSeason.seasonId, locale),
        getPlayersByClubAndSeasonDb(id, selectedSeason.seasonId, locale),
        getRecentFinishedMatchesByClubAndSeasonDb(id, selectedSeason.seasonId, selectedSeason.leagueId, locale, 30),
        getUpcomingScheduledMatchesByClubAndSeasonDb(id, selectedSeason.seasonId, selectedSeason.leagueId, locale, 20),
      ])
    : [{}, currentSquad, [], []];
  const clubStanding = selectedSeason;
  const tClub = await getTranslations('club');
  const tTable = await getTranslations('table');
  const tLeague = await getTranslations('league');
  const tStandings = await getTranslations('standings');
  const tCommon = await getTranslations('common');
  const detailTabs = [
    { key: 'overview', label: tCommon('tabOverview') },
    { key: 'squad', label: tCommon('tabSquad') },
    { key: 'archive', label: tCommon('tabArchive') },
  ] as const;
  const activeTab = (tab && detailTabs.some((entry) => entry.key === tab) ? tab : 'overview') as 'overview' | 'squad' | 'archive';

  const totalGoals = squad.reduce((sum, p) => sum + p.seasonStats.goals, 0);
  const totalAssists = squad.reduce((sum, p) => sum + p.seasonStats.assists, 0);
  const clubDisplayName = getClubDisplayName(club, locale);

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="lg" />
            <span>{clubDisplayName}</span>
          </div>
        )}
        subtitle={`${selectedSeason?.leagueName ?? league?.name ?? ''} · ${club.country}`}
        meta={`Founded ${club.founded} · ${club.stadium} (${formatNumber(club.stadiumCapacity)})`}
      />

      <StatPanel
        stats={[
          {
            label: tClub('leaguePos'),
            value: clubStanding ? `#${clubStanding.position}` : '-',
            highlight: clubStanding?.position === 1,
          },
          { label: tClub('points'), value: clubStanding?.points ?? '-' },
          { label: tClub('goalsScored'), value: totalGoals },
          { label: tClub('assists'), value: totalAssists },
        ]}
        columns={4}
        className="mb-4"
      />

      <DetailTabNav
        activeTab={activeTab}
        basePath={`/clubs/${id}`}
        className="mb-4"
        query={selectedSeason ? { season: selectedSeason.seasonId, competition: selectedSeason.leagueId } : undefined}
        tabs={detailTabs.map((entry) => ({ ...entry }))}
      />

      <div className="grid grid-cols-12 gap-4">
        {/* Main content */}
        <div className="col-span-8 space-y-4">
          {activeTab === 'archive' ? (
            <SectionCard title={tClub('seasonHistory')} noPadding>
            <div className="border-b border-border-subtle px-3 py-2">
              <div className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-text-muted">
                {tClub('selectSeason')}
              </div>
              <ClubSeasonSelect
                clubId={id}
                selectedValue={selectedSeason ? `/clubs/${id}?season=${selectedSeason.seasonId}&competition=${selectedSeason.leagueId}` : undefined}
                groups={seasonGroups.map(([seasonLabel, entries]) => ({ seasonLabel, entries }))}
              />
            </div>
            <div className="space-y-3 p-3">
              {visibleSeasonGroups.map(([seasonLabel, entries]) => (
                <div key={`table-${seasonLabel}`} className="overflow-hidden rounded border border-border-subtle bg-surface-2">
                  <div className="border-b border-border-subtle px-3 py-2 text-[12px] font-semibold text-text-primary">
                    {seasonLabel}
                  </div>
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-border-subtle">
                        <th className="px-3 py-2 text-left">{tClub('league')}</th>
                        <th className="px-3 py-2 text-center">{tClub('leaguePos')}</th>
                        <th className="px-3 py-2 text-center">{tStandings('played')}</th>
                        <th className="px-3 py-2 text-center">{tStandings('goalDifference')}</th>
                        <th className="px-3 py-2 text-center">{tStandings('points')}</th>
                        <th className="px-3 py-2 text-center">{tStandings('form')}</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border-subtle">
                      {entries.map((entry) => {
                        const isActive = entry.seasonId === selectedSeason?.seasonId && entry.leagueId === selectedSeason?.leagueId;

                        return (
                          <tr key={`${entry.seasonId}:${entry.leagueId}`} className={cn('hover:bg-surface-1', isActive && 'bg-surface-1')}>
                            <td className="px-3 py-2 text-[13px] text-text-secondary">{entry.leagueName}</td>
                            <td className="px-3 py-2 text-center text-[13px] tabular-nums font-semibold">
                              {entry.position ? `#${entry.position}` : '-'}
                            </td>
                            <td className="px-3 py-2 text-center text-[13px] tabular-nums">{entry.played}</td>
                            <td className={cn(
                              'px-3 py-2 text-center text-[13px] tabular-nums font-medium',
                              entry.goalDifference > 0 ? 'text-emerald-400' : entry.goalDifference < 0 ? 'text-red-400' : 'text-text-secondary'
                            )}>
                              {entry.goalDifference > 0 ? `+${entry.goalDifference}` : entry.goalDifference}
                            </td>
                            <td className="px-3 py-2 text-center text-[13px] tabular-nums font-semibold">{entry.points}</td>
                            <td className="px-3 py-2">
                              <div className="flex justify-center gap-1">
                                {entry.form.map((value, index) => (
                                  <span
                                    key={`${entry.seasonId}:${index}`}
                                    className={cn(
                                      'inline-flex h-5 w-5 items-center justify-center rounded text-[10px] font-bold',
                                      value === 'W' && 'bg-emerald-500/15 text-emerald-400',
                                      value === 'D' && 'bg-amber-500/15 text-amber-300',
                                      value === 'L' && 'bg-red-500/15 text-red-400',
                                    )}
                                  >
                                    {value}
                                  </span>
                                ))}
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ))}
            </div>
            </SectionCard>
          ) : null}

          {activeTab === 'archive' && selectedSeason ? (
            <Suspense
              fallback={
                <SectionCard title={`${tClub('seasonStandings')} · ${selectedSeason.seasonLabel}`}>
                  <div className="py-8 text-center text-[13px] text-text-muted">{tClub('selectSeason')}</div>
                </SectionCard>
              }
            >
              <ClubSeasonArchiveSections
                clubId={id}
                leagueId={selectedSeason.leagueId}
                seasonId={selectedSeason.seasonId}
                seasonLabel={selectedSeason.seasonLabel}
                locale={locale}
              />
            </Suspense>
          ) : null}

          {activeTab === 'squad' ? (
            <SectionCard title={`${tClub('squad')} · ${selectedSeason?.seasonLabel ?? league?.season ?? '-'} (${squad.length})`} noPadding>
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
                          <PlayerAvatar name={player.name} position={player.position} imageUrl={player.photoUrl} size="sm" />
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
          ) : null}

          {activeTab === 'overview' ? (
            <>
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
            </>
          ) : null}

        </div>

        {/* Sidebar */}
        <div className="col-span-4 space-y-4">
          {activeTab === 'overview' ? (
            <SectionCard title={tClub('clubInfo')}>
            <div className="mb-3 flex items-center justify-center">
                <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="lg" />
            </div>
            <dl className="space-y-2">
              {[
                [tLeague('season'), selectedSeason?.seasonLabel ?? league?.season ?? '-'],
                [tClub('fullName'), clubDisplayName],
                [tClub('shortName'), club.shortName],
                [tClub('founded'), String(club.founded)],
                [tClub('stadium'), club.stadium],
                [tClub('capacity'), formatNumber(club.stadiumCapacity)],
                [tClub('country'), club.country],
                [tClub('league'), selectedSeason?.leagueName ?? league?.name ?? '-'],
                ...(seasonMeta.coachName ? [[tClub('coach'), seasonMeta.coachName] as const] : []),
              ].map(([label, value]) => (
                <div key={label} className="flex justify-between">
                  <dt className="text-[12px] text-text-muted">{label}</dt>
                  <dd className="text-[13px] text-text-primary font-medium">{value}</dd>
                </div>
                ))}
              </dl>
            </SectionCard>
          ) : null}
        </div>
      </div>
    </div>
  );
}
