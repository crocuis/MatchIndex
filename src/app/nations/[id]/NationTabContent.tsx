import { getTranslations } from 'next-intl/server';
import { FixtureCard } from '@/components/data/FixtureCard';
import { MatchCard } from '@/components/data/MatchCard';
import { SectionCard } from '@/components/ui/SectionCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { MatchSectionTitle, renderMatchSectionDateLabel } from '@/components/ui/MatchSectionTitle';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import type { Nation } from '@/data/types';
import { getClubsByIdsDb, getMatchesByNationDb, getPlayersByNationDb } from '@/data/server';
import { isUpcomingMatchStatus } from '@/lib/matchStatus';
import { cn, getClubDisplayName, getPositionColor } from '@/lib/utils';

interface NationTabContentProps {
  nation: Nation;
  activeTab: 'overview' | 'squad' | 'matches';
  locale: string;
}

export async function NationTabContent({ nation, activeTab, locale }: NationTabContentProps) {
  const [nationalPlayers, nationMatches, tNation, tTable, tCommon] = await Promise.all([
    activeTab === 'squad' ? getPlayersByNationDb(nation.id, locale) : Promise.resolve([]),
    activeTab === 'matches' ? getMatchesByNationDb(nation.id, locale) : Promise.resolve([]),
    getTranslations('nation'),
    getTranslations('table'),
    getTranslations('common'),
  ]);
  const recentMatches = nationMatches.filter((match) => !isUpcomingMatchStatus(match.status));
  const upcomingFixtures = nationMatches.filter((match) => isUpcomingMatchStatus(match.status));
  const clubs = activeTab === 'squad'
    ? await getClubsByIdsDb(nationalPlayers.map((player) => player.clubId), locale)
    : [];
  const clubMap = new Map(clubs.map((club) => [club.id, club]));
  const recentMatchesDateLabel = renderMatchSectionDateLabel(recentMatches[0], locale);
  const upcomingFixturesDateLabel = renderMatchSectionDateLabel(upcomingFixtures[0], locale);

  return (
    <div className="grid grid-cols-12 gap-4">
      <div className="col-span-8 space-y-4">
        {activeTab === 'overview' ? (
          <SectionCard title={tNation('recentTournaments')} noPadding>
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="px-3 py-2 text-left">{tNation('competition')}</th>
                  <th className="px-3 py-2 text-center w-20">{tNation('year')}</th>
                  <th className="px-3 py-2 text-left w-36">{tNation('result')}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-subtle">
                {(nation.recentTournaments ?? []).map((entry) => (
                  <tr key={`${entry.competition}-${entry.year}`} className="hover:bg-surface-2">
                    <td className="px-3 py-2 text-[13px] font-medium text-text-primary">{entry.competition}</td>
                    <td className="px-3 py-2 text-[13px] text-center tabular-nums text-text-secondary">{entry.year}</td>
                    <td className="px-3 py-2 text-[13px] text-text-secondary">{entry.result}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </SectionCard>
        ) : null}

        {activeTab === 'squad' ? (
          <SectionCard title={`${tNation('players')} (${nationalPlayers.length})`} noPadding>
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="px-3 py-2 text-left">{tTable('player')}</th>
                  <th className="px-3 py-2 text-center w-16">{tTable('pos')}</th>
                  <th className="px-3 py-2 text-center w-10">{tTable('age')}</th>
                  <th className="px-3 py-2 text-left">{tTable('club')}</th>
                  <th className="px-3 py-2 text-center w-10">{tTable('app')}</th>
                  <th className="px-3 py-2 text-center w-10">{tTable('goals')}</th>
                  <th className="px-3 py-2 text-center w-10">{tTable('assists')}</th>
                  <th className="px-3 py-2 text-center w-14">{tTable('mins')}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-subtle">
                {nationalPlayers
                  .slice()
                  .sort((a, b) => {
                    const order = { GK: 0, DEF: 1, MID: 2, FWD: 3 };
                    return order[a.position] - order[b.position];
                  })
                  .map((player) => {
                    const club = clubMap.get(player.clubId);

                    return (
                      <tr key={player.id} className="hover:bg-surface-2">
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
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">{player.age}</td>
                        <td className="px-3 py-1.5 text-[13px]">
                          {club ? (
                            <EntityLink type="club" id={club.id} className="flex items-center gap-2 text-text-secondary">
                              <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="sm" />
                              <span>{getClubDisplayName(club, locale)}</span>
                            </EntityLink>
                          ) : (
                            <span className="text-text-muted">{tCommon('freeAgent')}</span>
                          )}
                        </td>
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums">{player.seasonStats.appearances}</td>
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">{player.seasonStats.goals}</td>
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">{player.seasonStats.assists}</td>
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-muted">{player.seasonStats.minutesPlayed}</td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </SectionCard>
        ) : null}
      </div>

      <div className="col-span-4 space-y-4">
        {activeTab === 'matches' ? (
          <SectionCard title={<MatchSectionTitle title={tNation('recentMatches')} count={recentMatches.length} dateLabel={recentMatchesDateLabel} variant="results" />}>
            <div className="space-y-1.5">
              {recentMatches.length > 0 ? recentMatches.map((match) => <MatchCard key={match.id} match={match} />) : (
                <div className="text-[13px] text-text-muted">{tCommon('unknown')}</div>
              )}
            </div>
          </SectionCard>
        ) : null}

        {activeTab === 'matches' ? (
          <SectionCard title={<MatchSectionTitle title={tNation('upcomingFixtures')} count={upcomingFixtures.length} dateLabel={upcomingFixturesDateLabel} variant="fixtures" />}>
            <div className="space-y-1.5">
              {upcomingFixtures.length > 0 ? upcomingFixtures.map((match) => <FixtureCard key={match.id} match={match} />) : (
                <div className="text-[13px] text-text-muted">{tCommon('unknown')}</div>
              )}
            </div>
          </SectionCard>
        ) : null}
      </div>
    </div>
  );
}
