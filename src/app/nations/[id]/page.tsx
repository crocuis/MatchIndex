import { notFound } from 'next/navigation';
import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { FixtureCard } from '@/components/data/FixtureCard';
import { MatchCard } from '@/components/data/MatchCard';
import { SectionCard } from '@/components/ui/SectionCard';
import { StatPanel } from '@/components/data/StatPanel';
import { DetailTabNav } from '@/components/ui/DetailTabNav';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationBadge } from '@/components/ui/NationBadge';
import { NationFlag } from '@/components/ui/NationFlag';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { cn, getClubDisplayName, getPositionColor } from '@/lib/utils';
import {
  getClubsByIdsDb,
  getFinishedMatchesByNationDb,
  getNationByIdDb,
  getPlayersByNationDb,
  getScheduledMatchesByNationDb,
} from '@/data/server';

function getContinentLabel(confederation: string, locale: string) {
  const labels = locale === 'ko'
    ? {
        UEFA: '유럽',
        CONMEBOL: '남미',
        CONCACAF: '북중미',
        AFC: '아시아',
        CAF: '아프리카',
        OFC: '오세아니아',
      }
    : {
        UEFA: 'Europe',
        CONMEBOL: 'South America',
        CONCACAF: 'North & Central America',
        AFC: 'Asia',
        CAF: 'Africa',
        OFC: 'Oceania',
      };

  return labels[confederation as keyof typeof labels] ?? confederation;
}

function formatRankingChange(change?: number) {
  if (change === undefined || change === 0) {
    return '-';
  }

  return change > 0 ? `▲${change}` : `▼${Math.abs(change)}`;
}

export default async function NationPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ tab?: string }>;
}) {
  const { id } = await params;
  const { tab } = await searchParams;
  const locale = await getLocale();
  const nation = await getNationByIdDb(id, locale);
  if (!nation) notFound();

  const [nationalPlayers, recentMatches, upcomingFixtures] = await Promise.all([
    getPlayersByNationDb(id, locale),
    getFinishedMatchesByNationDb(id, locale),
    getScheduledMatchesByNationDb(id, locale),
  ]);
  const clubs = await getClubsByIdsDb(nationalPlayers.map((player) => player.clubId), locale);
  const clubMap = new Map(clubs.map((club) => [club.id, club]));
  const tNation = await getTranslations('nation');
  const tTable = await getTranslations('table');
  const tCommon = await getTranslations('common');
  const detailTabs = [
    { key: 'overview', label: tCommon('tabOverview') },
    { key: 'squad', label: tCommon('tabSquad') },
    { key: 'matches', label: tCommon('tabMatches') },
  ] as const;
  const activeTab = (tab && detailTabs.some((entry) => entry.key === tab) ? tab : 'overview') as 'overview' | 'squad' | 'matches';

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <NationBadge nationId={nation.id} code={nation.code} crest={nation.crest} size="lg" />
            <NationFlag nationId={nation.id} code={nation.code} size="lg" />
            <span>{nation.name}</span>
          </div>
        )}
        subtitle={`${getContinentLabel(nation.confederation, locale)} · FIFA Ranking #${nation.fifaRanking}`}
      />

      <StatPanel
        stats={[
          { label: tNation('fifaRanking'), value: `#${nation.fifaRanking}`, highlight: nation.fifaRanking <= 5 },
          {
            label: tNation('rankChange'),
            value: formatRankingChange(nation.rankingChange),
            highlight: (nation.rankingChange ?? 0) > 0,
          },
          { label: tNation('continent'), value: getContinentLabel(nation.confederation, locale) },
          { label: tNation('players'), value: nationalPlayers.length },
        ]}
        columns={4}
        className="mb-4"
      />

      <DetailTabNav
        activeTab={activeTab}
        basePath={`/nations/${id}`}
        className="mb-4"
        tabs={detailTabs.map((entry) => ({ ...entry }))}
      />

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
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                          {player.age}
                        </td>
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
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums">
                          {player.seasonStats.appearances}
                        </td>
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">
                          {player.seasonStats.goals}
                        </td>
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                          {player.seasonStats.assists}
                        </td>
                        <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-muted">
                          {player.seasonStats.minutesPlayed}
                        </td>
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
            <SectionCard title={tNation('recentMatches')}>
            <div className="space-y-1.5">
              {recentMatches.length > 0 ? recentMatches.map((match) => <MatchCard key={match.id} match={match} />) : (
                <div className="text-[13px] text-text-muted">{tCommon('unknown')}</div>
              )}
            </div>
            </SectionCard>
          ) : null}

          {activeTab === 'matches' ? (
            <SectionCard title={tNation('upcomingFixtures')}>
            <div className="space-y-1.5">
              {upcomingFixtures.length > 0 ? upcomingFixtures.map((match) => <FixtureCard key={match.id} match={match} />) : (
                <div className="text-[13px] text-text-muted">{tCommon('unknown')}</div>
              )}
            </div>
            </SectionCard>
          ) : null}
        </div>
      </div>
    </div>
  );
}
