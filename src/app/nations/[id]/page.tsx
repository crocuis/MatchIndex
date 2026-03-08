import { notFound } from 'next/navigation';
import type { Metadata } from 'next';
import { getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { StatPanel } from '@/components/data/StatPanel';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationFlag } from '@/components/ui/NationFlag';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { getPositionColor, cn } from '@/lib/utils';
import {
  getNationById,
  getNations,
  getPlayersByNation,
  getClubById,
} from '@/data';

export async function generateStaticParams() {
  return getNations().map((n) => ({ id: n.id }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ id: string }>;
}): Promise<Metadata> {
  const { id } = await params;
  const nation = getNationById(id);
  return { title: nation?.name ?? 'Nation' };
}

export default async function NationPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const nation = getNationById(id);
  if (!nation) notFound();

  const nationalPlayers = getPlayersByNation(id);
  const tNation = await getTranslations('nation');
  const tTable = await getTranslations('table');
  const tCommon = await getTranslations('common');

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <NationFlag nationId={nation.id} code={nation.code} size="lg" />
            <span>{nation.name}</span>
          </div>
        )}
        subtitle={`${nation.confederation} · FIFA Ranking #${nation.fifaRanking}`}
      />

      <StatPanel
        stats={[
          { label: tNation('fifaRanking'), value: `#${nation.fifaRanking}`, highlight: nation.fifaRanking <= 5 },
          { label: tNation('confederation'), value: nation.confederation },
          { label: tNation('countryCode'), value: nation.code },
          { label: tNation('players'), value: nationalPlayers.length },
        ]}
        columns={4}
        className="mb-4"
      />

      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-12">
          {/* Squad */}
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
                    const club = getClubById(player.clubId);
                    return (
                      <tr key={player.id} className="hover:bg-surface-2">
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
                        <td className="px-3 py-1.5 text-[13px]">
                          {club ? (
                            <EntityLink type="club" id={club.id} className="flex items-center gap-2 text-text-secondary">
                              <ClubBadge shortName={club.shortName} clubId={club.id} size="sm" />
                              <span>{club.name}</span>
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
        </div>
      </div>
    </div>
  );
}
