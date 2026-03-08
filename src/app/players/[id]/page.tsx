import { notFound } from 'next/navigation';
import type { Metadata } from 'next';
import { getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { StatPanel } from '@/components/data/StatPanel';
import { MatchCard } from '@/components/data/MatchCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationFlag } from '@/components/ui/NationFlag';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { getPositionColor, cn } from '@/lib/utils';
import {
  getPlayerById,
  getPlayers,
  getClubById,
  getNationById,
  getMatchesByClub,
} from '@/data';

export async function generateStaticParams() {
  return getPlayers().map((p) => ({ id: p.id }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ id: string }>;
}): Promise<Metadata> {
  const { id } = await params;
  const player = getPlayerById(id);
  return { title: player?.name ?? 'Player' };
}

export default async function PlayerPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const player = getPlayerById(id);
  if (!player) notFound();

  const club = getClubById(player.clubId);
  const nation = getNationById(player.nationId);
  const recentMatches = getMatchesByClub(player.clubId)
    .filter((m) => m.status === 'finished')
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())
    .slice(0, 10);

  const stats = player.seasonStats;
  const tPlayer = await getTranslations('player');
  const tCommon = await getTranslations('common');

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <PlayerAvatar name={player.name} position={player.position} size="lg" />
            <span>{player.name}</span>
          </div>
        )}
        subtitle={`${club?.name ?? tCommon('freeAgent')} · ${player.position}`}
      >
        <span className={cn('px-2 py-1 rounded text-[11px] font-bold', getPositionColor(player.position))}>
          {player.position}
        </span>
      </PageHeader>

      <StatPanel
        stats={[
          { label: tPlayer('appearances'), value: stats.appearances },
          { label: tPlayer('goals'), value: stats.goals, highlight: stats.goals >= 10 },
          { label: tPlayer('assists'), value: stats.assists, highlight: stats.assists >= 10 },
          { label: tPlayer('minutes'), value: stats.minutesPlayed.toLocaleString() },
        ]}
        columns={4}
        className="mb-4"
      />

      <div className="grid grid-cols-12 gap-4">
        {/* Main */}
        <div className="col-span-8 space-y-4">
          {/* Season Stats Detail */}
          <SectionCard title={tPlayer('seasonStats')}>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                {[
                  [tPlayer('appearances'), stats.appearances],
                  [tPlayer('goals'), stats.goals],
                  [tPlayer('assists'), stats.assists],
                  [tPlayer('minutesPlayed'), stats.minutesPlayed.toLocaleString()],
                ].map(([label, value]) => (
                  <div key={label as string} className="flex justify-between items-center">
                    <span className="text-[13px] text-text-secondary">{label}</span>
                    <span className="text-[13px] text-text-primary font-semibold tabular-nums">{value}</span>
                  </div>
                ))}
              </div>
              <div className="space-y-2">
                {[
                  [tPlayer('yellowCards'), stats.yellowCards],
                  [tPlayer('redCards'), stats.redCards],
                  ...(stats.cleanSheets !== undefined
                    ? [[tPlayer('cleanSheets'), stats.cleanSheets] as const]
                    : []),
                  [tPlayer('goalsPer90'), stats.minutesPlayed > 0 ? ((stats.goals / stats.minutesPlayed) * 90).toFixed(2) : '0.00'],
                ].map(([label, value]) => (
                  <div key={label as string} className="flex justify-between items-center">
                    <span className="text-[13px] text-text-secondary">{label}</span>
                    <span className="text-[13px] text-text-primary font-semibold tabular-nums">{String(value)}</span>
                  </div>
                ))}
              </div>
            </div>
          </SectionCard>

          {/* Recent Matches */}
          <SectionCard title={tPlayer('recentMatches')}>
            <div className="space-y-1.5">
              {recentMatches.map((m) => (
                <MatchCard key={m.id} match={m} />
              ))}
            </div>
          </SectionCard>
        </div>

        {/* Sidebar — Bio */}
        <div className="col-span-4 space-y-4">
          <SectionCard title={tPlayer('playerInfo')}>
            <dl className="space-y-2">
              {[
                [tPlayer('fullName'), `${player.firstName} ${player.lastName}`],
                [tPlayer('dateOfBirth'), player.dateOfBirth],
                [tPlayer('age'), String(player.age)],
                [tPlayer('height'), `${player.height} cm`],
                [tPlayer('nationality'), player.nationality],
                [tPlayer('position'), player.position],
                [tPlayer('shirtNumber'), `#${player.shirtNumber}`],
                [tPlayer('preferredFoot'), player.preferredFoot],
              ].map(([label, value]) => (
                <div key={label} className="flex justify-between">
                  <dt className="text-[12px] text-text-muted">{label}</dt>
                  <dd className="text-[13px] text-text-primary font-medium">{value}</dd>
                </div>
              ))}
            </dl>
          </SectionCard>

          <SectionCard title={tPlayer('club')}>
            {club ? (
              <EntityLink type="club" id={club.id} className="flex items-center gap-3">
                <ClubBadge shortName={club.shortName} clubId={club.id} size="lg" />
                <div>
                  <div className="text-[13px] font-medium">{club.name}</div>
                  <div className="text-[11px] text-text-muted">{club.stadium}</div>
                </div>
              </EntityLink>
            ) : (
              <div className="text-[13px] text-text-muted">{tCommon('freeAgent')}</div>
            )}
          </SectionCard>

          <SectionCard title={tPlayer('nation')}>
            {nation ? (
              <EntityLink type="nation" id={nation.id} className="flex items-center gap-3">
                <NationFlag nationId={nation.id} code={nation.code} size="lg" />
                <div>
                  <div className="text-[13px] font-medium">{nation.name}</div>
                  <div className="text-[11px] text-text-muted">FIFA #{nation.fifaRanking} · {nation.confederation}</div>
                </div>
              </EntityLink>
            ) : (
              <div className="text-[13px] text-text-muted">{tCommon('unknown')}</div>
            )}
          </SectionCard>
        </div>
      </div>
    </div>
  );
}
