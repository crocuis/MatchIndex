import { notFound } from 'next/navigation';
import { Suspense } from 'react';
import { getLocale, getTranslations } from 'next-intl/server';
import { PlayerTabContent } from '@/app/players/[id]/PlayerTabContent';
import { PageHeader } from '@/components/layout/PageHeader';
import { DetailTabSkeleton } from '@/components/ui/DetailTabSkeleton';
import { SectionCard } from '@/components/ui/SectionCard';
import { DetailTabNav } from '@/components/ui/DetailTabNav';
import { StatPanel } from '@/components/data/StatPanel';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { cn, getPositionColor } from '@/lib/utils';
import { getPlayerByIdDb } from '@/data/server';

export default async function PlayerPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ tab?: string }>;
}) {
  const { id } = await params;
  const { tab } = await searchParams;
  const activeTab = tab === 'stats' || tab === 'market-value' || tab === 'transfers' || tab === 'national-team'
    ? tab
    : 'profile';
  const locale = await getLocale();
  const player = await getPlayerByIdDb(id, locale);
  if (!player) notFound();
  const currentYear = new Date().getUTCFullYear();
  const latestClubHistoryYear = player.clubHistory?.[player.clubHistory.length - 1]?.endYear;
  const isRetired = Boolean(player.isRetired || (latestClubHistoryYear !== undefined && latestClubHistoryYear < currentYear - 2));
  const stats = player.seasonStats;
  const tPlayer = await getTranslations('player');
  const tCommon = await getTranslations('common');
  const detailTabs = [
    { key: 'profile', label: tCommon('tabProfile') },
    { key: 'stats', label: tCommon('tabStats') },
    { key: 'market-value', label: tPlayer('marketValueTab') },
    { key: 'transfers', label: tPlayer('transfersTab') },
    { key: 'national-team', label: tPlayer('nationalTeamTab') },
  ] as const;

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <PlayerAvatar name={player.name} position={player.position} imageUrl={player.photoUrl} size="xl" />
            <span>{player.name}</span>
          </div>
        )}
        subtitle={`${player.nationality} · ${player.position}`}
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

      <DetailTabNav
        activeTab={activeTab}
        basePath={`/players/${id}`}
        className="mb-4"
        tabs={detailTabs.map((entry) => ({ ...entry }))}
      />

      <Suspense
        fallback={
          <DetailTabSkeleton
            title={detailTabs.find((entry) => entry.key === activeTab)?.label ?? tCommon('loading')}
            primaryCount={activeTab === 'profile' || activeTab === 'national-team' ? 3 : 2}
            secondaryCount={activeTab === 'stats' ? 1 : 0}
            sidebarCount={2}
          />
        }
      >
        <PlayerTabContent player={player} activeTab={activeTab} locale={locale} isRetired={isRetired} />
      </Suspense>
    </div>
  );
}
