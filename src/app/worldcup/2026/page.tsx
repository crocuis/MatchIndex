import type { Metadata } from 'next';
import { Suspense } from 'react';
import { getLocale, getTranslations } from 'next-intl/server';
import { StatPanel } from '@/components/data/StatPanel';
import { DetailTabSkeleton } from '@/components/ui/DetailTabSkeleton';
import { SectionCardSkeleton } from '@/components/ui/SectionCardSkeleton';
import { PageHeader } from '@/components/layout/PageHeader';
import { WorldCupGroupsSection } from '@/app/worldcup/2026/WorldCupGroupsSection';
import { WorldCupScheduleSection } from '@/app/worldcup/2026/WorldCupScheduleSection';
import { WorldCupSpotlightsSection } from '@/app/worldcup/2026/WorldCupSpotlightsSection';
import {
  getWorldCup2026Db,
} from '@/data/server';

export const metadata: Metadata = {
  title: '2026 World Cup',
};

export default async function WorldCup2026Page() {
  const locale = await getLocale();
  const [tournament, tWorldCup] = await Promise.all([
    getWorldCup2026Db(),
    getTranslations('worldCup'),
  ]);
  const nationIds = Array.from(new Set(tournament.groups.flatMap((group) => group.standings.map((row) => row.nationId))));
  const matchCount = new Set([
    ...tournament.matches.map((match) => match.id),
    ...tournament.stages.flatMap((stage) => stage.matchIds),
  ]).size;

  return (
    <div>
      <PageHeader title={tWorldCup('title')} subtitle={tWorldCup('subtitle')} meta={tournament.year}>
        <StatPanel
          stats={[
            { label: tWorldCup('host'), value: tournament.host },
            { label: tWorldCup('nations'), value: nationIds.length },
            { label: tWorldCup('matches'), value: matchCount },
          ]}
          columns={3}
          className="w-[32rem]"
        />
      </PageHeader>

      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-8 space-y-4">
          <Suspense fallback={<DetailTabSkeleton title={tWorldCup('groups')} primaryCount={2} secondaryCount={0} sidebarCount={0} />}>
            <WorldCupGroupsSection locale={locale} />
          </Suspense>
        </div>

        <div className="col-span-4 space-y-4">
          <Suspense fallback={<SectionCardSkeleton title={tWorldCup('schedule')} rows={3} blocks={1} />}>
            <WorldCupScheduleSection locale={locale} />
          </Suspense>
          <Suspense fallback={<SectionCardSkeleton title={tWorldCup('spotlights')} rows={4} compact />}>
            <WorldCupSpotlightsSection locale={locale} />
          </Suspense>
        </div>
      </div>
    </div>
  );
}
