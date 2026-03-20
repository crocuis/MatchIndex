import { notFound } from 'next/navigation';
import { Suspense } from 'react';
import { getLocale, getTranslations } from 'next-intl/server';
import { NationTabContent } from '@/app/nations/[id]/NationTabContent';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { DetailTabSkeleton } from '@/components/ui/DetailTabSkeleton';
import { StatPanel } from '@/components/data/StatPanel';
import { DetailTabNav } from '@/components/ui/DetailTabNav';
import { NationBadge } from '@/components/ui/NationBadge';
import { NationFlag } from '@/components/ui/NationFlag';
import { getNationByIdDb, getPlayerCountByNationDb } from '@/data/server';

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
  const activeTab = tab === 'squad' || tab === 'matches' ? tab : 'overview';
  const locale = await getLocale();
  const nation = await getNationByIdDb(id, locale);
  if (!nation) notFound();

  const playerCount = await getPlayerCountByNationDb(id);
  const tNation = await getTranslations('nation');
  const tCommon = await getTranslations('common');
  const detailTabs = [
    { key: 'overview', label: tCommon('tabOverview') },
    { key: 'squad', label: tCommon('tabSquad') },
    { key: 'matches', label: tCommon('tabMatches') },
  ] as const;

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
          { label: tNation('players'), value: playerCount },
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

      <Suspense
        fallback={
          <DetailTabSkeleton
            title={detailTabs.find((entry) => entry.key === activeTab)?.label ?? tCommon('loading')}
            primaryCount={1}
            secondaryCount={activeTab === 'matches' ? 0 : 1}
            sidebarCount={activeTab === 'matches' ? 2 : 0}
          />
        }
      >
        <NationTabContent nation={nation} activeTab={activeTab} locale={locale} />
      </Suspense>
    </div>
  );
}
