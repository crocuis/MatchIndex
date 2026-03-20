import Link from 'next/link';
import type { Metadata } from 'next';
import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { PaginationNav } from '@/components/ui/PaginationNav';
import { SectionCard } from '@/components/ui/SectionCard';
import { NationFlag } from '@/components/ui/NationFlag';
import { getPaginatedNationsDb } from '@/data/server';

const PAGE_SIZE = 50;

function parsePage(value?: string) {
  const page = Number.parseInt(value ?? '1', 10);
  return Number.isFinite(page) && page > 0 ? page : 1;
}

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

function renderRankingChange(change?: number) {
  if (change === undefined || change === 0) {
    return <span className="text-text-muted">-</span>;
  }

  if (change > 0) {
    return <span className="font-semibold text-win">▲{change}</span>;
  }

  return <span className="font-semibold text-loss">▼{Math.abs(change)}</span>;
}

export const metadata: Metadata = {
  title: 'Nations',
};

export default async function NationsPage({
  searchParams,
}: {
  searchParams: Promise<{ gender?: string; page?: string }>;
}) {
  const locale = await getLocale();
  const { gender, page } = await searchParams;
  const rankingCategory = gender === 'women' ? 'women' : 'men';
  const currentPage = parsePage(page);
  const nationsResult = await getPaginatedNationsDb(locale, rankingCategory, { page: currentPage, pageSize: PAGE_SIZE });
  const t = await getTranslations('nationsList');
  const tNation = await getTranslations('nation');
  const tCommon = await getTranslations('common');

  const hrefForPage = (nextPage: number) => `/nations?gender=${rankingCategory}&page=${nextPage}`;

  return (
    <div>
      <PageHeader
        title={t('title')}
        subtitle={t('subtitle', { count: nationsResult.totalCount })}
      />

       <SectionCard
         title={t('title')}
         noPadding
         action={(
           <PaginationNav
             currentPage={nationsResult.currentPage}
             totalPages={nationsResult.totalPages}
             hrefForPage={hrefForPage}
             previousLabel={tCommon('previous')}
             nextLabel={tCommon('next')}
             pageLabel={tCommon('pageOf', { page: nationsResult.currentPage, totalPages: nationsResult.totalPages })}
           />
         )}
       >
        <div className="flex items-center gap-2 border-b border-border px-3 py-2">
          <Link
            href="/nations?gender=men"
            className={rankingCategory === 'men'
              ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
              : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
          >
            {tNation('rankingMen')}
          </Link>
          <Link
            href="/nations?gender=women"
            className={rankingCategory === 'women'
              ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
              : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
          >
            {tNation('rankingWomen')}
          </Link>
        </div>
        <table className="w-full">
          <thead>
            <tr className="border-b border-border">
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tNation('fifaRanking')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {t('name')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tNation('countryCode')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tNation('continent')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tNation('rankChange')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tNation('players')}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-subtle">
            {nationsResult.items.map((nation) => {
              return (
                <tr key={nation.id} className="hover:bg-surface-2 transition-colors">
                  <td className="px-3 py-2 text-[13px] text-center tabular-nums font-semibold">
                    #{nation.fifaRanking}
                  </td>
                  <td className="px-3 py-2 text-[13px]">
                    <Link
                      href={`/nations/${nation.id}`}
                      prefetch
                      className="flex items-center gap-2.5 font-medium text-text-primary hover:text-accent-emerald transition-colors"
                    >
                      <NationFlag nationId={nation.id} code={nation.code} flag={nation.flag} size="sm" />
                      <span>{nation.name}</span>
                    </Link>
                  </td>
                  <td className="px-3 py-2 text-[13px] text-center font-mono text-text-secondary">
                    {nation.code}
                  </td>
                  <td className="px-3 py-2 text-[13px] text-text-secondary">
                    {getContinentLabel(nation.confederation, locale)}
                  </td>
                  <td className="px-3 py-2 text-[13px] text-center tabular-nums">
                    {renderRankingChange(nation.rankingChange)}
                  </td>
                  <td className="px-3 py-2 text-[13px] text-center tabular-nums text-text-secondary">
                    {nation.playerCount}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </SectionCard>
    </div>
  );
}
