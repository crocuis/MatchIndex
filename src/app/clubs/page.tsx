import Link from 'next/link';
import type { Metadata } from 'next';
import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { ListSearchForm } from '@/components/ui/ListSearchForm';
import { PaginationNav } from '@/components/ui/PaginationNav';
import { getPaginatedClubsDb } from '@/data/server';
import { formatNumber, getClubDisplayName } from '@/lib/utils';

const PAGE_SIZE = 50;

function parsePage(value?: string) {
  const page = Number.parseInt(value ?? '1', 10);
  return Number.isFinite(page) && page > 0 ? page : 1;
}

export const metadata: Metadata = {
  title: 'Clubs',
};

export default async function ClubsPage({
  searchParams,
}: {
  searchParams: Promise<{ page?: string; q?: string; gender?: string }>;
}) {
  const locale = await getLocale();
  const { page, q, gender } = await searchParams;
  const currentPage = parsePage(page);
  const query = q?.trim() ?? '';
  const genderCategory = gender === 'women' ? 'women' : 'men';
  const genderFilter = genderCategory === 'women' ? 'female' : 'male';
  const clubsResult = await getPaginatedClubsDb(locale, query, genderFilter, { page: currentPage, pageSize: PAGE_SIZE });
  const t = await getTranslations('clubsList');
  const tClub = await getTranslations('club');
  const tCommon = await getTranslations('common');

  const hrefForPage = (nextPage: number) => {
    const params = new URLSearchParams();
    if (nextPage > 1) params.set('page', String(nextPage));
    if (query) params.set('q', query);
    params.set('gender', genderCategory);
    const queryString = params.toString();
    return queryString ? `/clubs?${queryString}` : '/clubs';
  };

  return (
    <div>
      <PageHeader
        title={t('title')}
        subtitle={t('subtitle', { count: clubsResult.totalCount })}
      />

      <div className="mb-3 flex items-center gap-2">
        <Link
          href={query ? `/clubs?gender=men&q=${encodeURIComponent(query)}` : '/clubs?gender=men'}
          className={genderCategory === 'men'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {t('men')}
        </Link>
        <Link
          href={query ? `/clubs?gender=women&q=${encodeURIComponent(query)}` : '/clubs?gender=women'}
          className={genderCategory === 'women'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {t('women')}
        </Link>
      </div>

      <ListSearchForm
        action="/clubs"
        query={query}
        placeholder={t('searchPlaceholder')}
        searchLabel={tCommon('search')}
        clearLabel={tCommon('clear')}
        hiddenValues={{ gender: genderCategory }}
      />

      <SectionCard
        title={t('title')}
        noPadding
        action={(
          <PaginationNav
            currentPage={clubsResult.currentPage}
            totalPages={clubsResult.totalPages}
            hrefForPage={hrefForPage}
            previousLabel={tCommon('previous')}
            nextLabel={tCommon('next')}
            pageLabel={tCommon('pageOf', { page: clubsResult.currentPage, totalPages: clubsResult.totalPages })}
          />
        )}
      >
        <table className="w-full">
          <thead>
            <tr className="border-b border-border">
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {t('name')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tClub('league')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tClub('country')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tClub('stadium')}
              </th>
              <th className="px-3 py-2 text-right text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tClub('capacity')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tClub('founded')}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-subtle">
            {clubsResult.items.map((club) => {
              return (
                <tr key={club.id} className="hover:bg-surface-2 transition-colors">
                  <td className="px-3 py-2 text-[13px]">
                    <Link
                      href={`/clubs/${club.id}`}
                      className="flex items-center gap-2.5 text-text-primary hover:text-accent-emerald transition-colors"
                    >
                      <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="sm" />
                       <span className="font-medium">{getClubDisplayName(club, locale)}</span>
                     </Link>
                   </td>
                  <td className="px-3 py-2 text-[13px] text-text-secondary">
                    {club.leagueName ?? '-'}
                  </td>
                  <td className="px-3 py-2 text-[13px] text-text-secondary">
                    {club.country}
                  </td>
                  <td className="px-3 py-2 text-[13px] text-text-secondary">
                    {club.stadium}
                  </td>
                  <td className="px-3 py-2 text-[13px] text-right tabular-nums text-text-secondary">
                    {formatNumber(club.stadiumCapacity)}
                  </td>
                  <td className="px-3 py-2 text-[13px] text-center tabular-nums text-text-secondary">
                    {club.founded}
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
