import Link from 'next/link';
import type { Metadata } from 'next';
import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { ListSearchForm } from '@/components/ui/ListSearchForm';
import { Badge } from '@/components/ui/Badge';
import { PaginationNav } from '@/components/ui/PaginationNav';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { getCompetitionFormatDetailKey, isTournamentCompetition } from '@/data/competitionTypes';
import { getPaginatedLeaguesDb } from '@/data/server';

const PAGE_SIZE = 50;

function parsePage(value?: string) {
  const page = Number.parseInt(value ?? '1', 10);
  return Number.isFinite(page) && page > 0 ? page : 1;
}

export const metadata: Metadata = {
  title: 'Leagues',
};

export default async function LeaguesPage({
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
  const leaguesResult = await getPaginatedLeaguesDb(locale, query, genderFilter, { page: currentPage, pageSize: PAGE_SIZE });
  const t = await getTranslations('leaguesList');
  const tLeague = await getTranslations('league');
  const tCommon = await getTranslations('common');

  const hrefForPage = (nextPage: number) => {
    const params = new URLSearchParams();
    if (nextPage > 1) params.set('page', String(nextPage));
    if (query) params.set('q', query);
    params.set('gender', genderCategory);
    const queryString = params.toString();
    return queryString ? `/leagues?${queryString}` : '/leagues';
  };

  return (
    <div>
      <PageHeader
        title={t('title')}
        subtitle={t('subtitle', { count: leaguesResult.totalCount })}
      />

      <div className="mb-3 flex items-center gap-2">
        <Link
          href={query ? `/leagues?gender=men&q=${encodeURIComponent(query)}` : '/leagues?gender=men'}
          className={genderCategory === 'men'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {t('men')}
        </Link>
        <Link
          href={query ? `/leagues?gender=women&q=${encodeURIComponent(query)}` : '/leagues?gender=women'}
          className={genderCategory === 'women'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {t('women')}
        </Link>
      </div>

      <ListSearchForm
        action="/leagues"
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
            currentPage={leaguesResult.currentPage}
            totalPages={leaguesResult.totalPages}
            hrefForPage={hrefForPage}
            previousLabel={tCommon('previous')}
            nextLabel={tCommon('next')}
            pageLabel={tCommon('pageOf', { page: leaguesResult.currentPage, totalPages: leaguesResult.totalPages })}
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
                {tLeague('country')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tLeague('season')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tLeague('format')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tLeague('clubs')}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-subtle">
            {leaguesResult.items.map((league) => (
              (() => {
                const isTournament = isTournamentCompetition(league);

                return (
               <tr key={league.id} className="hover:bg-surface-2 transition-colors">
                <td className="px-3 py-2 text-[13px]">
                  <Link
                    href={`/leagues/${league.id}`}
                    className="flex items-center gap-2.5 text-text-primary hover:text-accent-emerald transition-colors"
                  >
                  <LeagueLogo leagueId={league.id} name={league.name} competitionType={league.competitionType} logo={league.logo} size="sm" />
                    <span className="font-medium">{league.name}</span>
                  </Link>
                </td>
                <td className="px-3 py-2 text-[13px] text-text-secondary">
                  {league.country}
                </td>
                <td className="px-3 py-2 text-[13px] text-text-secondary">
                  {league.season}
                </td>
                <td className="px-3 py-2 text-[13px] text-text-secondary">
                  <div className="flex items-center gap-2">
                    <Badge variant={isTournament ? 'info' : 'default'}>
                      {isTournament ? tLeague('formatTournament') : tLeague('formatLeague')}
                    </Badge>
                    <span>{tLeague(getCompetitionFormatDetailKey(league))}</span>
                  </div>
                </td>
                <td className="px-3 py-2 text-[13px] text-center tabular-nums text-text-secondary">
                  {league.numberOfClubs}
                </td>
              </tr>
                );
              })()
            ))}
          </tbody>
        </table>
      </SectionCard>
    </div>
  );
}
