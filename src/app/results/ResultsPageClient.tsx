import Link from 'next/link';
import { getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { ListSearchForm } from '@/components/ui/ListSearchForm';
import { NationFlag } from '@/components/ui/NationFlag';
import { LocalizedMatchText } from '@/components/ui/LocalizedMatchText';
import { PaginationNav } from '@/components/ui/PaginationNav';
import type { Match, PaginatedResult } from '@/data/types';
import { cn } from '@/lib/utils';

interface LeagueFilterOption {
  id: string;
  name: string;
}

interface ResultsPageClientProps {
  initialLeagues: LeagueFilterOption[];
  results: PaginatedResult<Match>;
  selectedLeague: string;
  query: string;
  gender: 'men' | 'women';
}

function buildResultsHref(page: number, league?: string, query?: string, gender: 'men' | 'women' = 'men') {
  const params = new URLSearchParams();

  if (page > 1) {
    params.set('page', String(page));
  }

  if (league && league !== 'all') {
    params.set('league', league);
  }

  if (query) {
    params.set('q', query);
  }

  params.set('gender', gender);

  const queryString = params.toString();
  return queryString ? `/results?${queryString}` : '/results';
}

export async function ResultsPageClient({ initialLeagues, results, selectedLeague, query, gender }: ResultsPageClientProps) {
  const [tResults, tCommon] = await Promise.all([
    getTranslations('results'),
    getTranslations('common'),
  ]);

  const hrefForPage = (page: number) => buildResultsHref(page, selectedLeague, query, gender);

  return (
    <div>
      <PageHeader
        title={tResults('title')}
        subtitle={tResults('matchCount', { count: results.totalCount })}
      />

      <ListSearchForm
        action="/results"
        query={query}
        placeholder={tResults('searchPlaceholder')}
        searchLabel={tCommon('search')}
        clearLabel={tCommon('clear')}
        hiddenValues={{ ...(selectedLeague !== 'all' ? { league: selectedLeague } : {}), gender }}
      />

      <div className="mb-3 flex items-center gap-2">
        <Link
          href={query ? `/results?gender=men&q=${encodeURIComponent(query)}` : '/results?gender=men'}
          className={gender === 'men'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {tResults('men')}
        </Link>
        <Link
          href={query ? `/results?gender=women&q=${encodeURIComponent(query)}` : '/results?gender=women'}
          className={gender === 'women'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {tResults('women')}
        </Link>
      </div>

      <div className="flex gap-1.5 mb-4 flex-wrap">
        <Link
          href={buildResultsHref(1, undefined, query, gender)}
          className={cn(
            'px-3 py-1.5 rounded text-[12px] font-medium transition-colors',
            selectedLeague === 'all'
              ? 'bg-surface-3 text-text-primary'
              : 'bg-surface-2 text-text-muted hover:text-text-secondary'
          )}
        >
          {tResults('allLeagues')}
        </Link>
        {initialLeagues.map((league) => (
          <Link
            key={league.id}
            href={buildResultsHref(1, league.id, query, gender)}
            className={cn(
              'px-3 py-1.5 rounded text-[12px] font-medium transition-colors',
              selectedLeague === league.id
                ? 'bg-surface-3 text-text-primary'
                : 'bg-surface-2 text-text-muted hover:text-text-secondary'
            )}
          >
            {league.name}
          </Link>
        ))}
      </div>

      <SectionCard
        title={tResults('matchResults')}
        noPadding
        action={(
          <PaginationNav
            currentPage={results.currentPage}
            totalPages={results.totalPages}
            hrefForPage={hrefForPage}
            previousLabel={tCommon('previous')}
            nextLabel={tCommon('next')}
            pageLabel={tCommon('pageOf', { page: results.currentPage, totalPages: results.totalPages })}
          />
        )}
      >
        <table className="w-full">
          <thead>
            <tr className="border-b border-border">
              <th className="px-3 py-2 text-left w-20">{tResults('date')}</th>
              <th className="px-3 py-2 text-right">{tResults('home')}</th>
              <th className="px-3 py-2 text-center w-16">{tResults('score')}</th>
              <th className="px-3 py-2 text-left">{tResults('away')}</th>
              <th className="px-3 py-2 text-left">{tResults('league')}</th>
              <th className="px-3 py-2 text-left">{tResults('venue')}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-subtle">
            {results.items.map((match) => {
              const isNationMatch = match.teamType === 'nation';
              const matchHref = `/matches/${match.id}`;

              return (
                <tr key={match.id} className="transition-colors hover:bg-surface-2">
                  <td className="p-0 text-[13px] text-text-muted tabular-nums">
                    <Link href={matchHref} className="block px-3 py-1.5 text-inherit">
                      <LocalizedMatchText date={match.date} time={match.time} variant="dateShort" />
                    </Link>
                  </td>
                  <td className="p-0 text-[13px] text-right font-medium text-text-primary">
                    <Link href={matchHref} className="block px-3 py-1.5 text-inherit">
                      <div className="flex items-center justify-end gap-2">
                        <span>{match.homeTeamName ?? '-'}</span>
                        {isNationMatch
                          ? <NationFlag nationId={match.homeTeamId} code={match.homeTeamCode ?? '???'} size="sm" />
                          : <ClubBadge shortName={match.homeTeamCode ?? '???'} clubId={match.homeTeamId} logo={match.homeTeamLogo} size="sm" />}
                      </div>
                    </Link>
                  </td>
                  <td className="p-0 text-[13px] text-center font-bold tabular-nums">
                    <Link href={matchHref} className="block px-3 py-1.5 text-inherit">
                      {match.homeScore} - {match.awayScore}
                    </Link>
                  </td>
                  <td className="p-0 text-[13px] font-medium text-text-primary">
                    <Link href={matchHref} className="block px-3 py-1.5 text-inherit">
                      <div className="flex items-center gap-2">
                        {isNationMatch
                          ? <NationFlag nationId={match.awayTeamId} code={match.awayTeamCode ?? '???'} size="sm" />
                          : <ClubBadge shortName={match.awayTeamCode ?? '???'} clubId={match.awayTeamId} logo={match.awayTeamLogo} size="sm" />}
                        <span>{match.awayTeamName ?? '-'}</span>
                      </div>
                    </Link>
                  </td>
                  <td className="p-0 text-[13px] text-text-secondary"><Link href={matchHref} className="block px-3 py-1.5 text-inherit">{match.competitionName ?? '-'}</Link></td>
                  <td className="p-0 text-[13px] text-text-muted"><Link href={matchHref} className="block px-3 py-1.5 text-inherit">{match.venue}</Link></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </SectionCard>
    </div>
  );
}
