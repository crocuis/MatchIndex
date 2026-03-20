'use client';

import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import { Badge } from '@/components/ui/Badge';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { PaginationNav } from '@/components/ui/PaginationNav';
import { SectionCard } from '@/components/ui/SectionCard';
import { getCompetitionFormatDetailKey, isTournamentCompetition } from '@/data/competitionTypes';
import type { League, PaginatedResult } from '@/data/types';

type CompetitionListFilter = 'all' | 'league' | 'tournament';

interface CompetitionsListPanelProps {
  initialFilter: CompetitionListFilter;
  initialPage: number;
  query: string;
  labels: {
    title: string;
    subtitle: string;
    name: string;
    country: string;
    season: string;
    format: string;
    clubs: string;
    previous: string;
    next: string;
    pageOf: string;
    formatLeague: string;
    formatTournament: string;
    formatLeagueDetail: string;
    formatTournamentDetail: string;
    formatTournamentGroupStageDetail: string;
    formatTournamentKnockoutOnlyDetail: string;
    allTypes: string;
    allTypesMeta: string;
    leagueTypes: string;
    leagueTypesMeta: string;
    tournamentTypes: string;
    tournamentTypesMeta: string;
  };
}

function getFormatDetailLabel(league: League, labels: CompetitionsListPanelProps['labels']) {
  const detailKey = getCompetitionFormatDetailKey(league);
  return labels[detailKey];
}

function formatPageLabel(template: string, page: number, totalPages: number) {
  return template.replace('{page}', String(page)).replace('{totalPages}', String(totalPages));
}

function buildCompetitionsUrl(filter: CompetitionListFilter, query: string, page: number) {
  const params = new URLSearchParams();
  if (page > 1) {
    params.set('page', String(page));
  }
  if (query) {
    params.set('q', query);
  }
  if (filter !== 'all') {
    params.set('type', filter);
  }

  const queryString = params.toString();
  return queryString ? `/competitions?${queryString}` : '/competitions';
}

export function CompetitionsListPanel({ initialFilter, initialPage, query, labels }: CompetitionsListPanelProps) {
  const [activeFilter, setActiveFilter] = useState<CompetitionListFilter>(initialFilter);
  const [page, setPage] = useState(initialPage);
  const [result, setResult] = useState<PaginatedResult<League> | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    setActiveFilter(initialFilter);
    setPage(initialPage);
    setResult(null);
    setIsLoading(true);
  }, [initialFilter, initialPage, query]);

  useEffect(() => {
    const controller = new AbortController();
    const nextUrl = buildCompetitionsUrl(activeFilter, query, page);

    setIsLoading(true);
    fetch(`/api/competitions${nextUrl.replace('/competitions', '')}`, {
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error('Failed to load competitions');
        }

        const data = (await response.json()) as PaginatedResult<League>;
        setResult(data);
        window.history.replaceState(window.history.state, '', nextUrl);
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') {
          return;
        }
      })
      .finally(() => {
        setIsLoading(false);
      });

    return () => controller.abort();
  }, [activeFilter, page, query]);

  const filterOptions = useMemo(() => [
    { key: 'all', label: labels.allTypes, meta: labels.allTypesMeta },
    { key: 'league', label: labels.leagueTypes, meta: labels.leagueTypesMeta },
    { key: 'tournament', label: labels.tournamentTypes, meta: labels.tournamentTypesMeta },
  ] as const, [labels]);

  const totalCount = result?.totalCount ?? 0;
  const currentPage = result?.currentPage ?? page;
  const totalPages = result?.totalPages ?? 1;

  const tableContent = result ? (
    <table className="w-full">
      <thead>
        <tr className="border-b border-border">
          <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">{labels.name}</th>
          <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">{labels.country}</th>
          <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">{labels.season}</th>
          <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">{labels.format}</th>
          <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">{labels.clubs}</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-border-subtle">
        {result.items.map((league) => {
          const isTournament = isTournamentCompetition(league);

          return (
            <tr key={league.id} className="hover:bg-surface-2 transition-colors">
              <td className="px-3 py-2 text-[13px]">
                <Link
                  href={`/competitions/${league.id}`}
                  prefetch
                  className="flex items-center gap-2.5 text-text-primary hover:text-accent-emerald transition-colors"
                >
                  <LeagueLogo leagueId={league.id} name={league.name} competitionType={league.competitionType} logo={league.logo} size="sm" />
                  <span className="font-medium">{league.name}</span>
                </Link>
              </td>
              <td className="px-3 py-2 text-[13px] text-text-secondary">{league.country}</td>
              <td className="px-3 py-2 text-[13px] text-text-secondary">{league.season}</td>
              <td className="px-3 py-2 text-[13px] text-text-secondary">
                <div className="flex items-center gap-2">
                  <Badge variant={isTournament ? 'info' : 'default'}>
                    {isTournament ? labels.formatTournament : labels.formatLeague}
                  </Badge>
                  <span>{getFormatDetailLabel(league, labels)}</span>
                </div>
              </td>
              <td className="px-3 py-2 text-center text-[13px] tabular-nums text-text-secondary">{league.numberOfClubs}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  ) : (
    <div className="p-3">
      <div className="space-y-2">
        {Array.from({ length: 10 }, (_, index) => (
          <div key={index} className="grid grid-cols-[2.5fr_1fr_1fr_2fr_80px] items-center gap-3 rounded border border-border-subtle bg-surface-1/70 px-3 py-3">
            <div className="h-3 w-2/3 animate-pulse rounded bg-surface-3/80" />
            <div className="h-3 w-3/4 animate-pulse rounded bg-surface-3/80" />
            <div className="h-3 w-1/2 animate-pulse rounded bg-surface-3/80" />
            <div className="h-3 w-5/6 animate-pulse rounded bg-surface-3/80" />
            <div className="ml-auto h-3 w-8 animate-pulse rounded bg-surface-3/80" />
          </div>
        ))}
      </div>
    </div>
  );

  return (
    <>
      <div className="mb-4 flex items-end justify-between gap-3">
        <div>
          <div className="text-[22px] font-semibold tracking-tight text-text-primary">{labels.title}</div>
          <div className="mt-1 text-[13px] text-text-secondary">{labels.subtitle.replace('{count}', String(totalCount))}</div>
        </div>
      </div>

      <div className="mb-4 grid gap-2 md:grid-cols-3">
        {filterOptions.map((filterOption) => {
          const isActive = activeFilter === filterOption.key;

          return (
            <button
              key={filterOption.key}
              type="button"
              onClick={() => {
                setActiveFilter(filterOption.key);
                setPage(1);
              }}
              className={isActive
                ? 'rounded-lg border border-accent-emerald/50 bg-surface-2 px-3 py-3 text-left text-text-primary shadow-[inset_0_0_0_1px_rgba(255,255,255,0.04)] transition-colors'
                : 'rounded-lg border border-border bg-surface-1 px-3 py-3 text-left text-text-secondary transition-colors hover:border-border-subtle hover:bg-surface-2/60 hover:text-text-primary'}
            >
              <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-text-muted">
                {filterOption.label}
              </div>
              <div className="mt-1 text-[12px] leading-5 text-text-secondary">{filterOption.meta}</div>
            </button>
          );
        })}
      </div>

      <SectionCard
        title={labels.title}
        noPadding
        action={(
          <PaginationNav
            currentPage={currentPage}
            totalPages={totalPages}
            hrefForPage={(nextPage) => buildCompetitionsUrl(activeFilter, query, nextPage)}
            previousLabel={labels.previous}
            nextLabel={labels.next}
            pageLabel={formatPageLabel(labels.pageOf, currentPage, totalPages)}
          />
        )}
      >
        <div className="relative">
          {isLoading ? <div className="absolute inset-0 z-10 bg-surface-0/35 backdrop-blur-[1px]" /> : null}
          {tableContent}
        </div>
      </SectionCard>
    </>
  );
}
