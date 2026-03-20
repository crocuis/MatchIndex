'use client';

import { useEffect, useMemo, useState } from 'react';
import { MatchArchiveSplitList } from '@/components/data/MatchArchiveSplitList';
import { SectionCard } from '@/components/ui/SectionCard';
import { MatchSectionTitle, renderMatchSectionDateLabel } from '@/components/ui/MatchSectionTitle';
import { useStaticDetailTabActive } from '@/components/ui/StaticDetailTabs';
import { cn } from '@/lib/utils';
import type { Match } from '@/data/types';

const MATCHES_PAGE_SIZE = 20;

interface CompetitionMatchesPanelProps {
  competitionId: string;
  seasonId?: string;
  initialMatches?: Match[];
  initialResultsPage: number;
  initialFixturesPage: number;
  locale: string;
  labels: {
    resultsTitle: string;
    fixturesTitle: string;
    recentResults: string;
    upcomingFixtures: string;
    noMatches: string;
    previous: string;
    next: string;
    pageOf: string;
  };
}

function normalizePage(value: number, totalPages: number) {
  if (totalPages <= 0) {
    return 1;
  }

  return Math.min(Math.max(1, value), totalPages);
}

function paginateMatches(matches: Match[], page: number) {
  const totalPages = Math.max(1, Math.ceil(matches.length / MATCHES_PAGE_SIZE));
  const currentPage = normalizePage(page, totalPages);
  const start = (currentPage - 1) * MATCHES_PAGE_SIZE;

  return {
    currentPage,
    totalPages,
    items: matches.slice(start, start + MATCHES_PAGE_SIZE),
  };
}

function formatPageLabel(template: string, page: number, totalPages: number) {
  return template.replace('{page}', String(page)).replace('{totalPages}', String(totalPages));
}

function buildMatchesPanelUrl(competitionId: string, seasonId: string | undefined, resultsPage: number, fixturesPage: number) {
  const query = new URLSearchParams({ tab: 'matches' });

  if (seasonId) {
    query.set('season', seasonId);
  }

  if (resultsPage > 1) {
    query.set('resultsPage', String(resultsPage));
  }

  if (fixturesPage > 1) {
    query.set('fixturesPage', String(fixturesPage));
  }

  return `/competitions/${competitionId}?${query.toString()}`;
}

function MatchesPager({
  currentPage,
  totalPages,
  previousLabel,
  nextLabel,
  pageLabel,
  onChange,
}: {
  currentPage: number;
  totalPages: number;
  previousLabel: string;
  nextLabel: string;
  pageLabel: string;
  onChange: (page: number) => void;
}) {
  if (totalPages <= 1) {
    return null;
  }

  return (
    <div className="flex items-center gap-2 text-[11px]">
      <button
        type="button"
        onClick={() => onChange(currentPage - 1)}
        disabled={currentPage <= 1}
        className={cn(
          'rounded border border-border px-2 py-1 transition-colors',
          currentPage <= 1
            ? 'pointer-events-none opacity-40'
            : 'text-text-secondary hover:bg-surface-2 hover:text-text-primary'
        )}
      >
        {previousLabel}
      </button>
      <span className="min-w-20 text-center font-medium text-text-secondary">{pageLabel}</span>
      <button
        type="button"
        onClick={() => onChange(currentPage + 1)}
        disabled={currentPage >= totalPages}
        className={cn(
          'rounded border border-border px-2 py-1 transition-colors',
          currentPage >= totalPages
            ? 'pointer-events-none opacity-40'
            : 'text-text-secondary hover:bg-surface-2 hover:text-text-primary'
        )}
      >
        {nextLabel}
      </button>
    </div>
  );
}

export function CompetitionMatchesPanel({
  competitionId,
  seasonId,
  initialMatches,
  initialResultsPage,
  initialFixturesPage,
  locale,
  labels,
}: CompetitionMatchesPanelProps) {
  const isActive = useStaticDetailTabActive('matches');
  const [matches, setMatches] = useState<Match[] | null>(initialMatches ?? null);
  const [resultsPage, setResultsPage] = useState(initialResultsPage);
  const [fixturesPage, setFixturesPage] = useState(initialFixturesPage);

  useEffect(() => {
    if (!isActive || matches !== null) {
      return;
    }

    const controller = new AbortController();
    const params = new URLSearchParams();
    if (seasonId) {
      params.set('seasonId', seasonId);
    }

    fetch(`/api/competitions/${competitionId}/matches?${params.toString()}`, {
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error('Failed to load competition matches');
        }

        const data = (await response.json()) as { matches: Match[] };
        setMatches(data.matches);
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') {
          return;
        }

        setMatches([]);
      });

    return () => controller.abort();
  }, [competitionId, isActive, matches, seasonId]);

  const finishedMatches = useMemo(
    () => (matches ?? []).filter((match) => match.status === 'finished'),
    [matches],
  );
  const scheduledMatches = useMemo(
    () => (matches ?? []).filter((match) => match.status === 'scheduled'),
    [matches],
  );
  const paginatedRecentResults = useMemo(
    () => paginateMatches(finishedMatches, resultsPage),
    [finishedMatches, resultsPage],
  );
  const paginatedUpcomingFixtures = useMemo(
    () => paginateMatches(scheduledMatches, fixturesPage),
    [fixturesPage, scheduledMatches],
  );

  useEffect(() => {
    if (!isActive) {
      return;
    }

    window.history.replaceState(
      window.history.state,
      '',
      buildMatchesPanelUrl(competitionId, seasonId, paginatedRecentResults.currentPage, paginatedUpcomingFixtures.currentPage),
    );
  }, [competitionId, isActive, paginatedRecentResults.currentPage, paginatedUpcomingFixtures.currentPage, seasonId]);

  if (matches === null) {
    return (
      <div className="space-y-4">
        {Array.from({ length: 2 }, (_, index) => (
          <SectionCard key={index} title={index === 0 ? labels.resultsTitle : labels.fixturesTitle}>
            <div className="space-y-2">
              {Array.from({ length: 4 }, (_, rowIndex) => (
                <div key={rowIndex} className="rounded border border-border-subtle bg-surface-2/60 px-3 py-3">
                  <div className="h-3 w-1/4 animate-pulse rounded bg-surface-3/80" />
                  <div className="mt-2 h-3 w-full animate-pulse rounded bg-surface-3/80" />
                </div>
              ))}
            </div>
          </SectionCard>
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <SectionCard
        title={<MatchSectionTitle title={labels.resultsTitle} count={finishedMatches.length} dateLabel={renderMatchSectionDateLabel(finishedMatches[0], locale)} variant="results" />}
        action={(
          <MatchesPager
            currentPage={paginatedRecentResults.currentPage}
            totalPages={paginatedRecentResults.totalPages}
            previousLabel={labels.previous}
            nextLabel={labels.next}
            pageLabel={formatPageLabel(labels.pageOf, paginatedRecentResults.currentPage, paginatedRecentResults.totalPages)}
            onChange={setResultsPage}
          />
        )}
      >
        <MatchArchiveSplitList
          matches={paginatedRecentResults.items}
          locale={locale}
          recentResultsLabel={labels.recentResults}
          upcomingFixturesLabel={labels.upcomingFixtures}
          emptyLabel={labels.noMatches}
        />
      </SectionCard>

      <SectionCard
        title={<MatchSectionTitle title={labels.fixturesTitle} count={scheduledMatches.length} dateLabel={renderMatchSectionDateLabel(scheduledMatches[0], locale)} variant="fixtures" />}
        action={(
          <MatchesPager
            currentPage={paginatedUpcomingFixtures.currentPage}
            totalPages={paginatedUpcomingFixtures.totalPages}
            previousLabel={labels.previous}
            nextLabel={labels.next}
            pageLabel={formatPageLabel(labels.pageOf, paginatedUpcomingFixtures.currentPage, paginatedUpcomingFixtures.totalPages)}
            onChange={setFixturesPage}
          />
        )}
      >
        <MatchArchiveSplitList
          matches={paginatedUpcomingFixtures.items}
          locale={locale}
          recentResultsLabel={labels.recentResults}
          upcomingFixturesLabel={labels.upcomingFixtures}
          emptyLabel={labels.noMatches}
        />
      </SectionCard>
    </div>
  );
}
