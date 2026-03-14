import { getLocale } from 'next-intl/server';
import { ResultsPageClient } from '@/app/results/ResultsPageClient';
import { getLeagueFilterOptionsDb, getPaginatedFinishedMatchesDb } from '@/data/server';

const PAGE_SIZE = 50;

function parsePage(value?: string) {
  const page = Number.parseInt(value ?? '1', 10);
  return Number.isFinite(page) && page > 0 ? page : 1;
}

export default async function ResultsPage({
  searchParams,
}: {
  searchParams: Promise<{ page?: string; league?: string; q?: string; gender?: string }>;
}) {
  const locale = await getLocale();
  const { page, league, q, gender } = await searchParams;
  const currentPage = parsePage(page);
  const selectedLeague = league && league !== 'all' ? league : undefined;
  const query = q?.trim() ?? '';
  const genderCategory = gender === 'women' ? 'women' : 'men';
  const genderFilter = genderCategory === 'women' ? 'female' : 'male';
  const [initialLeagues, results] = await Promise.all([
    getLeagueFilterOptionsDb(locale, genderFilter),
    getPaginatedFinishedMatchesDb(locale, selectedLeague, query, genderFilter, { page: currentPage, pageSize: PAGE_SIZE }),
  ]);

  return (
    <ResultsPageClient
      initialLeagues={initialLeagues}
      results={results}
      selectedLeague={selectedLeague ?? 'all'}
      query={query}
      gender={genderCategory}
    />
  );
}
