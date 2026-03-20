import type { Metadata } from 'next';
import { getTranslations } from 'next-intl/server';
import { ListSearchForm } from '@/components/ui/ListSearchForm';
import { CompetitionsListPanel } from '@/app/competitions/CompetitionsListPanel';

type CompetitionListFilter = 'all' | 'league' | 'tournament';

function parsePage(value?: string) {
  const page = Number.parseInt(value ?? '1', 10);
  return Number.isFinite(page) && page > 0 ? page : 1;
}

function parseCompetitionListFilter(value?: string): CompetitionListFilter {
  if (value === 'league' || value === 'tournament') {
    return value;
  }

  return 'all';
}

export const metadata: Metadata = {
  title: 'Competitions',
};

export default async function LeaguesPage({
  searchParams,
}: {
  searchParams: Promise<{ page?: string; q?: string; type?: string }>;
}) {
  const { page, q, type } = await searchParams;
  const currentPage = parsePage(page);
  const query = q?.trim() ?? '';
  const competitionFilter = parseCompetitionListFilter(type);
  const t = await getTranslations('leaguesList');
  const tLeague = await getTranslations('league');
  const tCommon = await getTranslations('common');

  return (
    <div>
      <ListSearchForm
        action="/competitions"
        query={query}
        placeholder={t('searchPlaceholder')}
        searchLabel={tCommon('search')}
        clearLabel={tCommon('clear')}
        hiddenValues={competitionFilter !== 'all' ? { type: competitionFilter } : undefined}
      />

      <CompetitionsListPanel
        initialFilter={competitionFilter}
        initialPage={currentPage}
        query={query}
        labels={{
          title: t('title'),
          subtitle: t('subtitle', { count: '{count}' }),
          name: t('name'),
          country: tLeague('country'),
          season: tLeague('season'),
          format: tLeague('format'),
          clubs: tLeague('clubs'),
          previous: tCommon('previous'),
          next: tCommon('next'),
          pageOf: tCommon('pageOf', { page: '{page}', totalPages: '{totalPages}' }),
          formatLeague: tLeague('formatLeague'),
          formatTournament: tLeague('formatTournament'),
          formatLeagueDetail: tLeague('formatLeagueDetail'),
          formatTournamentDetail: tLeague('formatTournamentDetail'),
          formatTournamentGroupStageDetail: tLeague('formatTournamentGroupStageDetail'),
          formatTournamentKnockoutOnlyDetail: tLeague('formatTournamentKnockoutOnlyDetail'),
          allTypes: t('allTypes'),
          allTypesMeta: t('allTypesMeta'),
          leagueTypes: t('leagueTypes'),
          leagueTypesMeta: t('leagueTypesMeta'),
          tournamentTypes: t('tournamentTypes'),
          tournamentTypesMeta: t('tournamentTypesMeta'),
        }}
      />
    </div>
  );
}
