import type { Metadata } from 'next';
import { notFound, permanentRedirect } from 'next/navigation';
import { getLocale, getTranslations } from 'next-intl/server';
import { LeagueDetailSections } from '@/app/competitions/[id]/LeagueDetailSections';
import { LeagueSeasonSelect } from '@/app/competitions/[id]/LeagueSeasonSelect';
import { SectionCard } from '@/components/ui/SectionCard';
import { PageHeader } from '@/components/layout/PageHeader';
import { StatPanel } from '@/components/data/StatPanel';
import { Badge } from '@/components/ui/Badge';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { getCanonicalSeasonSlug, seasonSlugMatches } from '@/lib/utils';
import { isTournamentCompetition } from '@/data/competitionTypes';
import {
  getLeagueByIdDb,
  getSeasonsByLeagueDb,
} from '@/data/server';

function buildLeaguePageUrl(id: string, params: { season?: string; tab?: string }) {
  const searchParams = new URLSearchParams();

  if (params.season) {
    searchParams.set('season', params.season);
  }

  if (params.tab) {
    searchParams.set('tab', params.tab);
  }

  const queryString = searchParams.toString();
  return queryString ? `/competitions/${id}?${queryString}` : `/competitions/${id}`;
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ id: string }>;
}): Promise<Metadata> {
  const { id } = await params;
  const locale = await getLocale();
  const competition = await getLeagueByIdDb(id, locale);

  if (!competition) {
    return {
      title: 'Competition',
    };
  }

  return {
    title: competition.name,
    description: `${competition.name} competition overview, matches, and stats.`,
  };
}

async function LeagueSeasonHistorySection({
  leagueId,
  selectedSeasonId,
  currentSeasonLabel,
  title,
  selectSeasonLabel,
  tab,
}: {
  leagueId: string;
  selectedSeasonId?: string;
  currentSeasonLabel: string;
  title: string;
  selectSeasonLabel: string;
  tab?: string;
}) {
  const availableSeasons = await getSeasonsByLeagueDb(leagueId);

  if (availableSeasons.length <= 1) {
    return null;
  }

  const currentSeason = availableSeasons.find((season) => season.seasonLabel === currentSeasonLabel)
    ?? availableSeasons.find((season) => season.isCurrent)
    ?? availableSeasons[0];
  const activeSeasonId = selectedSeasonId ?? currentSeason?.seasonId;
  const activeSeasonLabel = availableSeasons.find((season) => season.seasonId === activeSeasonId)?.seasonLabel
    ?? currentSeason?.seasonLabel
    ?? currentSeasonLabel;

  return (
    <SectionCard
      title={title}
      className="mb-4"
      action={(
        <LeagueSeasonSelect
          leagueId={leagueId}
          seasons={availableSeasons.map((entry) => ({
            seasonId: entry.seasonId,
            seasonLabel: entry.seasonLabel,
          }))}
          selectedSeasonId={activeSeasonId}
          tab={tab}
          className="h-7 min-w-[128px] max-w-[152px] border-border-subtle bg-surface-1 px-2 py-1 pr-7 text-[11px] font-medium text-text-secondary hover:text-text-primary"
        />
      )}
    >
      <div className="flex items-center gap-2 text-[10px] text-text-muted">
        <span className="font-semibold uppercase tracking-wider text-text-muted">
          {selectSeasonLabel}
        </span>
        <span className="rounded-sm border border-border-subtle/70 bg-surface-2/55 px-1.5 py-0.5 text-[10px] font-medium tracking-wide text-text-secondary">
          {activeSeasonLabel}
        </span>
      </div>
    </SectionCard>
  );
}

export default async function LeaguePage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ season?: string; tab?: string; resultsPage?: string; fixturesPage?: string }>;
}) {
  const { id } = await params;
  const { season, tab, resultsPage, fixturesPage } = await searchParams;
  const locale = await getLocale();
  const league = await getLeagueByIdDb(id, locale);
  if (!league) notFound();

  const availableSeasons = await getSeasonsByLeagueDb(id);
  const currentSeason = availableSeasons?.find((s) => s.seasonLabel === league.season)
    ?? availableSeasons?.find((s) => s.isCurrent)
    ?? availableSeasons?.[0];
  const matchedSeason = season && availableSeasons
    ? availableSeasons.find((entry) => seasonSlugMatches(entry.seasonId, season))
    : undefined;
  const selectedSeason = matchedSeason ?? currentSeason;
  const canonicalSeasonParam = matchedSeason ? getCanonicalSeasonSlug(matchedSeason.seasonId) : undefined;

  if (season && canonicalSeasonParam && season !== canonicalSeasonParam) {
    permanentRedirect(buildLeaguePageUrl(id, { season: canonicalSeasonParam, tab }));
  }

  const isNonDefaultSeason = selectedSeason && selectedSeason.seasonId !== currentSeason?.seasonId;

  const [tLeague, tCommon] = await Promise.all([
    getTranslations('league'),
    getTranslations('common'),
  ]);
  const detailTabs = [
    { key: 'overview', label: tCommon('tabOverview') },
    { key: 'matches', label: tCommon('tabMatches') },
    { key: 'stats', label: tCommon('tabStats') },
  ] as const;
  const activeTab = (tab && detailTabs.some((entry) => entry.key === tab) ? tab : 'overview') as 'overview' | 'matches' | 'stats';
  const isTournament = isTournamentCompetition(league);
  const competitionFormat = selectedSeason?.competitionFormat ?? league.competitionFormat;
  const isKnockoutOnlyTournament = isTournament && competitionFormat === 'knockout';
  const formatLabel = isTournament ? tLeague('formatTournament') : tLeague('formatLeague');
  const formatDetail = isTournament
    ? competitionFormat === 'league_phase'
      ? tLeague('formatChampionsLeagueLeaguePhaseDetail')
      : isKnockoutOnlyTournament
        ? tLeague('formatTournamentKnockoutOnlyDetail')
        : competitionFormat === 'group_knockout'
          ? tLeague('formatTournamentGroupStageDetail')
          : tLeague('formatTournamentDetail')
    : tLeague('formatLeagueDetail');

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
                <LeagueLogo leagueId={league.id} name={league.name} competitionType={league.competitionType} logo={league.logo} size="lg" />
            <span>{league.name}</span>
            <Badge variant={isTournament ? 'info' : 'default'}>{formatLabel}</Badge>
          </div>
        )}
        subtitle={`${league.country} · ${tLeague('season')} ${selectedSeason?.seasonLabel ?? league.season}`}
        meta={isTournament ? formatDetail : `${league.numberOfClubs} ${tLeague('clubs')}`}
      />

      <StatPanel
        stats={[
          { label: tLeague('country'), value: league.country },
          { label: tLeague('season'), value: selectedSeason?.seasonLabel ?? league.season },
          { label: tLeague('format'), value: formatLabel },
          { label: tLeague('clubs'), value: league.numberOfClubs },
        ]}
        columns={4}
        className="mb-4"
      />

      <LeagueSeasonHistorySection
        leagueId={id}
        selectedSeasonId={selectedSeason?.seasonId}
        currentSeasonLabel={league.season}
        title={tLeague('seasonHistory')}
        selectSeasonLabel={tLeague('selectSeason')}
        tab={activeTab}
      />

      <LeagueDetailSections
        league={league}
        locale={locale}
        selectedSeason={selectedSeason}
        isNonDefaultSeason={Boolean(isNonDefaultSeason)}
        isTournament={isTournament}
        seasonQueryParam={isNonDefaultSeason && selectedSeason ? getCanonicalSeasonSlug(selectedSeason.seasonId) : undefined}
        resultsPage={Number(resultsPage) || 1}
        fixturesPage={Number(fixturesPage) || 1}
        initialTab={activeTab}
      />
    </div>
  );
}
