import { notFound, permanentRedirect } from 'next/navigation';
import { Suspense } from 'react';
import { getLocale, getTranslations } from 'next-intl/server';
import { ClubTabContent } from '@/app/clubs/[id]/ClubTabContent';
import { PageHeader } from '@/components/layout/PageHeader';
import { DetailTabSkeleton } from '@/components/ui/DetailTabSkeleton';
import { SectionCard } from '@/components/ui/SectionCard';
import { DetailTabNav } from '@/components/ui/DetailTabNav';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { formatNumber, getCanonicalSeasonSlug, getClubDisplayName, seasonSlugMatches } from '@/lib/utils';
import {
  getCanonicalClubSlugDb,
  getClubByIdDb,
  getClubSeasonHistoryDb,
  getLeagueByIdDb,
} from '@/data/server';

function buildClubPageUrl(id: string, params: { season?: string; competition?: string; tab?: string }) {
  const searchParams = new URLSearchParams();

  if (params.season) {
    searchParams.set('season', params.season);
  }

  if (params.competition) {
    searchParams.set('competition', params.competition);
  }

  if (params.tab) {
    searchParams.set('tab', params.tab);
  }

  const queryString = searchParams.toString();
  return queryString ? `/clubs/${id}?${queryString}` : `/clubs/${id}`;
}

export default async function ClubPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ season?: string; competition?: string; tab?: string }>;
}) {
  const { id } = await params;
  const { season, competition, tab } = await searchParams;
  const activeTab = tab === 'squad' || tab === 'archive' ? tab : 'overview';
  const locale = await getLocale();
  const canonicalClubId = await getCanonicalClubSlugDb(id) ?? id;
  const club = await getClubByIdDb(canonicalClubId, locale);
  if (!club) notFound();

  const [league, seasonHistory] = await Promise.all([
    getLeagueByIdDb(club.leagueId, locale),
    getClubSeasonHistoryDb(club.id, locale),
  ]);

  const preferredCompetitionId = competition ?? club.leagueId;

  const defaultSeason = seasonHistory.find((entry) => entry.leagueId === preferredCompetitionId && (entry.played > 0 || entry.position !== undefined))
    ?? seasonHistory.find((entry) => entry.leagueId === preferredCompetitionId)
    ?? seasonHistory.find((entry) => entry.played > 0 || entry.position !== undefined)
    ?? seasonHistory[0];
  const matchedSeason = season ? seasonHistory.find((entry) => {
    if (!seasonSlugMatches(entry.seasonId, season)) {
      return false;
    }

    return competition ? entry.leagueId === competition : true;
  }) : undefined;
  const selectedSeason = matchedSeason ?? defaultSeason;
  const canonicalSeasonParam = matchedSeason ? getCanonicalSeasonSlug(matchedSeason.seasonId) : undefined;

  if (id !== club.id || (season && canonicalSeasonParam && season !== canonicalSeasonParam)) {
    permanentRedirect(buildClubPageUrl(club.id, {
      season: canonicalSeasonParam ?? season,
      competition,
      tab,
    }));
  }

  const selectedArchiveSeasonHref = selectedSeason
    ? buildClubPageUrl(club.id, {
        season: getCanonicalSeasonSlug(selectedSeason.seasonId),
        competition: selectedSeason.leagueId,
        tab: 'archive',
      })
    : undefined;
  const tClub = await getTranslations('club');
  const tCommon = await getTranslations('common');
  const detailTabs = [
    { key: 'overview', label: tCommon('tabOverview') },
    { key: 'squad', label: tCommon('tabSquad') },
    { key: 'archive', label: tCommon('tabArchive') },
  ] as const;

  const clubDisplayName = getClubDisplayName(club, locale);

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="lg" />
            <span>{clubDisplayName}</span>
          </div>
        )}
        subtitle={`${selectedSeason?.leagueName ?? league?.name ?? ''} · ${club.country}`}
        meta={`Founded ${club.founded} · ${club.stadium} (${formatNumber(club.stadiumCapacity)})`}
      />

        <DetailTabNav
          activeTab={activeTab}
          basePath={`/clubs/${club.id}`}
          className="mb-4"
          query={selectedSeason ? { season: getCanonicalSeasonSlug(selectedSeason.seasonId), competition: selectedSeason.leagueId } : undefined}
          tabs={detailTabs.map((entry) => ({ ...entry }))}
        />

      <Suspense
        fallback={
          <DetailTabSkeleton
            title={detailTabs.find((entry) => entry.key === activeTab)?.label ?? tCommon('loading')}
            primaryCount={activeTab === 'overview' ? 2 : 1}
            secondaryCount={activeTab === 'archive' ? 1 : 0}
            sidebarCount={activeTab === 'overview' ? 1 : 0}
          />
        }
      >
        <ClubTabContent
          activeTab={activeTab}
          club={club}
          league={league}
          locale={locale}
          seasonHistory={seasonHistory}
          selectedArchiveSeasonHref={selectedArchiveSeasonHref}
          selectedSeason={selectedSeason}
        />
      </Suspense>
    </div>
  );
}
