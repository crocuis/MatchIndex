import { getTranslations } from 'next-intl/server';
import { MatchArchiveSplitList } from '@/components/data/MatchArchiveSplitList';
import { StandingsTable } from '@/components/data/StandingsTable';
import { SectionCard } from '@/components/ui/SectionCard';
import { TabGroup } from '@/components/ui/TabGroup';
import {
  buildGroupStageMatches,
  buildKnockoutStages,
  buildLeaguePhaseMatchdays,
  buildQualifyingStages,
  getEuropeanCompetitionFormat,
} from '@/app/competitions/[id]/tournamentView';
import { isTournamentCompetition } from '@/data/competitionTypes';
import { getLeagueByIdDb, getMatchesByClubAndSeasonDb, getStandingsByLeagueAndSeasonDb } from '@/data/server';

interface ClubSeasonArchiveSectionsProps {
  clubId: string;
  leagueId: string;
  seasonId: string;
  seasonLabel: string;
  locale: string;
}

export async function ClubSeasonArchiveSections({
  clubId,
  leagueId,
  seasonId,
  seasonLabel,
  locale,
}: ClubSeasonArchiveSectionsProps) {
  const [tClub, tLeague, league, seasonMatches] = await Promise.all([
    getTranslations('club'),
    getTranslations('league'),
    getLeagueByIdDb(leagueId, locale),
    getMatchesByClubAndSeasonDb(clubId, seasonId, leagueId, locale),
  ]);

  const isTournament = league ? isTournamentCompetition(league) : false;

  if (isTournament) {
    const europeanCompetitionFormat = getEuropeanCompetitionFormat(leagueId, seasonLabel);
    const qualifyingStages = europeanCompetitionFormat === 'legacy' ? buildQualifyingStages(seasonMatches) : [];
    const leaguePhaseStages = europeanCompetitionFormat === 'league-phase' ? buildLeaguePhaseMatchdays(seasonMatches) : [];
    const legacyGroupStageMatches = europeanCompetitionFormat === 'legacy' ? buildGroupStageMatches(seasonMatches) : undefined;
    const knockoutStages = buildKnockoutStages(seasonMatches);
    const stageTabs = [
      ...knockoutStages.slice().reverse().map((stage) => ({
        key: `knockout-${stage.name}`,
        label: stage.name,
        content: (
          <MatchArchiveSplitList
            matches={stage.matches}
            locale={locale}
            recentResultsLabel={tLeague('recentResults')}
            upcomingFixturesLabel={tLeague('upcomingFixtures')}
            emptyLabel={tClub('seasonMatchesEmpty')}
          />
        ),
      })),
      ...(europeanCompetitionFormat === 'league-phase'
        ? leaguePhaseStages.slice().reverse().map((stage) => ({
            key: stage.id,
            label: stage.name,
            content: (
              <MatchArchiveSplitList
                matches={stage.matches}
                locale={locale}
                recentResultsLabel={tLeague('recentResults')}
                upcomingFixturesLabel={tLeague('upcomingFixtures')}
                emptyLabel={tClub('seasonMatchesEmpty')}
              />
            ),
          }))
        : legacyGroupStageMatches
          ? [{
              key: legacyGroupStageMatches.id,
              label: tLeague('groupStage'),
              content: (
                <MatchArchiveSplitList
                  matches={legacyGroupStageMatches.matches}
                  locale={locale}
                  recentResultsLabel={tLeague('recentResults')}
                  upcomingFixturesLabel={tLeague('upcomingFixtures')}
                  emptyLabel={tClub('seasonMatchesEmpty')}
                />
              ),
            }]
          : []),
      ...qualifyingStages.map((stage) => ({
        key: stage.id,
        label: stage.name,
        content: (
          <MatchArchiveSplitList
            matches={stage.matches}
            locale={locale}
            recentResultsLabel={tLeague('recentResults')}
            upcomingFixturesLabel={tLeague('upcomingFixtures')}
            emptyLabel={tClub('seasonMatchesEmpty')}
          />
        ),
      })),
    ];

    return (
      <SectionCard title={`${tLeague('stageMatches')} · ${seasonLabel}`}>
        {stageTabs.length > 0 ? (
          <TabGroup tabs={stageTabs} defaultTab={stageTabs[0]?.key} />
        ) : (
          <MatchArchiveSplitList
            matches={seasonMatches}
            locale={locale}
            recentResultsLabel={tLeague('recentResults')}
            upcomingFixturesLabel={tLeague('upcomingFixtures')}
            emptyLabel={tClub('seasonMatchesEmpty')}
          />
        )}
      </SectionCard>
    );
  }

  const standings = await getStandingsByLeagueAndSeasonDb(leagueId, seasonId, locale);

  return (
    <>
      <SectionCard title={`${tClub('seasonStandings')} · ${seasonLabel}`} noPadding>
        <StandingsTable standings={standings} />
      </SectionCard>

      <SectionCard title={`${tClub('seasonMatchArchive')} · ${seasonLabel}`}>
        <MatchArchiveSplitList
          matches={seasonMatches}
          locale={locale}
          recentResultsLabel={tLeague('recentResults')}
          upcomingFixturesLabel={tLeague('upcomingFixtures')}
          emptyLabel={tClub('seasonMatchesEmpty')}
        />
      </SectionCard>
    </>
  );
}
