import { getTranslations } from 'next-intl/server';
import { FixtureCard } from '@/components/data/FixtureCard';
import { MatchCard } from '@/components/data/MatchCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { SectionCard } from '@/components/ui/SectionCard';
import { TabGroup } from '@/components/ui/TabGroup';
import {
  buildGroupStageMatches,
  buildKnockoutStages,
  buildLeaguePhaseMatchdays,
  buildQualifyingStages,
  getChampionsLeagueFormat,
} from '@/app/leagues/[id]/tournamentView';
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
    const championsLeagueFormat = leagueId === 'champions-league'
      ? getChampionsLeagueFormat(seasonLabel)
      : undefined;
    const qualifyingStages = championsLeagueFormat === 'legacy' ? buildQualifyingStages(seasonMatches) : [];
    const leaguePhaseStages = championsLeagueFormat === 'league-phase' ? buildLeaguePhaseMatchdays(seasonMatches) : [];
    const legacyGroupStageMatches = championsLeagueFormat === 'legacy' ? buildGroupStageMatches(seasonMatches) : undefined;
    const knockoutStages = buildKnockoutStages(seasonMatches);
    const stageTabs = [
      ...knockoutStages.slice().reverse().map((stage) => ({
        key: `knockout-${stage.name}`,
        label: stage.name,
        content: (
          <div className="space-y-1.5">
            {stage.matches.map((match) => (
              match.status === 'finished'
                ? <MatchCard key={match.id} match={match} />
                : <FixtureCard key={match.id} match={match} />
            ))}
          </div>
        ),
      })),
      ...(championsLeagueFormat === 'league-phase'
        ? leaguePhaseStages.slice().reverse().map((stage) => ({
            key: stage.id,
            label: stage.name,
            content: (
              <div className="space-y-1.5">
                {stage.matches.map((match) => (
                  match.status === 'finished'
                    ? <MatchCard key={match.id} match={match} />
                    : <FixtureCard key={match.id} match={match} />
                ))}
              </div>
            ),
          }))
        : legacyGroupStageMatches
          ? [{
              key: legacyGroupStageMatches.id,
              label: tLeague('groupStage'),
              content: (
                <div className="space-y-1.5">
                  {legacyGroupStageMatches.matches.map((match) => (
                    match.status === 'finished'
                      ? <MatchCard key={match.id} match={match} />
                      : <FixtureCard key={match.id} match={match} />
                  ))}
                </div>
              ),
            }]
          : []),
      ...qualifyingStages.slice().reverse().map((stage) => ({
        key: stage.id,
        label: stage.name,
        content: (
          <div className="space-y-1.5">
            {stage.matches.map((match) => (
              match.status === 'finished'
                ? <MatchCard key={match.id} match={match} />
                : <FixtureCard key={match.id} match={match} />
            ))}
          </div>
        ),
      })),
    ];

    return (
      <SectionCard title={`${tLeague('stageMatches')} · ${seasonLabel}`}>
        {stageTabs.length > 0 ? (
          <TabGroup tabs={stageTabs} defaultTab={stageTabs[0]?.key} />
        ) : (
          <div className="space-y-1.5">
            {seasonMatches.length > 0 ? seasonMatches.map((match) => (
              match.status === 'finished'
                ? <MatchCard key={match.id} match={match} />
                : <FixtureCard key={match.id} match={match} />
            )) : <div className="text-[13px] text-text-secondary">{tClub('seasonMatchesEmpty')}</div>}
          </div>
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
        <div className="space-y-1.5">
          {seasonMatches.length > 0 ? seasonMatches.map((match) => (
            match.status === 'scheduled'
              ? <FixtureCard key={match.id} match={match} />
              : <MatchCard key={match.id} match={match} />
          )) : <div className="text-[13px] text-text-secondary">{tClub('seasonMatchesEmpty')}</div>}
        </div>
      </SectionCard>
    </>
  );
}
