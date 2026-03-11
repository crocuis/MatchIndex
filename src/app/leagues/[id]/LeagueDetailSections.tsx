import Link from 'next/link';
import { getTranslations } from 'next-intl/server';
import { FixtureCard } from '@/components/data/FixtureCard';
import { MatchCard } from '@/components/data/MatchCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { SectionCard } from '@/components/ui/SectionCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import type { League } from '@/data/types';
import {
  buildLeaguePhaseStandings,
  buildKnockoutStages,
  buildLeaguePhaseMatchdays,
  buildQualifyingStages,
  buildTournamentGroups,
  getChampionsLeagueFormat,
  getTournamentChampion,
} from '@/app/leagues/[id]/tournamentView';
import {
  getClubsByLeagueAndSeasonDb,
  getClubsByLeagueDb,
  getMatchesByLeagueAndSeasonDb,
  getMatchesByLeagueDb,
  getStandingsByLeagueAndSeasonDb,
  getStandingsByLeagueDb,
  getTopScorerRowsBySeasonDb,
  getTopScorerRowsDb,
} from '@/data/server';

interface SelectedSeasonValue {
  seasonId: string;
  seasonLabel: string;
}

interface LeagueDetailSectionsProps {
  league: League;
  locale: string;
  selectedSeason?: SelectedSeasonValue;
  isNonDefaultSeason: boolean;
  isTournament: boolean;
}

export async function LeagueDetailSections({
  league,
  locale,
  selectedSeason,
  isNonDefaultSeason,
  isTournament,
}: LeagueDetailSectionsProps) {
  const [tLeague, tTable, standings, clubs, allMatches, topScorerRows] = await Promise.all([
    getTranslations('league'),
    getTranslations('table'),
    isNonDefaultSeason && selectedSeason
      ? getStandingsByLeagueAndSeasonDb(league.id, selectedSeason.seasonId, locale)
      : getStandingsByLeagueDb(league.id, locale),
    isNonDefaultSeason && selectedSeason
      ? getClubsByLeagueAndSeasonDb(league.id, selectedSeason.seasonId, locale)
      : getClubsByLeagueDb(league.id, locale),
    isNonDefaultSeason && selectedSeason
      ? getMatchesByLeagueAndSeasonDb(league.id, selectedSeason.seasonId, locale)
      : getMatchesByLeagueDb(league.id, locale),
    isNonDefaultSeason && selectedSeason
      ? getTopScorerRowsBySeasonDb(league.id, selectedSeason.seasonId, locale, 10)
      : getTopScorerRowsDb(league.id, locale, 10),
  ]);

  const finishedMatches = allMatches.filter((match) => match.status === 'finished');
  const scheduledMatches = allMatches.filter((match) => match.status === 'scheduled');
  const recentResults = finishedMatches.slice(0, 10);
  const upcomingFixtures = scheduledMatches.slice(0, 10);
  const championsLeagueFormat = league.id === 'champions-league'
    ? getChampionsLeagueFormat(selectedSeason?.seasonLabel ?? league.season)
    : undefined;
  const standingsTitle = isTournament
    ? championsLeagueFormat === 'legacy'
      ? tLeague('groupStageSnapshotLegacy')
      : tLeague('groupStageSnapshot')
    : tLeague('standings');
  const clubsTitle = isTournament ? tLeague('participants') : tLeague('clubsList');
  const resultsTitle = isTournament ? tLeague('tournamentResults') : tLeague('recentResults');
  const fixturesTitle = isTournament ? tLeague('tournamentFixtures') : tLeague('upcomingFixtures');
  const topScorersTitle = isTournament ? tLeague('topPerformers') : tLeague('topScorers');
  const participantRows = clubs.slice(0, 8);
  const tournamentGroups = isTournament && championsLeagueFormat !== 'league-phase' ? buildTournamentGroups(allMatches, clubs) : [];
  const qualifyingStages = isTournament && championsLeagueFormat === 'legacy' ? buildQualifyingStages(allMatches) : [];
  const leaguePhaseStages = isTournament && championsLeagueFormat === 'league-phase' ? buildLeaguePhaseMatchdays(allMatches) : [];
  const leaguePhaseStandings = isTournament && championsLeagueFormat === 'league-phase'
    ? buildLeaguePhaseStandings(allMatches, clubs)
    : standings;
  const knockoutStages = isTournament ? buildKnockoutStages(allMatches) : [];
  const champion = isTournament ? await getTournamentChampion(knockoutStages, locale) : undefined;
  const formatDetail = championsLeagueFormat === 'legacy'
    ? tLeague('formatChampionsLeagueLegacyDetail')
    : championsLeagueFormat === 'league-phase'
      ? tLeague('formatChampionsLeagueLeaguePhaseDetail')
      : tLeague('formatTournamentDetail');
  const trackingMode = championsLeagueFormat === 'legacy'
    ? tLeague('trackingModeChampionsLeagueLegacy')
    : championsLeagueFormat === 'league-phase'
      ? tLeague('trackingModeChampionsLeagueLeaguePhase')
      : tLeague('trackingModeTournament');
  const advancingRule = championsLeagueFormat === 'legacy'
    ? tLeague('advancingRuleLegacy')
    : championsLeagueFormat === 'league-phase'
      ? tLeague('advancingRuleLeaguePhase')
      : tLeague('advancingRule');

  return (
    <div className="grid grid-cols-12 gap-4">
      {isTournament ? (
        <>
          <div className="col-span-8 space-y-4">
            <SectionCard title={tLeague('competitionOverview')}>
              <div className="grid grid-cols-3 gap-3 text-[12px] text-text-secondary">
                <div>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{tLeague('format')}</div>
                  <div className="text-[13px] font-medium text-text-primary">{formatDetail}</div>
                </div>
                <div>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{tLeague('participants')}</div>
                  <div className="text-[13px] font-medium text-text-primary">{league.numberOfClubs}</div>
                </div>
                <div>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{tLeague('trackingMode')}</div>
                  <div className="text-[13px] font-medium text-text-primary">{trackingMode}</div>
                </div>
                {champion ? (
                  <div>
                    <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-text-muted">{tLeague('champion')}</div>
                    <div className="text-[13px] font-medium text-text-primary">{champion}</div>
                  </div>
                ) : null}
              </div>
            </SectionCard>

            {qualifyingStages.length > 0 ? (
              <SectionCard title={tLeague('qualifyingRounds')}>
                <div className="space-y-4">
                  {qualifyingStages.map((stage) => (
                    <div key={stage.id}>
                      <div className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-text-muted">{stage.name}</div>
                      <div className="space-y-1.5">
                        {stage.matches.map((match) => (
                          match.status === 'finished'
                            ? <MatchCard key={match.id} match={match} />
                            : <FixtureCard key={match.id} match={match} />
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </SectionCard>
            ) : null}

            {tournamentGroups.length > 0 ? (
              <SectionCard title={tLeague('groupStage')}>
                <div className="mb-3 text-[12px] text-text-secondary">{advancingRule}</div>
                <div className="grid grid-cols-2 gap-4">
                  {tournamentGroups.map((group) => (
                    <div key={group.id} className="overflow-hidden rounded border border-border-subtle bg-surface-2">
                      <div className="border-b border-border-subtle px-3 py-2 text-[11px] font-semibold uppercase tracking-wider text-text-primary">
                        {group.name}
                      </div>
                      <StandingsTable standings={group.standings} compact />
                    </div>
                  ))}
                </div>
              </SectionCard>
            ) : null}

            {leaguePhaseStages.length > 0 ? (
              <SectionCard title={tLeague('leaguePhase')}>
                <div className="mb-3 text-[12px] text-text-secondary">{advancingRule}</div>
                <div className="space-y-4">
                  {leaguePhaseStages.map((stage) => (
                    <div key={stage.id}>
                      <div className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-text-muted">{stage.name}</div>
                      <div className="space-y-1.5">
                        {stage.matches.map((match) => (
                          match.status === 'finished'
                            ? <MatchCard key={match.id} match={match} />
                            : <FixtureCard key={match.id} match={match} />
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </SectionCard>
            ) : null}

            {knockoutStages.length > 0 ? (
              <SectionCard title={tLeague('knockoutRounds')}>
                <div className="space-y-4">
                  {knockoutStages.map((stage) => (
                    <div key={stage.name}>
                      <div className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-text-muted">{stage.name}</div>
                      <div className="space-y-1.5">
                        {stage.matches.map((match) => (
                          match.status === 'finished'
                            ? <MatchCard key={match.id} match={match} />
                            : <FixtureCard key={match.id} match={match} />
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </SectionCard>
            ) : null}

            <SectionCard title={resultsTitle}>
              <div className="space-y-1.5">
                {recentResults.map((m) => <MatchCard key={m.id} match={m} />)}
              </div>
            </SectionCard>

            <SectionCard title={fixturesTitle}>
              <div className="space-y-1.5">
                {upcomingFixtures.map((m) => <FixtureCard key={m.id} match={m} />)}
              </div>
            </SectionCard>
          </div>

          <div className="col-span-4 space-y-4">
            {tournamentGroups.length === 0 ? (
              <SectionCard title={standingsTitle} noPadding>
                <StandingsTable standings={leaguePhaseStandings} compact />
              </SectionCard>
            ) : null}

            <SectionCard title={clubsTitle}>
              <div className="space-y-2">
                {participantRows.map((club) => (
                  <Link key={club.id} href={`/clubs/${club.id}`} className="flex items-center gap-3 rounded border border-border-subtle bg-surface-2 px-3 py-2 transition-colors hover:bg-surface-3">
                    <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="sm" />
                    <div className="min-w-0">
                      <div className="truncate text-[13px] font-medium text-text-primary">{club.name}</div>
                      <div className="truncate text-[11px] text-text-muted">{club.stadium}</div>
                    </div>
                  </Link>
                ))}
              </div>
            </SectionCard>

            <SectionCard title={topScorersTitle} noPadding>
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="px-3 py-1.5 text-left">{tTable('rank')}</th>
                    <th className="px-3 py-1.5 text-left">{tTable('player')}</th>
                    <th className="px-3 py-1.5 text-center">{tTable('goals')}</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border-subtle">
                  {topScorerRows.map((s, i) => (
                    <tr key={s.playerId} className="hover:bg-surface-2">
                      <td className="px-3 py-1.5 text-[13px] text-text-muted tabular-nums">{i + 1}</td>
                      <td className="px-3 py-1.5 text-[13px]">
                        <div className="flex flex-col gap-0.5">
                          <EntityLink type="player" id={s.playerId}>{s.playerName}</EntityLink>
                          <div className="flex items-center gap-1.5 text-[11px] text-text-muted">
                            <ClubBadge shortName={s.clubShortName} clubId={s.clubId} size="sm" showText={false} />
                            <span>{s.clubShortName}</span>
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-1.5 text-center text-[13px] font-semibold tabular-nums">{s.goals}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </SectionCard>
          </div>
        </>
      ) : (
        <>
          <div className="col-span-8 space-y-4">
            <SectionCard title={standingsTitle} noPadding>
              <StandingsTable standings={standings} />
            </SectionCard>

            <SectionCard title={clubsTitle}>
              <div className="grid grid-cols-2 gap-2">
                {clubs.map((club) => (
                  <Link key={club.id} href={`/clubs/${club.id}`} className="flex items-center gap-3 rounded border border-border-subtle bg-surface-2 px-3 py-2 transition-colors hover:bg-surface-3">
                    <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="lg" />
                    <div>
                      <div className="text-[13px] font-medium text-text-primary">{club.name}</div>
                      <div className="text-[11px] text-text-muted">{club.stadium}</div>
                    </div>
                  </Link>
                ))}
              </div>
            </SectionCard>
          </div>

          <div className="col-span-4 space-y-4">
            <SectionCard title={resultsTitle}>
              <div className="space-y-1.5">
                {recentResults.map((m) => <MatchCard key={m.id} match={m} />)}
              </div>
            </SectionCard>

            <SectionCard title={fixturesTitle}>
              <div className="space-y-1.5">
                {upcomingFixtures.map((m) => <FixtureCard key={m.id} match={m} />)}
              </div>
            </SectionCard>

            <SectionCard title={topScorersTitle} noPadding>
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="px-3 py-1.5 text-left">{tTable('rank')}</th>
                    <th className="px-3 py-1.5 text-left">{tTable('player')}</th>
                    <th className="px-3 py-1.5 text-center">{tTable('club')}</th>
                    <th className="px-3 py-1.5 text-center">{tTable('goals')}</th>
                    <th className="px-3 py-1.5 text-center">{tTable('assists')}</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border-subtle">
                  {topScorerRows.map((s, i) => (
                    <tr key={s.playerId} className="hover:bg-surface-2">
                      <td className="px-3 py-1.5 text-[13px] text-text-muted tabular-nums">{i + 1}</td>
                      <td className="px-3 py-1.5 text-[13px]"><EntityLink type="player" id={s.playerId}>{s.playerName}</EntityLink></td>
                      <td className="px-3 py-1.5 text-[13px] text-center text-text-secondary">
                        <div className="flex items-center justify-center gap-2">
                          <ClubBadge shortName={s.clubShortName} clubId={s.clubId} size="sm" showText={false} />
                          <span>{s.clubShortName}</span>
                        </div>
                      </td>
                      <td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">{s.goals}</td>
                      <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">{s.assists}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </SectionCard>
          </div>
        </>
      )}
    </div>
  );
}
