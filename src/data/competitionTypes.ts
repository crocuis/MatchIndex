import type { CompetitionDataType, League } from './types';

export function isTournamentCompetition(league: Pick<League, 'competitionType'>) {
  return league.competitionType === 'tournament';
}

const KNOCKOUT_ONLY_COMPETITION_DATA_TYPES = new Set<CompetitionDataType>(['cup', 'super_cup']);

export type CompetitionFormatDetailKey =
  | 'formatLeagueDetail'
  | 'formatTournamentDetail'
  | 'formatTournamentGroupStageDetail'
  | 'formatTournamentKnockoutOnlyDetail';

export function getCompetitionFormatDetailKey(league: Pick<League, 'competitionType' | 'competitionDataType' | 'competitionFormat'>): CompetitionFormatDetailKey {
  if (!isTournamentCompetition(league)) {
    return 'formatLeagueDetail';
  }

  if (league.competitionFormat === 'group_knockout') {
    return 'formatTournamentGroupStageDetail';
  }

  if (league.competitionFormat === 'league_phase') {
    return 'formatTournamentDetail';
  }

  if (league.competitionFormat === 'knockout') {
    return 'formatTournamentKnockoutOnlyDetail';
  }

  if (league.competitionDataType && KNOCKOUT_ONLY_COMPETITION_DATA_TYPES.has(league.competitionDataType)) {
    return 'formatTournamentKnockoutOnlyDetail';
  }

  return 'formatTournamentDetail';
}
