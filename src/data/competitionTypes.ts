import type { League } from './types';

const TOURNAMENT_LEAGUE_IDS = new Set([
  'ucl',
  'uel',
  'cwc',
  'champions-league',
  'uefa-europa-league',
  'club-world-cup',
  'fifa-world-cup',
]);
const TOURNAMENT_KEYWORDS = [
  'champions league',
  'europa league',
  'conference league',
  'club world cup',
  'world cup',
  'euro',
  'copa',
  'coppa',
  'coupe',
  'super cup',
  'african cup of nations',
  'nations cup',
  'cup',
  'pokal',
  'knockout',
  'trophy',
];

export function deriveCompetitionType(id: string, name?: string): League['competitionType'] {
  if (TOURNAMENT_LEAGUE_IDS.has(id)) {
    return 'tournament';
  }

  const normalized = `${id} ${name ?? ''}`.toLowerCase().replace(/[-_]+/g, ' ');
  return TOURNAMENT_KEYWORDS.some((keyword) => normalized.includes(keyword)) ? 'tournament' : 'league';
}

export function isTournamentCompetition(league: Pick<League, 'competitionType'>) {
  return league.competitionType === 'tournament';
}

const KNOCKOUT_ONLY_COMPETITION_IDS = new Set([
  'fa-cup',
  'copa-del-rey',
  'coppa-italia',
  'dfb-pokal',
  'coupe-de-france',
]);

function getSeasonStartYear(seasonLabel?: string) {
  const match = seasonLabel?.match(/\d{4}/);
  return match ? Number.parseInt(match[0], 10) : Number.NaN;
}

export type CompetitionFormatDetailKey =
  | 'formatLeagueDetail'
  | 'formatTournamentDetail'
  | 'formatTournamentGroupStageDetail'
  | 'formatTournamentKnockoutOnlyDetail';

export function getCompetitionFormatDetailKey(league: Pick<League, 'id' | 'season' | 'competitionType'>): CompetitionFormatDetailKey {
  if (league.competitionType === 'league') {
    return 'formatLeagueDetail';
  }

  if (KNOCKOUT_ONLY_COMPETITION_IDS.has(league.id)) {
    return 'formatTournamentKnockoutOnlyDetail';
  }

  if (league.id === 'champions-league' || league.id === 'europa-league') {
    const startYear = getSeasonStartYear(league.season);
    return Number.isFinite(startYear) && startYear >= 2024
      ? 'formatTournamentDetail'
      : 'formatTournamentGroupStageDetail';
  }

  return 'formatTournamentDetail';
}
