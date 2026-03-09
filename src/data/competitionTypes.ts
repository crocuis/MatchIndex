import type { League } from './types';

const TOURNAMENT_LEAGUE_IDS = new Set(['ucl', 'uel', 'cwc']);
const TOURNAMENT_KEYWORDS = [
  'champions league',
  'europa league',
  'conference league',
  'club world cup',
  'world cup',
  'euro',
  'copa',
  'super cup',
  'african cup of nations',
  'nations cup',
  'cup',
  'knockout',
  'trophy',
];

export function deriveCompetitionType(id: string, name?: string): League['competitionType'] {
  if (TOURNAMENT_LEAGUE_IDS.has(id)) {
    return 'tournament';
  }

  const normalized = `${id} ${name ?? ''}`.toLowerCase();
  return TOURNAMENT_KEYWORDS.some((keyword) => normalized.includes(keyword)) ? 'tournament' : 'league';
}

export function isTournamentCompetition(league: Pick<League, 'competitionType'>) {
  return league.competitionType === 'tournament';
}
