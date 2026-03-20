import type { Match, Nation, WorldCupTournament } from '@/data/types';
import { getMatchesByIdsDb } from '@/data/server';

export function buildWorldCupNationRefs(tournament: WorldCupTournament) {
  const refs = new Map<string, string>();

  for (const group of tournament.groups) {
    for (const row of group.standings) {
      refs.set(row.nationId, row.nationCode ?? '');
    }
  }

  for (const match of tournament.matches) {
    if (match.teamType !== 'nation') {
      continue;
    }

    refs.set(match.homeTeamId, match.homeTeamCode ?? '');
    refs.set(match.awayTeamId, match.awayTeamCode ?? '');
  }

  return refs;
}

export function createWorldCupNationResolver(nations: Nation[], tournament: WorldCupTournament) {
  const worldCupNationRefs = buildWorldCupNationRefs(tournament);
  const nationIdMap = new Map(nations.map((nation) => [nation.id, nation]));
  const nationCodeMap = new Map(nations.map((nation) => [nation.code.toUpperCase(), nation]));

  return (nationId: string) => nationIdMap.get(nationId) ?? nationCodeMap.get(worldCupNationRefs.get(nationId)?.toUpperCase() ?? '');
}

export async function getResolvedWorldCupMatches(tournament: WorldCupTournament, locale: string): Promise<Match[]> {
  const allMatchIds = new Set([
    ...tournament.matches.map((match) => match.id),
    ...tournament.stages.flatMap((stage) => stage.matchIds),
  ]);
  const tournamentMatchMap = new Map(tournament.matches.map((match) => [match.id, match]));
  const missingMatchIds = Array.from(allMatchIds).filter((id) => !tournamentMatchMap.has(id));
  const missingMatches = missingMatchIds.length > 0
    ? await getMatchesByIdsDb(missingMatchIds, locale)
    : [];

  return [
    ...tournament.matches,
    ...missingMatches,
  ];
}
