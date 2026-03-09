/**
 * Data Access Layer
 *
 * All data access goes through this module.
 * Currently returns mock data — swap implementations to connect a real API.
 * No consumer changes needed.
 */

import type { League, Club, Player, Nation, Match, StandingRow, StatLeader, SearchResult, WorldCupTournament } from './types';
import { leagues } from './leagues';
import { clubs } from './clubs';
import { players } from './players';
import { nations } from './nations';
import { matches } from './matches';
import { standings } from './standings';
import { worldCup2026 } from './worldCup';

// ═══════════════════════════════════════
// Maps for O(1) lookup
// ═══════════════════════════════════════
const leagueMap = new Map(leagues.map((l) => [l.id, l]));
const clubMap = new Map(clubs.map((c) => [c.id, c]));
const playerMap = new Map(players.map((p) => [p.id, p]));
const nationMap = new Map(nations.map((n) => [n.id, n]));

function getAllMatches() {
  return [...matches, ...worldCup2026.matches];
}

const matchMap = new Map(getAllMatches().map((m) => [m.id, m]));

// ═══════════════════════════════════════
// League queries
// ═══════════════════════════════════════
export function getLeagues(): League[] {
  return leagues;
}

export function getLeagueById(id: string): League | undefined {
  return leagueMap.get(id);
}

// ═══════════════════════════════════════
// Club queries
// ═══════════════════════════════════════
export function getClubs(): Club[] {
  return clubs;
}

export function getClubById(id: string): Club | undefined {
  return clubMap.get(id);
}

export function getClubsByLeague(leagueId: string): Club[] {
  return clubs.filter((c) => c.leagueId === leagueId);
}

export function getClubName(id: string): string {
  return clubMap.get(id)?.name ?? 'Unknown';
}

export function getClubShortName(id: string): string {
  return clubMap.get(id)?.shortName ?? '???';
}

// ═══════════════════════════════════════
// Player queries
// ═══════════════════════════════════════
export function getPlayers(): Player[] {
  return players;
}

export function getPlayerById(id: string): Player | undefined {
  return playerMap.get(id);
}

export function getPlayersByClub(clubId: string): Player[] {
  return players.filter((p) => p.clubId === clubId);
}

export function getPlayersByNation(nationId: string): Player[] {
  return players.filter((p) => p.nationId === nationId);
}

export function getPlayerName(id: string): string {
  return playerMap.get(id)?.name ?? 'Unknown';
}

// ═══════════════════════════════════════
// Nation queries
// ═══════════════════════════════════════
export function getNations(): Nation[] {
  return nations;
}

export function getNationById(id: string): Nation | undefined {
  return nationMap.get(id);
}

// ═══════════════════════════════════════
// Match queries
// ═══════════════════════════════════════
export function getMatches(): Match[] {
  return getAllMatches();
}

export function getMatchById(id: string): Match | undefined {
  return matchMap.get(id);
}

export function getMatchesByLeague(leagueId: string): Match[] {
  return getAllMatches().filter((m) => m.leagueId === leagueId);
}

export function getFinishedMatches(): Match[] {
  return getAllMatches()
    .filter((m) => m.status === 'finished')
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
}

export function getScheduledMatches(): Match[] {
  return getAllMatches()
    .filter((m) => m.status === 'scheduled')
    .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
}

export function getMatchesByClub(clubId: string): Match[] {
  return getAllMatches().filter((m) => m.homeTeamId === clubId || m.awayTeamId === clubId);
}

export function getMatchesByNation(nationId: string): Match[] {
  return getAllMatches().filter(
    (m) => m.teamType === 'nation' && (m.homeTeamId === nationId || m.awayTeamId === nationId)
  );
}

export function getFinishedMatchesByLeague(leagueId: string): Match[] {
  return matches
    .filter((m) => m.leagueId === leagueId && m.status === 'finished')
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
}

export function getScheduledMatchesByLeague(leagueId: string): Match[] {
  return matches
    .filter((m) => m.leagueId === leagueId && m.status === 'scheduled')
    .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
}

export function getFinishedMatchesByNation(nationId: string): Match[] {
  return getMatchesByNation(nationId)
    .filter((m) => m.status === 'finished')
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
}

export function getScheduledMatchesByNation(nationId: string): Match[] {
  return getMatchesByNation(nationId)
    .filter((m) => m.status === 'scheduled')
    .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
}

export function getWorldCup2026(): WorldCupTournament {
  return worldCup2026;
}

// ═══════════════════════════════════════
// Standings queries
// ═══════════════════════════════════════
export function getStandingsByLeague(leagueId: string): StandingRow[] {
  return standings[leagueId] ?? [];
}

// ═══════════════════════════════════════
// Top scorers / stat leaders
// ═══════════════════════════════════════
export function getTopScorers(leagueId: string, limit: number = 10): StatLeader[] {
  const leagueClubIds = new Set(clubs.filter((c) => c.leagueId === leagueId).map((c) => c.id));

  return players
    .filter((p) => leagueClubIds.has(p.clubId))
    .sort((a, b) => b.seasonStats.goals - a.seasonStats.goals)
    .slice(0, limit)
    .map((p) => ({
      playerId: p.id,
      clubId: p.clubId,
      leagueId,
      goals: p.seasonStats.goals,
      assists: p.seasonStats.assists,
    }));
}

// ═══════════════════════════════════════
// Search
// ═══════════════════════════════════════
export function searchAll(query: string): SearchResult[] {
  const q = query.toLowerCase().trim();
  if (!q) return [];

  const results: SearchResult[] = [];

  // Search players
  for (const p of players) {
    if (p.name.toLowerCase().includes(q)) {
      const club = clubMap.get(p.clubId);
      results.push({
        type: 'player',
        id: p.id,
        name: p.name,
        subtitle: `${club?.name ?? 'Free Agent'} · ${p.position}`,
      });
    }
  }

  // Search clubs
  for (const c of clubs) {
    if (c.name.toLowerCase().includes(q) || c.shortName.toLowerCase().includes(q)) {
      const league = leagueMap.get(c.leagueId);
      results.push({
        type: 'club',
        id: c.id,
        name: c.name,
        subtitle: `${league?.name ?? 'Unknown'} · ${c.country}`,
      });
    }
  }

  // Search leagues
  for (const l of leagues) {
    if (l.name.toLowerCase().includes(q)) {
      results.push({
        type: 'league',
        id: l.id,
        name: l.name,
        subtitle: `${l.country} · ${l.season}`,
      });
    }
  }

  // Search nations
  for (const n of nations) {
    if (n.name.toLowerCase().includes(q) || n.code.toLowerCase().includes(q)) {
      results.push({
        type: 'nation',
        id: n.id,
        name: n.name,
        subtitle: `${n.confederation} · FIFA #${n.fifaRanking}`,
      });
    }
  }

  return results;
}
