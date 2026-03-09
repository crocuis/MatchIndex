const API_FOOTBALL_SOURCE_SLUG = 'api_football';
const API_FOOTBALL_SOURCE_NAME = 'API-Football v3';
const API_FOOTBALL_BASE_URL = 'https://v3.football.api-sports.io';

const DEFAULT_COMPETITION_TARGETS = [
  { code: 'CL', name: 'Champions League', leagueId: 2, competitionSlug: 'champions-league' },
  { code: 'PL', name: 'Premier League', leagueId: 39, competitionSlug: 'premier-league' },
  { code: 'PD', name: 'La Liga', leagueId: 140, competitionSlug: 'la-liga' },
  { code: 'BL1', name: '1. Bundesliga', leagueId: 78, competitionSlug: '1-bundesliga' },
  { code: 'SA', name: 'Serie A', leagueId: 135, competitionSlug: 'serie-a' },
  { code: 'FL1', name: 'Ligue 1', leagueId: 61, competitionSlug: 'ligue-1' },
] as const;

export interface ApiFootballCompetitionTarget {
  code: string;
  name: string;
  leagueId: number;
  competitionSlug: string;
}

export interface ApiFootballPlayersResponse {
  errors?: Record<string, string>;
  results?: number;
  paging?: {
    current?: number;
    total?: number;
  };
  response?: unknown[];
}

export function getApiFootballSourceConfig() {
  return {
    slug: API_FOOTBALL_SOURCE_SLUG,
    name: API_FOOTBALL_SOURCE_NAME,
    baseUrl: process.env.API_FOOTBALL_BASE_URL?.trim() || API_FOOTBALL_BASE_URL,
    apiKey: process.env.API_FOOTBALL_KEY?.trim(),
  };
}

export function getDefaultApiFootballCompetitionTargets(): ApiFootballCompetitionTarget[] {
  return DEFAULT_COMPETITION_TARGETS.map((target) => ({ ...target }));
}

export function parseApiFootballCompetitionTargets(rawCodes?: string[]) {
  const defaults = getDefaultApiFootballCompetitionTargets();
  if (!rawCodes || rawCodes.length === 0) {
    return defaults;
  }

  const byCode = new Map(defaults.map((target) => [target.code, target]));
  const codes = [...new Set(rawCodes.map((code) => code.trim().toUpperCase()).filter(Boolean))];

  return codes.map((code) => {
    const target = byCode.get(code);
    if (!target) {
      throw new Error(`Unsupported API-Football competition code: ${code}`);
    }

    return target;
  });
}

export function buildApiFootballPlayersPath(leagueId: number, season: number, page: number = 1) {
  return `/players?league=${leagueId}&season=${season}&page=${page}`;
}

export async function fetchApiFootballJson<T>(path: string): Promise<T> {
  const { apiKey, baseUrl } = getApiFootballSourceConfig();
  if (!apiKey) {
    throw new Error('API_FOOTBALL_KEY is not set');
  }

  const response = await fetch(`${baseUrl}${path}`, {
    headers: {
      'x-apisports-key': apiKey,
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`API-Football request failed: ${response.status} ${response.statusText}`);
  }

  return response.json() as Promise<T>;
}
