const API_FOOTBALL_SOURCE_SLUG = 'api_football';
const API_FOOTBALL_SOURCE_NAME = 'API-Football v3';
const API_FOOTBALL_BASE_URL = 'https://v3.football.api-sports.io';

const DEFAULT_PLAYER_STATS_COMPETITION_TARGETS = [
  { code: 'CL', name: 'Champions League', leagueId: 2, competitionSlug: 'champions-league' },
  { code: 'PL', name: 'Premier League', leagueId: 39, competitionSlug: 'premier-league' },
  { code: 'PD', name: 'La Liga', leagueId: 140, competitionSlug: 'la-liga' },
  { code: 'BL1', name: '1. Bundesliga', leagueId: 78, competitionSlug: '1-bundesliga' },
  { code: 'SA', name: 'Serie A', leagueId: 135, competitionSlug: 'serie-a' },
  { code: 'FL1', name: 'Ligue 1', leagueId: 61, competitionSlug: 'ligue-1' },
] as const;

const DEFAULT_COMPETITION_DATA_TARGETS = [
  { code: 'CL', name: 'Champions League', leagueId: 2, competitionSlug: 'champions-league' },
  { code: 'EL', name: 'Europa League', leagueId: 3, competitionSlug: 'europa-league' },
  { code: 'PL', name: 'Premier League', leagueId: 39, competitionSlug: 'premier-league' },
  { code: 'FAC', name: 'FA Cup', leagueId: 45, competitionSlug: 'fa-cup' },
  { code: 'BL1', name: '1. Bundesliga', leagueId: 78, competitionSlug: '1-bundesliga' },
  { code: 'DFP', name: 'DFB Pokal', leagueId: 81, competitionSlug: 'dfb-pokal' },
  { code: 'FL1', name: 'Ligue 1', leagueId: 61, competitionSlug: 'ligue-1' },
  { code: 'CDF', name: 'Coupe de France', leagueId: 66, competitionSlug: 'coupe-de-france' },
  { code: 'SA', name: 'Serie A', leagueId: 135, competitionSlug: 'serie-a' },
  { code: 'CI', name: 'Coppa Italia', leagueId: 137, competitionSlug: 'coppa-italia' },
  { code: 'PD', name: 'La Liga', leagueId: 140, competitionSlug: 'la-liga' },
  { code: 'CDR', name: 'Copa del Rey', leagueId: 143, competitionSlug: 'copa-del-rey' },
] as const;

export interface ApiFootballCompetitionTarget {
  code: string;
  name: string;
  leagueId: number;
  competitionSlug: string;
}

export interface ApiFootballEnvelope<T> {
  errors?: Record<string, string>;
  results?: number;
  paging?: {
    current?: number;
    total?: number;
  };
  response?: T[];
}

export interface ApiFootballLeagueSeasonCoverage {
  fixtures?: {
    events?: boolean;
    lineups?: boolean;
    statistics_fixtures?: boolean;
    statistics_players?: boolean;
  };
  standings?: boolean;
  players?: boolean;
  top_scorers?: boolean;
}

export interface ApiFootballLeagueSeasonSummary {
  year?: number;
  start?: string;
  end?: string;
  current?: boolean;
  coverage?: ApiFootballLeagueSeasonCoverage;
}

export interface ApiFootballLeagueResponseItem {
  league?: {
    id?: number;
    name?: string;
    type?: string;
    logo?: string;
  };
  country?: {
    name?: string;
    code?: string | null;
    flag?: string | null;
  };
  seasons?: ApiFootballLeagueSeasonSummary[];
}

export interface ApiFootballFixtureResponseItem {
  fixture?: {
    id?: number;
    date?: string;
    referee?: string | null;
    timezone?: string;
    venue?: {
      id?: number | null;
      name?: string | null;
      city?: string | null;
    };
    status?: {
      long?: string;
      short?: string;
      elapsed?: number | null;
    };
  };
  league?: {
    id?: number;
    season?: number;
    round?: string | null;
  };
  teams?: {
    home?: {
      id?: number;
      name?: string;
      logo?: string;
      winner?: boolean | null;
    };
    away?: {
      id?: number;
      name?: string;
      logo?: string;
      winner?: boolean | null;
    };
  };
  goals?: {
    home?: number | null;
    away?: number | null;
  };
}

export interface ApiFootballFixtureEventResponseItem {
  time?: {
    elapsed?: number | null;
    extra?: number | null;
  };
  team?: {
    id?: number | null;
    name?: string | null;
    logo?: string | null;
  };
  player?: {
    id?: number | string | null;
    name?: string | null;
  };
  assist?: {
    id?: number | string | null;
    name?: string | null;
  };
  type?: string | null;
  detail?: string | null;
  comments?: string | null;
}

export interface ApiFootballFixtureLineupPlayer {
  id?: number | null;
  name?: string | null;
  number?: number | null;
  pos?: string | null;
  grid?: string | null;
}

export interface ApiFootballFixtureLineupResponseItem {
  team?: {
    id?: number | null;
    name?: string | null;
    logo?: string | null;
  };
  coach?: {
    id?: number | null;
    name?: string | null;
    photo?: string | null;
  };
  formation?: string | null;
  startXI?: Array<{
    player?: ApiFootballFixtureLineupPlayer;
  }>;
  substitutes?: Array<{
    player?: ApiFootballFixtureLineupPlayer;
  }>;
}

export interface ApiFootballFixtureStatistic {
  type?: string | null;
  value?: string | number | null;
}

export interface ApiFootballFixtureStatisticsResponseItem {
  team?: {
    id?: number | null;
    name?: string | null;
    logo?: string | null;
  };
  statistics?: ApiFootballFixtureStatistic[];
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
  return DEFAULT_PLAYER_STATS_COMPETITION_TARGETS.map((target) => ({ ...target }));
}

export function getDefaultApiFootballDataCompetitionTargets(): ApiFootballCompetitionTarget[] {
  return DEFAULT_COMPETITION_DATA_TARGETS.map((target) => ({ ...target }));
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

export function parseApiFootballDataCompetitionTargets(rawCodes?: string[]) {
  const defaults = getDefaultApiFootballDataCompetitionTargets();
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

export function getApiFootballRecentSeasonYears(count: number = 2) {
  const currentSeasonYear = new Date().getUTCFullYear() - 1;
  const years: number[] = [];
  for (let offset = count - 1; offset >= 0; offset -= 1) {
    years.push(currentSeasonYear - offset);
  }

  return years;
}

export function buildApiFootballPlayersPath(leagueId: number, season: number, page: number = 1) {
  return `/players?league=${leagueId}&season=${season}&page=${page}`;
}

export function buildApiFootballLeaguePath(leagueId: number) {
  return `/leagues?id=${leagueId}`;
}

export function buildApiFootballFixturesPath(leagueId: number, season: number) {
  return `/fixtures?league=${leagueId}&season=${season}`;
}

export function buildApiFootballFixturesByDatePath(date: string, timeZone?: string) {
  const params = new URLSearchParams({ date });
  if (timeZone?.trim()) {
    params.set('timezone', timeZone.trim());
  }

  return `/fixtures?${params.toString()}`;
}

export function buildApiFootballFixtureEventsPath(fixtureId: number | string) {
  return `/fixtures/events?fixture=${fixtureId}`;
}

export function buildApiFootballFixtureLineupsPath(fixtureId: number | string) {
  return `/fixtures/lineups?fixture=${fixtureId}`;
}

export function buildApiFootballFixtureStatisticsPath(fixtureId: number | string) {
  return `/fixtures/statistics?fixture=${fixtureId}`;
}

export function buildApiFootballStandingsPath(leagueId: number, season: number) {
  return `/standings?league=${leagueId}&season=${season}`;
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
