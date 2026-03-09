const FOOTBALL_DATA_SOURCE_SLUG = 'football_data_org';
const FOOTBALL_DATA_SOURCE_NAME = 'football-data.org v4';
const FOOTBALL_DATA_BASE_URL = 'https://api.football-data.org/v4';

const DEFAULT_COMPETITION_CODES = ['CL', 'PD', 'PL', 'BL1', 'FL1', 'SA'] as const;

export interface FootballDataOrgCompetitionTarget {
  code: string;
  name: string;
}

export interface FootballDataOrgMatchSummary {
  id?: number;
  utcDate?: string;
  status?: string;
  matchday?: number;
  stage?: string;
  group?: string | null;
  venue?: string | null;
  homeTeam?: { id?: number; name?: string };
  awayTeam?: { id?: number; name?: string };
  score?: {
    fullTime?: {
      home?: number | null;
      away?: number | null;
    };
  };
}

export interface FootballDataOrgAreaSummary {
  id?: number;
  name?: string;
  code?: string | null;
}

export interface FootballDataOrgSeasonSummary {
  id?: number;
  startDate?: string;
  endDate?: string;
  currentMatchday?: number | null;
}

export interface FootballDataOrgCompetitionResponse {
  id?: number;
  code?: string;
  name?: string;
  type?: string;
  emblem?: string | null;
  area?: FootballDataOrgAreaSummary;
  currentSeason?: FootballDataOrgSeasonSummary;
  seasons?: FootballDataOrgSeasonSummary[];
}

export interface FootballDataOrgTeamSummary {
  id?: number;
  name?: string;
  shortName?: string;
  tla?: string;
  crest?: string | null;
  area?: FootballDataOrgAreaSummary;
}

export interface FootballDataOrgTeamsResponse {
  competition?: FootballDataOrgCompetitionResponse;
  season?: FootballDataOrgSeasonSummary;
  teams?: FootballDataOrgTeamSummary[];
}

export interface FootballDataOrgMatchesResponse {
  competition?: FootballDataOrgCompetitionResponse;
  filters?: {
    season?: string;
  };
  resultSet?: {
    count?: number;
  };
  matches?: FootballDataOrgMatchSummary[];
}

const COMPETITION_LABELS: Record<string, string> = {
  BL1: '1. Bundesliga',
  CL: 'Champions League',
  FL1: 'Ligue 1',
  PD: 'La Liga',
  PL: 'Premier League',
  SA: 'Serie A',
};

export function getFootballDataSourceConfig() {
  return {
    slug: FOOTBALL_DATA_SOURCE_SLUG,
    name: FOOTBALL_DATA_SOURCE_NAME,
    baseUrl: process.env.FOOTBALL_DATA_BASE_URL?.trim() || FOOTBALL_DATA_BASE_URL,
    apiKey: process.env.FOOTBALL_DATA_API_KEY?.trim(),
  };
}

export function getDefaultFootballDataCompetitionTargets(): FootballDataOrgCompetitionTarget[] {
  return DEFAULT_COMPETITION_CODES.map((code) => ({
    code,
    name: COMPETITION_LABELS[code] ?? code,
  }));
}

export function parseFootballDataCompetitionTargets(rawCodes?: string[]) {
  const normalizedCodes = (rawCodes ?? []).map((code) => code.trim().toUpperCase()).filter(Boolean);
  const codes = normalizedCodes.length > 0 ? normalizedCodes : [...DEFAULT_COMPETITION_CODES];

  return codes.map((code) => ({
    code,
    name: COMPETITION_LABELS[code] ?? code,
  }));
}

export function buildFootballDataCompetitionMatchesPath(code: string, season: number) {
  return `/competitions/${code}/matches?season=${season}`;
}

export function buildFootballDataCompetitionTeamsPath(code: string, season: number) {
  return `/competitions/${code}/teams?season=${season}`;
}

export async function fetchFootballDataJson<T>(path: string): Promise<T> {
  const { apiKey, baseUrl } = getFootballDataSourceConfig();

  if (!apiKey) {
    throw new Error('FOOTBALL_DATA_API_KEY is not set');
  }

  const response = await fetch(`${baseUrl}${path}`, {
    headers: {
      'X-Auth-Token': apiKey,
    },
  });

  if (!response.ok) {
    throw new Error(`football-data.org request failed: ${response.status} ${response.statusText}`);
  }

  return response.json() as Promise<T>;
}
